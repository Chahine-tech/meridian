import asyncio
import time
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from .._codec import encode

if TYPE_CHECKING:
    from .._transport import Transport

_UNSET = object()
_MAX_UNDO_STACK = 50


class LwwRegister:
    """Last-Write-Wins register CRDT.

    The winning entry is chosen by (wall_ms DESC, logical DESC, author DESC).
    Encryption is supported via an optional async ``encrypt_fn``/``decrypt_fn`` pair.

    Undo is server-validated (Kleppmann & Stewen PaPoC 2024): ``undo()`` sends an
    ``UndoLww`` request; the server only applies it when the target entry is still
    current, preventing concurrent-write stomping.
    """

    def __init__(
        self,
        crdt_id: str,
        client_id: int,
        transport: "Transport",
        *,
        encrypt_fn=None,
        decrypt_fn=None,
    ) -> None:
        self._id = crdt_id
        self._client_id = client_id
        self._transport = transport
        self._encrypt_fn = encrypt_fn
        self._decrypt_fn = decrypt_fn

        self._entry: dict | None = None  # {"value", "hlc", "author"}
        self._listeners: list[asyncio.Queue] = []
        self._undo_stack: list[dict] = []  # [{"target_hlc_bytes", "prev_entry"}]
        self._undo_result_listeners: list[asyncio.Queue] = []


    def value(self) -> Any:
        return self._entry["value"] if self._entry else None

    def meta(self) -> dict | None:
        if not self._entry:
            return None
        hlc = self._entry["hlc"]
        return {
            "updated_at_ms": int(hlc["wall_ms"]),
            "author": int(self._entry["author"]),
        }


    def set(self, value: Any, *, ttl_ms: int | None = None) -> None:
        wall_ms = int(time.time() * 1000)
        hlc = {"wall_ms": wall_ms, "logical": 0, "node_id": self._client_id}
        new_entry = {"value": value, "hlc": hlc, "author": self._client_id}
        prev_entry = self._entry
        if self._wins(new_entry, self._entry):
            self._entry = new_entry
            self._notify()
        self._push_undo(hlc, prev_entry)
        if self._encrypt_fn is None:
            # No encryption — send synchronously.
            op = encode({"LwwRegister": {"value": value, "hlc": hlc, "author": self._client_id}})
            msg: dict = {"Op": {"crdt_id": self._id, "op_bytes": op}}
            if ttl_ms is not None:
                msg["Op"]["ttl_ms"] = ttl_ms
            self._transport.send(msg)
        else:
            t = asyncio.create_task(self._send_set(value, hlc, ttl_ms))
            t.add_done_callback(lambda _: None)

    async def set_async(self, value: Any, *, ttl_ms: int | None = None) -> None:
        """Awaitable version — use when an encrypt_fn is configured."""
        wall_ms = int(time.time() * 1000)
        hlc = {"wall_ms": wall_ms, "logical": 0, "node_id": self._client_id}
        new_entry = {"value": value, "hlc": hlc, "author": self._client_id}
        prev_entry = self._entry
        if self._wins(new_entry, self._entry):
            self._entry = new_entry
            self._notify()
        self._push_undo(hlc, prev_entry)
        await self._send_set(value, hlc, ttl_ms)


    @property
    def can_undo(self) -> bool:
        return bool(self._undo_stack)

    def undo(self) -> None:
        """Send a server-validated undo request for the most recent local set().

        The server applies the undo only if the target entry is still current
        (i.e. no concurrent remote write has overwritten it since). The result
        arrives via ``undo_results()``.
        """
        if not self._undo_stack:
            return
        entry = self._undo_stack.pop()
        prev_entry = entry["prev_entry"]
        restore_value = prev_entry["value"] if prev_entry is not None else None
        self._transport.send({
            "UndoLww": {
                "crdt_id": self._id,
                "target_hlc": entry["target_hlc_bytes"],
                "restore_entry": encode(restore_value),
            }
        })

    async def undo_results(self) -> AsyncIterator[dict]:
        """Yields ``{"ok": True}`` on UndoAck or ``{"ok": False, "reason": str}``
        on UndoSkipped."""
        q: asyncio.Queue[dict] = asyncio.Queue()
        self._undo_result_listeners.append(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._undo_result_listeners.remove(q)


    async def changes(self) -> AsyncIterator[Any]:
        q: asyncio.Queue = asyncio.Queue()
        self._listeners.append(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._listeners.remove(q)


    def _push_undo(self, hlc: dict, prev_entry: dict | None) -> None:
        if len(self._undo_stack) >= _MAX_UNDO_STACK:
            self._undo_stack.pop(0)
        self._undo_stack.append({"target_hlc_bytes": encode(hlc), "prev_entry": prev_entry})

    def _apply_undo_ack(self) -> None:
        self._notify_undo_result({"ok": True})

    def _apply_undo_skipped(self, reason: str) -> None:
        self._notify_undo_result({"ok": False, "reason": reason})

    def _notify_undo_result(self, result: dict) -> None:
        for q in self._undo_result_listeners:
            try:
                q.put_nowait(result)
            except asyncio.QueueFull:
                pass

    async def _send_set(
        self,
        value: Any,
        hlc: dict,
        ttl_ms: int | None,
    ) -> None:
        wire_value = await self._encrypt_fn(value) if self._encrypt_fn else value
        op = encode({"LwwRegister": {"value": wire_value, "hlc": hlc, "author": self._client_id}})
        msg: dict = {"Op": {"crdt_id": self._id, "op_bytes": op}}
        if ttl_ms is not None:
            msg["Op"]["ttl_ms"] = ttl_ms
        self._transport.send(msg)

    def _apply_delta_sync(self, delta: dict) -> None:
        """Apply a delta synchronously (no decryption). Used internally and in tests."""
        entry = delta.get("entry")
        if entry is None:
            return
        if self._wins(entry, self._entry):
            self._entry = entry
            self._notify()

    async def _apply_delta(self, delta: dict) -> None:
        entry = delta.get("entry")
        if entry is None:
            return
        if self._wins(entry, self._entry):
            v = entry.get("value")
            if self._decrypt_fn and isinstance(v, dict) and v.get("$e") == 1:
                entry = dict(entry)
                entry["value"] = await self._decrypt_fn(entry["value"])
            self._entry = entry
            self._notify()

    @staticmethod
    def _wins(candidate: dict, existing: dict | None) -> bool:
        if existing is None:
            return True
        c_hlc, e_hlc = candidate["hlc"], existing["hlc"]
        if int(c_hlc["wall_ms"]) != int(e_hlc["wall_ms"]):
            return int(c_hlc["wall_ms"]) > int(e_hlc["wall_ms"])
        if c_hlc["logical"] != e_hlc["logical"]:
            return c_hlc["logical"] > e_hlc["logical"]
        return int(candidate["author"]) > int(existing["author"])

    def _notify(self) -> None:
        v = self.value()
        for q in self._listeners:
            try:
                q.put_nowait(v)
            except asyncio.QueueFull:
                pass
