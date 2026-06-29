import asyncio
import time
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from .._codec import encode

if TYPE_CHECKING:
    from .._transport import Transport

_UNSET = object()


class LwwRegister:
    """Last-Write-Wins register CRDT.

    The winning entry is chosen by (wall_ms DESC, logical DESC, author DESC).
    Encryption is supported via an optional async ``encrypt_fn``/``decrypt_fn`` pair.
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

    # ── read ─────────────────────────────────────────────────────────────────

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

    # ── write ─────────────────────────────────────────────────────────────────

    def set(self, value: Any, *, ttl_ms: int | None = None) -> None:
        wall_ms = int(time.time() * 1000)
        hlc = {"wall_ms": wall_ms, "logical": 0, "node_id": self._client_id}
        new_entry = {"value": value, "hlc": hlc, "author": self._client_id}
        if self._wins(new_entry, self._entry):
            self._entry = new_entry
            self._notify()
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
        if self._wins(new_entry, self._entry):
            self._entry = new_entry
            self._notify()
        await self._send_set(value, hlc, ttl_ms)

    # ── subscribe ─────────────────────────────────────────────────────────────

    async def changes(self) -> AsyncIterator[Any]:
        q: asyncio.Queue = asyncio.Queue()
        self._listeners.append(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._listeners.remove(q)

    # ── internal ──────────────────────────────────────────────────────────────

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
