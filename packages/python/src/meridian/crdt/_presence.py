import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .._codec import encode

if TYPE_CHECKING:
    from .._transport import Transport


@dataclass
class PresenceEntry:
    client_id: int
    data: Any
    expires_at_ms: int


class Presence:
    """Presence CRDT — tracks which clients are currently online with optional metadata.

    Each client broadcasts a heartbeat to signal that it's still alive.
    Entries expire automatically based on ``ttl_ms``.
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
        self._entries: dict[str, PresenceEntry] = {}
        self._listeners: list[asyncio.Queue] = []

    # ── read ─────────────────────────────────────────────────────────────────

    def online(self) -> list[PresenceEntry]:
        now = int(time.time() * 1000)
        return [e for e in self._entries.values() if e.expires_at_ms > now]

    # ── write ─────────────────────────────────────────────────────────────────

    def join(self, data: Any, *, ttl_ms: int = 30_000) -> None:
        t = asyncio.create_task(self._send_heartbeat(data, ttl_ms))
        t.add_done_callback(lambda _: None)

    async def join_async(self, data: Any, *, ttl_ms: int = 30_000) -> None:
        await self._send_heartbeat(data, ttl_ms)

    def leave(self) -> None:
        wall_ms = int(time.time() * 1000)
        hlc = {"wall_ms": wall_ms, "logical": 0, "node_id": self._client_id}
        op = encode({"Presence": {"Leave": {"client_id": self._client_id, "hlc": hlc}}})
        self._transport.send({"Op": {"crdt_id": self._id, "op_bytes": op}})
        self._entries.pop(str(self._client_id), None)
        self._notify()

    # ── subscribe ─────────────────────────────────────────────────────────────

    async def changes(self) -> AsyncIterator[list[PresenceEntry]]:
        q: asyncio.Queue = asyncio.Queue()
        self._listeners.append(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._listeners.remove(q)

    # ── internal ──────────────────────────────────────────────────────────────

    async def _send_heartbeat(self, data: Any, ttl_ms: int) -> None:
        wall_ms = int(time.time() * 1000)
        hlc = {"wall_ms": wall_ms, "logical": 0, "node_id": self._client_id}
        wire_data = await self._encrypt_fn(data) if self._encrypt_fn else data
        op = encode(
            {
                "Presence": {
                    "Heartbeat": {
                        "client_id": self._client_id,
                        "data": wire_data,
                        "hlc": hlc,
                        "ttl_ms": ttl_ms,
                    }
                }
            }
        )
        self._transport.send({"Op": {"crdt_id": self._id, "op_bytes": op}})

    async def _apply_delta(self, delta: dict) -> None:
        changed = False
        for client_id_str, entry in (delta.get("changes") or {}).items():
            if entry is None:
                if client_id_str in self._entries:
                    del self._entries[client_id_str]
                    changed = True
            else:
                hlc = entry.get("hlc", {})
                ttl = int(entry.get("ttl_ms", 0))
                wall = int(hlc.get("wall_ms", 0))
                expires = wall + ttl
                data = entry.get("data")
                if self._decrypt_fn and isinstance(data, dict) and data.get("$e") == 1:
                    data = await self._decrypt_fn(data)
                new_entry = PresenceEntry(
                    client_id=int(client_id_str),
                    data=data,
                    expires_at_ms=expires,
                )
                existing = self._entries.get(client_id_str)
                if existing is None or expires > existing.expires_at_ms:
                    self._entries[client_id_str] = new_entry
                    changed = True
        if changed:
            self._notify()

    def _notify(self) -> None:
        entries = self.online()
        for q in self._listeners:
            try:
                q.put_nowait(entries)
            except asyncio.QueueFull:
                pass
