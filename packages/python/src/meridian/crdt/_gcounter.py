import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from .._codec import encode

if TYPE_CHECKING:
    from .._transport import Transport


class GCounter:
    """Grow-only counter CRDT. Increment-only, convergent across all nodes."""

    def __init__(self, crdt_id: str, client_id: int, transport: "Transport") -> None:
        self._id = crdt_id
        self._client_id = client_id
        self._transport = transport
        self._counts: dict[str, int] = {}
        self._listeners: list[asyncio.Queue[int]] = []


    def value(self) -> int:
        return sum(self._counts.values())

    def counts(self) -> dict[str, int]:
        return dict(self._counts)


    def increment(self, amount: int = 1, *, ttl_ms: int | None = None) -> None:
        if amount <= 0:
            raise ValueError("amount must be > 0")
        key = str(self._client_id)
        self._counts[key] = self._counts.get(key, 0) + amount
        self._notify()
        op = encode({"GCounter": {"client_id": self._client_id, "amount": amount}})
        msg: dict = {"Op": {"crdt_id": self._id, "op_bytes": op}}
        if ttl_ms is not None:
            msg["Op"]["ttl_ms"] = ttl_ms
        self._transport.send(msg)


    async def changes(self) -> AsyncIterator[int]:
        """Yields the counter value every time it changes."""
        q: asyncio.Queue[int] = asyncio.Queue()
        self._listeners.append(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._listeners.remove(q)


    def _apply_delta(self, delta: dict) -> None:
        changed = False
        for k, v in (delta.get("counters") or {}).items():
            if v > self._counts.get(k, 0):
                self._counts[k] = v
                changed = True
        if changed:
            self._notify()

    def _notify(self) -> None:
        v = self.value()
        for q in self._listeners:
            try:
                q.put_nowait(v)
            except asyncio.QueueFull:
                pass
