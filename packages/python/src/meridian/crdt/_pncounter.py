import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from .._codec import encode

if TYPE_CHECKING:
    from .._transport import Transport


class PNCounter:
    """Positive-negative counter CRDT. Supports both increment and decrement."""

    def __init__(self, crdt_id: str, client_id: int, transport: "Transport") -> None:
        self._id = crdt_id
        self._client_id = client_id
        self._transport = transport
        self._pos: dict[str, int] = {}
        self._neg: dict[str, int] = {}
        self._listeners: list[asyncio.Queue[int]] = []


    def value(self) -> int:
        return sum(self._pos.values()) - sum(self._neg.values())


    def increment(self, amount: int = 1, *, ttl_ms: int | None = None) -> None:
        if amount <= 0:
            raise ValueError("amount must be > 0")
        key = str(self._client_id)
        self._pos[key] = self._pos.get(key, 0) + amount
        self._notify()
        self._send({"Increment": {"client_id": self._client_id, "amount": amount}}, ttl_ms)

    def decrement(self, amount: int = 1, *, ttl_ms: int | None = None) -> None:
        if amount <= 0:
            raise ValueError("amount must be > 0")
        key = str(self._client_id)
        self._neg[key] = self._neg.get(key, 0) + amount
        self._notify()
        self._send({"Decrement": {"client_id": self._client_id, "amount": amount}}, ttl_ms)


    async def changes(self) -> AsyncIterator[int]:
        q: asyncio.Queue[int] = asyncio.Queue()
        self._listeners.append(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._listeners.remove(q)


    def _apply_delta(self, delta: dict) -> None:
        changed = False
        for k, v in (delta.get("pos") or {}).get("counters", {}).items():
            if v > self._pos.get(k, 0):
                self._pos[k] = v
                changed = True
        for k, v in (delta.get("neg") or {}).get("counters", {}).items():
            if v > self._neg.get(k, 0):
                self._neg[k] = v
                changed = True
        if changed:
            self._notify()

    def _send(self, op: dict, ttl_ms: int | None) -> None:
        msg: dict = {"Op": {"crdt_id": self._id, "op_bytes": encode({"PNCounter": op})}}
        if ttl_ms is not None:
            msg["Op"]["ttl_ms"] = ttl_ms
        self._transport.send(msg)

    def _notify(self) -> None:
        v = self.value()
        for q in self._listeners:
            try:
                q.put_nowait(v)
            except asyncio.QueueFull:
                pass
