"""MeridianClient — main entry point for the Python SDK."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

from ._codec import decode, encode, encode_vector_clock
from ._transport import Transport
from .crdt._gcounter import GCounter
from .crdt._lww import LwwRegister
from .crdt._pncounter import PNCounter
from .crdt._presence import Presence


class MeridianClient:
    """Real-time CRDT client for Meridian.

    Usage::

        async with MeridianClient.connect(
            "ws://localhost:8080/v1/namespaces/myapp/connect",
            token="...",
        ) as client:
            counter = client.gcounter("page-views")
            counter.increment()
            print(counter.value())

            async for value in counter.changes():
                print("new value:", value)

    The client reconnects automatically on disconnection.
    """

    def __init__(
        self,
        url: str,
        token: str,
        *,
        client_id: int | None = None,
        signing_keypair=None,
    ) -> None:
        self._url = url
        self._token = token
        self._client_id = client_id or _derive_client_id(token)
        self._signing_keypair = signing_keypair

        self._gcounters: dict[str, GCounter] = {}
        self._pncounters: dict[str, PNCounter] = {}
        self._lwws: dict[str, LwwRegister] = {}
        self._presences: dict[str, Presence] = {}

        self._query_queues: dict[str, list[asyncio.Queue]] = {}

        self._transport = Transport(url, token, self._on_message)

    # ── lifecycle ─────────────────────────────────────────────────────────────

    @classmethod
    async def connect(
        cls,
        url: str,
        *,
        token: str,
        client_id: int | None = None,
        signing_keypair=None,
        wait_connected: bool = True,
    ) -> "MeridianClient":
        """Create a client and wait for the WebSocket to be established."""
        c = cls(url, token, client_id=client_id, signing_keypair=signing_keypair)
        await c._transport.start()
        if wait_connected:
            await c._transport.wait_connected()
        return c

    async def close(self) -> None:
        await self._transport.stop()

    async def __aenter__(self) -> "MeridianClient":
        await self._transport.start()
        await self._transport.wait_connected()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ── CRDT handles ─────────────────────────────────────────────────────────

    def gcounter(self, crdt_id: str) -> GCounter:
        if crdt_id not in self._gcounters:
            h = GCounter(crdt_id, self._client_id, self._transport)
            self._gcounters[crdt_id] = h
            self._subscribe(crdt_id)
        return self._gcounters[crdt_id]

    def pncounter(self, crdt_id: str) -> PNCounter:
        if crdt_id not in self._pncounters:
            h = PNCounter(crdt_id, self._client_id, self._transport)
            self._pncounters[crdt_id] = h
            self._subscribe(crdt_id)
        return self._pncounters[crdt_id]

    def lww_register(
        self,
        crdt_id: str,
        *,
        encrypt_fn=None,
        decrypt_fn=None,
    ) -> LwwRegister:
        if crdt_id not in self._lwws:
            h = LwwRegister(
                crdt_id, self._client_id, self._transport,
                encrypt_fn=encrypt_fn, decrypt_fn=decrypt_fn,
            )
            self._lwws[crdt_id] = h
            self._subscribe(crdt_id)
        return self._lwws[crdt_id]

    def presence(
        self,
        crdt_id: str,
        *,
        encrypt_fn=None,
        decrypt_fn=None,
    ) -> Presence:
        if crdt_id not in self._presences:
            h = Presence(
                crdt_id, self._client_id, self._transport,
                encrypt_fn=encrypt_fn, decrypt_fn=decrypt_fn,
            )
            self._presences[crdt_id] = h
            self._subscribe(crdt_id)
        return self._presences[crdt_id]

    # ── live queries ──────────────────────────────────────────────────────────

    async def live_query(
        self,
        from_: str,
        aggregate: str,
        *,
        crdt_type: str | None = None,
        where: dict | None = None,
    ) -> AsyncIterator[dict]:
        """Subscribe to a live query. Yields {value, matched} on every update.

        Example::

            async for result in client.live_query("page-views:*", aggregate="sum"):
                print(result["value"])
        """
        query_id = str(uuid.uuid4())
        q: asyncio.Queue[dict] = asyncio.Queue()
        self._query_queues.setdefault(query_id, []).append(q)

        query: dict = {"from": from_, "aggregate": aggregate}
        if crdt_type is not None:
            query["type"] = crdt_type
        if where is not None:
            query["where"] = where

        self._transport.send({"SubscribeQuery": {"query_id": query_id, "query": query}})
        try:
            while True:
                yield await q.get()
        finally:
            self._transport.send({"UnsubscribeQuery": {"query_id": query_id}})
            listeners = self._query_queues.get(query_id, [])
            if q in listeners:
                listeners.remove(q)

    # ── awareness (low-level) ─────────────────────────────────────────────────

    def send_awareness(self, key: str, data: bytes) -> None:
        self._transport.send({"AwarenessUpdate": {"key": key, "data": data}})

    # ── sync ─────────────────────────────────────────────────────────────────

    def sync(self, crdt_id: str, since: dict[str, int] | None = None) -> None:
        """Request a delta sync from the server for a given CRDT."""
        vc_bytes = encode_vector_clock(since or {})
        self._transport.send({"Sync": {"crdt_id": crdt_id, "since_vc": vc_bytes}})

    # ── internal ──────────────────────────────────────────────────────────────

    def _subscribe(self, crdt_id: str) -> None:
        self._transport.send({"Subscribe": {"crdt_id": crdt_id}})

    def _on_message(self, msg: Any) -> None:
        if not isinstance(msg, dict):
            return

        if "Delta" in msg:
            self._handle_delta(msg["Delta"])
        elif "QueryResult" in msg:
            qr = msg["QueryResult"]
            for q in self._query_queues.get(qr.get("query_id", ""), []):
                try:
                    q.put_nowait({"value": qr.get("value"), "matched": qr.get("matched", 0)})
                except asyncio.QueueFull:
                    pass
        elif "Error" in msg:
            import logging
            logging.getLogger(__name__).warning("server error %s: %s", msg["Error"].get("code"), msg["Error"].get("message"))

    def _handle_delta(self, delta: dict) -> None:
        crdt_id = delta.get("crdt_id", "")
        delta_bytes = delta.get("delta_bytes", b"")
        if not delta_bytes:
            return

        try:
            raw = decode(delta_bytes)
        except Exception:
            return

        if not isinstance(raw, dict):
            return

        if crdt_id in self._gcounters:
            self._gcounters[crdt_id]._apply_delta(raw)
        elif crdt_id in self._pncounters:
            self._pncounters[crdt_id]._apply_delta(raw)
        elif crdt_id in self._lwws:
            h = self._lwws[crdt_id]
            if h._decrypt_fn is None:
                h._apply_delta_sync(raw)
            else:
                asyncio.create_task(h._apply_delta(raw))
        elif crdt_id in self._presences:
            asyncio.create_task(self._presences[crdt_id]._apply_delta(raw))


def _derive_client_id(token: str) -> int:
    """Decode the client_id from a Meridian token (msgpack + no signature check)."""
    try:
        import base64
        import msgpack
        # Meridian tokens: base64url(msgpack(claims)) + "." + base64url(hmac)
        payload = token.split(".")[0]
        pad = 4 - len(payload) % 4
        raw = base64.urlsafe_b64decode(payload + "=" * (pad % 4))
        claims = msgpack.unpackb(raw, raw=False)
        return int(claims.get("client_id", 0))
    except Exception:
        return 0
