import asyncio
import logging
from collections.abc import Callable
from urllib.parse import quote

import websockets
import websockets.asyncio.client
from websockets.exceptions import ConnectionClosed

from ._codec import decode, encode

log = logging.getLogger(__name__)

_BACKOFF_INITIAL = 0.1
_BACKOFF_MAX = 30.0
_BACKOFF_FACTOR = 2.0
_QUEUE_MAX = 512


class Transport:
    """WebSocket transport with automatic reconnect and offline queuing."""

    def __init__(
        self,
        url: str,
        token: str,
        on_message: Callable[[dict], None],
    ) -> None:
        sep = "&" if "?" in url else "?"
        self._url = f"{url}{sep}token={quote(token)}"
        self._on_message = on_message

        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._closed = False
        self._connected = asyncio.Event()
        self._send_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._recv_task: asyncio.Task | None = None
        self._send_task: asyncio.Task | None = None
        self._loop_task: asyncio.Task | None = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._loop_task = asyncio.create_task(self._connect_loop(), name="meridian-transport")

    async def stop(self) -> None:
        self._closed = True
        if self._ws is not None:
            await self._ws.close()
        for task in (self._loop_task, self._recv_task, self._send_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

    async def wait_connected(self, deadline: float = 10.0) -> None:
        async with asyncio.timeout(deadline):
            await self._connected.wait()

    # ── send ─────────────────────────────────────────────────────────────────

    def send(self, msg: dict) -> None:
        """Enqueue a message for sending. Drops silently if queue is full."""
        data = encode(msg)
        try:
            self._send_queue.put_nowait(data)
        except asyncio.QueueFull:
            log.warning("send queue full, dropping message")

    # ── internal ─────────────────────────────────────────────────────────────

    async def _connect_loop(self) -> None:
        backoff = _BACKOFF_INITIAL
        while not self._closed:
            try:
                _20mb = 20 * 1024 * 1024
                async with websockets.asyncio.client.connect(self._url, max_size=_20mb) as ws:
                    self._ws = ws
                    self._connected.set()
                    backoff = _BACKOFF_INITIAL
                    log.debug("meridian connected")

                    try:
                        async with asyncio.TaskGroup() as tg:
                            self._recv_task = tg.create_task(self._recv_loop(ws))
                            self._send_task = tg.create_task(self._send_loop(ws))
                    except* ConnectionClosed:
                        pass
                    except* asyncio.CancelledError:
                        raise asyncio.CancelledError from None
            except ConnectionClosed:
                pass
            except Exception:
                log.exception("meridian transport error")
            finally:
                self._ws = None
                self._connected.clear()

            if not self._closed:
                log.debug("meridian reconnecting in %.1fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX)

    async def _recv_loop(self, ws: websockets.asyncio.client.ClientConnection) -> None:
        async for raw in ws:
            if isinstance(raw, bytes):
                try:
                    self._on_message(decode(raw))
                except Exception:
                    log.exception("failed to decode server message")

    async def _send_loop(self, ws: websockets.asyncio.client.ClientConnection) -> None:
        while True:
            data = await self._send_queue.get()
            try:
                await ws.send(data)
            except ConnectionClosed:
                # Re-queue so it's sent after reconnect.
                try:
                    self._send_queue.put_nowait(data)
                except asyncio.QueueFull:
                    pass
                raise
