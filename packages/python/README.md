# meridian-crdt

Python asyncio client for [Meridian](../../README.md) — real-time CRDT sync over WebSocket.

## Install

```bash
pip install meridian-crdt

# With AES-GCM encryption and Ed25519 signing support
pip install meridian-crdt[crypto]
```

## Quick start

```python
import asyncio
from meridian import MeridianClient

async def main():
    async with MeridianClient(
        url="ws://localhost:3000",
        namespace="my-room",
        token="...",
    ) as client:
        views = client.gcounter("gc:views")
        views.increment(1)
        print(views.value())  # 1

asyncio.run(main())
```

## CRDT handles

### GCounter

```python
views = client.gcounter("gc:views")
views.increment(5)
print(views.value())  # int

views.on_change(lambda v: print("views:", v))  # returns unsubscribe callable
```

### PNCounter

```python
balance = client.pncounter("pn:balance")
balance.increment(100)
balance.decrement(20)
print(balance.value())  # int (can be negative)
```

### LwwRegister

```python
profile = client.lww_register("lw:profile")
profile.set({"name": "Alice", "role": "admin"})
print(profile.value())  # dict | None

# With AES-GCM encryption (requires meridian-crdt[crypto])
from meridian.crypto import generate_aes_gcm_key
key = generate_aes_gcm_key()

enc_profile = client.lww_register("lw:private-profile", encrypt_key=key, decrypt_key=key)
enc_profile.set({"secret": "value"})  # encrypted on the wire
```

### Presence

```python
room = client.presence("pr:room-1")
room.heartbeat({"cursor": {"x": 100, "y": 200}}, ttl_ms=30_000)
print(room.online())  # list[PresenceEntry]

room.on_change(lambda entries: print("online:", [e.data for e in entries]))
```

## Undo

PNCounter operations can be undone via `UndoManager`:

```python
from meridian import MeridianClient
from meridian.crdt import UndoManager

async with MeridianClient(...) as client:
    balance = client.pncounter("pn:balance")
    undo = UndoManager()

    undo.pn_increment(balance, 100)  # increment and record
    print(balance.value())  # 100

    if undo.can_undo:
        undo.undo()  # sends inverse decrement
        print(balance.value())  # 0
```

`LwwRegister` has built-in server-validated undo:

```python
profile = client.lww_register("lw:profile")
profile.set({"name": "Alice"})
profile.set({"name": "Bob"})

await profile.undo()   # server validates atomically before applying
print(profile.value()) # {"name": "Alice"} if undo succeeded
```

## Live queries

```python
async for result in client.live_query("gc:views-*", aggregate="sum"):
    print(f"total views: {result['value']}")  # pushed on every matching delta
```

Supported aggregates: `sum`, `max`, `min`, `count`, `union`, `intersection`, `latest`, `collect`, `merge`.

## Conflict notifications

The client logs conflicts to `logging.getLogger("meridian._client")` at `INFO` level:

```
INFO meridian._client — conflict on lw:profile: LwwOverwritten
```

## Encryption

Pass `encrypt_key` / `decrypt_key` to any handle that supports it (`LwwRegister`, `Presence`). Keys are `AesGcmKey` objects from `meridian.crypto`:

```python
from meridian.crypto import generate_aes_gcm_key, import_aes_gcm_key

key = generate_aes_gcm_key()                   # generate
key = import_aes_gcm_key(b"32-byte-raw-key")   # import from bytes
```

Requires `pip install meridian-crdt[crypto]`.

## API

### `MeridianClient(url, namespace, token, *, connect_timeout=10.0)`

Async context manager. Also supports manual lifecycle:

```python
client = MeridianClient(url=..., namespace=..., token=...)
await client.connect()
# ...
await client.close()
```

### CRDT handles

| Method | Returns |
|--------|---------|
| `client.gcounter(crdt_id)` | `GCounter` |
| `client.pncounter(crdt_id)` | `PNCounter` |
| `client.lww_register(crdt_id, *, encrypt_key?, decrypt_key?)` | `LwwRegister` |
| `client.presence(crdt_id, *, encrypt_key?, decrypt_key?)` | `Presence` |

### Other

| Method | Description |
|--------|-------------|
| `client.live_query(from_, aggregate, *, crdt_type?, where?)` | Async generator — yields `{value, matched}` on every update |
| `client.sync(crdt_id, since?)` | Request a delta sync from the server |

## Development

```bash
pip install -e ".[crypto]"
pip install pytest pytest-asyncio
pytest
ruff check src/
```
