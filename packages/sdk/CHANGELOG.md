# meridian-sdk

## 1.0.1

### Patch Changes

- 5cb3908: Fix `issueToken` sending msgpack instead of JSON — the `/tokens` endpoint only accepts JSON.

## 0.4.0

### Minor Changes

- 61e494b: Add TTL-based expiry (`ttlMs` parameter on all CRDT handle methods) and awareness protocol (`AwarenessHandle`, `client.awareness()`).
