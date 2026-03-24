# meridian-sdk

## 1.1.0

### Minor Changes

- fe97f56: Add op latency P50/P99 tracking and WAL history tab to devtools.

  `MeridianClient` now exposes `getLatencyStats()` returning round-trip P50/P99 computed over a rolling 128-sample window. The devtools panel gains a WAL history tab (time-travel inspection) and a live latency display in the connection section.

## 1.0.1

### Patch Changes

- 5cb3908: Fix `issueToken` sending msgpack instead of JSON — the `/tokens` endpoint only accepts JSON.

## 0.4.0

### Minor Changes

- 61e494b: Add TTL-based expiry (`ttlMs` parameter on all CRDT handle methods) and awareness protocol (`AwarenessHandle`, `client.awareness()`).
