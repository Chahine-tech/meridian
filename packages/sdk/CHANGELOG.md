# meridian-sdk

## 1.4.1

### Patch Changes

- 886cbf7: feat: type-safe RPC layer (createMeridianRpc), live queries, devtools panel, edge query engine

## 1.4.0

### Minor Changes

- f234dcc: **meridian-sdk**: Add Query Engine (`client.query()`, `QuerySpec`, `QueryResult`) for cross-CRDT scan/filter/aggregate over HTTP. Add Live Queries (`client.liveQuery()`, `LiveQueryHandle`, `LiveQueryResult`) for reactive WebSocket subscriptions that push updated results on every matching CRDT delta, with automatic re-subscription on reconnect.

  **meridian-react**: Add `useQuery` hook for one-shot HTTP queries and `useLiveQuery` hook for reactive WebSocket-pushed query results.

## 1.3.0

### Minor Changes

- 48647ce: **meridian-sdk**: Add granular permissions V2 schema (`PermissionsV2`, `PermEntry`, `OpMask`), multi-provider AI agent adapters (`executeOpenAITool`, `executeGeminiTool`, `OpenAIToolCall`, `GeminiFunctionCall`), and fix ORSet delta sync to return real deltas based on VectorClock instead of full state.

  **meridian-devtools**: Add colors for `rga` (orange) and `tree` (sky) CRDT types in the visualization panel.

  **meridian-react**: Compatibility update for granular permissions V2 and ORSet delta sync improvements in meridian-sdk.

## 1.2.1

### Minor Changes

- 3086c3d: feat(crdt): add TreeCRDT for hierarchical collaborative editing

  Implements a convergent hierarchical tree CRDT based on Kleppmann et al. (2021). Supports addNode, moveNode, updateNode, and deleteNode operations with cycle detection, LWW value updates, and tombstone deletes. The `client.tree(id)` factory and `useTree()` React hook are now available.

## 1.2.0

### Minor Changes

- 9286eec: Add RGA (Replicated Growable Array) CRDT for collaborative text editing. Includes `useRga` React hook and devtools inspector support.

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
