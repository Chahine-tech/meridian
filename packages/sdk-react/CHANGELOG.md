# meridian-react

## 1.5.1

### Patch Changes

- 664b1e4: Add E2E encryption, BFT signing, and cross-SDK feature parity.

  - AES-GCM-256 envelope encryption and Ed25519 BFT signing in the TypeScript SDK
  - New `packages/python/` — `meridian-crdt` Python package with full CRDT support (GCounter, PNCounter, LwwRegister, Presence), optional AES-GCM-256 encryption and Ed25519 signing, asyncio-native transport with auto-reconnect
  - Rust client SDK (`meridian-client`): AES-GCM-256 encryption and Ed25519 BFT signing on LwwRegister and Presence handles (`--features crypto`), HTTP client for REST endpoints (`--features http`), auth token parsing and permission checks
  - CRDT compactor background task for RGA and Tree tombstone cleanup

## 1.5.0

### Minor Changes

- 0360ecc: Upgrade to TypeScript 6, Effect 3.21.4, and Biome 2.5.1. Fix Effect usage across all packages: `getHistory` now returns `Effect` instead of `Promise`, `parseToken` uses proper `yield* Effect.fail` and `Effect.try` patterns, WebSocket message decoding uses `runFork` instead of orphaned `runPromise`. Full type-safety pass for TypeScript 6 strictness (`Uint8Array<ArrayBuffer>`, `exactOptionalPropertyTypes`, DOM lib explicit).

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

### Patch Changes

- Updated dependencies [3086c3d]
  - meridian-sdk@1.2.1

## 1.2.0

### Minor Changes

- 9286eec: Add RGA (Replicated Growable Array) CRDT for collaborative text editing. Includes `useRga` React hook and devtools inspector support.

### Patch Changes

- Updated dependencies [9286eec]
  - meridian-sdk@1.2.0

## 1.1.0

### Patch Changes

- Updated dependencies [fe97f56]
  - meridian-sdk@1.1.0

## 1.0.1

### Patch Changes

- Updated dependencies [5cb3908]
  - meridian-sdk@1.0.1

## 1.0.0

### Minor Changes
