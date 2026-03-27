# meridian-react

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
