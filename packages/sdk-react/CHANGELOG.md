# meridian-react

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
