---
"meridian-sdk": minor
"meridian-react": minor
"meridian-devtools": patch
---

feat(crdt): add TreeCRDT for hierarchical collaborative editing

Implements a convergent hierarchical tree CRDT based on Kleppmann et al. (2021). Supports addNode, moveNode, updateNode, and deleteNode operations with cycle detection, LWW value updates, and tombstone deletes. The `client.tree(id)` factory and `useTree()` React hook are now available.
