---
"meridian-sdk": minor
---

- Add offline-first persistence: snapshot serialization for all 8 CRDT handles, `SyncStateStorage` op queue with `memoryStateStorage` and `localStorageSyncOpsAdapter`
- Add Postgres pg-sync integration and configuration reference (`DATABASE_URL`, `MERIDIAN_WAL_*`)
- Fix `ORSetHandle<T>` type invariance with Effect Schema in snapshot functions
