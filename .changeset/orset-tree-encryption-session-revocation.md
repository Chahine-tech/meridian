---
"meridian-sdk": patch
---

Add deterministic AES-GCM encryption for ORSet and Tree, and session revocation API.

- `encryptJsonDeterministic(key, value)` — nonce derived from SHA-256(plaintext)[0:12], required for ORSet so Remove ops can reconstruct the same server-side key
- `ORSetHandle` and `TreeHandle` now accept an `encryptFn` via the `encryption` config in `MeridianClient.create()`
- `client.http.revokeSession(ns, clientId)` — forcefully close a WebSocket session (admin token required, server sends close code 4401)
- `encryptJsonDeterministic` exported from `meridian-sdk`
