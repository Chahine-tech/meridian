---
"@meridian/sdk": minor
"meridian-react": patch
"meridian-devtools": patch
"meridian-cli": patch
---

Add E2E encryption, BFT signing, and cross-SDK feature parity.

- AES-GCM-256 envelope encryption and Ed25519 BFT signing in the TypeScript SDK
- New `packages/python/` — `meridian-sdk` Python package with full CRDT support (GCounter, PNCounter, LwwRegister, Presence), optional AES-GCM-256 encryption and Ed25519 signing, asyncio-native transport with auto-reconnect
- Rust client SDK (`meridian-client`): AES-GCM-256 encryption and Ed25519 BFT signing on LwwRegister and Presence handles (`--features crypto`), HTTP client for REST endpoints (`--features http`), auth token parsing and permission checks
- CRDT compactor background task for RGA and Tree tombstone cleanup
