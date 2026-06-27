---
"meridian-sdk": minor
"meridian-react": minor
"meridian-devtools": minor
"meridian-cli": minor
---

Upgrade to TypeScript 6, Effect 3.21.4, and Biome 2.5.1. Fix Effect usage across all packages: `getHistory` now returns `Effect` instead of `Promise`, `parseToken` uses proper `yield* Effect.fail` and `Effect.try` patterns, WebSocket message decoding uses `runFork` instead of orphaned `runPromise`. Full type-safety pass for TypeScript 6 strictness (`Uint8Array<ArrayBuffer>`, `exactOptionalPropertyTypes`, DOM lib explicit).
