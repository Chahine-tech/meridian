---
"meridian-sdk": patch
---

Fix `issueToken` sending msgpack instead of JSON — the `/tokens` endpoint only accepts JSON.
