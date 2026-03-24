---
"meridian-sdk": minor
"meridian-devtools": minor
---

Add op latency P50/P99 tracking and WAL history tab to devtools.

`MeridianClient` now exposes `getLatencyStats()` returning round-trip P50/P99 computed over a rolling 128-sample window. The devtools panel gains a WAL history tab (time-travel inspection) and a live latency display in the connection section.
