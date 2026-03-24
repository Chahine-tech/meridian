# meridian-devtools

## 2.0.0

### Minor Changes

- fe97f56: Add op latency P50/P99 tracking and WAL history tab to devtools.

  `MeridianClient` now exposes `getLatencyStats()` returning round-trip P50/P99 computed over a rolling 128-sample window. The devtools panel gains a WAL history tab (time-travel inspection) and a live latency display in the connection section.

### Patch Changes

- Updated dependencies [fe97f56]
  - meridian-sdk@1.1.0

## 1.0.1

### Patch Changes

- Updated dependencies [5cb3908]
  - meridian-sdk@1.0.1

## 1.0.0

### Patch Changes

- Updated dependencies [61e494b]
  - meridian-sdk@0.4.0
