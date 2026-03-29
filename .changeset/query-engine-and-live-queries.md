---
"meridian-sdk": minor
"meridian-react": minor
---

**meridian-sdk**: Add Query Engine (`client.query()`, `QuerySpec`, `QueryResult`) for cross-CRDT scan/filter/aggregate over HTTP. Add Live Queries (`client.liveQuery()`, `LiveQueryHandle`, `LiveQueryResult`) for reactive WebSocket subscriptions that push updated results on every matching CRDT delta, with automatic re-subscription on reconnect.

**meridian-react**: Add `useQuery` hook for one-shot HTTP queries and `useLiveQuery` hook for reactive WebSocket-pushed query results.
