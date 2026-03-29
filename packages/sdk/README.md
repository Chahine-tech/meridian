# meridian-sdk

TypeScript SDK for [Meridian](../README.md) — Effect-based, fully typed, msgpack wire protocol.

## Install

```bash
bun add meridian-sdk
# or
npm install meridian-sdk
```

## Quick start

```ts
import { Effect, Schema } from "effect";
import { MeridianClient } from "meridian-sdk";

const client = await Effect.runPromise(
  MeridianClient.create({
    url: "http://localhost:3000",
    namespace: "my-room",
    token: process.env.MERIDIAN_TOKEN!,
  })
);

// GCounter
const views = client.gcounter("gc:views");
views.increment(1);
views.onChange(v => console.log("views:", v));

// LWW Register with runtime schema validation
const Profile = Schema.Struct({ name: Schema.String, avatar: Schema.String });
const profile = client.lwwregister("lw:profile", Profile);
profile.set({ name: "Chahine", avatar: "https://..." });
profile.onChange(v => console.log("profile:", v)); // v: { name, avatar } | null

// Presence
const Cursor = Schema.Struct({ x: Schema.Number, y: Schema.Number });
const room = client.presence("pr:room", Cursor);
room.heartbeat({ x: 100, y: 200 }, 30_000);
room.onChange(entries => console.log("online:", entries));

// CRDTMap — composite document with named CRDT fields
const doc = client.crdtmap("doc:settings");
doc.lwwSet("theme", "dark");
doc.incrementCounter("edits");

// Close WebSocket when done
client.close();
```

## Error handling

All errors are `Data.TaggedError` — matchable with `Effect.catchTag`:

```ts
import { Effect } from "effect";
import { MeridianClient, TokenExpiredError, HttpError, NetworkError } from "meridian-sdk";

await Effect.runPromise(
  MeridianClient.create(config).pipe(
    Effect.catchTag("TokenExpiredError", (e) =>
      Effect.die(`Token expired at ${new Date(e.expiredAt).toISOString()}`)
    ),
    Effect.flatMap(client =>
      client.http.getCrdt("my-room", "gc:views").pipe(
        Effect.catchTag("HttpError", e => Effect.succeed(`HTTP ${e.status}`)),
        Effect.catchTag("NetworkError", e => Effect.succeed(`Network: ${e.message}`)),
      )
    ),
  )
);
```

## API

### `MeridianClient.create(config)` → `Effect<MeridianClient, TokenParseError | TokenExpiredError>`

| Option | Type | Description |
|--------|------|-------------|
| `url` | `string` | Server base URL (`http://` or `ws://`) |
| `namespace` | `string` | Namespace to connect to |
| `token` | `string` | Meridian token |
| `autoConnect` | `boolean?` | Open WebSocket immediately (default: `true`) |

### CRDT handles

| Method | Returns | Schema? |
|--------|---------|---------|
| `client.gcounter(id)` | `GCounterHandle` | — |
| `client.pncounter(id)` | `PNCounterHandle` | — |
| `client.orset(id, schema?)` | `ORSetHandle<T>` | Optional |
| `client.lwwregister(id, schema?)` | `LwwRegisterHandle<T>` | Optional |
| `client.presence(id, schema?)` | `PresenceHandle<T>` | Optional |
| `client.crdtmap(id)` | `CRDTMapHandle` | — |
| `client.awareness(key, schema?)` | `AwarenessHandle<T>` | Optional |
| `client.rga(id)` | `RGAHandle` | — |
| `client.tree(id)` | `TreeHandle` | — |

Without a schema, `T = unknown`. With a schema, incoming deltas are validated at runtime via `Schema.decodeUnknownSync`.

Every handle exposes a `stream()` method returning a `Stream.Stream<T, never, never>` — composable with the full Effect ecosystem:

```ts
import { Stream, Effect } from "effect";

const views = client.gcounter("gc:views");

// Consume as an Effect Stream
await Effect.runPromise(
  views.stream().pipe(
    Stream.tap(v => Effect.log(`views: ${v}`)),
    Stream.take(10),
    Stream.runDrain,
  )
);
```

### Effect Layer (dependency injection)

`MeridianLive` is an Effect `Layer` that provides `MeridianService` — use it to inject the client into any Effect program without manual wiring:

```ts
import { Effect, Layer } from "effect";
import { MeridianLive, MeridianService } from "meridian-sdk";

const config = { url: "http://localhost:3000", namespace: "my-room", token };

const program = Effect.gen(function* () {
  const meridian = yield* MeridianService;
  const views = meridian.gcounter("gc:views");
  views.increment(1);
});

await Effect.runPromise(program.pipe(Effect.provide(MeridianLive(config))));
```

### Awareness

Ephemeral pub/sub channel — updates are fanned out to all other subscribers in real time but are **not** persisted. Use this for high-frequency transient state like cursor positions or "is typing" indicators.

```ts
const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });
const cursors = client.awareness("cursors", CursorSchema);

// Send our state (fire-and-forget)
cursors.update({ x: 120, y: 80 });

// Listen to peer updates
const unsub = cursors.onChange(peers => {
  console.log("peers:", peers); // AwarenessEntry<{ x, y }>[]
});

// Clear our entry when leaving (e.g. tab hidden, component unmount)
cursors.clear();
```

Unlike `presence`, awareness entries are never stored — if a client connects after a peer's last update, it will not see that peer's state until the peer sends another update.

### TTL-based expiry

Any op can include an optional `ttlMs` to schedule automatic server-side deletion after the given duration. The GC task runs every 5 seconds and permanently removes expired entries.

```ts
// LWW register that auto-deletes after 60 seconds
const session = client.lwwregister("lw:session:abc");
session.set({ userId: 42, role: "editor" }, 60_000);

// GCounter with a 1-hour TTL
const views = client.gcounter("gc:daily-views");
views.increment(1, 3_600_000);

// ORSet entry with a 5-minute TTL
const cart = client.orset("or:cart");
cart.add({ sku: "ABC" }, 300_000);

// CRDTMap with TTL on a single field write
const doc = client.crdtmap("cm:doc");
doc.lwwSet("draft", "Hello world", 86_400_000); // 24h
```

### Offline queue

Operations sent while disconnected are buffered automatically and flushed in order on reconnect. No configuration needed — it works transparently for all CRDT handles.

```ts
// Check how many ops are pending (e.g. for a UI indicator)
client.pendingOpCount; // number

// Subscribe to connection state changes
const unsub = client.onStateChange(state => {
  console.log("connection state:", state); // "CONNECTED" | "DISCONNECTED" | "CONNECTING" | "CLOSING"
});
unsub(); // unsubscribe

// Subscribe to incoming deltas (devtools / debugging)
const unsubDelta = client.onDelta(event => {
  console.log(event.crdtId, event.type, event.at); // "gc:views", "gcounter", 1718000000000
});
unsubDelta();
```

The queue holds up to 500 ops. If the limit is reached, the oldest op is dropped to make room for the newest.

### Op latency

`client.getLatencyStats()` returns P50 and P99 round-trip latency in milliseconds, computed from the last 128 acknowledged ops. Returns `null` if fewer than 2 samples have been collected.

```ts
const stats = client.getLatencyStats();
// { p50: 12.4, p99: 87.1, count: 64 } | null
```

Latency is measured from the moment an op is sent over the WebSocket to the moment the server `Ack` is received. The `meridian-devtools` panel displays this automatically.

### Query Engine — `client.query()`

Aggregate data across multiple CRDTs in a single HTTP request:

```ts
// Sum all page view counters matching a glob
const result = await client.query({ from: "gc:views-*", aggregate: "sum" });
console.log(result.value);        // total
console.log(result.matched);      // number of CRDTs matched
console.log(result.execution_ms); // server latency
```

Supported aggregations: `sum`, `max`, `min`, `count`, `union`, `intersection`, `latest`, `collect`, `merge`.

### Live Queries — `client.liveQuery()`

Subscribe once — get a push every time matching CRDTs change. Uses the existing WebSocket connection, no extra socket opened.

```ts
const handle = client.liveQuery({
  from: "gc:views-*",
  aggregate: "sum",
});

handle.onResult(result => {
  console.log("live total:", result.value);  // pushed on every matching delta
  console.log("matched:", result.matched);
});

// Cancel
handle.close();
```

Set `type` to avoid re-execution for unrelated CRDT deltas:

```ts
client.liveQuery({ from: "gc:views-*", type: "gcounter", aggregate: "sum" });
```

The SDK automatically re-sends active subscriptions after a WebSocket reconnect.

### HTTP client (`client.http`)

All methods return `Effect<T, HttpError | NetworkError>`:

```ts
client.http.getCrdt(ns, id)              // → Effect<CrdtGetResponse, ...>
client.http.postOp(ns, id, op)           // → Effect<CrdtOpResponse, ...>
client.http.syncCrdt(ns, id, sinceVc?)   // → Effect<CrdtGetResponse, ...>
client.http.issueToken(ns, opts)         // → Effect<TokenIssueResponse, ...>
```

## Wire protocol

- **Transport**: HTTP + WebSocket
- **Serialization**: MessagePack (msgpackr)
- **Auth**: Bearer token in `Authorization` header or `?token=` query param

## Development

```bash
bun install
bun test          # run tests
bun run typecheck # tsc --noEmit
bun run build     # compile to dist/
```
