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

Without a schema, `T = unknown`. With a schema, incoming deltas are validated at runtime via `Schema.decodeUnknownSync`.

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
```

The queue holds up to 500 ops. If the limit is reached, the oldest op is dropped to make room for the newest.

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
