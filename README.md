<p align="center">
  <img src="https://raw.githubusercontent.com/Chahine-tech/meridian/main/docs/logo/light.svg#gh-light-mode-only" width="220" alt="Meridian" />
  <img src="https://raw.githubusercontent.com/Chahine-tech/meridian/main/docs/logo/dark.svg#gh-dark-mode-only" width="220" alt="Meridian" />
</p>

<p align="center">
  Self-hosted real-time CRDT sync server.<br/>
  Open-source alternative to Liveblocks and PartyKit — no vendor lock-in, no merge conflicts.<br/>
  <sub>Built on <a href="https://effect.website">Effect TS</a> — type-safe errors, composable async, runtime schema validation.</sub>
</p>

<p align="center">
  <a href="https://github.com/Chahine-tech/meridian/actions"><img src="https://github.com/Chahine-tech/meridian/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <img src="https://img.shields.io/badge/rust-2024-orange" alt="Rust 2024" />
  <a href="https://www.npmjs.com/package/meridian-sdk"><img src="https://img.shields.io/npm/dw/meridian-sdk" alt="meridian-sdk downloads" /></a>
  <a href="https://www.npmjs.com/package/meridian-react"><img src="https://img.shields.io/npm/dw/meridian-react" alt="meridian-react downloads" /></a>
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT License" />
</p>

---

## What is Meridian?

Meridian is a real-time CRDT sync server that runs **self-hosted on native infrastructure** or **at the edge on Cloudflare Workers** — same SDK, same protocol, two deployment targets.

You pick a CRDT type (counter, set, register, presence), apply operations from any client, and every client converges to the same value automatically — no locks, no last-write-wins bugs.

## Quick start

```bash
# Start the server
MERIDIAN_SIGNING_KEY=$(openssl rand -hex 32) docker compose up -d

# Issue a token
curl -X POST http://localhost:3000/v1/namespaces/my-room/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"client_id": 1, "ttl_ms": 3600000}'
```

## Usage

### React

```tsx
import { Effect } from "effect";
import { MeridianClient } from "meridian-sdk";
import { MeridianProvider, useAwareness, useGCounter } from "meridian-react";
import { Schema } from "effect";

const client = await Effect.runPromise(
  MeridianClient.create({ url: "http://localhost:3000", namespace: "my-room", token })
);

const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });

function Room() {
  const { value, increment } = useGCounter("gc:views");
  // Awareness: ephemeral cursor positions, not persisted
  const { peers, update } = useAwareness("cursors", CursorSchema);

  return <p>{value} views · {peers.length} peers live</p>;
}

function App() {
  return (
    <MeridianProvider client={client}>
      <Room />
    </MeridianProvider>
  );
}
```

### TypeScript (framework-agnostic)

The SDK exposes a simple imperative API on top of an Effect-based core — use `runPromise` to bridge into your existing async code, or compose with `Effect.gen` for full type-safe pipelines.

```ts
import { MeridianClient } from "meridian-sdk";
import { Effect, Schema } from "effect";

const client = await Effect.runPromise(
  MeridianClient.create({ url: "http://localhost:3000", namespace: "my-room", token })
);

// Counters, registers, sets — all conflict-free
const views = client.gcounter("gc:views");
views.increment(1);
views.onChange(v => console.log("views:", v));

// Every handle has stream() — composable with Effect
import { Stream } from "effect";
await Effect.runPromise(
  views.stream().pipe(Stream.take(5), Stream.runForEach(v => Effect.log(`views: ${v}`)))
);

// Awareness — ephemeral cursors, not persisted, schema-validated at runtime
const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });
const cursors = client.awareness("cursors", CursorSchema);
cursors.update({ x: 120, y: 80 });
cursors.onChange(peers => console.log("live cursors:", peers));

client.close();
```

## CRDT types

| Type | Use case | Example key |
|------|----------|-------------|
| `GCounter` | Page views, likes | `gc:views` |
| `PNCounter` | Inventory, votes | `pn:stock` |
| `ORSet` | Shopping cart, tags | `or:cart` |
| `LwwRegister` | User profile, config | `lw:title` |
| `Presence` | Who's online, visitor count | `pr:room` |
| `CRDTMap` | Structured document with typed fields | `cm:doc` |
| `RGA` | Ordered sequence — collaborative text editing | `rg:doc` |
| `TreeCRDT` | Hierarchical tree — outlines, document trees, mind maps | `tr:outline` |

### Awareness

Non-CRDT ephemeral channel for high-frequency transient state (cursors, selections, "is typing"). Updates fan out in real time but are never persisted — use `Presence` when you need durability and TTL-based cleanup, `Awareness` when you need raw speed.

`CRDTMap` lets you assign a different CRDT type to each key within a single document. Each key merges independently using its own conflict resolution semantics.

## Collaborative text editing (RGA)

`RGA` (Replicated Growable Array) is Meridian's ordered-sequence CRDT — the same algorithm behind collaborative editors like Google Docs. Every character has a stable, unique identity across all peers, so concurrent insertions and deletions converge without conflict.

```ts
const doc = client.rga("rg:document");

// Insert text at position 5 (0-indexed character offset)
doc.insert(5, "Hello");

// Delete 3 characters starting at position 2
doc.delete(2, 3);

// Read the current text
const { text } = doc.value();

// React to remote changes
doc.onChange(({ text }) => editor.setValue(text));
```

```tsx
// React
const { text, insert, delete: del } = useRga("rg:document");
```

Concurrent edits from multiple clients are merged automatically — the final text is identical on every peer regardless of arrival order.

## Hierarchical trees (TreeCRDT)

`TreeCRDT` implements the [Kleppmann et al. 2021 move-operation algorithm](https://martin.kleppmann.com/papers/move-op.pdf) — the only known CRDT that handles concurrent *move* operations correctly. This makes it suitable for outlines, task hierarchies, document trees, and mind maps where nodes are frequently reorganized.

```ts
const tree = client.tree("tr:outline");

// Create nodes
const root = tree.addNode(null, { title: "Project" });
const task1 = tree.addNode(root, { title: "Research" });
const task2 = tree.addNode(root, { title: "Implementation" });

// Move a node to a different parent — safe under concurrent moves
tree.move(task2, task1);

// Read the current tree
const { roots } = tree.value();
// roots = [{ id, data, children: [...] }]

// React to remote changes
tree.onChange(({ roots }) => renderTree(roots));
```

```tsx
// React
const { roots, addNode, move } = useTree("tr:outline");
```

Concurrent moves (e.g. two peers moving the same node to different parents at the same time) are resolved deterministically — no cycles, no lost nodes.

## Query Engine

Aggregate data across multiple CRDTs in a single request — no need to read them one by one.

```ts
// Sum all page view counters matching a glob pattern
const result = await client.query({ from: "gc:views-*", aggregate: "sum" });
console.log(result.value);  // total across all matched GCounters

// Union all shopping carts
const carts = await client.query({ from: "or:cart-*", aggregate: "union" });

// Latest config value across regions
const config = await client.query({ from: "lw:config-*", aggregate: "latest" });
```

Or reactively in React:

```tsx
const spec = useMemo(() => ({ from: "gc:views-*", aggregate: "sum" as const }), []);
const { data, loading } = useQuery(spec);
```

See the [Query Engine docs](https://meridian.example.com/sdk/query) for the full aggregation table and `where` clause filters.

## Live Queries

Subscribe once — get a push every time matching CRDTs change. No polling, no manual re-fetch.

```ts
const handle = client.liveQuery({ from: "gc:views-*", aggregate: "sum" });
handle.onResult(result => console.log("live total:", result.value));

// Cancel
handle.close();
```

Or in React — `useLiveQuery` connects on mount, updates on every delta, and disconnects on unmount:

```tsx
const spec = useMemo(() => ({ from: "gc:views-*", aggregate: "sum" as const }), []);
const { data, loading } = useLiveQuery(spec);
```

The SDK re-sends subscriptions automatically after a WebSocket reconnect. Set `type` to avoid re-executing queries for unrelated CRDT deltas:

```ts
// Only re-executes when a GCounter changes — skips ORSet/LwwRegister deltas
client.liveQuery({ from: "gc:views-*", type: "gcounter", aggregate: "sum" });
```

## Edge deploy (Cloudflare Workers)

Deploy Meridian to the edge in minutes — no server, no Docker, no ops.

```bash
cd crates/meridian-edge
cp .dev.vars.example .dev.vars  # add your MERIDIAN_SIGNING_KEY
wrangler dev                     # local dev
wrangler deploy                  # production on Cloudflare
```

The edge runtime uses **Durable Objects** for per-namespace state (replaces sled), compiles to WASM via `wasm-bindgen`, and exposes the exact same WebSocket + REST API as the native server. Your SDK client connects to either without any code change:

```ts
import { Effect } from "effect";
import { MeridianClient } from "meridian-sdk";

// Native server
const client = await Effect.runPromise(
  MeridianClient.create({ url: "http://localhost:3000", namespace: "my-room", token })
);

// Edge worker (same SDK, different URL)
const client = await Effect.runPromise(
  MeridianClient.create({ url: "https://my-worker.workers.dev", namespace: "my-room", token })
);
```

See [`crates/meridian-edge/`](crates/meridian-edge/) for full setup.

## Packages

**npm**

| Package | Description |
|---------|-------------|
| [`meridian-sdk`](packages/sdk) | TypeScript SDK — Effect-based, msgpack, fully typed. Includes `stream()` on all CRDT handles and `MeridianLive` Layer for DI |
| [`meridian-react`](packages/sdk-react) | React hooks — `useGCounter`, `usePresence`, `useAwareness`, etc. |
| [`meridian-devtools`](packages/devtools) | Devtools panel — CRDT inspector, event stream, WAL history, op latency P50/P99 |
| [`meridian-cli`](packages/cli) | CLI — `meridian inspect` and `meridian replay` for terminal-based debugging |

**Rust crates**

| Crate | Description |
|-------|-------------|
| [`server`](server) | Native binary — axum + tokio |
| [`meridian-core`](crates/meridian-core) | Shared logic — CRDTs, auth, protocol (no runtime dep) |
| [`meridian-edge`](crates/meridian-edge) | Cloudflare Workers runtime — WASM, Durable Objects |
| [`meridian-storage`](crates/meridian-storage) | Pluggable storage backends — sled, PostgreSQL, Redis, in-memory; S3 WAL archive (`--features wal-archive-s3`) |
| [`meridian-cluster`](crates/meridian-cluster) | Multinode clustering — Redis Pub/Sub + HTTP push transport |
| [`meridian-client`](crates/meridian-client) | Rust client SDK — all 8 CRDT handles, WebSocket transport, `FakeTransport` for tests |
| [`meridian-pg`](crates/meridian-pg) | Postgres extension — CRDT SQL functions + notify trigger for pg-sync |

## Postgres integration (`--features pg-sync`)

Meridian can sync directly from your existing Postgres tables — no separate message broker needed. Every SQL write on a CRDT column is broadcast to all WebSocket clients in real time.

```
SQL UPDATE → meridian_pg trigger → pg_notify → Meridian server → WebSocket → client
                                 ↑
                    WAL stream (for payloads > 8 KB)
```

**How it works:**

1. Install the `meridian_pg` Postgres extension — adds SQL functions (`gcounter_increment`, `pncounter_increment`, `orset_add`, `lww_set`, …) and a notify trigger
2. Add a `BYTEA` column per CRDT type to your table and attach the trigger
3. Start Meridian with `--features pg-sync` and `DATABASE_URL`

```bash
MERIDIAN_FEATURES=pg-sync \
DATABASE_URL=postgres://user:pass@localhost/mydb \
MERIDIAN_WAL_CONNSTR=postgres://user:pass@localhost/mydb \
docker compose --profile pg up
```

The server opens a logical replication slot (`meridian_wal`) and subscribes to `pg_notify` — both paths feed the same idempotent merge, so large payloads (RGA, Tree) are covered by WAL while small ones arrive via notify.

See [`examples/postgres-articles/`](examples/postgres-articles/) for a full working demo.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MERIDIAN_BIND` | `0.0.0.0:3000` | TCP bind address |
| `MERIDIAN_DATA_DIR` | `./data` | sled storage path |
| `MERIDIAN_SIGNING_KEY` | *(random)* | 32-byte hex ed25519 seed |
| `MERIDIAN_WEBHOOK_URL` | *(unset)* | Webhook endpoint URL |
| `MERIDIAN_WEBHOOK_SECRET` | *(unset)* | HMAC-SHA256 signing secret |
| `REDIS_URL` | *(unset)* | Redis URL — enables cluster mode (`--features cluster`) |
| `MERIDIAN_PEERS` | *(unset)* | Comma-separated peer URLs — enables HTTP cluster mode (`--features cluster-http`) |
| `MERIDIAN_NODE_ID` | *(auto)* | Unique node ID — auto-derived from hostname+port if unset |
| `MERIDIAN_ANTI_ENTROPY_SECS` | `30` | Gossip interval in seconds |
| `S3_BUCKET` | *(unset)* | Enable S3 WAL archive — bucket name (`--features wal-archive-s3`) |
| `S3_ENDPOINT` | *(unset)* | S3-compatible endpoint override (R2, MinIO, LocalStack) |
| `S3_REGION` | `us-east-1` | AWS region |
| `S3_KEY_PREFIX` | `wal/` | Object key prefix for WAL segments |
| `WAL_SEGMENT_SIZE` | `500` | Number of WAL entries per S3 segment |
| `DATABASE_URL` | *(unset)* | PostgreSQL URL — enables pg-sync transport and Postgres storage (`--features pg-sync`) |
| `MERIDIAN_WAL_CONNSTR` | *(unset)* | Replication connection URL — enables WAL stream for payloads > 8 KB |
| `MERIDIAN_WAL_SLOT` | `meridian_wal` | Logical replication slot name |
| `MERIDIAN_WAL_PUB` | `meridian_pub` | Publication name (`FOR ALL TABLES`) |