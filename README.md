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
  <img src="https://img.shields.io/badge/tests-138-brightgreen" alt="138 tests" />
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT License" />
</p>

---

## What is Meridian?

Meridian is a self-hosted server that lets multiple clients share state in real-time without conflicts. You pick a CRDT type (counter, set, register, presence), apply operations from any client, and every client converges to the same value automatically — no locks, no last-write-wins bugs.

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
import { MeridianProvider, useGCounter, usePresence } from "meridian-react";
import { Schema } from "effect";

const client = await Effect.runPromise(
  MeridianClient.create({ url: "http://localhost:3000", namespace: "my-room", token })
);

const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });

function Room() {
  const { value, increment } = useGCounter("gc:views");
  const { online } = usePresence("pr:cursors", {
    schema: CursorSchema,
    data: { x: mouseX, y: mouseY }, // auto-heartbeat on data change
    ttlMs: 5_000,
  });

  return <p>{value} views · {online.length} cursors live</p>;
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
views.onChange(v => console.log("views:", v.total));

// Live cursors — schema-validated at runtime
const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });
const cursors = client.presence("pr:cursors", CursorSchema);
cursors.heartbeat({ x: 120, y: 80 }, 5_000);
cursors.onChange(users => console.log("live cursors:", users));

client.close();
```

## CRDT types

| Type | Use case | Example key |
|------|----------|-------------|
| `GCounter` | Page views, likes | `gc:views` |
| `PNCounter` | Inventory, votes | `pn:stock` |
| `ORSet` | Shopping cart, tags | `or:cart` |
| `LwwRegister` | User profile, config | `lw:title` |
| `Presence` | Who's online, cursors | `pr:room` |
| `CRDTMap` | Structured document with typed fields | `cm:doc` |

`CRDTMap` lets you assign a different CRDT type to each key within a single document. Each key merges independently using its own conflict resolution semantics.

## Packages

| Package | Description |
|---------|-------------|
| [`meridian-sdk`](packages/sdk) | TypeScript SDK — Effect-based, msgpack, fully typed |
| [`meridian-react`](packages/sdk-react) | React hooks — `useGCounter`, `usePresence`, etc. |

## Features

- **Real-time sync** over WebSocket with automatic reconnection
- **6 CRDT types** covering the most common collaborative patterns
- **Multinode clustering** — horizontal scaling with Redis Pub/Sub fan-out or HTTP push transport; anti-entropy gossip for convergence after partitions (`--features cluster` / `--features cluster-http`)
- **Pluggable storage** — sled (default), PostgreSQL, Redis, in-memory; WAL with point-in-time recovery
- **Scoped permissions** — token-level read/write access with glob patterns (`allowed:*`)
- **Rate limiting** — 100 req/s per token, sliding window
- **Webhooks** — `POST` to your backend on every op, HMAC-SHA256 signed
- **Prometheus metrics** — ops counter, active WS connections, WAL entries (`GET /metrics`)
- **WAL compaction** — automatic background truncation
- **History API** — audit log per CRDT (`GET /v1/namespaces/:ns/crdts/:id/history`)
- **138 tests** — unit, property-based, and integration

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