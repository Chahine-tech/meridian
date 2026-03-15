<p align="center">
  <img src="https://raw.githubusercontent.com/Chahine-tech/meridian/main/docs/logo/light.svg#gh-light-mode-only" width="220" alt="Meridian" />
  <img src="https://raw.githubusercontent.com/Chahine-tech/meridian/main/docs/logo/dark.svg#gh-dark-mode-only" width="220" alt="Meridian" />
</p>

<p align="center">
  Self-hosted real-time CRDT sync server.<br/>
  Open-source alternative to Liveblocks and PartyKit — no vendor lock-in, no merge conflicts.
</p>

<p align="center">
  <a href="https://github.com/Chahine-tech/meridian/actions"><img src="https://github.com/Chahine-tech/meridian/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <img src="https://img.shields.io/badge/rust-2024-orange" alt="Rust 2024" />
  <img src="https://img.shields.io/badge/tests-135-brightgreen" alt="135 tests" />
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
import { MeridianProvider, useGCounter, usePresence } from "meridian-react";
import { Schema } from "effect";

function App() {
  return (
    <MeridianProvider url="ws://localhost:3000" token={token}>
      <Room />
    </MeridianProvider>
  );
}

const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });

function Room() {
  const { value, increment } = useGCounter("gc:views");
  const { online } = usePresence("pr:cursors", {
    schema: CursorSchema,
    data: { x: mouseX, y: mouseY },
    ttlMs: 5_000,
  });

  return <p>{value} views · {online.length} online</p>;
}
```

### TypeScript (framework-agnostic)

```ts
import { MeridianClient } from "meridian-sdk";
import { Effect } from "effect";

const client = await Effect.runPromise(
  MeridianClient.create({ url: "http://localhost:3000", namespace: "my-room", token })
);

const views = client.gcounter("gc:views");
views.increment(1);
views.onChange(v => console.log("views:", v.total));

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
- **Scoped permissions** — token-level read/write access with glob patterns (`allowed:*`)
- **Rate limiting** — 100 req/s per token, sliding window
- **Webhooks** — `POST` to your backend on every op, HMAC-SHA256 signed
- **Prometheus metrics** — ops counter, active WS connections, WAL entries (`GET /metrics`)
- **WAL compaction** — automatic background truncation
- **History API** — audit log per CRDT (`GET /v1/namespaces/:ns/crdts/:id/history`)
- **135 tests** — unit, property-based, and integration

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MERIDIAN_BIND` | `0.0.0.0:3000` | TCP bind address |
| `MERIDIAN_DATA_DIR` | `./data` | sled storage path |
| `MERIDIAN_SIGNING_KEY` | *(random)* | 32-byte hex ed25519 seed |
| `MERIDIAN_WEBHOOK_URL` | *(unset)* | Webhook endpoint URL |
| `MERIDIAN_WEBHOOK_SECRET` | *(unset)* | HMAC-SHA256 signing secret |

## Stack

**Server:** Rust · tokio · axum · sled · ed25519 · proptest

**SDK:** TypeScript · Effect 3 · msgpackr · Bun
