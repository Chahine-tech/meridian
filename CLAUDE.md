# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Rust

```bash
cargo check -p meridian-core -p meridian-server -p meridian-edge   # type-check all crates
cargo clippy --all-targets -- -D warnings                           # lint (CI gate)
cargo clippy --all-targets --features cluster -- -D warnings        # lint with cluster feature
cargo test                                                          # all server + core tests
cargo test -p meridian-server -- token_me                          # single test by name
cargo check -p meridian-edge --target wasm32-unknown-unknown        # edge WASM check
```

### TypeScript (Bun + Biome)

Each package is independent — run commands from the package directory:

```bash
# SDK
bun run --cwd packages/sdk typecheck     # tsc --noEmit
bun run --cwd packages/sdk lint          # biome lint ./src  (CI gate)
bun run --cwd packages/sdk test          # bun test
bun run --cwd packages/sdk build         # tsc (required before sdk-react/devtools/cli)

# SDK React / Devtools / CLI
bun run --cwd packages/sdk-react typecheck
bun run --cwd packages/devtools typecheck
bun run --cwd packages/cli typecheck
```

Biome is the only linter/formatter — no ESLint, no Prettier. Never add `eslint-disable` comments.

### Releases

```bash
bun changeset          # create changeset entry
bun run release        # publish all packages (from repo root)
```

## Architecture

### Crate layout

```
crates/meridian-core    — pure library: CRDTs, auth, protocol, query engine (no tokio/axum)
crates/meridian-storage — storage trait + backends (sled, postgres, redis, S3 WAL archive)
crates/meridian-cluster — optional multi-node clustering (Redis pub/sub or HTTP push)
crates/meridian-edge    — Cloudflare Workers edge runtime (wasm32, worker crate)
server/                 — axum HTTP server binary + WebSocket handler
```

`meridian-core` has zero async/storage dependencies — it compiles to WASM and is shared between the native server and the edge worker.

### Key data flow

1. **Op ingestion** — client sends a msgpack `CrdtOp` → server validates (HLC drift check, permission check) → appends to WAL → applies to snapshot → broadcasts msgpack delta to all WebSocket subscribers.
2. **Delta sync** — client sends `Sync { crdt_id, since_vc }` with its local vector clock → server returns only ops the client hasn't seen yet.
3. **Live queries** — client sends `SubscribeQuery` with a `LiveQueryPayload` (glob pattern + aggregate) → server evaluates via `meridian_core::query::execute_query_on_values` after every matching op → pushes `QueryResult` frames.

### Auth

Tokens are HMAC-signed msgpack blobs (not JWTs). Two permission formats:
- **V1** — glob lists `{ read: ["*"], write: ["docs:*"] }`
- **V2** — per-rule entries `{ r: [{p, o, e}], w: [{p, o, e}] }` with op masks, TTL, `{clientId}` template

`meridian-core::auth::claims` is the single source of truth for both formats. The TypeScript `packages/sdk/src/auth/permissions.ts` mirrors this logic exactly for client-side checks.

### Storage feature flags

The server selects a backend at compile time:
- `storage-sled` (default) — embedded key-value, single node
- `storage-postgres` — PostgreSQL, required for clustering
- `storage-redis` — Redis
- `cluster` / `cluster-http` — enables `meridian-cluster`

### Edge worker

The edge worker (`crates/meridian-edge`) has full feature parity with the native server. All state lives in a single Durable Object (`NsObject`) per namespace. Live query subscriptions survive hibernation by being persisted to DO KV under `lq:{conn_id}:{query_id}`.

### TypeScript packages

```
packages/sdk            — core SDK: MeridianClient, CRDT handles, HTTP client, WS transport, RPC layer
packages/sdk-react      — React hooks (useMeridianClient, useGCounter, useLiveQuery, …)
packages/devtools       — MeridianDevtools React component (panel + WAL time-travel)
packages/cli            — CLI tool
```

The SDK uses **Effect** for typed errors and async composition, and **@msgpack/msgpack** for wire encoding. All server↔client messages are defined in `packages/sdk/src/schema.ts` as Effect Schemas — these must stay in sync with the Rust `meridian-core::protocol` structs.

The RPC layer (`packages/sdk/src/rpc.ts`) piggybacks on `AwarenessUpdate` frames using the reserved key `"__rpc__"` — no server changes needed for custom typed events.

## Code style

- Rust: Rust 2024 edition. Clippy `-D warnings` must pass, including with `--features cluster`.
- TypeScript: no `any`, no non-null assertions (`!`) without a guard, no `eslint-disable`. Code must pass `biome lint` before committing.
- No ASCII separator comments (`// -----`).
