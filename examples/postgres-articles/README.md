# postgres-articles

Demonstrates Meridian's Postgres integration: every SQL write on a `BYTEA` CRDT column is instantly broadcast to all connected WebSocket clients via `pg_notify` and the WAL replication stream.

```
SQL UPDATE → meridian_pg trigger → pg_notify → Meridian server → WebSocket → client.ts
                                 ↑
                    WAL stream (for payloads > 8 KB)
```

## What this example covers

| Column | CRDT | Written from |
|--------|------|-------------|
| `views` | GCounter | SQL (`gcounter_increment`) |
| `likes` | PNCounter | SQL (`pncounter_increment/decrement`) |
| `tags` | ORSet | SQL (`orset_add/remove`) |
| `headline` | LwwRegister | SQL (`lww_set`) |
| `body` | RGA | SDK only (collaborative text) |

## Prerequisites

- Docker + Docker Compose
- Bun (or Node 20+)
- `cargo-pgrx` 0.13 to build the extension (or use a pre-built `.so`)

## Run

### 1. Start Postgres + Meridian

```bash
# From the repo root
MERIDIAN_FEATURES=pg-sync \
DATABASE_URL=postgres://meridian:meridian@localhost/meridian \
MERIDIAN_WAL_CONNSTR=postgres://meridian:meridian@localhost/meridian \
docker compose --profile pg up
```

### 2. Install the extension and create the schema

```bash
# Install meridian_pg into the running Postgres container
cd crates/meridian-pg
cargo pgrx install --pg-config /path/to/pg_config

# Apply schema and seed data
psql postgres://meridian:meridian@localhost/meridian -f examples/postgres-articles/schema.sql
psql postgres://meridian:meridian@localhost/meridian -f examples/postgres-articles/seed.sql
```

### 3. Generate a token

```bash
MERIDIAN_SIGNING_KEY=4242424242424242424242424242424242424242424242424242424242424242 \
cargo run --bin gen_token -- articles 1
# Copy the printed token
```

### 4. Start the live subscriber

```bash
cd examples/postgres-articles
bun install
TOKEN=<your-token> bun start
```

You should see the initial state printed for each article.

### 5. Simulate concurrent SQL writes

In a separate terminal:

```bash
psql postgres://meridian:meridian@localhost/meridian -f examples/postgres-articles/simulate.sql
```

Watch the `client.ts` terminal — every write arrives within milliseconds:

```
[12:34:56.123] rust-intro.views → 3
[12:34:56.145] crdt-explained.likes → 1
[12:34:56.167] rust-intro.likes → -1
[12:34:56.189] postgres-tips.tags → [ 'postgres', 'performance', 'indexing' ]
[12:34:56.201] crdt-explained.headline → CRDTs Explained — A Practical Guide
```

### 6. Write from a second SDK client (RGA body)

Open a second terminal and run `client.ts` again with the same token. Then insert text into the body from the first client — both clients see the merged result simultaneously, with no conflicts.

```ts
// In a quick script or REPL:
const body = client.rga("article:rust-intro:body");
body.insert(0, "Rust is a systems programming language...");
```

## Key files

| File | Description |
|------|-------------|
| `schema.sql` | Extension, table, trigger |
| `seed.sql` | Initial articles with CRDT values |
| `simulate.sql` | SQL writes that trigger live updates |
| `client.ts` | SDK subscriber — prints deltas in real time |
