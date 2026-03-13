<p align="center">
  <img src="https://raw.githubusercontent.com/Chahine-tech/meridian/main/docs/logo/light.svg#gh-light-mode-only" height="40" alt="Meridian" />
  <img src="https://raw.githubusercontent.com/Chahine-tech/meridian/main/docs/logo/dark.svg#gh-dark-mode-only" height="40" alt="Meridian" />
</p>

<p align="center">Self-hosted real-time CRDT store. Alternative to Liveblocks / PartyKit.</p>

No locks, no merge conflicts. Concurrent updates converge automatically.

## Run the server

**Docker (recommended):**

```bash
MERIDIAN_SIGNING_KEY=$(openssl rand -hex 32) docker compose up -d
```

**From source:**

```bash
MERIDIAN_SIGNING_KEY=$(openssl rand -hex 32) \
MERIDIAN_DATA_DIR=./data \
cargo run --release
```

| Variable | Default | |
|---|---|---|
| `MERIDIAN_BIND` | `0.0.0.0:3000` | TCP bind address |
| `MERIDIAN_DATA_DIR` | `./data` | sled storage path |
| `MERIDIAN_SIGNING_KEY` | *(random)* | 32-byte hex ed25519 seed — ephemeral if unset |

## SDK

See [sdk/README.md](sdk/README.md).

## CRDT types

| Type | Use case |
|---|---|
| `GCounter` | Page views, likes |
| `PNCounter` | Inventory, votes |
| `ORSet` | Shopping cart, tags |
| `LwwRegister` | User profile, config |
| `Presence` | Who's online, cursors |

## Stack

**Server:** Rust · tokio · axum · sled · ed25519 · proptest (104 tests)

**SDK:** TypeScript · Effect 3.19 · msgpackr · Bun
