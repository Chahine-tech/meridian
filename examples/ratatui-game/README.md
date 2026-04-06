# ratatui-game

Multiplayer terminal leaderboard powered by Meridian CRDTs.

Each player has a **GCounter** — press `Space` to score a point. Everyone connected to the same namespace sees the live leaderboard update in real time, with no server-side game logic.

```
┌─────────────────────────────────────────────┐
│        🎮 Meridian Leaderboard (alice)       │
├─────────────────────────────────────────────┤
│ 🥇 alice        ████████████░░░░░░░░   42 pts│
│ 🥈 bob          ████████░░░░░░░░░░░░   28 pts│
│ 🥉 charlie      ██░░░░░░░░░░░░░░░░░░    7 pts│
├─────────────────────────────────────────────┤
│ Your score — alice          ████░░░  42 pts  │
├─────────────────────────────────────────────┤
│      [Space] Score a point    [q] Quit       │
└─────────────────────────────────────────────┘
```

## Usage

**1. Start the Meridian server**

```bash
MERIDIAN_SIGNING_KEY=dev cargo run --bin meridian
```

**2. Generate a token** (or use the dev token from the CLI)

```bash
cargo run --bin meridian-cli -- token --key dev --namespace game-room
```

**3. Run the game in multiple terminals**

```bash
# Terminal 1
cargo run -p ratatui-game -- \
  --player alice \
  --peers bob,charlie \
  --token <TOKEN>

# Terminal 2
cargo run -p ratatui-game -- \
  --player bob \
  --peers alice,charlie \
  --token <TOKEN>
```

## Options

| Flag          | Default                  | Description                        |
|---------------|--------------------------|------------------------------------|
| `--url`       | `ws://localhost:3000`    | Meridian server WebSocket URL      |
| `--namespace` | `game-room`              | Namespace (shared room)            |
| `--player`    | `player1`                | Your player name                   |
| `--token`     | `dev`                    | Auth token                         |
| `--peers`     | _(empty)_                | Comma-separated list of peer names |

## How it works

- Each player corresponds to a GCounter CRDT keyed `gc:score:<name>`
- `Space` calls `gcounter.increment(1)` — optimistic local apply + sends op to server
- Server broadcasts the delta to all subscribers
- The 100 ms tick + `on_change` callback trigger a redraw whenever any score changes
- Works offline — ops are queued and replayed on reconnect
