# Collaborative Workspace — Meridian Example

A minimal collaborative app showing all four main CRDT types in action.

| Feature | CRDT | Behaviour |
|---------|------|-----------|
| Workspace title | `LWWRegister<string>` | Last write wins — edits converge automatically |
| Shared task list | `ORSet<{ text }>` | Add-wins — concurrent add+remove keeps the add |
| Community vote | `PNCounter` | Increment / decrement, can go negative |
| Who's online | `Presence` | 30s TTL, refreshed every 15s, explicit leave on tab close |

Open the same URL in two browser tabs to see live sync.

## Run

**1. Start the server**

```bash
docker-compose up          # from the repo root
```

**2. Generate a token**

```bash
cargo run --bin gen_token -- --namespace workspace
```

**3. Start the example**

```bash
cd examples/collaborative-workspace
npm install
npm run dev
```

Open [http://localhost:5173](http://localhost:5173), paste the token, click **Connect**.
