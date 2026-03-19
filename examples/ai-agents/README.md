# Meridian — Collaborative AI Agents

Multiple Claude agents collaborate in real time using Meridian CRDTs as shared state.

Each worker agent summarizes a chunk of a document. A coordinator agent waits for all workers to finish, then aggregates their summaries into a final document summary. A React UI visualizes agent activity live as it happens.

## Architecture

```
run.ts          → splits document, issues tokens, spawns agents
worker.ts (×3)  → summarizes a chunk via Claude API → writes to CRDTMap
coordinator.ts  → waits for workers → aggregates → writes final summary
ui/             → React dashboard (useCRDTMap, usePresence, useGCounter)
```

**Shared CRDTs** (namespace: `ai-agents`):
- `CRDTMap "agents:state"` — per-worker status, chunk, summary, model
- `CRDTMap "coordinator:state"` — coordinator status + final summary
- `GCounter "agents:tasks-completed"` — progress bar
- `Presence "agents:presence"` — which agents are currently online

## Prerequisites

- Meridian server running (`cargo run --release`)
- Anthropic API key
- An admin token for the `ai-agents` namespace

## Setup

```bash
# From the repo root
bun install

# Set environment variables
export MERIDIAN_URL=ws://localhost:3000
export MERIDIAN_ADMIN_TOKEN=<your admin token>
export ANTHROPIC_API_KEY=<your Anthropic API key>
```

## Run

**Terminal 1 — UI:**
```bash
cd examples/ai-agents/ui
bun run dev
# → http://localhost:5175
# Connect with any valid token and namespace "ai-agents"
```

**Terminal 2 — Agents:**
```bash
cd examples/ai-agents

# With the built-in sample document
bun run agents

# Or with your own document
bun run agents:file -- --file /path/to/document.txt
```

The agents will appear in the UI as they connect. Watch their status change from `idle` → `processing` → `done` in real time, and see the final summary appear when the coordinator finishes.

## Audit trail

After the agents finish, inspect the full WAL history of every op applied:

```bash
cd examples/ai-agents
bun run audit
```

Output shows every op in chronological order with its WAL sequence number, timestamp, and decoded operation — useful for tracing exactly what each agent wrote and when.
