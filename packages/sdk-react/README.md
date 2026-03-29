# meridian-react

React hooks for [Meridian](../sdk/README.md) — a CRDT server with live sync over WebSocket.

## Installation

```bash
bun add meridian-react
# or
npm install meridian-react
```

Requires `react ^19.0.0`, `meridian-sdk ^1.1.0`, and `effect ^3.21.0` as peer dependencies.

## Setup

`MeridianProvider` takes a `MeridianClient` instance created via `MeridianClient.create()`:

```tsx
import { Effect } from "effect";
import { MeridianClient } from "meridian-sdk";
import { MeridianProvider } from "meridian-react";

const client = await Effect.runPromise(
  MeridianClient.create({
    url: "ws://localhost:3000",
    namespace: "my-app",
    token: process.env.MERIDIAN_TOKEN!,
  })
);

function App() {
  return (
    <MeridianProvider client={client}>
      <YourApp />
    </MeridianProvider>
  );
}
```

The client is automatically closed when the provider unmounts.

## Hooks

### `useGCounter`

```tsx
const { value, increment } = useGCounter("gc:page-views");
```

### `usePNCounter`

```tsx
const { value, increment, decrement } = usePNCounter("pn:votes");
```

### `useORSet`

```tsx
import { Schema } from "effect";

// Define schema outside the component for a stable reference
const Task = Schema.Struct({ id: Schema.String, title: Schema.String });

const { elements, add, remove } = useORSet("or:tasks", Task);
```

### `useLwwRegister`

```tsx
import { Schema } from "effect";

const TitleSchema = Schema.String;

const { value, set } = useLwwRegister("lw:doc-title", TitleSchema);
```

### `usePresence`

Takes an optional `opts` object. When `opts.data` is provided, heartbeats are sent automatically and `leave()` is called on unmount.

```tsx
import { Schema } from "effect";

// Define schema outside the component for a stable reference
const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });

const { online, heartbeat, leave } = usePresence("pr:cursors", {
  schema: CursorSchema,
  data: { x: mouseX, y: mouseY },
  ttlMs: 5_000,
});
```

| Option | Type | Description |
|--------|------|-------------|
| `schema` | `Schema<T>?` | Decode peer data at runtime |
| `data` | `T?` | Data to broadcast — triggers auto-heartbeat |
| `ttlMs` | `number?` | Entry lifetime in ms (default: 30 000) |
| `heartbeatIntervalMs` | `number?` | Override heartbeat send interval |

### `useAwareness`

Ephemeral pub/sub channel for high-frequency transient state (cursors, selections, "is typing"). Updates are fanned out in real time but **not** persisted — new peers won't see your state until you send another update.

```tsx
import { Schema } from "effect";

// Define schema outside the component for a stable reference
const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });

const { peers, update, clear } = useAwareness("cursors", CursorSchema);

// peers: AwarenessEntry<{ x, y }>[] — other clients only, self excluded
// update({ x, y }) — send our position
// clear()         — remove our entry (e.g. on mouse leave)
```

| Option | Type | Description |
|--------|------|-------------|
| `key` | `string` | Awareness channel name (e.g. `"cursors"`, `"selection:doc-1"`) |
| `schema` | `Schema<T>?` | Decode peer payloads at runtime |

`peers` excludes the current client. Use `peers.length + 1` for a total visitor count — or combine with `usePresence` for an accurate count that includes clients who haven't moved yet.

### `useRGA`

Collaborative text editing — ordered sequence CRDT.

```tsx
const { value, insert, delete: del } = useRGA("rg:doc-123");
```

### `useTree`

Collaborative hierarchical tree — outlines, mind maps, document trees.

```tsx
const { roots, addNode, moveNode, updateNode, deleteNode } = useTree("tr:outline");

// Add root node
const rootId = addNode(null, "a0", "Introduction");
// Add child
const childId = addNode(rootId, "a0", "Chapter 1");
// Move, update, delete
moveNode(childId, null, "b0");
updateNode(childId, "Chapter 1 — Updated");
deleteNode(childId);
```

`roots` is an array of `TreeNodeValue` — `{ id, value, children: TreeNodeValue[] }`. Concurrent moves converge via Kleppmann et al. (2021) — cycle-creating moves are silently discarded.

### `useCRDTMap`

```tsx
const { value, lwwSet, incrementCounter } = useCRDTMap("doc:meta");

lwwSet("theme", "dark");
incrementCounter("views");
```

### `useQuery`

One-shot cross-CRDT query over HTTP. Re-runs when `spec` changes.

```tsx
import { useMemo } from "react";
import { useQuery } from "meridian-react";

function TotalViews() {
  const spec = useMemo(() => ({ from: "gc:views-*", aggregate: "sum" as const }), []);
  const { data, loading, error } = useQuery(spec);

  if (loading) return <span>Loading…</span>;
  return <span>Total: {String(data?.value)}</span>;
}
```

### `useLiveQuery`

Reactive WebSocket subscription — the server pushes a new result on every matching CRDT delta. Subscribes on mount, unsubscribes on unmount.

```tsx
import { useMemo } from "react";
import { useLiveQuery } from "meridian-react";

function LiveTotal() {
  const spec = useMemo(() => ({ from: "gc:views-*", aggregate: "sum" as const }), []);
  const { data, loading, error } = useLiveQuery(spec);

  if (loading) return <span>Connecting…</span>;
  return <span>Live views: {String(data?.value)}</span>;
}
```

Stabilize `spec` with `useMemo` — a new object reference re-subscribes. The SDK re-sends the subscription automatically after a WebSocket reconnect.

| | `useQuery` | `useLiveQuery` |
|---|---|---|
| Transport | HTTP POST | WebSocket push |
| Updates on | `spec` change | every matching delta |
| Use case | one-shot reads | live dashboards, reactive aggregates |

### `usePendingOpCount`

Returns the number of operations buffered locally, waiting to be sent on reconnect. Useful for building a "syncing" or "offline" indicator.

```tsx
import { usePendingOpCount } from "meridian-react";

function SyncIndicator() {
  const pending = usePendingOpCount();
  if (pending === 0) return null;
  return <span>{pending} change{pending > 1 ? "s" : ""} pending...</span>;
}
```

### `useMeridianClient`

Access the underlying `MeridianClient` directly when needed:

```tsx
import { useMeridianClient } from "meridian-react";

function DebugPanel() {
  const client = useMeridianClient();
  return <pre>{JSON.stringify(client.claims)}</pre>;
}
```

## Requirements

- React 19+
- meridian-sdk 1.1+
