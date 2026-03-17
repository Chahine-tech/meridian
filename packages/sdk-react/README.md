# meridian-react

React hooks for [Meridian](../sdk/README.md) — a CRDT server with live sync over WebSocket.

## Installation

```bash
bun add meridian-react
# or
npm install meridian-react
```

Requires `react ^19.0.0` and `meridian-sdk ^0.3.0` as peer dependencies.

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

### `useCRDTMap`

```tsx
const { value, lwwSet, incrementCounter } = useCRDTMap("doc:meta");

lwwSet("theme", "dark");
incrementCounter("views");
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
- meridian-sdk 0.3+
