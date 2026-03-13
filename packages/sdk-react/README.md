# meridian-react

React hooks for [Meridian](https://github.com/your-org/meridian) — a CRDT server with live sync over WebSocket.

## Installation

```bash
npm install meridian-react meridian-sdk react
# or
pnpm add meridian-react meridian-sdk react
# or
bun add meridian-react meridian-sdk react
```

## Setup

```tsx
import { MeridianProvider } from "meridian-react";

function App() {
  return (
    <MeridianProvider url="ws://localhost:3000" token="your-token">
      <YourApp />
    </MeridianProvider>
  );
}
```

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
const Task = Schema.Struct({ id: Schema.String, title: Schema.String });

// Define schema outside the component for a stable reference
const { elements, add, remove } = useORSet("or:tasks", Task);
```

### `useLwwRegister`

```tsx
const TitleSchema = Schema.String;

const { value, set } = useLwwRegister("lw:doc-title", TitleSchema);
```

### `usePresence`

```tsx
const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });

// Define schema outside the component
const { online, heartbeat, leave } = usePresence("pr:cursors", {
  schema: CursorSchema,
  data: { x: mouseX, y: mouseY },
  ttlMs: 5_000,
});
```

## Requirements

- React 19+
- meridian-sdk 0.2+
