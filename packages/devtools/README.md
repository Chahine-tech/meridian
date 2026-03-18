# meridian-devtools

React devtools overlay for [Meridian](https://github.com/Chahine-tech/meridian) — inspect live CRDT state, connection status, and pending ops during development.

## Installation

```bash
bun add meridian-devtools
# or
npm install meridian-devtools
```

Requires `react ^19.0.0` and `meridian-sdk ^0.3.0` as peer dependencies.

## Usage

Drop `<MeridianDevtools />` anywhere in your component tree, passing the same `client` instance you pass to `MeridianProvider`:

```tsx
import { Effect } from "effect";
import { MeridianClient } from "meridian-sdk";
import { MeridianProvider } from "meridian-react";
import { MeridianDevtools } from "meridian-devtools";

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
      <MeridianDevtools client={client} />
    </MeridianProvider>
  );
}
```

A floating **M** button appears in the bottom-right corner. Click it to toggle the panel.

## Props

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `client` | `MeridianClient` | required | The client instance to inspect |
| `defaultOpen` | `boolean` | `false` | Open the panel on first render |

## Panel

The devtools panel shows:

- **Connection** — WebSocket state (`CONNECTED` / `CONNECTING` / `DISCONNECTED` / `CLOSING`), namespace, client ID, and a warning when ops are pending
- **CRDTs** — live list of all active handles with their current value as JSON, color-coded by type

## Production

`MeridianDevtools` renders nothing in production. The component and its dependencies are removed by bundlers (Vite, Next.js, webpack) via `NODE_ENV` dead-code elimination.

## Requirements

- React 19+
- `meridian-sdk` 0.3+
