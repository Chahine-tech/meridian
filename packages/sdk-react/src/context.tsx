import { createContext, useContext, type ReactNode } from "react";
import { useMountEffect } from "./hooks/useMountEffect.js";
import type { MeridianClient } from "meridian-sdk";

interface MeridianProviderProps {
  client: MeridianClient;
  children: ReactNode;
}

const MeridianContext = createContext<MeridianClient | null>(null);

/**
 * Provides a `MeridianClient` instance to the React component tree.
 *
 * Wrap your application (or the subtree that uses Meridian hooks) with this
 * component. The client is automatically closed when the provider unmounts.
 *
 * @param props.client - A `MeridianClient` instance created via `MeridianClient.create()`.
 * @param props.children - Child components that can access the client via `useMeridianClient()`.
 *
 * @example
 * ```tsx
 * import { Effect } from 'effect';
 * import { MeridianClient } from 'meridian-sdk';
 * import { MeridianProvider } from 'meridian-react';
 *
 * const client = await Effect.runPromise(
 *   MeridianClient.create({ url: 'ws://localhost:8080', namespace: 'app', token: '...' })
 * );
 *
 * function App() {
 *   return (
 *     <MeridianProvider client={client}>
 *       <YourApp />
 *     </MeridianProvider>
 *   );
 * }
 * ```
 */
export const MeridianProvider = ({ client, children }: MeridianProviderProps) => {
  useMountEffect(() => {
    return () => {
      client.close();
    };
  });

  return (
    <MeridianContext.Provider value={client}>
      {children}
    </MeridianContext.Provider>
  );
};

/**
 * Returns the `MeridianClient` provided by the nearest `<MeridianProvider>`.
 *
 * Throws if called outside of a `<MeridianProvider>`. Prefer the typed CRDT
 * hooks (`useGCounter`, `usePNCounter`, etc.) for day-to-day use; reach for
 * this hook when you need direct access to the underlying client.
 *
 * @returns The `MeridianClient` instance from context.
 *
 * @example
 * ```tsx
 * import { useMeridianClient } from 'meridian-react';
 *
 * function DebugPanel() {
 *   const client = useMeridianClient();
 *   return <pre>{JSON.stringify(client.claims)}</pre>;
 * }
 * ```
 */
export const useMeridianClient = (): MeridianClient => {
  const client = useContext(MeridianContext);
  if (client === null) {
    throw new Error("useMeridianClient must be used inside <MeridianProvider>");
  }
  return client;
};
