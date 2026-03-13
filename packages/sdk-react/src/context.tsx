import { createContext, useContext, useEffect, type ReactNode } from "react";
import type { MeridianClient } from "meridian-sdk";

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

const MeridianContext = createContext<MeridianClient | null>(null);

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

interface MeridianProviderProps {
  client: MeridianClient;
  children: ReactNode;
}

/**
 * Provides a `MeridianClient` to the React tree.
 *
 * ```tsx
 * const client = await Effect.runPromise(MeridianClient.create({ ... }));
 *
 * <MeridianProvider client={client}>
 *   <App />
 * </MeridianProvider>
 * ```
 */
export function MeridianProvider({ client, children }: MeridianProviderProps) {
  useEffect(() => {
    // Disconnect when the provider unmounts (e.g. page navigation / logout).
    return () => {
      client.close();
    };
  }, [client]);

  return (
    <MeridianContext.Provider value={client}>
      {children}
    </MeridianContext.Provider>
  );
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

/**
 * Returns the nearest `MeridianClient` from context.
 * Must be used inside a `<MeridianProvider>`.
 */
export function useMeridianClient(): MeridianClient {
  const client = useContext(MeridianContext);
  if (client === null) {
    throw new Error("useMeridianClient must be used inside <MeridianProvider>");
  }
  return client;
}
