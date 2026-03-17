import { useSyncExternalStore } from "react";
import { useMeridianClient } from "../context.js";

/**
 * Returns the number of operations buffered locally, waiting to be sent on reconnect.
 *
 * Useful for building "syncing" or "offline" indicators in the UI — the count
 * is non-zero while the client is disconnected and has pending writes, and drops
 * to zero once the connection is restored and all ops have been flushed.
 *
 * @returns The number of pending (unsent) operations.
 *
 * @example
 * ```tsx
 * import { usePendingOpCount } from 'meridian-react';
 *
 * function SyncIndicator() {
 *   const pending = usePendingOpCount();
 *   if (pending === 0) return null;
 *   return <span>{pending} change{pending > 1 ? "s" : ""} pending...</span>;
 * }
 * ```
 */
export const usePendingOpCount = (): number => {
  const client = useMeridianClient();

  return useSyncExternalStore(
    (notify) => client.onStateChange(() => notify()),
    () => client.pendingOpCount,
    () => 0,
  );
};
