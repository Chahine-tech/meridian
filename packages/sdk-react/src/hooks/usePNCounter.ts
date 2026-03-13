import { useMemo, useSyncExternalStore } from "react";
import { useMeridianClient } from "../context.js";

/**
 * Subscribe to a PNCounter CRDT.
 *
 * ```tsx
 * const { value, increment, decrement } = usePNCounter("pn:votes");
 * ```
 */
export function usePNCounter(crdtId: string): {
  value: number;
  increment: (amount?: number) => void;
  decrement: (amount?: number) => void;
} {
  const client = useMeridianClient();
  const handle = useMemo(() => client.pncounter(crdtId), [client, crdtId]);

  const value = useSyncExternalStore(
    (notify) => handle.onChange(() => notify()),
    () => handle.value(),
    () => handle.value(),
  );

  return {
    value,
    increment: (amount = 1) => handle.increment(amount),
    decrement: (amount = 1) => handle.decrement(amount),
  };
}
