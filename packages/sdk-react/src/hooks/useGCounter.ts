import { useMemo, useSyncExternalStore } from "react";
import { useMeridianClient } from "../context.js";

interface GCounterHandle {
  value: number;
  increment: (amount?: number) => void;
}

/**
 * Subscribe to a grow-only counter (GCounter) CRDT.
 *
 * The counter can only be incremented and never decremented. All increments
 * from every connected client are merged automatically in real time.
 *
 * @param crdtId - Unique identifier for the CRDT within the current namespace.
 * @returns An object containing the current `value` and an `increment` function.
 *
 * @example
 * ```tsx
 * import { useGCounter } from 'meridian-react';
 *
 * function PageViews() {
 *   const { value, increment } = useGCounter('page-views');
 *
 *   return (
 *     <button onClick={() => increment()}>
 *       Views: {value}
 *     </button>
 *   );
 * }
 * ```
 */
export const useGCounter = (crdtId: string): GCounterHandle => {
  const client = useMeridianClient();
  const handle = useMemo(() => client.gcounter(crdtId), [client, crdtId]);

  const value = useSyncExternalStore(
    (notify) => handle.onChange(() => notify()),
    () => handle.value(),
    () => handle.value(),
  );

  return {
    value,
    increment: (amount = 1) => handle.increment(amount),
  };
};
