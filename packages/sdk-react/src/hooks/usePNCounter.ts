import { useMemo, useSyncExternalStore } from "react";
import { useMeridianClient } from "../context.js";

interface PNCounterHandle {
  value: number;
  increment: (amount?: number) => void;
  decrement: (amount?: number) => void;
}

/**
 * Subscribe to a positive-negative counter (PNCounter) CRDT.
 *
 * Unlike a GCounter, a PNCounter supports both increments and decrements.
 * All operations from every connected client are merged automatically in real time.
 *
 * @param crdtId - Unique identifier for the CRDT within the current namespace.
 * @returns An object containing the current `value`, an `increment` function, and a `decrement` function.
 *
 * @example
 * ```tsx
 * import { usePNCounter } from 'meridian-react';
 *
 * function Score() {
 *   const { value, increment, decrement } = usePNCounter('game-score');
 *
 *   return (
 *     <div>
 *       <button onClick={() => decrement()}>-</button>
 *       <span>{value}</span>
 *       <button onClick={() => increment()}>+</button>
 *     </div>
 *   );
 * }
 * ```
 */
export const usePNCounter = (crdtId: string): PNCounterHandle => {
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
};
