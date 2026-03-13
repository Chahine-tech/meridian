import { useMemo, useSyncExternalStore } from "react";
import { useMeridianClient } from "../context.js";

/**
 * Subscribe to a GCounter CRDT.
 *
 * ```tsx
 * const { value, increment } = useGCounter("gc:page-views");
 * ```
 */
export function useGCounter(crdtId: string): {
  value: number;
  increment: (amount?: number) => void;
} {
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
}
