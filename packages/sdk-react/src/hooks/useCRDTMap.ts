import { useCallback, useMemo, useSyncExternalStore } from "react";
import { useMeridianClient } from "../context.js";
import type { CrdtMapValue } from "meridian-sdk";

interface CRDTMapHandle {
  value: CrdtMapValue;
  lwwSet: (key: string, val: unknown) => void;
  incrementCounter: (key: string, amount?: number) => void;
  incrementPNCounter: (key: string, amount?: number) => void;
  decrementPNCounter: (key: string, amount?: number) => void;
  orsetAdd: (key: string, element: unknown, tag: Uint8Array) => void;
  orsetRemove: (key: string, element: unknown, knownTags: Uint8Array[]) => void;
}

/**
 * Subscribe to a CRDTMap CRDT — a map of named CRDT values.
 *
 * Each key holds an independent CRDT (GCounter, PNCounter, ORSet, LwwRegister,
 * or Presence). The type of each key is fixed at first write.
 *
 * @param crdtId - Unique identifier for the CRDT within the current namespace.
 * @returns An object containing the current `value` and mutation functions.
 *
 * @example
 * ```tsx
 * import { useCRDTMap } from 'meridian-react';
 *
 * function DocMeta() {
 *   const { value, lwwSet, incrementCounter } = useCRDTMap('doc-meta');
 *
 *   return (
 *     <div>
 *       <input
 *         value={(value.title as { value?: string })?.value ?? ''}
 *         onChange={(e) => lwwSet('title', e.target.value)}
 *       />
 *       <button onClick={() => incrementCounter('views')}>
 *         Views: {(value.views as { total?: number })?.total ?? 0}
 *       </button>
 *     </div>
 *   );
 * }
 * ```
 */
export const useCRDTMap = (crdtId: string): CRDTMapHandle => {
  const client = useMeridianClient();
  const handle = useMemo(() => client.crdtmap(crdtId), [client, crdtId]);

  const value = useSyncExternalStore(
    (notify) => handle.onChange(() => notify()),
    () => handle.value(),
    () => handle.value(),
  );

  const lwwSet = useCallback((key: string, val: unknown) => handle.lwwSet(key, val), [handle]);
  const incrementCounter = useCallback((key: string, amount = 1) => handle.incrementCounter(key, amount), [handle]);
  const incrementPNCounter = useCallback((key: string, amount = 1) => handle.incrementPNCounter(key, amount), [handle]);
  const decrementPNCounter = useCallback((key: string, amount = 1) => handle.decrementPNCounter(key, amount), [handle]);
  const orsetAdd = useCallback((key: string, element: unknown, tag: Uint8Array) => handle.orsetAdd(key, element, tag), [handle]);
  const orsetRemove = useCallback((key: string, element: unknown, knownTags: Uint8Array[]) => handle.orsetRemove(key, element, knownTags), [handle]);

  return useMemo(
    () => ({ value, lwwSet, incrementCounter, incrementPNCounter, decrementPNCounter, orsetAdd, orsetRemove }),
    [value, lwwSet, incrementCounter, incrementPNCounter, decrementPNCounter, orsetAdd, orsetRemove],
  );
};
