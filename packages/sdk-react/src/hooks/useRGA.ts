import { useMemo, useSyncExternalStore } from "react";
import { useMeridianClient } from "../context.js";

interface RGAHandle {
  text: string;
  insert: (pos: number, text: string, ttlMs?: number) => void;
  delete: (pos: number, length: number, ttlMs?: number) => void;
}

/**
 * Subscribe to an RGA (Replicated Growable Array) CRDT for collaborative text editing.
 *
 * Concurrent inserts from multiple clients are merged automatically using
 * Hybrid Logical Clock ordering — no merge conflicts.
 *
 * @param crdtId - Unique identifier for the CRDT within the current namespace.
 * @returns An object containing the current `text`, an `insert` function, and a `delete` function.
 *
 * @example
 * ```tsx
 * import { useRGA } from 'meridian-react';
 *
 * function CollaborativeEditor() {
 *   const { text, insert, delete: del } = useRGA('doc:content');
 *
 *   return (
 *     <textarea
 *       value={text}
 *       onChange={(e) => {
 *         // simplified — production use needs diff-based ops
 *         insert(0, e.target.value);
 *       }}
 *     />
 *   );
 * }
 * ```
 */
export const useRGA = (crdtId: string): RGAHandle => {
  const client = useMeridianClient();
  const handle = useMemo(() => client.rga(crdtId), [client, crdtId]);

  const text = useSyncExternalStore(
    (notify) => handle.onChange(() => notify()),
    () => handle.value(),
    () => handle.value(),
  );

  return {
    text,
    insert: (pos, t, ttlMs) => handle.insert(pos, t, ttlMs),
    delete: (pos, length, ttlMs) => handle.delete(pos, length, ttlMs),
  };
};
