import { useMemo, useRef, useSyncExternalStore } from "react";
import type { Schema } from "effect";
import type { AwarenessEntry } from "meridian-sdk";
import { useMeridianClient } from "../context.js";

interface AwarenessHandle<T> {
  peers: AwarenessEntry<T>[];
  update: (data: T) => void;
  clear: () => void;
}

/**
 * Subscribe to an ephemeral awareness channel.
 *
 * Awareness is a stateless pub/sub mechanism: updates are fanned out to all
 * other subscribers in the namespace in real time but are **not** persisted.
 * Use this for high-frequency, transient UI state like cursor positions,
 * text selections, or "is typing" indicators.
 *
 * Unlike `usePresence`, awareness updates are never stored — if a client
 * connects after a peer's last update, it will not see that peer's state
 * until the peer sends another update.
 *
 * @param key    - Logical channel name (e.g. `"cursors"`, `"selection:doc-1"`).
 * @param schema - Optional Effect schema used to decode peer payloads at runtime.
 * @returns An object with the current `peers` array, an `update` function, and a `clear` function.
 *
 * @example
 * ```tsx
 * import { useAwareness } from 'meridian-react';
 * import { Schema } from 'effect';
 *
 * const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });
 *
 * function CursorCanvas() {
 *   const { peers, update, clear } = useAwareness('cursors', CursorSchema);
 *
 *   return (
 *     <div
 *       onMouseMove={(e) => update({ x: e.clientX, y: e.clientY })}
 *       onMouseLeave={() => clear()}
 *     >
 *       {peers.map(peer => (
 *         <div key={peer.clientId} style={{ left: peer.data.x, top: peer.data.y }}>
 *           cursor
 *         </div>
 *       ))}
 *     </div>
 *   );
 * }
 * ```
 */
export const useAwareness = <T = unknown>(
  key: string,
  schema?: Schema.Schema<T>,
): AwarenessHandle<T> => {
  const client = useMeridianClient();
  const handle = useMemo(
    () => client.awareness<T>(key, schema),
    [client, key, schema],
  );

  const snapshotRef = useRef<AwarenessEntry<T>[]>([]);
  const getSnapshot = useMemo(
    () => () => {
      const next = handle.peers();
      const prev = snapshotRef.current;
      // Reference-equality check per entry to avoid creating a new array on
      // every render when nothing has changed (prevents useSyncExternalStore
      // infinite loop).
      if (next.length === prev.length && next.every((e, i) => e === prev[i])) {
        return prev;
      }
      snapshotRef.current = next;
      return next;
    },
    [handle],
  );

  const peers = useSyncExternalStore(
    (notify) => handle.onChange(() => notify()),
    getSnapshot,
    getSnapshot,
  );

  return {
    peers,
    update: (data: T) => handle.update(data),
    clear: () => handle.clear(),
  };
};
