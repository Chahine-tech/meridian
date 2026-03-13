import { useEffect, useMemo, useRef, useSyncExternalStore } from "react";
import { Schema } from "effect";
import type { PresenceEntry } from "meridian-sdk";
import { useMeridianClient } from "../context.js";

/**
 * Subscribe to a Presence CRDT.
 *
 * Automatically sends a heartbeat on mount and every `heartbeatIntervalMs`.
 * Calls `leave()` on unmount.
 *
 * ```tsx
 * // Observe only (no auto-heartbeat)
 * const { online } = usePresence("pr:room-123");
 *
 * // With auto-heartbeat — define schema outside the component.
 * const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });
 * function MyComponent() {
 *   const { online } = usePresence("pr:cursors", {
 *     schema: CursorSchema,
 *     data: { x: mouseX, y: mouseY },
 *     ttlMs: 5_000,
 *   });
 * }
 * ```
 */
export function usePresence<T = unknown>(
  crdtId: string,
  opts?: {
    schema?: Schema.Schema<T>;
    /** Presence data to heartbeat with. If omitted, no auto-heartbeat. */
    data?: T;
    /** TTL in ms. Default: 30_000 */
    ttlMs?: number;
    /** How often to re-heartbeat (ms). Default: ttlMs / 2 */
    heartbeatIntervalMs?: number;
  },
): {
  online: PresenceEntry<T>[];
  heartbeat: (data: T, ttlMs?: number) => void;
  leave: () => void;
} {
  const client = useMeridianClient();
  const handle = useMemo(
    () => client.presence<T>(crdtId, opts?.schema),
    [client, crdtId, opts?.schema],
  );

  const online = useSyncExternalStore(
    (notify) => handle.onChange(() => notify()),
    () => handle.online(),
    () => handle.online(),
  );

  // Ref so the interval always reads the latest data/ttl without restarting.
  const optsRef = useRef(opts);
  optsRef.current = opts;

  useEffect(() => {
    if (optsRef.current?.data === undefined) return;

    const ttlMs = optsRef.current.ttlMs ?? 30_000;
    const intervalMs = optsRef.current.heartbeatIntervalMs ?? ttlMs / 2;

    handle.heartbeat(optsRef.current.data, ttlMs);
    const timer = setInterval(() => {
      const o = optsRef.current;
      if (o?.data !== undefined) handle.heartbeat(o.data, o.ttlMs ?? 30_000);
    }, intervalMs);

    return () => {
      clearInterval(timer);
      handle.leave();
    };
  }, [handle]);

  return {
    online,
    heartbeat: (data: T, ttlMs = 30_000) => handle.heartbeat(data, ttlMs),
    leave: () => handle.leave(),
  };
}
