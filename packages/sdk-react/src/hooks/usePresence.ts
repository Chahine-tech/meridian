import { useMemo, useRef, useSyncExternalStore } from "react";
import { useMountEffect } from "./useMountEffect.js";
import type { Schema } from "effect";
import type { PresenceEntry } from "meridian-sdk";
import { useMeridianClient } from "../context.js";
import { DEFAULT_PRESENCE_TTL_MS, HEARTBEAT_INTERVAL_DIVISOR } from "../constants.js";

interface PresenceOptions<T> {
  schema?: Schema.Schema<T>;
  data?: T;
  ttlMs?: number;
  heartbeatIntervalMs?: number;
}

interface PresenceHandle<T> {
  online: PresenceEntry<T>[];
  heartbeat: (data: T, ttlMs?: number) => void;
  leave: () => void;
}

/**
 * Subscribe to a presence channel to track which clients are currently online.
 *
 * When `opts.data` is provided the hook automatically sends periodic heartbeats
 * to keep the current client visible to others, and calls `leave()` on unmount.
 * Each entry expires after `ttlMs` milliseconds if no heartbeat is received.
 *
 * @param crdtId - Unique identifier for the presence channel within the current namespace.
 * @param opts - Optional configuration: `data` to broadcast, `schema` to decode peer data,
 *   `ttlMs` for entry lifetime, and `heartbeatIntervalMs` to override the send interval.
 * @returns An object containing the `online` list of presence entries, a manual `heartbeat`
 *   function, and a `leave` function.
 *
 * @example
 * ```tsx
 * import { usePresence } from 'meridian-react';
 * import { Schema } from 'effect';
 *
 * const UserSchema = Schema.Struct({ name: Schema.String });
 *
 * function OnlineUsers() {
 *   const { online } = usePresence('room-1', {
 *     schema: UserSchema,
 *     data: { name: 'Alice' },
 *     ttlMs: 10_000,
 *   });
 *
 *   return (
 *     <ul>
 *       {online.map(entry => (
 *         <li key={entry.clientId}>{entry.data.name}</li>
 *       ))}
 *     </ul>
 *   );
 * }
 * ```
 */
export const usePresence = <T = unknown>(
  crdtId: string,
  opts?: PresenceOptions<T>,
): PresenceHandle<T> => {
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

  const optsRef = useRef(opts);
  optsRef.current = opts;

  useMountEffect(() => {
    if (optsRef.current?.data === undefined) return;

    const ttlMs = optsRef.current.ttlMs ?? DEFAULT_PRESENCE_TTL_MS;
    const intervalMs = optsRef.current.heartbeatIntervalMs ?? ttlMs / HEARTBEAT_INTERVAL_DIVISOR;

    handle.heartbeat(optsRef.current.data, ttlMs);
    const timer = setInterval(() => {
      const currentOpts = optsRef.current;
      if (currentOpts?.data !== undefined) {
        handle.heartbeat(currentOpts.data, currentOpts.ttlMs ?? DEFAULT_PRESENCE_TTL_MS);
      }
    }, intervalMs);

    return () => {
      clearInterval(timer);
      handle.leave();
    };
  });

  return {
    online,
    heartbeat: (data: T, ttlMs = DEFAULT_PRESENCE_TTL_MS) => handle.heartbeat(data, ttlMs),
    leave: () => handle.leave(),
  };
};
