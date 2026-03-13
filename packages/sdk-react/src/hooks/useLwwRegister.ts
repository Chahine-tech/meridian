import { useMemo, useSyncExternalStore } from "react";
import type { Schema } from "effect";
import { useMeridianClient } from "../context.js";

interface LwwRegisterHandle<T> {
  value: T | null;
  set: (value: T) => void;
}

/**
 * Subscribe to a Last-Write-Wins register (LWW-Register) CRDT.
 *
 * The register holds a single value. When multiple clients write concurrently
 * the write with the highest hybrid-logical-clock timestamp wins. The resolved
 * value is eventually consistent across all clients.
 *
 * Pass an `effect/Schema` to validate and decode the value from the wire format.
 *
 * @param crdtId - Unique identifier for the CRDT within the current namespace.
 * @param schema - Optional Effect schema used to decode the stored value.
 * @returns An object containing the current `value` (or `null` if unset) and a `set` function.
 *
 * @example
 * ```tsx
 * import { useLwwRegister } from 'meridian-react';
 * import { Schema } from 'effect';
 *
 * const ThemeSchema = Schema.Literal('light', 'dark');
 *
 * function ThemePicker() {
 *   const { value, set } = useLwwRegister('ui-theme', ThemeSchema);
 *
 *   return (
 *     <button onClick={() => set(value === 'dark' ? 'light' : 'dark')}>
 *       Current theme: {value ?? 'unset'}
 *     </button>
 *   );
 * }
 * ```
 */
export const useLwwRegister = <T = unknown>(
  crdtId: string,
  schema?: Schema.Schema<T>,
): LwwRegisterHandle<T> => {
  const client = useMeridianClient();
  const handle = useMemo(
    () => client.lwwregister<T>(crdtId, schema),
    [client, crdtId, schema],
  );

  const value = useSyncExternalStore(
    (notify) => handle.onChange(() => notify()),
    () => handle.value(),
    () => handle.value(),
  );

  return {
    value,
    set: (newValue: T) => handle.set(newValue),
  };
};
