import { useMemo, useSyncExternalStore } from "react";
import { Schema } from "effect";
import { useMeridianClient } from "../context.js";

/**
 * Subscribe to a LwwRegister CRDT (last-write-wins single value).
 *
 * ```tsx
 * // Untyped
 * const { value, set } = useLwwRegister("lw:doc-title");
 *
 * // Typed — define schema outside the component for a stable reference.
 * const TitleSchema = Schema.String;
 * function MyComponent() {
 *   const { value, set } = useLwwRegister("lw:doc-title", TitleSchema);
 * }
 * ```
 */
export function useLwwRegister<T = unknown>(
  crdtId: string,
  schema?: Schema.Schema<T>,
): {
  value: T | null;
  set: (value: T) => void;
} {
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
    set: (v: T) => handle.set(v),
  };
}
