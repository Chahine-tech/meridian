import { useMemo, useSyncExternalStore } from "react";
import { Schema } from "effect";
import { useMeridianClient } from "../context.js";

/**
 * Subscribe to an ORSet CRDT (add-wins observed-remove set).
 *
 * ```tsx
 * // Untyped
 * const { elements, add, remove } = useORSet("or:tasks");
 *
 * // Typed with schema validation — define schema outside the component
 * // so its reference is stable across renders.
 * const Task = Schema.Struct({ id: Schema.String, title: Schema.String });
 * function MyComponent() {
 *   const { elements, add, remove } = useORSet("or:tasks", Task);
 * }
 * ```
 */
export function useORSet<T = unknown>(
  crdtId: string,
  schema?: Schema.Schema<T>,
): {
  elements: T[];
  add: (value: T) => void;
  remove: (value: T) => void;
} {
  const client = useMeridianClient();
  const handle = useMemo(
    () => client.orset<T>(crdtId, schema),
    [client, crdtId, schema],
  );

  const elements = useSyncExternalStore(
    (notify) => handle.onChange(() => notify()),
    () => handle.elements(),
    () => handle.elements(),
  );

  return {
    elements,
    add: (value: T) => handle.add(value),
    remove: (value: T) => handle.remove(value),
  };
}
