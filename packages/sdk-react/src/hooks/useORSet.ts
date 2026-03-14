import { useMemo, useSyncExternalStore } from "react";
import type { Schema } from "effect";
import { useMeridianClient } from "../context.js";

interface ORSetHandle<T> {
  elements: T[];
  add: (value: T) => void;
  remove: (value: T) => void;
}

/**
 * Subscribe to an Observed-Remove Set (OR-Set) CRDT.
 *
 * An OR-Set allows concurrent adds and removes across clients with no conflicts.
 * Each element addition is tagged so that a remove only removes what was
 * observed at that point in time, preserving concurrently added copies.
 *
 * Pass an `effect/Schema` to validate and decode elements from the wire format.
 *
 * @param crdtId - Unique identifier for the CRDT within the current namespace.
 * @param schema - Optional Effect schema used to decode each element.
 * @returns An object containing the current `elements` array, an `add` function, and a `remove` function.
 *
 * @example
 * ```tsx
 * import { useORSet } from 'meridian-react';
 * import { Schema } from 'effect';
 *
 * const TagSchema = Schema.String;
 *
 * function TagList() {
 *   const { elements, add, remove } = useORSet('article-tags', TagSchema);
 *
 *   return (
 *     <ul>
 *       {elements.map(tag => (
 *         <li key={tag}>
 *           {tag} <button onClick={() => remove(tag)}>x</button>
 *         </li>
 *       ))}
 *       <button onClick={() => add('new-tag')}>Add tag</button>
 *     </ul>
 *   );
 * }
 * ```
 */
export const useORSet = <T = unknown>(
  crdtId: string,
  schema?: Schema.Schema<T>,
): ORSetHandle<T> => {
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
};
