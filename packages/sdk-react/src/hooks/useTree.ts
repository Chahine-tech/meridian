import { useMemo, useSyncExternalStore } from "react";
import { useMeridianClient } from "../context.js";
import type { TreeNodeValue } from "meridian-sdk";

interface TreeHandle {
  roots: TreeNodeValue[];
  addNode: (parentId: string | null, position: string, value: string, ttlMs?: number) => string;
  moveNode: (nodeId: string, newParentId: string | null, newPosition: string, ttlMs?: number) => void;
  updateNode: (nodeId: string, value: string, ttlMs?: number) => void;
  deleteNode: (nodeId: string, ttlMs?: number) => void;
}

/**
 * Subscribe to a TreeCRDT for collaborative hierarchical data (outlines, document trees, mind maps).
 *
 * Concurrent moves use Kleppmann (2021) semantics — cycles are prevented and all
 * replicas converge to the same tree regardless of operation order.
 *
 * @param crdtId - Unique identifier for the CRDT within the current namespace.
 * @returns An object containing the current `roots` tree and mutation functions.
 *
 * @example
 * ```tsx
 * import { useTree } from 'meridian-react';
 *
 * function Outline() {
 *   const { roots, addNode, deleteNode } = useTree('doc:outline');
 *
 *   return (
 *     <ul>
 *       {roots.map(node => (
 *         <li key={node.id}>
 *           {node.value}
 *           <button onClick={() => addNode(node.id, 'z0', 'New child')}>+</button>
 *           <button onClick={() => deleteNode(node.id)}>×</button>
 *         </li>
 *       ))}
 *       <button onClick={() => addNode(null, 'a0', 'New root')}>Add root</button>
 *     </ul>
 *   );
 * }
 * ```
 */
export const useTree = (crdtId: string): TreeHandle => {
  const client = useMeridianClient();
  const handle = useMemo(() => client.tree(crdtId), [client, crdtId]);

  const roots = useSyncExternalStore(
    (notify) => handle.onChange(() => notify()),
    () => handle.value().roots,
    () => handle.value().roots,
  );

  return {
    roots,
    addNode: (parentId, position, value, ttlMs) => handle.addNode(parentId, position, value, ttlMs),
    moveNode: (nodeId, newParentId, newPosition, ttlMs) => handle.moveNode(nodeId, newParentId, newPosition, ttlMs),
    updateNode: (nodeId, value, ttlMs) => handle.updateNode(nodeId, value, ttlMs),
    deleteNode: (nodeId, ttlMs) => handle.deleteNode(nodeId, ttlMs),
  };
};
