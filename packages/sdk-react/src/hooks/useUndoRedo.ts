import { useMemo, useSyncExternalStore } from "react";
import { UndoManager } from "meridian-sdk";
import type { RGAHandle, TreeHandle } from "meridian-sdk";

type SupportedHandle = RGAHandle | TreeHandle;

interface UseUndoRedoResult<H extends SupportedHandle> {
  /** True if there are ops that can be undone. */
  canUndo: boolean;
  /** True if there are ops that can be redone. */
  canRedo: boolean;
  /** Undo the last recorded op (or batch). */
  undo: () => void;
  /** Redo the last undone op (or batch). */
  redo: () => void;
  /** Begin a manual batch — multiple ops share one undo step. */
  startBatch: () => void;
  /** Commit the current manual batch. */
  commitBatch: () => void;
  /** The underlying UndoManager — use its proxy methods (insert, addNode, etc.) instead of the raw handle. */
  manager: UndoManager<H>;
}

/**
 * Adds CRDT-aware undo/redo to an RGAHandle or TreeHandle.
 *
 * Use the returned `manager` for all mutating operations instead of the raw handle.
 * The manager intercepts calls, records undo entries, and exposes `undo()`/`redo()`.
 *
 * RGA: `manager.insert()` uses a 250ms debounce — consecutive keystrokes form one undo step.
 * RGA Delete is NOT undoable (tombstone semantics).
 *
 * @param handle - An RGAHandle or TreeHandle obtained from MeridianClient.
 *
 * @example
 * ```tsx
 * const handle = useMemo(() => client.rga('doc'), [client]);
 * const { canUndo, canRedo, undo, redo, manager } = useUndoRedo(handle);
 *
 * // Use manager.insert() to record undo history
 * manager.insert(pos, text);
 *
 * return (
 *   <>
 *     <button disabled={!canUndo} onClick={undo}>Undo</button>
 *     <button disabled={!canRedo} onClick={redo}>Redo</button>
 *   </>
 * );
 * ```
 */
export const useUndoRedo = <H extends SupportedHandle>(handle: H): UseUndoRedoResult<H> => {
  const manager = useMemo(() => new UndoManager(handle), [handle]);

  const { canUndo, canRedo } = useSyncExternalStore(
    (notify) => manager.onStackChange(notify),
    () => ({ canUndo: manager.canUndo, canRedo: manager.canRedo }),
    () => ({ canUndo: false, canRedo: false }),
  );

  return {
    canUndo,
    canRedo,
    undo: () => manager.undo(),
    redo: () => manager.redo(),
    startBatch: () => manager.startBatch(),
    commitBatch: () => manager.commitBatch(),
    manager,
  };
};
