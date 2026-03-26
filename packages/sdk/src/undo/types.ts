/**
 * Undo/redo entry types for CRDT-aware per-client history.
 *
 * Each entry stores enough information to produce the inverse operation.
 * RGA Delete is intentionally not recorded — tombstones are final and
 * the content is not recoverable (same semantics as ProseMirror/Yjs).
 */

export interface RgaInsertEntry {
  readonly kind: "rga_insert";
  readonly crdtId: string;
  /** HLC string "wall_ms:logical:node_id" of the inserted node. Undo = deleteById(nodeId). */
  readonly nodeId: string;
  /** Original visible position — used for redo re-insert. */
  readonly pos: number;
  /** The character that was inserted. */
  readonly content: string;
}

export interface TreeAddEntry {
  readonly kind: "tree_add";
  readonly crdtId: string;
  /** Node ID returned by addNode(). Undo = deleteNode(nodeId). */
  readonly nodeId: string;
  /** Stored for redo: re-add with same parent/position/value. */
  readonly parentId: string | null;
  readonly position: string;
  readonly value: string;
}

export interface TreeDeleteEntry {
  readonly kind: "tree_delete";
  readonly crdtId: string;
  readonly nodeId: string;
  /** State captured before the delete — used to reconstruct addNode for undo. */
  readonly parentId: string | null;
  readonly position: string;
  readonly value: string;
  readonly updatedAt: string;
}

export interface TreeMoveEntry {
  readonly kind: "tree_move";
  readonly crdtId: string;
  readonly nodeId: string;
  readonly oldParentId: string | null;
  readonly oldPosition: string;
  /** Stored for redo. */
  readonly newParentId: string | null;
  readonly newPosition: string;
}

export interface TreeUpdateEntry {
  readonly kind: "tree_update";
  readonly crdtId: string;
  readonly nodeId: string;
  /** Captured before the update — used for undo. */
  readonly previousValue: string;
  readonly previousUpdatedAt: string;
  /** Stored for redo. */
  readonly newValue: string;
}

export type UndoEntry =
  | RgaInsertEntry
  | TreeAddEntry
  | TreeDeleteEntry
  | TreeMoveEntry
  | TreeUpdateEntry;

/** A batch groups one or more entries into a single logical undo step. */
export interface UndoBatch {
  readonly entries: readonly UndoEntry[];
}
