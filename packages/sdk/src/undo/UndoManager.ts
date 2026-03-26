import { RGAHandle } from "../crdt/rga.js";
import { TreeHandle } from "../crdt/tree.js";
import type {
  UndoBatch,
  UndoEntry,
  RgaInsertEntry,
  TreeAddEntry,
  TreeDeleteEntry,
  TreeMoveEntry,
  TreeUpdateEntry,
} from "./types.js";

const MAX_STACK_DEPTH = 100;
const RGA_DEBOUNCE_MS = 250;

type SupportedHandle = RGAHandle | TreeHandle;

/**
 * CRDT-aware per-client undo/redo manager.
 *
 * Wraps an RGAHandle or TreeHandle. Intercept mutating methods via the manager
 * instead of the handle directly to record undo history.
 *
 * Rules:
 * - Undo sends a new valid CrdtOp (never a state rollback).
 * - Only the local client's ops are tracked — remote ops are unaffected.
 * - RGA Delete is NOT recorded (tombstones are final).
 * - Stack is capped at 100 batches; oldest entries are dropped silently.
 *
 * @example
 * ```ts
 * const manager = new UndoManager(client.rga("doc"));
 * manager.insert(0, "hello");
 * manager.undo(); // sends 5 × RgaOp::Delete
 * ```
 */
export class UndoManager<H extends SupportedHandle> {
  private readonly handle: H;
  private undoStack: UndoBatch[] = [];
  private redoStack: UndoBatch[] = [];
  private readonly stackListeners = new Set<() => void>();

  // Batch accumulation
  private openBatch: UndoEntry[] | null = null;
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(handle: H) {
    this.handle = handle;
  }

  // ─── Stack state ────────────────────────────────────────────────────────────

  get canUndo(): boolean {
    return this.undoStack.length > 0;
  }

  get canRedo(): boolean {
    return this.redoStack.length > 0;
  }

  /**
   * Subscribe to stack change notifications (fired after any undo/redo/record).
   * @returns Unsubscribe function.
   */
  onStackChange(listener: () => void): () => void {
    this.stackListeners.add(listener);
    return () => { this.stackListeners.delete(listener); };
  }

  // ─── Manual batching ────────────────────────────────────────────────────────

  /** Begin accumulating entries into a single undo step. */
  startBatch(): void {
    if (this.openBatch === null) {
      this.openBatch = [];
    }
  }

  /** Commit the open batch as one entry on the undo stack. No-op if no batch is open. */
  commitBatch(): void {
    if (this.openBatch !== null && this.openBatch.length > 0) {
      this.pushUndo({ entries: this.openBatch });
    }
    this.openBatch = null;
  }

  /** Discard the open batch without recording anything. */
  cancelBatch(): void {
    this.openBatch = null;
  }

  // ─── Undo / Redo ────────────────────────────────────────────────────────────

  undo(): void {
    const batch = this.undoStack.pop();
    if (batch === undefined) return;
    const inverseEntries: UndoEntry[] = [];

    // Apply inverse ops in reverse entry order.
    for (let i = batch.entries.length - 1; i >= 0; i--) {
      const entry = batch.entries[i]!;
      const inverse = this.applyInverse(entry);
      if (inverse !== null) inverseEntries.unshift(inverse);
    }

    if (inverseEntries.length > 0) {
      this.redoStack.push({ entries: inverseEntries });
    }
    this.notifyStackChange();
  }

  redo(): void {
    const batch = this.redoStack.pop();
    if (batch === undefined) return;
    const inverseEntries: UndoEntry[] = [];

    for (let i = batch.entries.length - 1; i >= 0; i--) {
      const entry = batch.entries[i]!;
      const inverse = this.applyInverse(entry);
      if (inverse !== null) inverseEntries.unshift(inverse);
    }

    if (inverseEntries.length > 0) {
      this.undoStack.push({ entries: inverseEntries });
      if (this.undoStack.length > MAX_STACK_DEPTH) {
        this.undoStack.shift();
      }
    }
    this.notifyStackChange();
  }

  // ─── RGA handle proxy methods ────────────────────────────────────────────────

  /**
   * Insert text at visible position. Records undo entry with HLC node IDs.
   * Uses 250ms debounce — consecutive inserts within the window form one batch.
   */
  insert(pos: number, text: string, ttlMs?: number): string[] {
    const handle = this.asRGA();
    const nodeIds = handle.insert(pos, text, ttlMs);

    for (let i = 0; i < nodeIds.length; i++) {
      const entry: RgaInsertEntry = {
        kind: "rga_insert",
        crdtId: handle.id,
        nodeId: nodeIds[i]!,
        pos: pos + i,
        content: text[i]!,
      };
      this.recordWithDebounce(entry);
    }

    this.clearRedo();
    return nodeIds;
  }

  /**
   * Delete characters by position. NOT recorded — RGA delete is non-undoable
   * (tombstones are final; content is irrecoverable).
   * clearRedo() is intentional: a destructive untracked op should invalidate
   * any pending redo history, matching ProseMirror/Yjs behaviour.
   */
  delete(pos: number, length: number, ttlMs?: number): void {
    this.asRGA().delete(pos, length, ttlMs);
    this.clearRedo();
  }

  // ─── Tree handle proxy methods ───────────────────────────────────────────────

  addNode(parentId: string | null, position: string, value: string, ttlMs?: number): string {
    const handle = this.asTree();
    const nodeId = handle.addNode(parentId, position, value, ttlMs);
    const entry: TreeAddEntry = {
      kind: "tree_add",
      crdtId: this.getCrdtId(),
      nodeId,
      parentId,
      position,
      value,
    };
    this.record(entry);
    this.clearRedo();
    return nodeId;
  }

  moveNode(nodeId: string, newParentId: string | null, newPosition: string, ttlMs?: number): void {
    const handle = this.asTree();
    const meta = handle.getNodeMeta(nodeId);
    handle.moveNode(nodeId, newParentId, newPosition, ttlMs);
    const entry: TreeMoveEntry = {
      kind: "tree_move",
      crdtId: this.getCrdtId(),
      nodeId,
      oldParentId: meta?.parentId ?? null,
      oldPosition: meta?.position ?? "a0",
      newParentId,
      newPosition,
    };
    this.record(entry);
    this.clearRedo();
  }

  updateNode(nodeId: string, value: string, ttlMs?: number): void {
    const handle = this.asTree();
    const meta = handle.getNodeMeta(nodeId);
    handle.updateNode(nodeId, value, ttlMs);
    const entry: TreeUpdateEntry = {
      kind: "tree_update",
      crdtId: this.getCrdtId(),
      nodeId,
      previousValue: meta?.value ?? "",
      previousUpdatedAt: meta?.updatedAt ?? nodeId,
      newValue: value,
    };
    this.record(entry);
    this.clearRedo();
  }

  deleteNode(nodeId: string, ttlMs?: number): void {
    const handle = this.asTree();
    const meta = handle.getNodeMeta(nodeId);
    handle.deleteNode(nodeId, ttlMs);
    const entry: TreeDeleteEntry = {
      kind: "tree_delete",
      crdtId: this.getCrdtId(),
      nodeId,
      parentId: meta?.parentId ?? null,
      position: meta?.position ?? "a0",
      value: meta?.value ?? "",
      updatedAt: meta?.updatedAt ?? nodeId,
    };
    this.record(entry);
    this.clearRedo();
  }

  // ─── Internal ────────────────────────────────────────────────────────────────

  private applyInverse(entry: UndoEntry): UndoEntry | null {
    switch (entry.kind) {
      case "rga_insert": {
        this.asRGA().deleteById(entry.nodeId);
        // Inverse of an insert is a delete — not re-recordable as an undo entry.
        return null;
      }

      case "tree_add": {
        this.asTree().deleteNode(entry.nodeId);
        // Inverse: a delete entry (for redo — re-add the node).
        const inverse: TreeDeleteEntry = {
          kind: "tree_delete",
          crdtId: entry.crdtId,
          nodeId: entry.nodeId,
          parentId: entry.parentId,
          position: entry.position,
          value: entry.value,
          updatedAt: entry.nodeId,
        };
        return inverse;
      }

      case "tree_delete": {
        // Re-add with original state. Gets a new nodeId (tombstone is permanent).
        const newNodeId = this.asTree().addNode(
          entry.parentId,
          entry.position,
          entry.value,
        );
        const inverse: TreeAddEntry = {
          kind: "tree_add",
          crdtId: entry.crdtId,
          nodeId: newNodeId,
          parentId: entry.parentId,
          position: entry.position,
          value: entry.value,
        };
        return inverse;
      }

      case "tree_move": {
        this.asTree().moveNode(entry.nodeId, entry.oldParentId, entry.oldPosition);
        const inverse: TreeMoveEntry = {
          kind: "tree_move",
          crdtId: entry.crdtId,
          nodeId: entry.nodeId,
          oldParentId: entry.newParentId,
          oldPosition: entry.newPosition,
          newParentId: entry.oldParentId,
          newPosition: entry.oldPosition,
        };
        return inverse;
      }

      case "tree_update": {
        this.asTree().updateNode(entry.nodeId, entry.previousValue);
        const inverse: TreeUpdateEntry = {
          kind: "tree_update",
          crdtId: entry.crdtId,
          nodeId: entry.nodeId,
          previousValue: entry.newValue,
          previousUpdatedAt: entry.previousUpdatedAt,
          newValue: entry.previousValue,
        };
        return inverse;
      }
    }
  }

  private record(entry: UndoEntry): void {
    if (this.openBatch !== null) {
      this.openBatch.push(entry);
    } else {
      this.pushUndo({ entries: [entry] });
    }
  }

  /** Record with 250ms debounce — consecutive calls within window share one batch. */
  private recordWithDebounce(entry: UndoEntry): void {
    if (this.debounceTimer === null) {
      // Start a new debounce batch.
      this.openBatch = [entry];
    } else {
      // Extend the current batch.
      clearTimeout(this.debounceTimer);
      this.openBatch?.push(entry);
    }

    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = null;
      this.commitBatch();
    }, RGA_DEBOUNCE_MS);
  }

  private pushUndo(batch: UndoBatch): void {
    this.undoStack.push(batch);
    if (this.undoStack.length > MAX_STACK_DEPTH) {
      this.undoStack.shift();
    }
    this.notifyStackChange();
  }

  private clearRedo(): void {
    if (this.redoStack.length > 0) {
      this.redoStack = [];
      this.notifyStackChange();
    }
  }

  private notifyStackChange(): void {
    for (const listener of this.stackListeners) listener();
  }

  private asRGA(): RGAHandle {
    if (!(this.handle instanceof RGAHandle)) {
      throw new Error("UndoManager: handle is not an RGAHandle");
    }
    return this.handle;
  }

  private asTree(): TreeHandle {
    if (!(this.handle instanceof TreeHandle)) {
      throw new Error("UndoManager: handle is not a TreeHandle");
    }
    return this.handle;
  }

  private getCrdtId(): string {
    return this.handle.id;
  }
}
