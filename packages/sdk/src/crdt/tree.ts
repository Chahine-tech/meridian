import { Chunk, Effect, Stream } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { TreeDelta, TreeNodeValue } from "../sync/delta.js";
import type { ConflictEvent, LwwOverwriteEvent, MoveDiscardedEvent, MoveReorderedEvent } from "../conflict/types.js";
import type { DiscardedMove } from "../sync/delta.js";
import { type CrdtValidator, runValidator } from "../validation/index.js";

/**
 * Low-level handle for a TreeCRDT — a convergent hierarchical tree.
 *
 * Obtained via `MeridianClient.tree()`. Supports add, move, update, and delete
 * operations on tree nodes. Concurrent moves use Kleppmann et al. (2021)
 * move semantics — cycles are prevented, all replicas converge.
 *
 * Node IDs are returned by `addNode()` as opaque strings of the form
 * "wall_ms:logical:node_id" matching the Rust HLC serialization.
 */
export interface TreeNodeMeta {
  parentId: string | null;
  position: string;
  /** HLC string "wall_ms:logical:node_id" of the last UpdateNode op on this node. */
  updatedAt: string;
  value: string;
}

/** Maximum number of conflict events retained in the in-memory log. */
const CONFLICT_LOG_CAP = 200;

interface FlatTreeNode {
  id: string;
  parentId: string | null;
  position: string;
  value: string;
  deleted: boolean;
  updatedAt: string;
}

export class TreeHandle {
  private roots: TreeNodeValue[] = [];
  /** Flat node store keyed by HLC string — source of truth for applying ops. */
  private readonly flatNodes = new Map<string, FlatTreeNode>();
  private readonly crdtId: string;
  private readonly clientId: number;
  private readonly transport: WsTransport;
  private readonly validator: CrdtValidator | undefined;
  private readonly listeners = new Set<(value: { roots: TreeNodeValue[] }) => void>();
  private readonly conflictListeners = new Set<(event: ConflictEvent) => void>();
  private readonly conflictHistory: ConflictEvent[] = [];
  private opCounter = 0;
  /** Tracks parent, position, updatedAt, and value per node for undo support. */
  private readonly nodeMetaMap = new Map<string, TreeNodeMeta>();

  constructor(opts: {
    crdtId: string;
    clientId: number;
    transport: WsTransport;
    validator?: CrdtValidator;
  }) {
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
    this.validator = opts.validator;
  }

  /** The CRDT key this handle is bound to. */
  get id(): string { return this.crdtId; }

  /** Returns the current tree value. */
  value(): { roots: TreeNodeValue[] } {
    return { roots: this.roots };
  }

  /**
   * Registers a listener called whenever the tree changes.
   *
   * @returns An unsubscribe function.
   */
  onChange(listener: (value: { roots: TreeNodeValue[] }) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /** Returns an Effect Stream that emits the tree on every change. */
  stream(): Stream.Stream<{ roots: TreeNodeValue[] }, never, never> {
    return Stream.async<{ roots: TreeNodeValue[] }>((emit) => {
      const unsub = this.onChange((value) => { void emit(Effect.succeed(Chunk.of(value))); });
      return Effect.sync(unsub);
    });
  }

  /**
   * Adds a new node to the tree.
   *
   * @param parentId  - ID of the parent node, or null for a root-level node.
   * @param position  - Fractional index string for sibling ordering (e.g. "a0", "b0").
   * @param value     - String content of the node.
   * @param ttlMs     - Optional TTL.
   * @returns The new node's ID as a string.
   */
  addNode(parentId: string | null, position: string, value: string, ttlMs?: number): string {
    runValidator(this.validator, value);
    const wallMs = Date.now();
    const logical = this.opCounter++;
    const id = { wall_ms: wallMs, logical, node_id: this.clientId };
    const idStr = `${wallMs}:${logical}:${this.clientId}`;

    // Optimistic update — reflects immediately in value() and UndoManager.
    this.flatNodes.set(idStr, { id: idStr, parentId, position, value, deleted: false, updatedAt: idStr });
    this.nodeMetaMap.set(idStr, { parentId, position, updatedAt: idStr, value });
    this.roots = this.buildRoots();
    this.emit();

    const op = encode({
      Tree: {
        AddNode: {
          id,
          parent_id: parentId !== null ? this.parseHlc(parentId) : null,
          position,
          value,
        },
      },
    });

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: op,
        ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
      },
    });

    return idStr;
  }

  /**
   * Moves a node to a new parent and/or position.
   *
   * @param nodeId      - ID of the node to move.
   * @param newParentId - New parent node ID, or null for root level.
   * @param newPosition - New fractional index position.
   * @param ttlMs       - Optional TTL.
   */
  moveNode(nodeId: string, newParentId: string | null, newPosition: string, ttlMs?: number): void {
    if (this.parseHlc(nodeId) === null) return;
    if (newParentId !== null && this.parseHlc(newParentId) === null) return;
    const wallMs = Date.now();
    const logical = this.opCounter++;
    const opId = { wall_ms: wallMs, logical, node_id: this.clientId };

    // Optimistic update.
    const existing = this.nodeMetaMap.get(nodeId);
    if (existing !== undefined) {
      this.nodeMetaMap.set(nodeId, { ...existing, parentId: newParentId, position: newPosition });
    }
    const flatNode = this.flatNodes.get(nodeId);
    if (flatNode) {
      flatNode.parentId = newParentId;
      flatNode.position = newPosition;
      this.roots = this.buildRoots();
      this.emit();
    }

    const op = encode({
      Tree: {
        MoveNode: {
          op_id: opId,
          node_id: this.parseHlc(nodeId),
          new_parent_id: newParentId !== null ? this.parseHlc(newParentId) : null,
          new_position: newPosition,
        },
      },
    });

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: op,
        ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
      },
    });
  }

  /**
   * Updates the value of an existing node (LWW — last write wins).
   *
   * @param nodeId  - ID of the node to update.
   * @param value   - New string value.
   * @param ttlMs   - Optional TTL.
   */
  updateNode(nodeId: string, value: string, ttlMs?: number): void {
    if (this.parseHlc(nodeId) === null) return;
    runValidator(this.validator, value);
    const wallMs = Date.now();
    const logical = this.opCounter++;
    const updatedAt = { wall_ms: wallMs, logical, node_id: this.clientId };
    const updatedAtStr = `${wallMs}:${logical}:${this.clientId}`;

    // Optimistic update.
    const existing = this.nodeMetaMap.get(nodeId);
    if (existing !== undefined) {
      this.nodeMetaMap.set(nodeId, { ...existing, value, updatedAt: updatedAtStr });
    }
    const flatNode = this.flatNodes.get(nodeId);
    if (flatNode) {
      flatNode.value = value;
      flatNode.updatedAt = updatedAtStr;
      this.roots = this.buildRoots();
      this.emit();
    }

    const op = encode({
      Tree: {
        UpdateNode: {
          id: this.parseHlc(nodeId),
          value,
          updated_at: updatedAt,
        },
      },
    });

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: op,
        ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
      },
    });
  }

  /**
   * Tombstone-deletes a node. Children are preserved in the tree structure
   * but become invisible until the node is undeleted via a concurrent move.
   *
   * @param nodeId - ID of the node to delete.
   * @param ttlMs  - Optional TTL.
   */
  deleteNode(nodeId: string, ttlMs?: number): void {
    if (this.parseHlc(nodeId) === null) return;
    // Optimistic tombstone.
    const flatNode = this.flatNodes.get(nodeId);
    if (flatNode) {
      flatNode.deleted = true;
      this.roots = this.buildRoots();
      this.emit();
    }
    const op = encode({
      Tree: {
        DeleteNode: {
          id: this.parseHlc(nodeId),
        },
      },
    });

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: op,
        ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
      },
    });
  }

  /**
   * Returns the last-known metadata for a node (parent, position, value, updatedAt).
   * Used by UndoManager to capture pre-op state before mutations.
   */
  getNodeMeta(nodeId: string): TreeNodeMeta | undefined {
    return this.nodeMetaMap.get(nodeId);
  }

  /**
   * Registers a listener called whenever a conflict is detected in an incoming delta.
   * Conflicts include concurrent move resolutions, LWW overwrites, and discarded moves.
   *
   * @returns An unsubscribe function.
   */
  onConflict(listener: (event: ConflictEvent) => void): () => void {
    this.conflictListeners.add(listener);
    return () => { this.conflictListeners.delete(listener); };
  }

  /**
   * Returns the last up-to-200 conflict events in chronological order.
   * Useful for rendering an audit log or "track changes" UI.
   */
  conflictLog(): readonly ConflictEvent[] {
    return this.conflictHistory;
  }

  /**
   * Restore state from a persisted snapshot (hierarchical roots).
   * Rebuilds the flat node store from the snapshot and emits.
   */
  restoreSnapshot(roots: TreeNodeValue[]): void {
    this.flatNodes.clear();
    const addNodes = (nodes: TreeNodeValue[], parentId: string | null): void => {
      for (const node of nodes) {
        this.flatNodes.set(node.id, {
          id: node.id, parentId, position: node.position, value: node.value,
          deleted: false, updatedAt: node.id,
        });
        addNodes(node.children, node.id);
      }
    };
    addNodes(roots, null);
    this.roots = this.buildRoots();
    this.syncMetaMapFromFlatNodes();
    this.emit();
  }

  /**
   * Apply a delta received from the server.
   *
   * Applies each TreeOp (AddNode / MoveNode / UpdateNode / DeleteNode) to the
   * flat node store, then rebuilds the hierarchical roots view and emits.
   */
  applyDelta(delta: TreeDelta): void {
    this.emitDiscardedMoveConflicts(delta.discarded_moves ?? []);

    for (const op of delta.ops) {
      if ("AddNode" in op) {
        const { id, parent_id, position, value } = op.AddNode;
        const idStr = this.hlcToStr(id);
        if (!this.flatNodes.has(idStr)) {
          const parentStr = parent_id !== null ? this.hlcToStr(parent_id) : null;
          this.flatNodes.set(idStr, {
            id: idStr, parentId: parentStr, position, value, deleted: false, updatedAt: idStr,
          });
        }
      } else if ("MoveNode" in op) {
        const { node_id, new_parent_id, new_position } = op.MoveNode;
        const idStr = this.hlcToStr(node_id);
        const node = this.flatNodes.get(idStr);
        if (node) {
          const prevPos = node.position;
          node.parentId = new_parent_id !== null ? this.hlcToStr(new_parent_id) : null;
          node.position = new_position;
          // Conflict detection for concurrent move.
          const meta = this.nodeMetaMap.get(idStr);
          if (meta && (meta.parentId !== node.parentId || meta.position !== prevPos)) {
            this.emitConflict({
              kind: "move_reordered",
              nodeId: idStr,
              localParentId: meta.parentId,
              localPosition: prevPos,
              serverParentId: node.parentId,
              serverPosition: new_position,
              at: Date.now(),
            } satisfies MoveReorderedEvent);
          }
        }
      } else if ("UpdateNode" in op) {
        const { id, value, updated_at } = op.UpdateNode;
        const idStr = this.hlcToStr(id);
        const node = this.flatNodes.get(idStr);
        if (node) {
          const newUpdatedAt = this.hlcToStr(updated_at);
          // LWW: only accept if strictly newer.
          if (newUpdatedAt > node.updatedAt) {
            const meta = this.nodeMetaMap.get(idStr);
            if (meta && meta.value !== value) {
              this.emitConflict({
                kind: "lww_overwrite",
                nodeId: idStr,
                discardedValue: meta.value,
                winnerValue: value,
                at: Date.now(),
              } satisfies LwwOverwriteEvent);
            }
            node.value = value;
            node.updatedAt = newUpdatedAt;
          }
        }
      } else if ("DeleteNode" in op) {
        const { id } = op.DeleteNode;
        const idStr = this.hlcToStr(id);
        const node = this.flatNodes.get(idStr);
        if (node) node.deleted = true;
      }
    }

    this.roots = this.buildRoots();
    this.syncMetaMapFromFlatNodes();
    this.emit();
  }

  private hlcToStr(hlc: { wall_ms: number | bigint; logical: number; node_id: number | bigint }): string {
    return `${Number(hlc.wall_ms)}:${hlc.logical}:${Number(hlc.node_id)}`;
  }

  /** Build the hierarchical roots view from the flat node store. */
  private buildRoots(): TreeNodeValue[] {
    const buildChildren = (parentId: string | null): TreeNodeValue[] => {
      const children: FlatTreeNode[] = [];
      for (const node of this.flatNodes.values()) {
        if (!node.deleted && node.parentId === parentId) children.push(node);
      }
      children.sort((a, b) =>
        a.position < b.position ? -1 : a.position > b.position ? 1 :
        b.id < a.id ? -1 : b.id > a.id ? 1 : 0,
      );
      return children.map(n => ({
        id: n.id,
        value: n.value,
        position: n.position,
        children: buildChildren(n.id),
      }));
    };
    return buildChildren(null);
  }

  /** Sync nodeMetaMap from flatNodes after applying server ops. */
  private syncMetaMapFromFlatNodes(): void {
    for (const node of this.flatNodes.values()) {
      if (!node.deleted) {
        this.nodeMetaMap.set(node.id, {
          parentId: node.parentId,
          position: node.position,
          updatedAt: node.updatedAt,
          value: node.value,
        });
      }
    }
  }

  /** Emit a conflict event to all listeners and append to history. */
  private emitConflict(event: ConflictEvent): void {
    if (this.conflictHistory.length >= CONFLICT_LOG_CAP) {
      this.conflictHistory.shift();
    }
    this.conflictHistory.push(event);
    for (const listener of this.conflictListeners) listener(event);
  }

  /** Emit move_discarded conflict events for server-rejected cycle moves. */
  private emitDiscardedMoveConflicts(discardedMoves: DiscardedMove[]): void {
    const now = Date.now();
    for (const dm of discardedMoves) {
      this.emitConflict({
        kind: "move_discarded",
        nodeId: this.hlcToStr(dm.node_id),
        attemptedParentId: dm.attempted_parent_id !== null ? this.hlcToStr(dm.attempted_parent_id) : null,
        attemptedPosition: dm.attempted_position,
        actualParentId: dm.actual_parent_id !== null ? this.hlcToStr(dm.actual_parent_id) : null,
        actualPosition: dm.actual_position,
        at: now,
      } satisfies MoveDiscardedEvent);
    }
  }

  private emit(): void {
    const v = { roots: this.roots };
    for (const listener of this.listeners) listener(v);
  }

  /** Parse an HLC string "wall_ms:logical:node_id" back to a Rust-compatible object. */
  private parseHlc(id: string): { wall_ms: number; logical: number; node_id: number } | null {
    const parts = id.split(":");
    if (parts.length !== 3) return null;
    return {
      wall_ms: Number(parts[0]),
      logical: Number(parts[1]),
      node_id: Number(parts[2]),
    };
  }
}
