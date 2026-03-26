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

export class TreeHandle {
  private roots: TreeNodeValue[] = [];
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

    // Optimistic meta update — allows UndoManager to read state immediately.
    this.nodeMetaMap.set(idStr, {
      parentId,
      position,
      updatedAt: idStr,
      value,
    });

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
    const wallMs = Date.now();
    const logical = this.opCounter++;
    const opId = { wall_ms: wallMs, logical, node_id: this.clientId };

    // Optimistic meta update.
    const existing = this.nodeMetaMap.get(nodeId);
    if (existing !== undefined) {
      this.nodeMetaMap.set(nodeId, { ...existing, parentId: newParentId, position: newPosition });
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
    runValidator(this.validator, value);
    const wallMs = Date.now();
    const logical = this.opCounter++;
    const updatedAt = { wall_ms: wallMs, logical, node_id: this.clientId };
    const updatedAtStr = `${wallMs}:${logical}:${this.clientId}`;

    // Optimistic meta update.
    const existing = this.nodeMetaMap.get(nodeId);
    if (existing !== undefined) {
      this.nodeMetaMap.set(nodeId, { ...existing, value, updatedAt: updatedAtStr });
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

  /** Apply a delta received from the server. Replaces local state with authoritative tree. */
  applyDelta(delta: TreeDelta): void {
    this.detectConflicts(delta.roots, null, delta.discarded_moves ?? []);
    this.roots = delta.roots;
    this.rebuildMetaMap(delta.roots, null);
    this.emit();
  }

  /** Emit a conflict event to all listeners and append to history. */
  private emitConflict(event: ConflictEvent): void {
    if (this.conflictHistory.length >= CONFLICT_LOG_CAP) {
      this.conflictHistory.shift();
    }
    this.conflictHistory.push(event);
    for (const listener of this.conflictListeners) listener(event);
  }

  /**
   * Walks the incoming delta tree and compares each node's server-authoritative
   * parent/position/value against what the local client last recorded.
   * Also processes discarded_moves from the server to emit move_discarded events.
   * Emits conflict events for discrepancies caused by concurrent op resolution.
   */
  private detectConflicts(
    nodes: TreeNodeValue[],
    serverParentId: string | null,
    discardedMoves: DiscardedMove[],
  ): void {
    const now = Date.now();

    // Emit move_discarded events for server-rejected cycle moves.
    for (const dm of discardedMoves) {
      const hlcToStr = (hlc: { wall_ms: number; logical: number; node_id: number }): string =>
        `${hlc.wall_ms}:${hlc.logical}:${hlc.node_id}`;
      const event: MoveDiscardedEvent = {
        kind: "move_discarded",
        nodeId: hlcToStr(dm.node_id),
        attemptedParentId: dm.attempted_parent_id !== null ? hlcToStr(dm.attempted_parent_id) : null,
        attemptedPosition: dm.attempted_position,
        actualParentId: dm.actual_parent_id !== null ? hlcToStr(dm.actual_parent_id) : null,
        actualPosition: dm.actual_position,
        at: now,
      };
      this.emitConflict(event);
    }

    for (const node of nodes) {
      const local = this.nodeMetaMap.get(node.id);
      if (local !== undefined) {
        // Detect move reordering: server placed the node somewhere different
        // from what the local optimistic update recorded.
        if (local.parentId !== serverParentId || local.position !== node.position) {
          const event: MoveReorderedEvent = {
            kind: "move_reordered",
            nodeId: node.id,
            localParentId: local.parentId,
            localPosition: local.position,
            serverParentId,
            serverPosition: node.position,
            at: now,
          };
          this.emitConflict(event);
        }

        // Detect LWW overwrite: server has a different value than local recorded.
        if (local.value !== node.value) {
          const event: LwwOverwriteEvent = {
            kind: "lww_overwrite",
            nodeId: node.id,
            discardedValue: local.value,
            winnerValue: node.value,
            at: now,
          };
          this.emitConflict(event);
        }
      }
      // Recurse into children — serverParentId for children is this node's id.
      // discardedMoves are top-level only (already processed above).
      this.detectConflicts(node.children, node.id, []);
    }
  }

  /** Rebuilds nodeMetaMap from a full tree snapshot (called on each server delta). */
  private rebuildMetaMap(nodes: TreeNodeValue[], parentId: string | null): void {
    for (const node of nodes) {
      const existing = this.nodeMetaMap.get(node.id);
      this.nodeMetaMap.set(node.id, {
        parentId,
        position: node.position,
        updatedAt: existing?.updatedAt ?? node.id,
        value: node.value,
      });
      this.rebuildMetaMap(node.children, node.id);
    }
  }

  private emit(): void {
    const v = { roots: this.roots };
    for (const listener of this.listeners) listener(v);
  }

  /** Parse an HLC string "wall_ms:logical:node_id" back to a Rust-compatible object. */
  private parseHlc(id: string): { wall_ms: number; logical: number; node_id: number } {
    const parts = id.split(":");
    return {
      wall_ms: Number(parts[0]),
      logical: Number(parts[1]),
      node_id: Number(parts[2]),
    };
  }
}
