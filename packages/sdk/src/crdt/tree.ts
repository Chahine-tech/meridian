import { Chunk, Effect, Stream } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { TreeDelta, TreeNodeValue } from "../sync/delta.js";

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
export class TreeHandle {
  private roots: TreeNodeValue[] = [];
  private readonly crdtId: string;
  private readonly clientId: number;
  private readonly transport: WsTransport;
  private readonly listeners = new Set<(value: { roots: TreeNodeValue[] }) => void>();
  private opCounter = 0;

  constructor(opts: {
    crdtId: string;
    clientId: number;
    transport: WsTransport;
  }) {
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
  }

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
    const wallMs = Date.now();
    const logical = this.opCounter++;
    const id = { wall_ms: wallMs, logical, node_id: this.clientId };
    const idStr = `${wallMs}:${logical}:${this.clientId}`;

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
    const wallMs = Date.now();
    const logical = this.opCounter++;
    const updatedAt = { wall_ms: wallMs, logical, node_id: this.clientId };

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

  /** Apply a delta received from the server. Replaces local state with authoritative tree. */
  applyDelta(delta: TreeDelta): void {
    this.roots = delta.roots;
    this.emit();
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
