import { Chunk, Effect, Stream } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { RGADelta, RgaHlc } from "../sync/delta.js";
import { type CrdtValidator, runValidator } from "../validation/index.js";

/**
 * Low-level handle for an RGA (Replicated Growable Array) CRDT — collaborative text editing.
 *
 * Obtained via `MeridianClient.rga()`. The RGA converges concurrent edits from multiple
 * clients using Hybrid Logical Clock IDs and left-origin insertion ordering.
 *
 * Operations use visible-position indices (tombstones are invisible to callers).
 */
interface RgaNode {
  id: RgaHlc;
  originId: RgaHlc | null;
  content: string;
  deleted: boolean;
}

/** Compare two HLC values: returns negative if a < b, 0 if equal, positive if a > b. */
function hlcCompare(a: RgaHlc, b: RgaHlc): number {
  const aw = Number(a.wall_ms), bw = Number(b.wall_ms);
  if (aw !== bw) return aw - bw;
  if (a.logical !== b.logical) return a.logical - b.logical;
  return Number(a.node_id) - Number(b.node_id);
}

function hlcEqual(a: RgaHlc, b: RgaHlc): boolean {
  return Number(a.wall_ms) === Number(b.wall_ms) &&
    a.logical === b.logical &&
    Number(a.node_id) === Number(b.node_id);
}

function hlcEqualNullable(a: RgaHlc | null, b: RgaHlc | null): boolean {
  if (a === null && b === null) return true;
  if (a === null || b === null) return false;
  return hlcEqual(a, b);
}

export class RGAHandle {
  /** Full node list including tombstones — matches Rust Rga.nodes layout. */
  private nodes: RgaNode[] = [];
  private readonly crdtId: string;
  private readonly clientId: number;
  private readonly transport: WsTransport;
  private readonly validator: CrdtValidator | undefined;
  private readonly listeners = new Set<(value: string) => void>();

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

  /** Returns the current text content (visible characters only, tombstones excluded). */
  value(): string {
    return this.nodes.filter(n => !n.deleted).map(n => n.content).join("");
  }

  /**
   * Registers a listener called whenever the text changes.
   *
   * @returns An unsubscribe function.
   */
  onChange(listener: (value: string) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /** Returns an Effect Stream that emits the text on every change. */
  stream(): Stream.Stream<string, never, never> {
    return Stream.async<string>((emit) => {
      const unsub = this.onChange((value) => { void emit(Effect.succeed(Chunk.of(value))); });
      return Effect.sync(unsub);
    });
  }

  /**
   * Inserts `text` at visible position `pos` (0 = before all characters).
   * Characters are inserted one by one using RGA Insert ops.
   *
   * @param pos   - Visible character position (0-indexed).
   * @param text  - String to insert.
   * @param ttlMs - Optional TTL for the op.
   * @returns The HLC strings ("wall_ms:logical:node_id") of the inserted nodes,
   *          in insertion order. Used by UndoManager to record undo entries.
   */
  insert(pos: number, text: string, ttlMs?: number): string[] {
    if (text.length === 0) return [];
    runValidator(this.validator, text);

    const wallMs = Date.now();
    const nodeIds: string[] = [];

    // Find the origin node at visible position `pos - 1` in the node list.
    let visibleCount = 0;
    let originIdx = -1; // -1 = insert before all visible nodes
    for (let i = 0; i < this.nodes.length; i++) {
      const n = this.nodes[i];
      if (n && !n.deleted) {
        visibleCount++;
        if (visibleCount === pos) { originIdx = i; break; }
      }
    }
    const originNode = originIdx >= 0 ? this.nodes[originIdx] : undefined;
    const originId = originNode ? originNode.id : null;

    // Insert nodes optimistically into local node list and send ops.
    let prevOriginId = originId;
    let insertAfterIdx = originIdx; // index after which each char is inserted
    const chars = [...text];
    for (let i = 0; i < chars.length; i++) {
      const ch = chars[i] ?? "";
      const id: RgaHlc = { wall_ms: wallMs, logical: i, node_id: this.clientId };
      nodeIds.push(`${wallMs}:${i}:${this.clientId}`);
      const newNode: RgaNode = { id, originId: prevOriginId, content: ch, deleted: false };
      // Insert immediately after the previous char (or after origin if first char).
      this.nodes.splice(insertAfterIdx + 1, 0, newNode);
      insertAfterIdx++;
      prevOriginId = id;

      const op = encode({
        RGA: {
          Insert: {
            id,
            origin_id: newNode.originId,
            content: ch,
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
    this.emit();
    return nodeIds;
  }

  /**
   * Deletes the RGA node with the given HLC string ID directly.
   * Used by UndoManager to undo an insert by its exact node identity,
   * regardless of the current visible position.
   *
   * @param hlcString - HLC string "wall_ms:logical:node_id" returned by insert().
   * @param ttlMs     - Optional TTL.
   */
  deleteById(hlcString: string, ttlMs?: number): void {
    const parts = hlcString.split(":");
    if (parts.length !== 3) return;
    const id = {
      wall_ms: Number(parts[0]),
      logical: Number(parts[1]),
      node_id: Number(parts[2]),
    };
    const op = encode({ RGA: { Delete: { id } } });
    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: op,
        ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
      },
    });
  }

  /**
   * Deletes `length` visible characters starting at visible position `pos`.
   *
   * @param pos    - Visible character position (0-indexed).
   * @param length - Number of characters to delete.
   * @param ttlMs  - Optional TTL for the op.
   */
  delete(pos: number, length: number, ttlMs?: number): void {
    if (length <= 0) return;

    // Collect the node IDs of the `length` visible characters starting at `pos`.
    const toDelete: RgaHlc[] = [];
    let visibleCount = 0;
    for (const node of this.nodes) {
      if (!node.deleted) {
        if (visibleCount >= pos && visibleCount < pos + length) {
          toDelete.push(node.id);
        }
        visibleCount++;
        if (visibleCount >= pos + length) break;
      }
    }

    // Optimistic local tombstone.
    for (const id of toDelete) {
      const node = this.nodes.find(n => hlcEqual(n.id, id));
      if (node) node.deleted = true;
    }
    this.emit();

    // Send one Delete op per character.
    for (const id of toDelete) {
      const op = encode({ RGA: { Delete: { id } } });
      this.transport.send({
        Op: {
          crdt_id: this.crdtId,
          op_bytes: op,
          ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
        },
      });
    }
  }

  /**
   * Restore state from a persisted snapshot (text string).
   * Creates synthetic sequential nodes — used only for offline snapshot restore,
   * not for live delta application.
   */
  restoreSnapshot(text: string): void {
    this.nodes = [];
    const chars = [...text];
    for (let i = 0; i < chars.length; i++) {
      const ch = chars[i] ?? "";
      const id: RgaHlc = { wall_ms: 0, logical: i, node_id: 0 };
      const originId: RgaHlc | null = i === 0 ? null : { wall_ms: 0, logical: i - 1, node_id: 0 };
      this.nodes.push({ id, originId, content: ch, deleted: false });
    }
    this.emit();
  }

  /**
   * Apply a delta received from the server.
   *
   * Accepts either the op-based wire format `{ ops }` or the legacy text format
   * `{ text }` (used by tests and snapshot paths). The ops-based path applies
   * each RGA op using the convergence algorithm; the text path replaces state.
   */
  applyDelta(delta: RGADelta): void {
    if (delta.text !== undefined) {
      this.restoreSnapshot(delta.text);
      return;
    }
    let changed = false;
    for (const op of (delta.ops ?? [])) {
      if ("Insert" in op) {
        const { id, origin_id, content } = op.Insert;
        // Idempotent: skip if already present.
        if (this.nodes.some(n => hlcEqual(n.id, id))) continue;
        const insertPos = this.insertPosition(origin_id, id);
        this.nodes.splice(insertPos, 0, { id, originId: origin_id, content, deleted: false });
        changed = true;
      } else {
        const { id } = op.Delete;
        const node = this.nodes.find(n => hlcEqual(n.id, id));
        if (node && !node.deleted) {
          node.deleted = true;
          changed = true;
        }
      }
    }
    if (changed) this.emit();
  }

  /**
   * Find insertion index for a new node with the given origin_id and id.
   * Mirrors the Rust `Rga::insert_position` algorithm exactly.
   */
  private insertPosition(originId: RgaHlc | null, id: RgaHlc): number {
    const start = originId === null
      ? 0
      : (() => {
          const idx = this.nodes.findIndex(n => hlcEqual(n.id, originId));
          return idx === -1 ? this.nodes.length : idx + 1;
        })();

    let pos = start;
    while (pos < this.nodes.length) {
      const sibling = this.nodes[pos];
      if (sibling && hlcEqualNullable(sibling.originId, originId) && hlcCompare(sibling.id, id) > 0) {
        pos++;
      } else {
        break;
      }
    }
    return pos;
  }

  private emit(): void {
    const text = this.value();
    for (const listener of this.listeners) listener(text);
  }
}
