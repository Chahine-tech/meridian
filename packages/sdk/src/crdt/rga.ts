import { Chunk, Effect, Stream } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { RGADelta } from "../sync/delta.js";

/**
 * Low-level handle for an RGA (Replicated Growable Array) CRDT — collaborative text editing.
 *
 * Obtained via `MeridianClient.rga()`. The RGA converges concurrent edits from multiple
 * clients using Hybrid Logical Clock IDs and left-origin insertion ordering.
 *
 * Operations use visible-position indices (tombstones are invisible to callers).
 */
export class RGAHandle {
  private text = "";
  private readonly crdtId: string;
  private readonly clientId: number;
  private readonly transport: WsTransport;
  private readonly listeners = new Set<(value: string) => void>();

  constructor(opts: {
    crdtId: string;
    clientId: number;
    transport: WsTransport;
  }) {
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
  }

  /** Returns the current text content. */
  value(): string {
    return this.text;
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
   * Characters are inserted one by one in order using the RGA Insert op.
   *
   * @param pos  - Visible character position (0-indexed).
   * @param text - String to insert.
   * @param ttlMs - Optional TTL for the op.
   */
  insert(pos: number, text: string, ttlMs?: number): void {
    if (text.length === 0) return;

    // Optimistic local update.
    this.text = this.text.slice(0, pos) + text + this.text.slice(pos);
    this.emit();

    // Send each character as a separate RGA Insert op. The server reorders
    // them using their HLC IDs — sending individually preserves per-char identity.
    const wallMs = Date.now();
    for (let i = 0; i < text.length; i++) {
      const op = encode({
        RGA: {
          Insert: {
            id: { wall_ms: wallMs, logical: i, node_id: this.clientId },
            origin_id: pos + i === 0 ? null : null, // server resolves via WAL
            content: text[i],
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

    // Optimistic local update.
    this.text = this.text.slice(0, pos) + this.text.slice(pos + length);
    this.emit();

    // Send a single Delete op per character position.
    // The server resolves the actual RGA node IDs from position.
    for (let i = 0; i < length; i++) {
      const op = encode({
        RGA: {
          Delete: {
            pos: pos + i,
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
  }

  /** Apply a delta received from the server. Replaces local text with authoritative state. */
  applyDelta(delta: RGADelta): void {
    if (delta.text === this.text) return;
    this.text = delta.text;
    this.emit();
  }

  private emit(): void {
    for (const listener of this.listeners) listener(this.text);
  }
}
