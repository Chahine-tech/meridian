import { Chunk, Effect, Schema, Stream } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { LwwDelta, LwwEntry } from "../sync/delta.js";

/**
 * Low-level handle for a Last-Write-Wins register (LWW-Register) CRDT.
 *
 * Obtained via `MeridianClient.lwwregister()`. Prefer the `useLwwRegister` React
 * hook for component-level usage; use this handle directly in non-React environments.
 */
export class LwwRegisterHandle<T> {
  private entry: LwwEntry | null = null;
  private readonly crdtId: string;
  private readonly clientId: number;
  private readonly transport: WsTransport;
  private readonly schema: Schema.Schema<T> | null;
  private readonly listeners = new Set<(value: T | null) => void>();

  constructor(opts: {
    ns: string;
    crdtId: string;
    clientId: number;
    transport: WsTransport;
    schema?: Schema.Schema<T>;
    initial?: LwwEntry | null;
  }) {
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
    this.schema = opts.schema ?? null;
    this.entry = opts.initial ?? null;
  }

  /** Returns the current register value, or `null` if no value has been written yet. */
  value(): T | null {
    if (this.entry === null) return null;
    return this.decode(this.entry.value);
  }

  /**
   * Returns metadata about the winning entry, or `null` if the register is empty.
   *
   * `updatedAtMs` is the wall-clock timestamp of the write; `author` is the
   * numeric client id of the writer.
   */
  meta(): { updatedAtMs: number; author: number } | null {
    if (this.entry === null) return null;
    return { updatedAtMs: Number(this.entry.hlc.wall_ms), author: Number(this.entry.author) };
  }

  /**
   * Registers a listener that is called whenever the register value changes.
   *
   * @returns An unsubscribe function — call it to stop receiving updates.
   */
  onChange(listener: (value: T | null) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /** Returns a Stream that emits the register value (or null) on every change. */
  stream(): Stream.Stream<T | null, never, never> {
    return Stream.async<T | null>((emit) => {
      const unsub = this.onChange((value) => { void emit(Effect.succeed(Chunk.of(value))); });
      return Effect.sync(unsub);
    });
  }

  /**
   * Writes `value` to the register and broadcasts the operation.
   *
   * The write is stamped with the current wall-clock time. If a concurrent
   * write from another client has a later timestamp it will win over this one.
   */
  set(value: T, ttlMs?: number): void {
    const wallMs = Date.now();
    const hlc = { wall_ms: wallMs, logical: 0, node_id: this.clientId };

    const newEntry: LwwEntry = { value, hlc, author: this.clientId };
    if (this.entryWins(newEntry, this.entry)) {
      this.entry = newEntry;
      this.emit();
    }

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: encode({ LwwRegister: { value, hlc: { wall_ms: wallMs, logical: 0, node_id: this.clientId }, author: this.clientId } }),
        ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
      },
    });
  }

  applyDelta(delta: LwwDelta): void {
    if (delta.entry === null) return;
    if (this.entryWins(delta.entry, this.entry)) {
      this.entry = delta.entry;
      this.emit();
    }
  }

  private decode(raw: unknown): T {
    if (this.schema !== null) {
      return Schema.decodeUnknownSync(this.schema)(raw);
    }
    // HACK: No schema provided — T defaults to unknown, cast is the caller's responsibility.
    return raw as T;
  }

  private entryWins(candidate: LwwEntry, existing: LwwEntry | null): boolean {
    if (existing === null) return true;
    if (candidate.hlc.wall_ms !== existing.hlc.wall_ms) {
      return candidate.hlc.wall_ms > existing.hlc.wall_ms;
    }
    if (candidate.hlc.logical !== existing.hlc.logical) {
      return candidate.hlc.logical > existing.hlc.logical;
    }
    return candidate.author > existing.author;
  }

  private emit(): void {
    const v = this.value();
    for (const l of this.listeners) l(v);
  }
}
