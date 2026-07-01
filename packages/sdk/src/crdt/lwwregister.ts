import { Chunk, Effect, Option, Schema, Stream } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { LwwDelta, LwwEntry } from "../sync/delta.js";
import type { LwwRegisterConflictEvent } from "../conflict/types.js";

/** One entry in the per-handle undo stack (last-in, first-out). */
interface UndoEntry {
  /** msgpack-encoded HybridLogicalClock that was stamped on the `Set` op. */
  readonly targetHlcBytes: Uint8Array;
  /** The register value before the `Set` op, or `undefined` when the register was empty. */
  readonly prevValue: unknown;
}

const MAX_UNDO_STACK = 50;

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
  private readonly conflictListeners = new Set<(event: LwwRegisterConflictEvent) => void>();
  private readonly undoListeners = new Set<(skipped: boolean, reason?: string) => void>();
  private readonly encryptFn: ((v: unknown) => Promise<unknown>) | null;

  /** LIFO stack — most recent write is at the end. */
  private readonly undoStack: UndoEntry[] = [];

  constructor(opts: {
    ns: string;
    crdtId: string;
    clientId: number;
    transport: WsTransport;
    schema?: Schema.Schema<T>;
    initial?: LwwEntry | null;
    /** AES-GCM encryption function. When set, the value is encrypted before sending. */
    encryptFn?: (v: unknown) => Promise<unknown>;
  }) {
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
    this.schema = opts.schema ?? null;
    this.entry = opts.initial ?? null;
    this.encryptFn = opts.encryptFn ?? null;
  }

  /** Returns the raw LwwEntry for snapshot serialization. */
  rawEntry(): Readonly<LwwEntry> | null {
    return this.entry;
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

  /** Whether there is at least one write by this client that can be undone. */
  get canUndo(): boolean {
    return this.undoStack.length > 0;
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
   * Subscribes to the outcome of `undo()` calls.
   *
   * `skipped=false` — the undo was applied and broadcast.
   * `skipped=true`  — the undo was skipped because another client had since
   *                   overwritten the register (concurrent write, LWW conflict).
   *                   `reason` explains why.
   *
   * @returns An unsubscribe function.
   */
  onUndo(listener: (skipped: boolean, reason?: string) => void): () => void {
    this.undoListeners.add(listener);
    return () => { this.undoListeners.delete(listener); };
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

    // Capture the previous value BEFORE optimistic update for undo.
    const prevValue = this.entry?.value;

    const newEntry: LwwEntry = { value, hlc, author: this.clientId };
    if (this.entryWins(newEntry, this.entry)) {
      this.entry = newEntry;
      this.emit();
    }

    // Push onto the undo stack. The HLC bytes sent to the server must match
    // what we encode in op_bytes so the server can compare entry.hlc == target_hlc.
    const targetHlcBytes = encode(hlc);
    if (this.undoStack.length >= MAX_UNDO_STACK) this.undoStack.shift();
    this.undoStack.push({ targetHlcBytes, prevValue });

    void this._sendSet(value, hlc, ttlMs);
  }

  /**
   * Undo the most recent `set()` call made by this client.
   *
   * The server validates that the target write is still the winning entry before
   * applying the restore. If another client has since overwritten the register,
   * the undo is skipped and `onUndo` listeners are called with `skipped=true`.
   *
   * Does nothing if `canUndo` is false.
   */
  undo(): void {
    const entry = this.undoStack.pop();
    if (entry === undefined) return;

    // restore_entry: null means "the register was empty before this write".
    const restoreValue = entry.prevValue !== undefined ? entry.prevValue : null;
    const restoreEntryBytes = encode(restoreValue);

    this.transport.send({
      UndoLww: {
        crdt_id: this.crdtId,
        target_hlc: entry.targetHlcBytes,
        restore_entry: restoreEntryBytes,
      },
    });
  }

  private async _sendSet(
    value: T,
    hlc: { wall_ms: number; logical: number; node_id: number },
    ttlMs: number | undefined,
  ): Promise<void> {
    const wireValue = this.encryptFn !== null ? await this.encryptFn(value) : value;
    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: encode({ LwwRegister: { value: wireValue, hlc, author: this.clientId } }),
        ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
      },
    });
  }

  /**
   * Registers a listener that fires when this client's write was superseded
   * by a concurrent write from another client (server-pushed conflict event).
   *
   * @returns An unsubscribe function.
   */
  onConflict(listener: (event: LwwRegisterConflictEvent) => void): () => void {
    this.conflictListeners.add(listener);
    return () => { this.conflictListeners.delete(listener); };
  }

  applyConflict(winningClientId: number, winningTsMs: number): void {
    const event: LwwRegisterConflictEvent = {
      kind: "lww_overwritten",
      winningClientId,
      winningTsMs,
    };
    for (const l of this.conflictListeners) l(event);
  }

  /** Called by the client router when the server sends `UndoAck`. */
  applyUndoAck(): void {
    for (const l of this.undoListeners) l(false);
  }

  /** Called by the client router when the server sends `UndoSkipped`. */
  applyUndoSkipped(reason: string): void {
    for (const l of this.undoListeners) l(true, reason);
  }

  applyDelta(delta: LwwDelta): void {
    if (delta.entry === null) return;
    if (this.entryWins(delta.entry, this.entry)) {
      this.entry = delta.entry;
      this.emit();
    }
  }

  private decode(raw: unknown): T | null {
    if (this.schema !== null) {
      return Option.getOrNull(Schema.decodeUnknownOption(this.schema)(raw));
    }
    // No schema provided — T defaults to unknown, cast is the caller's responsibility.
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
