/**
 * LWW Register handle — Last-Write-Wins single-value cell.
 *
 * The winning write is determined by HLC (highest wall_ms wins),
 * tie-broken by author (higher client_id wins). The client sends its
 * local wall clock as HLC wall_ms; the server enforces ±30s drift limit.
 *
 * Pass a `schema` to get runtime validation of incoming deltas:
 *   client.lwwregister("id", Schema.Struct({ x: Schema.Number }))
 */

import { Schema } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { LwwDelta, LwwEntry } from "../sync/delta.js";

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

  // ---- Read ----

  value(): T | null {
    if (this.entry === null) return null;
    return this.decode(this.entry.value);
  }

  /** Metadata: when was the last write and by whom. */
  meta(): { updatedAtMs: number; author: number } | null {
    if (this.entry === null) return null;
    return { updatedAtMs: this.entry.hlc.wall_ms, author: this.entry.author };
  }

  onChange(listener: (value: T | null) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  // ---- Write ----

  set(value: T): void {
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
        op_bytes: encode({ LwwRegister: { value, hlc, author: this.clientId } }),
      },
    });
  }

  // ---- Delta application ----

  applyDelta(delta: LwwDelta): void {
    if (delta.entry === null) return;
    if (this.entryWins(delta.entry, this.entry)) {
      this.entry = delta.entry;
      this.emit();
    }
  }

  // ---- Internal ----

  private decode(raw: unknown): T {
    if (this.schema !== null) {
      return Schema.decodeUnknownSync(this.schema)(raw);
    }
    // No schema provided — T defaults to unknown, cast is the caller's responsibility
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
