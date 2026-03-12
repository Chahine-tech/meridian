/**
 * Presence handle — TTL-aware presence tracking.
 *
 * `heartbeat(data, ttlMs)` upserts the local client's entry.
 * `leave()` creates an explicit tombstone (ttl = 0).
 * Background GC on the server prunes expired entries every 5s.
 *
 * Pass a `schema` to get runtime validation of incoming delta data:
 *   client.presence("id", Schema.Struct({ cursor: Schema.Tuple(Schema.Number, Schema.Number) }))
 */

import { Schema } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { PresenceDelta, PresenceEntryDelta } from "../sync/delta.js";

export interface PresenceEntry<T> {
  clientId: number;
  data: T;
  expiresAtMs: number;
}

export class PresenceHandle<T> {
  private readonly entries = new Map<string, PresenceEntry<T>>();
  private readonly crdtId: string;
  private readonly clientId: number;
  private readonly transport: WsTransport;
  private readonly schema: Schema.Schema<T> | null;
  private readonly listeners = new Set<(entries: PresenceEntry<T>[]) => void>();

  constructor(opts: {
    ns: string;
    crdtId: string;
    clientId: number;
    transport: WsTransport;
    schema?: Schema.Schema<T>;
  }) {
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
    this.schema = opts.schema ?? null;
  }

  // ---- Read ----

  /** Returns all live entries (expired entries are filtered out). */
  online(): PresenceEntry<T>[] {
    const now = Date.now();
    const live: PresenceEntry<T>[] = [];
    for (const entry of this.entries.values()) {
      if (entry.expiresAtMs > now) live.push(entry);
    }
    return live;
  }

  onChange(listener: (entries: PresenceEntry<T>[]) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  // ---- Write ----

  /** Send a presence heartbeat. Call every `ttlMs / 2` to stay alive. Default ttl: 30s. */
  heartbeat(data: T, ttlMs: number = 30_000): void {
    const wallMs = Date.now();
    const hlc = { wall_ms: wallMs, logical: 0, node_id: this.clientId };

    this.entries.set(String(this.clientId), {
      clientId: this.clientId,
      data,
      expiresAtMs: wallMs + ttlMs,
    });
    this.emit();

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: encode({
          Presence: { Heartbeat: { client_id: this.clientId, data, hlc, ttl_ms: ttlMs } },
        }),
      },
    });
  }

  /** Explicitly leave — entry expires immediately for all peers. */
  leave(): void {
    const wallMs = Date.now();
    const hlc = { wall_ms: wallMs, logical: 0, node_id: this.clientId };

    this.entries.delete(String(this.clientId));
    this.emit();

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: encode({ Presence: { Leave: { client_id: this.clientId, hlc } } }),
      },
    });
  }

  // ---- Delta application ----

  applyDelta(delta: PresenceDelta): void {
    let changed = false;
    for (const [clientIdStr, entry] of Object.entries(delta.changes)) {
      if (entry === null) {
        if (this.entries.has(clientIdStr)) {
          this.entries.delete(clientIdStr);
          changed = true;
        }
      } else {
        changed = this.mergeEntry(clientIdStr, entry) || changed;
      }
    }
    if (changed) this.emit();
  }

  // ---- Internal ----

  private decode(raw: unknown): T {
    if (this.schema !== null) {
      return Schema.decodeUnknownSync(this.schema)(raw);
    }
    return raw as T;
  }

  private mergeEntry(clientIdStr: string, incoming: PresenceEntryDelta): boolean {
    const existing = this.entries.get(clientIdStr);
    const incomingExpiresAt = incoming.hlc.wall_ms + incoming.ttl_ms;

    if (existing === undefined) {
      if (incoming.ttl_ms > 0) {
        this.entries.set(clientIdStr, {
          clientId: Number(clientIdStr),
          data: this.decode(incoming.data),
          expiresAtMs: incomingExpiresAt,
        });
        return true;
      }
      return false;
    }

    if (incoming.hlc.wall_ms > 0) {
      if (incoming.ttl_ms === 0) {
        this.entries.delete(clientIdStr);
      } else {
        this.entries.set(clientIdStr, {
          clientId: Number(clientIdStr),
          data: this.decode(incoming.data),
          expiresAtMs: incomingExpiresAt,
        });
      }
      return true;
    }
    return false;
  }

  private emit(): void {
    const live = this.online();
    for (const l of this.listeners) l(live);
  }
}
