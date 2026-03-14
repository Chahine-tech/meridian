import { Schema } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { PresenceDelta, PresenceEntryDelta } from "../sync/delta.js";

export interface PresenceEntry<T> {
  clientId: number;
  data: T;
  expiresAtMs: number;
}

/**
 * Low-level handle for a presence channel CRDT.
 *
 * Obtained via `MeridianClient.presence()`. Prefer the `usePresence` React hook
 * for component-level usage (it manages heartbeat timers and cleanup automatically);
 * use this handle directly in non-React environments.
 */
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

  /**
   * Returns all currently online entries — those whose TTL has not yet elapsed
   * as of the call time.
   */
  online(): PresenceEntry<T>[] {
    const now = Date.now();
    const live: PresenceEntry<T>[] = [];
    for (const entry of this.entries.values()) {
      if (entry.expiresAtMs > now) live.push(entry);
    }
    return live;
  }

  /**
   * Registers a listener that is called whenever the online entries change.
   *
   * @returns An unsubscribe function — call it to stop receiving updates.
   */
  onChange(listener: (entries: PresenceEntry<T>[]) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /**
   * Broadcasts a heartbeat that marks the current client as online for `ttlMs`
   * milliseconds (default `30 000`).
   *
   * Call this periodically (at least once per `ttlMs`) to remain visible to
   * other clients. The `usePresence` hook manages this automatically when
   * `opts.data` is provided.
   *
   * @param data - Arbitrary payload broadcast to other clients.
   * @param ttlMs - Time-to-live in milliseconds before the entry is considered stale.
   */
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

  /**
   * Marks the current client as offline and broadcasts a leave operation.
   *
   * After calling this, the client's entry is removed from the `online()` list
   * on all peers. The `usePresence` hook calls this automatically on unmount.
   */
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

  private decode(raw: unknown): T {
    if (this.schema !== null) {
      return Schema.decodeUnknownSync(this.schema)(raw);
    }
    return raw as T;
  }

  private mergeEntry(clientIdStr: string, incoming: PresenceEntryDelta): boolean {
    const existing = this.entries.get(clientIdStr);
    const incomingExpiresAt = Number(incoming.hlc.wall_ms) + Number(incoming.ttl_ms);

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

    if (incomingExpiresAt >= existing.expiresAtMs) {
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
    for (const listener of this.listeners) listener(live);
  }
}
