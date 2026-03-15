import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { CRDTMapDelta, CrdtValueDelta } from "../sync/delta.js";

export interface CrdtMapValue {
  [key: string]: unknown;
}

/**
 * Low-level handle for a CRDTMap — a map of named CRDT values.
 *
 * Each key holds an independent CRDT (GCounter, PNCounter, ORSet, LwwRegister,
 * or Presence). The type of each key is fixed at first write.
 *
 * Obtained via `MeridianClient.crdtmap()`. Prefer the `useCRDTMap` React hook
 * for component-level usage; use this handle directly in non-React environments.
 */
export class CRDTMapHandle {
  private state: CrdtMapValue = {};
  private readonly crdtId: string;
  private readonly clientId: number;
  private readonly transport: WsTransport;

  private readonly listeners = new Set<(value: CrdtMapValue) => void>();

  constructor(opts: {
    ns: string;
    crdtId: string;
    clientId: number;
    transport: WsTransport;
    initial?: CrdtMapValue;
  }) {
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
    this.state = opts.initial ?? {};
  }

  /** Returns a snapshot of the current map value (key → CRDT observable value). */
  value(): Readonly<CrdtMapValue> {
    return this.state;
  }

  /** Returns the value at a specific key, or `undefined` if absent. */
  get(key: string): unknown {
    return this.state[key];
  }

  /**
   * Registers a listener that is called whenever any key in the map changes.
   *
   * @returns An unsubscribe function — call it to stop receiving updates.
   */
  onChange(listener: (value: CrdtMapValue) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /** Increment a GCounter key by `amount` (default `1`). */
  incrementCounter(key: string, amount: number = 1): void {
    if (amount <= 0) throw new RangeError("CRDTMap.incrementCounter: amount must be > 0");
    const op = encode({
      CRDTMap: {
        key,
        crdt_type: "GCounter",
        op: { GCounter: { client_id: this.clientId, amount } },
      },
    });
    this.transport.send({ Op: { crdt_id: this.crdtId, op_bytes: op } });
  }

  /** Increment a PNCounter key by `amount` (default `1`). */
  incrementPNCounter(key: string, amount: number = 1): void {
    if (amount <= 0) throw new RangeError("CRDTMap.incrementPNCounter: amount must be > 0");
    const op = encode({
      CRDTMap: {
        key,
        crdt_type: "PNCounter",
        op: { PNCounter: { Increment: { client_id: this.clientId, amount } } },
      },
    });
    this.transport.send({ Op: { crdt_id: this.crdtId, op_bytes: op } });
  }

  /** Decrement a PNCounter key by `amount` (default `1`). */
  decrementPNCounter(key: string, amount: number = 1): void {
    if (amount <= 0) throw new RangeError("CRDTMap.decrementPNCounter: amount must be > 0");
    const op = encode({
      CRDTMap: {
        key,
        crdt_type: "PNCounter",
        op: { PNCounter: { Decrement: { client_id: this.clientId, amount } } },
      },
    });
    this.transport.send({ Op: { crdt_id: this.crdtId, op_bytes: op } });
  }

  /** Add an element to an ORSet key. `tag` must be a 16-byte UUID as Uint8Array. */
  orsetAdd(key: string, element: unknown, tag: Uint8Array): void {
    const op = encode({
      CRDTMap: {
        key,
        crdt_type: "ORSet",
        op: { ORSet: { Add: { element, tag } } },
      },
    });
    this.transport.send({ Op: { crdt_id: this.crdtId, op_bytes: op } });
  }

  /** Remove an element from an ORSet key. `knownTags` is the set of tags to remove. */
  orsetRemove(key: string, element: unknown, knownTags: Uint8Array[]): void {
    const op = encode({
      CRDTMap: {
        key,
        crdt_type: "ORSet",
        op: { ORSet: { Remove: { element, known_tags: knownTags } } },
      },
    });
    this.transport.send({ Op: { crdt_id: this.crdtId, op_bytes: op } });
  }

  /** Write a value to an LWW-Register key. */
  lwwSet(key: string, value: unknown): void {
    const wallMs = Date.now();
    const op = encode({
      CRDTMap: {
        key,
        crdt_type: "LwwRegister",
        op: {
          LwwRegister: {
            value,
            hlc: { wall_ms: wallMs, logical: 0, node_id: this.clientId },
            author: this.clientId,
          },
        },
      },
    });
    this.transport.send({ Op: { crdt_id: this.crdtId, op_bytes: op } });
  }

  applyDelta(delta: CRDTMapDelta): void {
    let changed = false;
    for (const [key, valueDelta] of Object.entries(delta.deltas)) {
      const updated = applyValueDelta(this.state[key], valueDelta);
      if (updated !== undefined) {
        this.state[key] = updated;
        changed = true;
      }
    }
    if (changed) this.emit();
  }

  private emit(): void {
    for (const listener of this.listeners) listener(this.state);
  }
}

const applyValueDelta = (current: unknown, delta: CrdtValueDelta): unknown => {
  if ("GCounter" in delta) {
    // HACK: current is unknown — shape is set by previous applyValueDelta call
    const currentCounts = (current as { counts?: Record<string, number> } | undefined)?.counts ?? {};
    const merged: Record<string, number> = { ...currentCounts };
    for (const [clientId, count] of Object.entries(delta.GCounter.counters)) {
      if ((merged[clientId] ?? 0) < count) merged[clientId] = count;
    }
    const total = Object.values(merged).reduce((sum, count) => sum + count, 0);
    return { total, counts: merged };
  }

  if ("PNCounter" in delta) {
    // HACK: current is unknown — shape is set by previous applyValueDelta call
    const currentPn = current as { pos?: Record<string, number>; neg?: Record<string, number> } | undefined;
    const pos = mergeCounterMap(currentPn?.pos ?? {}, delta.PNCounter.pos?.counters ?? {});
    const neg = mergeCounterMap(currentPn?.neg ?? {}, delta.PNCounter.neg?.counters ?? {});
    const value = Object.values(pos).reduce((sum, count) => sum + count, 0)
                - Object.values(neg).reduce((sum, count) => sum + count, 0);
    return { value, pos, neg };
  }

  if ("ORSet" in delta) {
    // HACK: current is unknown — shape is set by previous applyValueDelta call
    const currentOrset = current as { elements?: unknown[] } | undefined;
    const existing = new Set((currentOrset?.elements ?? []).map((element) => JSON.stringify(element)));
    for (const key of Object.keys(delta.ORSet.adds)) {
      existing.add(key);
    }
    for (const key of Object.keys(delta.ORSet.removes)) {
      existing.delete(key);
    }
    return { elements: Array.from(existing).map((key) => { try { return JSON.parse(key); } catch { return key; } }) };
  }

  if ("LwwRegister" in delta) {
    const entry = delta.LwwRegister.entry;
    if (entry === null) return current;
    // HACK: current is unknown — shape is set by previous applyValueDelta call
    const currentLww = current as { hlc?: { wall_ms: number; logical: number }; author?: number } | undefined;
    if (currentLww?.hlc) {
      const currentWallMs = Number(currentLww.hlc.wall_ms);
      const newWallMs = Number(entry.hlc.wall_ms);
      if (newWallMs < currentWallMs) return current;
      if (newWallMs === currentWallMs && entry.hlc.logical < (currentLww.hlc.logical ?? 0)) return current;
      if (newWallMs === currentWallMs && entry.hlc.logical === (currentLww.hlc.logical ?? 0) && Number(entry.author) <= Number(currentLww.author ?? 0)) return current;
    }
    return { value: entry.value, updatedAtMs: Number(entry.hlc.wall_ms), author: Number(entry.author) };
  }

  if ("Presence" in delta) {
    // HACK: current is unknown — shape is set by previous applyValueDelta call
    const currentPresence = current as { entries?: Record<string, unknown> } | undefined;
    const entries = { ...(currentPresence?.entries ?? {}) };
    for (const [clientId, entry] of Object.entries(delta.Presence.changes)) {
      if (entry === null) {
        delete entries[clientId];
      } else {
        entries[clientId] = { data: entry.data, expiresAtMs: Number(entry.hlc.wall_ms) + Number(entry.ttl_ms) };
      }
    }
    return { entries };
  }

  return current;
};

const mergeCounterMap = (source: Record<string, number>, incoming: Record<string, number>): Record<string, number> => {
  const result = { ...source };
  for (const [clientId, count] of Object.entries(incoming)) {
    if ((result[clientId] ?? 0) < count) result[clientId] = count;
  }
  return result;
};
