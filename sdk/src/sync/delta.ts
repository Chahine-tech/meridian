/**
 * Delta application helpers.
 *
 * The server sends deltas as opaque msgpack bytes inside `ServerMsg.Delta`.
 * Each CRDT type decodes its own delta format matching Rust serde output.
 *
 * Field names mirror the Rust structs exactly (rmp-serde named encoding).
 */

import { unpack } from "msgpackr";

// ---------------------------------------------------------------------------
// GCounter delta
// ---------------------------------------------------------------------------

/** GCounterDelta { counters: BTreeMap<u64, u64> } */
export interface GCounterDelta {
  counters: Record<string, number>;
}

export function decodeGCounterDelta(bytes: Uint8Array): GCounterDelta {
  const raw = unpack(bytes) as { counters?: Record<string, number> };
  return { counters: raw.counters ?? {} };
}

// ---------------------------------------------------------------------------
// PNCounter delta
// ---------------------------------------------------------------------------

/** PNCounterDelta { pos: Option<GCounterDelta>, neg: Option<GCounterDelta> } */
export interface PNCounterDelta {
  pos: GCounterDelta | null;
  neg: GCounterDelta | null;
}

export function decodePNCounterDelta(bytes: Uint8Array): PNCounterDelta {
  const raw = unpack(bytes) as {
    pos?: { counters?: Record<string, number> } | null;
    neg?: { counters?: Record<string, number> } | null;
  };
  return {
    pos: raw.pos ? { counters: raw.pos.counters ?? {} } : null,
    neg: raw.neg ? { counters: raw.neg.counters ?? {} } : null,
  };
}

// ---------------------------------------------------------------------------
// ORSet delta
// ---------------------------------------------------------------------------

/**
 * ORSetDelta { adds: HashMap<String, HashSet<Uuid>>, removes: HashMap<String, HashSet<Uuid>> }
 * Uuid bytes are decoded by msgpackr as Uint8Array or number[].
 */
export interface ORSetDelta {
  adds: Record<string, Uint8Array[]>;
  removes: Record<string, Uint8Array[]>;
}

export function decodeORSetDelta(bytes: Uint8Array): ORSetDelta {
  const raw = unpack(bytes) as {
    adds?: Record<string, unknown[]>;
    removes?: Record<string, unknown[]>;
  };
  const toBytes = (v: unknown): Uint8Array =>
    v instanceof Uint8Array ? v : new Uint8Array(v as number[]);
  const decodeMap = (m?: Record<string, unknown[]>) =>
    Object.fromEntries(Object.entries(m ?? {}).map(([k, tags]) => [k, tags.map(toBytes)]));
  return { adds: decodeMap(raw.adds), removes: decodeMap(raw.removes) };
}

// ---------------------------------------------------------------------------
// LWW Register delta
// ---------------------------------------------------------------------------

/** LwwEntry { value, hlc: HybridLogicalClock, author: u64 } */
export interface LwwEntry {
  value: unknown;
  hlc: { wall_ms: number | bigint; logical: number; node_id: number | bigint };
  author: number | bigint;
}

/** LwwDelta { entry: Option<LwwEntry> } */
export interface LwwDelta {
  entry: LwwEntry | null;
}

export function decodeLwwDelta(bytes: Uint8Array): LwwDelta {
  const raw = unpack(bytes) as { entry?: LwwEntry | null };
  return { entry: raw.entry ?? null };
}

// ---------------------------------------------------------------------------
// Presence delta
// ---------------------------------------------------------------------------

export interface PresenceEntryDelta {
  data: unknown;
  hlc: { wall_ms: number | bigint; logical: number; node_id: number | bigint };
  ttl_ms: number | bigint;
}

/** PresenceDelta — changes per client_id */
export interface PresenceDelta {
  changes: Record<string, PresenceEntryDelta | null>;
}

export function decodePresenceDelta(bytes: Uint8Array): PresenceDelta {
  const raw = unpack(bytes) as { changes?: Record<string, PresenceEntryDelta | null> };
  return { changes: raw.changes ?? {} };
}
