/**
 * Delta application helpers.
 *
 * The server sends deltas as opaque msgpack bytes inside `ServerMsg.Delta`.
 * Each CRDT type decodes its own delta format. This module provides typed
 * delta decoders that match the Rust server's serde output.
 */

import { unpack } from "msgpackr";

// ---------------------------------------------------------------------------
// GCounter delta
// ---------------------------------------------------------------------------

/** Sparse map of client_id → new count. Only changed entries are included. */
export interface GCounterDelta {
  increments: Record<string, number>;
}

export function decodeGCounterDelta(bytes: Uint8Array): GCounterDelta {
  const raw = unpack(bytes) as { increments?: Record<string, number> };
  return { increments: raw.increments ?? {} };
}

// ---------------------------------------------------------------------------
// PNCounter delta
// ---------------------------------------------------------------------------

export interface PNCounterDelta {
  p: GCounterDelta;
  n: GCounterDelta;
}

export function decodePNCounterDelta(bytes: Uint8Array): PNCounterDelta {
  const raw = unpack(bytes) as {
    p?: { increments?: Record<string, number> };
    n?: { increments?: Record<string, number> };
  };
  return {
    p: { increments: raw.p?.increments ?? {} },
    n: { increments: raw.n?.increments ?? {} },
  };
}

// ---------------------------------------------------------------------------
// ORSet delta
// ---------------------------------------------------------------------------

export interface ORSetDelta {
  /** element → set of add-tags (UUIDs as strings) */
  added: Record<string, string[]>;
  /** element → set of removed tags */
  removed: Record<string, string[]>;
}

export function decodeORSetDelta(bytes: Uint8Array): ORSetDelta {
  const raw = unpack(bytes) as {
    added?: Record<string, string[]>;
    removed?: Record<string, string[]>;
  };
  return {
    added: raw.added ?? {},
    removed: raw.removed ?? {},
  };
}

// ---------------------------------------------------------------------------
// LWW Register delta
// ---------------------------------------------------------------------------

export interface LwwEntry {
  value: unknown;
  hlc: { wall_ms: number; logical: number; node_id: number };
  author: number;
}

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
  hlc: { wall_ms: number; logical: number; node_id: number };
  ttl_ms: number;
}

export interface PresenceDelta {
  /** client_id → entry (null = removed/tombstone) */
  changes: Record<string, PresenceEntryDelta | null>;
}

export function decodePresenceDelta(bytes: Uint8Array): PresenceDelta {
  const raw = unpack(bytes) as { changes?: Record<string, PresenceEntryDelta | null> };
  return { changes: raw.changes ?? {} };
}
