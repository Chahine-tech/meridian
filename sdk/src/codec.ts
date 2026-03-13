/**
 * Msgpack codec — wraps msgpackr with Meridian-specific conventions.
 *
 * The server uses rmp-serde with `to_vec_named` (named fields) so enum
 * variants are encoded as `{ "VariantName": { ...fields } }` maps.
 * msgpackr handles this transparently in JS land.
 */

import { pack, unpack } from "msgpackr";
import { Effect, Schema } from "effect";
import { CodecError } from "./errors.js";
import { ServerMsg } from "./schema.js";
import type { ClientMsg, VectorClock } from "./schema.js";

// ---------------------------------------------------------------------------
// Low-level encode / decode
// ---------------------------------------------------------------------------

/** Encode any value to msgpack bytes. */
export function encode(value: unknown): Uint8Array<ArrayBuffer> {
  return pack(value) as Uint8Array<ArrayBuffer>;
}

/** Decode msgpack bytes to a plain JS value (unsafe — no schema validation). */
export function decode(bytes: Uint8Array): unknown {
  return unpack(bytes);
}

// ---------------------------------------------------------------------------
// Typed helpers for WebSocket frames
// ---------------------------------------------------------------------------

/**
 * Encode a ClientMsg to a binary WebSocket frame.
 * The msgpack encoding matches rmp-serde's "named" enum format:
 *   `{ Subscribe: { crdt_id: "foo" } }` → msgpack map with one key.
 */
export function encodeClientMsg(msg: ClientMsg): Uint8Array {
  return encode(msg);
}

/**
 * Decode a binary WebSocket frame into a ServerMsg.
 * Returns Effect<ServerMsg, CodecError>.
 */
export function decodeServerMsg(bytes: Uint8Array): Effect.Effect<ServerMsg, CodecError> {
  let raw: unknown;
  try {
    raw = unpack(bytes);
  } catch {
    return Effect.fail(new CodecError({ message: "msgpack decode failed", raw: bytes }));
  }

  // Schema.decodeUnknown accepts `unknown` input — correct for runtime decode boundaries
  return Schema.decodeUnknown(ServerMsg)(raw).pipe(
    Effect.mapError((parseError) =>
      new CodecError({
        message: `ServerMsg schema validation failed: ${parseError.message}`,
        raw: bytes,
      }),
    ),
  );
}

// ---------------------------------------------------------------------------
// Wire type helpers
// ---------------------------------------------------------------------------

/**
 * Convert a UUID string ("xxxxxxxx-xxxx-...") to a 16-byte Uint8Array.
 * Rust's `uuid::Uuid` is serialized by rmp-serde as raw bytes (bin type).
 */
export function uuidToBytes(uuid: string): Uint8Array {
  const hex = uuid.replace(/-/g, "");
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

/**
 * Encode a wall-clock timestamp (ms) as BigInt for msgpack u64 encoding.
 * msgpackr encodes JS `number` as float64 for large values; Rust u64 fields
 * require an integer encoding — BigInt forces msgpackr to use uint64.
 */
export function wallMsToBigInt(ms: number): bigint {
  return BigInt(ms);
}

// ---------------------------------------------------------------------------
// VectorClock <-> msgpack bytes
// ---------------------------------------------------------------------------

export function encodeVectorClock(vc: VectorClock): Uint8Array {
  // Server expects { entries: { "client_id": version, ... } }
  return encode({ entries: vc });
}

export function decodeVectorClock(bytes: Uint8Array): Effect.Effect<VectorClock, CodecError> {
  let raw: unknown;
  try {
    raw = unpack(bytes);
  } catch {
    return Effect.fail(new CodecError({ message: "VectorClock msgpack decode failed", raw: bytes }));
  }

  const entries = raw !== null && typeof raw === "object" && "entries" in raw
    ? (raw as { entries: unknown }).entries
    : {};
  return Schema.decodeUnknown(Schema.Record({ key: Schema.String, value: Schema.Number }))(
    entries ?? {},
  ).pipe(
    Effect.mapError((e) =>
      new CodecError({ message: `VectorClock schema validation failed: ${e.message}`, raw: bytes }),
    ),
  );
}
