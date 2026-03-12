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
export function encode(value: unknown): Uint8Array {
  return pack(value) as Uint8Array;
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

  const obj = raw as { entries?: unknown };
  return Schema.decodeUnknown(Schema.Record({ key: Schema.String, value: Schema.Number }))(
    obj.entries ?? {},
  ).pipe(
    Effect.mapError((e) =>
      new CodecError({ message: `VectorClock schema validation failed: ${e.message}`, raw: bytes }),
    ),
  );
}
