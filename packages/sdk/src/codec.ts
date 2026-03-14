import { decode as msgpackDecode, encode as msgpackEncode } from "@msgpack/msgpack";
import { Effect, Schema } from "effect";
import { CodecError } from "./errors.js";
import { ServerMsg } from "./schema.js";
import type { ClientMsg, VectorClock } from "./schema.js";

export const encode = (value: unknown): Uint8Array => msgpackEncode(value);

export const decode = (bytes: Uint8Array): unknown => msgpackDecode(bytes);

export const encodeClientMsg = (msg: ClientMsg): Uint8Array => encode(msg);

export const decodeServerMsg = (bytes: Uint8Array): Effect.Effect<ServerMsg, CodecError> => {
  let raw: unknown;
  try {
    raw = decode(bytes);
  } catch {
    return Effect.fail(new CodecError({ message: "msgpack decode failed", raw: bytes }));
  }

  return Schema.decodeUnknown(ServerMsg)(raw).pipe(
    Effect.mapError((parseError) =>
      new CodecError({
        message: `ServerMsg schema validation failed: ${parseError.message}`,
        raw: bytes,
      }),
    ),
  );
};

export const uuidToBytes = (uuid: string): Uint8Array => {
  const hex = uuid.replace(/-/g, "");
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
};

export const encodeVectorClock = (vc: VectorClock): Uint8Array => encode({ entries: vc });

export const decodeVectorClock = (bytes: Uint8Array): Effect.Effect<VectorClock, CodecError> => {
  let raw: unknown;
  try {
    raw = decode(bytes);
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
};
