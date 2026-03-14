/**
 * Wire schemas — mirror the Rust server's serde structs exactly.
 *
 * We use Effect Schema for runtime validation at decode boundaries.
 * Plain TypeScript types are inferred via Schema.Type<typeof S>.
 */

import { Schema } from "effect";

/** Unix timestamp in milliseconds. Rust u64 decodes as bigint when > Number.MAX_SAFE_INTEGER — normalise to number. */
export const TimestampMs = Schema.Union(Schema.Number, Schema.BigIntFromSelf.pipe(Schema.transform(
  Schema.Number,
  { decode: (n) => Number(n), encode: (n) => BigInt(n) },
)));
export type TimestampMs = number;

/** client_id / author — Rust u64, may decode as bigint for large values. Normalise to number. */
export const ClientId = Schema.Union(Schema.Number, Schema.BigIntFromSelf.pipe(Schema.transform(
  Schema.Number,
  { decode: (n) => Number(n), encode: (n) => BigInt(n) },
)));
export type ClientId = number;

export const Permissions = Schema.Struct({
  read: Schema.Array(Schema.String),
  write: Schema.Array(Schema.String),
  admin: Schema.Boolean,
});
export type Permissions = typeof Permissions.Type;

export const TokenClaims = Schema.Struct({
  namespace: Schema.String,
  client_id: ClientId,
  expires_at: TimestampMs,
  permissions: Permissions,
});
export type TokenClaims = typeof TokenClaims.Type;

/** BTreeMap<client_id, version> — matches server VectorClock.entries */
export const VectorClock = Schema.Record({ key: Schema.String, value: Schema.Number });
export type VectorClock = typeof VectorClock.Type;

export const ClientMsg = Schema.Union(
  Schema.Struct({ Subscribe: Schema.Struct({ crdt_id: Schema.String }) }),
  Schema.Struct({
    Op: Schema.Struct({ crdt_id: Schema.String, op_bytes: Schema.Uint8ArrayFromSelf }),
  }),
  Schema.Struct({
    Sync: Schema.Struct({ crdt_id: Schema.String, since_vc: Schema.Uint8ArrayFromSelf }),
  }),
);
export type ClientMsg = typeof ClientMsg.Type;

export const ServerMsg = Schema.Union(
  Schema.Struct({
    Delta: Schema.Struct({
      crdt_id: Schema.String,
      delta_bytes: Schema.Uint8ArrayFromSelf,
    }),
  }),
  Schema.Struct({ Ack: Schema.Struct({ seq: Schema.Number }) }),
  Schema.Struct({
    Error: Schema.Struct({ code: Schema.Number, message: Schema.String }),
  }),
);
export type ServerMsg = typeof ServerMsg.Type;

export const GCounterValue = Schema.Struct({
  value: Schema.Number,
  counts: Schema.Record({ key: Schema.String, value: Schema.Number }),
});
export type GCounterValue = typeof GCounterValue.Type;

export const PNCounterValue = Schema.Struct({
  value: Schema.Number,
});
export type PNCounterValue = typeof PNCounterValue.Type;

export const ORSetValue = Schema.Struct({
  elements: Schema.Array(Schema.Unknown),
});
export type ORSetValue = typeof ORSetValue.Type;

export const LwwValue = Schema.Struct({
  value: Schema.NullOr(Schema.Unknown),
  updated_at_ms: Schema.NullOr(TimestampMs),
  author: Schema.NullOr(ClientId),
});
export type LwwValue = typeof LwwValue.Type;

export const PresenceEntryView = Schema.Struct({
  data: Schema.Unknown,
  expires_at_ms: TimestampMs,
});
export type PresenceEntryView = typeof PresenceEntryView.Type;

export const PresenceValue = Schema.Struct({
  entries: Schema.Record({ key: Schema.String, value: PresenceEntryView }),
});
export type PresenceValue = typeof PresenceValue.Type;

/** GET /v1/namespaces/:ns/crdts/:id — server returns the raw JSON value directly. */
export const CrdtGetResponse = Schema.Unknown;
export type CrdtGetResponse = unknown;

/** POST /v1/namespaces/:ns/crdts/:id/ops — server returns msgpack-encoded delta bytes. */
export const CrdtOpResponse = Schema.Unknown;
export type CrdtOpResponse = unknown;

export const TokenIssueResponse = Schema.Struct({
  token: Schema.String,
});
export type TokenIssueResponse = typeof TokenIssueResponse.Type;

export const ErrorResponse = Schema.Struct({
  error: Schema.String,
  message: Schema.String,
});
export type ErrorResponse = typeof ErrorResponse.Type;
