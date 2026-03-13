/**
 * Wire schemas — mirror the Rust server's serde structs exactly.
 *
 * We use Effect Schema for runtime validation at decode boundaries.
 * Plain TypeScript types are inferred via Schema.Type<typeof S>.
 */

import { Schema } from "effect";

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------

/** Unix timestamp in milliseconds (u64 fits in JS number up to 2^53). */
export const TimestampMs = Schema.Number;
export type TimestampMs = typeof TimestampMs.Type;

/** client_id / author — opaque u64 encoded as number. */
export const ClientId = Schema.Number;
export type ClientId = typeof ClientId.Type;

// ---------------------------------------------------------------------------
// Auth / Token claims
// ---------------------------------------------------------------------------

export const Permissions = Schema.Struct({
  read: Schema.Boolean,
  write: Schema.Boolean,
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

// ---------------------------------------------------------------------------
// Vector clock
// ---------------------------------------------------------------------------

/** BTreeMap<client_id, version> — matches server VectorClock.entries */
export const VectorClock = Schema.Record({ key: Schema.String, value: Schema.Number });
export type VectorClock = typeof VectorClock.Type;

// ---------------------------------------------------------------------------
// Client → Server WebSocket messages
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Server → Client WebSocket messages
// ---------------------------------------------------------------------------

export const ServerMsg = Schema.Union(
  Schema.Struct({
    Delta: Schema.Struct({ crdt_id: Schema.String, delta_bytes: Schema.instanceOf(Uint8Array) }),
  }),
  Schema.Struct({ Ack: Schema.Struct({ seq: Schema.Number }) }),
  Schema.Struct({
    Error: Schema.Struct({ code: Schema.Number, message: Schema.String }),
  }),
);
export type ServerMsg = typeof ServerMsg.Type;

// ---------------------------------------------------------------------------
// CRDT value types (returned by GET /v1/namespaces/:ns/crdts/:id)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// HTTP response envelopes
// ---------------------------------------------------------------------------

export const CrdtGetResponse = Schema.Struct({
  crdt_type: Schema.String,
  value: Schema.Unknown,
  vector_clock: VectorClock,
});
export type CrdtGetResponse = typeof CrdtGetResponse.Type;

export const CrdtOpResponse = Schema.Struct({
  delta: Schema.Unknown,
  vector_clock: VectorClock,
  seq: Schema.Number,
});
export type CrdtOpResponse = typeof CrdtOpResponse.Type;

export const TokenIssueResponse = Schema.Struct({
  token: Schema.String,
});
export type TokenIssueResponse = typeof TokenIssueResponse.Type;

export const ErrorResponse = Schema.Struct({
  error: Schema.String,
  message: Schema.String,
});
export type ErrorResponse = typeof ErrorResponse.Type;
