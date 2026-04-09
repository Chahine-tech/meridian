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

/** V1 permissions — glob-list style (legacy tokens). */
export const PermissionsV1 = Schema.Struct({
  read: Schema.Array(Schema.String),
  write: Schema.Array(Schema.String),
  admin: Schema.Boolean,
});
export type PermissionsV1 = typeof PermissionsV1.Type;

/** A single permission rule in a V2 token. */
export const PermEntry = Schema.Struct({
  p: Schema.String,
  o: Schema.optional(Schema.Number),
  e: Schema.optional(Schema.Number),
});
export type PermEntry = typeof PermEntry.Type;

/** V2 fine-grained permissions. */
export const PermissionsV2 = Schema.Struct({
  v: Schema.Literal(2),
  r: Schema.Array(PermEntry),
  w: Schema.Array(PermEntry),
  admin: Schema.Boolean,
  rl: Schema.optional(Schema.Number),
});
export type PermissionsV2 = typeof PermissionsV2.Type;

/** Token permissions — V1 or V2. */
export const Permissions = Schema.Union(PermissionsV2, PermissionsV1);
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
    Op: Schema.Struct({
      crdt_id: Schema.String,
      op_bytes: Schema.Uint8ArrayFromSelf,
      ttl_ms: Schema.optional(Schema.Number),
      client_seq: Schema.optional(Schema.Number),
    }),
  }),
  Schema.Struct({
    Sync: Schema.Struct({ crdt_id: Schema.String, since_vc: Schema.Uint8ArrayFromSelf }),
  }),
  Schema.Struct({
    AwarenessUpdate: Schema.Struct({
      key: Schema.String,
      data: Schema.Uint8ArrayFromSelf,
    }),
  }),
  Schema.Struct({
    SubscribeQuery: Schema.Struct({
      query_id: Schema.String,
      query: Schema.Struct({
        from: Schema.String,
        type: Schema.optional(Schema.String),
        aggregate: Schema.String,
        where: Schema.optional(
          Schema.Struct({
            contains: Schema.optional(Schema.Unknown),
            updated_after: Schema.optional(Schema.Number),
          }),
        ),
      }),
    }),
  }),
  Schema.Struct({
    UnsubscribeQuery: Schema.Struct({ query_id: Schema.String }),
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
  Schema.Struct({ Ack: Schema.Struct({ seq: Schema.Number, client_seq: Schema.optional(Schema.Number) }) }),
  Schema.Struct({ BatchAck: Schema.Struct({ seq: Schema.Number, count: Schema.Number, client_seq: Schema.optional(Schema.Number) }) }),
  Schema.Struct({
    Error: Schema.Struct({ code: Schema.Number, message: Schema.String }),
  }),
  Schema.Struct({
    AwarenessBroadcast: Schema.Struct({
      client_id: ClientId,
      key: Schema.String,
      data: Schema.Uint8ArrayFromSelf,
    }),
  }),
  Schema.Struct({
    QueryResult: Schema.Struct({
      query_id: Schema.String,
      value: Schema.Unknown,
      matched: Schema.Number,
    }),
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

export const QuerySpec = Schema.Struct({
  from: Schema.String,
  type: Schema.optional(Schema.String),
  aggregate: Schema.Literal(
    "sum", "max", "min", "count",
    "union", "intersection",
    "latest", "collect",
    "merge",
  ),
  where: Schema.optional(Schema.Struct({
    contains: Schema.optional(Schema.Unknown),
    updatedAfter: Schema.optional(Schema.Number),
  })),
});
export type QuerySpec = typeof QuerySpec.Type;

export const QueryResult = Schema.Struct({
  value: Schema.Unknown,
  matched: Schema.Number,
  scanned: Schema.Number,
  execution_ms: Schema.Number,
});
export type QueryResult = typeof QueryResult.Type;

/** Result pushed by the server for a live query subscription (WebSocket only). */
export const LiveQueryResult = Schema.Struct({
  value: Schema.Unknown,
  matched: Schema.Number,
});
export type LiveQueryResult = typeof LiveQueryResult.Type;
