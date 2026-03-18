// Public SDK surface

export { MeridianClient } from "./client.js";
export type {
  MeridianClientConfig,
  ClientSnapshot,
  DeltaEvent,
  CRDTSnapshotEntry,
  GCounterSnapshotEntry,
  PNCounterSnapshotEntry,
  ORSetSnapshotEntry,
  LwwRegisterSnapshotEntry,
  PresenceSnapshotEntry,
  CRDTMapSnapshotEntry,
} from "./client.js";

// CRDT handles
export { GCounterHandle } from "./crdt/gcounter.js";
export { PNCounterHandle } from "./crdt/pncounter.js";
export { ORSetHandle } from "./crdt/orset.js";
export { LwwRegisterHandle } from "./crdt/lwwregister.js";
export { PresenceHandle } from "./crdt/presence.js";
export type { PresenceEntry } from "./crdt/presence.js";
export { CRDTMapHandle } from "./crdt/crdtmap.js";
export type { CrdtMapValue } from "./crdt/crdtmap.js";

// Transport
export { HttpClient } from "./transport/http.js";
export type { HttpClientConfig } from "./transport/http.js";
export { WsTransport } from "./transport/websocket.js";
export type { WsTransportConfig, WsState } from "./transport/websocket.js";

// Auth
export { parseToken, parseAndValidateToken, checkTokenExpiry, tokenTtlMs } from "./auth/token.js";

// Errors (all Data.TaggedError — matchable with Effect.catchTag)
export {
  CodecError,
  TokenParseError,
  TokenExpiredError,
  HttpError,
  NetworkError,
  TransportError,
} from "./errors.js";

// Schema (Effect Schema — use for runtime validation / advanced use)
export {
  TokenClaims,
  Permissions,
  VectorClock,
  ClientMsg,
  ServerMsg,
  GCounterValue,
  PNCounterValue,
  ORSetValue,
  LwwValue,
  PresenceValue,
  CrdtGetResponse,
  CrdtOpResponse,
  ErrorResponse,
} from "./schema.js";
export type { TimestampMs, ClientId } from "./schema.js";

// Codec (for advanced use / testing)
export {
  encode,
  decode,
  encodeClientMsg,
  decodeServerMsg,
  encodeVectorClock,
  decodeVectorClock,
} from "./codec.js";
