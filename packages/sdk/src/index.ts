// Public SDK surface

export { MeridianClient } from "./client.js";
export type {
  MeridianClientConfig,
  ClientSnapshot,
  DeltaEvent,
  LiveQueryHandle,
  CRDTSnapshotEntry,
  GCounterSnapshotEntry,
  PNCounterSnapshotEntry,
  ORSetSnapshotEntry,
  LwwRegisterSnapshotEntry,
  PresenceSnapshotEntry,
  CRDTMapSnapshotEntry,
  RGASnapshotEntry,
  TreeSnapshotEntry,
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
export { AwarenessHandle } from "./crdt/awareness.js";
export type { AwarenessEntry } from "./crdt/awareness.js";
export { RGAHandle } from "./crdt/rga.js";
export { TreeHandle } from "./crdt/tree.js";
export type { TreeNodeValue, TreeDelta } from "./sync/delta.js";

// Transport
export { HttpClient } from "./transport/http.js";
export type { HttpClientConfig, HistoryEntry, HistoryResponse } from "./transport/http.js";
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
  PermissionsV1,
  PermissionsV2,
  PermEntry,
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
  QuerySpec,
  QueryResult,
  LiveQueryResult,
} from "./schema.js";
export type { TimestampMs, ClientId } from "./schema.js";

// Undo/redo
export { UndoManager } from "./undo/UndoManager.js";
export type { UndoEntry, UndoBatch, RgaInsertEntry, TreeAddEntry, TreeDeleteEntry, TreeMoveEntry, TreeUpdateEntry } from "./undo/types.js";

// Fractional indexing — position helpers for TreeCRDT
export { fi, between, before, after, start, end, spread } from "./utils/fractional.js";

// Validation — runtime validators for CRDT string values
export { CrdtValidationError, zodValidator, fnValidator } from "./validation/index.js";
export type { CrdtValidator } from "./validation/index.js";

// Conflict visualization — events emitted by TreeHandle on concurrent op resolution
export type { ConflictEvent, MoveDiscardedEvent, MoveReorderedEvent, LwwOverwriteEvent } from "./conflict/types.js";
export type { DiscardedMove } from "./sync/delta.js";

// AI Agents — provider-agnostic tool use helpers (Anthropic, OpenAI, Gemini)
export {
  getMeridianTools,
  toOpenAITools,
  toGeminiTools,
  executeMeridianTool,
  executeOpenAITool,
  executeGeminiTool,
} from "./agents.js";
export type {
  Tool,
  OpenAITool,
  GeminiFunctionDeclaration,
  GeminiTool,
  ToolUseBlock,
  OpenAIToolCall,
  GeminiFunctionCall,
  MeridianAgentConfig,
} from "./agents.js";

// Effect Layer — dependency injection
export { MeridianService, MeridianLive } from "./layer.js";

// Codec (for advanced use / testing)
export {
  encode,
  decode,
  encodeClientMsg,
  decodeServerMsg,
  encodeVectorClock,
  decodeVectorClock,
} from "./codec.js";
