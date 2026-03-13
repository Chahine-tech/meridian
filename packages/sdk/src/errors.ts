/**
 * Typed errors for the Meridian SDK — all extend Data.TaggedError so they
 * can be matched with Effect.catchTag / Effect.catchAll.
 */

import { Data } from "effect";
import type { ErrorResponse } from "./schema.js";

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

export class CodecError extends Data.TaggedError("CodecError")<{
  readonly message: string;
  readonly raw: Uint8Array;
}> {}

// ---------------------------------------------------------------------------
// Auth / Token
// ---------------------------------------------------------------------------

export class TokenParseError extends Data.TaggedError("TokenParseError")<{
  readonly message: string;
}> {}

export class TokenExpiredError extends Data.TaggedError("TokenExpiredError")<{
  readonly expiredAt: number;
}> {}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

export class HttpError extends Data.TaggedError("HttpError")<{
  readonly status: number;
  readonly body: ErrorResponse;
}> {}

export class NetworkError extends Data.TaggedError("NetworkError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

// ---------------------------------------------------------------------------
// WebSocket / Transport
// ---------------------------------------------------------------------------

export class TransportError extends Data.TaggedError("TransportError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}
