/**
 * Token parsing and expiry check (client-side only — no signature verification).
 *
 * Wire format: `base64url_no_pad(msgpack(TokenClaims)) + "." + base64url_no_pad(sig[64B])`
 */

import { unpack } from "msgpackr";
import { Effect, Schema } from "effect";
import { TokenParseError, TokenExpiredError } from "../errors.js";
import { TokenClaims } from "../schema.js";

// ---------------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------------

/**
 * Parse a Meridian token string and return the decoded claims.
 * Returns Effect<TokenClaims, TokenParseError>.
 * Does NOT verify the ed25519 signature — the server enforces that.
 */
export const parseToken = (token: string): Effect.Effect<TokenClaims, TokenParseError> =>
  Effect.gen(function* () {
    const dotIndex = token.indexOf(".");
    if (dotIndex === -1) {
      yield* Effect.fail(new TokenParseError({ message: "Invalid token format: missing '.'" }));
      return undefined as never;
    }

    const payloadB64 = token.slice(0, dotIndex);

    let bytes: Uint8Array;
    try {
      bytes = base64urlDecode(payloadB64);
    } catch {
      yield* Effect.fail(new TokenParseError({ message: "Invalid token format: base64url decode failed" }));
      return undefined as never;
    }

    let raw: unknown;
    try {
      raw = unpack(bytes);
    } catch {
      yield* Effect.fail(new TokenParseError({ message: "Invalid token format: msgpack decode failed" }));
      return undefined as never;
    }

    return yield* Schema.decodeUnknown(TokenClaims)(raw).pipe(
      Effect.mapError((e) =>
        new TokenParseError({ message: `Invalid token format: ${e.message}` }),
      ),
    );
  });

/**
 * Check token expiry. Returns Effect<TokenClaims, TokenExpiredError>.
 * Clock-skew tolerance: ±5s.
 */
export const checkTokenExpiry = (
  claims: TokenClaims,
  nowMs = Date.now(),
): Effect.Effect<TokenClaims, TokenExpiredError> => {
  const SKEW_MS = 5_000;
  if (nowMs >= claims.expires_at + SKEW_MS) {
    return Effect.fail(new TokenExpiredError({ expiredAt: claims.expires_at }));
  }
  return Effect.succeed(claims);
};

/**
 * Parse and check expiry in one step.
 * Returns Effect<TokenClaims, TokenParseError | TokenExpiredError>.
 */
export const parseAndValidateToken = (
  token: string,
): Effect.Effect<TokenClaims, TokenParseError | TokenExpiredError> =>
  parseToken(token).pipe(Effect.flatMap(checkTokenExpiry));

/** Returns milliseconds until the token expires (negative if already expired). */
export const tokenTtlMs = (claims: TokenClaims, nowMs = Date.now()): number =>
  claims.expires_at - nowMs;

// ---------------------------------------------------------------------------
// Base64url (no-padding) decoder — no external dep
// ---------------------------------------------------------------------------

function base64urlDecode(input: string): Uint8Array {
  const padded = input.replace(/-/g, "+").replace(/_/g, "/");
  const padLen = (4 - (padded.length % 4)) % 4;
  const b64 = padded + "=".repeat(padLen);
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}
