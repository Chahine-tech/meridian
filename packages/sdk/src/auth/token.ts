import { decode } from "@msgpack/msgpack";
import { Effect, Schema } from "effect";
import { TokenParseError, TokenExpiredError } from "../errors.js";
import { TokenClaims } from "../schema.js";
import { TOKEN_SKEW_MS } from "../constants.js";

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
      raw = decode(bytes);
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

export const checkTokenExpiry = (
  claims: TokenClaims,
  nowMs = Date.now(),
): Effect.Effect<TokenClaims, TokenExpiredError> => {
  if (nowMs >= claims.expires_at + TOKEN_SKEW_MS) {
    return Effect.fail(new TokenExpiredError({ expiredAt: claims.expires_at }));
  }
  return Effect.succeed(claims);
};

export const parseAndValidateToken = (
  token: string,
): Effect.Effect<TokenClaims, TokenParseError | TokenExpiredError> =>
  parseToken(token).pipe(Effect.flatMap(checkTokenExpiry));

export const tokenTtlMs = (claims: TokenClaims, nowMs = Date.now()): number =>
  claims.expires_at - nowMs;

const base64urlDecode = (input: string): Uint8Array => {
  const padded = input.replace(/-/g, "+").replace(/_/g, "/");
  const padLen = (4 - (padded.length % 4)) % 4;
  const b64 = padded + "=".repeat(padLen);
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
};
