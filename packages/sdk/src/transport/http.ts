/**
 * HTTP transport — REST API client for Meridian.
 *
 * All methods return Effect<T, HttpError | NetworkError> so callers
 * can handle network failures and server errors as typed values.
 */

import { encode, decode } from "../codec.js";
import { Effect, Schema } from "effect";
import { HttpError, NetworkError } from "../errors.js";
import {
  CrdtGetResponse,
  CrdtOpResponse,
  TokenIssueResponse,
  ErrorResponse,
  VectorClock,
} from "../schema.js";
import type { Permissions } from "../schema.js";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface HttpClientConfig {
  /** Base URL of the Meridian server, e.g. "http://localhost:3000" */
  baseUrl: string;
  /** Bearer token. Can be updated after construction (e.g. after refresh). */
  token: string;
  /** Request timeout in ms. Default: 10_000 */
  timeoutMs?: number;
}

// ---------------------------------------------------------------------------
// HttpClient
// ---------------------------------------------------------------------------

export class HttpClient {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  token: string;

  constructor(config: HttpClientConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.token = config.token;
    this.timeoutMs = config.timeoutMs ?? 10_000;
  }

  // ---- CRDT REST ----

  /** GET /v1/namespaces/:ns/crdts/:id */
  getCrdt(ns: string, id: string): Effect.Effect<CrdtGetResponse, HttpError | NetworkError> {
    return this.request(CrdtGetResponse, "GET", `/v1/namespaces/${ns}/crdts/${id}`);
  }

  /** POST /v1/namespaces/:ns/crdts/:id/ops */
  postOp(ns: string, id: string, op: unknown): Effect.Effect<CrdtOpResponse, HttpError | NetworkError> {
    return this.request(CrdtOpResponse, "POST", `/v1/namespaces/${ns}/crdts/${id}/ops`, op);
  }

  /** GET /v1/namespaces/:ns/crdts/:id/sync?since=<base64url(msgpack(vc))> */
  syncCrdt(
    ns: string,
    id: string,
    sinceVc?: VectorClock,
  ): Effect.Effect<CrdtGetResponse, HttpError | NetworkError> {
    let path = `/v1/namespaces/${ns}/crdts/${id}/sync`;
    if (sinceVc && Object.keys(sinceVc).length > 0) {
      const bytes = encode({ entries: sinceVc });
      path += `?since=${base64urlEncode(bytes)}`;
    }
    return this.request(CrdtGetResponse, "GET", path);
  }

  /** POST /v1/namespaces/:ns/tokens  (admin-only) */
  issueToken(
    ns: string,
    opts: { client_id: number; ttl_ms: number; permissions: Permissions },
  ): Effect.Effect<TokenIssueResponse, HttpError | NetworkError> {
    return this.request(TokenIssueResponse, "POST", `/v1/namespaces/${ns}/tokens`, opts);
  }

  // ---- Internal ----

  private request<A, I>(
    responseSchema: Schema.Schema<A, I>,
    method: string,
    path: string,
    body?: unknown,
  ): Effect.Effect<A, HttpError | NetworkError> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.token}`,
      Accept: "application/msgpack",
    };

    let bodyBytes: Uint8Array | undefined;
    if (body !== undefined) {
      bodyBytes = encode(body);
      headers["Content-Type"] = "application/msgpack";
    }

    const fetchEffect = Effect.tryPromise({
      try: async () => {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeoutMs);
        try {
          const response = await fetch(url, { method, headers, body: bodyBytes, signal: controller.signal });
          const bytes = new Uint8Array(await response.arrayBuffer());
          return { response, bytes };
        } finally {
          clearTimeout(timer);
        }
      },
      catch: (e) =>
        new NetworkError({ message: e instanceof Error ? e.message : "fetch failed", cause: e }),
    });

    return fetchEffect.pipe(
      Effect.flatMap(({ response, bytes }): Effect.Effect<A, HttpError | NetworkError> => {
        if (!response.ok) {
          let errBody: ErrorResponse;
          try {
            errBody = Schema.decodeUnknownSync(ErrorResponse)(JSON.parse(new TextDecoder().decode(bytes)));
          } catch {
            errBody = { error: "unknown", message: `HTTP ${response.status}` };
          }
          return Effect.fail(new HttpError({ status: response.status, body: errBody }));
        }
        const contentType = response.headers.get("content-type") ?? "";
        const raw = contentType.includes("application/msgpack")
          ? decode(bytes)
          : JSON.parse(new TextDecoder().decode(bytes));
        return Schema.decodeUnknown(responseSchema)(raw).pipe(
          Effect.mapError((e) =>
            new NetworkError({ message: `Response schema validation failed: ${e.message}` }),
          ),
        );
      }),
    );
  }
}

// ---------------------------------------------------------------------------
// base64url (no-padding) encoder
// ---------------------------------------------------------------------------

function base64urlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
}
