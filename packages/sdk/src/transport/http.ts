import { encode, decode } from "../codec.js";
import { Effect, Schema } from "effect";
import { HttpError, NetworkError } from "../errors.js";
import {
  CrdtGetResponse,
  CrdtOpResponse,
  TokenIssueResponse,
  ErrorResponse,
  QueryResult,
  type VectorClock,
  type QuerySpec,
} from "../schema.js";
import type { Permissions, PermissionsV2, TokenClaims } from "../schema.js";
import { TokenClaims as TokenClaimsSchema } from "../schema.js";

export interface HttpClientConfig {
  baseUrl: string;
  token: string;
  timeoutMs?: number;
}

export interface HistoryEntry {
  seq: number;
  timestamp_ms: number;
  op: unknown;
}

export interface HistoryResponse {
  crdt_id: string;
  entries: HistoryEntry[];
  next_seq: number | null;
}

export class HttpClient {
  readonly baseUrl: string;
  private readonly timeoutMs: number;
  token: string;

  constructor(config: HttpClientConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.token = config.token;
    this.timeoutMs = config.timeoutMs ?? 10_000;
  }

  getCrdt(ns: string, id: string): Effect.Effect<CrdtGetResponse, HttpError | NetworkError> {
    return this.request(CrdtGetResponse, "GET", `/v1/namespaces/${ns}/crdts/${id}`);
  }

  postOp(ns: string, id: string, op: unknown): Effect.Effect<CrdtOpResponse, HttpError | NetworkError> {
    return this.request(CrdtOpResponse, "POST", `/v1/namespaces/${ns}/crdts/${id}/ops`, op);
  }

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

  getHistory(ns: string, id: string, sinceSeq = 0, limit = 50): Promise<HistoryResponse> {
    const url = `${this.baseUrl}/v1/namespaces/${ns}/crdts/${encodeURIComponent(id)}/history?since_seq=${sinceSeq}&limit=${limit}`;
    return fetch(url, { headers: { Authorization: `Bearer ${this.token}` } }).then(r => r.json() as Promise<HistoryResponse>);
  }

  /**
   * Issue a new token. Requires admin permission.
   *
   * Pass `permissions` for V1 (glob lists) or `rules` for V2 (fine-grained).
   * `rules` takes precedence if both are provided.
   */
  issueToken(
    ns: string,
    opts: {
      client_id: number;
      ttl_ms?: number;
      /** V1 glob-list permissions. Mutually exclusive with `rules`. */
      permissions?: Permissions;
      /** V2 fine-grained rules. Takes precedence over `permissions`. */
      rules?: Pick<PermissionsV2, "r" | "w" | "admin" | "rl">;
    },
  ): Effect.Effect<TokenIssueResponse, HttpError | NetworkError> {
    return this.requestJson(TokenIssueResponse, "POST", `/v1/namespaces/${ns}/tokens`, opts);
  }

  /**
   * Decode the caller's own token and return its claims.
   *
   * Useful for debugging — verifies exactly what permissions and TTL the
   * current token carries without decoding msgpack manually.
   */
  tokenMe(ns: string): Effect.Effect<TokenClaims, HttpError | NetworkError> {
    return this.requestJson(TokenClaimsSchema, "GET", `/v1/namespaces/${ns}/tokens/me`);
  }

  query(ns: string, spec: QuerySpec): Effect.Effect<QueryResult, HttpError | NetworkError> {
    return this.requestJson(QueryResult, "POST", `/v1/namespaces/${ns}/query`, spec);
  }

  private requestJson<A, I>(
    responseSchema: Schema.Schema<A, I>,
    method: string,
    path: string,
    body?: unknown,
  ): Effect.Effect<A, HttpError | NetworkError> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.token}`,
    };

    let bodyStr: string | undefined;
    if (body !== undefined) {
      bodyStr = JSON.stringify(body);
      headers["Content-Type"] = "application/json";
    }

    const fetchEffect = Effect.tryPromise({
      try: async () => {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeoutMs);
        try {
          const response = await fetch(url, { method, headers, body: bodyStr, signal: controller.signal });
          const text = await response.text();
          return { response, text };
        } finally {
          clearTimeout(timer);
        }
      },
      catch: (e) =>
        new NetworkError({ message: e instanceof Error ? e.message : "fetch failed", cause: e }),
    });

    return fetchEffect.pipe(
      Effect.flatMap(({ response, text }): Effect.Effect<A, HttpError | NetworkError> => {
        if (!response.ok) {
          let errBody: ErrorResponse;
          try {
            errBody = Schema.decodeUnknownSync(ErrorResponse)(JSON.parse(text));
          } catch {
            errBody = { error: "unknown", message: `HTTP ${response.status}` };
          }
          return Effect.fail(new HttpError({ status: response.status, body: errBody }));
        }
        const raw = JSON.parse(text);
        return Schema.decodeUnknown(responseSchema)(raw).pipe(
          Effect.mapError((e) =>
            new NetworkError({ message: `Response schema validation failed: ${e.message}` }),
          ),
        );
      }),
    );
  }

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

const base64urlEncode = (bytes: Uint8Array): string => {
  let binary = "";
  for (const b of bytes) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
};
