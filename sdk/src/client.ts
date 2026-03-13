/**
 * MeridianClient — top-level SDK entry point.
 *
 * Use `MeridianClient.create(config)` (returns Effect) to parse the token
 * and validate it before connecting.
 *
 * ```ts
 * const client = await Effect.runPromise(
 *   MeridianClient.create({ url: "ws://localhost:3000", namespace: "room", token })
 * );
 * const counter = client.gcounter("gc:page-views");
 * counter.increment();
 * counter.onChange(v => console.log("views:", v));
 * ```
 */

import { Effect, Schema } from "effect";
import { WsTransport } from "./transport/websocket.js";
import { HttpClient } from "./transport/http.js";
import { GCounterHandle } from "./crdt/gcounter.js";
import { PNCounterHandle } from "./crdt/pncounter.js";
import { ORSetHandle } from "./crdt/orset.js";
import { LwwRegisterHandle } from "./crdt/lwwregister.js";
import { PresenceHandle } from "./crdt/presence.js";
import {
  decodeGCounterDelta,
  decodePNCounterDelta,
  decodeORSetDelta,
  decodeLwwDelta,
  decodePresenceDelta,
} from "./sync/delta.js";
import { parseAndValidateToken } from "./auth/token.js";
import type { ServerMsg, TokenClaims } from "./schema.js";
import type { TokenParseError, TokenExpiredError } from "./errors.js";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface MeridianClientConfig {
  /** Base URL of the Meridian server, e.g. "http://localhost:3000" */
  url: string;
  /** Namespace to connect to. */
  namespace: string;
  /** Meridian token for this namespace. */
  token: string;
  /** If true, open the WebSocket immediately. Default: true */
  autoConnect?: boolean;
}

// ---------------------------------------------------------------------------
// MeridianClient
// ---------------------------------------------------------------------------

export class MeridianClient {
  readonly namespace: string;
  readonly clientId: number;
  readonly claims: TokenClaims;

  private readonly transport: WsTransport;
  readonly http: HttpClient;

  // Handle caches — keyed by crdt_id. Generic params erased at storage level;
  // factories restore them via typed get+cast on retrieval.
  private readonly gcHandles = new Map<string, GCounterHandle>();
  private readonly pnHandles = new Map<string, PNCounterHandle>();
  private readonly orHandles = new Map<string, ORSetHandle<unknown>>();
  private readonly lwHandles = new Map<string, LwwRegisterHandle<unknown>>();
  private readonly prHandles = new Map<string, PresenceHandle<unknown>>();

  private constructor(config: MeridianClientConfig, claims: TokenClaims) {
    this.namespace = config.namespace;
    this.claims = claims;
    this.clientId = claims.client_id;

    const httpBase = config.url
      .replace(/^wss:\/\//, "https://")
      .replace(/^ws:\/\//, "http://");
    this.http = new HttpClient({ baseUrl: httpBase, token: config.token });

    const wsUrl = `${config.url.replace(/^http/, "ws")}/v1/namespaces/${config.namespace}/connect`;
    this.transport = new WsTransport({
      url: wsUrl,
      token: config.token,
      onMessage: (msg) => { this.handleServerMsg(msg); },
    });

    if (config.autoConnect !== false) {
      this.transport.connect();
    }
  }

  /**
   * Create a MeridianClient, parsing and validating the token.
   * Returns Effect<MeridianClient, TokenParseError | TokenExpiredError>.
   */
  static create(
    config: MeridianClientConfig,
  ): Effect.Effect<MeridianClient, TokenParseError | TokenExpiredError> {
    return parseAndValidateToken(config.token).pipe(
      Effect.map((claims) => new MeridianClient(config, claims)),
    );
  }

  // ---- CRDT factory methods ----

  gcounter(crdtId: string): GCounterHandle {
    let h = this.gcHandles.get(crdtId);
    if (!h) {
      h = new GCounterHandle({ ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport });
      this.gcHandles.set(crdtId, h);
      this.transport.subscribe(crdtId);
    }
    return h;
  }

  pncounter(crdtId: string): PNCounterHandle {
    let h = this.pnHandles.get(crdtId);
    if (!h) {
      h = new PNCounterHandle({ ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport });
      this.pnHandles.set(crdtId, h);
      this.transport.subscribe(crdtId);
    }
    return h;
  }

  orset<T>(crdtId: string, schema?: Schema.Schema<T>): ORSetHandle<T> {
    let h = this.orHandles.get(crdtId) as ORSetHandle<T> | undefined;
    if (!h) {
      const base = { ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport };
      h = schema ? new ORSetHandle<T>({ ...base, schema }) : new ORSetHandle<T>(base);
      this.orHandles.set(crdtId, h as ORSetHandle<unknown>);
      this.transport.subscribe(crdtId);
    }
    return h;
  }

  lwwregister<T>(crdtId: string, schema?: Schema.Schema<T>): LwwRegisterHandle<T> {
    let h = this.lwHandles.get(crdtId) as LwwRegisterHandle<T> | undefined;
    if (!h) {
      const base = { ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport };
      h = schema ? new LwwRegisterHandle<T>({ ...base, schema }) : new LwwRegisterHandle<T>(base);
      this.lwHandles.set(crdtId, h as LwwRegisterHandle<unknown>);
      this.transport.subscribe(crdtId);
    }
    return h;
  }

  presence<T>(crdtId: string, schema?: Schema.Schema<T>): PresenceHandle<T> {
    let h = this.prHandles.get(crdtId) as PresenceHandle<T> | undefined;
    if (!h) {
      const base = { ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport };
      h = schema ? new PresenceHandle<T>({ ...base, schema }) : new PresenceHandle<T>(base);
      this.prHandles.set(crdtId, h as PresenceHandle<unknown>);
      this.transport.subscribe(crdtId);
    }
    return h;
  }

  // ---- Lifecycle ----

  /** Resolves when the WebSocket is connected and ready. */
  waitForConnected(timeoutMs = 5_000): Promise<void> {
    return this.transport.waitForConnected(timeoutMs);
  }

  close(): void {
    this.transport.close();
  }

  // ---- Internal: route ServerMsg.Delta to the right handle ----

  private handleServerMsg(msg: ServerMsg): void {
    if (!("Delta" in msg)) return;
    const { crdt_id, delta_bytes } = msg.Delta;

    const gcHandle = this.gcHandles.get(crdt_id);
    if (gcHandle) {
      try { gcHandle.applyDelta(decodeGCounterDelta(delta_bytes)); } catch { /* stale */ }
      return;
    }
    const pnHandle = this.pnHandles.get(crdt_id);
    if (pnHandle) {
      try { pnHandle.applyDelta(decodePNCounterDelta(delta_bytes)); } catch { /* stale */ }
      return;
    }
    const orHandle = this.orHandles.get(crdt_id);
    if (orHandle) {
      try { orHandle.applyDelta(decodeORSetDelta(delta_bytes)); } catch { /* stale */ }
      return;
    }
    const lwHandle = this.lwHandles.get(crdt_id);
    if (lwHandle) {
      try { lwHandle.applyDelta(decodeLwwDelta(delta_bytes)); } catch { /* stale */ }
      return;
    }
    const prHandle = this.prHandles.get(crdt_id);
    if (prHandle) {
      try { prHandle.applyDelta(decodePresenceDelta(delta_bytes)); } catch { /* stale */ }
    }
  }
}
