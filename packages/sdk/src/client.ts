import { Effect, type Schema } from "effect";
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

export interface MeridianClientConfig {
  url: string;
  namespace: string;
  token: string;
  autoConnect?: boolean;
}

/**
 * The main entry point for the Meridian real-time CRDT SDK.
 *
 * Create an instance with the static `MeridianClient.create()` factory, then
 * use the handle methods to obtain typed CRDT handles that sync automatically
 * over WebSocket.
 *
 * @example
 * ```ts
 * import { Effect } from 'effect';
 * import { MeridianClient } from 'meridian-sdk';
 *
 * const client = await Effect.runPromise(
 *   MeridianClient.create({
 *     url: 'wss://example.com',
 *     namespace: 'my-app',
 *     token: '<JWT>',
 *   })
 * );
 *
 * const counter = client.gcounter('visitors');
 * counter.increment();
 *
 * client.close();
 * ```
 */
export class MeridianClient {
  readonly namespace: string;
  readonly clientId: number;
  readonly claims: TokenClaims;

  private readonly transport: WsTransport;
  readonly http: HttpClient;

  // HACK: Generic params are erased at storage level; factories restore them via typed get+cast on retrieval.
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
   * Creates and validates a new `MeridianClient` from the supplied configuration.
   *
   * The JWT `token` is parsed and validated synchronously inside the Effect; the
   * Effect fails with `TokenParseError` or `TokenExpiredError` if the token is
   * malformed or expired. The WebSocket connection is opened immediately unless
   * `autoConnect` is set to `false`.
   *
   * @param config - Connection configuration including the server `url`,
   *   `namespace`, and a signed `token`.
   *
   * @example
   * ```ts
   * import { Effect } from 'effect';
   * import { MeridianClient } from 'meridian-sdk';
   *
   * const client = await Effect.runPromise(
   *   MeridianClient.create({ url: 'ws://localhost:8080', namespace: 'demo', token: myToken })
   * );
   * ```
   */
  static create(
    config: MeridianClientConfig,
  ): Effect.Effect<MeridianClient, TokenParseError | TokenExpiredError> {
    return parseAndValidateToken(config.token).pipe(
      Effect.map((claims) => new MeridianClient(config, claims)),
    );
  }

  /**
   * Returns a handle for a grow-only counter (GCounter) CRDT.
   *
   * Handles are cached by `crdtId`; calling this method multiple times with the
   * same id returns the same handle instance and creates only one subscription.
   *
   * @param crdtId - Unique identifier for the CRDT within this namespace.
   *
   * @example
   * ```ts
   * const counter = client.gcounter('page-views');
   * counter.increment(5);
   * console.log(counter.value()); // 5
   * ```
   */
  gcounter(crdtId: string): GCounterHandle {
    let handle = this.gcHandles.get(crdtId);
    if (!handle) {
      handle = new GCounterHandle({ ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport });
      this.gcHandles.set(crdtId, handle);
      this.transport.subscribe(crdtId);
    }
    return handle;
  }

  /**
   * Returns a handle for a positive-negative counter (PNCounter) CRDT.
   *
   * Handles are cached by `crdtId`; calling this method multiple times with the
   * same id returns the same handle instance and creates only one subscription.
   *
   * @param crdtId - Unique identifier for the CRDT within this namespace.
   *
   * @example
   * ```ts
   * const score = client.pncounter('game-score');
   * score.increment(10);
   * score.decrement(3);
   * console.log(score.value()); // 7
   * ```
   */
  pncounter(crdtId: string): PNCounterHandle {
    let handle = this.pnHandles.get(crdtId);
    if (!handle) {
      handle = new PNCounterHandle({ ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport });
      this.pnHandles.set(crdtId, handle);
      this.transport.subscribe(crdtId);
    }
    return handle;
  }

  /**
   * Returns a handle for an Observed-Remove Set (OR-Set) CRDT.
   *
   * Handles are cached by `crdtId`; calling this method multiple times with the
   * same id returns the same handle instance and creates only one subscription.
   *
   * @param crdtId - Unique identifier for the CRDT within this namespace.
   * @param schema - Optional Effect schema used to decode elements from the wire format.
   *
   * @example
   * ```ts
   * import { Schema } from 'effect';
   *
   * const tags = client.orset('article-tags', Schema.String);
   * tags.add('typescript');
   * tags.remove('typescript');
   * ```
   */
  orset<T>(crdtId: string, schema?: Schema.Schema<T>): ORSetHandle<T> {
    let handle = this.orHandles.get(crdtId) as ORSetHandle<T> | undefined;
    if (!handle) {
      const base = { ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport };
      handle = schema ? new ORSetHandle<T>({ ...base, schema }) : new ORSetHandle<T>(base);
      this.orHandles.set(crdtId, handle as ORSetHandle<unknown>);
      this.transport.subscribe(crdtId);
    }
    return handle;
  }

  /**
   * Returns a handle for a Last-Write-Wins register (LWW-Register) CRDT.
   *
   * Handles are cached by `crdtId`; calling this method multiple times with the
   * same id returns the same handle instance and creates only one subscription.
   *
   * @param crdtId - Unique identifier for the CRDT within this namespace.
   * @param schema - Optional Effect schema used to decode the value from the wire format.
   *
   * @example
   * ```ts
   * import { Schema } from 'effect';
   *
   * const theme = client.lwwregister('ui-theme', Schema.Literal('light', 'dark'));
   * theme.set('dark');
   * console.log(theme.value()); // 'dark'
   * ```
   */
  lwwregister<T>(crdtId: string, schema?: Schema.Schema<T>): LwwRegisterHandle<T> {
    let handle = this.lwHandles.get(crdtId) as LwwRegisterHandle<T> | undefined;
    if (!handle) {
      const base = { ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport };
      handle = schema ? new LwwRegisterHandle<T>({ ...base, schema }) : new LwwRegisterHandle<T>(base);
      this.lwHandles.set(crdtId, handle as LwwRegisterHandle<unknown>);
      this.transport.subscribe(crdtId);
    }
    return handle;
  }

  /**
   * Returns a handle for a presence channel CRDT.
   *
   * Handles are cached by `crdtId`; calling this method multiple times with the
   * same id returns the same handle instance and creates only one subscription.
   *
   * @param crdtId - Unique identifier for the presence channel within this namespace.
   * @param schema - Optional Effect schema used to decode peer data from the wire format.
   *
   * @example
   * ```ts
   * import { Schema } from 'effect';
   *
   * const room = client.presence('room-1', Schema.Struct({ name: Schema.String }));
   * room.heartbeat({ name: 'Alice' }, 10_000);
   * console.log(room.online()); // [{ clientId: 1, data: { name: 'Alice' }, expiresAtMs: ... }]
   * room.leave();
   * ```
   */
  presence<T>(crdtId: string, schema?: Schema.Schema<T>): PresenceHandle<T> {
    let handle = this.prHandles.get(crdtId) as PresenceHandle<T> | undefined;
    if (!handle) {
      const base = { ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport };
      handle = schema ? new PresenceHandle<T>({ ...base, schema }) : new PresenceHandle<T>(base);
      this.prHandles.set(crdtId, handle as PresenceHandle<unknown>);
      this.transport.subscribe(crdtId);
    }
    return handle;
  }

  waitForConnected(timeoutMs = 5_000): Promise<void> {
    return this.transport.waitForConnected(timeoutMs);
  }

  /**
   * Closes the underlying WebSocket connection.
   *
   * Call this when the client is no longer needed to free resources. If you are
   * using `<MeridianProvider>` in React, the provider calls this automatically
   * on unmount.
   */
  close(): void {
    this.transport.close();
  }

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
