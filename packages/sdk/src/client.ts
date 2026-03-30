import { Effect, type Schema } from "effect";
import type { QuerySpec, QueryResult, LiveQueryResult } from "./schema.js";
import type { WsState } from "./transport/websocket.js";
import { WsTransport } from "./transport/websocket.js";
import type { CrdtMapValue } from "./crdt/crdtmap.js";
import { HttpClient } from "./transport/http.js";
import { GCounterHandle } from "./crdt/gcounter.js";
import { PNCounterHandle } from "./crdt/pncounter.js";
import { ORSetHandle } from "./crdt/orset.js";
import { LwwRegisterHandle } from "./crdt/lwwregister.js";
import { PresenceHandle } from "./crdt/presence.js";
import { CRDTMapHandle } from "./crdt/crdtmap.js";
import { AwarenessHandle } from "./crdt/awareness.js";
import { RGAHandle } from "./crdt/rga.js";
import { TreeHandle } from "./crdt/tree.js";
import type { CrdtValidator } from "./validation/index.js";
import {
  decodeGCounterDelta,
  decodePNCounterDelta,
  decodeORSetDelta,
  decodeLwwDelta,
  decodePresenceDelta,
  decodeCRDTMapDelta,
  decodeRGADelta,
  decodeTreeDelta,
} from "./sync/delta.js";
import type { TreeNodeValue } from "./sync/delta.js";
import { parseAndValidateToken } from "./auth/token.js";
import { canRead as evalCanRead, canWrite as evalCanWrite } from "./auth/permissions.js";
import { decode as msgpackDecode } from "./codec.js";
import type { OpMask } from "./auth/permissions.js";
import type { ServerMsg, TokenClaims } from "./schema.js";
import type { TokenParseError, TokenExpiredError } from "./errors.js";

// --- Devtools snapshot types ---

export interface GCounterSnapshotEntry {
  type: "gcounter";
  crdtId: string;
  value: number;
  counts: Readonly<Record<string, number>>;
}
export interface PNCounterSnapshotEntry {
  type: "pncounter";
  crdtId: string;
  value: number;
}
export interface ORSetSnapshotEntry {
  type: "orset";
  crdtId: string;
  elements: unknown[];
}
export interface LwwRegisterSnapshotEntry {
  type: "lwwregister";
  crdtId: string;
  value: unknown;
  meta: { updatedAtMs: number; author: number } | null;
}
export interface PresenceSnapshotEntry {
  type: "presence";
  crdtId: string;
  online: { clientId: number; data: unknown; expiresAtMs: number }[];
}
export interface CRDTMapSnapshotEntry {
  type: "crdtmap";
  crdtId: string;
  value: Readonly<CrdtMapValue>;
}
export interface RGASnapshotEntry {
  type: "rga";
  crdtId: string;
  text: string;
}
export interface TreeSnapshotEntry {
  type: "tree";
  crdtId: string;
  roots: TreeNodeValue[];
}
export type CRDTSnapshotEntry =
  | GCounterSnapshotEntry
  | PNCounterSnapshotEntry
  | ORSetSnapshotEntry
  | LwwRegisterSnapshotEntry
  | PresenceSnapshotEntry
  | CRDTMapSnapshotEntry
  | RGASnapshotEntry
  | TreeSnapshotEntry;

/**
 * A handle returned by `client.liveQuery()`. Receives pushed `LiveQueryResult`
 * frames whenever matching CRDTs change.
 */
export interface LiveQueryHandle {
  /** Register a listener for live query result pushes. Returns an unsubscribe fn. */
  onResult(listener: (result: LiveQueryResult) => void): () => void;
  /** Cancel the subscription and clean up. */
  close(): void;
}

export interface DeltaEvent {
  crdtId: string;
  type: CRDTSnapshotEntry["type"];
  at: number; // Date.now()
}

export interface ClientSnapshot {
  namespace: string;
  clientId: number;
  wsState: WsState;
  pendingOpCount: number;
  crdts: CRDTSnapshotEntry[];
}

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
  private readonly cmHandles = new Map<string, CRDTMapHandle>();
  private readonly awHandles = new Map<string, AwarenessHandle<unknown>>();
  private readonly rgaHandles = new Map<string, RGAHandle>();
  private readonly treeHandles = new Map<string, TreeHandle>();

  private readonly anyListeners = new Set<() => void>();
  private readonly deltaListeners = new Set<(event: DeltaEvent) => void>();
  private readonly handleUnsubs: Array<() => void> = [];

  // Live query subscriptions: query_id → { spec, listeners }
  private readonly liveQueries = new Map<string, {
    spec: QuerySpec;
    listeners: Set<(result: LiveQueryResult) => void>;
  }>();
  private liveQueryCounter = 0;

  // RPC frame listeners — registered by createMeridianRpc
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly rpcListeners = new Set<(frame: any) => void>();

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

    // Internal listener: routes transport state changes through onAnyChange
    // so devtools only needs one subscription (avoids WsTransport chaining issue).
    this.transport.onStateChange(() => { this.notifyAnyChange(); });

    // Re-send all active SubscribeQuery frames after each reconnect so the
    // server-side registry is restored and live queries keep firing.
    this.transport.onReconnect(() => { this.resubscribeLiveQueries(); });
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
      this.handleUnsubs.push(handle.onChange(() => { this.notifyAnyChange(); }));
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
      this.handleUnsubs.push(handle.onChange(() => { this.notifyAnyChange(); }));
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
      this.handleUnsubs.push(handle.onChange(() => { this.notifyAnyChange(); }));
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
      this.handleUnsubs.push(handle.onChange(() => { this.notifyAnyChange(); }));
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
      this.handleUnsubs.push(handle.onChange(() => { this.notifyAnyChange(); }));
    }
    return handle;
  }

  /**
   * Returns a handle for a CRDTMap — a map of named CRDT values.
   *
   * Handles are cached by `crdtId`; calling this method multiple times with the
   * same id returns the same handle instance and creates only one subscription.
   *
   * @param crdtId - Unique identifier for the CRDT within this namespace.
   *
   * @example
   * ```ts
   * const doc = client.crdtmap('document-1');
   * doc.lwwSet('title', 'Hello World');
   * doc.incrementCounter('views');
   * console.log(doc.value()); // { title: { value: 'Hello World', ... }, views: { total: 1, ... } }
   * ```
   */
  crdtmap(crdtId: string): CRDTMapHandle {
    let handle = this.cmHandles.get(crdtId);
    if (!handle) {
      handle = new CRDTMapHandle({ ns: this.namespace, crdtId, clientId: this.clientId, transport: this.transport });
      this.cmHandles.set(crdtId, handle);
      this.transport.subscribe(crdtId);
      this.handleUnsubs.push(handle.onChange(() => { this.notifyAnyChange(); }));
    }
    return handle;
  }

  /**
   * Returns a handle for an RGA (Replicated Growable Array) CRDT — collaborative text editing.
   *
   * Handles are cached by `crdtId`; calling this method multiple times with the
   * same id returns the same handle instance and creates only one subscription.
   *
   * @param crdtId - Unique identifier for the CRDT within this namespace.
   *
   * @example
   * ```ts
   * const doc = client.rga('doc:content');
   * doc.insert(0, 'Hello');
   * doc.onChange(text => console.log(text));
   * ```
   */
  rga(crdtId: string, opts?: { validator?: CrdtValidator }): RGAHandle {
    let handle = this.rgaHandles.get(crdtId);
    if (!handle) {
      handle = new RGAHandle({ crdtId, clientId: this.clientId, transport: this.transport, ...(opts?.validator !== undefined ? { validator: opts.validator } : {}) });
      this.rgaHandles.set(crdtId, handle);
      this.transport.subscribe(crdtId);
      this.handleUnsubs.push(handle.onChange(() => { this.notifyAnyChange(); }));
    }
    return handle;
  }

  /**
   * Returns a handle for a TreeCRDT — a convergent hierarchical tree.
   *
   * Use this for outlines, document trees, mind maps, or any nested structure
   * that needs concurrent editing. Concurrent moves use Kleppmann (2021)
   * move semantics: cycle-creating moves are discarded, all replicas converge.
   *
   * Handles are cached by `crdtId`; the same handle is returned for repeated calls.
   *
   * @param crdtId - Logical CRDT identifier (e.g. `"tree:outline"`).
   *
   * @example
   * ```ts
   * const tree = client.tree('doc:outline');
   * const rootId = tree.addNode(null, 'a0', 'Introduction');
   * const childId = tree.addNode(rootId, 'a0', 'Chapter 1');
   * tree.onChange(t => console.log(t.roots));
   * ```
   */
  tree(crdtId: string, opts?: { validator?: CrdtValidator }): TreeHandle {
    let handle = this.treeHandles.get(crdtId);
    if (!handle) {
      handle = new TreeHandle({ crdtId, clientId: this.clientId, transport: this.transport, ...(opts?.validator !== undefined ? { validator: opts.validator } : {}) });
      this.treeHandles.set(crdtId, handle);
      this.transport.subscribe(crdtId);
      this.handleUnsubs.push(handle.onChange(() => { this.notifyAnyChange(); }));
    }
    return handle;
  }

  /**
   * Returns a handle for an ephemeral awareness channel.
   *
   * Awareness updates are fanned out to all other subscribers in the namespace
   * in real time but are **not** persisted. Use this for high-frequency,
   * transient UI state like cursor positions or "is typing" indicators.
   *
   * Handles are cached by `key`; the same handle is returned for repeated calls
   * with the same key.
   *
   * @param key    - Logical channel name (e.g. `"cursors"`, `"selection:doc-1"`).
   * @param schema - Optional Effect schema used to decode peer payloads at runtime.
   *
   * @example
   * ```ts
   * import { Schema } from 'effect';
   *
   * const CursorSchema = Schema.Struct({ x: Schema.Number, y: Schema.Number });
   * const cursors = client.awareness('cursors', CursorSchema);
   * cursors.update({ x: 42, y: 100 });
   * cursors.onChange(peers => console.log(peers));
   * ```
   */
  awareness<T = unknown>(key: string, schema?: Schema.Schema<T>): AwarenessHandle<T> {
    const existing = this.awHandles.get(key);
    if (existing) return existing as AwarenessHandle<T>;
    const handle = new AwarenessHandle<T>(key, this.transport, this.clientId, schema);
    this.awHandles.set(key, handle as AwarenessHandle<unknown>);
    this.handleUnsubs.push(handle.onChange(() => { this.notifyAnyChange(); }));
    return handle;
  }

  waitForConnected(timeoutMs = 5_000): Promise<void> {
    return this.transport.waitForConnected(timeoutMs);
  }

  /** Number of ops buffered locally, waiting to be sent on reconnect. */
  get pendingOpCount(): number {
    return this.transport.pendingOpCount;
  }

  /** P50 and P99 op round-trip latency in ms. Returns null if not enough samples. */
  getLatencyStats(): { p50: number; p99: number; count: number } | null {
    return this.transport.getLatencyStats();
  }

  /**
   * Subscribe to connection state changes. Returns an unsubscribe function.
   * Useful for building "syncing" indicators in the UI.
   */
  onStateChange(listener: (state: WsState) => void): () => void {
    return this.transport.onStateChange(listener);
  }

  /**
   * Register a listener for incoming RPC frames (used by `createMeridianRpc`).
   * Frames arrive as `AwarenessBroadcast` messages with key `"__rpc__"`.
   * Returns an unsubscribe function.
   * @internal
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onRpcFrame(listener: (frame: any) => void): () => void {
    this.rpcListeners.add(listener);
    return () => { this.rpcListeners.delete(listener); };
  }

  /**
   * Send a raw msgpack-encoded `ClientMsg` over the WebSocket transport.
   * Used by `createMeridianRpc` to send typed RPC frames.
   * @internal
   */
  sendRpcFrame(data: Uint8Array): void {
    this.transport.send({ AwarenessUpdate: { key: "__rpc__", data } });
  }

  /**
   * Subscribe to any state change (CRDT value or connection state).
   * Fires whenever any handle emits or the WebSocket transitions.
   * Used by `<MeridianDevtools>` to avoid multiple `onStateChange` subscriptions.
   */
  onAnyChange(listener: () => void): () => void {
    this.anyListeners.add(listener);
    return () => { this.anyListeners.delete(listener); };
  }

  /**
   * Subscribe to incoming CRDT deltas from the server.
   * Fires after each delta is applied to the local handle.
   * Intended for devtools — not for production data flows.
   */
  onDelta(listener: (event: DeltaEvent) => void): () => void {
    this.deltaListeners.add(listener);
    return () => { this.deltaListeners.delete(listener); };
  }

  /**
   * Returns a point-in-time snapshot of all active CRDT handles and transport state.
   * Intended for devtools — not for production data flows.
   */
  snapshot(): ClientSnapshot {
    const crdts: CRDTSnapshotEntry[] = [];
    for (const [crdtId, h] of this.gcHandles)
      crdts.push({ type: "gcounter", crdtId, value: h.value(), counts: h.counts() });
    for (const [crdtId, h] of this.pnHandles)
      crdts.push({ type: "pncounter", crdtId, value: h.value() });
    for (const [crdtId, h] of this.orHandles)
      crdts.push({ type: "orset", crdtId, elements: h.elements() as unknown[] });
    for (const [crdtId, h] of this.lwHandles)
      crdts.push({ type: "lwwregister", crdtId, value: h.value(), meta: h.meta() });
    for (const [crdtId, h] of this.prHandles)
      crdts.push({ type: "presence", crdtId, online: h.online() as { clientId: number; data: unknown; expiresAtMs: number }[] });
    for (const [crdtId, h] of this.cmHandles)
      crdts.push({ type: "crdtmap", crdtId, value: h.value() });
    for (const [crdtId, h] of this.rgaHandles)
      crdts.push({ type: "rga", crdtId, text: h.value() });
    for (const [crdtId, h] of this.treeHandles)
      crdts.push({ type: "tree", crdtId, roots: h.value().roots });
    return {
      namespace: this.namespace,
      clientId: this.clientId,
      wsState: this.transport.currentState,
      pendingOpCount: this.transport.pendingOpCount,
      crdts,
    };
  }

  /**
   * Closes the underlying WebSocket connection.
   *
   * Call this when the client is no longer needed to free resources. If you are
   * using `<MeridianProvider>` in React, the provider calls this automatically
   * on unmount.
   */

  /**
   * Returns `true` if the token's permissions allow reading `crdtId`.
   *
   * Evaluated locally against the parsed claims — no network round-trip.
   * Useful for disabling UI elements before attempting an op that would fail.
   *
   * @example
   * ```ts
   * if (!client.canRead("or:cart-42")) showLockIcon();
   * ```
   */
  canRead(crdtId: string): boolean {
    return evalCanRead(this.claims.permissions, crdtId, this.clientId);
  }

  /**
   * Returns `true` if the token's permissions allow writing `crdtId`.
   *
   * Pass an optional `opMask` to check op-level access (V2 tokens only).
   * Without `opMask`, checks key-level write access.
   *
   * @example
   * ```ts
   * import { OpMasks } from "meridian-sdk";
   *
   * if (!client.canWrite("or:cart-42", OpMasks.OR_ADD)) disableAddButton();
   * if (!client.canWrite("gc:views")) disableIncrementButton();
   * ```
   */
  canWrite(crdtId: string, opMask?: OpMask): boolean {
    return evalCanWrite(this.claims.permissions, crdtId, this.clientId, opMask);
  }

  close(): void {
    for (const unsub of this.handleUnsubs) unsub();
    this.handleUnsubs.length = 0;
    this.anyListeners.clear();
    this.deltaListeners.clear();
    for (const queryId of this.liveQueries.keys()) {
      this.transport.send({ UnsubscribeQuery: { query_id: queryId } });
    }
    this.liveQueries.clear();
    this.transport.close();
  }

  /** Re-opens the WebSocket after a `close()`. Used by MeridianProvider to survive React StrictMode double-mount. */
  reopen(): void {
    this.transport.reopen();
  }

  /**
   * Execute a cross-CRDT query against the namespace.
   *
   * @example
   * ```ts
   * const result = await client.query({ from: "gc:views-*", aggregate: "sum" });
   * console.log(result.value); // total view count
   * ```
   */
  query(spec: QuerySpec): Promise<QueryResult> {
    return Effect.runPromise(this.http.query(this.namespace, spec));
  }

  /**
   * Subscribe to a live cross-CRDT query over WebSocket.
   * The server re-executes the query and pushes a result whenever a matching CRDT changes.
   *
   * @example
   * ```ts
   * const handle = client.liveQuery({ from: "gc:views-*", aggregate: "sum" });
   * handle.onResult(result => console.log("live total:", result.value));
   * // later:
   * handle.close();
   * ```
   */
  liveQuery(spec: QuerySpec): LiveQueryHandle {
    const queryId = `lq-${++this.liveQueryCounter}`;
    const listeners = new Set<(result: LiveQueryResult) => void>();
    this.liveQueries.set(queryId, { spec, listeners });
    this.sendSubscribeQuery(queryId, spec);

    return {
      onResult: (listener) => {
        listeners.add(listener);
        return () => { listeners.delete(listener); };
      },
      close: () => {
        this.transport.send({ UnsubscribeQuery: { query_id: queryId } });
        this.liveQueries.delete(queryId);
      },
    };
  }

  private sendSubscribeQuery(queryId: string, spec: QuerySpec): void {
    this.transport.send({
      SubscribeQuery: {
        query_id: queryId,
        query: {
          from: spec.from,
          ...(spec.type !== undefined && { type: spec.type }),
          aggregate: spec.aggregate,
          ...(spec.where !== undefined && {
            where: {
              ...(spec.where.contains !== undefined && { contains: spec.where.contains }),
              ...(spec.where.updatedAfter !== undefined && { updated_after: spec.where.updatedAfter }),
            },
          }),
        },
      },
    });
  }

  private resubscribeLiveQueries(): void {
    for (const [queryId, { spec }] of this.liveQueries) {
      this.sendSubscribeQuery(queryId, spec);
    }
  }

  private notifyAnyChange(): void {
    for (const fn of this.anyListeners) fn();
  }

  private handleServerMsg(msg: ServerMsg): void {
    if ("AwarenessBroadcast" in msg) {
      const { client_id, key, data } = msg.AwarenessBroadcast;
      // Dispatch RPC frames — these are AwarenessUpdates with a "__rpc__" key.
      if (key === "__rpc__" && this.rpcListeners.size > 0) {
        try {
          const frame = msgpackDecode(data);
          for (const fn of this.rpcListeners) fn(frame);
        } catch { /* malformed frame — drop silently */ }
        return;
      }
      this.awHandles.get(key)?.applyBroadcast(client_id, data);
      return;
    }

    if ("QueryResult" in msg) {
      const { query_id, value, matched } = msg.QueryResult;
      const entry = this.liveQueries.get(query_id);
      if (entry) {
        for (const fn of entry.listeners) fn({ value, matched });
      }
      return;
    }

    if (!("Delta" in msg)) return;
    const { crdt_id, delta_bytes } = msg.Delta;

    const gcHandle = this.gcHandles.get(crdt_id);
    if (gcHandle) {
      try { gcHandle.applyDelta(decodeGCounterDelta(delta_bytes)); } catch { /* stale */ }
      this.notifyDelta(crdt_id, "gcounter");
      return;
    }
    const pnHandle = this.pnHandles.get(crdt_id);
    if (pnHandle) {
      try { pnHandle.applyDelta(decodePNCounterDelta(delta_bytes)); } catch { /* stale */ }
      this.notifyDelta(crdt_id, "pncounter");
      return;
    }
    const orHandle = this.orHandles.get(crdt_id);
    if (orHandle) {
      try { orHandle.applyDelta(decodeORSetDelta(delta_bytes)); } catch { /* stale */ }
      this.notifyDelta(crdt_id, "orset");
      return;
    }
    const lwHandle = this.lwHandles.get(crdt_id);
    if (lwHandle) {
      try { lwHandle.applyDelta(decodeLwwDelta(delta_bytes)); } catch { /* stale */ }
      this.notifyDelta(crdt_id, "lwwregister");
      return;
    }
    const prHandle = this.prHandles.get(crdt_id);
    if (prHandle) {
      try { prHandle.applyDelta(decodePresenceDelta(delta_bytes)); } catch { /* stale */ }
      this.notifyDelta(crdt_id, "presence");
      return;
    }
    const cmHandle = this.cmHandles.get(crdt_id);
    if (cmHandle) {
      try { cmHandle.applyDelta(decodeCRDTMapDelta(delta_bytes)); } catch { /* stale */ }
      this.notifyDelta(crdt_id, "crdtmap");
      return;
    }
    const rgaHandle = this.rgaHandles.get(crdt_id);
    if (rgaHandle) {
      try { rgaHandle.applyDelta(decodeRGADelta(delta_bytes)); } catch { /* stale */ }
      this.notifyDelta(crdt_id, "rga");
      return;
    }
    const treeHandle = this.treeHandles.get(crdt_id);
    if (treeHandle) {
      try { treeHandle.applyDelta(decodeTreeDelta(delta_bytes)); } catch { /* stale */ }
      this.notifyDelta(crdt_id, "tree");
    }
  }

  private notifyDelta(crdtId: string, type: CRDTSnapshotEntry["type"]): void {
    if (this.deltaListeners.size === 0) return;
    const event: DeltaEvent = { crdtId, type, at: Date.now() };
    for (const fn of this.deltaListeners) fn(event);
  }
}
