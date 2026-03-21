import {
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Option,
  Queue,
  Schedule,
  Duration,
} from "effect";
import { encodeClientMsg, decodeServerMsg, encodeVectorClock } from "../codec.js";
import type { ServerMsg, VectorClock } from "../schema.js";
import {
  BACKOFF_INITIAL_MS,
  BACKOFF_MAX_MS,
  BACKOFF_MULTIPLIER,
  DEFAULT_TIMEOUT_MS,
  OFFLINE_QUEUE_MAX,
} from "../constants.js";
import { TransportError } from "../errors.js";

export type WsState =
  | "DISCONNECTED"
  | "CONNECTING"
  | "CONNECTED"
  | "CLOSING";

export interface WsTransportConfig {
  url: string;
  token: string;
  onMessage: (msg: ServerMsg) => void;
  onStateChange?: (state: WsState) => void;
  maxBackoffMs?: number;
}

type ClientMsg = Parameters<typeof encodeClientMsg>[0];

export class WsTransport {
  private readonly config: WsTransportConfig;
  private readonly maxBackoffMs: number;

  private ws: WebSocket | null = null;
  private state: WsState = "DISCONNECTED";
  private closed = false;

  // Effect runtime owned by this transport
  private readonly runtime = ManagedRuntime.make(Layer.empty);
  private lifecycleFiber: Fiber.RuntimeFiber<void, never> | null = null;

  // State change listeners (replacing fragile function-chain pattern)
  private readonly stateListeners = new Set<(state: WsState) => void>();

  private readonly subscriptions = new Map<string, VectorClock>();

  // Effect sliding queue — drops oldest when full (same behaviour as previous array)
  private readonly pendingQueue: Queue.Queue<ClientMsg>;

  constructor(config: WsTransportConfig) {
    this.config = config;
    this.maxBackoffMs = config.maxBackoffMs ?? BACKOFF_MAX_MS;

    // Queue creation is synchronous
    this.pendingQueue = Effect.runSync(Queue.sliding<ClientMsg>(OFFLINE_QUEUE_MAX));

    // Register initial onStateChange listener if provided
    if (config.onStateChange) {
      this.stateListeners.add(config.onStateChange);
    }
  }

  connect(): void {
    if (this.closed) return;
    this.closed = false;
    this.transitionTo("CONNECTING");
    const ws = this.makeWs();
    this.lifecycleFiber = this.runtime.runFork(this.buildLifecycleEffect(ws));
  }

  close(): void {
    this.closed = true;
    // Drain synchronously so pendingOpCount returns 0 immediately after close()
    Effect.runSync(Queue.takeAll(this.pendingQueue));
    this.runtime.runFork(Queue.shutdown(this.pendingQueue));
    this.transitionTo("CLOSING");
    if (this.lifecycleFiber !== null) {
      const fiber = this.lifecycleFiber;
      this.lifecycleFiber = null;
      this.runtime.runFork(Fiber.interrupt(fiber));
    }
    this.ws?.close(1000, "client close");
    this.ws = null;
    void this.runtime.dispose();
  }

  reopen(): void {
    if (this.lifecycleFiber !== null) {
      const fiber = this.lifecycleFiber;
      this.lifecycleFiber = null;
      this.runtime.runFork(Fiber.interrupt(fiber));
    }
    const old = this.ws;
    this.ws = null;
    old?.close(1000, "reopen");
    this.closed = false;
    this.transitionTo("CONNECTING");
    this.lifecycleFiber = this.runtime.runFork(this.buildLifecycleEffect(this.makeWs()));
  }

  subscribe(crdtId: string, sinceVc: VectorClock = {}): void {
    this.subscriptions.set(crdtId, sinceVc);
    if (this.state === "CONNECTED") {
      this.sendSubscribe(crdtId, sinceVc);
    }
  }

  updateClock(crdtId: string, vc: VectorClock): void {
    this.subscriptions.set(crdtId, vc);
  }

  send(msg: ClientMsg): void {
    if (this.state !== "CONNECTED" || this.ws === null) {
      // sliding queue drops oldest automatically if full
      this.runtime.runFork(Queue.offer(this.pendingQueue, msg));
      return;
    }
    this.ws.send(encodeClientMsg(msg));
  }

  get pendingOpCount(): number {
    try {
      return Effect.runSync(Queue.size(this.pendingQueue));
    } catch {
      return 0;
    }
  }

  /**
   * Subscribe to connection state changes. Returns an unsubscribe function.
   * Compatible with React's `useSyncExternalStore` subscribe parameter.
   */
  onStateChange(listener: (state: WsState) => void): () => void {
    this.stateListeners.add(listener);
    return () => { this.stateListeners.delete(listener); };
  }

  get currentState(): WsState {
    return this.state;
  }

  waitForConnected(timeoutMs = DEFAULT_TIMEOUT_MS): Promise<void> {
    if (this.state === "CONNECTED") return Promise.resolve();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.stateListeners.delete(listener);
        reject(new Error("WsTransport: connect timeout"));
      }, timeoutMs);
      const listener = (s: WsState) => {
        if (s === "CONNECTED") {
          clearTimeout(timer);
          this.stateListeners.delete(listener);
          resolve();
        }
      };
      this.stateListeners.add(listener);
    });
  }

  // ---------------------------------------------------------------------------
  // Effect lifecycle
  // ---------------------------------------------------------------------------

  private makeWs(): {
    ws: WebSocket;
    connected: Promise<WebSocket>;
    cancel: () => void;
  } {
    const url = `${this.config.url}${this.config.url.includes("?") ? "&" : "?"}token=${encodeURIComponent(this.config.token)}`;
    const ws = new WebSocket(url);
    ws.binaryType = "arraybuffer";

    // Register open/error listeners synchronously so they fire even if the
    // Fiber hasn't started yet (runFork is async).
    let resolve!: (ws: WebSocket) => void;
    let reject!: (e: TransportError) => void;
    const connected = new Promise<WebSocket>((res, rej) => { resolve = res; reject = rej; });

    const onOpen = () => {
      this.ws = ws;
      this.transitionTo("CONNECTED");
      this.resubscribeAll();
      resolve(ws);
    };
    const onError = () => {
      reject(new TransportError({ message: "WebSocket connection failed" }));
    };
    ws.addEventListener("open", onOpen, { once: true });
    ws.addEventListener("error", onError, { once: true });

    const cancel = () => {
      ws.removeEventListener("open", onOpen);
      ws.removeEventListener("error", onError);
      ws.close(1000, "fiber interrupt");
    };
    return { ws, connected, cancel };
  }

  private buildLifecycleEffect(wsHandle: ReturnType<WsTransport["makeWs"]>): Effect.Effect<void, never> {
    const { ws, connected, cancel } = wsHandle;

    const connectOnce = Effect.async<WebSocket, TransportError>((resume) => {
      if (this.closed) {
        cancel();
        resume(Effect.fail(new TransportError({ message: "transport closed" })));
        return Effect.void;
      }

      // connected already has the listeners; just await its outcome
      connected.then(
        (w) => resume(Effect.succeed(w)),
        () => resume(Effect.fail(new TransportError({ message: "WebSocket connection failed" }))),
      );

      // Finalizer — called if Fiber is interrupted before open
      return Effect.sync(cancel);
    });

    const handleConnection = (_ws: WebSocket): Effect.Effect<void, TransportError> =>
      Effect.async((resume) => {
        const onMessage = (event: MessageEvent) => {
          const bytes = new Uint8Array(event.data as ArrayBuffer);
          Effect.runPromise(decodeServerMsg(bytes)).then(
            (msg) => { this.config.onMessage(msg); },
            (e) => { console.warn("[meridian] failed to decode server message", e); },
          );
        };
        const onClose = () => {
          this.ws = null;
          resume(Effect.fail(new TransportError({ message: "WebSocket closed" })));
        };

        ws.addEventListener("message", onMessage);
        ws.addEventListener("close", onClose, { once: true });

        // Finalizer — clean up listeners if Fiber is interrupted
        return Effect.sync(() => {
          ws.removeEventListener("message", onMessage);
          ws.removeEventListener("close", onClose);
          ws.close(1000, "fiber interrupt");
        });
      });

    const reconnectSchedule = Schedule.exponential(
      Duration.millis(BACKOFF_INITIAL_MS),
      BACKOFF_MULTIPLIER,
    ).pipe(
      Schedule.map((d) => Duration.min(d, Duration.millis(this.maxBackoffMs))),
      Schedule.jittered,
    );

    return connectOnce.pipe(
      Effect.flatMap(handleConnection),
      Effect.tapError(() => Effect.sync(() => {
        if (!this.closed) this.transitionTo("DISCONNECTED");
      })),
      Effect.retry(reconnectSchedule),
      Effect.catchAll(() => Effect.void),
    );
  }

  // ---------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------

  private transitionTo(next: WsState): void {
    if (this.state === next) return;
    this.state = next;
    // Notify initial listener from config (for backward compat with client.ts)
    this.config.onStateChange?.(next);
    // Notify all registered listeners
    for (const listener of this.stateListeners) {
      listener(next);
    }
  }

  private resubscribeAll(): void {
    for (const [crdtId, vc] of this.subscriptions) {
      this.sendSubscribe(crdtId, vc);
    }
    this.flushPendingOps();
  }

  private flushPendingOps(): void {
    this.runtime.runFork(
      Effect.gen((function* (this: WsTransport) {
        let next: Option.Option<ClientMsg>;
        while (Option.isSome(next = yield* Queue.poll(this.pendingQueue))) {
          if (this.ws === null || this.state !== "CONNECTED") {
            yield* Queue.offer(this.pendingQueue, next.value);
            break;
          }
          try {
            this.ws.send(encodeClientMsg(next.value));
          } catch {
            yield* Queue.offer(this.pendingQueue, next.value);
            break;
          }
        }
      }).bind(this)),
    );
  }

  private sendSubscribe(crdtId: string, vc: VectorClock): void {
    if (this.ws === null || this.state !== "CONNECTED") return;
    this.ws.send(encodeClientMsg({ Subscribe: { crdt_id: crdtId } }));
    const vcBytes = encodeVectorClock(vc);
    this.ws.send(encodeClientMsg({ Sync: { crdt_id: crdtId, since_vc: vcBytes } }));
  }
}
