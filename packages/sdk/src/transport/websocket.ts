import { Effect } from "effect";
import { encodeClientMsg, decodeServerMsg, encodeVectorClock } from "../codec.js";
import type { ServerMsg, VectorClock } from "../schema.js";
import {
  BACKOFF_INITIAL_MS,
  BACKOFF_MAX_MS,
  BACKOFF_MULTIPLIER,
  JITTER_MULTIPLIER,
  DEFAULT_TIMEOUT_MS,
  OFFLINE_QUEUE_MAX,
} from "../constants.js";

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

export class WsTransport {
  private readonly config: WsTransportConfig;
  private readonly maxBackoffMs: number;

  private ws: WebSocket | null = null;
  private state: WsState = "DISCONNECTED";
  private backoffMs = BACKOFF_INITIAL_MS;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  private readonly subscriptions = new Map<string, VectorClock>();
  private readonly pendingOps: Parameters<typeof encodeClientMsg>[0][] = [];

  constructor(config: WsTransportConfig) {
    this.config = config;
    this.maxBackoffMs = config.maxBackoffMs ?? BACKOFF_MAX_MS;
  }

  connect(): void {
    if (this.closed) return;
    this.closed = false;
    this.doConnect();
  }

  close(): void {
    this.closed = true;
    this.pendingOps.length = 0;
    this.clearReconnectTimer();
    this.transitionTo("CLOSING");
    this.ws?.close(1000, "client close");
  }

  reopen(): void {
    // Disown the current ws before creating a new one so its close event
    // fires with ws !== this.ws and does not trigger scheduleReconnect.
    const old = this.ws;
    this.ws = null;
    old?.close(1000, "reopen");
    this.closed = false;
    this.doConnect();
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

  send(msg: Parameters<typeof encodeClientMsg>[0]): void {
    if (this.state !== "CONNECTED" || this.ws === null) {
      if (this.pendingOps.length >= OFFLINE_QUEUE_MAX) {
        this.pendingOps.shift(); // drop oldest to make room
      }
      this.pendingOps.push(msg);
      return;
    }
    this.ws.send(encodeClientMsg(msg));
  }

  get pendingOpCount(): number {
    return this.pendingOps.length;
  }

  /**
   * Subscribe to connection state changes. Returns an unsubscribe function.
   * Compatible with React's `useSyncExternalStore` subscribe parameter.
   */
  onStateChange(listener: (state: WsState) => void): () => void {
    const prev = this.config.onStateChange;
    this.config.onStateChange = (s) => {
      prev?.(s);
      listener(s);
    };
    return () => {
      if (prev !== undefined) {
        this.config.onStateChange = prev;
      } else {
        delete (this.config as Partial<WsTransportConfig>).onStateChange;
      }
    };
  }

  get currentState(): WsState {
    return this.state;
  }

  waitForConnected(timeoutMs = DEFAULT_TIMEOUT_MS): Promise<void> {
    if (this.state === "CONNECTED") return Promise.resolve();
    return new Promise((resolve, reject) => {
      const orig = this.config.onStateChange;
      const restore = () => {
        if (orig !== undefined) {
          this.config.onStateChange = orig;
        } else {
          delete (this.config as Partial<WsTransportConfig>).onStateChange;
        }
      };
      const timer = setTimeout(() => {
        restore();
        reject(new Error("WsTransport: connect timeout"));
      }, timeoutMs);
      this.config.onStateChange = (s) => {
        orig?.(s);
        if (s === "CONNECTED") {
          clearTimeout(timer);
          restore();
          resolve();
        }
      };
    });
  }

  private doConnect(): void {
    if (this.closed) return;
    this.transitionTo("CONNECTING");

    const url = `${this.config.url}${this.config.url.includes("?") ? "&" : "?"}token=${encodeURIComponent(this.config.token)}`;
    const ws = new WebSocket(url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    ws.addEventListener("open", () => {
      if (ws !== this.ws) return;
      this.backoffMs = BACKOFF_INITIAL_MS;
      this.transitionTo("CONNECTED");
      this.resubscribeAll();
    });

    ws.addEventListener("message", (event: MessageEvent) => {
      if (ws !== this.ws) return;
      const bytes = new Uint8Array(event.data as ArrayBuffer);
      Effect.runPromise(decodeServerMsg(bytes)).then(
        (msg) => { this.config.onMessage(msg); },
        (e) => { console.warn("[meridian] failed to decode server message", e); },
      );
    });

    ws.addEventListener("close", () => {
      if (ws !== this.ws) return;
      this.ws = null;
      if (!this.closed) {
        this.transitionTo("DISCONNECTED");
        this.scheduleReconnect();
      }
    });

    ws.addEventListener("error", () => {
      // HACK: The "close" event fires right after an error — let that handler drive reconnect logic.
    });
  }

  private scheduleReconnect(): void {
    if (this.closed) return;
    const jitter = this.backoffMs * JITTER_MULTIPLIER * (Math.random() * 2 - 1);
    const delay = Math.round(this.backoffMs + jitter);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.doConnect();
    }, delay);
    this.backoffMs = Math.min(this.backoffMs * BACKOFF_MULTIPLIER, this.maxBackoffMs);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private transitionTo(next: WsState): void {
    if (this.state === next) return;
    this.state = next;
    this.config.onStateChange?.(next);
  }

  private resubscribeAll(): void {
    for (const [crdtId, vc] of this.subscriptions) {
      this.sendSubscribe(crdtId, vc);
    }
    this.flushPendingOps();
  }

  private flushPendingOps(): void {
    const ops = this.pendingOps.splice(0);
    for (let i = 0; i < ops.length; i++) {
      const op = ops[i];
      if (op === undefined) continue;
      try {
        this.ws?.send(encodeClientMsg(op));
      } catch {
        // WebSocket closed between open and flush — re-queue remaining ops.
        this.pendingOps.unshift(...ops.slice(i));
        break;
      }
    }
  }

  private sendSubscribe(crdtId: string, vc: VectorClock): void {
    if (this.ws === null || this.state !== "CONNECTED") return;
    this.ws.send(encodeClientMsg({ Subscribe: { crdt_id: crdtId } }));
    const vcBytes = encodeVectorClock(vc);
    this.ws.send(encodeClientMsg({ Sync: { crdt_id: crdtId, since_vc: vcBytes } }));
  }
}
