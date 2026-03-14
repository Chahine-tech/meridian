import { Effect } from "effect";
import { encodeClientMsg, decodeServerMsg, encodeVectorClock } from "../codec.js";
import type { ServerMsg, VectorClock } from "../schema.js";
import {
  BACKOFF_INITIAL_MS,
  BACKOFF_MAX_MS,
  BACKOFF_MULTIPLIER,
  JITTER_MULTIPLIER,
  DEFAULT_TIMEOUT_MS,
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
    this.clearReconnectTimer();
    this.transitionTo("CLOSING");
    this.ws?.close(1000, "client close");
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
      throw new Error("WsTransport: not connected");
    }
    this.ws.send(encodeClientMsg(msg));
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
  }

  private sendSubscribe(crdtId: string, vc: VectorClock): void {
    if (this.ws === null || this.state !== "CONNECTED") return;
    this.ws.send(encodeClientMsg({ Subscribe: { crdt_id: crdtId } }));
    const vcBytes = encodeVectorClock(vc);
    this.ws.send(encodeClientMsg({ Sync: { crdt_id: crdtId, since_vc: vcBytes } }));
  }
}
