/**
 * WebSocket transport — reconnect FSM + Sync on reconnect.
 *
 * States:
 *   DISCONNECTED → CONNECTING → AUTHENTICATING → CONNECTED → CLOSING
 *
 * On reconnect: re-subscribes to all known CRDTs and sends Sync(localVectorClock)
 * so the server can push missed deltas.
 *
 * Backoff: 100ms → 200ms → 400ms → … → 30s (±20% jitter).
 */

import { Effect } from "effect";
import { encodeClientMsg, decodeServerMsg, encodeVectorClock } from "../codec.js";
import type { ServerMsg, VectorClock } from "../schema.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type WsState =
  | "DISCONNECTED"
  | "CONNECTING"
  | "CONNECTED"
  | "CLOSING";

export interface WsTransportConfig {
  /** Full WebSocket URL, e.g. "ws://localhost:3000/v1/namespaces/my-room/connect" */
  url: string;
  /** Bearer token passed as ?token= query param (WS can't set headers). */
  token: string;
  /** Called whenever a ServerMsg arrives. */
  onMessage: (msg: ServerMsg) => void;
  /** Called on state transitions. */
  onStateChange?: (state: WsState) => void;
  /** Maximum reconnect delay in ms. Default: 30_000 */
  maxBackoffMs?: number;
}

// ---------------------------------------------------------------------------
// WsTransport
// ---------------------------------------------------------------------------

export class WsTransport {
  private readonly config: WsTransportConfig;
  private readonly maxBackoffMs: number;

  private ws: WebSocket | null = null;
  private state: WsState = "DISCONNECTED";
  private backoffMs = 100;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  /** CRDTs to re-subscribe on reconnect: crdt_id → last known VectorClock */
  private readonly subscriptions = new Map<string, VectorClock>();

  constructor(config: WsTransportConfig) {
    this.config = config;
    this.maxBackoffMs = config.maxBackoffMs ?? 30_000;
  }

  // ---- Public API ----

  connect(): void {
    if (this.closed) return;
    this.closed = false;
    this.doConnect();
  }

  /** Gracefully close — will not reconnect. */
  close(): void {
    this.closed = true;
    this.clearReconnectTimer();
    this.transitionTo("CLOSING");
    this.ws?.close(1000, "client close");
  }

  /**
   * Subscribe to a CRDT's deltas.
   * If already connected, sends Subscribe immediately.
   * On reconnect, the subscription is re-sent automatically.
   */
  subscribe(crdtId: string, sinceVc: VectorClock = {}): void {
    this.subscriptions.set(crdtId, sinceVc);
    if (this.state === "CONNECTED") {
      this.sendSubscribe(crdtId, sinceVc);
    }
  }

  /** Update the local vector clock for a CRDT (used for reconnect Sync). */
  updateClock(crdtId: string, vc: VectorClock): void {
    this.subscriptions.set(crdtId, vc);
  }

  /** Send a raw ClientMsg. Throws if not connected. */
  send(msg: Parameters<typeof encodeClientMsg>[0]): void {
    if (this.state !== "CONNECTED" || this.ws === null) {
      throw new Error("WsTransport: not connected");
    }
    this.ws.send(encodeClientMsg(msg));
  }

  get currentState(): WsState {
    return this.state;
  }

  // ---- FSM internals ----

  private doConnect(): void {
    if (this.closed) return;
    this.transitionTo("CONNECTING");

    const url = `${this.config.url}${this.config.url.includes("?") ? "&" : "?"}token=${encodeURIComponent(this.config.token)}`;
    const ws = new WebSocket(url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    ws.addEventListener("open", () => {
      if (ws !== this.ws) return; // stale socket
      this.backoffMs = 100; // reset backoff on successful connect
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
      // The "close" event fires right after — let that handle reconnect.
    });
  }

  private scheduleReconnect(): void {
    if (this.closed) return;
    const jitter = this.backoffMs * 0.2 * (Math.random() * 2 - 1); // ±20%
    const delay = Math.round(this.backoffMs + jitter);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.doConnect();
    }, delay);
    this.backoffMs = Math.min(this.backoffMs * 2, this.maxBackoffMs);
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

  /** On reconnect: re-subscribe and send Sync for each known CRDT. */
  private resubscribeAll(): void {
    for (const [crdtId, vc] of this.subscriptions) {
      this.sendSubscribe(crdtId, vc);
    }
  }

  private sendSubscribe(crdtId: string, vc: VectorClock): void {
    if (this.ws === null || this.state !== "CONNECTED") return;

    // First subscribe so the server starts pushing future deltas
    this.ws.send(encodeClientMsg({ Subscribe: { crdt_id: crdtId } }));

    // Then sync to get missed deltas since our last known VC
    const vcBytes = encodeVectorClock(vc);
    this.ws.send(encodeClientMsg({ Sync: { crdt_id: crdtId, since_vc: vcBytes } }));
  }
}
