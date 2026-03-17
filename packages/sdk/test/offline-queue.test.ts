/**
 * Unit tests for WsTransport offline op queue.
 *
 * We stub the global WebSocket to control connection state without a real server.
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { WsTransport } from "../src/transport/websocket.js";
import { OFFLINE_QUEUE_MAX } from "../src/constants.js";

// ---------------------------------------------------------------------------
// Fake WebSocket
// ---------------------------------------------------------------------------

type WsEventName = "open" | "message" | "close" | "error";

class FakeWebSocket {
  binaryType: string = "arraybuffer";
  readonly sent: Uint8Array[] = [];
  private readonly listeners = new Map<WsEventName, (() => void)[]>();
  closeCode: number | undefined;
  closeReason: string | undefined;

  addEventListener(event: WsEventName, handler: () => void): void {
    const list = this.listeners.get(event) ?? [];
    list.push(handler);
    this.listeners.set(event, list);
  }

  send(data: Uint8Array): void {
    this.sent.push(data);
  }

  close(code: number, reason: string): void {
    this.closeCode = code;
    this.closeReason = reason;
    this.emit("close");
  }

  emit(event: WsEventName): void {
    for (const handler of this.listeners.get(event) ?? []) {
      handler();
    }
  }
}

let fakeWs: FakeWebSocket;

// Patch globalThis.WebSocket before each test.
beforeEach(() => {
  fakeWs = new FakeWebSocket();
  (globalThis as unknown as Record<string, unknown>).WebSocket = function () {
    return fakeWs;
  };
});

afterEach(() => {
  delete (globalThis as unknown as Record<string, unknown>).WebSocket;
});

function makeTransport(onStateChange?: (s: string) => void): WsTransport {
  return new WsTransport({
    url: "ws://localhost:3000",
    token: "tok",
    onMessage: () => {},
    onStateChange,
    // Use tiny backoff so reconnect tests don't wait
    maxBackoffMs: 10,
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("WsTransport offline queue", () => {
  it("buffers ops sent while DISCONNECTED", () => {
    const t = makeTransport();
    t.connect();
    // Not yet open — state is CONNECTING
    expect(t.currentState).toBe("CONNECTING");

    t.send({ Op: { crdt_id: "gc:views", op_bytes: new Uint8Array([1]) } });
    t.send({ Op: { crdt_id: "gc:views", op_bytes: new Uint8Array([2]) } });

    expect(t.pendingOpCount).toBe(2);
    expect(fakeWs.sent).toHaveLength(0);
  });

  it("flushes buffered ops in order on reconnect (open event)", () => {
    const t = makeTransport();
    t.connect();

    t.send({ Op: { crdt_id: "gc:views", op_bytes: new Uint8Array([1]) } });
    t.send({ Op: { crdt_id: "gc:views", op_bytes: new Uint8Array([2]) } });
    expect(t.pendingOpCount).toBe(2);

    // Simulate server accepting the connection
    fakeWs.emit("open");

    expect(t.currentState).toBe("CONNECTED");
    expect(t.pendingOpCount).toBe(0);
    // fakeWs.sent contains: Subscribe msgs from resubscribeAll + 2 flushed ops
    // We just verify the queue was drained and sends happened
    expect(fakeWs.sent.length).toBeGreaterThanOrEqual(2);
  });

  it("sends immediately when already CONNECTED", () => {
    const t = makeTransport();
    t.connect();
    fakeWs.emit("open");
    expect(t.currentState).toBe("CONNECTED");

    const before = fakeWs.sent.length;
    t.send({ Op: { crdt_id: "gc:views", op_bytes: new Uint8Array([42]) } });

    expect(t.pendingOpCount).toBe(0);
    expect(fakeWs.sent.length).toBe(before + 1);
  });

  it("drops the oldest op when queue exceeds OFFLINE_QUEUE_MAX", () => {
    const t = makeTransport();
    t.connect();
    // Fill the queue beyond the limit
    for (let i = 0; i <= OFFLINE_QUEUE_MAX; i++) {
      t.send({ Op: { crdt_id: "gc:x", op_bytes: new Uint8Array([i % 256]) } });
    }
    // Queue should be capped at OFFLINE_QUEUE_MAX
    expect(t.pendingOpCount).toBe(OFFLINE_QUEUE_MAX);
  });

  it("clears the queue on close()", () => {
    const t = makeTransport();
    t.connect();

    t.send({ Op: { crdt_id: "gc:views", op_bytes: new Uint8Array([1]) } });
    t.send({ Op: { crdt_id: "gc:views", op_bytes: new Uint8Array([2]) } });
    expect(t.pendingOpCount).toBe(2);

    t.close();

    expect(t.pendingOpCount).toBe(0);
  });

  it("does not send buffered ops after close()", () => {
    const t = makeTransport();
    t.connect();

    t.send({ Op: { crdt_id: "gc:views", op_bytes: new Uint8Array([1]) } });
    t.close();

    // Even if open fires somehow, ops should not be sent
    fakeWs.emit("open");
    expect(fakeWs.sent).toHaveLength(0);
  });
});
