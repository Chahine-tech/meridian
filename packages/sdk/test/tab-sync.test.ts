/**
 * Unit tests for TabSync (BroadcastChannel-based multi-tab delta sync).
 *
 * BroadcastChannel is not available in Bun's test worker by default,
 * so we patch globalThis with a synchronous fake that mirrors the
 * standard behaviour: messages are delivered to all OTHER instances
 * sharing the same channel name, never to the sender itself.
 */

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { TabSync } from "../src/sync/tab-sync.js";

// ---------------------------------------------------------------------------
// Fake BroadcastChannel
// ---------------------------------------------------------------------------

class FakeBroadcastChannel {
  static readonly registry = new Map<string, Set<FakeBroadcastChannel>>();

  onmessage: ((ev: { data: unknown }) => void) | null = null;

  constructor(private readonly name: string) {
    const peers = FakeBroadcastChannel.registry.get(name) ?? new Set();
    peers.add(this);
    FakeBroadcastChannel.registry.set(name, peers);
  }

  postMessage(data: unknown): void {
    for (const peer of FakeBroadcastChannel.registry.get(this.name) ?? []) {
      if (peer !== this) peer.onmessage?.({ data });
    }
  }

  close(): void {
    FakeBroadcastChannel.registry.get(this.name)?.delete(this);
  }

  static reset(): void {
    FakeBroadcastChannel.registry.clear();
  }
}

beforeEach(() => {
  FakeBroadcastChannel.reset();
  (globalThis as Record<string, unknown>).BroadcastChannel = FakeBroadcastChannel;
});

afterEach(() => {
  FakeBroadcastChannel.reset();
  delete (globalThis as Record<string, unknown>).BroadcastChannel;
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("TabSync", () => {
  it("delivers a broadcast delta to another instance on the same namespace", () => {
    const sender = new TabSync("ns");
    const receiver = new TabSync("ns");

    const received: { crdtId: string; deltaBytes: Uint8Array }[] = [];
    receiver.onDelta(({ crdtId, deltaBytes }) => { received.push({ crdtId, deltaBytes }); });

    const bytes = new Uint8Array([1, 2, 3]);
    sender.broadcast("gc:views", bytes);

    expect(received).toHaveLength(1);
    expect(received[0]?.crdtId).toBe("gc:views");
    expect(received[0]?.deltaBytes).toEqual(bytes);

    sender.close();
    receiver.close();
  });

  it("does not deliver to instances on a different namespace", () => {
    const sender = new TabSync("ns-a");
    const receiver = new TabSync("ns-b");

    const received: unknown[] = [];
    receiver.onDelta((msg) => { received.push(msg); });

    sender.broadcast("gc:x", new Uint8Array([1]));

    expect(received).toHaveLength(0);

    sender.close();
    receiver.close();
  });

  it("does not echo back to the sender", () => {
    const tab = new TabSync("ns");
    const selfReceived: unknown[] = [];
    tab.onDelta((msg) => { selfReceived.push(msg); });

    tab.broadcast("gc:views", new Uint8Array([1]));

    expect(selfReceived).toHaveLength(0);
    tab.close();
  });

  it("onDelta returns an unsubscribe function that stops delivery", () => {
    const sender = new TabSync("ns");
    const receiver = new TabSync("ns");

    const received: unknown[] = [];
    const unsub = receiver.onDelta((msg) => { received.push(msg); });

    unsub();
    sender.broadcast("gc:views", new Uint8Array([9]));

    expect(received).toHaveLength(0);
    sender.close();
    receiver.close();
  });

  it("broadcasts to multiple receivers simultaneously", () => {
    const sender = new TabSync("ns");
    const r1 = new TabSync("ns");
    const r2 = new TabSync("ns");

    const got1: string[] = [];
    const got2: string[] = [];
    r1.onDelta(({ crdtId }) => { got1.push(crdtId); });
    r2.onDelta(({ crdtId }) => { got2.push(crdtId); });

    sender.broadcast("or:cart", new Uint8Array([1]));
    sender.broadcast("lw:theme", new Uint8Array([2]));

    expect(got1).toEqual(["or:cart", "lw:theme"]);
    expect(got2).toEqual(["or:cart", "lw:theme"]);

    sender.close(); r1.close(); r2.close();
  });

  it("ignores messages with an unexpected type field", () => {
    const receiver = new TabSync("ns");
    const received: unknown[] = [];
    receiver.onDelta((msg) => { received.push(msg); });

    // Simulate a rogue postMessage from another script on the same channel
    const raw = FakeBroadcastChannel.registry.get("ns");
    for (const peer of raw ?? []) {
      peer.onmessage?.({ data: { type: "unknown", crdtId: "gc:x", deltaBytes: new Uint8Array() } });
    }

    expect(received).toHaveLength(0);
    receiver.close();
  });

  it("stops receiving after close()", () => {
    const sender = new TabSync("ns");
    const receiver = new TabSync("ns");

    const received: unknown[] = [];
    receiver.onDelta((msg) => { received.push(msg); });

    receiver.close();
    sender.broadcast("gc:x", new Uint8Array([1]));

    expect(received).toHaveLength(0);
    sender.close();
  });
});
