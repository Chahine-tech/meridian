/**
 * Unit tests for CRDT handles (no server required).
 *
 * We stub the WsTransport so sends are captured but not actually sent.
 */

import { describe, it, expect, beforeEach } from "bun:test";
import { GCounterHandle } from "../src/crdt/gcounter.js";
import { PNCounterHandle } from "../src/crdt/pncounter.js";
import { ORSetHandle } from "../src/crdt/orset.js";
import { LwwRegisterHandle } from "../src/crdt/lwwregister.js";
import { PresenceHandle } from "../src/crdt/presence.js";
import type { WsTransport } from "../src/transport/websocket.js";

// ---------------------------------------------------------------------------
// Stub transport — captures sends, never actually connects
// ---------------------------------------------------------------------------

function stubTransport(): WsTransport & { sent: unknown[] } {
  const sent: unknown[] = [];
  return {
    sent,
    connect: () => {},
    close: () => {},
    subscribe: () => {},
    updateClock: () => {},
    send: (msg: unknown) => { sent.push(msg); },
    get currentState() { return "CONNECTED" as const; },
  } as unknown as WsTransport & { sent: unknown[] };
}

const BASE_OPTS = { ns: "test", clientId: 1 };

// ---------------------------------------------------------------------------
// GCounter
// ---------------------------------------------------------------------------

describe("GCounterHandle", () => {
  it("starts at 0", () => {
    const t = stubTransport();
    const h = new GCounterHandle({ ...BASE_OPTS, crdtId: "c", transport: t });
    expect(h.value()).toBe(0);
  });

  it("increment updates value optimistically", () => {
    const t = stubTransport();
    const h = new GCounterHandle({ ...BASE_OPTS, crdtId: "c", transport: t });
    h.increment(5);
    expect(h.value()).toBe(5);
    expect(t.sent).toHaveLength(1);
  });

  it("increment fires onChange", () => {
    const t = stubTransport();
    const h = new GCounterHandle({ ...BASE_OPTS, crdtId: "c", transport: t });
    const values: number[] = [];
    h.onChange(v => values.push(v));
    h.increment(3);
    h.increment(2);
    expect(values).toEqual([3, 5]);
  });

  it("applyDelta merges remote counts (max-merge)", () => {
    const t = stubTransport();
    const h = new GCounterHandle({ ...BASE_OPTS, crdtId: "c", transport: t });
    h.increment(10); // client 1 = 10
    h.applyDelta({ counters: { "2": 20 } });
    expect(h.value()).toBe(30);
  });

  it("applyDelta ignores stale counts (no regression)", () => {
    const t = stubTransport();
    const h = new GCounterHandle({ ...BASE_OPTS, crdtId: "c", transport: t });
    h.increment(10);
    h.applyDelta({ counters: { "1": 5 } }); // 5 < 10 — stale
    expect(h.value()).toBe(10);
  });

  it("unsubscribe stops onChange notifications", () => {
    const t = stubTransport();
    const h = new GCounterHandle({ ...BASE_OPTS, crdtId: "c", transport: t });
    const values: number[] = [];
    const unsub = h.onChange(v => values.push(v));
    h.increment(1);
    unsub();
    h.increment(1);
    expect(values).toEqual([1]); // only the first increment
  });

  it("increment rejects non-positive amount", () => {
    const t = stubTransport();
    const h = new GCounterHandle({ ...BASE_OPTS, crdtId: "c", transport: t });
    expect(() => h.increment(0)).toThrow(RangeError);
    expect(() => h.increment(-1)).toThrow(RangeError);
  });
});

// ---------------------------------------------------------------------------
// PNCounter
// ---------------------------------------------------------------------------

describe("PNCounterHandle", () => {
  it("starts at 0", () => {
    const t = stubTransport();
    const h = new PNCounterHandle({ ...BASE_OPTS, crdtId: "pn", transport: t });
    expect(h.value()).toBe(0);
  });

  it("increment and decrement", () => {
    const t = stubTransport();
    const h = new PNCounterHandle({ ...BASE_OPTS, crdtId: "pn", transport: t });
    h.increment(10);
    h.decrement(3);
    expect(h.value()).toBe(7);
  });

  it("can go negative", () => {
    const t = stubTransport();
    const h = new PNCounterHandle({ ...BASE_OPTS, crdtId: "pn", transport: t });
    h.decrement(5);
    expect(h.value()).toBe(-5);
  });

  it("applyDelta merges both GCounters", () => {
    const t = stubTransport();
    const h = new PNCounterHandle({ ...BASE_OPTS, crdtId: "pn", transport: t });
    h.applyDelta({ pos: { counters: { "2": 100 } }, neg: { counters: { "2": 40 } } });
    expect(h.value()).toBe(60);
  });
});

// ---------------------------------------------------------------------------
// ORSet
// ---------------------------------------------------------------------------

describe("ORSetHandle", () => {
  it("empty by default", () => {
    const t = stubTransport();
    const h = new ORSetHandle({ ...BASE_OPTS, crdtId: "s", transport: t });
    expect(h.elements()).toEqual([]);
  });

  it("add makes element visible", () => {
    const t = stubTransport();
    const h = new ORSetHandle<string>({ ...BASE_OPTS, crdtId: "s", transport: t });
    h.add("alice");
    expect(h.has("alice")).toBe(true);
  });

  it("remove makes element invisible", () => {
    const t = stubTransport();
    const h = new ORSetHandle<string>({ ...BASE_OPTS, crdtId: "s", transport: t });
    h.add("alice");
    h.remove("alice");
    expect(h.has("alice")).toBe(false);
  });

  it("add-wins: concurrent add+remove → element survives via applyDelta", () => {
    const t = stubTransport();
    const h = new ORSetHandle<string>({ ...BASE_OPTS, crdtId: "s", transport: t });
    // Remote adds "alice" with tag t1 (16 bytes), local remove only removes t2 (different tag)
    const t1 = new Uint8Array(16).fill(0x01);
    const t2 = new Uint8Array(16).fill(0x02);
    h.applyDelta({ adds: { '"alice"': [t1] }, removes: {} });
    h.applyDelta({ adds: {}, removes: { '"alice"': [t2] } }); // unknown tag — no effect
    expect(h.has("alice")).toBe(true);
  });

  it("fires onChange on add and remove", () => {
    const t = stubTransport();
    const h = new ORSetHandle<string>({ ...BASE_OPTS, crdtId: "s", transport: t });
    const snapshots: string[][] = [];
    h.onChange(elems => snapshots.push([...elems].sort()));
    h.add("a");
    h.add("b");
    h.remove("a");
    expect(snapshots).toEqual([["a"], ["a", "b"], ["b"]]);
  });
});

// ---------------------------------------------------------------------------
// LWW Register
// ---------------------------------------------------------------------------

describe("LwwRegisterHandle", () => {
  it("starts null", () => {
    const t = stubTransport();
    const h = new LwwRegisterHandle({ ...BASE_OPTS, crdtId: "r", transport: t });
    expect(h.value()).toBeNull();
  });

  it("set updates value", () => {
    const t = stubTransport();
    const h = new LwwRegisterHandle<string>({ ...BASE_OPTS, crdtId: "r", transport: t });
    h.set("hello");
    expect(h.value()).toBe("hello");
  });

  it("applyDelta with higher HLC wins", () => {
    const t = stubTransport();
    const h = new LwwRegisterHandle<string>({ ...BASE_OPTS, crdtId: "r", transport: t });
    h.set("first");
    const localMeta = h.meta()!;
    h.applyDelta({
      entry: {
        value: "second",
        hlc: { wall_ms: localMeta.updatedAtMs + 1000, logical: 0, node_id: 2 },
        author: 2,
      },
    });
    expect(h.value()).toBe("second");
  });

  it("applyDelta with lower HLC is rejected", () => {
    const t = stubTransport();
    const h = new LwwRegisterHandle<string>({ ...BASE_OPTS, crdtId: "r", transport: t });
    // Set local value with current time
    h.set("current");
    h.applyDelta({
      entry: {
        value: "old",
        hlc: { wall_ms: 1, logical: 0, node_id: 99 }, // epoch — always loses
        author: 99,
      },
    });
    expect(h.value()).toBe("current");
  });
});

// ---------------------------------------------------------------------------
// Presence
// ---------------------------------------------------------------------------

describe("PresenceHandle", () => {
  it("empty by default", () => {
    const t = stubTransport();
    const h = new PresenceHandle({ ...BASE_OPTS, crdtId: "p", transport: t });
    expect(h.online()).toEqual([]);
  });

  it("heartbeat adds self to online list", () => {
    const t = stubTransport();
    const h = new PresenceHandle<{ name: string }>({ ...BASE_OPTS, crdtId: "p", transport: t });
    h.heartbeat({ name: "alice" }, 30_000);
    expect(h.online()).toHaveLength(1);
    expect(h.online()[0]?.data).toEqual({ name: "alice" });
  });

  it("leave removes self from online list", () => {
    const t = stubTransport();
    const h = new PresenceHandle<{ name: string }>({ ...BASE_OPTS, crdtId: "p", transport: t });
    h.heartbeat({ name: "alice" }, 30_000);
    h.leave();
    expect(h.online()).toHaveLength(0);
  });

  it("applyDelta adds remote entries", () => {
    const t = stubTransport();
    const h = new PresenceHandle<{ name: string }>({ ...BASE_OPTS, crdtId: "p", transport: t });
    const now = Date.now();
    h.applyDelta({
      changes: {
        "2": { data: { name: "bob" }, hlc: { wall_ms: now, logical: 0, node_id: 2 }, ttl_ms: 30_000 },
      },
    });
    expect(h.online()).toHaveLength(1);
    expect(h.online()[0]?.data).toEqual({ name: "bob" });
  });

  it("applyDelta null removes entry", () => {
    const t = stubTransport();
    const h = new PresenceHandle<{ name: string }>({ ...BASE_OPTS, crdtId: "p", transport: t });
    const now = Date.now();
    h.applyDelta({
      changes: {
        "2": { data: { name: "bob" }, hlc: { wall_ms: now, logical: 0, node_id: 2 }, ttl_ms: 30_000 },
      },
    });
    h.applyDelta({ changes: { "2": null } });
    expect(h.online()).toHaveLength(0);
  });

  it("fires onChange on heartbeat and leave", () => {
    const t = stubTransport();
    const h = new PresenceHandle<string>({ ...BASE_OPTS, crdtId: "p", transport: t });
    const counts: number[] = [];
    h.onChange(entries => counts.push(entries.length));
    h.heartbeat("alice", 30_000);
    h.leave();
    expect(counts).toEqual([1, 0]);
  });
});
