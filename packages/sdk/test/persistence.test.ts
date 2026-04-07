/**
 * Unit tests for offline-first persistence.
 *
 * Covers the two non-trivial paths:
 * 1. ORSet snapshot round-trip (hex-string tags, not Uint8Array)
 * 2. Pending op queue persistence via SyncStateStorage
 */

import { describe, it, expect } from "bun:test";
import { ORSetHandle } from "../src/crdt/orset.js";
import { WsTransport } from "../src/transport/websocket.js";
import {
  snapshotFromORSet,
  snapshotToBytes,
  bytesToSnapshot,
  restoreSnapshotToHandle,
} from "../src/persistence/snapshot.js";
import { localStorageSyncOpsAdapter, memoryStateStorage } from "../src/persistence/storage.js";
import { encode, decode } from "../src/codec.js";

function stubTransport(): WsTransport {
  return {
    connect: () => {},
    close: () => {},
    subscribe: () => {},
    updateClock: () => {},
    send: () => {},
    get currentState() { return "CONNECTED" as const; },
  } as unknown as WsTransport;
}

describe("ORSet snapshot", () => {
  it("round-trips elements through snapshot → bytes → restore", () => {
    const t = stubTransport();
    const original = new ORSetHandle<string>({ ns: "test", crdtId: "or:cart", clientId: 1, transport: t });
    original.add("apple");
    original.add("banana");

    const snap = snapshotFromORSet(original);
    const bytes = snapshotToBytes(snap);
    const decoded = bytesToSnapshot(bytes);

    expect(decoded).not.toBeNull();
    expect(decoded?.type).toBe("orset");

    const restored = new ORSetHandle<string>({ ns: "test", crdtId: "or:cart", clientId: 2, transport: t });
    if (decoded !== null) restoreSnapshotToHandle(decoded, restored);

    expect(restored.elements().sort()).toEqual(["apple", "banana"]);
  });

  it("restores an empty set correctly", () => {
    const t = stubTransport();
    const original = new ORSetHandle<string>({ ns: "test", crdtId: "or:cart", clientId: 1, transport: t });

    const snap = snapshotFromORSet(original);
    const bytes = snapshotToBytes(snap);
    const decoded = bytesToSnapshot(bytes);

    const restored = new ORSetHandle<string>({ ns: "test", crdtId: "or:cart", clientId: 2, transport: t });
    if (decoded !== null) restoreSnapshotToHandle(decoded, restored);

    expect(restored.elements()).toEqual([]);
  });

  it("preserves add-wins semantics: removed element not in snapshot", () => {
    const t = stubTransport();
    const handle = new ORSetHandle<string>({ ns: "test", crdtId: "or:cart", clientId: 1, transport: t });
    handle.add("apple");
    handle.add("banana");
    handle.remove("apple");

    const snap = snapshotFromORSet(handle);
    const bytes = snapshotToBytes(snap);
    const decoded = bytesToSnapshot(bytes);

    const restored = new ORSetHandle<string>({ ns: "test", crdtId: "or:cart", clientId: 2, transport: t });
    if (decoded !== null) restoreSnapshotToHandle(decoded, restored);

    expect(restored.elements()).toEqual(["banana"]);
  });
});

describe("SyncStateStorage op queue", () => {
  it("encodes and decodes a queue of ClientMsg-shaped objects", () => {
    const store = new Map<string, Uint8Array>();
    const adapter = {
      load: (key: string) => store.get(key) ?? null,
      save: (key: string, data: Uint8Array) => { store.set(key, data); },
      delete: (key: string) => { store.delete(key); },
    };

    const msgs = [
      { Op: { crdt_id: "gc:views", op_bytes: new Uint8Array([1, 2, 3]), client_seq: 1 } },
      { Op: { crdt_id: "gc:likes", op_bytes: new Uint8Array([4, 5, 6]), client_seq: 2 } },
    ];

    adapter.save("ops:my-room", encode(msgs));

    const raw = adapter.load("ops:my-room");
    expect(raw).not.toBeNull();

    const decoded = decode(raw as Uint8Array) as typeof msgs;
    expect(decoded).toHaveLength(2);
    expect(decoded[0]).toMatchObject({ Op: { crdt_id: "gc:views" } });
    expect(decoded[1]).toMatchObject({ Op: { crdt_id: "gc:likes" } });

    adapter.delete("ops:my-room");
    expect(adapter.load("ops:my-room")).toBeNull();
  });

  it("memoryStateStorage load returns null for missing key", async () => {
    const result = await memoryStateStorage.load("nonexistent");
    expect(result).toBeNull();
  });

  it("memoryStateStorage save/load round-trips bytes", async () => {
    const data = new Uint8Array([10, 20, 30]);
    await memoryStateStorage.save("test-key", data);
    const loaded = await memoryStateStorage.load("test-key");
    expect(loaded).toEqual(data);
    await memoryStateStorage.delete("test-key");
    expect(await memoryStateStorage.load("test-key")).toBeNull();
  });

  it("localStorageSyncOpsAdapter is defined and returns null for missing key", () => {
    // localStorage is available in Bun's test environment
    const adapter = localStorageSyncOpsAdapter("test:");
    expect(adapter.load("missing")).toBeNull();
  });
});
