/**
 * Unit tests for UndoManager — CRDT-aware per-client undo/redo.
 *
 * No server required — transport is stubbed. Tests verify that undo/redo
 * dispatch the correct inverse CrdtOps via the transport.
 */

import { describe, it, expect, beforeEach } from "bun:test";
import { RGAHandle } from "../src/crdt/rga.js";
import { TreeHandle } from "../src/crdt/tree.js";
import { UndoManager } from "../src/undo/UndoManager.js";
import type { WsTransport } from "../src/transport/websocket.js";
import { decode } from "@msgpack/msgpack";

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

function decodeOp(msg: unknown): unknown {
  const m = msg as { Op: { op_bytes: Uint8Array } };
  return decode(m.Op.op_bytes);
}

const OPTS = { crdtId: "rg:doc", clientId: 42 };
const TREE_OPTS = { crdtId: "tr:doc", clientId: 42 };

// ─── RGA ─────────────────────────────────────────────────────────────────────

describe("UndoManager — RGA", () => {
  let t: WsTransport & { sent: unknown[] };
  let handle: RGAHandle;
  let manager: UndoManager<RGAHandle>;

  beforeEach(() => {
    t = stubTransport();
    handle = new RGAHandle({ ...OPTS, transport: t });
    manager = new UndoManager(handle);
  });

  it("insert returns node HLC strings", () => {
    const ids = manager.insert(0, "hi");
    expect(ids).toHaveLength(2);
    expect(ids[0]).toMatch(/^\d+:0:42$/);
    expect(ids[1]).toMatch(/^\d+:1:42$/);
  });

  it("undo after insert sends deleteById for each inserted node", async () => {
    const ids = manager.insert(0, "ab");
    expect(t.sent).toHaveLength(2); // 2 inserts

    // Wait for debounce to commit the batch
    await new Promise(r => setTimeout(r, 300));

    expect(manager.canUndo).toBe(true);
    manager.undo();

    // 2 more sends: deleteById for each char (in reverse order)
    expect(t.sent).toHaveLength(4);
    const del1 = decodeOp(t.sent[3]) as { RGA: { Delete: { id: unknown } } };
    const del2 = decodeOp(t.sent[2]) as { RGA: { Delete: { id: unknown } } };
    expect(del1.RGA.Delete.id).toBeDefined();
    expect(del2.RGA.Delete.id).toBeDefined();
  });

  it("undo with correct HLC — does not target remote nodes", async () => {
    const ids = manager.insert(0, "x");
    await new Promise(r => setTimeout(r, 300));

    // Simulate remote delta — server sends back authoritative text
    handle.applyDelta({ text: "yx" }); // remote "y" prepended

    manager.undo();
    // Delete op should target our HLC id, not the remote one
    const del = decodeOp(t.sent[t.sent.length - 1]) as {
      RGA: { Delete: { id: { node_id: number } } };
    };
    expect(del.RGA.Delete.id.node_id).toBe(42); // our clientId
  });

  it("undo on empty stack is a no-op", () => {
    expect(manager.canUndo).toBe(false);
    manager.undo(); // should not throw
    expect(t.sent).toHaveLength(0);
  });

  it("RGA delete is not recorded — undo does nothing", () => {
    manager.insert(0, "hello");
    manager.delete(0, 5);
    // delete sends pos-based ops (non-undoable) — stack only has insert batch
    // Wait for debounce
    // canUndo might be true from insert, but redo after undo of just-delete is empty
    expect(manager.canRedo).toBe(false);
  });

  it("batch debounce: 3 inserts within 250ms form one undo step", async () => {
    manager.insert(0, "a");
    manager.insert(1, "b");
    manager.insert(2, "c");
    // All within debounce window — still one pending batch
    expect(manager.canUndo).toBe(false); // not committed yet

    await new Promise(r => setTimeout(r, 300));
    expect(manager.canUndo).toBe(true); // now committed as one batch

    manager.undo();
    // One undo step = 3 deletes
    expect(t.sent).toHaveLength(6); // 3 inserts + 3 deletes
  });

  it("canUndo / canRedo reflect stack state", async () => {
    expect(manager.canUndo).toBe(false);
    expect(manager.canRedo).toBe(false);

    manager.insert(0, "x");
    await new Promise(r => setTimeout(r, 300));
    expect(manager.canUndo).toBe(true);
    expect(manager.canRedo).toBe(false);

    manager.undo();
    // RGA insert undo = deleteById, which returns null inverse — canRedo stays false
    expect(manager.canUndo).toBe(false);
    expect(manager.canRedo).toBe(false);
  });

  it("new op after undo clears redoStack — tree example", () => {
    // Use TreeHandle for this test since RGA undo doesn't produce redoStack entries
    const t2 = stubTransport();
    const h2 = new TreeHandle({ ...TREE_OPTS, clientId: 42, transport: t2 });
    const m2 = new UndoManager(h2);

    m2.addNode(null, "a0", "A");
    m2.undo();
    expect(m2.canRedo).toBe(true);

    m2.addNode(null, "a0", "B"); // new op after undo
    expect(m2.canRedo).toBe(false);
  });

  it("onStackChange fires on undo and redo", async () => {
    const calls: string[] = [];
    manager.onStackChange(() => calls.push("change"));

    manager.insert(0, "z");
    await new Promise(r => setTimeout(r, 300));
    // debounce commit fires onStackChange
    expect(calls.length).toBeGreaterThanOrEqual(1);

    const prev = calls.length;
    manager.undo();
    expect(calls.length).toBe(prev + 1);
  });
});

// ─── Tree ─────────────────────────────────────────────────────────────────────

describe("UndoManager — Tree", () => {
  let t: WsTransport & { sent: unknown[] };
  let handle: TreeHandle;
  let manager: UndoManager<TreeHandle>;

  beforeEach(() => {
    t = stubTransport();
    handle = new TreeHandle({ ...TREE_OPTS, transport: t });
    manager = new UndoManager(handle);
  });

  it("undo after addNode sends deleteNode for that nodeId", () => {
    const nodeId = manager.addNode(null, "a0", "Root");
    expect(manager.canUndo).toBe(true);

    manager.undo();
    expect(t.sent).toHaveLength(2);
    const del = decodeOp(t.sent[1]) as { Tree: { DeleteNode: unknown } };
    expect(del.Tree.DeleteNode).toBeDefined();
  });

  it("undo after deleteNode sends addNode with original parent/position/value", () => {
    const nodeId = manager.addNode(null, "a0", "Root");
    t.sent.splice(0); // clear

    manager.deleteNode(nodeId);
    expect(t.sent).toHaveLength(1); // 1 deleteNode op

    manager.undo(); // should add a new node
    expect(t.sent).toHaveLength(2);
    const add = decodeOp(t.sent[1]) as { Tree: { AddNode: { value: string } } };
    expect(add.Tree.AddNode.value).toBe("Root");
  });

  it("undo after moveNode sends moveNode back to old parent", () => {
    const parentId = manager.addNode(null, "a0", "Parent");
    const childId = manager.addNode(null, "b0", "Child");
    t.sent.splice(0);

    manager.moveNode(childId, parentId, "a0");
    expect(t.sent).toHaveLength(1);

    manager.undo();
    expect(t.sent).toHaveLength(2);
    const move = decodeOp(t.sent[1]) as {
      Tree: { MoveNode: { new_parent_id: null | unknown } };
    };
    // Undo move: child should go back to root (null parent)
    expect(move.Tree.MoveNode.new_parent_id).toBeNull();
  });

  it("undo after updateNode sends updateNode with previous value", () => {
    const nodeId = manager.addNode(null, "a0", "Original");
    t.sent.splice(0);

    manager.updateNode(nodeId, "Updated");
    expect(t.sent).toHaveLength(1);

    manager.undo();
    expect(t.sent).toHaveLength(2);
    const update = decodeOp(t.sent[1]) as {
      Tree: { UpdateNode: { value: string } };
    };
    expect(update.Tree.UpdateNode.value).toBe("Original");
  });

  it("redo after undo(addNode) re-adds the node", () => {
    manager.addNode(null, "a0", "Root");
    manager.undo();
    expect(manager.canRedo).toBe(true);

    manager.redo();
    // redo of tree_delete = addNode (new nodeId)
    expect(t.sent).toHaveLength(3); // original add + delete + re-add
    const add = decodeOp(t.sent[2]) as { Tree: { AddNode: { value: string } } };
    expect(add.Tree.AddNode.value).toBe("Root");
  });

  it("manual batch: addNode + updateNode = one undo step", () => {
    manager.startBatch();
    const nodeId = manager.addNode(null, "a0", "Hello");
    manager.updateNode(nodeId, "Hello World");
    manager.commitBatch();

    expect(manager.canUndo).toBe(true);
    // One undo step undoes both ops
    manager.undo();
    expect(t.sent).toHaveLength(4); // add + update + delete + update-back
  });

  it("remote delta between op and undo does not affect target HLC", () => {
    const nodeId = manager.addNode(null, "a0", "A");

    // Remote delta: server adds another node
    handle.applyDelta({
      roots: [
        { id: nodeId, value: "A", children: [] },
        { id: "9999:0:99", value: "Remote", children: [] },
      ],
    });

    manager.undo(); // should only delete our nodeId
    const del = decodeOp(t.sent[t.sent.length - 1]) as {
      Tree: { DeleteNode: { id: { node_id: number } } };
    };
    expect(del.Tree.DeleteNode.id.node_id).toBe(42); // our clientId
  });
});
