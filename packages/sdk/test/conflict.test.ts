/**
 * Unit tests for TreeHandle conflict visualization.
 *
 * Conflict events are detected by comparing the local optimistic state
 * (nodeMetaMap) against the incoming server delta. Transport is stubbed.
 */

import { describe, it, expect, beforeEach } from "bun:test";
import { TreeHandle } from "../src/crdt/tree.js";
import type { WsTransport } from "../src/transport/websocket.js";
import type { ConflictEvent } from "../src/conflict/types.js";
import type { TreeDelta } from "../src/sync/delta.js";

function stubTransport(): WsTransport & { sent: unknown[] } {
  const sent: unknown[] = [];
  return {
    sent,
    connect: () => {},
    close: () => {},
    send: (msg) => { sent.push(msg); },
    onStateChange: () => () => {},
    onMessage: () => () => {},
    pendingCount: () => 0,
  } as unknown as WsTransport & { sent: unknown[] };
}

function makeTree(clientId = 1): TreeHandle {
  return new TreeHandle({
    crdtId: "tree-1",
    clientId,
    transport: stubTransport(),
  });
}

/** Helper: build a flat TreeDelta (no children) */
function flatDelta(nodes: Array<{ id: string; value: string; position: string; parentId?: string }>): TreeDelta {
  return {
    roots: nodes
      .filter((n) => n.parentId === undefined)
      .map((root) => ({
        id: root.id,
        value: root.value,
        position: root.position,
        children: nodes
          .filter((n) => n.parentId === root.id)
          .map((child) => ({
            id: child.id,
            value: child.value,
            position: child.position,
            children: [],
          })),
      })),
  };
}

describe("conflict visualization — move_reordered", () => {
  it("fires when server places node at different position than local optimistic", () => {
    const tree = makeTree();
    const events: ConflictEvent[] = [];
    tree.onConflict((e) => events.push(e));

    // Local client adds node at position "a0"
    const nodeId = tree.addNode(null, "a0", "Node");

    // Server responds with the node at "b0" (concurrent move won)
    tree.applyDelta(flatDelta([{ id: nodeId, value: "Node", position: "b0" }]));

    expect(events).toHaveLength(1);
    expect(events[0]!.kind).toBe("move_reordered");
    if (events[0]!.kind === "move_reordered") {
      expect(events[0]!.nodeId).toBe(nodeId);
      expect(events[0]!.localPosition).toBe("a0");
      expect(events[0]!.serverPosition).toBe("b0");
    }
  });

  it("fires when server places node under different parent", () => {
    const tree = makeTree();
    const events: ConflictEvent[] = [];
    tree.onConflict((e) => events.push(e));

    const parentA = tree.addNode(null, "a0", "ParentA");
    const parentB = tree.addNode(null, "b0", "ParentB");
    const child = tree.addNode(parentA, "a0", "Child");

    // Locally: child is under parentA. Server says it moved to parentB.
    tree.applyDelta(
      flatDelta([
        { id: parentA, value: "ParentA", position: "a0" },
        { id: parentB, value: "ParentB", position: "b0" },
        { id: child, value: "Child", position: "a0", parentId: parentB },
      ])
    );

    const moveEvents = events.filter((e) => e.kind === "move_reordered");
    expect(moveEvents.length).toBeGreaterThanOrEqual(1);
    const childEvent = moveEvents.find((e) => e.kind === "move_reordered" && e.nodeId === child);
    expect(childEvent).toBeDefined();
    if (childEvent?.kind === "move_reordered") {
      expect(childEvent.localParentId).toBe(parentA);
      expect(childEvent.serverParentId).toBe(parentB);
    }
  });

  it("does not fire when server confirms local state", () => {
    const tree = makeTree();
    const events: ConflictEvent[] = [];
    tree.onConflict((e) => events.push(e));

    const nodeId = tree.addNode(null, "a0", "Node");

    // Server agrees with local
    tree.applyDelta(flatDelta([{ id: nodeId, value: "Node", position: "a0" }]));

    expect(events).toHaveLength(0);
  });
});

describe("conflict visualization — lww_overwrite", () => {
  it("fires when server value differs from local value", () => {
    const tree = makeTree();
    const events: ConflictEvent[] = [];
    tree.onConflict((e) => events.push(e));

    const nodeId = tree.addNode(null, "a0", "LocalValue");

    // Server resolves LWW in favor of a concurrent update
    tree.applyDelta(flatDelta([{ id: nodeId, value: "RemoteWinner", position: "a0" }]));

    const lwwEvents = events.filter((e) => e.kind === "lww_overwrite");
    expect(lwwEvents).toHaveLength(1);
    if (lwwEvents[0]!.kind === "lww_overwrite") {
      expect(lwwEvents[0]!.discardedValue).toBe("LocalValue");
      expect(lwwEvents[0]!.winnerValue).toBe("RemoteWinner");
    }
  });

  it("does not fire when value matches local", () => {
    const tree = makeTree();
    const events: ConflictEvent[] = [];
    tree.onConflict((e) => events.push(e));

    const nodeId = tree.addNode(null, "a0", "Same");
    tree.applyDelta(flatDelta([{ id: nodeId, value: "Same", position: "a0" }]));

    expect(events.filter((e) => e.kind === "lww_overwrite")).toHaveLength(0);
  });
});

describe("conflict visualization — move_discarded", () => {
  it("fires when delta contains a discarded_move from server", () => {
    const tree = makeTree();
    const events: ConflictEvent[] = [];
    tree.onConflict((e) => events.push(e));

    const nodeId = tree.addNode(null, "a0", "Root");
    const childId = tree.addNode(nodeId, "a0", "Child");

    // Simulate server returning a delta with a discarded cycle move.
    // The server rejected: move nodeId under childId (would be a cycle).
    // Both nodeId parts: wall_ms:logical:node_id
    const [wall, logical, nodeIdNum] = nodeId.split(":").map(Number) as [number, number, number];
    const [cwall, clogical, cNodeIdNum] = childId.split(":").map(Number) as [number, number, number];

    tree.applyDelta({
      roots: [
        {
          id: nodeId,
          value: "Root",
          position: "a0",
          children: [{ id: childId, value: "Child", position: "a0", children: [] }],
        },
      ],
      discarded_moves: [
        {
          node_id: { wall_ms: wall, logical, node_id: nodeIdNum },
          attempted_parent_id: { wall_ms: cwall, logical: clogical, node_id: cNodeIdNum },
          attempted_position: "a0",
          actual_parent_id: null,
          actual_position: "a0",
        },
      ],
    });

    const discarded = events.filter((e) => e.kind === "move_discarded");
    expect(discarded).toHaveLength(1);
    if (discarded[0]!.kind === "move_discarded") {
      expect(discarded[0]!.nodeId).toBe(nodeId);
      expect(discarded[0]!.attemptedParentId).toBe(childId);
      expect(discarded[0]!.attemptedPosition).toBe("a0");
      expect(discarded[0]!.actualParentId).toBeNull();
    }
  });

  it("does not fire when discarded_moves is empty", () => {
    const tree = makeTree();
    const events: ConflictEvent[] = [];
    tree.onConflict((e) => events.push(e));

    const nodeId = tree.addNode(null, "a0", "Root");
    tree.applyDelta({
      roots: [{ id: nodeId, value: "Root", position: "a0", children: [] }],
      discarded_moves: [],
    });

    expect(events.filter((e) => e.kind === "move_discarded")).toHaveLength(0);
  });
});

describe("conflict visualization — conflictLog()", () => {
  it("accumulates events in chronological order", () => {
    const tree = makeTree();
    const n1 = tree.addNode(null, "a0", "V1");
    const n2 = tree.addNode(null, "b0", "V2");

    tree.applyDelta(flatDelta([
      { id: n1, value: "X", position: "a0" }, // lww_overwrite
      { id: n2, value: "V2", position: "c0" }, // move_reordered
    ]));

    const log = tree.conflictLog();
    expect(log.length).toBeGreaterThanOrEqual(2);
    expect(log.map((e) => e.kind)).toContain("lww_overwrite");
    expect(log.map((e) => e.kind)).toContain("move_reordered");
  });

  it("onConflict unsubscribe works", () => {
    const tree = makeTree();
    const events: ConflictEvent[] = [];
    const unsub = tree.onConflict((e) => events.push(e));

    const nodeId = tree.addNode(null, "a0", "A");
    unsub();

    tree.applyDelta(flatDelta([{ id: nodeId, value: "B", position: "z9" }]));

    // Listener was removed — no events delivered
    expect(events).toHaveLength(0);
    // But log still captures it
    expect(tree.conflictLog().length).toBeGreaterThan(0);
  });
});
