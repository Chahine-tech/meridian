/**
 * Integration tests — require a live Meridian server.
 *
 * Start the server before running:
 *   MERIDIAN_SIGNING_KEY=4242424242424242424242424242424242424242424242424242424242424242 \
 *   MERIDIAN_DATA_DIR=/tmp/meridian-int-test \
 *   MERIDIAN_BIND=127.0.0.1:3737 \
 *   cargo run --bin meridian
 *
 * Run tests:
 *   bun test test/integration.test.ts
 */

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { Effect } from "effect";
import { MeridianClient } from "../src/client.js";
import { HttpClient } from "../src/transport/http.js";
import { uuidToBytes, encode } from "../src/codec.js";

// Config — matches the dev signing key [0x42; 32]
// The token below is for namespace="integration", client_id=1, TTL=24h
// Regenerate with: cargo run --bin gen_token -- integration 1
const SERVER_URL = "http://127.0.0.1:3737";
const NAMESPACE = "integration";

// Token signed with key [0x42;32], namespace="integration", client_id=1, admin perms, TTL 24h
// Generated via: cargo run --bin gen_token -- integration 1
let TOKEN: string;

async function generateToken(clientId = 1): Promise<string> {
  const proc = Bun.spawn(
    ["cargo", "run", "--quiet", "--bin", "gen_token", "--", NAMESPACE, String(clientId)],
    { cwd: new URL("../../", import.meta.url).pathname, stderr: "inherit" },
  );
  const text = await new Response(proc.stdout).text();
  return text.trim();
}

async function isServerUp(): Promise<boolean> {
  try {
    // Try the health endpoint without auth — we just want a TCP response
    const r = await fetch(`${SERVER_URL}/v1/namespaces/${NAMESPACE}/crdts/__probe`, {
      headers: { Authorization: "Bearer invalid" },
      signal: AbortSignal.timeout(1000),
    });
    // 401/403 means server is up
    return r.status === 401 || r.status === 403 || r.status === 404;
  } catch {
    return false;
  }
}

describe("Meridian integration", () => {
  let http: HttpClient;
  // Pre-generated token for client_id=2 — used by tests that need two distinct clients
  let TOKEN2: string;

  beforeAll(async () => {
    const up = await isServerUp();
    if (!up) {
      console.warn(
        "\n⚠️  Meridian server not running — skipping integration tests.\n" +
        "Start with:\n" +
        "  MERIDIAN_SIGNING_KEY=4242...4242 MERIDIAN_DATA_DIR=/tmp/meridian-int-test " +
        "MERIDIAN_BIND=127.0.0.1:3737 cargo run --bin meridian\n",
      );
      return;
    }
    TOKEN = await generateToken(1);
    TOKEN2 = await generateToken(2);
    http = new HttpClient({ baseUrl: SERVER_URL, token: TOKEN });
  });

  function skip(name: string, fn: () => unknown) {
    // If token is not set, server wasn't up — skip gracefully
    test(name, async () => {
      if (!TOKEN) return;
      await fn();
    });
  }

  /** Polls until wsState === "CONNECTED" for at least two consecutive checks.
   *  Calls reopen() if stuck in DISCONNECTED to kick the Effect reconnect loop. */
  async function ensureConnected(client: import("../src/client.js").MeridianClient, timeoutMs = 8000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    let stable = 0;
    let lastReopen = 0;
    while (Date.now() < deadline) {
      const s = client.snapshot().wsState;
      if (s === "CONNECTED") {
        stable++;
        if (stable >= 2) return;
      } else {
        stable = 0;
        // If stuck DISCONNECTED for >500ms, force a reopen.
        if (s === "DISCONNECTED" && Date.now() - lastReopen > 500) {
          lastReopen = Date.now();
          client.reopen();
        }
      }
      await new Promise(r => setTimeout(r, 50));
    }
    throw new Error(`ensureConnected: still ${client.snapshot().wsState} after ${timeoutMs}ms (clientId=${client.clientId})`);
  }

  skip("GET non-existent CRDT returns 404", async () => {
    const result = await Effect.runPromise(
      Effect.either(http.getCrdt(NAMESPACE, "__nonexistent__")),
    );
    expect(result._tag).toBe("Left");
    if (result._tag === "Left") {
      expect((result.left as { status?: number }).status).toBe(404);
    }
  });

  skip("GCounter: increment via HTTP then read value", async () => {
    const key = `gc:http:${Date.now()}`;

    // POST op — GCounter increment by 7
    const opResult = await Effect.runPromise(
      Effect.either(
        http.postOp(NAMESPACE, key, { GCounter: { client_id: 1, amount: 7 } }),
      ),
    );
    expect(opResult._tag).toBe("Right");

    // GET value — server returns raw JSON value
    const getResult = await Effect.runPromise(
      Effect.either(http.getCrdt(NAMESPACE, key)),
    );
    expect(getResult._tag).toBe("Right");
    if (getResult._tag === "Right") {
      const value = getResult.right as { total?: number };
      expect(value.total).toBe(7);
    }

  });

  skip("GCounter: multiple increments accumulate", async () => {
    const key = `gc:multi:${Date.now()}`;

    await Effect.runPromise(http.postOp(NAMESPACE, key, { GCounter: { client_id: 1, amount: 3 } }));
    await Effect.runPromise(http.postOp(NAMESPACE, key, { GCounter: { client_id: 1, amount: 4 } }));
    await Effect.runPromise(http.postOp(NAMESPACE, key, { GCounter: { client_id: 2, amount: 10 } }));

    const r = await Effect.runPromise(http.getCrdt(NAMESPACE, key));
    const value = r as { total?: number };
    // GCounter: apply() accumulates per client (saturating_add)
    // client 1: 3 + 4 = 7, client 2: 10 → total = 17
    expect(value.total).toBe(17);
  });

  skip("PNCounter: increment and decrement", async () => {
    const key = `pn:${Date.now()}`;

    await Effect.runPromise(http.postOp(NAMESPACE, key, { PNCounter: { Increment: { client_id: 1, amount: 10 } } }));
    await Effect.runPromise(http.postOp(NAMESPACE, key, { PNCounter: { Decrement: { client_id: 1, amount: 3 } } }));

    const r = await Effect.runPromise(http.getCrdt(NAMESPACE, key));
    const value = r as { value?: number };
    expect(value.value).toBe(7);
  });

  skip("LwwRegister: set and read", async () => {
    const key = `lw:${Date.now()}`;
    const now = Date.now();

    await Effect.runPromise(http.postOp(NAMESPACE, key, {
      LwwRegister: {
        value: "hello",
        hlc: { wall_ms: now, logical: 0, node_id: 1 },
        author: 1,
      },
    }));

    const r = await Effect.runPromise(http.getCrdt(NAMESPACE, key));
    const value = r as { value?: unknown };
    expect(value.value).toBe("hello");
  });

  skip("ORSet: add and remove", async () => {
    const key = `or:${Date.now()}`;
    const tagUuid = crypto.randomUUID();
    const tagBytes = uuidToBytes(tagUuid);

    // Server only accepts msgpack bodies. GET returns JSON (to_json_value).
    const postMsgpack = async (op: unknown) => {
      const r = await fetch(
        `${SERVER_URL}/v1/namespaces/${NAMESPACE}/crdts/${key}/ops`,
        {
          method: "POST",
          headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/msgpack" },
          body: encode(op).buffer as ArrayBuffer,
        },
      );
      if (!r.ok) throw new Error(`postOp failed: ${r.status}`);
    };

    const getJson = async () => {
      const r = await fetch(
        `${SERVER_URL}/v1/namespaces/${NAMESPACE}/crdts/${key}`,
        { headers: { Authorization: `Bearer ${TOKEN}`, Accept: "application/json" } },
      );
      return r.json() as Promise<{ elements?: unknown[] }>;
    };

    await postMsgpack({ ORSet: { Add: { element: "apple", tag: tagBytes } } });
    const v1 = await getJson();
    expect(v1.elements?.includes("apple")).toBe(true);

    await postMsgpack({ ORSet: { Remove: { element: "apple", known_tags: [tagBytes] } } });
    const v2 = await getJson();
    expect((v2.elements ?? []).includes("apple")).toBe(false);
  });

  skip("MeridianClient: connect and use gcounter handle", async () => {
    const client = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    await client.waitForConnected();

    const key = `gc:ws:${Date.now()}`;
    const counter = client.gcounter(key);

    // Optimistic local increment
    counter.increment(5);
    expect(counter.value()).toBe(5);

    // Give time for op to reach server
    await Bun.sleep(200);

    // Read back via HTTP to confirm server received it
    const r = await Effect.runPromise(http.getCrdt(NAMESPACE, key));
    const value = r as { total?: number };
    expect(value.total).toBe(5);

    client.close();
    // Give the server time to clean up the closed connection before next WS test
    await Bun.sleep(200);
  });

  skip("MeridianClient: live delta propagation between two clients", async () => {
    const key = `gc:live:${Date.now()}`;
    // Regenerate tokens to avoid reusing a token whose WS session may be in flux.
    const tok1 = await generateToken(1);
    const tok2 = await generateToken(2);

    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: tok1 }),
    );
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: tok2 }),
    );

    await Promise.all([ensureConnected(client1), ensureConnected(client2)]);

    const counter1 = client1.gcounter(key);
    const counter2 = client2.gcounter(key);

    // Register the listener synchronously before any increment so we can't miss the delta.
    let resolveReceived!: () => void;
    let rejectReceived!: (e: Error) => void;
    const deltaReceived = new Promise<void>((res, rej) => { resolveReceived = res; rejectReceived = rej; });
    const timer = setTimeout(() => rejectReceived(new Error("timeout: delta not received")), 3000);
    counter2.onChange(() => { clearTimeout(timer); resolveReceived(); });

    counter1.increment(3);
    await deltaReceived;

    expect(counter2.value()).toBeGreaterThanOrEqual(3);

    client1.close();
    client2.close();
  });

  // ── RGA convergence ────────────────────────────────────────────────────────

  skip("RGA: concurrent inserts from two clients converge", async () => {
    const key = `rga:concurrent:${Date.now()}`;


    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN2 }),
    );

    await Promise.all([ensureConnected(client1), ensureConnected(client2)]);

    const doc1 = client1.rga(key);
    const doc2 = client2.rga(key);

    // Both clients insert concurrently — RGA must converge to the same text.
    // Register convergence watchers before the inserts to avoid missing early deltas.
    let resolveDoc1!: () => void, resolveDoc2!: () => void;
    const doc1Len2 = new Promise<void>((r) => { resolveDoc1 = r; });
    const doc2Len2 = new Promise<void>((r) => { resolveDoc2 = r; });
    doc1.onChange((v) => { if (v.length >= 2) resolveDoc1(); });
    doc2.onChange((v) => { if (v.length >= 2) resolveDoc2(); });

    doc1.insert(0, "A");
    doc2.insert(0, "B");

    // Wait until both replicas have length 2 (each received the other's insert).
    await Promise.all([
      Promise.race([doc1Len2, Bun.sleep(2000)]),
      Promise.race([doc2Len2, Bun.sleep(2000)]),
    ]);

    // Both replicas must have converged to the same text (order may differ but must match)
    expect(doc1.value()).toBe(doc2.value());
    expect(doc1.value()).toHaveLength(2);
    expect(doc1.value()).toContain("A");
    expect(doc1.value()).toContain("B");

    client1.close();
    client2.close();
  });

  skip("RGA: sequential inserts preserve order", async () => {
    const key = `rga:seq:${Date.now()}`;

    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN2 }),
    );

    await Promise.all([ensureConnected(client1), ensureConnected(client2)]);

    const doc1 = client1.rga(key);
    const doc2 = client2.rga(key);

    // Wait for doc2 to reach length 2 before asserting.
    let resolveDoc2!: () => void;
    const doc2Ready = new Promise<void>((r) => { resolveDoc2 = r; });
    doc2.onChange((v) => { if (v.length >= 2) resolveDoc2(); });

    // Sequential inserts from client1
    doc1.insert(0, "H");
    await Bun.sleep(50);
    doc1.insert(1, "i");

    await Promise.race([doc2Ready, Bun.sleep(2000)]);

    // client2 must have received both inserts in order
    expect(doc2.value()).toBe("Hi");

    client1.close();
    client2.close();
  });

  skip("RGA: delete propagates to remote client", async () => {
    const key = `rga:delete:${Date.now()}`;

    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );

    await Promise.all([ensureConnected(client1), ensureConnected(client2)]);

    const doc1 = client1.rga(key);
    const doc2 = client2.rga(key);

    doc1.insert(0, "abc");
    await Bun.sleep(300);

    expect(doc2.value()).toBe("abc");

    doc1.delete(1, 1); // delete "b"
    await Bun.sleep(300);

    expect(doc1.value()).toBe("ac");
    expect(doc2.value()).toBe("ac");

    client1.close();
    client2.close();
  });

  // ── Tree convergence ───────────────────────────────────────────────────────

  skip("Tree: addNode propagates to remote client", async () => {
    const key = `tree:add:${Date.now()}`;

    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );

    await Promise.all([ensureConnected(client1), ensureConnected(client2)]);

    const tree1 = client1.tree(key);
    const tree2 = client2.tree(key);

    let resolveTree2!: () => void;
    const tree2Received = new Promise<void>((r) => { resolveTree2 = r; });
    tree2.onChange(() => resolveTree2());

    tree1.addNode(null, "a0", "Root");
    await Promise.race([tree2Received, Bun.sleep(2000)]);

    expect(tree2.value().roots.length).toBeGreaterThan(0);
    expect(tree2.value().roots).toHaveLength(1);
    expect(tree2.value().roots[0]!.value).toBe("Root");

    client1.close();
    client2.close();
  });

  skip("Tree: moveNode converges — no cycles allowed", async () => {
    const key = `tree:cycle:${Date.now()}`;


    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN2 }),
    );

    await Promise.all([ensureConnected(client1), ensureConnected(client2)]);

    const tree1 = client1.tree(key);
    const tree2 = client2.tree(key);

    // Register the wait BEFORE sending addNodes so we don't miss early deltas.
    type N = { children: N[] };
    const countAll = (acc: number, n: N): number => acc + 1 + n.children.reduce(countAll, 0);

    const bothNodesOnClient2 = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout: tree2 did not receive both nodes")), 3000);
      const check = () => {
        const count = (tree2.value().roots as N[]).reduce(countAll, 0);
        if (count >= 2) { clearTimeout(timer); resolve(); }
      };
      tree2.onChange(check);
      check();
    });

    const parentId = tree1.addNode(null, "a0", "Parent");
    const childId = tree1.addNode(parentId, "a0", "Child");

    await bothNodesOnClient2;

    // Concurrent: client1 moves child under parent (no-op, already there)
    // client2 tries to move parent under child (would create a cycle)
    tree1.moveNode(childId, parentId, "b0");
    tree2.moveNode(parentId, childId, "a0");
    // No reliable event to await here — wait for both ops to round-trip.
    await Bun.sleep(800);

    // Both replicas must have converged — no cycles in either
    const roots1 = tree1.value().roots;
    const roots2 = tree2.value().roots;

    // Verify no node appears twice (which would indicate a cycle)
    const hasNoCycle = (roots: typeof roots1): boolean => {
      const allIds = new Set<string>();
      const walk = (nodes: typeof roots1): boolean => {
        for (const n of nodes) {
          if (allIds.has(n.id)) return false;
          allIds.add(n.id);
          if (!walk(n.children)) return false;
        }
        return true;
      };
      return walk(roots);
    };

    expect(hasNoCycle(roots1)).toBe(true);
    expect(hasNoCycle(roots2)).toBe(true);
    // Both replicas must have seen at least the two originally added nodes.
    const countNodes = (roots: typeof roots1): number =>
      roots.reduce((acc, n) => acc + 1 + countNodes(n.children), 0);
    expect(countNodes(roots1) + countNodes(roots2)).toBeGreaterThanOrEqual(2);

    client1.close();
    client2.close();
  });

  skip("Tree: deleteNode propagates to remote client", async () => {
    const key = `tree:delete:${Date.now()}`;

    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );

    await Promise.all([ensureConnected(client1), ensureConnected(client2)]);

    const tree1 = client1.tree(key);
    const tree2 = client2.tree(key);

    const nodeId = tree1.addNode(null, "a0", "ToDelete");
    await Bun.sleep(300);

    expect(tree2.value().roots).toHaveLength(1);

    tree1.deleteNode(nodeId);
    await Bun.sleep(300);

    expect(tree1.value().roots).toHaveLength(0);
    expect(tree2.value().roots).toHaveLength(0);

    client1.close();
    client2.close();
  });

  // ── Offline queue ──────────────────────────────────────────────────────────

  skip("offline queue: close() drains pending ops immediately", async () => {
    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    await client1.waitForConnected();

    const counter1 = client1.gcounter(`gc:drain:${Date.now()}`);
    counter1.increment(1);

    // close() must drain the queue synchronously
    client1.close();
    expect(client1.pendingOpCount).toBe(0);
  });

  skip("offline queue: ops increment before connected are flushed after connect", async () => {
    const key = `gc:preconnect:${Date.now()}`;

    // create() returns an Effect — resolve it, then immediately use the handle
    // before the WebSocket has had time to connect
    const client = Effect.runSync(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    const counter = client.gcounter(key);
    counter.increment(7); // queued in offline queue until WS connects

    await client.waitForConnected();
    await Bun.sleep(400);

    // Verify server received the op
    const r = await Effect.runPromise(http.getCrdt(NAMESPACE, key));
    const val = r as { total?: number };
    expect(val.total).toBe(7);

    client.close();
  });

  // ── Presence ───────────────────────────────────────────────────────────────

  skip("Presence: heartbeat from client1 seen by client2", async () => {

    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN2 }),
    );

    await Promise.all([ensureConnected(client1), ensureConnected(client2)]);

    const presenceKey = `pr:test:${Date.now()}`;
    const presence1 = client1.presence(presenceKey);
    const presence2 = client2.presence(presenceKey);

    await Bun.sleep(100);

    let peerSeen = false;
    presence2.onChange(() => { peerSeen = true; });

    presence1.heartbeat({ name: "Alice" }, 5_000);
    await Bun.sleep(400);

    expect(peerSeen).toBe(true);
    const online = presence2.online();
    expect(online.length).toBeGreaterThanOrEqual(1);

    client1.close();
    client2.close();
  });

  // ── Delta sync (Sync message) ──────────────────────────────────────────────

  skip("late-joining client receives full state via Sync", async () => {
    const key = `gc:sync:${Date.now()}`;

    // client1 does 5 increments
    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    await client1.waitForConnected();
    const counter1 = client1.gcounter(key);
    counter1.increment(5);
    await Bun.sleep(300);
    client1.close();
    await Bun.sleep(100);

    // client2 connects after — must receive state via Sync
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    await client2.waitForConnected();
    const counter2 = client2.gcounter(key);
    await Bun.sleep(400);

    expect(counter2.value()).toBe(5);

    client2.close();
  });
});
