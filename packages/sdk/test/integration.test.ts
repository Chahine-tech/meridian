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
import { uuidToBytes } from "../src/codec.js";

// Config — matches the dev signing key [0x42; 32]
// The token below is for namespace="integration", client_id=1, TTL=24h
// Regenerate with: cargo run --bin gen_token -- integration 1
const SERVER_URL = "http://127.0.0.1:3737";
const NAMESPACE = "integration";

// Token signed with key [0x42;32], namespace="integration", client_id=1, admin perms, TTL 24h
// Generated via: cargo run --bin gen_token -- integration 1
let TOKEN: string;

async function generateToken(): Promise<string> {
  const proc = Bun.spawn(
    ["cargo", "run", "--quiet", "--bin", "gen_token", "--", NAMESPACE, "1"],
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
    TOKEN = await generateToken();
    http = new HttpClient({ baseUrl: SERVER_URL, token: TOKEN });
  });

  function skip(name: string, fn: () => unknown) {
    // If token is not set, server wasn't up — skip gracefully
    test(name, async () => {
      if (!TOKEN) return;
      await fn();
    });
  }

  // ---- HTTP REST ----

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
    // Rust Uuid is serialized as 16-byte bin — generate a proper UUID and encode as bytes
    const tagUuid = crypto.randomUUID();
    const tagBytes = uuidToBytes(tagUuid);

    // Add "apple"
    await Effect.runPromise(http.postOp(NAMESPACE, key, {
      ORSet: { Add: { element: "apple", tag: tagBytes } },
    }));

    const r1 = await Effect.runPromise(http.getCrdt(NAMESPACE, key));
    const v1 = r1 as { elements?: unknown[] };
    expect(v1.elements?.includes("apple")).toBe(true);

    // Remove "apple" — known_tags is a set of 16-byte UUIDs
    await Effect.runPromise(http.postOp(NAMESPACE, key, {
      ORSet: { Remove: { element: "apple", known_tags: [tagBytes] } },
    }));

    const r2 = await Effect.runPromise(http.getCrdt(NAMESPACE, key));
    const v2 = r2 as { elements?: unknown[] };
    expect((v2.elements ?? []).includes("apple")).toBe(false);
  });

  // ---- MeridianClient (WebSocket) ----

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

    const client1 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );
    const client2 = await Effect.runPromise(
      MeridianClient.create({ url: SERVER_URL, namespace: NAMESPACE, token: TOKEN }),
    );

    await Promise.all([client1.waitForConnected(), client2.waitForConnected()]);

    const counter1 = client1.gcounter(key);
    const counter2 = client2.gcounter(key);

    // Give time for subscriptions to be established
    await Bun.sleep(100);

    let received = false;
    counter2.onChange(() => { received = true; });

    counter1.increment(3);

    // Wait for delta to propagate
    await Bun.sleep(300);

    expect(received).toBe(true);
    expect(counter2.value()).toBeGreaterThanOrEqual(3);

    client1.close();
    client2.close();
  });
});
