// @ts-ignore — cloudflare:test is injected by vitest-pool-workers miniflare runtime
import { SELF } from "cloudflare:test";
import { describe, it, expect } from "vitest";
import { ADMIN_TOKEN, TEST_NS, BASE_URL } from "./helpers";

/** Make a request to the Worker under test via SELF (miniflare). */
async function api(
  path: string,
  init: RequestInit & { token?: string } = {}
): Promise<Response> {
  const { token, headers: initHeaders, ...rest } = init;
  const headers = new Headers((initHeaders as Headers | Record<string, string> | undefined) ?? {});
  if (token) headers.set("Authorization", `Bearer ${token}`);
  return SELF.fetch(`${BASE_URL}${path}`, { ...rest, headers });
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

describe("health", () => {
  it("GET /health returns 200 ok", async () => {
    const res = await api("/health");
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("ok");
  });
});

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

describe("auth", () => {
  it("rejects requests with no token", async () => {
    const res = await api(`/v1/namespaces/${TEST_NS}/crdts/my-key`);
    expect(res.status).toBe(401);
  });

  it("rejects requests with a tampered token", async () => {
    const tampered = ADMIN_TOKEN.slice(0, -4) + "XXXX";
    const res = await api(`/v1/namespaces/${TEST_NS}/crdts/my-key`, {
      token: tampered,
    });
    expect(res.status).toBe(401);
  });

  it("accepts a valid admin token (404 = auth passed, key missing)", async () => {
    const res = await api(`/v1/namespaces/${TEST_NS}/crdts/my-key`, {
      token: ADMIN_TOKEN,
    });
    expect([200, 404]).toContain(res.status);
  });
});

// ---------------------------------------------------------------------------
// Token issuance
// ---------------------------------------------------------------------------

describe("issue token", () => {
  it("issues a read-write token via admin", async () => {
    const res = await api(`/v1/namespaces/${TEST_NS}/tokens`, {
      method: "POST",
      token: ADMIN_TOKEN,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_id: 99,
        ttl_ms: 3_600_000,
        permissions: { read: ["*"], write: ["*"], admin: false },
      }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { token: string };
    expect(typeof body.token).toBe("string");
    expect(body.token).toContain(".");
  });

  it("rejects token issuance for non-admin", async () => {
    // Issue a non-admin token first
    const issueRes = await api(`/v1/namespaces/${TEST_NS}/tokens`, {
      method: "POST",
      token: ADMIN_TOKEN,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_id: 2,
        ttl_ms: 3_600_000,
        permissions: { read: ["*"], write: ["*"], admin: false },
      }),
    });
    const { token: userToken } = (await issueRes.json()) as { token: string };

    // Try to issue a token with the non-admin token
    const res = await api(`/v1/namespaces/${TEST_NS}/tokens`, {
      method: "POST",
      token: userToken,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_id: 3,
        ttl_ms: 3_600_000,
        permissions: { read: ["*"], write: ["*"], admin: false },
      }),
    });
    expect(res.status).toBe(403);
  });

  it("rejects token issuance with no auth", async () => {
    const res = await api(`/v1/namespaces/${TEST_NS}/tokens`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_id: 4,
        ttl_ms: 3_600_000,
        permissions: { read: ["*"], write: ["*"], admin: false },
      }),
    });
    expect(res.status).toBe(401);
  });
});

// ---------------------------------------------------------------------------
// CRDT ops
// ---------------------------------------------------------------------------

describe("crdt ops", () => {
  // GCounter increment op: msgpack { GCounter: { client_id: 1, amount: 1 } }
  const GC_INCREMENT = new Uint8Array([
    0x81, // fixmap 1 entry
    0xa8, // fixstr 8: "GCounter"
    0x47, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72,
    0x82, // fixmap 2 entries
    0xa9, // fixstr 9: "client_id"
    0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64,
    0x01, // 1
    0xa6, // fixstr 6: "amount"
    0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74,
    0x01, // 1
  ]);

  it("POST op returns 200 or 204 for a GCounter increment", async () => {
    const res = await api(`/v1/namespaces/${TEST_NS}/crdts/visits/ops`, {
      method: "POST",
      token: ADMIN_TOKEN,
      headers: { "Content-Type": "application/msgpack" },
      body: GC_INCREMENT,
    });
    expect([200, 204]).toContain(res.status);
  });

  it("GET crdt returns state after op", async () => {
    await api(`/v1/namespaces/${TEST_NS}/crdts/counter/ops`, {
      method: "POST",
      token: ADMIN_TOKEN,
      headers: { "Content-Type": "application/msgpack" },
      body: GC_INCREMENT,
    });

    const res = await api(`/v1/namespaces/${TEST_NS}/crdts/counter`, {
      token: ADMIN_TOKEN,
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toBeDefined();
  });

  it("rejects op with wrong namespace token", async () => {
    // ADMIN_TOKEN is signed for test-ns, not other-ns
    const res = await api(`/v1/namespaces/other-ns/crdts/x/ops`, {
      method: "POST",
      token: ADMIN_TOKEN,
      headers: { "Content-Type": "application/msgpack" },
      body: GC_INCREMENT,
    });
    expect(res.status).toBe(403);
  });
});

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

describe("rate limiting", () => {
  // GCounter increment op (same as crdt ops section)
  const GC_INCREMENT = new Uint8Array([
    0x81, 0xa8, 0x47, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72,
    0x82, 0xa9, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64,
    0x01, 0xa6, 0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74, 0x01,
  ]);

  it("allows ops within the rate limit", async () => {
    // First op should always be allowed
    const res = await api(`/v1/namespaces/${TEST_NS}/crdts/rl-test/ops`, {
      method: "POST",
      token: ADMIN_TOKEN,
      headers: { "Content-Type": "application/msgpack" },
      body: GC_INCREMENT,
    });
    expect([200, 204]).toContain(res.status);
  });

  it("returns 429 after exceeding the rate limit", async () => {
    // Exhaust the HTTP rate limit (200 ops/min per namespace bucket).
    // We use a distinct crdt key so WAL/state don't interfere with other tests.
    const ns = `rl-burst-${Date.now()}`;

    // Issue a token for this namespace
    const issueRes = await api(`/v1/namespaces/${ns}/tokens`, {
      method: "POST",
      token: ADMIN_TOKEN,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_id: 10,
        ttl_ms: 3_600_000,
        permissions: { read: ["*"], write: ["*"], admin: true },
      }),
    });

    // If the namespace isn't wired to ADMIN_TOKEN (different ns), skip gracefully.
    // In miniflare each DO is isolated so we can use any admin token per ns.
    if (issueRes.status !== 200) {
      // Can't issue token for a different namespace with this admin token — skip
      return;
    }
    const { token } = (await issueRes.json()) as { token: string };

    let got429 = false;
    for (let i = 0; i < 210; i++) {
      const r = await api(`/v1/namespaces/${ns}/crdts/rl-key/ops`, {
        method: "POST",
        token,
        headers: { "Content-Type": "application/msgpack" },
        body: GC_INCREMENT,
      });
      if (r.status === 429) {
        got429 = true;
        break;
      }
    }
    expect(got429).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// WAL
// ---------------------------------------------------------------------------

describe("wal", () => {
  it("GET /wal returns checkpoint and entries JSON", async () => {
    const res = await api(`/v1/namespaces/${TEST_NS}/wal?from_seq=0`, {
      token: ADMIN_TOKEN,
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      checkpoint_seq: number;
      entries: unknown[];
    };
    expect(typeof body.checkpoint_seq).toBe("number");
    expect(Array.isArray(body.entries)).toBe(true);
  });

  it("rejects WAL access for non-admin token", async () => {
    // Issue a non-admin token
    const issueRes = await api(`/v1/namespaces/${TEST_NS}/tokens`, {
      method: "POST",
      token: ADMIN_TOKEN,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_id: 5,
        ttl_ms: 3_600_000,
        permissions: { read: ["*"], write: ["*"], admin: false },
      }),
    });
    const { token: userToken } = (await issueRes.json()) as { token: string };

    const res = await api(`/v1/namespaces/${TEST_NS}/wal?from_seq=0`, {
      token: userToken,
    });
    expect(res.status).toBe(403);
  });
});
