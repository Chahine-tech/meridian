import { describe, it, expect } from "bun:test";
import { TreeHandle } from "../src/crdt/tree.js";
import { RGAHandle } from "../src/crdt/rga.js";
import { CrdtValidationError, zodValidator, fnValidator } from "../src/validation/index.js";
import type { WsTransport } from "../src/transport/websocket.js";

function stubTransport(): WsTransport {
  return {
    connect: () => {},
    close: () => {},
    send: () => {},
    onStateChange: () => () => {},
    onMessage: () => () => {},
    pendingCount: () => 0,
  } as unknown as WsTransport;
}

function makeTree(validator?: Parameters<typeof TreeHandle.prototype.constructor>[0]["validator"]) {
  return new TreeHandle({ crdtId: "t", clientId: 1, transport: stubTransport(), validator });
}

function makeRga(validator?: Parameters<typeof RGAHandle.prototype.constructor>[0]["validator"]) {
  return new RGAHandle({ crdtId: "r", clientId: 1, transport: stubTransport(), validator });
}

// ── fnValidator ──────────────────────────────────────────────────────────────

describe("fnValidator", () => {
  it("accepts when fn returns true", () => {
    const v = fnValidator(() => true);
    expect(() => {
      const tree = makeTree(v);
      tree.addNode(null, "a0", "anything");
    }).not.toThrow();
  });

  it("rejects when fn returns false", () => {
    const v = fnValidator(() => false);
    const tree = makeTree(v);
    expect(() => tree.addNode(null, "a0", "x")).toThrow(CrdtValidationError);
  });

  it("uses string return as error message", () => {
    const v = fnValidator(() => "must be non-empty");
    const tree = makeTree(v);
    let err: unknown;
    try { tree.addNode(null, "a0", ""); } catch (e) { err = e; }
    expect(err).toBeInstanceOf(CrdtValidationError);
    expect((err as CrdtValidationError).reason).toBe("must be non-empty");
  });

  it("captures thrown errors as reason", () => {
    const v = fnValidator(() => { throw new Error("boom"); });
    const tree = makeTree(v);
    let err: unknown;
    try { tree.addNode(null, "a0", "x"); } catch (e) { err = e; }
    expect(err).toBeInstanceOf(CrdtValidationError);
    expect((err as CrdtValidationError).reason).toBe("boom");
  });
});

// ── zodValidator ─────────────────────────────────────────────────────────────

describe("zodValidator", () => {
  // Minimal stub matching Zod's safeParse API — no zod dependency needed
  const schema = {
    safeParse(value: unknown) {
      if (typeof value === "object" && value !== null && "title" in value && typeof (value as Record<string, unknown>)["title"] === "string") {
        return { success: true as const };
      }
      return { success: false as const, error: { message: "title must be a string" } };
    },
  };

  it("accepts valid JSON matching schema", () => {
    const tree = makeTree(zodValidator(schema));
    expect(() => tree.addNode(null, "a0", JSON.stringify({ title: "Task" }))).not.toThrow();
  });

  it("rejects invalid JSON value", () => {
    const tree = makeTree(zodValidator(schema));
    expect(() => tree.addNode(null, "a0", JSON.stringify({ title: 42 }))).toThrow(CrdtValidationError);
  });

  it("passes raw string to schema when not valid JSON", () => {
    // Schema rejects non-objects — plain string "hello" fails
    const tree = makeTree(zodValidator(schema));
    expect(() => tree.addNode(null, "a0", "hello")).toThrow(CrdtValidationError);
  });
});

// ── TreeHandle integration ───────────────────────────────────────────────────

describe("TreeHandle validation", () => {
  it("validates addNode value", () => {
    const tree = makeTree(fnValidator((v) => v.length > 0 || "empty"));
    expect(() => tree.addNode(null, "a0", "")).toThrow(CrdtValidationError);
    expect(() => tree.addNode(null, "a0", "valid")).not.toThrow();
  });

  it("validates updateNode value", () => {
    const tree = makeTree(fnValidator((v) => v !== "banned" || "banned word"));
    const id = tree.addNode(null, "a0", "ok");
    expect(() => tree.updateNode(id, "banned")).toThrow(CrdtValidationError);
    expect(() => tree.updateNode(id, "fine")).not.toThrow();
  });

  it("no validator — all values accepted", () => {
    const tree = makeTree();
    expect(() => tree.addNode(null, "a0", "")).not.toThrow();
    expect(() => tree.addNode(null, "a0", "anything")).not.toThrow();
  });
});

// ── RGAHandle integration ────────────────────────────────────────────────────

describe("RGAHandle validation", () => {
  it("validates insert text", () => {
    const rga = makeRga(fnValidator((v) => !v.includes("<") || "no HTML"));
    expect(() => rga.insert(0, "<script>")).toThrow(CrdtValidationError);
    expect(() => rga.insert(0, "hello")).not.toThrow();
  });

  it("no validator — all text accepted", () => {
    const rga = makeRga();
    expect(() => rga.insert(0, "<anything>")).not.toThrow();
  });

  it("CrdtValidationError has value and reason fields", () => {
    const rga = makeRga(fnValidator(() => "nope"));
    let err: unknown;
    try { rga.insert(0, "bad"); } catch (e) { err = e; }
    expect(err).toBeInstanceOf(CrdtValidationError);
    expect((err as CrdtValidationError).value).toBe("bad");
    expect((err as CrdtValidationError).reason).toBe("nope");
  });
});
