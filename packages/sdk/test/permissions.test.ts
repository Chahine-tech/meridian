/**
 * Unit tests for client-side permission evaluation.
 *
 * Logic must mirror the Rust `PermEntry::matches` / `glob_match` exactly.
 * No server required.
 */

import { describe, it, expect } from "bun:test";
import { canRead, canWrite, OpMasks } from "../src/auth/permissions.js";
import type { Permissions } from "../src/schema.js";

// Helpers

const v1 = (read: string[], write: string[]): Permissions => ({
  read,
  write,
  admin: false,
});

const v2 = (
  r: Array<{ p: string; o?: number; e?: number }>,
  w: Array<{ p: string; o?: number; e?: number }>,
): Permissions => ({ v: 2, r, w, admin: false });

// glob matching

describe("canRead — V1 glob matching", () => {
  it("wildcard * grants access to everything", () => {
    const p = v1(["*"], []);
    expect(canRead(p, "gc:views", 1)).toBe(true);
    expect(canRead(p, "or:cart-42", 1)).toBe(true);
    expect(canRead(p, "", 1)).toBe(true);
  });

  it("prefix glob matches only matching keys", () => {
    const p = v1(["gc:*"], []);
    expect(canRead(p, "gc:views", 1)).toBe(true);
    expect(canRead(p, "gc:downloads", 1)).toBe(true);
    expect(canRead(p, "or:tags", 1)).toBe(false);
  });

  it("exact pattern matches only that key", () => {
    const p = v1(["lw:title"], []);
    expect(canRead(p, "lw:title", 1)).toBe(true);
    expect(canRead(p, "lw:body", 1)).toBe(false);
  });

  it("multiple patterns — any match wins", () => {
    const p = v1(["gc:*", "lw:title"], []);
    expect(canRead(p, "gc:views", 1)).toBe(true);
    expect(canRead(p, "lw:title", 1)).toBe(true);
    expect(canRead(p, "or:tags", 1)).toBe(false);
  });

  it("empty read list — no access", () => {
    const p = v1([], []);
    expect(canRead(p, "gc:views", 1)).toBe(false);
  });

  it("mid-string glob", () => {
    const p = v1(["or:cart-*"], []);
    expect(canRead(p, "or:cart-42", 1)).toBe(true);
    expect(canRead(p, "or:cart-", 1)).toBe(true);
    expect(canRead(p, "or:tags", 1)).toBe(false);
  });

  it("multiple wildcards", () => {
    const p = v1(["gc:*-*"], []);
    expect(canRead(p, "gc:daily-views", 1)).toBe(true);
    expect(canRead(p, "gc:views", 1)).toBe(false);
  });
});

describe("canWrite — V1", () => {
  it("write list scoped correctly", () => {
    const p = v1(["*"], ["or:cart-42"]);
    expect(canWrite(p, "or:cart-42", 1)).toBe(true);
    expect(canWrite(p, "or:cart-99", 1)).toBe(false);
    expect(canWrite(p, "gc:views", 1)).toBe(false);
  });

  it("glob write pattern", () => {
    const p = v1([], ["gc:*"]);
    expect(canWrite(p, "gc:views", 1)).toBe(true);
    expect(canWrite(p, "or:tags", 1)).toBe(false);
  });
});

// V2 — key-level (no op mask)

describe("canRead — V2 key-level", () => {
  it("first-match-wins: read granted by first rule", () => {
    const p = v2([{ p: "*" }], []);
    expect(canRead(p, "gc:views", 1)).toBe(true);
  });

  it("no matching rule — denied", () => {
    const p = v2([{ p: "gc:*" }], []);
    expect(canRead(p, "or:tags", 1)).toBe(false);
  });

  it("empty rules — denied", () => {
    const p = v2([], []);
    expect(canRead(p, "gc:views", 1)).toBe(false);
  });
});

// V2 — op masks

describe("canWrite — V2 op masks", () => {
  it("no mask on rule (ALL) — any op allowed", () => {
    const p = v2([], [{ p: "or:cart" }]);
    expect(canWrite(p, "or:cart", 1, OpMasks.OR_ADD)).toBe(true);
    expect(canWrite(p, "or:cart", 1, OpMasks.OR_REMOVE)).toBe(true);
  });

  it("mask restricts to specific op", () => {
    const p = v2([], [{ p: "gc:views", o: OpMasks.GC_INCREMENT }]);
    expect(canWrite(p, "gc:views", 1, OpMasks.GC_INCREMENT)).toBe(true);
    expect(canWrite(p, "gc:views", 1, OpMasks.PN_DECREMENT)).toBe(false);
  });

  it("combined mask allows both ops", () => {
    const p = v2([], [{ p: "or:cart", o: OpMasks.OR_ADD | OpMasks.OR_REMOVE }]);
    expect(canWrite(p, "or:cart", 1, OpMasks.OR_ADD)).toBe(true);
    expect(canWrite(p, "or:cart", 1, OpMasks.OR_REMOVE)).toBe(true);
  });

  it("caller passes ALL (0) — key-level check ignores rule mask", () => {
    // canWrite without opMask should succeed even if rule has a mask
    const p = v2([], [{ p: "gc:views", o: OpMasks.GC_INCREMENT }]);
    expect(canWrite(p, "gc:views", 1, OpMasks.ALL)).toBe(true);
    expect(canWrite(p, "gc:views", 1)).toBe(true); // default = ALL
  });

  it("tree op masks", () => {
    const p = v2([], [{ p: "tr:doc", o: OpMasks.TREE_ADD | OpMasks.TREE_MOVE }]);
    expect(canWrite(p, "tr:doc", 1, OpMasks.TREE_ADD)).toBe(true);
    expect(canWrite(p, "tr:doc", 1, OpMasks.TREE_MOVE)).toBe(true);
    expect(canWrite(p, "tr:doc", 1, OpMasks.TREE_DELETE)).toBe(false);
    expect(canWrite(p, "tr:doc", 1, OpMasks.TREE_UPDATE)).toBe(false);
  });
});

// V2 — per-rule TTL

describe("V2 — per-rule expiry", () => {
  it("expired rule is skipped", () => {
    const past = Date.now() - 10_000;
    const p = v2([{ p: "*", e: past }], [{ p: "*", e: past }]);
    expect(canRead(p, "gc:views", 1)).toBe(false);
    expect(canWrite(p, "gc:views", 1)).toBe(false);
  });

  it("future expiry — rule still active", () => {
    const future = Date.now() + 60_000;
    const p = v2([{ p: "*", e: future }], [{ p: "*", e: future }]);
    expect(canRead(p, "gc:views", 1)).toBe(true);
    expect(canWrite(p, "gc:views", 1)).toBe(true);
  });

  it("expired first rule, valid fallback rule", () => {
    const past = Date.now() - 10_000;
    const p = v2([{ p: "gc:*", e: past }, { p: "*" }], []);
    // gc:* rule expired — falls through to * rule
    expect(canRead(p, "gc:views", 1)).toBe(true);
  });
});

// V2 — {clientId} template

describe("V2 — {clientId} template", () => {
  it("expands to the token's client_id", () => {
    const p = v2([{ p: "or:cart-{clientId}" }], [{ p: "or:cart-{clientId}" }]);
    expect(canRead(p, "or:cart-42", 42)).toBe(true);
    expect(canWrite(p, "or:cart-42", 42)).toBe(true);
  });

  it("does not match another agent's key", () => {
    const p = v2([{ p: "or:cart-{clientId}" }], [{ p: "or:cart-{clientId}" }]);
    expect(canRead(p, "or:cart-99", 42)).toBe(false);
    expect(canWrite(p, "or:cart-99", 42)).toBe(false);
  });

  it("combines with glob wildcard", () => {
    const p = v2([{ p: "or:cart-{clientId}-*" }], []);
    expect(canRead(p, "or:cart-7-items", 7)).toBe(true);
    expect(canRead(p, "or:cart-7-wishlist", 7)).toBe(true);
    expect(canRead(p, "or:cart-8-items", 7)).toBe(false);
  });

  it("multiple {clientId} in same pattern", () => {
    const p = v2([{ p: "{clientId}:data-{clientId}" }], []);
    expect(canRead(p, "5:data-5", 5)).toBe(true);
    expect(canRead(p, "5:data-9", 5)).toBe(false);
  });

  it("different client_ids get different scopes", () => {
    const p = v2([], [{ p: "pr:agent-{clientId}" }]);
    expect(canWrite(p, "pr:agent-1", 1)).toBe(true);
    expect(canWrite(p, "pr:agent-2", 2)).toBe(true);
    expect(canWrite(p, "pr:agent-2", 1)).toBe(false);
    expect(canWrite(p, "pr:agent-1", 2)).toBe(false);
  });
});

// V2 — first-match-wins ordering

describe("V2 — first-match-wins", () => {
  it("deny-all first rule blocks even if later rule would match", () => {
    // The SDK doesn't have explicit deny rules, but first-match-wins means
    // a specific rule before a wildcard takes precedence.
    const p = v2(
      [{ p: "gc:views" }, { p: "*" }],
      [],
    );
    expect(canRead(p, "gc:views", 1)).toBe(true);  // matched by first rule
    expect(canRead(p, "or:tags", 1)).toBe(true);   // matched by * fallback
  });

  it("expired specific rule falls through to wildcard", () => {
    const past = Date.now() - 1;
    const p = v2(
      [{ p: "gc:views", e: past }, { p: "gc:*" }],
      [],
    );
    expect(canRead(p, "gc:views", 1)).toBe(true); // expired specific → wildcard gc:*
  });
});
