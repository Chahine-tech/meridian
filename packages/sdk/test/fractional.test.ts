import { describe, it, expect } from "bun:test";
import { fi, between, before, after, start, end, spread } from "../src/utils/fractional.js";

describe("fractional indexing — between()", () => {
  it("returns a string strictly between two positions", () => {
    const mid = between("a0", "z9");
    expect(mid > "a0").toBe(true);
    expect(mid < "z9").toBe(true);
  });

  it("works for adjacent single-char positions", () => {
    const mid = between("a0", "b0");
    expect(mid > "a0").toBe(true);
    expect(mid < "b0").toBe(true);
  });

  it("handles equal-length strings", () => {
    const mid = between("a0", "a2");
    expect(mid > "a0").toBe(true);
    expect(mid < "a2").toBe(true);
  });

  it("handles different-length strings", () => {
    const mid = between("a", "b0");
    expect(mid > "a").toBe(true);
    expect(mid < "b0").toBe(true);
  });

  it("subdivides when gap is minimal", () => {
    // "a0" and "a1" are adjacent — between should append a char
    const mid = between("a0", "a1");
    expect(mid > "a0").toBe(true);
    expect(mid < "a1").toBe(true);
  });

  it("throws when a >= b", () => {
    expect(() => between("b0", "a0")).toThrow();
    expect(() => between("a0", "a0")).toThrow();
  });

  it("can be called recursively to produce ordered sequence", () => {
    const p1 = between("a0", "z9");
    const p2 = between(p1, "z9");
    const p3 = between(p1, p2);
    expect("a0" < p1).toBe(true);
    expect(p1 < p2).toBe(true);
    expect(p1 < p3).toBe(true);
    expect(p3 < p2).toBe(true);
    expect(p2 < "z9").toBe(true);
  });
});

describe("fractional indexing — before() / after()", () => {
  it("before returns a string less than the input", () => {
    const p = before("a0");
    expect(p < "a0").toBe(true);
  });

  it("after returns a string greater than the input", () => {
    const p = after("z9");
    expect(p > "z9").toBe(true);
  });

  it("before/after round-trips are monotone", () => {
    const base = "m4";
    const b = before(base);
    const a = after(base);
    expect(b < base).toBe(true);
    expect(a > base).toBe(true);
    expect(b < a).toBe(true);
  });
});

describe("fractional indexing — start() / end()", () => {
  it("start < end", () => {
    expect(start() < end()).toBe(true);
  });

  it("between(start, end) is valid", () => {
    const mid = between(start(), end());
    expect(mid > start()).toBe(true);
    expect(mid < end()).toBe(true);
  });
});

describe("fractional indexing — spread()", () => {
  it("returns n positions in ascending order", () => {
    const positions = spread("a0", "z9", 4);
    expect(positions).toHaveLength(4);
    for (let i = 1; i < positions.length; i++) {
      expect(positions[i]! > positions[i - 1]!).toBe(true);
    }
  });

  it("all positions are within bounds", () => {
    const positions = spread("a0", "z9", 5);
    for (const p of positions) {
      expect(p > "a0").toBe(true);
      expect(p < "z9").toBe(true);
    }
  });

  it("returns empty array for n=0", () => {
    expect(spread("a0", "z9", 0)).toEqual([]);
  });

  it("returns single midpoint for n=1", () => {
    const [p] = spread("a0", "z9", 1);
    expect(p! > "a0").toBe(true);
    expect(p! < "z9").toBe(true);
  });
});

describe("fractional indexing — fi namespace", () => {
  it("fi object exposes all helpers", () => {
    expect(typeof fi.between).toBe("function");
    expect(typeof fi.before).toBe("function");
    expect(typeof fi.after).toBe("function");
    expect(typeof fi.start).toBe("function");
    expect(typeof fi.end).toBe("function");
    expect(typeof fi.spread).toBe("function");
  });

  it("fi.between matches named export", () => {
    expect(fi.between("a0", "z9")).toBe(between("a0", "z9"));
  });
});
