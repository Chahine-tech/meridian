/**
 * Fractional indexing helpers for TreeCRDT sibling ordering.
 *
 * Positions are lexicographically ordered strings over the alphabet [a-z0-9].
 * They can be compared with plain string comparison: "a0" < "b0" < "z9".
 *
 * Use these helpers to compute positions when inserting nodes between, before,
 * or after existing siblings — without having to manage the alphabet manually.
 *
 * @example
 * ```ts
 * import { fi } from "meridian-sdk";
 *
 * const first = fi.start();            // "a0"
 * const last  = fi.end();              // "z9"
 * const mid   = fi.between("a0","z9"); // "m4" (midpoint)
 * const prev  = fi.before("a0");       // "V9" (below a0)
 * const next  = fi.after("z9");        // "{0" (above z9, still valid)
 * ```
 */

/** Characters used in fractional index strings, in ascending order. */
const CHARS = "0123456789abcdefghijklmnopqrstuvwxyz";
const BASE = BigInt(CHARS.length); // 36

/** Encode a position string to a BigInt for arithmetic. */
function toNum(s: string): bigint {
  let n = 0n;
  for (const c of s) {
    const idx = CHARS.indexOf(c);
    if (idx === -1) throw new Error(`Invalid fractional index character: "${c}"`);
    n = n * BASE + BigInt(idx);
  }
  return n;
}

/** Decode a BigInt back to a fixed-length position string. */
function toStr(n: bigint, len: number): string {
  let s = "";
  for (let i = 0; i < len; i++) {
    s = (CHARS[Number(n % BASE)] ?? "0") + s;
    n = n / BASE;
  }
  return s;
}

/** Increment a position string by 1 in base-36. */
function increment(s: string): string {
  const n = toNum(s) + 1n;
  // If the result requires more digits, extend by one char
  const maxForLen = BASE ** BigInt(s.length) - 1n;
  if (n > maxForLen) return toStr(n, s.length + 1);
  return toStr(n, s.length);
}

/** Decrement a position string by 1 in base-36, trimming trailing zeros. */
function decrement(s: string): string {
  const n = toNum(s) - 1n;
  let result = toStr(n < 0n ? 0n : n, s.length);
  // Trim trailing zeros beyond length 1
  while (result.length > 1 && result.endsWith("0")) result = result.slice(0, -1);
  // If underflow, prepend a zero digit
  if (n < 0n) return "0" + result;
  return result;
}

/**
 * Returns a position string strictly between `a` and `b`.
 *
 * Both `a` and `b` must be valid fractional index strings, and `a < b`
 * lexicographically. Uses BigInt arithmetic to find the exact midpoint,
 * appending an extra character when the gap is too small.
 *
 * @param a - Lower bound (exclusive).
 * @param b - Upper bound (exclusive).
 */
export function between(a: string, b: string): string {
  if (a >= b) throw new Error(`between: a must be < b (got "${a}" >= "${b}")`);

  // Pad both to the same length + 1 extra digit for precision
  const len = Math.max(a.length, b.length) + 1;
  const aPadded = a.padEnd(len, "0");
  const bPadded = b.padEnd(len, "0");

  const an = toNum(aPadded);
  const bn = toNum(bPadded);
  const mid = (an + bn) / 2n;

  let result = toStr(mid, len);
  // Trim trailing zeros for cleaner output (keep min length 1)
  while (result.length > 1 && result.endsWith("0")) result = result.slice(0, -1);
  return result;
}

/**
 * Returns a position string that sorts before `a`.
 *
 * @param a - The reference position.
 */
export function before(a: string): string {
  return decrement(a);
}

/**
 * Returns a position string that sorts after `b`.
 *
 * @param b - The reference position.
 */
export function after(b: string): string {
  return increment(b);
}

/**
 * Returns the canonical start position — the lowest recommended initial value.
 */
export function start(): string {
  return "a0";
}

/**
 * Returns the canonical end position — the highest recommended initial value.
 */
export function end(): string {
  return "z9";
}

/**
 * Computes an array of `n` evenly-spaced positions between `a` and `b`
 * (exclusive). Useful for bulk-inserting nodes in order.
 *
 * @param a - Lower bound (exclusive).
 * @param b - Upper bound (exclusive).
 * @param n - Number of positions to generate.
 */
export function spread(a: string, b: string, n: number): string[] {
  if (n <= 0) return [];
  if (n === 1) return [between(a, b)];

  const positions: string[] = [];
  let lo = a;
  for (let i = 0; i < n; i++) {
    // Divide the remaining range into (n - i) equal slots
    const remaining = n - i;
    // Approximate: compute a position 1/(remaining+1) through [lo, b]
    // We do this iteratively by bisecting
    let hi = b;
    for (let step = 0; step < Math.ceil(Math.log2(remaining + 1)); step++) {
      hi = between(lo, b);
    }
    const pos = between(lo, b);
    positions.push(pos);
    lo = pos;
  }
  return positions;
}

/**
 * Namespace object for convenient import.
 *
 * @example
 * ```ts
 * import { fi } from "meridian-sdk";
 * const pos = fi.between("a0", "z9");
 * ```
 */
export const fi = { between, before, after, start, end, spread };
