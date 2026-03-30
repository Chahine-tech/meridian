/**
 * Client-side permission evaluation — mirrors the Rust `claims.rs` logic exactly.
 *
 * Lets callers check `canRead` / `canWrite` locally without a round-trip to the
 * server. Useful for disabling UI elements before attempting an op that would
 * fail with 403.
 */

import type { Permissions, PermEntry } from "../schema.js";

// ---------------------------------------------------------------------------
// Op mask constants — must stay in sync with server `op_masks` module
// ---------------------------------------------------------------------------

export const OpMasks = {
  /** Allow all ops (sentinel — 0 means no restriction). */
  ALL: 0,

  // GCounter / PNCounter
  GC_INCREMENT: 0b0000_0001,
  PN_INCREMENT: 0b0000_0001,
  PN_DECREMENT: 0b0000_0010,

  // ORSet
  OR_ADD:    0b0000_0001,
  OR_REMOVE: 0b0000_0010,

  // LWW Register
  LWW_SET: 0b0000_0001,

  // Presence
  PRESENCE_UPDATE: 0b0000_0001,

  // RGA
  RGA_INSERT: 0b0000_0001,
  RGA_DELETE: 0b0000_0010,

  // Tree
  TREE_ADD:    0b0000_0001,
  TREE_MOVE:   0b0000_0010,
  TREE_UPDATE: 0b0000_0100,
  TREE_DELETE: 0b0000_1000,

  // CRDTMap
  MAP_WRITE: 0b0000_0001,
} as const;

export type OpMask = number;

// ---------------------------------------------------------------------------
// Glob matching — mirrors Rust `glob_match`
// ---------------------------------------------------------------------------

function globMatch(pattern: string, value: string): boolean {
  if (pattern === "*") return true;
  const starPos = pattern.indexOf("*");
  if (starPos === -1) return pattern === value;

  const prefix = pattern.slice(0, starPos);
  const suffix = pattern.slice(starPos + 1);
  if (!value.startsWith(prefix)) return false;
  const rest = value.slice(prefix.length);
  if (suffix === "") return true;
  for (let i = 0; i <= rest.length; i++) {
    if (globMatch(suffix, rest.slice(i))) return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// PermEntry evaluation — mirrors Rust `PermEntry::matches`
// ---------------------------------------------------------------------------

function isAllMask(mask: number): boolean {
  return mask === 0 || mask === 0xffff;
}

function entryMatches(
  entry: PermEntry,
  crdtId: string,
  opMask: OpMask,
  nowMs: number,
  clientId: number,
): boolean {
  // TTL gate
  if (entry.e !== undefined && nowMs >= entry.e) return false;

  // Expand {clientId} placeholder
  const pattern = entry.p.includes("{clientId}")
    ? entry.p.replaceAll("{clientId}", String(clientId))
    : entry.p;

  if (!globMatch(pattern, crdtId)) return false;

  const ruleOp = entry.o ?? OpMasks.ALL;
  if (isAllMask(ruleOp) || isAllMask(opMask)) return true;
  return (opMask & ruleOp) !== 0;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Check whether the token's permissions allow reading `crdtId`.
 *
 * @param permissions - parsed from `TokenClaims.permissions`
 * @param crdtId      - e.g. `"gc:views"`, `"or:cart-42"`
 * @param clientId    - `TokenClaims.client_id` (needed for `{clientId}` expansion)
 * @param nowMs       - current time in ms (default: `Date.now()`)
 */
export function canRead(
  permissions: Permissions,
  crdtId: string,
  clientId: number,
  nowMs = Date.now(),
): boolean {
  if ("v" in permissions) {
    // V2
    return permissions.r.some((e) => entryMatches(e, crdtId, OpMasks.ALL, nowMs, clientId));
  }
  // V1
  return permissions.read.some((p) => globMatch(p, crdtId));
}

/**
 * Check whether the token's permissions allow writing `crdtId`.
 *
 * @param opMask - op-level bitmask (e.g. `OpMasks.OR_ADD`). Omit or pass `0` to
 *                 check key-level access without op granularity (V2 rule with a
 *                 mask will still be matched if the caller passes `ALL=0`).
 */
export function canWrite(
  permissions: Permissions,
  crdtId: string,
  clientId: number,
  opMask: OpMask = OpMasks.ALL,
  nowMs = Date.now(),
): boolean {
  if ("v" in permissions) {
    // V2
    return permissions.w.some((e) => entryMatches(e, crdtId, opMask, nowMs, clientId));
  }
  // V1
  return permissions.write.some((p) => globMatch(p, crdtId));
}
