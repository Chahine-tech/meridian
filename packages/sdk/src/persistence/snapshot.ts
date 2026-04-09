import { encode, decode } from "../codec.js";
import type { LwwEntry, TreeNodeValue } from "../sync/delta.js";
import type { CrdtMapValue } from "../crdt/crdtmap.js";
import { GCounterHandle } from "../crdt/gcounter.js";
import { PNCounterHandle } from "../crdt/pncounter.js";
import { LwwRegisterHandle } from "../crdt/lwwregister.js";
import { ORSetHandle } from "../crdt/orset.js";
import { PresenceHandle } from "../crdt/presence.js";
import { RGAHandle } from "../crdt/rga.js";
import { TreeHandle } from "../crdt/tree.js";
import { CRDTMapHandle } from "../crdt/crdtmap.js";

export type GCounterSnapshot = { type: "gcounter"; counters: Record<string, number> };
export type PNCounterSnapshot = { type: "pncounter"; pos: Record<string, number>; neg: Record<string, number> };
export type ORSetSnapshot = { type: "orset"; adds: Record<string, string[]> };
export type LwwSnapshot = { type: "lwwregister"; entry: LwwEntry | null };
export type PresenceSnapshot = { type: "presence"; entries: Record<string, { data: unknown; ttl_ms: number; wall_ms: number }> };
export type RGASnapshot = { type: "rga"; text: string };
export type TreeSnapshot = { type: "tree"; roots: TreeNodeValue[] };
export type CRDTMapSnapshot = { type: "crdtmap"; state: CrdtMapValue };

export type CRDTSnapshot =
  | GCounterSnapshot
  | PNCounterSnapshot
  | ORSetSnapshot
  | LwwSnapshot
  | PresenceSnapshot
  | RGASnapshot
  | TreeSnapshot
  | CRDTMapSnapshot;

export const snapshotToBytes = (snap: CRDTSnapshot): Uint8Array => encode(snap);

export const bytesToSnapshot = (bytes: Uint8Array): CRDTSnapshot | null => {
  try {
    const raw = decode(bytes);
    if (raw !== null && typeof raw === "object" && "type" in raw) {
      return raw as CRDTSnapshot;
    }
    return null;
  } catch {
    return null;
  }
};

export const snapshotFromGCounter = (h: GCounterHandle): GCounterSnapshot => ({
  type: "gcounter",
  counters: h.rawCounters(),
});

export const snapshotFromPNCounter = (h: PNCounterHandle): PNCounterSnapshot => {
  const s = h.rawState();
  return { type: "pncounter", pos: s.p, neg: s.n };
};

export const snapshotFromLww = (h: LwwLike): LwwSnapshot => ({
  type: "lwwregister",
  entry: h.rawEntry(),
});

interface ORSetLike {
  rawTags(): ReadonlyMap<string, ReadonlySet<string>>;
  restoreSnapshot(data: ORSetSnapshot): void;
}

interface LwwLike {
  rawEntry(): import("../sync/delta.js").LwwEntry | null;
  applyDelta(delta: { entry: import("../sync/delta.js").LwwEntry | null }): void;
}

interface PresenceLike {
  rawEntries(): ReadonlyMap<string, { data: unknown; expiresAtMs: number }>;
  restoreSnapshot(data: PresenceSnapshot): void;
}

export const snapshotFromORSet = (h: ORSetLike): ORSetSnapshot => {
  const adds: Record<string, string[]> = {};
  for (const [key, tagSet] of h.rawTags()) {
    adds[key] = Array.from(tagSet);
  }
  return { type: "orset", adds };
};

export const snapshotFromPresence = (h: PresenceLike): PresenceSnapshot => {
  const entries: Record<string, { data: unknown; ttl_ms: number; wall_ms: number }> = {};
  for (const [key, entry] of h.rawEntries()) {
    const ttl_ms = entry.expiresAtMs - Date.now();
    if (ttl_ms > 0) {
      entries[key] = { data: entry.data, ttl_ms, wall_ms: entry.expiresAtMs };
    }
  }
  return { type: "presence", entries };
};

export const snapshotFromRGA = (h: RGAHandle): RGASnapshot => ({
  type: "rga",
  text: h.value(),
});

export const snapshotFromTree = (h: TreeHandle): TreeSnapshot => ({
  type: "tree",
  roots: h.value().roots,
});

export const snapshotFromCRDTMap = (h: CRDTMapHandle): CRDTMapSnapshot => ({
  type: "crdtmap",
  state: h.value(),
});

type AnyHandle =
  | GCounterHandle
  | PNCounterHandle
  | LwwLike
  | ORSetLike
  | PresenceLike
  | RGAHandle
  | TreeHandle
  | CRDTMapHandle;

export const snapshotFromHandle = (handle: AnyHandle): CRDTSnapshot => {
  if (handle instanceof GCounterHandle) return snapshotFromGCounter(handle);
  if (handle instanceof PNCounterHandle) return snapshotFromPNCounter(handle);
  if (handle instanceof LwwRegisterHandle) return snapshotFromLww(handle as LwwRegisterHandle<unknown>);
  if (handle instanceof ORSetHandle) return snapshotFromORSet(handle as ORSetHandle<unknown>);
  if (handle instanceof PresenceHandle) return snapshotFromPresence(handle as PresenceHandle<unknown>);
  if (handle instanceof RGAHandle) return snapshotFromRGA(handle);
  if (handle instanceof TreeHandle) return snapshotFromTree(handle);
  return snapshotFromCRDTMap(handle as CRDTMapHandle);
};

export const restoreSnapshotToHandle = (snap: CRDTSnapshot, handle: AnyHandle): void => {
  if (snap.type === "gcounter" && handle instanceof GCounterHandle) {
    handle.applyDelta({ counters: snap.counters });
    return;
  }
  if (snap.type === "pncounter" && handle instanceof PNCounterHandle) {
    handle.applyDelta({ pos: { counters: snap.pos }, neg: { counters: snap.neg } });
    return;
  }
  if (snap.type === "lwwregister" && handle instanceof LwwRegisterHandle) {
    (handle as LwwRegisterHandle<unknown>).applyDelta({ entry: snap.entry });
    return;
  }
  if (snap.type === "orset" && handle instanceof ORSetHandle) {
    (handle as ORSetHandle<unknown>).restoreSnapshot(snap);
    return;
  }
  if (snap.type === "presence" && handle instanceof PresenceHandle) {
    (handle as PresenceHandle<unknown>).restoreSnapshot(snap);
    return;
  }
  if (snap.type === "rga" && handle instanceof RGAHandle) {
    handle.restoreSnapshot(snap.text);
    return;
  }
  if (snap.type === "tree" && handle instanceof TreeHandle) {
    handle.restoreSnapshot(snap.roots);
    return;
  }
  if (snap.type === "crdtmap" && handle instanceof CRDTMapHandle) {
    handle.restoreSnapshot(snap.state);
    return;
  }
};
