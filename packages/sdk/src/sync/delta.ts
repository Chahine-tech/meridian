import { unpack } from "msgpackr";

export interface GCounterDelta {
  counters: Record<string, number>;
}

export const decodeGCounterDelta = (bytes: Uint8Array): GCounterDelta => {
  const raw = unpack(bytes) as { counters?: Record<string, number> };
  return { counters: raw.counters ?? {} };
};

export interface PNCounterDelta {
  pos: GCounterDelta | null;
  neg: GCounterDelta | null;
}

export const decodePNCounterDelta = (bytes: Uint8Array): PNCounterDelta => {
  const raw = unpack(bytes) as {
    pos?: { counters?: Record<string, number> } | null;
    neg?: { counters?: Record<string, number> } | null;
  };
  return {
    pos: raw.pos ? { counters: raw.pos.counters ?? {} } : null,
    neg: raw.neg ? { counters: raw.neg.counters ?? {} } : null,
  };
};

export interface ORSetDelta {
  adds: Record<string, Uint8Array[]>;
  removes: Record<string, Uint8Array[]>;
}

export const decodeORSetDelta = (bytes: Uint8Array): ORSetDelta => {
  const raw = unpack(bytes) as {
    adds?: Record<string, unknown[]>;
    removes?: Record<string, unknown[]>;
  };
  const toBytes = (v: unknown): Uint8Array =>
    v instanceof Uint8Array ? v : new Uint8Array(v as number[]);
  const decodeMap = (m?: Record<string, unknown[]>) =>
    Object.fromEntries(Object.entries(m ?? {}).map(([k, tags]) => [k, tags.map(toBytes)]));
  return { adds: decodeMap(raw.adds), removes: decodeMap(raw.removes) };
};

export interface LwwEntry {
  value: unknown;
  hlc: { wall_ms: number | bigint; logical: number; node_id: number | bigint };
  author: number | bigint;
}

export interface LwwDelta {
  entry: LwwEntry | null;
}

export const decodeLwwDelta = (bytes: Uint8Array): LwwDelta => {
  const raw = unpack(bytes) as { entry?: LwwEntry | null };
  return { entry: raw.entry ?? null };
};

export interface PresenceEntryDelta {
  data: unknown;
  hlc: { wall_ms: number | bigint; logical: number; node_id: number | bigint };
  ttl_ms: number | bigint;
}

export interface PresenceDelta {
  changes: Record<string, PresenceEntryDelta | null>;
}

export const decodePresenceDelta = (bytes: Uint8Array): PresenceDelta => {
  const raw = unpack(bytes) as { changes?: Record<string, PresenceEntryDelta | null> };
  return { changes: raw.changes ?? {} };
};
