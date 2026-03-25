import { decode } from "@msgpack/msgpack";

export interface GCounterDelta {
  counters: Record<string, number>;
}

export const decodeGCounterDelta = (bytes: Uint8Array): GCounterDelta => {
  const raw = decode(bytes) as { counters?: Record<string, number> };
  return { counters: raw.counters ?? {} };
};

export interface PNCounterDelta {
  pos: GCounterDelta | null;
  neg: GCounterDelta | null;
}

export const decodePNCounterDelta = (bytes: Uint8Array): PNCounterDelta => {
  const raw = decode(bytes) as {
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
  const raw = decode(bytes) as {
    adds?: Record<string, unknown[]>;
    removes?: Record<string, unknown[]>;
  };
  const toBytes = (value: unknown): Uint8Array =>
    value instanceof Uint8Array ? value : new Uint8Array(value as number[]);
  const decodeMap = (map?: Record<string, unknown[]>) =>
    Object.fromEntries(Object.entries(map ?? {}).map(([key, tags]) => [key, tags.map(toBytes)]));
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
  const raw = decode(bytes) as { entry?: LwwEntry | null };
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
  const raw = decode(bytes) as { changes?: Record<string, PresenceEntryDelta | null> };
  return { changes: raw.changes ?? {} };
};

export interface RGADelta {
  text: string;
}

export const decodeRGADelta = (bytes: Uint8Array): RGADelta => {
  const raw = decode(bytes) as { text?: string };
  return { text: raw.text ?? "" };
};

export type CrdtValueDelta =
  | { GCounter: GCounterDelta }
  | { PNCounter: PNCounterDelta }
  | { ORSet: ORSetDelta }
  | { LwwRegister: LwwDelta }
  | { Presence: PresenceDelta }
  | { RGA: RGADelta };

export interface CRDTMapDelta {
  deltas: Record<string, CrdtValueDelta>;
}

export const decodeCRDTMapDelta = (bytes: Uint8Array): CRDTMapDelta => {
  const raw = decode(bytes) as { deltas?: Record<string, unknown> };
  return { deltas: (raw.deltas ?? {}) as Record<string, CrdtValueDelta> };
};
