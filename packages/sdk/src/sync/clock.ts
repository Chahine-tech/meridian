import type { VectorClock } from "../schema.js";

export interface ClockStorage {
  load(key: string): VectorClock | null;
  save(key: string, vc: VectorClock): void;
}

export const memoryStorage: ClockStorage = (() => {
  const store = new Map<string, VectorClock>();
  return {
    load: (key) => store.get(key) ?? null,
    save: (key, vc) => { store.set(key, vc); },
  };
})();

export const localStorageAdapter = (prefix = "meridian:"): ClockStorage => ({
  load(key) {
    try {
      const raw = localStorage.getItem(`${prefix}${key}`);
      if (raw === null) return null;
      return JSON.parse(raw) as VectorClock;
    } catch {
      return null;
    }
  },
  save(key, vc) {
    try {
      localStorage.setItem(`${prefix}${key}`, JSON.stringify(vc));
    } catch {
    }
  },
});

export class VectorClockTracker {
  // HACK: VectorClock (Schema type) is readonly, so we keep a plain mutable Record internally and expose snapshots.
  private readonly clock: Record<string, number>;
  private readonly storage: ClockStorage | null;
  private readonly storageKey: string;

  constructor(opts: {
    namespace: string;
    crdtId: string;
    storage?: ClockStorage | null;
    initial?: VectorClock;
  }) {
    this.storageKey = `vc:${opts.namespace}:${opts.crdtId}`;
    this.storage = opts.storage ?? null;

    const persisted = this.storage?.load(this.storageKey) ?? null;
    this.clock = { ...(opts.initial ?? {}), ...(persisted ?? {}) };
  }

  observe(clientId: string, version: number): void {
    const current = this.clock[clientId] ?? 0;
    if (version > current) {
      this.clock[clientId] = version;
      this.storage?.save(this.storageKey, this.snapshot());
    }
  }

  merge(remote: VectorClock): void {
    let changed = false;
    for (const [id, ver] of Object.entries(remote)) {
      const current = this.clock[id] ?? 0;
      if (ver > current) {
        this.clock[id] = ver;
        changed = true;
      }
    }
    if (changed) {
      this.storage?.save(this.storageKey, this.snapshot());
    }
  }

  snapshot(): VectorClock {
    return { ...this.clock };
  }

  reset(): void {
    for (const key of Object.keys(this.clock)) {
      delete this.clock[key];
    }
    this.storage?.save(this.storageKey, this.snapshot());
  }
}
