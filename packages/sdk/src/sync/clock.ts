/**
 * Client-side VectorClock.
 *
 * Tracks the latest version seen per client_id. Used to request
 * delta-since on reconnect (Sync message).
 *
 * Persisted to localStorage (browser) or a JSON file (Node/Bun) via
 * the optional `ClockStorage` adapter injected at construction.
 */

import type { VectorClock } from "../schema.js";

// ---------------------------------------------------------------------------
// ClockStorage adapter
// ---------------------------------------------------------------------------

export interface ClockStorage {
  load(key: string): VectorClock | null;
  save(key: string, vc: VectorClock): void;
}

/** In-memory only — useful for tests and SSR. */
export const memoryStorage: ClockStorage = (() => {
  const store = new Map<string, VectorClock>();
  return {
    load: (key) => store.get(key) ?? null,
    save: (key, vc) => { store.set(key, vc); },
  };
})();

/** localStorage-backed storage (browser). Falls back to memory if unavailable. */
export function localStorageAdapter(prefix = "meridian:"): ClockStorage {
  return {
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
        // quota exceeded — ignore
      }
    },
  };
}

// ---------------------------------------------------------------------------
// VectorClockTracker
// ---------------------------------------------------------------------------

/**
 * Mutable wrapper around VectorClock that tracks seen versions and
 * optionally persists them across page reloads.
 */
export class VectorClockTracker {
  // Mutable internal copy — VectorClock (Schema type) is readonly, so we
  // keep a plain mutable Record internally and expose snapshots.
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

  /** Observe a version from a peer. Updates clock if newer. */
  observe(clientId: string, version: number): void {
    const current = this.clock[clientId] ?? 0;
    if (version > current) {
      this.clock[clientId] = version;
      this.storage?.save(this.storageKey, this.snapshot());
    }
  }

  /** Merge an entire remote VectorClock (take max per entry). */
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

  /** Returns an immutable snapshot suitable for the Sync message. */
  snapshot(): VectorClock {
    return { ...this.clock };
  }

  /** Reset to empty (e.g. when the namespace is cleared). */
  reset(): void {
    for (const key of Object.keys(this.clock)) {
      delete this.clock[key];
    }
    this.storage?.save(this.storageKey, this.snapshot());
  }
}
