// IDB type declarations (subset of DOM IDBDatabase API — avoids full DOM lib dependency)
declare const indexedDB: {
  open(name: string, version?: number): IDBOpenRequest;
};
interface IDBOpenRequest {
  result: IDBDb;
  onupgradeneeded: ((ev: { target: IDBOpenRequest }) => void) | null;
  onsuccess: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  error: Error | null;
}
interface IDBDb {
  transaction(store: string, mode: "readonly" | "readwrite"): IDBTx;
  createObjectStore(name: string): void;
}
interface IDBTx {
  objectStore(name: string): IDBStore;
}
interface IDBStore {
  get(key: string): IDBReq<unknown>;
  put(value: unknown, key: string): IDBReq<unknown>;
  delete(key: string): IDBReq<undefined>;
}
interface IDBReq<T> {
  result: T;
  onsuccess: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  error: Error | null;
}

export interface StateStorage {
  load(key: string): Promise<Uint8Array | null>;
  save(key: string, data: Uint8Array): Promise<void>;
  delete(key: string): Promise<void>;
}

export interface SyncStateStorage {
  load(key: string): Uint8Array | null;
  save(key: string, data: Uint8Array): void;
  delete(key: string): void;
}

export const memoryStateStorage: StateStorage = (() => {
  const store = new Map<string, Uint8Array>();
  return {
    load: (key) => Promise.resolve(store.get(key) ?? null),
    save: (key, data) => { store.set(key, data); return Promise.resolve(); },
    delete: (key) => { store.delete(key); return Promise.resolve(); },
  };
})();

export const indexedDbStateStorage = (dbName = "meridian"): StateStorage => {
  let dbPromise: Promise<IDBDb> | null = null;

  const openDb = (): Promise<IDBDb> => {
    if (dbPromise !== null) return dbPromise;
    if (typeof indexedDB === "undefined") {
      return Promise.reject(new Error("indexedDB is not available in this environment"));
    }
    dbPromise = new Promise<IDBDb>((resolve, reject) => {
      const req = indexedDB.open(dbName, 1);
      req.onupgradeneeded = (ev) => {
        ev.target.result.createObjectStore("kv");
      };
      req.onsuccess = () => { resolve(req.result); };
      req.onerror = () => { reject(req.error); };
    });
    return dbPromise;
  };

  const withStore = <T>(
    mode: "readonly" | "readwrite",
    fn: (store: IDBStore) => IDBReq<T>,
  ): Promise<T> =>
    openDb().then(
      (db) =>
        new Promise<T>((resolve, reject) => {
          const tx = db.transaction("kv", mode);
          const req = fn(tx.objectStore("kv"));
          req.onsuccess = () => { resolve(req.result as T); };
          req.onerror = () => { reject(req.error); };
        }),
    );

  return {
    load: (key) =>
      withStore<unknown>("readonly", (s) => s.get(key)).then(
        (v) => (v instanceof Uint8Array ? v : null),
      ),
    save: (key, data) =>
      withStore<unknown>("readwrite", (s) => s.put(data, key)).then(
        () => undefined,
      ),
    delete: (key) =>
      withStore<undefined>("readwrite", (s) => s.delete(key)).then(
        () => undefined,
      ),
  };
};

export const localStorageSyncOpsAdapter = (
  prefix = "meridian:ops:",
): SyncStateStorage => ({
  load: (key) => {
    try {
      if (typeof localStorage === "undefined") return null;
      const raw = localStorage.getItem(`${prefix}${key}`);
      if (raw === null) return null;
      const arr = JSON.parse(raw) as number[];
      return new Uint8Array(arr);
    } catch {
      return null;
    }
  },
  save: (key, data) => {
    try {
      if (typeof localStorage === "undefined") return;
      localStorage.setItem(`${prefix}${key}`, JSON.stringify(Array.from(data)));
    } catch {
      // quota exceeded or unavailable — skip silently
    }
  },
  delete: (key) => {
    try {
      if (typeof localStorage === "undefined") return;
      localStorage.removeItem(`${prefix}${key}`);
    } catch {
      // ignore
    }
  },
});
