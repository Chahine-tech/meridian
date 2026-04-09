import { Chunk, Effect, Option, Schema, Stream } from "effect";
import { encode, uuidToBytes } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { ORSetDelta } from "../sync/delta.js";
import { toHex } from "../utils/to-hex.js";
import type { ORSetSnapshot } from "../persistence/snapshot.js";

/**
 * Low-level handle for an Observed-Remove Set (OR-Set) CRDT.
 *
 * Obtained via `MeridianClient.orset()`. Prefer the `useORSet` React hook for
 * component-level usage; use this handle directly in non-React environments.
 */
export class ORSetHandle<T> {
  private readonly tags = new Map<string, Set<string>>();
  private readonly crdtId: string;
  private readonly clientId: number;
  private readonly transport: WsTransport;
  private readonly schema: Schema.Schema<T> | null;
  private readonly listeners = new Set<(elements: T[]) => void>();

  constructor(opts: {
    ns: string;
    crdtId: string;
    clientId: number;
    transport: WsTransport;
    schema?: Schema.Schema<T>;
  }) {
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
    this.schema = opts.schema ?? null;
  }

  /** Returns the current set elements as an array, decoded via the optional schema. */
  elements(): T[] {
    return Array.from(this.tags.keys())
      .filter(k => (this.tags.get(k)?.size ?? 0) > 0)
      .flatMap(k => {
        const decoded = this.decode(JSON.parse(k));
        return decoded !== null ? [decoded] : [];
      });
  }

  /** Returns `true` if the set currently contains `element`. */
  has(element: T): boolean {
    const key = JSON.stringify(element);
    return (this.tags.get(key)?.size ?? 0) > 0;
  }

  /**
   * Registers a listener that is called whenever the set contents change.
   *
   * @returns An unsubscribe function — call it to stop receiving updates.
   */
  onChange(listener: (elements: T[]) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /** Returns a Stream that emits the set elements on every change. */
  stream(): Stream.Stream<T[], never, never> {
    return Stream.async<T[]>((emit) => {
      const unsub = this.onChange((elements) => { void emit(Effect.succeed(Chunk.of(elements))); });
      return Effect.sync(unsub);
    });
  }

  /**
   * Adds `element` to the set and broadcasts the operation.
   *
   * Each call generates a unique tag so concurrent adds of the same value
   * are treated as distinct entries.
   */
  add(element: T, ttlMs?: number): void {
    const tag = crypto.randomUUID();
    const key = JSON.stringify(element);

    if (!this.tags.has(key)) this.tags.set(key, new Set());
    const tagSet = this.tags.get(key) as Set<string>;
    tagSet.add(tag);
    this.emit();

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: encode({ ORSet: { Add: { element, tag: uuidToBytes(tag) } } }),
        ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
      },
    });
  }

  /**
   * Removes `element` from the set and broadcasts the operation.
   *
   * Only the tags observed locally at the time of this call are removed;
   * concurrently added copies on other clients are left intact.
   */
  remove(element: T, ttlMs?: number): void {
    const key = JSON.stringify(element);
    const currentTags = Array.from(this.tags.get(key) ?? []);
    if (currentTags.length === 0) return;

    this.tags.delete(key);
    this.emit();

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: encode({ ORSet: { Remove: { element, known_tags: currentTags.map(uuidToBytes) } } }),
        ...(ttlMs !== undefined && { ttl_ms: ttlMs }),
      },
    });
  }

  /** Returns the raw tag map for snapshot serialization. */
  rawTags(): ReadonlyMap<string, ReadonlySet<string>> {
    return this.tags;
  }

  /** Restores snapshot state directly (bypasses applyDelta which expects Uint8Array tags). */
  restoreSnapshot(data: ORSetSnapshot): void {
    this.tags.clear();
    for (const [key, tagArr] of Object.entries(data.adds)) {
      if (tagArr.length > 0) {
        this.tags.set(key, new Set(tagArr));
      }
    }
    this.emit();
  }

  applyDelta(delta: ORSetDelta): void {
    let changed = false;

    for (const [elem, addedTags] of Object.entries(delta.adds)) {
      if (!this.tags.has(elem)) this.tags.set(elem, new Set());
      const set = this.tags.get(elem) as Set<string>;
      for (const tag of addedTags) {
        const tagStr = Array.from(tag).map(toHex).join("");
        if (!set.has(tagStr)) { set.add(tagStr); changed = true; }
      }
    }

    for (const [elem, removedTags] of Object.entries(delta.removes)) {
      const set = this.tags.get(elem);
      if (!set) continue;
      for (const tag of removedTags) {
        const tagStr = Array.from(tag).map(toHex).join("");
        if (set.has(tagStr)) { set.delete(tagStr); changed = true; }
      }
      if (set.size === 0) this.tags.delete(elem);
    }

    if (changed) this.emit();
  }

  private decode(raw: unknown): T | null {
    if (this.schema !== null) {
      return Option.getOrNull(Schema.decodeUnknownOption(this.schema)(raw));
    }
    return raw as T;
  }

  private emit(): void {
    const elems = this.elements();
    for (const listener of this.listeners) listener(elems);
  }
}
