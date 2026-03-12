/**
 * ORSet handle — add-wins observed-remove set.
 *
 * Each element has a set of add-tags (UUIDs). Remove only removes tags
 * known at remove time — a concurrent add with a new tag survives.
 *
 * Elements are serialized as JSON for the wire (serde_json::Value).
 *
 * Pass a `schema` to get runtime validation of elements deserialized from deltas:
 *   client.orset("id", Schema.Struct({ id: Schema.String }))
 */

import { Schema } from "effect";
import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { ORSetDelta } from "../sync/delta.js";

export class ORSetHandle<T> {
  /** element (JSON-stringified) → Set of live add-tags */
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

  // ---- Read ----

  /** Returns all live elements (add-wins). */
  elements(): T[] {
    return Array.from(this.tags.keys())
      .filter(k => (this.tags.get(k)?.size ?? 0) > 0)
      .map(k => this.decode(JSON.parse(k)));
  }

  has(element: T): boolean {
    const key = JSON.stringify(element);
    return (this.tags.get(key)?.size ?? 0) > 0;
  }

  onChange(listener: (elements: T[]) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  // ---- Write ----

  add(element: T): void {
    const tag = crypto.randomUUID();
    const key = JSON.stringify(element);

    if (!this.tags.has(key)) this.tags.set(key, new Set());
    this.tags.get(key)!.add(tag);
    this.emit();

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: encode({ ORSet: { Add: { element, tag, client_id: this.clientId } } }),
      },
    });
  }

  remove(element: T): void {
    const key = JSON.stringify(element);
    const currentTags = Array.from(this.tags.get(key) ?? []);
    if (currentTags.length === 0) return;

    this.tags.delete(key);
    this.emit();

    this.transport.send({
      Op: {
        crdt_id: this.crdtId,
        op_bytes: encode({ ORSet: { Remove: { element, tags: currentTags, client_id: this.clientId } } }),
      },
    });
  }

  // ---- Delta application ----

  applyDelta(delta: ORSetDelta): void {
    let changed = false;

    for (const [elem, addedTags] of Object.entries(delta.added)) {
      if (!this.tags.has(elem)) this.tags.set(elem, new Set());
      const set = this.tags.get(elem)!;
      for (const tag of addedTags) {
        if (!set.has(tag)) { set.add(tag); changed = true; }
      }
    }

    for (const [elem, removedTags] of Object.entries(delta.removed)) {
      const set = this.tags.get(elem);
      if (!set) continue;
      for (const tag of removedTags) {
        if (set.has(tag)) { set.delete(tag); changed = true; }
      }
      if (set.size === 0) this.tags.delete(elem);
    }

    if (changed) this.emit();
  }

  // ---- Internal ----

  private decode(raw: unknown): T {
    if (this.schema !== null) {
      return Schema.decodeUnknownSync(this.schema)(raw);
    }
    return raw as T;
  }

  private emit(): void {
    const elems = this.elements();
    for (const l of this.listeners) l(elems);
  }
}
