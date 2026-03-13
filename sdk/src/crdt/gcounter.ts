/**
 * GCounter handle — increment-only counter.
 *
 * Local state is kept as a sparse map `{ client_id → count }`.
 * `value()` returns the sum. Deltas are merged on incoming ServerMsg.Delta.
 */

import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { GCounterDelta } from "../sync/delta.js";

export interface GCounterState {
  counts: Record<string, number>;
}

export class GCounterHandle {
  private state: GCounterState;
  private readonly clientId: number;
  private readonly crdtId: string;
  private readonly ns: string;
  private readonly transport: WsTransport;

  /** Emits on every state change. */
  private readonly listeners = new Set<(value: number) => void>();

  constructor(opts: {
    ns: string;
    crdtId: string;
    clientId: number;
    transport: WsTransport;
    initial?: GCounterState;
  }) {
    this.ns = opts.ns;
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
    this.state = opts.initial ?? { counts: {} };
  }

  // ---- Read ----

  value(): number {
    return Object.values(this.state.counts).reduce((a, b) => a + b, 0);
  }

  counts(): Readonly<Record<string, number>> {
    return this.state.counts;
  }

  /** Subscribe to value changes. Returns an unsubscribe function. */
  onChange(listener: (value: number) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  // ---- Write ----

  /** Increment by `amount` (must be > 0). */
  increment(amount: number = 1): void {
    if (amount <= 0) throw new RangeError("GCounter: increment amount must be > 0");

    // Optimistic local update
    const key = String(this.clientId);
    this.state.counts[key] = (this.state.counts[key] ?? 0) + amount;
    this.emit();

    // Send Op to server
    const op = encode({
      GCounter: {
        client_id: this.clientId,
        amount,
      },
    });
    this.transport.send({
      Op: { crdt_id: this.crdtId, op_bytes: op },
    });
  }

  // ---- Delta application (called by MeridianClient on incoming Delta) ----

  applyDelta(delta: GCounterDelta): void {
    let changed = false;
    for (const [id, count] of Object.entries(delta.counters)) {
      const current = this.state.counts[id] ?? 0;
      if (count > current) {
        this.state.counts[id] = count;
        changed = true;
      }
    }
    if (changed) this.emit();
  }

  // ---- Internal ----

  private emit(): void {
    const v = this.value();
    for (const listener of this.listeners) {
      listener(v);
    }
  }
}
