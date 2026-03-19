import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { GCounterDelta } from "../sync/delta.js";

export interface GCounterState {
  counts: Record<string, number>;
}

/**
 * Low-level handle for a grow-only counter (GCounter) CRDT.
 *
 * Obtained via `MeridianClient.gcounter()`. Prefer the `useGCounter` React hook
 * for component-level usage; use this handle directly in non-React environments.
 */
export class GCounterHandle {
  private state: GCounterState;
  private readonly clientId: number;
  private readonly crdtId: string;
  private readonly ns: string;
  private readonly transport: WsTransport;

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

  /** Returns the current counter value (sum of all client contributions). */
  value(): number {
    return Object.values(this.state.counts).reduce((a, b) => a + b, 0);
  }

  /** Returns the raw per-client contribution map, keyed by client id string. */
  counts(): Readonly<Record<string, number>> {
    return this.state.counts;
  }

  /**
   * Registers a listener that is called whenever the counter value changes.
   *
   * @returns An unsubscribe function — call it to stop receiving updates.
   */
  onChange(listener: (value: number) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  /**
   * Increments the counter by `amount` (default `1`) and broadcasts the delta.
   *
   * @throws {RangeError} If `amount` is not greater than zero.
   */
  increment(amount: number = 1, ttlMs?: number): void {
    if (amount <= 0) throw new RangeError("GCounter: increment amount must be > 0");

    const key = String(this.clientId);
    this.state.counts[key] = (this.state.counts[key] ?? 0) + amount;
    this.emit();

    const op = encode({
      GCounter: {
        client_id: this.clientId,
        amount,
      },
    });
    this.transport.send({
      Op: { crdt_id: this.crdtId, op_bytes: op, ...(ttlMs !== undefined && { ttl_ms: ttlMs }) },
    });
  }

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

  private emit(): void {
    const v = this.value();
    for (const listener of this.listeners) {
      listener(v);
    }
  }
}
