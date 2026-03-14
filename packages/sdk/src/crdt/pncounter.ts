import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { PNCounterDelta } from "../sync/delta.js";

export interface PNCounterState {
  p: Record<string, number>;
  n: Record<string, number>;
}

/**
 * Low-level handle for a positive-negative counter (PNCounter) CRDT.
 *
 * Obtained via `MeridianClient.pncounter()`. Prefer the `usePNCounter` React hook
 * for component-level usage; use this handle directly in non-React environments.
 */
export class PNCounterHandle {
  private state: PNCounterState;
  private readonly clientId: number;
  private readonly crdtId: string;
  private readonly transport: WsTransport;
  private readonly listeners = new Set<(value: number) => void>();

  constructor(opts: {
    ns: string;
    crdtId: string;
    clientId: number;
    transport: WsTransport;
    initial?: PNCounterState;
  }) {
    this.crdtId = opts.crdtId;
    this.clientId = opts.clientId;
    this.transport = opts.transport;
    this.state = opts.initial ?? { p: {}, n: {} };
  }

  /** Returns the current net counter value (sum of increments minus sum of decrements). */
  value(): number {
    const pos = Object.values(this.state.p).reduce((a, b) => a + b, 0);
    const neg = Object.values(this.state.n).reduce((a, b) => a + b, 0);
    return pos - neg;
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
  increment(amount: number = 1): void {
    if (amount <= 0) throw new RangeError("PNCounter: increment amount must be > 0");
    const key = String(this.clientId);
    this.state.p[key] = (this.state.p[key] ?? 0) + amount;
    this.emit();
    this.sendOp({ Increment: { client_id: this.clientId, amount } });
  }

  /**
   * Decrements the counter by `amount` (default `1`) and broadcasts the delta.
   *
   * @throws {RangeError} If `amount` is not greater than zero.
   */
  decrement(amount: number = 1): void {
    if (amount <= 0) throw new RangeError("PNCounter: decrement amount must be > 0");
    const key = String(this.clientId);
    this.state.n[key] = (this.state.n[key] ?? 0) + amount;
    this.emit();
    this.sendOp({ Decrement: { client_id: this.clientId, amount } });
  }

  applyDelta(delta: PNCounterDelta): void {
    let changed = false;
    for (const [id, count] of Object.entries(delta.pos?.counters ?? {})) {
      const currentCount = this.state.p[id] ?? 0;
      if (count > currentCount) { this.state.p[id] = count; changed = true; }
    }
    for (const [id, count] of Object.entries(delta.neg?.counters ?? {})) {
      const currentCount = this.state.n[id] ?? 0;
      if (count > currentCount) { this.state.n[id] = count; changed = true; }
    }
    if (changed) this.emit();
  }

  private sendOp(op: unknown): void {
    this.transport.send({
      Op: { crdt_id: this.crdtId, op_bytes: encode({ PNCounter: op }) },
    });
  }

  private emit(): void {
    const currentValue = this.value();
    for (const listener of this.listeners) listener(currentValue);
  }
}
