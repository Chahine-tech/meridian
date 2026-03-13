/**
 * PNCounter handle — increment and decrement counter.
 * value = sum(increments) - sum(decrements). Can be negative.
 */

import { encode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";
import type { PNCounterDelta } from "../sync/delta.js";

export interface PNCounterState {
  p: Record<string, number>; // positive GCounter
  n: Record<string, number>; // negative GCounter
}

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

  // ---- Read ----

  value(): number {
    const pos = Object.values(this.state.p).reduce((a, b) => a + b, 0);
    const neg = Object.values(this.state.n).reduce((a, b) => a + b, 0);
    return pos - neg;
  }

  onChange(listener: (value: number) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  // ---- Write ----

  increment(amount: number = 1): void {
    if (amount <= 0) throw new RangeError("PNCounter: increment amount must be > 0");
    const key = String(this.clientId);
    this.state.p[key] = (this.state.p[key] ?? 0) + amount;
    this.emit();
    this.sendOp({ Increment: { client_id: this.clientId, amount } });
  }

  decrement(amount: number = 1): void {
    if (amount <= 0) throw new RangeError("PNCounter: decrement amount must be > 0");
    const key = String(this.clientId);
    this.state.n[key] = (this.state.n[key] ?? 0) + amount;
    this.emit();
    this.sendOp({ Decrement: { client_id: this.clientId, amount } });
  }

  // ---- Delta application ----

  applyDelta(delta: PNCounterDelta): void {
    let changed = false;
    for (const [id, count] of Object.entries(delta.pos?.counters ?? {})) {
      const cur = this.state.p[id] ?? 0;
      if (count > cur) { this.state.p[id] = count; changed = true; }
    }
    for (const [id, count] of Object.entries(delta.neg?.counters ?? {})) {
      const cur = this.state.n[id] ?? 0;
      if (count > cur) { this.state.n[id] = count; changed = true; }
    }
    if (changed) this.emit();
  }

  // ---- Internal ----

  private sendOp(op: unknown): void {
    this.transport.send({
      Op: { crdt_id: this.crdtId, op_bytes: encode({ PNCounter: op }) },
    });
  }

  private emit(): void {
    const v = this.value();
    for (const l of this.listeners) l(v);
  }
}
