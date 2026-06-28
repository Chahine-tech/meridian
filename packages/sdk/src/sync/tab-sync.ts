export interface TabDeltaMsg {
  type: "delta";
  crdtId: string;
  deltaBytes: Uint8Array;
}

/**
 * BroadcastChannel adapter for same-origin multi-tab CRDT sync.
 *
 * When a tab receives a server delta it calls `broadcast()`. Other tabs
 * receive it via `onDelta()` and apply it locally without a server round-trip.
 * Idempotent: if the server delta also arrives via WebSocket, re-applying it
 * is safe because all CRDTs are convergent.
 */
export class TabSync {
  private readonly channel: BroadcastChannel;
  private readonly listeners = new Set<(msg: TabDeltaMsg) => void>();

  constructor(namespace: string) {
    this.channel = new BroadcastChannel(`meridian:${namespace}`);
    this.channel.onmessage = (ev: MessageEvent<TabDeltaMsg>) => {
      if (ev.data?.type === "delta") {
        for (const fn of this.listeners) fn(ev.data);
      }
    };
  }

  broadcast(crdtId: string, deltaBytes: Uint8Array): void {
    this.channel.postMessage({ type: "delta", crdtId, deltaBytes } satisfies TabDeltaMsg);
  }

  onDelta(listener: (msg: TabDeltaMsg) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }

  close(): void {
    this.channel.close();
  }
}
