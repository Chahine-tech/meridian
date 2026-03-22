import { Chunk, Effect, Schema as S, Stream } from "effect";
import type { Schema } from "effect";
import { encode, decode } from "../codec.js";
import type { WsTransport } from "../transport/websocket.js";

// ---------------------------------------------------------------------------
// AwarenessEntry
// ---------------------------------------------------------------------------

/**
 * A single peer's awareness state on a given channel.
 *
 * Unlike presence entries, awareness entries are ephemeral: they are not
 * persisted and disappear when the sender disconnects or clears their state.
 */
export interface AwarenessEntry<T> {
  /** The sender's client ID (from their token). */
  clientId: number;
  /** The decoded payload sent by the peer. */
  data: T;
  /** Local timestamp (ms) when this entry was last received. */
  receivedAt: number;
}

// ---------------------------------------------------------------------------
// AwarenessHandle
// ---------------------------------------------------------------------------

/**
 * Handle for an ephemeral awareness channel.
 *
 * Awareness is a stateless pub/sub channel: updates are fanned out to all
 * other subscribers in the same namespace in real time, but are **not**
 * persisted. If a client connects after a peer's last update, it will not
 * see that peer's state until the peer sends another update.
 *
 * Use this for high-frequency, transient UI state like cursor positions,
 * text selections, or "is typing" indicators — not for durable shared data.
 *
 * @example
 * ```ts
 * const cursors = client.awareness<{ x: number; y: number }>('cursors');
 * cursors.update({ x: 100, y: 200 });
 * cursors.onChange(peers => console.log('peers:', peers));
 * ```
 */
export class AwarenessHandle<T = unknown> {
  private readonly entries = new Map<number, AwarenessEntry<T>>();
  private readonly listeners = new Set<
    (entries: AwarenessEntry<T>[]) => void
  >();

  constructor(
    private readonly key: string,
    private readonly transport: WsTransport,
    private readonly clientId: number,
    private readonly schema?: Schema.Schema<T>,
  ) {}

  /**
   * Send our local awareness state to all peers (fire-and-forget).
   *
   * The server does not acknowledge awareness updates — this is a best-effort
   * broadcast. Offline updates are **not** queued: they are dropped silently
   * since awareness data is inherently time-sensitive.
   */
  update(data: T): void {
    const encoded = encode(data);
    this.transport.send({ AwarenessUpdate: { key: this.key, data: encoded } });
  }

  /**
   * Clear our own entry from remote peers (e.g. on tab hide or component unmount).
   *
   * Sends a `null` payload, which causes all subscribers to remove our entry.
   */
  clear(): void {
    this.transport.send({
      AwarenessUpdate: { key: this.key, data: encode(null) },
    });
  }

  /**
   * Apply an incoming `AwarenessBroadcast` from the server.
   * Called internally by `MeridianClient.handleServerMsg`.
   *
   * @internal
   */
  applyBroadcast(fromClientId: number, rawData: Uint8Array): void {
    const decoded = decode(rawData);
    if (decoded === null || decoded === undefined) {
      if (this.entries.has(fromClientId)) {
        this.entries.delete(fromClientId);
        this.emit();
      }
      return;
    }

    const data = this.schema
      ? S.decodeUnknownSync(this.schema)(decoded)
      : (decoded as T);

    const existing = this.entries.get(fromClientId);
    this.entries.set(fromClientId, {
      clientId: fromClientId,
      data,
      receivedAt: Date.now(),
    });

    // Only notify if the entry was new or data changed (reference check is
    // sufficient for the "new entry" case; data change always yields a new obj).
    if (!existing || existing.data !== data) {
      this.emit();
    }
  }

  /**
   * Returns the current awareness state for all **other** peers on this channel.
   *
   * Self is excluded (own updates are not echoed back by the server).
   */
  peers(): AwarenessEntry<T>[] {
    const result: AwarenessEntry<T>[] = [];
    for (const entry of this.entries.values()) {
      if (entry.clientId !== this.clientId) result.push(entry);
    }
    return result;
  }

  /**
   * Subscribe to peer awareness changes.
   *
   * The listener receives the updated `peers()` array whenever a remote peer
   * sends an update or clears their state. Returns an unsubscribe function.
   */
  onChange(
    listener: (entries: AwarenessEntry<T>[]) => void,
  ): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /** Returns a Stream that emits the peer entries on every change. */
  stream(): Stream.Stream<AwarenessEntry<T>[], never, never> {
    return Stream.async<AwarenessEntry<T>[]>((emit) => {
      const unsub = this.onChange((entries) => { void emit(Effect.succeed(Chunk.of(entries))); });
      return Effect.sync(unsub);
    });
  }

  private emit(): void {
    const entries = this.peers();
    for (const fn of this.listeners) fn(entries);
  }
}
