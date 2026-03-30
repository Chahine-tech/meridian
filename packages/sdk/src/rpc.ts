/**
 * Type-safe RPC layer for Meridian WebSocket + HTTP.
 *
 * Inspired by partyRPC — define your events and responses once, get fully
 * typed `send` / `on` on the client with Effect Schema validation at every
 * decode boundary.
 *
 * Custom messages piggyback on the existing `AwarenessUpdate` transport using
 * a reserved `"__rpc__"` key. The server broadcasts them via the
 * `AwarenessBroadcast` path — no server changes required.
 *
 * @example
 * ```ts
 * import { Schema } from "effect";
 * import { createMeridianRpc } from "meridian-sdk";
 *
 * const rpc = createMeridianRpc({
 *   events: {
 *     "cursor-move": Schema.Struct({ x: Schema.Number, y: Schema.Number }),
 *     "reaction":    Schema.Struct({ emoji: Schema.String }),
 *   },
 *   responses: {
 *     "cursor-moved": Schema.Struct({ clientId: Schema.Number, x: Schema.Number, y: Schema.Number }),
 *     "reacted":      Schema.Struct({ clientId: Schema.Number, emoji: Schema.String }),
 *   },
 * });
 *
 * export type AppRpc = typeof rpc;
 *
 * const client = rpc.createClient(meridianClient);
 *
 * client.send("cursor-move", { x: 10, y: 20 });     // ✅
 * client.send("cursor-move", { x: "oops" });         // ❌ compile error
 *
 * client.on("cursor-moved", (msg) => {
 *   console.log(msg.clientId, msg.x, msg.y);         // ✅ no casts
 * });
 * ```
 */

import { Schema, Effect, type ParseResult, pipe } from "effect";
import { encode } from "./codec.js";
import type { MeridianClient } from "./client.js";

type AnySchema = Schema.Schema<unknown, unknown, never>;
type SchemaMap = Record<string, AnySchema>;
type InferSchema<M extends SchemaMap, K extends keyof M> = Schema.Schema.Type<M[K]>;

/** Unsubscribe function returned by `on()`. */
export type Unsubscribe = () => void;

interface RpcFrame {
  t: string;
  p: unknown;
}

export interface MeridianRpcConfig<
  Events extends SchemaMap,
  Responses extends SchemaMap,
> {
  /** Messages the client can **send** to the server. Key = event type, value = Effect Schema for the payload. */
  events: Events;
  /** Messages the server can **send** to the client. Key = response type, value = Effect Schema for the payload. */
  responses: Responses;
}

export interface MeridianRpcClient<
  Events extends SchemaMap,
  Responses extends SchemaMap,
> {
  /**
   * Send a typed event to the server.
   * Payload is validated against the schema before encoding.
   */
  send<K extends keyof Events & string>(
    type: K,
    payload: InferSchema<Events, K>,
  ): Effect.Effect<void, ParseResult.ParseError>;

  /**
   * Subscribe to a typed response from the server.
   * Invalid frames are dropped silently.
   * Returns an unsubscribe function.
   */
  on<K extends keyof Responses & string>(
    type: K,
    listener: (payload: InferSchema<Responses, K>) => void,
  ): Unsubscribe;
}

export interface MeridianRpc<
  Events extends SchemaMap,
  Responses extends SchemaMap,
> {
  readonly events: Events;
  readonly responses: Responses;
  /** Bind this definition to a live `MeridianClient`. */
  createClient(client: MeridianClient): MeridianRpcClient<Events, Responses>;
}

/**
 * Define a type-safe RPC layer on top of a Meridian WebSocket connection.
 */
export function createMeridianRpc<
  Events extends SchemaMap,
  Responses extends SchemaMap,
>(config: MeridianRpcConfig<Events, Responses>): MeridianRpc<Events, Responses> {
  return {
    events: config.events,
    responses: config.responses,

    createClient(client: MeridianClient): MeridianRpcClient<Events, Responses> {
      const listeners = new Map<string, Set<(payload: unknown) => void>>();

      client.onRpcFrame((raw: unknown) => {
        const frame = raw as RpcFrame;
        if (typeof frame?.t !== "string") return;
        const schema = config.responses[frame.t];
        if (!schema) return;

        const parsed = Schema.decodeUnknownOption(schema)(frame.p);
        if (parsed._tag === "None") return;

        const typeListeners = listeners.get(frame.t);
        if (typeListeners) {
          for (const fn of typeListeners) fn(parsed.value);
        }
      });

      return {
        send<K extends keyof Events & string>(
          type: K,
          payload: InferSchema<Events, K>,
        ): Effect.Effect<void, ParseResult.ParseError> {
          const schema = config.events[type] as Events[K];
          return pipe(
            Schema.encode(schema)(payload),
            Effect.andThen((validated) => {
              const frame: RpcFrame = { t: type, p: validated };
              client.sendRpcFrame(encode(frame));
            }),
          );
        },

        on<K extends keyof Responses & string>(
          type: K,
          listener: (payload: InferSchema<Responses, K>) => void,
        ): Unsubscribe {
          if (!listeners.has(type)) listeners.set(type, new Set());
          listeners.get(type)?.add(listener as (p: unknown) => void);
          return () => { listeners.get(type)?.delete(listener as (p: unknown) => void); };
        },
      };
    },
  };
}
