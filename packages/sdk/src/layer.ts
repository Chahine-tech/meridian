import { Context, Effect, Layer } from "effect";
import { MeridianClient, type MeridianClientConfig } from "./client.js";
import type { TokenParseError, TokenExpiredError } from "./errors.js";

/**
 * Effect Context tag for `MeridianClient`.
 *
 * Use this to inject a `MeridianClient` into Effect programs via `MeridianLive`.
 *
 * @example
 * ```ts
 * import { Effect } from 'effect';
 * import { MeridianService, MeridianLive } from 'meridian-sdk';
 *
 * const program = Effect.gen(function* () {
 *   const client = yield* MeridianService;
 *   client.gcounter('visitors').increment();
 * });
 *
 * await Effect.runPromise(program.pipe(Effect.provide(MeridianLive(config))));
 * ```
 */
export class MeridianService extends Context.Tag("MeridianService")<
  MeridianService,
  MeridianClient
>() {}

/**
 * Scoped Layer that creates a `MeridianClient` and closes it on release.
 *
 * The client is created with `MeridianClient.create(config)` and closed
 * automatically when the scope is closed (e.g. at the end of `Effect.runPromise`
 * or when `Layer.build` is interrupted).
 */
export const MeridianLive = (
  config: MeridianClientConfig,
): Layer.Layer<MeridianService, TokenParseError | TokenExpiredError> =>
  Layer.scoped(
    MeridianService,
    Effect.acquireRelease(
      MeridianClient.create(config),
      (client) => Effect.sync(() => client.close()),
    ),
  );
