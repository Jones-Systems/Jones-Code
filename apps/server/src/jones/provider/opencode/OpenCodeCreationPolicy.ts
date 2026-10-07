import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import type * as ProviderAdapter from "../../../orchestration-v2/ProviderAdapter.ts";
import * as OpenCodeRuntime from "../../../provider/opencodeRuntime.ts";

export interface OpenCodeCreationCapture {
  readonly directory: string;
  readonly runtimeGeneration: string;
}

export interface OpenCodeCreationHooks {
  readonly reserveGeneration: Effect.Effect<string, ProviderAdapter.ProviderAdapterV2Error>;
  readonly authorize: (capture: OpenCodeCreationCapture) => Effect.Effect<void, OpenCodeRuntime.OpenCodeRuntimeError>;
  readonly abandonGeneration: (generation: string) => Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>;
}

// Failed authorization proves no spawn was attempted; a spawn failure does not.
export const prepareGeneration = Effect.fn("OpenCodeCreationPolicy.prepareGeneration")(
  (directory: string, hooks: OpenCodeCreationHooks) =>
    Effect.uninterruptibleMask((restore) => Effect.gen(function* () {
      const runtimeGeneration = yield* hooks.reserveGeneration.pipe(Effect.mapError(
        (cause) => new OpenCodeRuntime.OpenCodeRuntimeError({
          operation: "reserveGeneration",
          detail: "Could not reserve an OpenCode process generation.",
          cause,
        }),
      ));
      const capture = Object.freeze({ directory, runtimeGeneration });
      const authorized = yield* Effect.exit(restore(Effect.suspend(() => hooks.authorize(capture))));
      if (Exit.isFailure(authorized)) {
        yield* hooks.abandonGeneration(runtimeGeneration).pipe(Effect.mapError(
          (cause) => new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "abandonGeneration",
            detail: "Could not abandon the unspawned OpenCode process generation.",
            cause: { authorizationFailure: authorized.cause, abandonmentFailure: cause },
          }),
        ));
        return yield* Effect.failCause(authorized.cause);
      }
      return capture;
    })),
);
