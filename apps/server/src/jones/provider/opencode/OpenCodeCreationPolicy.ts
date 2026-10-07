import type { ProviderInstanceId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import * as ProviderAdapter from "../../../orchestration-v2/ProviderAdapter.ts";
import * as OpenCodeRuntime from "../../../provider/opencodeRuntime.ts";

export interface OpenCodeCreationCapture {
  readonly directory: string;
  readonly runtimeGeneration: string;
}

export interface OpenCodeCreationHooks {
  readonly reserveGeneration: Effect.Effect<string, ProviderAdapter.ProviderAdapterV2Error>;
  readonly authorize: (
    capture: OpenCodeCreationCapture,
  ) => Effect.Effect<void, OpenCodeRuntime.OpenCodeRuntimeError>;
  readonly abandonGeneration: (
    generation: string,
  ) => Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error>;
}

// Failed authorization proves no spawn was attempted; a spawn failure does not.
export const prepareGeneration = Effect.fn("OpenCodeCreationPolicy.prepareGeneration")(
  (directory: string, hooks: OpenCodeCreationHooks) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const runtimeGeneration = yield* hooks.reserveGeneration.pipe(
          Effect.mapError(
            (cause) =>
              new OpenCodeRuntime.OpenCodeRuntimeError({
                operation: "reserveGeneration",
                detail: "Could not reserve an OpenCode process generation.",
                cause,
              }),
          ),
        );
        const capture = Object.freeze({ directory, runtimeGeneration });
        const authorized = yield* Effect.exit(
          restore(Effect.suspend(() => hooks.authorize(capture))),
        );
        if (Exit.isFailure(authorized)) {
          yield* hooks.abandonGeneration(runtimeGeneration).pipe(
            Effect.mapError(
              (cause) =>
                new OpenCodeRuntime.OpenCodeRuntimeError({
                  operation: "abandonGeneration",
                  detail: "Could not abandon the unspawned OpenCode process generation.",
                  cause: { authorizationFailure: authorized.cause, abandonmentFailure: cause },
                }),
            ),
          );
          return yield* Effect.failCause(authorized.cause);
        }
        return capture;
      }),
    ),
);

export interface OpenCodePhysicalIncarnation extends OpenCodeCreationCapture {
  readonly pid: number;
  readonly url: string;
}

export interface OpenCodeOwnedProcess {
  readonly incarnation: OpenCodePhysicalIncarnation;
  readonly isCurrent: Effect.Effect<boolean>;
}

export interface OpenCodeSessionAdoptionCapture {
  readonly threadId: ThreadId;
  readonly parent: OpenCodePhysicalIncarnation;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerSessionId: ProviderSessionId;
  readonly runtimeGeneration: string;
}

export interface OpenCodeSessionAdoption {
  readonly capture: OpenCodeSessionAdoptionCapture;
  readonly isCurrent: Effect.Effect<boolean>;
}

export interface OpenCodeQualifiedAuthority {
  readonly creationHooks: OpenCodeCreationHooks;
  readonly authorizeAdoption: (
    capture: OpenCodeSessionAdoptionCapture,
  ) => Effect.Effect<Effect.Effect<boolean>, OpenCodeRuntime.OpenCodeRuntimeError>;
  readonly authorizeConsumption: (capture: {
    readonly parent: OpenCodePhysicalIncarnation;
    readonly kind: "models" | "inventory" | "session" | "other";
  }) => Effect.Effect<void, OpenCodeRuntime.OpenCodeRuntimeError>;
}

export class OpenCodeAuthority extends Context.Service<
  OpenCodeAuthority,
  {
    readonly forInstance: (
      instanceId: ProviderInstanceId,
    ) => OpenCodeQualifiedAuthority | undefined;
  }
>()("t3/jones/provider/opencode/OpenCodeCreationPolicy/OpenCodeAuthority") {}

export const requireCurrent = (parent: OpenCodeOwnedProcess | undefined) =>
  Effect.gen(function* () {
    if (parent === undefined || !(yield* parent.isCurrent)) {
      return yield* new OpenCodeRuntime.OpenCodeRuntimeError({
        operation: "adoptServer",
        detail: "A current captured OpenCode physical incarnation is required.",
      });
    }
    return parent;
  });

export const adoptSession = Effect.fn("OpenCodeCreationPolicy.adoptSession")(
  (input: {
    readonly parent: OpenCodeOwnedProcess | undefined;
    readonly providerInstanceId: ProviderInstanceId;
    readonly session: Pick<
      ProviderAdapter.ProviderAdapterV2OpenSessionInput,
      "threadId" | "providerSessionId" | "runtimeLifecycle"
    >;
    readonly authority: OpenCodeQualifiedAuthority;
  }) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const parent = yield* requireCurrent(input.parent);
        const lifecycle = input.session.runtimeLifecycle;
        if (lifecycle === undefined) {
          return yield* new OpenCodeRuntime.OpenCodeRuntimeError({
            operation: "adoptServer",
            detail: "Qualified OpenCode adoption requires the session lifecycle owner.",
          });
        }
        const runtimeGeneration = yield* lifecycle.reserve(input.session.threadId).pipe(
          Effect.mapError(
            (cause) =>
              new OpenCodeRuntime.OpenCodeRuntimeError({
                operation: "reserveAdoption",
                detail: "Could not reserve an OpenCode session adoption.",
                cause,
              }),
          ),
        );
        const capture = Object.freeze({
          threadId: input.session.threadId,
          parent: parent.incarnation,
          providerInstanceId: input.providerInstanceId,
          providerSessionId: input.session.providerSessionId,
          runtimeGeneration,
        });
        const authorized = yield* Effect.exit(
          restore(
            Effect.gen(function* () {
              const authorizedCurrent = yield* input.authority.authorizeAdoption(capture);
              yield* requireCurrent(parent);
              if (!(yield* authorizedCurrent)) {
                return yield* new OpenCodeRuntime.OpenCodeRuntimeError({
                  operation: "adoptServer",
                  detail: "The OpenCode session adoption authority is no longer current.",
                });
              }
              return Object.freeze({
                capture,
                isCurrent: parent.isCurrent.pipe(
                  Effect.flatMap((current) =>
                    current ? authorizedCurrent : Effect.succeed(false),
                  ),
                ),
              });
            }),
          ),
        );
        if (Exit.isFailure(authorized)) {
          yield* lifecycle.abandon(runtimeGeneration).pipe(
            Effect.mapError(
              (cause) =>
                new OpenCodeRuntime.OpenCodeRuntimeError({
                  operation: "abandonAdoption",
                  detail: "Could not abandon an unadopted OpenCode session generation.",
                  cause,
                }),
            ),
          );
          return yield* Effect.failCause(authorized.cause);
        }
        return authorized.value;
      }),
    ),
);

export const authorizeConsumption = (
  parent: OpenCodeOwnedProcess | undefined,
  authority: OpenCodeQualifiedAuthority | undefined,
  kind: "models" | "inventory" | "session" | "other",
) =>
  authority === undefined
    ? Effect.void
    : Effect.gen(function* () {
        const current = yield* requireCurrent(parent);
        yield* authority.authorizeConsumption(Object.freeze({ parent: current.incarnation, kind }));
        yield* requireCurrent(current);
      });

export const bindSessionAdoption = Effect.fn("OpenCodeCreationPolicy.bindSessionAdoption")(
  (
    adoption: OpenCodeSessionAdoption,
    lifecycle: ProviderAdapter.ProviderRuntimeLifecycle,
    input: Omit<
      Parameters<ProviderAdapter.ProviderRuntimeLifecycle["bind"]>[0],
      "runtimeGeneration"
    >,
  ) =>
    Effect.gen(function* () {
      const parent = { incarnation: adoption.capture.parent, isCurrent: adoption.isCurrent };
      yield* requireCurrent(parent);
      if (
        input.providerThread.appThreadId !== adoption.capture.threadId ||
        input.providerThread.providerSessionId !== adoption.capture.providerSessionId ||
        input.providerThread.providerInstanceId !== adoption.capture.providerInstanceId ||
        input.requested.providerInstanceId !== adoption.capture.providerInstanceId ||
        input.providerThread.driver !== "opencode" ||
        input.requested.providerDriver !== "opencode"
      ) {
        return yield* new OpenCodeRuntime.OpenCodeRuntimeError({
          operation: "bindAdoption",
          detail: "The OpenCode session adoption does not belong to this binding.",
        });
      }
      const bound = yield* lifecycle.bind({
        ...input,
        runtimeGeneration: adoption.capture.runtimeGeneration,
      });
      const verified = yield* requireCurrent(parent).pipe(Effect.exit);
      if (Exit.isFailure(verified)) {
        const binding = ProviderAdapter.runtimeBinding(bound, adoption.capture.runtimeGeneration);
        if (binding !== undefined) yield* lifecycle.invalidate(binding);
        return yield* Effect.failCause(verified.cause);
      }
      return bound;
    }),
);
