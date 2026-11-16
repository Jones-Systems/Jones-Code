import { ServerSelfUpdateError, type ThreadId } from "@t3tools/contracts";
import type { ServerSelfUpdate, QualifiedStageVerifier } from "../../cloud/selfUpdate.ts";
import type { StagedQualifiedRuntime } from "../cloud/qualifiedRuntime.ts";
import * as ServerConfig from "../../config.ts";
import * as ServiceLauncherClient from "../../cloud/serviceLauncherClient.ts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as HashSet from "effect/HashSet";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
const isServiceLauncherClientError = Schema.is(ServiceLauncherClient.ServiceLauncherClientError);

const timedQualifiedStep = Effect.fn("ServerSelfUpdate.timedQualifiedStep")(function* <A, E, R>(
  step: string,
  operationId: string | undefined,
  effect: Effect.Effect<A, E, R>,
) {
  const started = yield* Clock.currentTimeMillis;
  return yield* effect.pipe(
    Effect.ensuring(
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((completed) =>
          Effect.logInfo("Qualified installation timing", {
            operationId,
            step,
            durationMs: completed - started,
          }),
        ),
      ),
    ),
  );
});

export const withRunningThreadContinuation = Effect.fn(
  "cloud.server_self_update.withRunningThreadContinuation",
)(function* (input: {
  readonly mode: ServerConfig.RuntimeMode;
  readonly selfUpdate: ServerSelfUpdate["Service"];
  readonly prepare: Effect.Effect<ReadonlyArray<ThreadId>, ServerSelfUpdateError>;
  readonly clear: (
    threadIds: ReadonlyArray<ThreadId>,
  ) => Effect.Effect<void, ServerSelfUpdateError>;
}) {
  const desktopContinuationTokens = yield* Ref.make(HashSet.empty<string>());
  // Held after an uncertain handoff until the launcher proves it rejected.
  const qualifiedInstallInFlight = yield* Ref.make<
    | {
        readonly operationId: string | undefined;
        readonly uncertain: boolean;
        readonly threadIds: ReadonlyArray<ThreadId>;
      }
    | undefined
  >(undefined);
  const uncertainQualifiedHandoff = (cause: Cause.Cause<ServerSelfUpdateError>) => {
    const error = Cause.findErrorOption(cause);
    return (
      Option.isSome(error) &&
      isServiceLauncherClientError(error.value.cause) &&
      ["send", "disconnect", "timeout"].includes(error.value.cause.operation)
    );
  };
  const clearOnError = <A>(
    effect: Effect.Effect<A, ServerSelfUpdateError>,
    threadIds: () => ReadonlyArray<ThreadId>,
    handoffAccepted: () => boolean,
    preserveUncertainHandoff = false,
  ): Effect.Effect<A, ServerSelfUpdateError> =>
    effect.pipe(
      Effect.catchCause((cause) =>
        ((handoffAccepted() && Cause.hasInterruptsOnly(cause)) ||
        (preserveUncertainHandoff && uncertainQualifiedHandoff(cause))
          ? Effect.void
          : input.clear(threadIds())
        ).pipe(Effect.andThen(Effect.failCause(cause))),
      ),
    );

  const update: ServerSelfUpdate["Service"]["update"] = (
    request,
    reportProgress = () => Effect.void,
  ) => {
    let prepared = false;
    let handoffAccepted = false;
    let continuationThreadIds: ReadonlyArray<ThreadId> = [];
    return clearOnError(
      input.selfUpdate
        .update(
          request,
          (stage) =>
            (request.continueRunningThreads === true &&
            input.mode !== "desktop" &&
            stage === "installing" &&
            !prepared
              ? input.prepare.pipe(
                  Effect.tap((threadIds) =>
                    Effect.sync(() => {
                      prepared = true;
                      continuationThreadIds = threadIds;
                    }),
                  ),
                  Effect.asVoid,
                )
              : Effect.void
            ).pipe(Effect.andThen(reportProgress(stage))),
          () =>
            Effect.sync(() => {
              handoffAccepted = true;
            }),
        )
        .pipe(
          Effect.tap((result) => {
            if (
              result.method === "desktop-app" &&
              result.desktopUpdateToken !== undefined &&
              request.continueRunningThreads === true
            ) {
              return Ref.update(desktopContinuationTokens, HashSet.add(result.desktopUpdateToken));
            }
            return Effect.void;
          }),
        ),
      () => continuationThreadIds,
      () => handoffAccepted,
    );
  };

  /**
   * The launcher proved the uncertain handoff of `operationId` ended without acceptance: its
   * continuation markers are cleared and another Install may start.
   */
  const releaseRejectedHandoff = Effect.fn("ServerSelfUpdate.releaseRejectedHandoff")(function* (
    operationId: string,
  ) {
    const inFlight = yield* Ref.get(qualifiedInstallInFlight);
    if (inFlight?.operationId !== operationId || !inFlight.uncertain) return;
    yield* input
      .clear(inFlight.threadIds)
      .pipe(
        Effect.ensuring(
          Ref.update(qualifiedInstallInFlight, (current) =>
            current === inFlight ? undefined : current,
          ),
        ),
      );
  });

  const service: ServerSelfUpdate["Service"] = {
    update,
    ...(input.selfUpdate.installQualified === undefined
      ? {}
      : {
          installQualified: (
            request: {
              readonly stagedHandle: string;
              readonly continueRunningThreads?: boolean;
              readonly operationId?: string;
              readonly verifyStaged: QualifiedStageVerifier;
            },
            onAccepted?: () => Effect.Effect<void>,
            onPrepared?: (
              staged: StagedQualifiedRuntime,
            ) => Effect.Effect<void, ServerSelfUpdateError>,
            reportStep?: (
              step: "marking" | "verifying" | "preflight" | "capturing" | "handoff",
            ) => Effect.Effect<void>,
            onPreparationRefused?: () => Effect.Effect<void>,
          ) => {
            let handoffAccepted = false;
            let continuationThreadIds: ReadonlyArray<ThreadId> = [];
            return Effect.gen(function* () {
              const attempt = {
                operationId: request.operationId,
                uncertain: false,
                threadIds: [] as ReadonlyArray<ThreadId>,
              };
              const claimed = yield* Ref.modify(qualifiedInstallInFlight, (current) =>
                current === undefined ? ([true, attempt] as const) : ([false, current] as const),
              );
              if (!claimed)
                return yield* new ServerSelfUpdateError({
                  reason: "A qualified Install has already started or needs reconciliation.",
                });
              return yield* clearOnError(
                Effect.gen(function* () {
                  if (request.continueRunningThreads === true) {
                    yield* reportStep?.("marking") ?? Effect.void;
                    continuationThreadIds = yield* timedQualifiedStep(
                      "marking",
                      request.operationId,
                      input.prepare,
                    );
                  }
                  return yield* input.selfUpdate.installQualified!(
                    request,
                    () =>
                      Effect.sync(() => {
                        handoffAccepted = true;
                      }).pipe(Effect.andThen(onAccepted?.() ?? Effect.void)),
                    onPrepared,
                    reportStep,
                    onPreparationRefused,
                  );
                }),
                () => continuationThreadIds,
                () => handoffAccepted,
                true,
              ).pipe(
                Effect.catchCause((cause) =>
                  Ref.update(qualifiedInstallInFlight, (current) =>
                    current !== attempt
                      ? current
                      : handoffAccepted || uncertainQualifiedHandoff(cause)
                        ? {
                            ...attempt,
                            uncertain: !handoffAccepted,
                            threadIds: continuationThreadIds,
                          }
                        : undefined,
                  ).pipe(Effect.andThen(Effect.failCause(cause))),
                ),
              );
            }).pipe((effect) => timedQualifiedStep("total", request.operationId, effect));
          },
        }),
    commitDesktopUpdate: Effect.fn("selfUpdate.commitDesktopUpdate")(function* (requestId) {
      const shouldContinue = yield* Ref.modify(desktopContinuationTokens, (tokens) => [
        HashSet.has(tokens, requestId),
        HashSet.remove(tokens, requestId),
      ]);
      let handoffAccepted = false;
      let continuationThreadIds: ReadonlyArray<ThreadId> = [];
      return yield* clearOnError(
        Effect.gen(function* () {
          continuationThreadIds = shouldContinue ? yield* input.prepare : [];
          return yield* input.selfUpdate.commitDesktopUpdate(requestId, () =>
            Effect.sync(() => {
              handoffAccepted = true;
            }),
          );
        }),
        () => continuationThreadIds,
        () => handoffAccepted,
      ).pipe(
        Effect.catchCause((cause) =>
          (shouldContinue && !handoffAccepted
            ? Ref.update(desktopContinuationTokens, HashSet.add(requestId))
            : Effect.void
          ).pipe(Effect.andThen(Effect.failCause(cause))),
        ),
      );
    }),
  };
  return { ...service, releaseRejectedHandoff };
});
