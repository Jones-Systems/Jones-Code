import { assertLegacyBootstrapAllowed } from "../auth/RpcAuthorization.ts";
import { EnvironmentAuthenticatedPrincipal } from "@t3tools/contracts";
import { NativeCreationRepository } from "../persistence/Services/NativeCreationRepository.ts";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  type EnvironmentInternalError,
  type EnvironmentRequestInvalidError,
  type ProviderGoalStateObservation,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import { ProviderService } from "../provider/Services/ProviderService.ts";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import { projectThreadDetailSnapshot } from "./ActivityPayloadProjection.ts";
import { cleanupFailedUploadedAttachments, normalizeDispatchCommand } from "./Normalizer.ts";
import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
  failEnvironmentNotFound,
  requireEnvironmentScope,
} from "../auth/http.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";

export const orchestrationHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "orchestration",
  Effect.fnUntraced(function* (handlers) {
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const projectCloneTracker = yield* ProjectCloneTracker.ProjectCloneTracker;
    const nativeCreationRepository = yield* Effect.serviceOption(NativeCreationRepository);
    const providerService = yield* Effect.serviceOption(ProviderService);

    return handlers
      .handle(
        "snapshot",
        Effect.fn("environment.orchestration.snapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          // Serve the lightweight command read model (thread bodies empty)
          // instead of the fully hydrated snapshot. Hydrating every message
          // and activity payload in the database has OOM-killed servers, and
          // the route's only consumer (the project CLI) reads projects alone —
          // UI clients load the shell and per-thread snapshots instead.
          return yield* projectionSnapshotQuery
            .getCommandReadModel()
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_snapshot_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "shellSnapshot",
        Effect.fn("environment.orchestration.shellSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* projectionSnapshotQuery
            .getShellSnapshot()
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_snapshot_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "threadSnapshot",
        Effect.fn("environment.orchestration.threadSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const snapshot = yield* projectionSnapshotQuery
            .getThreadDetailSnapshot(
              args.params.threadId,
              args.payload.turnLimit === undefined
                ? undefined
                : {
                    turnLimit: args.payload.turnLimit,
                    ...(args.payload.beforeCursor !== undefined
                      ? { beforeCursor: args.payload.beforeCursor }
                      : {}),
                  },
            )
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_thread_snapshot_failed", cause),
              ),
            );
          if (Option.isNone(snapshot)) {
            return yield* failEnvironmentNotFound("thread_not_found");
          }
          return projectThreadDetailSnapshot(
            snapshot.value,
            args.payload.reasoningMessages === "true",
          );
        }),
      )
      .handle(
        "commandObservation",
        Effect.fn("environment.orchestration.commandObservation")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          if (orchestrationEngine.observeCommand === undefined) {
            return yield* failEnvironmentInvalidRequest("observation_unsupported");
          }
          return yield* orchestrationEngine
            .observeCommand({ ...args.params, messageId: args.payload.messageId })
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_thread_snapshot_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "providerGoalState",
        Effect.fn("environment.orchestration.providerGoalState")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const unknown = (reasonCode: ProviderGoalStateObservation["reasonCode"]) =>
            DateTime.now.pipe(
              Effect.map((now): ProviderGoalStateObservation => ({
                schema: "t3.provider-goal-state/v1",
                threadId: args.params.threadId,
                providerInstanceId: args.payload.expectedInstanceId,
                nativeThreadId: null,
                observedAtMs: DateTime.toEpochMillis(now),
                state: "unknown",
                reasonCode,
              })),
            );
          const matches = (thread: OrchestrationThreadShell) =>
            thread.id === args.params.threadId &&
            thread.modelSelection.instanceId === args.payload.expectedInstanceId &&
            (thread.session === null ||
              (thread.session.threadId === args.params.threadId &&
                thread.session.providerInstanceId === args.payload.expectedInstanceId &&
                thread.session.providerName === "codex"));
          const read = Effect.gen(function* () {
            const before = yield* projectionSnapshotQuery.getThreadShellById(args.params.threadId);
            if (Option.isNone(before)) return yield* unknown("no_session");
            if (!matches(before.value)) return yield* unknown("instance_mismatch");
            if (
              Option.isNone(providerService) ||
              providerService.value.getProviderGoalState === undefined
            )
              return yield* unknown("unsupported");
            const observation = yield* providerService.value.getProviderGoalState({
              threadId: args.params.threadId,
              expectedInstanceId: args.payload.expectedInstanceId,
            });
            const after = yield* projectionSnapshotQuery.getThreadShellById(args.params.threadId);
            if (Option.isNone(after) || !matches(after.value))
              return yield* unknown("context_changed");
            if (
              observation.threadId !== args.params.threadId ||
              observation.providerInstanceId !== args.payload.expectedInstanceId
            ) {
              return yield* unknown("context_changed");
            }
            return {
              schema: observation.schema,
              threadId: observation.threadId,
              providerInstanceId: observation.providerInstanceId,
              nativeThreadId: observation.nativeThreadId,
              observedAtMs: observation.observedAtMs,
              state: observation.state,
              reasonCode: observation.reasonCode,
            };
          });
          return yield* read.pipe(Effect.catchCause(() => unknown("rpc_error")));
        }),
      )
      .handle(
        "dispatch",
        Effect.fn("environment.orchestration.dispatch")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          if (args.payload.type === "thread.turn.start" && args.payload.bootstrap !== undefined) {
            const principal = yield* EnvironmentAuthenticatedPrincipal;
            if (Option.isNone(nativeCreationRepository))
              return yield* failEnvironmentInvalidRequest("invalid_command");
            yield* assertLegacyBootstrapAllowed({
              actorSessionId: principal.sessionId,
              command: args.payload,
              hasAutomationEnrollment: nativeCreationRepository.value.hasAutomationEnrollment,
            }).pipe(Effect.catch(() => failEnvironmentInvalidRequest("invalid_command")));
          }
          yield* ProjectCloneTracker.rejectCommandsDuringClone(
            projectCloneTracker,
            args.payload,
          ).pipe(
            Effect.catch((cause) =>
              failEnvironmentInternal("orchestration_dispatch_failed", cause),
            ),
          );
          if (
            args.payload.type === "thread.turn.start" &&
            args.payload.dispatchGuard !== undefined &&
            args.payload.bootstrap !== undefined
          ) {
            return yield* failEnvironmentInvalidRequest("dispatch_guard_bootstrap_unsupported");
          }
          const normalizedCommand = yield* normalizeDispatchCommand(args.payload).pipe(
            Effect.catch(() => failEnvironmentInvalidRequest("invalid_command")),
          );
          const result = yield* orchestrationEngine.dispatch(normalizedCommand).pipe(
            Effect.tapError(() =>
              cleanupFailedUploadedAttachments(args.payload, normalizedCommand),
            ),
            Effect.catch(
              (
                cause,
              ): Effect.Effect<
                never,
                EnvironmentInternalError | EnvironmentRequestInvalidError
              > => {
                if (
                  args.payload.type === "thread.turn.start" &&
                  args.payload.dispatchGuard !== undefined &&
                  (cause._tag === "OrchestrationCommandPreviouslyRejectedError" ||
                    (cause._tag === "OrchestrationCommandInvariantError" &&
                      cause.detail.startsWith("dispatch_guard_rejected:")))
                ) {
                  return failEnvironmentInvalidRequest("dispatch_guard_rejected");
                }
                return failEnvironmentInternal("orchestration_dispatch_failed", cause);
              },
            ),
          );
          yield* ProjectCloneTracker.discardCloneForDeletedProject(
            projectCloneTracker,
            normalizedCommand,
          );
          return result;
        }),
      );
  }),
);
