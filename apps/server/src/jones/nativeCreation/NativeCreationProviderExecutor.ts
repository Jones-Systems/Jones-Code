import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as Execution from "./NativeCreationProviderExecution.ts";
import * as Guard from "./NativeCreationProviderGuard.ts";
import { nativeCreationCanonicalJson } from "./NativeCreationPreparation.ts";
import * as TurnStart from "../../orchestration-v2/ProviderTurnStartService.ts";
import * as Adapters from "../../orchestration-v2/ProviderAdapterRegistry.ts";

export const layer = Layer.effect(
  Execution.NativeCreationProviderExecutor,
  Effect.gen(function* () {
    const repository = Option.getOrUndefined(
      yield* Effect.serviceOption(Repository.NativeCreationRepository),
    );
    const start = yield* TurnStart.ProviderTurnStartServiceV2;
    const adapters = yield* Adapters.ProviderAdapterRegistryV2;
    return Execution.NativeCreationProviderExecutor.of({
      assertAvailable: (input) =>
        Effect.gen(function* () {
          const adapter = yield* adapters.get(
            input.preparation.binding.provider_model_selection.instanceId,
          );
          if (
            adapter.driver !== "codex" ||
            adapter.nativeCreationExecution !== true ||
            !start.startNative ||
            !repository?.readExecutionReference ||
            !repository?.readWorkspaceVerified
          )
            return yield* new Execution.NativeCreationProviderExecutionError({
              message: "Whole native Codex execution owner is unavailable",
            });
        }).pipe(
          Effect.mapError(
            () =>
              new Execution.NativeCreationProviderExecutionError({
                message: "Whole native Codex execution owner is unavailable",
              }),
          ),
        ),
      executeWholeOperation: ({ effect, context }) =>
        Effect.gen(function* () {
          const reference = Authority.getNativeCreationExecutionReference(context);
          if (
            !reference ||
            !effect.nativeCreationExecutionReference ||
            nativeCreationCanonicalJson(reference) !==
              nativeCreationCanonicalJson(effect.nativeCreationExecutionReference) ||
            effect.request.type !== "provider-turn.start" ||
            !start.startNative ||
            !repository?.readExecutionReference ||
            !repository?.readWorkspaceVerified
          )
            return yield* new Execution.NativeCreationProviderExecutionError({
              message: "Native executor requires the original issued effect context",
            });
          const original = yield* repository.readExecutionReference(reference);
          const adapter = yield* adapters.get(
            original.history.intent.binding.providerModelSelection.instanceId,
          );
          if (adapter.driver !== "codex" || adapter.nativeCreationExecution !== true)
            return yield* new Execution.NativeCreationProviderExecutionError({
              message: "Native creation requires the actual direct Codex owner",
            });
          const verified = yield* repository.readWorkspaceVerified(reference.claimId);
          if (
            original.command.type !== "prepared-run.release" ||
            original.command.runId !== effect.request.runId ||
            original.command.commandId !== effect.commandId ||
            Option.isNone(verified) ||
            effect.threadId !== original.history.intent.threadId ||
            verified.value.proof.worktreePath !== original.history.intent.resources.worktreePath ||
            verified.value.proof.branch !== original.history.intent.resources.branch
          )
            return yield* new Execution.NativeCreationProviderExecutionError({
              message: "Native executor has no original verified workspace",
            });
          const guard = yield* Guard.issueNativeProviderGuard({
            context,
            resources: original.history.intent.resources,
            threadId: effect.threadId,
            effectId: effect.id,
            commandId: effect.commandId,
            runId: effect.request.runId,
            providerInstanceId: original.history.intent.binding.providerModelSelection.instanceId,
          });
          const evidence = yield* start.startNative({
            threadId: effect.threadId,
            runId: effect.request.runId,
            guard,
          });
          const acknowledgement = Guard.readNativeProviderAcknowledgement(guard);
          if (
            !acknowledgement ||
            nativeCreationCanonicalJson(evidence) !== nativeCreationCanonicalJson(acknowledgement)
          )
            return yield* new Execution.NativeCreationProviderExecutionError({
              message: "Whole native start has no direct exact acknowledgement",
            });
          yield* Guard.retireNativeProviderGuard(guard, acknowledgement);
          return acknowledgement;
        }).pipe(
          Effect.mapError(
            () =>
              new Execution.NativeCreationProviderExecutionError({
                message:
                  "Whole native Codex execution is unconfirmed; original start cannot replay",
              }),
          ),
        ),
    });
  }),
);
