import {
  ProviderDriverKind,
  type ThreadId,
  type ProviderRuntimeBinding,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as Execution from "./NativeCreationProviderExecution.ts";
import { NativeCreationWholeOperationEvidence } from "./NativeCreationExecutionTypes.ts";
import { nativeCreationCanonicalJson } from "./NativeCreationPreparation.ts";
import * as TurnStart from "../../orchestration-v2/ProviderTurnStartService.ts";
import * as Adapters from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import { ProviderAdapterProtocolError } from "../../orchestration-v2/ProviderAdapter.ts";

const privateGuard = Symbol("NativeProviderExecutionGuard");
export interface NativeProviderExecutionGuard {
  readonly [privateGuard]: true;
}
const guards = new WeakMap<
  NativeProviderExecutionGuard,
  {
    readonly context: Authority.NativeCreationExecutionContextV2;
    readonly resources: Authority.NativeCreationResources;
    readonly threadId: ThreadId;
    readonly effectId: string;
    readonly commandId: import("@t3tools/contracts").CommandId;
    readonly runId: import("@t3tools/contracts").RunId;
    readonly providerInstanceId: import("@t3tools/contracts").ProviderInstanceId;
    binding?: ProviderRuntimeBinding;
    acknowledgement?: NativeCreationWholeOperationEvidence;
    retired: boolean;
  }
>();
const refused = (message: string) =>
  new ProviderAdapterProtocolError({ driver: ProviderDriverKind.make("codex"), detail: message });

// This private handle is issued only after the native100 durable start; wire correlation cannot mint it.
export const revalidateNativeProviderGuard = Effect.fn("revalidateNativeProviderGuard")(function* (
  guard: NativeProviderExecutionGuard | undefined,
  input: {
    readonly threadId: ThreadId | null;
    readonly cwd?: string | null;
    readonly runtimeGeneration?: string;
  },
) {
  if (guard === undefined) return;
  const state = guards.get(guard);
  if (
    !state ||
    state.threadId !== input.threadId ||
    (input.cwd !== undefined && input.cwd !== state.resources.worktreePath)
  )
    return yield* refused("Native provider operation has no exact issued target");
  if (state.retired) return;
  if (
    input.runtimeGeneration !== undefined &&
    state.binding?.runtimeGeneration !== input.runtimeGeneration
  )
    return yield* refused("Native provider physical generation changed before its operation");
  yield* Authority.authorizeNativeCreationExecution(state.context, {
    resources: state.resources,
    stage: "native_command",
  }).pipe(Effect.mapError(() => refused("Current native creation authority is unavailable")));
});

export const bindNativeProviderGuard = Effect.fn("bindNativeProviderGuard")(function* (
  guard: NativeProviderExecutionGuard | undefined,
  binding: ProviderRuntimeBinding,
) {
  if (guard === undefined) return;
  yield* revalidateNativeProviderGuard(guard, { threadId: binding.threadId });
  const state = guards.get(guard);
  if (
    !state ||
    state.retired ||
    binding.driver !== "codex" ||
    binding.providerInstanceId !== state.providerInstanceId ||
    (state.binding !== undefined &&
      nativeCreationCanonicalJson(state.binding) !== nativeCreationCanonicalJson(binding))
  )
    return yield* refused("Native provider binding cannot be replaced or reconstructed");
  state.binding = Object.freeze({ ...binding });
});

// The Codex send owner calls this only after its exact physical request acknowledgement.
export const acknowledgeNativeProviderGuard = Effect.fn("acknowledgeNativeProviderGuard")(
  function* (
    guard: NativeProviderExecutionGuard | undefined,
    input: {
      readonly threadId: ThreadId;
      readonly runId: import("@t3tools/contracts").RunId;
      readonly providerInstanceId: import("@t3tools/contracts").ProviderInstanceId;
      readonly attemptId: import("@t3tools/contracts").RunAttemptId;
      readonly providerThread: OrchestrationV2ProviderThread;
      readonly runtimeGeneration: string;
    },
  ) {
    if (guard === undefined) return;
    yield* revalidateNativeProviderGuard(guard, {
      threadId: input.threadId,
      runtimeGeneration: input.runtimeGeneration,
    });
    const state = guards.get(guard);
    const binding = state?.binding;
    if (
      !state ||
      state.retired ||
      state.acknowledgement !== undefined ||
      !binding ||
      input.providerInstanceId !== state.providerInstanceId ||
      input.providerInstanceId !== binding.providerInstanceId ||
      input.runId !== state.runId ||
      input.providerThread.id !== binding.providerThreadId ||
      input.providerThread.providerSessionId !== binding.providerSessionId ||
      input.providerThread.nativeThreadRef?.nativeId !== binding.nativeThreadId
    )
      return yield* refused("Native provider acknowledgement differs from its captured operation");
    state.acknowledgement = yield* Schema.decodeUnknownEffect(NativeCreationWholeOperationEvidence)(
      {
        version: 1,
        outcome: "confirmed_success",
        effectId: state.effectId,
        threadId: state.threadId,
        commandId: state.commandId,
        providerSessionId: binding.providerSessionId,
        providerThreadId: binding.providerThreadId,
        runtimeGeneration: binding.runtimeGeneration,
        runId: input.runId,
        attemptId: input.attemptId,
        coverage: "whole_operation",
      },
    );
  },
);

export const isNativeProviderGuardActive = (
  guard: NativeProviderExecutionGuard | undefined,
): boolean => guard !== undefined && guards.get(guard)?.retired !== true;

export const readNativeProviderAcknowledgement = (guard: NativeProviderExecutionGuard) =>
  guards.get(guard)?.acknowledgement;

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
          const identity: NativeProviderExecutionGuard = { [privateGuard]: true };
          const guard = Object.freeze(identity);
          const state = {
            context,
            resources: original.history.intent.resources,
            threadId: effect.threadId,
            effectId: effect.id,
            commandId: effect.commandId,
            runId: effect.request.runId,
            providerInstanceId: original.history.intent.binding.providerModelSelection.instanceId,
            retired: false,
          };
          guards.set(guard, state);
          yield* revalidateNativeProviderGuard(guard, { threadId: effect.threadId });
          const evidence = yield* start.startNative({
            threadId: effect.threadId,
            runId: effect.request.runId,
            guard,
          });
          const acknowledgement = readNativeProviderAcknowledgement(guard);
          if (
            !acknowledgement ||
            nativeCreationCanonicalJson(evidence) !== nativeCreationCanonicalJson(acknowledgement)
          )
            return yield* new Execution.NativeCreationProviderExecutionError({
              message: "Whole native start has no direct exact acknowledgement",
            });
          state.retired = true;
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
