import {
  ProviderDriverKind,
  type ThreadId,
  type ProviderRuntimeBinding,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Authority from "./NativeCreationAuthority.ts";
import { NativeCreationWholeOperationEvidence } from "./NativeCreationExecutionTypes.ts";
import { nativeCreationCanonicalJson } from "./NativeCreationPreparation.ts";
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

// Only an authority-issued context created after native100's durable start can issue this private handle.
export const issueNativeProviderGuard = Effect.fn("issueNativeProviderGuard")(function* (input: {
  readonly context: Authority.NativeCreationExecutionContextV2;
  readonly resources: Authority.NativeCreationResources;
  readonly threadId: ThreadId;
  readonly effectId: string;
  readonly commandId: import("@t3tools/contracts").CommandId;
  readonly runId: import("@t3tools/contracts").RunId;
  readonly providerInstanceId: import("@t3tools/contracts").ProviderInstanceId;
}) {
  const reference = Authority.getNativeCreationExecutionReference(input.context);
  if (
    !reference ||
    reference.effectId !== input.effectId ||
    reference.stageCommandId !== input.commandId
  )
    return yield* refused(
      "Native provider guard requires the original authority-issued durable start",
    );
  const binding = yield* Authority.authorizeNativeCreationExecution(input.context, {
    resources: input.resources,
    stage: "native_command",
  }).pipe(Effect.mapError(() => refused("Current native creation authority is unavailable")));
  if (binding.providerModelSelection.instanceId !== input.providerInstanceId)
    return yield* refused("Native provider instance differs from its issued operation");
  const identity: NativeProviderExecutionGuard = { [privateGuard]: true };
  const guard = Object.freeze(identity);
  guards.set(guard, { ...input, retired: false });
  return guard;
});

export const retireNativeProviderGuard = Effect.fn("retireNativeProviderGuard")(function* (
  guard: NativeProviderExecutionGuard,
  evidence: NativeCreationWholeOperationEvidence,
) {
  const state = guards.get(guard);
  if (
    !state ||
    state.retired ||
    !state.acknowledgement ||
    nativeCreationCanonicalJson(state.acknowledgement) !== nativeCreationCanonicalJson(evidence)
  )
    return yield* refused(
      "Native guard cannot retire without its exact whole-operation acknowledgement",
    );
  state.retired = true;
});
