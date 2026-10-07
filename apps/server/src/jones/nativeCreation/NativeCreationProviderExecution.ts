import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Authority from "./NativeCreationAuthority.ts";
import * as Repository from "./NativeCreationRepository.ts";
import type { NativeCreationWholeOperationEvidence } from "./NativeCreationExecutionTypes.ts";
import type { OrchestrationEffectV2 } from "../../orchestration-v2/EffectOutbox.ts";

export class NativeCreationProviderExecutionError extends Schema.TaggedError<NativeCreationProviderExecutionError>()(
  "NativeCreationProviderExecutionError",
  { message: Schema.String },
) {}

// Installation requires an executor that covers activation, history injection and final start.
// Ordinary start methods cannot manufacture this whole-operation evidence.
export class NativeCreationProviderExecutor extends Context.Service<
  NativeCreationProviderExecutor,
  {
    readonly assertAvailable?: (
      input: Authority.NativeCreationAuthorityInput,
    ) => Effect.Effect<void, NativeCreationProviderExecutionError>;
    readonly executeWholeOperation: (input: {
      readonly effect: OrchestrationEffectV2;
      readonly context: Authority.NativeCreationExecutionContextV2;
    }) => Effect.Effect<NativeCreationWholeOperationEvidence, NativeCreationProviderExecutionError>;
  }
>()("t3/jones/nativeCreation/NativeCreationProviderExecution/NativeCreationProviderExecutor") {}

export const executeNativeProviderEffect = Effect.fn("executeNativeProviderEffect")(function* (
  effect: OrchestrationEffectV2,
) {
  const authority = yield* Effect.serviceOption(Authority.NativeCreationAuthority);
  const repository = yield* Effect.serviceOption(Repository.NativeCreationRepository);
  const executor = yield* Effect.serviceOption(NativeCreationProviderExecutor);
  const reference = effect.nativeCreationExecutionReference;
  if (
    reference === undefined ||
    effect.request.type !== "provider-turn.start" ||
    reference.effectId !== effect.id ||
    reference.stageCommandId !== effect.commandId ||
    effect.leaseOwner === null ||
    effect.leaseExpiresAt === null ||
    effect.attemptCount < 1 ||
    Option.isNone(authority) ||
    authority.value.issueExecution === undefined ||
    Option.isNone(repository) ||
    repository.value.confirmExecution === undefined ||
    repository.value.holdExecution === undefined ||
    Option.isNone(executor)
  )
    return yield* new NativeCreationProviderExecutionError({
      message: "Qualified native provider execution is unavailable",
    });
  const confirm = repository.value.confirmExecution;
  const hold = repository.value.holdExecution;
  // Once the durable start exists, cancellation and any uncertain completion retain an unknown hold.
  const issue = authority.value.issueExecution;
  const execute = executor.value.executeWholeOperation;
  yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const context = yield* issue({
        reference,
        timestamp: DateTime.formatIso(yield* DateTime.now),
      });
      yield* restore(
        Effect.gen(function* () {
          const issued = Authority.getNativeCreationExecutionReference(context);
          if (
            issued === null ||
            issued.effectId !== reference.effectId ||
            issued.claimId !== reference.claimId ||
            issued.stageCommandId !== reference.stageCommandId
          )
            return yield* new NativeCreationProviderExecutionError({
              message: "Native executor received an unissued or mismatched context",
            });
          const evidence = yield* execute({ effect, context });
          yield* confirm({
            reference,
            workerId: effect.leaseOwner!,
            expectedAttempt: effect.attemptCount,
            leaseExpiresAt: effect.leaseExpiresAt!,
            evidence,
          });
        }),
      ).pipe(
        Effect.onExit((exit) =>
          Exit.isSuccess(exit)
            ? Effect.void
            : hold(
                reference,
                "Whole native provider execution did not produce confirmed completion",
              ),
        ),
      );
    }),
  );
});
