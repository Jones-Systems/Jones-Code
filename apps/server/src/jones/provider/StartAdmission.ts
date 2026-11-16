import type {
  OrchestrationV2Run,
  OrchestrationV2ProviderThread,
  RunAttemptId,
  ThreadId,
} from "@t3tools/contracts";
import type {
  ProviderAdapterV2Error,
  ProviderAdapterV2SessionRuntime,
  ProviderAdapterV2TurnInput,
} from "@t3tools/provider-core/server/ProviderAdapter";
import type { GoalContinuityContext } from "@t3tools/provider-core/server/jones/GoalContinuityContext";
import type { NativeProviderExecutionGuard } from "@t3tools/provider-core/server/jones/runtimeTypes";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface CapturedStartAdmission {
  readonly revalidate: Effect.Effect<void, ProviderAdapterV2Error>;
  readonly nativeCreationGuard?: NativeProviderExecutionGuard;
  // Only durable original-prefix/no-effect evidence may authorize resending input.
  readonly classifyResume: Effect.Effect<
    "unsent" | "native_resume" | "held",
    ProviderAdapterV2Error
  >;
}

// The durable command owner supplies this port. There is deliberately no permissive layer.
export class StartAdmission extends Context.Service<StartAdmission, {
  readonly capture: (input: {
    readonly threadId: ThreadId;
    readonly run: OrchestrationV2Run;
    readonly attemptId: RunAttemptId;
    readonly providerThread: OrchestrationV2ProviderThread;
  }) => Effect.Effect<CapturedStartAdmission, ProviderAdapterV2Error>;
}>()("jones/provider/StartAdmission") {}

export function withStartAdmission(
  session: ProviderAdapterV2SessionRuntime,
  revalidate: Effect.Effect<void, ProviderAdapterV2Error>,
  continuity: GoalContinuityContext,
  nativeCreationGuard?: NativeProviderExecutionGuard,
): ProviderAdapterV2SessionRuntime {
  const input = (value: ProviderAdapterV2TurnInput): ProviderAdapterV2TurnInput => ({
    ...value,
    goalContinuityContext: continuity,
    ...(nativeCreationGuard === undefined ? {} : { nativeCreationGuard }),
    revalidateStartAdmission: revalidate.pipe(
      Effect.andThen(value.revalidateStartAdmission ?? Effect.void),
    ),
  });
  return {
    ...session,
    get providerSession() { return session.providerSession; },
    startTurn: (value) => revalidate.pipe(Effect.andThen(session.startTurn(input(value)))),
    ...(session.resumeTurn === undefined ? {} : {
      resumeTurn: (value) => {
        const guarded = input({ ...value, message: {
          messageId: value.messageId, text: "", attachments: [],
          createdBy: "user", creationSource: "server",
        } });
        const { message: _message, resume: _resume, ...rest } = guarded;
        return revalidate.pipe(Effect.andThen(session.resumeTurn!({
          ...rest, messageId: value.messageId,
        })));
      },
    }),
    ...(session.compactThread === undefined ? {} : {
      compactThread: (value) => revalidate.pipe(Effect.andThen(session.compactThread!(input(value)))),
    }),
  };
}
