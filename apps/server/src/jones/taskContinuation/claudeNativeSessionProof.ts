import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import type { ProviderAdapterV2Error } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";

import {
  NativeSessionIdentityRejected,
  type NativeSessionRequirement,
} from "./requiredNativeSession.ts";

export const claudeNativeSessionWarmQueryAction = (
  requirement: NativeSessionRequirement,
  observed: string | null,
  busy: boolean,
): "reuse" | "reopen" | NativeSessionIdentityRejected => {
  if (observed === requirement.nativeSessionId) return "reuse";
  if (observed === null && !busy) return "reopen";
  return new NativeSessionIdentityRejected({
    expected: requirement.nativeSessionId,
    observed,
    reason: observed === null ? "unobserved_live_query" : "init_mismatch",
  });
};

export const claudeNativeSessionFrameRequiresProof = (message: SDKMessage): boolean =>
  message.type === "assistant" ||
  message.type === "user" ||
  message.type === "result" ||
  message.type === "stream_event" ||
  message.type === "tool_progress" ||
  message.type === "tool_use_summary" ||
  (message.type === "system" &&
    (message.subtype === "task_started" ||
      message.subtype === "task_progress" ||
      message.subtype === "task_notification" ||
      (message.subtype as string) === "background_tasks_changed"));

export const makeClaudeNativeSessionProof = Effect.fn("ClaudeNativeSessionProof.make")(function* (
  requirement: NativeSessionRequirement,
) {
  const permit = yield* Semaphore.make(1);
  let confirmed = false;
  let failure: NativeSessionIdentityRejected | ProviderAdapterV2Error | undefined;
  const confirm = Effect.fn("ClaudeNativeSessionProof.confirm")(
    (observed: string | null, runtimeGeneration: string, init = false) =>
      permit.withPermits(1)(
        Effect.gen(function* () {
          if (failure !== undefined) return yield* failure;
          if (observed !== requirement.nativeSessionId) {
            failure = new NativeSessionIdentityRejected({
              expected: requirement.nativeSessionId,
              observed,
              reason: observed === null && !init ? "output_before_init" : "init_mismatch",
            });
            return yield* failure;
          }
          if (confirmed) return;
          yield* requirement
            .confirm({
              runId: requirement.runId,
              attemptId: requirement.attemptId,
              nativeSessionId: observed,
              runtimeGeneration,
            })
            .pipe(
              Effect.tapError((error) =>
                Effect.sync(() => {
                  failure = error;
                }),
              ),
            );
          confirmed = true;
        }),
      ),
  );
  return { requirement, confirm };
});

export type ClaudeNativeSessionProof = Effect.Success<
  ReturnType<typeof makeClaudeNativeSessionProof>
>;
