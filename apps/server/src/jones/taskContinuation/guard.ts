import {
  ProviderDriverKind,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import { ProviderAdapterProtocolError } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { NativeSessionRequirement } from "./requiredNativeSession.ts";
import type { TaskContinuationStore } from "./TaskContinuationStore.ts";
import { assertCurrentBinding, continuationRoot, TaskContinuationError } from "./sourceBinding.ts";

const isContinuationMessage = (messageId: string): boolean =>
  messageId.startsWith("message:jones:task-continue:");

export const requirementFor = Effect.fn("TaskContinuation.requirementFor")(function* (
  store: TaskContinuationStore["Service"] | undefined,
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "providerThreads">,
  run: OrchestrationV2Run,
  attempt: OrchestrationV2RunAttempt,
) {
  const root = continuationRoot(projection.runs, run);
  if (Schema.is(TaskContinuationError)(root)) return yield* root;
  if (!isContinuationMessage(root.userMessageId)) return undefined;
  if (store === undefined) {
    return yield* new TaskContinuationError({ reason: "guard_missing" });
  }
  const row = yield* store.get(root.userMessageId);
  if (row === null) {
    return yield* new TaskContinuationError({ reason: "guard_missing" });
  }
  if (
    row.runId !== root.id ||
    row.messageId !== root.userMessageId ||
    row.childThreadId !== projection.thread.id ||
    row.childThreadId !== run.threadId ||
    projection.thread.lineage.relationshipToParent !== "subagent" ||
    projection.thread.lineage.parentThreadId !== row.parentThreadId ||
    row.taskId !== run.delegatedTaskId ||
    row.providerInstanceId !== run.providerInstanceId ||
    row.providerThreadId !== run.providerThreadId ||
    attempt.id !== run.activeAttemptId ||
    attempt.runId !== run.id ||
    attempt.providerInstanceId !== row.providerInstanceId ||
    attempt.providerThreadId !== row.providerThreadId ||
    !modelSelectionsEqual(run.modelSelection, row.modelSelection)
  )
    return yield* new TaskContinuationError({ reason: "ancestry_invalid" });
  const invalid = assertCurrentBinding(row, projection);
  if (invalid !== undefined) return yield* invalid;
  const requirement: NativeSessionRequirement = {
    runId: run.id,
    attemptId: attempt.id,
    nativeSessionId: row.nativeSessionId,
    confirm: (input) =>
      (input.runId !== run.id ||
      input.attemptId !== attempt.id ||
      input.nativeSessionId !== row.nativeSessionId
        ? Effect.fail(new TaskContinuationError({ reason: "witness_conflict" }))
        : store.confirm(row, input)
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProtocolError({
              driver: ProviderDriverKind.make("claudeAgent"),
              detail: "Task continuation session confirmation failed",
              cause,
            }),
        ),
      ),
  };
  return requirement;
});

export function loadedSessionMatches(
  requirement: NativeSessionRequirement,
  before: OrchestrationV2ProviderThread,
  after: OrchestrationV2ProviderThread,
): boolean {
  return (
    before.id === after.id &&
    before.appThreadId === after.appThreadId &&
    before.providerInstanceId === after.providerInstanceId &&
    after.driver === "claudeAgent" &&
    after.nativeThreadRef?.driver === "claudeAgent" &&
    after.nativeThreadRef.nativeId === requirement.nativeSessionId
  );
}
