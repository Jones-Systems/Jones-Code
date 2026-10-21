import type * as CodexClient from "effect-codex-app-server/client";
import * as CodexSchema from "effect-codex-app-server/schema";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { OrchestrationV2ProviderGoal } from "@t3tools/contracts";
import type {
  ProviderGoalControlInput,
  ProviderGoalControlResult,
} from "@t3tools/provider-core/server/jones/runtimeTypes";

export const controlCodexGoal = Effect.fn("CodexGoalControl.controlCodexGoal")(function* (
  input: ProviderGoalControlInput,
  native: {
    readonly client: Pick<CodexClient.CodexAppServerClient["Service"], "request">;
    readonly current: Effect.Effect<boolean>;
    readonly beforeMutation: (action: ProviderGoalControlInput["action"]) => Effect.Effect<void>;
    readonly observe: (goal: OrchestrationV2ProviderGoal | null) => Effect.Effect<void>;
  },
): Effect.fn.Return<ProviderGoalControlResult> {
  const threadId = input.providerThread.nativeThreadRef?.nativeId;
  const result = (
    outcome: ProviderGoalControlResult["outcome"],
    reason: string,
    goal: OrchestrationV2ProviderGoal | null = null,
  ): ProviderGoalControlResult => ({ outcome, reason, goal });
  const current = native.current.pipe(
    Effect.flatMap((valid) => (valid ? input.revalidate : Effect.succeed(false))),
  );
  if (!threadId || !(yield* current)) return result("stale", "binding_changed");
  const read = native.client
    .request("thread/goal/get", { threadId })
    .pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(CodexSchema.V2ThreadGoalGetResponse)),
      Effect.timeout("3 seconds"),
      Effect.result,
    );
  const project = (
    goal: CodexSchema.V2ThreadGoalGetResponse["goal"],
  ): OrchestrationV2ProviderGoal | null =>
    goal == null
      ? null
      : {
          objective: goal.objective,
          status:
            goal.status === "usageLimited"
              ? "usage_limited"
              : goal.status === "budgetLimited"
                ? "budget_limited"
                : goal.status,
          tokensUsed: goal.tokensUsed,
          tokenBudget: goal.tokenBudget ?? null,
          timeUsedSeconds: goal.timeUsedSeconds,
        };
  const before = yield* read;
  if (before._tag === "Failure")
    return result(
      before.failure._tag === "CodexAppServerRequestError" && before.failure.code === -32601
        ? "unsupported"
        : "unavailable",
      "native_read_failed",
    );
  if (before.success.goal === undefined) return result("unavailable", "goal_field_omitted");
  if (
    !(yield* current) ||
    (before.success.goal !== null && before.success.goal.threadId !== threadId)
  )
    return result("stale", "binding_changed");
  const goal = project(before.success.goal);
  yield* native.observe(goal);
  if (input.action === "read") return result("observed", "native_read", goal);
  if (input.action === "replace" && !input.objective?.trim())
    return result("unsupported", "objective_required", goal);
  if (goal === null && input.action !== "clear") return result("unavailable", "goal_missing");
  if (!(yield* current)) return result("stale", "binding_changed");
  yield* native.beforeMutation(input.action);
  const mutation = yield* native.client
    .request(
      input.action === "clear" ? "thread/goal/clear" : "thread/goal/set",
      input.action === "clear"
        ? { threadId }
        : input.action === "pause"
          ? { threadId, status: "paused" }
          : { threadId, objective: input.objective },
    )
    .pipe(Effect.timeout("3 seconds"), Effect.result);
  // Even a lost acknowledgement requires readback; the caller must never replay an uncertain mutation.
  const after = yield* read;
  if (!(yield* current)) return result("unknown", "binding_changed_after_mutation");
  if (after._tag === "Failure") return result("unknown", "readback_failed");
  if (after.success.goal === undefined) return result("unknown", "goal_field_omitted_in_readback");
  if (after.success.goal !== null && after.success.goal.threadId !== threadId)
    return result("unknown", "readback_binding_mismatch");
  const observed = project(after.success.goal);
  yield* native.observe(observed);
  const matches =
    input.action === "clear"
      ? observed === null
      : input.action === "pause"
        ? observed?.status === "paused"
        : observed?.objective === input.objective && observed?.status === goal?.status;
  return result(
    matches ? "applied" : "unknown",
    matches
      ? mutation._tag === "Failure"
        ? "reconciled_after_lost_acknowledgement"
        : "native_readback"
      : "readback_did_not_confirm",
    observed,
  );
});
