import type * as CodexClient from "effect-codex-app-server/client";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as CodexSchema from "effect-codex-app-server/schema";

export type ProviderGoalReasonCode =
  | "goal_null"
  | "goal_present"
  | "no_session"
  | "session_stopped"
  | "instance_mismatch"
  | "native_cursor_missing"
  | "unsupported"
  | "timeout"
  | "malformed"
  | "goal_field_omitted"
  | "rpc_error"
  | "context_changed";

export interface ProviderGoalReadResult {
  readonly nativeThreadId: string | null;
  readonly state: "inactive" | "active" | "unknown";
  readonly reasonCode: ProviderGoalReasonCode;
}

export const unknownProviderGoal = (
  reasonCode: ProviderGoalReasonCode,
  nativeThreadId: string | null = null,
): ProviderGoalReadResult => ({ nativeThreadId, state: "unknown", reasonCode });

function classifyProviderGoal(response: unknown, nativeThreadId: string): ProviderGoalReadResult {
  if (typeof response !== "object" || response === null || Array.isArray(response)) {
    return unknownProviderGoal("malformed", nativeThreadId);
  }
  if (!Object.hasOwn(response, "goal"))
    return unknownProviderGoal("goal_field_omitted", nativeThreadId);
  if (!("goal" in response)) return unknownProviderGoal("malformed", nativeThreadId);
  if (response.goal === null) return { nativeThreadId, state: "inactive", reasonCode: "goal_null" };
  if (!Schema.is(CodexSchema.V2ThreadGoalGetResponse__ThreadGoal)(response.goal)) {
    return unknownProviderGoal("malformed", nativeThreadId);
  }
  if (response.goal.threadId !== nativeThreadId)
    return unknownProviderGoal("context_changed", nativeThreadId);
  return { nativeThreadId, state: "active", reasonCode: "goal_present" };
}

export const readCodexGoalState = (
  client: { readonly raw: Pick<CodexClient.CodexAppServerClient["Service"]["raw"], "request"> },
  context: Effect.Effect<{ readonly nativeThreadId: string | null; readonly stopped: boolean }>,
): Effect.Effect<ProviderGoalReadResult> =>
  Effect.gen(function* () {
    const before = yield* context;
    if (before.stopped) return unknownProviderGoal("session_stopped", before.nativeThreadId);
    if (!before.nativeThreadId) return unknownProviderGoal("native_cursor_missing");
    const nativeThreadId = before.nativeThreadId;
    const result = yield* client.raw.request("thread/goal/get", { threadId: nativeThreadId }).pipe(
      Effect.map((response) => classifyProviderGoal(response, nativeThreadId)),
      Effect.timeout("3 seconds"),
      Effect.catch((error) =>
        Effect.succeed(
          unknownProviderGoal(
            error._tag === "TimeoutError"
              ? "timeout"
              : error._tag === "CodexAppServerRequestError" && error.code === -32601
                ? "unsupported"
                : "rpc_error",
            nativeThreadId,
          ),
        ),
      ),
    );
    const after = yield* context;
    if (after.stopped || after.nativeThreadId !== nativeThreadId) {
      return unknownProviderGoal("context_changed", after.nativeThreadId);
    }
    return result;
  });
