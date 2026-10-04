import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { unknownProviderGoal, type ProviderGoalReadResult } from "../provider/providerGoal.ts";
import type { ProviderRuntimeBinding } from "./ProviderAdapter.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";

export class ProviderSessionGoalService extends Context.Service<ProviderSessionGoalService, {
  readonly get: (input: {
    readonly threadId: ThreadId;
    readonly expectedInstanceId: ProviderInstanceId;
  }) => Effect.Effect<ProviderGoalReadResult>;
}>()("t3/orchestration-v2/ProviderSessionGoalService") {}

const make = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  return ProviderSessionGoalService.of({
    get: Effect.fn("ProviderSessionGoalService.get")(function* (input) {
      const before = yield* projections.getThreadProviderContext(input.threadId).pipe(Effect.option);
      if (Option.isNone(before)) return unknownProviderGoal("no_session");
      const context = before.value;
      if (context.thread.modelSelection.instanceId !== input.expectedInstanceId)
        return unknownProviderGoal("instance_mismatch");
      const providerThread = context.providerThreads.find((thread) => thread.id === context.thread.activeProviderThreadId);
      if (providerThread === undefined) return unknownProviderGoal("no_session");
      const sessionId = providerThread?.providerSessionId;
      if (sessionId == null) return unknownProviderGoal("no_session");
      const persistedSession = context.providerSessions.find((session) => session.id === sessionId);
      if (persistedSession === undefined) return unknownProviderGoal("no_session");
      if (persistedSession.status === "stopped" || persistedSession.status === "error")
        return unknownProviderGoal("session_stopped");
      const resident = yield* sessions.get(sessionId).pipe(Effect.orElseSucceed(() => Option.none()));
      if (Option.isNone(resident)) return unknownProviderGoal("no_session");
      const runtime = resident.value;
      const nativeThreadId = providerThread.nativeThreadRef?.nativeId;
      if (runtime.instanceId !== input.expectedInstanceId || persistedSession.providerInstanceId !== input.expectedInstanceId ||
          providerThread.providerInstanceId !== input.expectedInstanceId || runtime.providerSessionId !== sessionId ||
          providerThread.appThreadId !== input.threadId || runtime.driver !== providerThread.driver || runtime.driver !== persistedSession.driver)
        return unknownProviderGoal("instance_mismatch");
      if (runtime.providerSession.status === "stopped" || runtime.providerSession.status === "error")
        return unknownProviderGoal("session_stopped");
      if (nativeThreadId == null || nativeThreadId.length === 0)
        return unknownProviderGoal("native_cursor_missing");
      if (runtime.driver !== "codex" || runtime.getGoal === undefined)
        return unknownProviderGoal("unsupported", nativeThreadId);
      const generation = runtime.runtimeGeneration;
      if (generation === undefined) return unknownProviderGoal("context_changed", nativeThreadId);
      const binding: ProviderRuntimeBinding = {
        threadId: input.threadId,
        providerThreadId: providerThread.id,
        providerSessionId: sessionId,
        instanceId: input.expectedInstanceId,
        runtimeGeneration: generation,
        nativeThreadId,
      };
      const result = yield* runtime.getGoal(binding).pipe(
        Effect.orElseSucceed(() => unknownProviderGoal("rpc_error", nativeThreadId)),
        Effect.timeoutOption("3 seconds"),
        Effect.map(Option.getOrElse(() => unknownProviderGoal("timeout", nativeThreadId))),
      );
      const after = yield* projections.getThreadProviderContext(input.threadId).pipe(Effect.option);
      const current = yield* sessions.get(sessionId).pipe(Effect.orElseSucceed(() => Option.none()));
      const afterThread = Option.isSome(after) ? after.value.providerThreads.find((thread) => thread.id === after.value.thread.activeProviderThreadId) : undefined;
      const afterSession = Option.isSome(after) ? after.value.providerSessions.find((session) => session.id === sessionId) : undefined;
      if (Option.isNone(current) || current.value !== runtime || runtime.runtimeGeneration !== generation ||
          runtime.instanceId !== input.expectedInstanceId || runtime.providerSession.status === "stopped" || runtime.providerSession.status === "error" ||
          Option.isNone(after) || after.value.thread.modelSelection.instanceId !== input.expectedInstanceId ||
          afterThread?.id !== providerThread.id || afterThread.providerSessionId !== sessionId ||
          afterThread.providerInstanceId !== input.expectedInstanceId || afterThread.nativeThreadRef?.nativeId !== nativeThreadId ||
          afterSession === undefined || afterSession.providerInstanceId !== input.expectedInstanceId || afterSession.driver !== runtime.driver ||
          afterSession.status === "stopped" || afterSession.status === "error" ||
          result.nativeThreadId !== nativeThreadId)
        return unknownProviderGoal("context_changed", nativeThreadId);
      return result;
    }),
  });
});

export const layer = Layer.effect(ProviderSessionGoalService, make);
