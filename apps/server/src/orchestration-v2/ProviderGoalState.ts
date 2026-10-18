import {
  RuntimeIdentityAttestation,
  type ProviderInstanceId,
  type ThreadId,
  type ProviderGoalStateObservation,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { unknownProviderGoal, type ProviderGoalReadResult } from "../provider/providerGoal.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";

const equivalentRuntimeIdentity = Schema.toEquivalence(
  Schema.Union([RuntimeIdentityAttestation, Schema.Undefined]),
);
export const makeProviderGoalState = Effect.fn("makeProviderGoalState")(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sessions = yield* Effect.serviceOption(ProviderSessionManager.ProviderSessionManagerV2);
  return Effect.fn("ProviderGoalState.read")(function* (input: {
    threadId: ThreadId;
    expectedInstanceId: ProviderInstanceId;
  }) {
    const read = Effect.gen(function* () {
      if (Option.isNone(sessions)) return unknownProviderGoal("unsupported");
      const shell = yield* projections.getThreadShell(input.threadId);
      if (shell === null) return unknownProviderGoal("no_session");
      const before = yield* projections.getThreadRecords(input.threadId, ["providerThreads"]);
      if (before.thread.providerInstanceId !== input.expectedInstanceId)
        return unknownProviderGoal("instance_mismatch");
      const provider = before.providerThreads.find(
        (p) => p.id === before.thread.activeProviderThreadId,
      );
      if (provider?.providerSessionId == null) return unknownProviderGoal("no_session");
      if (provider.providerInstanceId !== input.expectedInstanceId)
        return unknownProviderGoal("instance_mismatch");
      if (provider.nativeThreadRef == null || !provider.nativeThreadRef.nativeId?.trim())
        return unknownProviderGoal("native_cursor_missing");
      if (provider.nativeThreadRef.driver !== "codex") return unknownProviderGoal("unsupported");
      const runtime = yield* sessions.value.get(provider.providerSessionId);
      if (Option.isNone(runtime)) return unknownProviderGoal("session_stopped");
      if (runtime.value.instanceId !== input.expectedInstanceId)
        return unknownProviderGoal("instance_mismatch");
      if (runtime.value.readGoalState === undefined) return unknownProviderGoal("unsupported");
      const observation = yield* runtime.value.readGoalState(provider);
      const after = yield* projections.getThreadRecords(input.threadId, ["providerThreads"]);
      const current = after.providerThreads.find(
        (p) => p.id === after.thread.activeProviderThreadId,
      );
      const currentRuntime = yield* sessions.value.get(provider.providerSessionId);
      if (
        after.thread.providerInstanceId !== input.expectedInstanceId ||
        current?.id !== provider.id ||
        current.providerSessionId !== provider.providerSessionId ||
        current.nativeThreadRef?.nativeId !== provider.nativeThreadRef.nativeId ||
        !equivalentRuntimeIdentity(current.runtimeIdentity, provider.runtimeIdentity) ||
        Option.isNone(currentRuntime) ||
        currentRuntime.value !== runtime.value
      )
        return unknownProviderGoal("context_changed");
      if (
        observation.nativeThreadId !== null &&
        observation.nativeThreadId !== provider.nativeThreadRef.nativeId
      )
        return unknownProviderGoal("context_changed");
      if (observation.state !== "unknown" && observation.nativeThreadId === null)
        return unknownProviderGoal("malformed");
      return {
        nativeThreadId: observation.nativeThreadId,
        state: observation.state,
        reasonCode: observation.reasonCode,
      };
    }).pipe(Effect.catchCause(() => Effect.succeed(unknownProviderGoal("rpc_error"))));
    const result: ProviderGoalReadResult = yield* read;
    return {
      schema: "t3.provider-goal-state/v1",
      threadId: input.threadId,
      providerInstanceId: input.expectedInstanceId,
      observedAtMs: DateTime.toEpochMillis(yield* DateTime.now),
      ...result,
    } satisfies ProviderGoalStateObservation;
  });
});
