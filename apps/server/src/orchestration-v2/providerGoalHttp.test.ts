import { describe, expect, it } from "vite-plus/test";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { type ProviderGoalReadResult } from "../provider/providerGoal.ts";
import { ProviderSessionGoalService } from "./ProviderSessionGoalService.ts";
import { readProviderGoalState } from "./providerGoalHttp.ts";

const input = {
  threadId: ThreadId.make("goal-http-thread"),
  expectedInstanceId: ProviderInstanceId.make("codex-owner"),
};

describe("provider goal HTTP metadata adapter", () => {
  for (const result of [
    { nativeThreadId: "native-thread", state: "inactive", reasonCode: "goal_null" },
    { nativeThreadId: "native-thread", state: "active", reasonCode: "goal_present" },
    { nativeThreadId: "native-thread", state: "unknown", reasonCode: "unsupported" },
    { nativeThreadId: null, state: "unknown", reasonCode: "no_session" },
    { nativeThreadId: null, state: "unknown", reasonCode: "session_stopped" },
    { nativeThreadId: null, state: "unknown", reasonCode: "instance_mismatch" },
    { nativeThreadId: null, state: "unknown", reasonCode: "native_cursor_missing" },
    { nativeThreadId: "native-thread", state: "unknown", reasonCode: "timeout" },
    { nativeThreadId: "native-thread", state: "unknown", reasonCode: "malformed" },
    { nativeThreadId: "native-thread", state: "unknown", reasonCode: "rpc_error" },
    { nativeThreadId: "native-thread", state: "unknown", reasonCode: "context_changed" },
    { nativeThreadId: "native-thread", state: "unknown", reasonCode: "goal_field_omitted" },
  ] satisfies ReadonlyArray<ProviderGoalReadResult>) {
    it(`preserves ${result.reasonCode} and exposes only metadata`, async () => {
      const requests: (typeof input)[] = [];
      const observation = await Effect.runPromise(
        readProviderGoalState(input).pipe(
          Effect.provideService(ProviderSessionGoalService, {
            get: (request) =>
              Effect.sync(() => {
                requests.push(request);
                return { ...result, objective: "private objective", tokenBudget: 1000 };
              }),
          }),
        ),
      );
      expect(requests).toEqual([input]);
      expect(observation).toEqual({
        schema: "t3.provider-goal-state/v1",
        threadId: input.threadId,
        providerInstanceId: input.expectedInstanceId,
        ...result,
        observedAtMs: expect.any(Number),
      });
      expect(observation.observedAtMs).toBeGreaterThanOrEqual(0);
    });
  }

  it("keeps unavailable capability unknown without recovery", async () => {
    expect(await Effect.runPromise(readProviderGoalState(input))).toMatchObject({
      threadId: input.threadId,
      providerInstanceId: input.expectedInstanceId,
      nativeThreadId: null,
      state: "unknown",
      reasonCode: "unsupported",
    });
  });

  it("maps a failed resident read to the existing unknown reason", async () => {
    const result = await Effect.runPromise(
      readProviderGoalState(input).pipe(
        Effect.provideService(ProviderSessionGoalService, {
          get: () => Effect.die(new Error("Synthetic provider failure")),
        }),
      ),
    );
    expect(result).toMatchObject({
      nativeThreadId: null,
      state: "unknown",
      reasonCode: "rpc_error",
    });
  });
});
