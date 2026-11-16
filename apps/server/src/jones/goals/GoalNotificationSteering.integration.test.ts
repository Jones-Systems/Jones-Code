import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Controls from "../../orchestration-v2/ProviderTurnControlService.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import { workModeFixture } from "../workMode/Fixtures.testkit.ts";
import { isGoalNotificationDeliveryUnknown, isGoalNotificationNotDelivered } from "./GoalNotificationDelivery.ts";

for (const scenario of ["stopped", "missing-target", "unknown-native-outcome"] as const) {
  it.effect(`goal notification steering retains ${scenario} delivery evidence`, () =>
    Effect.gen(function* () {
      const fixture = workModeFixture();
      const providerThread = fixture.providerThreads[0]!;
      const providerTurn = { ...fixture.providerTurns[0]!, status: "running" as const };
      const run = { ...fixture.runs[0]!, status: "running" as const };
      const message = {
        ...fixture.messages[0]!,
        notification: {
          source: { kind: "delegated_task" as const, taskIds: [] },
          outcome: "completed" as const,
          summary: "Child finished",
        },
      };
      let nativeCalls = 0;
      // Only steerTurn is exercised; every other runtime port remains outside this fixture.
      const runtime = {
        steerTurn: () => Effect.suspend(() => {
          nativeCalls += 1;
          return Effect.fail(new Error("native response unavailable"));
        }),
      } as unknown as ProviderAdapterV2SessionRuntime;
      const projections = Layer.mock(Projections.ProjectionStoreV2)({
        getProviderControlContext: () => Effect.succeed({
          providerThread: scenario === "missing-target" ? undefined : providerThread,
          providerTurn, run, message, attempt: undefined,
        }),
        getThreadRecords: () => Effect.succeed({
          thread: fixture.thread,
          turnItems: scenario === "stopped" ? fixture.turnItems.concat({
            ...fixture.turnItems[0]!, type: "run_interrupt_request",
          } as never) : [],
        } as never),
      });
      const sessions = Layer.mock(Sessions.ProviderSessionManagerV2)({
        get: () => Effect.succeed(Option.some(runtime)),
      });
      const sink = Layer.mock(Sink.EventSinkV2)({});
      const result = yield* Effect.gen(function* () {
        const controls = yield* Controls.ProviderTurnControlServiceV2;
        return yield* controls.steer({
          threadId: fixture.thread.id,
          providerSessionId: providerThread.providerSessionId!,
          providerThreadId: providerThread.id,
          providerTurnId: providerTurn.id,
          messageId: message.id,
        }).pipe(Effect.flip);
      }).pipe(Effect.provide(Controls.layer.pipe(Layer.provide([projections, sessions, sink]))));
      expect(nativeCalls).toBe(scenario === "unknown-native-outcome" ? 1 : 0);
      expect(scenario === "unknown-native-outcome"
        ? isGoalNotificationDeliveryUnknown(result.cause)
        : isGoalNotificationNotDelivered(result.cause)).toBe(true);
    }),
  );
}
