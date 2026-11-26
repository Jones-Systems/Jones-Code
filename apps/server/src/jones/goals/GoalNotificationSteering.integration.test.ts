import { expect, it } from "@effect/vitest";
import { NodeId, TurnItemId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  ProviderAdapterSteerRunError,
  type ProviderAdapterV2SessionRuntime,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as Controls from "../../orchestration-v2/ProviderTurnControlService.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Sessions from "../../orchestration-v2/ProviderSessionManager.ts";
import * as Sink from "../../orchestration-v2/EventSink.ts";
import * as McpAppModelContext from "../../mcpApps/McpAppModelContext.ts";
import * as SupersededTerminalTrace from "../orchestration/SupersededTerminalTrace.ts";
import { workModeFixture } from "../workMode/Fixtures.testkit.ts";
import {
  isGoalNotificationDeliveryUnknown,
  isGoalNotificationNotDelivered,
} from "./GoalNotificationDelivery.ts";

it.effect.each(["stopped", "missing-target", "unknown-native-outcome"] as const)(
  "goal notification steering retains %s delivery evidence",
  (scenario) =>
    Effect.gen(function* () {
      const fixture = workModeFixture();
      const providerThread = {
        ...fixture.providerThreads[0]!,
        goal: { objective: "Finish the delegated work", status: "active" as const },
      };
      const providerTurn = { ...fixture.providerTurns[0]!, status: "running" as const };
      const run = { ...fixture.runs[0]!, status: "running" as const };
      const message = {
        ...fixture.messages[0]!,
        createdBy: "agent" as const,
        creationSource: "server" as const,
        notification: {
          source: { kind: "delegated_task" as const, taskIds: [NodeId.make("child-task")] },
          outcome: "completed" as const,
          summary: "Child finished",
        },
      };
      let nativeCalls = 0;
      // Only steerTurn is exercised; every other runtime port remains outside this fixture.
      const runtime = {
        steerTurn: () =>
          Effect.suspend(() => {
            nativeCalls += 1;
            return Effect.fail(
              new ProviderAdapterSteerRunError({
                driver: providerThread.driver,
                providerThreadId: providerThread.id,
                providerTurnId: providerTurn.id,
                cause: "native response unavailable",
              }),
            );
          }),
      } as unknown as ProviderAdapterV2SessionRuntime;
      const projections = Layer.mock(Projections.ProjectionStoreV2)({
        getProviderControlContext: () =>
          Effect.succeed({
            providerThread: scenario === "missing-target" ? undefined : providerThread,
            providerTurn,
            run,
            message,
            attempt: undefined,
          }),
        getThreadRecords: () =>
          Effect.succeed({
            ...fixture,
            messages: [message],
            turnItems:
              scenario === "stopped"
                ? [
                    {
                      id: TurnItemId.make("stop-before-notification"),
                      threadId: fixture.thread.id,
                      runId: run.id,
                      nodeId: run.rootNodeId,
                      providerThreadId: providerThread.id,
                      providerTurnId: null,
                      nativeItemRef: null,
                      parentItemId: null,
                      ordinal: 1,
                      status: "completed",
                      title: "Interrupt requested",
                      startedAt: fixture.thread.createdAt,
                      completedAt: fixture.thread.createdAt,
                      updatedAt: fixture.thread.createdAt,
                      type: "run_interrupt_request",
                      message: "User requested Stop",
                    },
                  ]
                : [],
          }),
      });
      const sessions = Layer.mock(Sessions.ProviderSessionManagerV2)({
        get: () => Effect.succeedSome(runtime),
      });
      const sink = Layer.mock(Sink.EventSinkV2)({
        supersededTerminalTrace: SupersededTerminalTrace.make(),
      });
      const result = yield* Effect.gen(function* () {
        const controls = yield* Controls.ProviderTurnControlServiceV2;
        return yield* controls
          .steer({
            threadId: fixture.thread.id,
            providerSessionId: providerThread.providerSessionId!,
            providerThreadId: providerThread.id,
            providerTurnId: providerTurn.id,
            messageId: message.id,
          })
          .pipe(Effect.flip);
      }).pipe(
        Effect.provide(
          Controls.layer.pipe(
            Layer.provide([projections, sessions, sink, McpAppModelContext.layerEmpty]),
          ),
        ),
      );
      expect(nativeCalls).toBe(scenario === "unknown-native-outcome" ? 1 : 0);
      expect(
        scenario === "unknown-native-outcome"
          ? isGoalNotificationDeliveryUnknown(result.cause)
          : isGoalNotificationNotDelivered(result.cause),
      ).toBe(true);
    }),
);
