import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  ThreadId,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import { THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY } from "@t3tools/contracts/jones/guardedThreadSettlement";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../../connection/model.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import type * as RpcSession from "../../rpc/session.ts";
import type { WsRpcProtocolClient } from "../../rpc/protocol.ts";
import { v2Projection } from "../../state/orchestrationV2TestFixtures.ts";
import { monitorReplyDispatchMode } from "./actions.ts";
import { settleThread, startThreadTurn } from "../../operations/commands.ts";

const crypto = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => new Uint8Array(size),
    digest: (_algorithm, bytes) => Effect.succeed(bytes),
  }),
);
const makeSupervisor = Effect.fnUntraced(function* (
  capability: () => string | undefined,
  commands: OrchestrationV2Command[],
  serverResolved = true,
) {
  const client = {
    [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: () => Effect.succeed(v2Projection),
    [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: (command: OrchestrationV2Command) =>
      Effect.sync(() => {
        commands.push(command);
        return { sequence: 1 };
      }),
  } as unknown as WsRpcProtocolClient;
  const session: RpcSession.RpcSession = {
    client,
    initialConfig: Effect.suspend(() =>
      Effect.succeed({
        environment: {
          capabilities: {
            repositoryIdentity: true,
            serverResolvedCommandContext: serverResolved,
            queuedToolBoundaryDelivery: true,
            threadMonitorGuardedSettle: capability(),
          },
        },
      } as never),
    ),
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
  return EnvironmentSupervisor.EnvironmentSupervisor.of({
    target: new PrimaryConnectionTarget({
      environmentId: EnvironmentId.make("guard-target"),
      label: "Test",
      httpBaseUrl: "https://guard.example.test",
      wsBaseUrl: "wss://guard.example.test",
    }),
    state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
    session: yield* SubscriptionRef.make(Option.some(session)),
    prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.void,
  });
});
const input = {
  threadId: ThreadId.make("t"),
  commandId: CommandId.make("stable-settle"),
  guard: {
    policy: "preserve_pending_work" as const,
    expectedUpdatedAt: "2026-10-26T07:00:00.000Z",
  },
};

describe("monitor settle uses the existing environment transport", () => {
  it.effect(
    "serializes the guard and caller command identity, then rejects a capability downgrade without fallback",
    () =>
      Effect.gen(function* () {
        const commands: OrchestrationV2Command[] = [];
        let capability: string | undefined = THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY;
        const supervisor = yield* makeSupervisor(() => capability, commands);
        const send = settleThread(input).pipe(
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        );
        yield* send;
        expect(commands).toEqual([{ type: "thread.settle", ...input }]);
        capability = undefined;
        expect((yield* send.pipe(Effect.result))._tag).toBe("Failure");
        expect(commands).toHaveLength(1);
      }).pipe(Effect.provide(crypto)),
  );
  it.effect.each([true, false])(
    "never emits auto/steer/restart for monitor replies (server resolves context: %s)",
    (serverResolved) =>
      Effect.gen(function* () {
        const commands: OrchestrationV2Command[] = [];
        const supervisor = yield* makeSupervisor(() => undefined, commands, serverResolved);
        for (const workOrRequestActive of [false, true]) {
          yield* startThreadTurn({
            threadId: v2Projection.thread.id,
            commandId: CommandId.make(`reply-${workOrRequestActive}`),
            message: {
              messageId: MessageId.make(`reply-${workOrRequestActive}`),
              role: "user",
              text: "A real monitor reply",
              attachments: [],
            },
            modelSelection: v2Projection.thread.modelSelection,
            runtimeMode: v2Projection.thread.runtimeMode,
            interactionMode: v2Projection.thread.interactionMode,
            dispatchMode: monitorReplyDispatchMode(workOrRequestActive),
            allowQueuedToolBoundaryDelivery: false,
          }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));
        }
        expect(commands).toHaveLength(2);
        for (const command of commands) {
          expect(command.type).toBe("message.dispatch");
          if (command.type !== "message.dispatch") throw new Error("Expected a reply command");
          expect(command.dispatchMode.type).toBe(
            serverResolved ? "queue_after_active" : "start_immediately",
          );
          expect(command.deliveryIntent).toBeUndefined();
          expect(command.queuedToolBoundaryEligible).toBeUndefined();
          expect(command.creationSource).toBe("web");
          expect(command.sourcePlanRef).toBeUndefined();
        }
      }).pipe(Effect.provide(crypto)),
  );
  it.effect("preserves legacy unguarded settlement without requiring a new capability", () =>
    Effect.gen(function* () {
      const commands: OrchestrationV2Command[] = [];
      const supervisor = yield* makeSupervisor(() => undefined, commands);
      yield* settleThread({ threadId: input.threadId, commandId: input.commandId }).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
      );
      expect(commands).toEqual([
        { type: "thread.settle", threadId: input.threadId, commandId: input.commandId },
      ]);
    }).pipe(Effect.provide(crypto)),
  );
});
