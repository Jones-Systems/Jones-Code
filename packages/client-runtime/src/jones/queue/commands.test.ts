import { beforeEach, vi } from "vite-plus/test";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  QUEUED_STEER_BATCH_LIMITS,
  RunId,
  ThreadId,
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  EnvironmentId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { EnvironmentSupervisor } from "../../connection/supervisor.ts";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../../connection/model.ts";
import type { RpcSession } from "../../rpc/session.ts";

const mocks = vi.hoisted(() => ({ config: vi.fn(), request: vi.fn() }));
vi.mock(
  import("../../rpc/client.ts"),
  () =>
    ({
      getInitialServerConfig: mocks.config,
      request: mocks.request,
    }) as never,
);
import { promoteQueuedBatch } from "./commands.ts";

const supervisor = Layer.effect(
  EnvironmentSupervisor,
  Effect.gen(function* () {
    return EnvironmentSupervisor.of({
      target: new PrimaryConnectionTarget({
        environmentId: EnvironmentId.make("batch-test"),
        label: "Synthetic batch environment",
        httpBaseUrl: "https://batch.example.test",
        wsBaseUrl: "wss://batch.example.test/ws",
      }),
      state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
      session: yield* SubscriptionRef.make(Option.none<RpcSession>()),
      prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
      connect: Effect.void,
      disconnect: Effect.void,
      retryNow: Effect.void,
    });
  }),
);

const input = {
  commandId: CommandId.make("batch-command"),
  threadId: ThreadId.make("thread"),
  targetRunId: RunId.make("active"),
  items: Array.from({ length: 22 }, (_, i) => ({
    queuedRunId: RunId.make(`queued-${i}`),
    messageId: MessageId.make(`message-${i}`),
    messageUpdatedAt: "2026-10-20T12:00:00.000Z",
  })),
};

beforeEach(() => {
  mocks.request.mockReset().mockReturnValue(Effect.succeed({ sequence: 1 }));
  mocks.config.mockReset().mockReturnValue(
    Effect.succeed({
      environment: { capabilities: { queuedSteerBatch: QUEUED_STEER_BATCH_LIMITS } },
    }),
  );
});

describe("batch command", () => {
  it.effect("dispatches one batch RPC with all message versions and the caller identity", () =>
    Effect.gen(function* () {
      yield* promoteQueuedBatch(input);
      expect(mocks.request).toHaveBeenCalledExactlyOnceWith(
        ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
        { ...input, type: "queued-message.promote-batch-to-steer" },
      );
    }).pipe(Effect.provide(supervisor)),
  );

  it.effect("refuses a server without capability and never falls back to per-row calls", () =>
    Effect.gen(function* () {
      mocks.config.mockReturnValue(Effect.succeed({ environment: { capabilities: {} } }));
      expect(yield* Effect.exit(promoteQueuedBatch(input))).toMatchObject({ _tag: "Failure" });
      expect(mocks.request).not.toHaveBeenCalled();
    }).pipe(Effect.provide(supervisor)),
  );
});
