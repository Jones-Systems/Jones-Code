import { expect } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Replay from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";

// Feed the actual start-service notice into the persisted terminal-event reactor.
// No effect worker or provider session runs in this isolated in-memory runtime.
export const assertTerminalQueueReaction = (
  error: Extract<OrchestrationV2TurnItem, { type: "error" }>,
) => {
  const instanceId = ProviderInstanceId.make("codex");
  const modelSelection = { instanceId, model: "synthetic" };
  const layer = Replay.layerWithRegistry(
    { name: "pre-session-failure-queue" },
    Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
      getMetadata: () =>
        Effect.succeed({
          driver: ProviderDriverKind.make("codex"),
          continuationKey: "synthetic",
          enabled: true,
          capabilities: CodexProviderCapabilitiesV2,
        }),
      get: () =>
        Effect.succeed({
          instanceId,
          driver: ProviderDriverKind.make("codex"),
          getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
          planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
          openSession: () => Effect.die("Pre-session queue hold must not open a provider"),
        }),
      list: () => Effect.succeed([instanceId]),
    }),
    { runEffectWorker: false },
  );
  return Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const threadId = ThreadId.make("pre-session-queue");
    const now = yield* DateTime.now;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("queue:create"),
      threadId,
      projectId: ProjectId.make("synthetic"),
      title: "Synthetic start failure",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    for (const [id, text] of [
      ["start", "First turn"],
      ["queued-1", "First waiting message"],
      ["queued-2", "Second waiting message"],
    ] as const)
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`queue:${id}`),
        threadId,
        messageId: MessageId.make(`queue:${id}`),
        text,
        attachments: [],
        dispatchMode: { type: id === "start" ? "start_immediately" : "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
    const original = yield* orchestrator.getThreadProjection(threadId);
    const [source, first, second] = original.runs;
    expect(source?.status).toBe("starting");
    expect(first?.status).toBe("queued");
    expect(second?.status).toBe("queued");
    yield* sink.write({
      events: [
        {
          id: EventId.make("queue:existing-hold"),
          type: "run.updated",
          threadId,
          occurredAt: now,
          payload: { ...second!, queueHeld: true },
        },
      ],
    });
    const before = yield* orchestrator.getThreadProjection(threadId);
    const startsBefore = (yield* outbox.listByThreadId(threadId)).filter(
      (row) => row.request.type === "provider-turn.start",
    );
    expect(startsBefore).toHaveLength(1);
    const held = yield* orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (e) =>
          e.type === "run.updated" && e.payload.id === first!.id && e.payload.queueHeld === true,
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* sink.write({
      events: [
        {
          id: EventId.make("queue:failure"),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: {
            ...error,
            id: TurnItemId.make("queue:failure"),
            threadId,
            runId: source!.id,
            nodeId: source!.rootNodeId,
            providerThreadId: source!.providerThreadId,
          },
        },
        {
          id: EventId.make("queue:terminal"),
          type: "run.updated",
          threadId,
          occurredAt: now,
          payload: { ...source!, status: "failed", completedAt: now },
        },
      ],
    });
    yield* Fiber.join(held);
    const after = yield* orchestrator.getThreadProjection(threadId);
    expect(after.messages).toEqual(before.messages);
    expect(after.runs.map((r) => [r.id, r.ordinal, r.status, r.queueHeld === true])).toEqual([
      [source!.id, source!.ordinal, "failed", false],
      [first!.id, first!.ordinal, "queued", true],
      [second!.id, second!.ordinal, "queued", true],
    ]);
    expect(after.turnItems.filter((item) => item.type === "user_message")).toEqual(
      before.turnItems.filter((item) => item.type === "user_message"),
    );
    expect(
      after.turnItems.some((item) => item.runId === first!.id || item.runId === second!.id),
    ).toBe(false);
    expect(
      (yield* outbox.listByThreadId(threadId)).filter(
        (row) => row.request.type === "provider-turn.start",
      ),
    ).toEqual(startsBefore);
  }).pipe(Effect.scoped, Effect.provide(layer));
};
