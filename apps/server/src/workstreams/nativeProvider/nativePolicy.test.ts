import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  EnvironmentAuthenticatedPrincipal,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { dispatchNativeWorkstreamSettlement } from "../../orchestration-v2/Orchestrator.ts";
import { makeWorkstreamsNativeProvider } from "./service.ts";
import {
  makeProviderFixture,
  binding,
  request,
  requestBytesSha256,
  now,
  nativeTestPrincipal,
} from "./testFixtures.ts";

const stores = Layer.mergeAll(
  SqlitePersistenceMemory,
  EventStore.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
  ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);
const testLayer = EventSink.layer.pipe(Layer.provideMerge(stores));

it.effect(
  "unavailable native V2 authority retains pins, snooze and history without producing a receipt or events",
  () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projection = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make(request.identity.native_id);
      const instanceId = ProviderInstanceId.make("synthetic-codex");
      const occurredAt = DateTime.makeUnsafe(now);
      const thread: OrchestrationV2AppThread = {
        createdBy: "user",
        creationSource: "web",
        id: threadId,
        projectId: ProjectId.make("project-synthetic"),
        title: "Synthetic thread",
        providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "synthetic-model" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "synthetic-branch",
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: occurredAt,
        updatedAt: occurredAt,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
        pinnedAt: occurredAt,
        pinOrderKey: "synthetic-order",
        snoozedAt: occurredAt,
        snoozedUntil: DateTime.makeUnsafe("2030-01-01T00:00:00.000Z"),
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("event-synthetic-birth"),
            threadId,
            providerInstanceId: instanceId,
            type: "thread.created",
            occurredAt,
            payload: thread,
          },
          {
            id: EventId.make("event-synthetic-history"),
            threadId,
            providerInstanceId: instanceId,
            type: "message.updated",
            occurredAt,
            payload: {
              id: MessageId.make("message-synthetic-history"),
              threadId,
              runId: null,
              nodeId: null,
              createdBy: "user",
              creationSource: "web",
              role: "user",
              text: "Synthetic retained history",
              attachments: [],
              streaming: false,
              createdAt: occurredAt,
              updatedAt: occurredAt,
            },
          },
        ],
      });
      const before = yield* projection.getThreadProjection(threadId);
      const sequenceBefore = yield* (yield* EventStore.EventStoreV2).latestApplicationSequence;
      const fixture = makeProviderFixture();
      let calls = 0;
      const provider = makeWorkstreamsNativeProvider({
        ...fixture.ports,
        eventSink: sink,
        threadExists: (id) =>
          projection.getThreadShell(id).pipe(Effect.map((value) => value !== null)),
        orchestrator: {
          dispatchNativeWorkstreamSettlement: (input) =>
            Effect.gen(function* () {
              calls += 1;
              assert.notStrictEqual(input.attempt.dispatchStartedAt, null);
              return yield* dispatchNativeWorkstreamSettlement(input);
            }),
        },
      });
      const result = yield* provider.settle(binding, request, requestBytesSha256);
      assert.strictEqual(result.state, "unknown");
      if (result.state === "unknown") assert.strictEqual(result.reason, "authority_unavailable");
      const attempt = fixture.attempts.get(fixture.key(request))!;
      const facts = yield* sink.readNativeCommandFacts({
        threadId,
        commandId: CommandId.make(attempt.nativeCommandId),
      });
      assert.isNull(facts.receipt);
      assert.isNull(facts.identity);
      assert.deepEqual(facts.eventMetadata, []);
      assert.deepEqual(facts.events, []);
      assert.strictEqual(facts.snapshotSequence, sequenceBefore);
      assert.deepEqual(yield* projection.getThreadProjection(threadId), before);
      yield* provider.settle(binding, request, requestBytesSha256);
      yield* provider.lookup(binding, request, requestBytesSha256);
      assert.strictEqual(calls, 1);
    }).pipe(
      Effect.provide(Layer.fresh(testLayer)),
      Effect.provideService(EnvironmentAuthenticatedPrincipal, nativeTestPrincipal),
    ),
);
