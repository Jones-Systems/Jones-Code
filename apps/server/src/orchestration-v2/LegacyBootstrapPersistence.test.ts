import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  OrchestrationV2ThreadStreamItem,
  type OrchestrationV2PrivateEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Schema from "effect/Schema";
import * as ThreadManagement from "./ThreadManagementService.ts";
import { OrchestratorProjectionError } from "./Orchestrator.ts";
import { subscribeOrchestrationV2Thread } from "../ws.ts";
import { projectThreadProjectionForWire } from "./WireProjection.ts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ApplicationEvents from "../persistence/OrchestrationEventStore.ts";

import * as Fiber from "effect/Fiber";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import { RecordedAppThreadJson, RecordedRunJson } from "./RecordedTypes.ts";
import {
  canonicalLegacyPayload,
  legacyBootstrapCreateCommandId,
  legacyPayloadHash,
} from "./LegacyBootstrap.ts";

const decodeRecordedThreadJson = Schema.decodeUnknownSync(RecordedAppThreadJson);
const decodeRecordedRunJson = Schema.decodeUnknownSync(RecordedRunJson);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

it.effect(
  "private preflight intent and outcome survive SQL reopen without shell birth or original C acceptance",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "legacy-preflight-" });
      const database = SqlitePersistence.layerFromPath(`${directory}/preflight.sqlite`);
      const stores = Layer.mergeAll(
        EventStore.layer,
        ProjectionStore.layer,
        CommandReceiptStore.layer,
      ).pipe(Layer.provideMerge(database));
      const layer = Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores)));
      const threadId = ThreadId.make("preflight:thread");
      const releaseCommandId = CommandId.make("preflight:original-C");
      const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
      const projectId = ProjectId.make("preflight:project");
      const messageId = MessageId.make("preflight:message");
      const modelSelection = {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.1-codex",
      };
      const payload = {
        type: "thread.turn.start",
        commandId: releaseCommandId,
        threadId,
        message: { messageId, role: "user", text: "Prompt", attachments: [] },
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        bootstrap: {
          createThread: {
            projectId,
            title: "Preflight",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdAt: "2026-10-05T00:00:00.000Z",
          },
          prepareWorktree: {
            projectCwd: "/repo",
            baseBranch: "main",
            startFromOrigin: true,
            requireWorktree: true,
          },
          runSetupScript: false,
        },
        createdAt: "2026-10-05T00:00:00.000Z",
      };
      const canonicalPayload = canonicalLegacyPayload(payload);
      const binding = {
        policy: {
          version: 1 as const,
          createCommandId,
          birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
          releaseCommandId,
          projectId,
          threadId,
          messageId,
          payloadHash: legacyPayloadHash(canonicalPayload),
          ownsNewThread: true,
        },
        canonicalPayload,
        fetch: {
          cwd: "/repo",
          baseRef: "main",
          startFromOrigin: true,
          requireWorktree: true,
          remote: "origin",
        },
      };
      const now = yield* DateTime.now;
      const intentId = CommandId.make(`${createCommandId}:preflight-intent`);
      const intent: OrchestrationV2PrivateEvent = {
        id: EventId.make("preflight:intent"),
        threadId,
        occurredAt: now,
        type: "legacy-bootstrap.preflight-intent",
        payload: binding,
      };
      const first = yield* Effect.scoped(
        Effect.gen(function* () {
          const sink = yield* EventSink.EventSinkV2;
          const results = yield* Effect.all(
            [
              sink.commitLegacyPreflight({ commandId: intentId, event: intent }),
              sink.commitLegacyPreflight({ commandId: intentId, event: intent }),
            ],
            { concurrency: 2 },
          );
          assert.equal(results.filter((r) => r.committed).length, 1);
          assert.equal(results[0]!.receipt.resultSequence, results[1]!.receipt.resultSequence);
          return results[0]!.receipt.resultSequence;
        }).pipe(Effect.provide(Layer.fresh(layer))),
      );
      const outcomeId = CommandId.make(`${createCommandId}:preflight-outcome`);
      const outcome: OrchestrationV2PrivateEvent = {
        id: EventId.make("preflight:outcome"),
        threadId,
        occurredAt: now,
        type: "legacy-bootstrap.preflight-outcome",
        payload: {
          binding,
          intentCommandId: intentId,
          intentSequence: first,
          status: "known_failed",
          detail: "Required base ref is unavailable.",
        },
      };
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sink = yield* EventSink.EventSinkV2;
          const store = yield* EventStore.EventStoreV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
          assert.isFalse(
            (yield* sink.commitLegacyPreflight({ commandId: intentId, event: intent })).committed,
          );
          assert.isEmpty(
            Array.from(
              yield* store.readByCommandId({ commandId: outcomeId }).pipe(Stream.runCollect),
            ),
          );
          assert.isNull(yield* projections.getThreadShell(threadId));
          assert.isTrue(Option.isNone(yield* receipts.getByCommandId(releaseCommandId)));
          const changed = {
            ...intent,
            payload: { ...binding, fetch: { ...binding.fetch, remote: "another-remote" } },
          };
          const collision = yield* sink
            .commitLegacyPreflight({ commandId: intentId, event: changed })
            .pipe(Effect.flip);
          assert.equal(collision._tag, "EventSinkWriteError");
          yield* sink.commitLegacyPreflight({ commandId: outcomeId, event: outcome });
        }).pipe(Effect.provide(Layer.fresh(layer))),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* EventStore.EventStoreV2;
          const sink = yield* EventSink.EventSinkV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
          const events = Array.from(yield* store.read({ threadId }).pipe(Stream.runCollect));
          assert.deepEqual(
            events.map(({ event }) => event.type),
            ["legacy-bootstrap.preflight-intent", "legacy-bootstrap.preflight-outcome"],
          );
          assert.isFalse(
            (yield* sink.commitLegacyPreflight({ commandId: outcomeId, event: outcome })).committed,
          );
          assert.isNull(yield* projections.getThreadShell(threadId));
          assert.isTrue(Option.isNone(yield* receipts.getByCommandId(createCommandId)));
          assert.isTrue(Option.isNone(yield* receipts.getByCommandId(releaseCommandId)));
        }).pipe(Effect.provide(Layer.fresh(layer))),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "private preflight leaves existing and deleted projections unchanged and preserves mixed live and replay cursors",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "legacy-preflight-wire-" });
      const database = SqlitePersistence.layerFromPath(`${directory}/wire.sqlite`);
      const stores = Layer.mergeAll(
        EventStore.layer,
        ProjectionStore.layer,
        CommandReceiptStore.layer,
        ApplicationEvents.layer,
      ).pipe(Layer.provideMerge(database));
      const layer = Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores)));
      yield* Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const app = yield* ApplicationEvents.OrchestrationEventStore;
        const now = yield* DateTime.now;
        const later = DateTime.add(now, { seconds: 30 });
        const threadId = ThreadId.make("wire:preflight:thread");
        const projectId = ProjectId.make("wire:preflight:project");
        const modelSelection = {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.1-codex",
        };
        const thread = {
          createdBy: "user" as const,
          creationSource: "web" as const,
          id: threadId,
          projectId,
          title: "Before preflight",
          providerInstanceId: modelSelection.instanceId,
          modelSelection,
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        };
        const created = yield* sink.write({
          events: [
            {
              id: EventId.make("wire:created"),
              type: "thread.created",
              threadId,
              occurredAt: now,
              payload: thread,
            },
          ],
        });
        const cursor = created[0]!.sequence;
        const before = yield* projections.getThread(threadId);
        const tail = yield* sink
          .stream({ threadId, afterSequence: cursor })
          .pipe(Stream.take(1), Stream.runCollect, Effect.forkChild);
        const releaseCommandId = CommandId.make("wire:original-C");
        const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
        const messageId = MessageId.make("wire:message");
        const payload = {
          type: "thread.turn.start",
          commandId: releaseCommandId,
          threadId,
          message: { messageId, role: "user", text: "Prompt", attachments: [] },
          runtimeMode: "full-access",
          interactionMode: "default",
          bootstrap: {
            prepareWorktree: {
              projectCwd: "/repo",
              baseBranch: "main",
              startFromOrigin: false,
              requireWorktree: false,
            },
          },
          createdAt: "2026-10-05T00:00:00.000Z",
        };
        const canonicalPayload = canonicalLegacyPayload(payload);
        const binding = {
          policy: {
            version: 1 as const,
            createCommandId,
            birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
            releaseCommandId,
            projectId,
            threadId,
            messageId,
            payloadHash: legacyPayloadHash(canonicalPayload),
            ownsNewThread: false,
          },
          canonicalPayload,
          fetch: {
            cwd: "/repo",
            baseRef: "main",
            startFromOrigin: false,
            requireWorktree: false,
            remote: null,
          },
        };
        const intentId = CommandId.make(`${createCommandId}:preflight-intent`);
        const intent: OrchestrationV2PrivateEvent = {
          id: EventId.make("wire:intent"),
          type: "legacy-bootstrap.preflight-intent",
          threadId,
          occurredAt: later,
          payload: binding,
        };
        const intentReceipt = yield* sink.commitLegacyPreflight({
          commandId: intentId,
          event: intent,
        });
        assert.deepEqual(yield* projections.getThread(threadId), before);
        const outcome: OrchestrationV2PrivateEvent = {
          id: EventId.make("wire:outcome"),
          type: "legacy-bootstrap.preflight-outcome",
          threadId,
          occurredAt: later,
          payload: {
            binding,
            intentCommandId: intentId,
            intentSequence: intentReceipt.receipt.resultSequence,
            status: "ready",
            workspaceStrategy: { type: "root" },
          },
        };
        yield* sink.commitLegacyPreflight({
          commandId: CommandId.make(`${createCommandId}:preflight-outcome`),
          event: outcome,
        });
        assert.deepEqual(yield* projections.getThread(threadId), before);
        const receivingThreads = Layer.mock(ThreadManagement.ThreadManagementService)({
          ensureLegacyTranscript: () => Effect.void,
          streamStoredEventsFrom: (input) =>
            sink
              .stream(input)
              .pipe(
                Stream.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
              ),
          getThreadSnapshot: () =>
            Effect.die("Private preflight must not require a synthetic snapshot"),
          getThreadSnapshotWindow: () =>
            Effect.die("Private preflight must not require a synthetic snapshot"),
        });
        const synchronized = yield* Deferred.make<void>();
        const wireLive = yield* Stream.unwrap(
          subscribeOrchestrationV2Thread({
            threadId,
            afterSequence: cursor,
            requestCompletionMarker: true,
          }),
        ).pipe(
          Stream.tap((item) =>
            item.kind === "synchronized" ? Deferred.succeed(synchronized, undefined) : Effect.void,
          ),
          Stream.take(2),
          Stream.runCollect,
          Effect.provide(receivingThreads),
          Effect.forkChild,
        );
        yield* Deferred.await(synchronized);
        const visible = yield* sink.write({
          events: [
            {
              id: EventId.make("wire:visible"),
              type: "thread.metadata-updated",
              threadId,
              occurredAt: later,
              payload: {
                ...thread,
                title: "Visible",
                updatedAt: later,
                legacyBootstrapClaim: binding.policy,
              },
            },
          ],
        });
        const served = Array.from(yield* Fiber.join(wireLive));
        assert.deepEqual(
          served.map((item) => item.kind),
          ["synchronized", "event"],
        );
        assert.equal(
          served[1]!.kind === "event" ? served[1]!.sequence : null,
          visible[0]!.sequence,
        );
        const publicFrames = yield* Effect.forEach(served, (item) =>
          Schema.encodeEffect(OrchestrationV2ThreadStreamItem)(item),
        );
        const serializedFrames = yield* encodeJson(publicFrames);
        assert.notInclude(serializedFrames, "legacy-bootstrap.preflight");
        assert.notInclude(serializedFrames, "legacyBootstrapClaim");
        const persisted = yield* projections.getThreadProjection(threadId);
        assert.deepEqual(persisted.thread.legacyBootstrapClaim, binding.policy);
        assert.notProperty(
          projectThreadProjectionForWire(persisted).thread,
          "legacyBootstrapClaim",
        );
        assert.deepEqual(
          (yield* projections.getThreadProjection(threadId)).thread.legacyBootstrapClaim,
          binding.policy,
        );
        const privateCursor = (yield* sink.commitLegacyPreflight({
          commandId: CommandId.make(`${createCommandId}:preflight-outcome`),
          event: outcome,
        })).receipt.resultSequence;
        const wireReplay = Array.from(
          yield* Stream.unwrap(
            subscribeOrchestrationV2Thread({
              threadId,
              afterSequence: privateCursor,
              requestCompletionMarker: true,
            }),
          ).pipe(Stream.take(2), Stream.runCollect, Effect.provide(receivingThreads)),
        );
        assert.deepEqual(
          wireReplay.map((item) => item.kind),
          ["event", "synchronized"],
        );
        assert.equal(
          wireReplay[0]!.kind === "event" ? wireReplay[0]!.sequence : null,
          visible[0]!.sequence,
        );
        const live = Array.from(yield* Fiber.join(tail));
        assert.deepEqual(
          live.map((stored) => stored.sequence),
          [visible[0]!.sequence],
        );
        const throughSequence = yield* app.latestApplicationSequence;
        const replay = Array.from(
          yield* app
            .readApplicationEvents({ afterSequence: cursor, throughSequence })
            .pipe(Stream.runCollect),
        );
        assert.deepEqual(
          replay.map((stored) => stored.sequence),
          [visible[0]!.sequence],
        );
        const directReplay = Array.from(
          yield* app
            .readAgentEvents({
              threadId,
              afterSequence: cursor,
              throughSequence,
              publicOnly: true,
              limit: 1,
            })
            .pipe(Stream.runCollect),
        );
        assert.deepEqual(
          directReplay.map((stored) => stored.sequence),
          [visible[0]!.sequence],
        );
        assert.equal(
          (yield* app.getAgentReplayStats({
            threadId,
            afterSequence: cursor,
            throughSequence,
            maxEvents: 1,
          })).eventCount,
          1,
        );
        const memory = yield* Effect.gen(function* () {
          return yield* ProjectionStore.ProjectionStoreV2;
        }).pipe(Effect.provide(Layer.fresh(ProjectionStore.layerMemory)));
        yield* memory.apply(created[0]!.event);
        yield* memory.apply(intent);
        yield* memory.apply(outcome);
        assert.deepEqual(yield* memory.getThread(threadId), thread);
        const deletedThread = { ...thread, deletedAt: later, updatedAt: later };
        yield* projections.apply({
          id: EventId.make("wire:deleted"),
          type: "thread.deleted",
          threadId,
          occurredAt: later,
          payload: deletedThread,
        });
        yield* projections.apply(intent);
        yield* projections.apply(outcome);
        assert.deepEqual(yield* projections.getThread(threadId), deletedThread);
      }).pipe(Effect.provide(layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("recorded thread and run values survive SQL reopen and native projection rebuild", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "recorded-bootstrap-codecs-" });
    const database = SqlitePersistence.layerFromPath(`${directory}/recorded.sqlite`);
    const stores = Layer.mergeAll(
      EventStore.layer,
      ProjectionStore.layer,
      CommandReceiptStore.layer,
    ).pipe(Layer.provideMerge(database));
    const layer = Layer.mergeAll(
      stores,
      EventSink.layer.pipe(Layer.provide(stores)),
      ProjectionMaintenance.layer.pipe(Layer.provide(stores)),
    );
    const threadId = ThreadId.make("recorded-codec:T");
    const runId = RunId.make("recorded-codec:R");
    const projectId = ProjectId.make("recorded-codec:P");
    const messageId = MessageId.make("recorded-codec:M");
    const releaseCommandId = CommandId.make("recorded-codec:C");
    const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
    const policy = {
      version: 1 as const,
      createCommandId,
      birthCommandId: CommandId.make(`${createCommandId}:initial-message`),
      releaseCommandId,
      projectId,
      threadId,
      messageId,
      payloadHash: "recorded-codec-payload",
      ownsNewThread: false,
      runId,
    };
    const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.1-codex" };
    const timestamp = "2026-10-05T00:00:00.000Z";
    const thread = decodeRecordedThreadJson({
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId,
      title: "Recorded codec fixture",
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: timestamp,
      updatedAt: timestamp,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
      legacyBootstrapClaim: policy,
    });
    const run = decodeRecordedRunJson({
      id: runId,
      threadId,
      ordinal: 1,
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      providerThreadId: null,
      userMessageId: messageId,
      rootNodeId: null,
      activeAttemptId: null,
      status: "preparing",
      requestedAt: timestamp,
      startedAt: null,
      completedAt: null,
      checkpointId: null,
      contextHandoffId: null,
      workspaceRunSetupScript: false,
      legacyPreparationFailureKnown: false,
      legacyBootstrap: policy,
    });
    yield* Effect.scoped(
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.write({
          events: [
            {
              id: EventId.make("recorded-codec:thread"),
              threadId,
              type: "thread.created",
              occurredAt: thread.createdAt,
              payload: thread,
            },
            {
              id: EventId.make("recorded-codec:run"),
              threadId,
              runId,
              type: "run.created",
              occurredAt: run.requestedAt,
              payload: run,
            },
          ],
        });
        const {
          legacyBootstrap: _policy,
          workspaceRunSetupScript: _setup,
          legacyPreparationFailureKnown: _known,
          ...staleRun
        } = run;
        yield* sink.write({
          events: [
            {
              id: EventId.make("recorded-codec:stale"),
              threadId,
              runId,
              type: "run.updated",
              occurredAt: run.requestedAt,
              payload: staleRun,
            },
          ],
        });
      }).pipe(Effect.provide(Layer.fresh(layer))),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const store = yield* EventStore.EventStoreV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
        const events = Array.from(yield* store.read({ threadId }).pipe(Stream.runCollect));
        const storedRun = events.find(({ event }) => event.type === "run.created")?.event;
        assert.equal(storedRun?.type, "run.created");
        if (storedRun?.type !== "run.created") return yield* Effect.die("Missing recorded run");
        assert.deepStrictEqual(storedRun.payload.legacyBootstrap, policy);
        assert.strictEqual(storedRun.payload.workspaceRunSetupScript, false);
        assert.strictEqual(storedRun.payload.legacyPreparationFailureKnown, false);
        assert.isTrue((yield* maintenance.rebuild).valid);
        const projection = yield* projections.getThreadProjection(threadId);
        assert.deepStrictEqual(projection.thread.legacyBootstrapClaim, policy);
        assert.deepStrictEqual(projection.runs[0]?.legacyBootstrap, policy);
        assert.strictEqual(projection.runs[0]?.workspaceRunSetupScript, false);
        assert.strictEqual(projection.runs[0]?.legacyPreparationFailureKnown, false);
        const publicProjection = projectThreadProjectionForWire(projection);
        assert.isFalse("legacyBootstrapClaim" in publicProjection.thread);
        assert.isFalse("legacyBootstrap" in publicProjection.runs[0]!);
        assert.isFalse("workspaceRunSetupScript" in publicProjection.runs[0]!);
        assert.isFalse("legacyPreparationFailureKnown" in publicProjection.runs[0]!);
        assert.deepStrictEqual(projection.runs[0]?.legacyBootstrap, policy);
        const sink = yield* EventSink.EventSinkV2;
        const pull = yield* Stream.toPull(
          sink.stream({ threadId, eventType: "run.created", bounded: true }),
        );
        const replay = yield* pull;
        assert.lengthOf(replay, 1);
        assert.equal(replay[0]!.event.type, "run.created");
        if (replay[0]!.event.type !== "run.created")
          return yield* Effect.die("Missing bounded run replay");
        assert.notProperty(replay[0]!.event.payload, "legacyBootstrap");
        assert.notProperty(replay[0]!.event.payload, "workspaceRunSetupScript");
        assert.notProperty(replay[0]!.event.payload, "legacyPreparationFailureKnown");
        const [liveStored] = yield* sink.write({
          commandId: CommandId.make("recorded-codec:live-command"),
          events: [
            {
              id: EventId.make("recorded-codec:live-run"),
              type: "run.created",
              threadId,
              runId,
              occurredAt: run.requestedAt,
              payload: run,
            },
          ],
        });
        const live = yield* pull;
        assert.deepEqual(
          live.map((stored) => stored.sequence),
          [liveStored!.sequence],
        );
        assert.equal(live[0]!.event.type, "run.created");
        if (live[0]!.event.type !== "run.created")
          return yield* Effect.die("Missing bounded live run");
        assert.notProperty(live[0]!.event.payload, "legacyBootstrap");
        assert.notProperty(live[0]!.event.payload, "workspaceRunSetupScript");
        const raw = Array.from(
          yield* sink
            .readByCommandId({ commandId: CommandId.make("recorded-codec:live-command") })
            .pipe(Stream.runCollect),
        );
        assert.equal(raw[0]!.event.type, "run.created");
        if (raw[0]!.event.type !== "run.created")
          return yield* Effect.die("Missing raw recorded run");
        assert.deepStrictEqual(raw[0]!.event.payload.legacyBootstrap, policy);
        assert.strictEqual(raw[0]!.event.payload.workspaceRunSetupScript, false);
        assert.strictEqual(raw[0]!.event.payload.legacyPreparationFailureKnown, false);
      }).pipe(Effect.provide(Layer.fresh(layer))),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
