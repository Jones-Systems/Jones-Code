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
  OrchestrationV2DomainEventJson,
  type OrchestrationV2PrivateEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ThreadManagement from "./ThreadManagementService.ts";
import { OrchestratorProjectionError } from "./Orchestrator.ts";
import { subscribeOrchestrationV2Thread } from "../ws.ts";
import {
  isPublicStoredOrchestrationEvent,
  projectThreadProjectionForWire,
} from "./WireProjection.ts";
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
import {
  LegacyPreparation,
  RecordedAppThreadJson,
  RecordedLifecycleEvent,
  RecordedLifecycleEventJson,
  RecordedRunJson,
} from "./RecordedTypes.ts";
import {
  canonicalLegacyPayload,
  legacyBootstrapBirth,
  legacyBootstrapCreateCommandId,
  legacyPayloadHash,
  legacyPreparationEffectId,
  legacyPreparationGeneration,
  legacyPreparationReleaseBlocker,
  transitionLegacyPreparation,
} from "./LegacyBootstrap.ts";
import * as RetainedRecords from "../jones/legacyBootstrap/RetainedRecords.ts";
import { overlayRetainedRecords } from "../jones/legacyBootstrap/RetainedRecords.testkit.ts";

const decodeRecordedThreadJson = Schema.decodeUnknownSync(RecordedAppThreadJson);
const decodeRecordedRunJson = Schema.decodeUnknownSync(RecordedRunJson);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeLegacyPreparation = Schema.decodeUnknownSync(LegacyPreparation);
const encodeRecordedThreadRow = Schema.encodeEffect(Schema.fromJsonString(RecordedAppThreadJson));
const encodeRecordedRunRow = Schema.encodeEffect(Schema.fromJsonString(RecordedRunJson));

// Receiving-era fixtures: the exact upstream ledger, receipt and project row shapes that
// predate sparse Jones storage. Fresh writes never produce inline private values or
// shared-ledger preflight.
const insertHistoricalReceipt = Effect.fn("LegacyPersistenceFixture.receipt")(function* (input: {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly commandType: string;
  readonly sequence: number;
  readonly acceptedAt: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_command_receipts (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
    VALUES (${input.commandId}, 'thread', ${input.threadId}, ${input.commandType}, ${input.acceptedAt}, ${input.sequence}, 'accepted', NULL)`;
});
const insertHistoricalPreflight = Effect.fn("LegacyPersistenceFixture.preflight")(
  function* (input: {
    readonly commandId: CommandId;
    readonly event: OrchestrationV2PrivateEvent;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const occurredAt = DateTime.formatIso(input.event.occurredAt);
    const [row] = yield* sql<{
      readonly sequence: number;
    }>`INSERT INTO orchestration_events (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${input.event.id}, 'thread', ${input.event.threadId}, COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events WHERE aggregate_kind = 'thread' AND stream_id = ${input.event.threadId}), 0), ${input.event.type}, ${occurredAt}, ${input.commandId}, NULL, ${input.commandId}, 'server', ${JSON.stringify(input.event.payload)}, '{}', 2)
    RETURNING sequence`;
    if (row === undefined) return yield* Effect.die("Historical preflight position required");
    yield* insertHistoricalReceipt({
      commandId: input.commandId,
      threadId: input.event.threadId,
      commandType: input.event.type,
      sequence: row.sequence,
      acceptedAt: occurredAt,
    });
    return row.sequence;
  },
);
const insertProject = Effect.fn("LegacyPersistenceFixture.project")(function* (
  projectId: ProjectId,
  workspaceRoot: string,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at)
    VALUES(${projectId},'Legacy fixture',${workspaceRoot},'[]','2026-10-05T00:00:00.000Z','2026-10-05T00:00:00.000Z')`;
});
// Public projection plus only the sparse legacy values from the explicit Jones reader.
const retainedProjection = Effect.fn("LegacyPersistenceFixture.retainedProjection")(function* (
  threadId: ThreadId,
) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const read = yield* RetainedRecords.make;
  return yield* overlayRetainedRecords(
    yield* projections.getThreadProjection(threadId),
    yield* read(threadId),
  );
});

type RecordedEventForType<EventType extends RecordedLifecycleEvent["type"]> = Extract<
  RecordedLifecycleEvent,
  { type: EventType }
> &
  Extract<RecordedLifecycleEventJson, { type: EventType }>;

const compiledDecodeSync1 = Schema.decodeSync(OrchestrationV2DomainEventJson);
const compiledDecodeSync2 = Schema.decodeSync(RecordedLifecycleEventJson);
const compiledDecodeSync3 = Schema.decodeSync(RecordedLifecycleEvent);
const compiledEncodeSync4 = Schema.encodeSync(RecordedLifecycleEventJson);

it("recorded event codecs preserve upstream pull request link payloads", () => {
  const input = {
    id: EventId.make("recorded-codec:pull-request"),
    threadId: ThreadId.make("recorded-codec:T"),
    type: "thread.pull-request-link-updated",
    occurredAt: "2026-10-05T00:00:00.000Z",
    payload: {
      host: "github.com",
      repository: "example/project",
      number: 42,
      url: "https://github.com/example/project/pull/42",
      source: "manual",
      linkedAt: "2026-10-05T00:00:00.000Z",
      snapshot: null,
      stack: null,
    },
  } satisfies typeof OrchestrationV2DomainEventJson.Encoded;
  const publicEvent = compiledDecodeSync1(input);
  const recordedEvent = compiledDecodeSync2(input);
  assert.deepStrictEqual(recordedEvent, publicEvent);
  assert.deepStrictEqual(compiledDecodeSync3(publicEvent), publicEvent);
  assert.deepStrictEqual(compiledEncodeSync4(recordedEvent), input);
  assert.throws(() =>
    compiledDecodeSync2({
      ...input,
      payload: { ...input.payload, number: 0 },
    }),
  );
});

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
          // Preflight binds its fetch cwd to the live project row.
          yield* insertProject(projectId, "/repo");
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
          assert.isTrue(Option.isNone(yield* sink.readLegacyPreflight(outcomeId)));
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
          // Fresh private preflight stays out of the public ledger; after reopen the Jones reader
          // returns each record with its private-ordinal receipt.
          assert.isEmpty(Array.from(yield* store.read({ threadId }).pipe(Stream.runCollect)));
          const recorded = yield* Effect.forEach([intentId, outcomeId], (id) =>
            sink.readLegacyPreflight(id),
          );
          assert.deepEqual(
            recorded.map((entry) => (Option.isSome(entry) ? entry.value.event.type : null)),
            ["legacy-bootstrap.preflight-intent", "legacy-bootstrap.preflight-outcome"],
          );
          const [recordedIntent, recordedOutcome] = recorded;
          if (
            recordedIntent === undefined ||
            Option.isNone(recordedIntent) ||
            recordedOutcome === undefined ||
            Option.isNone(recordedOutcome)
          )
            return yield* Effect.die("Reopened private preflight records required");
          assert.equal(recordedIntent.value.receipt.resultSequence, first);
          assert.isAbove(recordedOutcome.value.receipt.resultSequence, first);
          for (const entry of [recordedIntent.value, recordedOutcome.value]) {
            assert.equal(entry.receipt.status, "accepted");
            assert.equal(entry.receipt.commandType, entry.event.type);
            assert.equal(entry.receipt.sequenceDomain, "jones-legacy-ordinal");
            assert.isTrue(Option.isNone(yield* receipts.getByCommandId(entry.receipt.commandId)));
          }
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

const compiledEncodeEffect5 = Schema.encodeEffect(OrchestrationV2ThreadStreamItem);

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
        const store = yield* EventStore.EventStoreV2;
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
        yield* insertProject(projectId, "/repo");
        // Receiving committed preflight at upstream positions in the shared thread ledger. Those
        // rows replay in their historical domain and are never adopted as private ordinals.
        const intentSequence = yield* insertHistoricalPreflight({
          commandId: intentId,
          event: intent,
        });
        const intentReceipt = yield* sink.commitLegacyPreflight({
          commandId: intentId,
          event: intent,
        });
        assert.isFalse(intentReceipt.committed);
        assert.equal(intentReceipt.receipt.resultSequence, intentSequence);
        assert.equal(intentReceipt.receipt.sequenceDomain, "historical-upstream");
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
        const outcomeId = CommandId.make(`${createCommandId}:preflight-outcome`);
        const outcomeSequence = yield* insertHistoricalPreflight({
          commandId: outcomeId,
          event: outcome,
        });
        assert.isFalse(
          (yield* sink.commitLegacyPreflight({ commandId: outcomeId, event: outcome })).committed,
        );
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
            // This client resumes by cursor replay, which the visit-compatibility gate requires.
            acceptCompactThreadVisits: true,
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
          commandId: createCommandId,
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
        // Receiving persisted this visible claim inline with its accepted claim receipt.
        const visibleClaim = {
          ...thread,
          title: "Visible",
          updatedAt: later,
          legacyBootstrapClaim: binding.policy,
        };
        const sql = yield* SqlClient.SqlClient;
        const visibleRow = yield* encodeRecordedThreadRow(visibleClaim);
        yield* sql`UPDATE orchestration_events SET payload_json = ${visibleRow} WHERE event_id = ${"wire:visible"}`;
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${visibleRow} WHERE thread_id = ${threadId}`;
        yield* insertHistoricalReceipt({
          commandId: createCommandId,
          threadId,
          commandType: "thread.metadata.update",
          sequence: visible[0]!.sequence,
          acceptedAt: DateTime.formatIso(later),
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
        const publicFrames = yield* Effect.forEach(served, (item) => compiledEncodeEffect5(item));
        const serializedFrames = yield* encodeJson(publicFrames);
        assert.notInclude(serializedFrames, "legacy-bootstrap.preflight");
        assert.notInclude(serializedFrames, "legacyBootstrapClaim");
        const persisted = yield* retainedProjection(threadId);
        assert.deepEqual(persisted.thread.legacyBootstrapClaim, binding.policy);
        assert.notProperty(
          projectThreadProjectionForWire(persisted).thread,
          "legacyBootstrapClaim",
        );
        assert.deepEqual(
          (yield* retainedProjection(threadId)).thread.legacyBootstrapClaim,
          binding.policy,
        );
        const privateCursor = (yield* sink.commitLegacyPreflight({
          commandId: outcomeId,
          event: outcome,
        })).receipt.resultSequence;
        assert.equal(privateCursor, outcomeSequence);
        const wireReplay = Array.from(
          yield* Stream.unwrap(
            subscribeOrchestrationV2Thread({
              threadId,
              afterSequence: privateCursor,
              requestCompletionMarker: true,
              acceptCompactThreadVisits: true,
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
        const memory = yield* ProjectionStore.ProjectionStoreV2.pipe(
          Effect.provide(Layer.fresh(ProjectionStore.layerMemory)),
        );
        // Replay the actual ledger through birth and both historical private rows; the public
        // selection, not the projection, owns private exclusion.
        const ledger = Array.from(
          yield* store.read({ threadId, throughSequence: outcomeSequence }).pipe(Stream.runCollect),
        );
        assert.deepEqual(
          ledger.map((stored) => stored.event.type),
          [
            "thread.created",
            "legacy-bootstrap.preflight-intent",
            "legacy-bootstrap.preflight-outcome",
          ],
        );
        assert.deepEqual(
          ledger
            .filter((stored) => !isPublicStoredOrchestrationEvent(stored))
            .map((stored) => stored.sequence),
          [intentSequence, outcomeSequence],
        );
        for (const stored of ledger.filter(isPublicStoredOrchestrationEvent))
          yield* memory.apply(stored.event);
        assert.deepEqual(yield* memory.getThread(threadId), thread);
        const deletedThread = { ...thread, deletedAt: later, updatedAt: later };
        yield* projections.apply({
          id: EventId.make("wire:deleted"),
          type: "thread.deleted",
          threadId,
          occurredAt: later,
          payload: deletedThread,
        });
        // The deletion write preserved the inline claim first; historical private rows remain
        // outside public replay after deletion.
        const deletedLedger = Array.from(yield* store.read({ threadId }).pipe(Stream.runCollect));
        assert.deepEqual(
          deletedLedger
            .filter((stored) => !isPublicStoredOrchestrationEvent(stored))
            .map((stored) => stored.sequence),
          [intentSequence, outcomeSequence],
        );
        assert.deepEqual(yield* projections.getThread(threadId), deletedThread);
        assert.deepEqual(
          (yield* retainedProjection(threadId)).thread.legacyBootstrapClaim,
          binding.policy,
        );
      }).pipe(Effect.provide(layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

const compiledDecodeEffect6 = Schema.decodeEffect(RecordedLifecycleEvent);
const compiledEncodeEffect7 = Schema.encodeEffect(RecordedLifecycleEventJson);
const compiledEncodeEffect8 = Schema.encodeEffect(RecordedRunJson);
const compiledDecodeEffect9 = Schema.decodeEffect(RecordedLifecycleEventJson);
const compiledEncodeEffect10 = Schema.encodeEffect(RecordedAppThreadJson);

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
      // The receiving claim is anchored by thread.created, which binds an owned new thread.
      ownsNewThread: true,
      runId,
    };
    const { runId: _claimRunId, ...claimPolicy } = policy;
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
      legacyBootstrapClaim: claimPolicy,
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
    const runEventBase = {
      threadId,
      runId,
      occurredAt: run.requestedAt,
      payload: run,
    };
    const runEventInputs = [
      {
        ...runEventBase,
        id: EventId.make("recorded-codec:run.created:round-trip"),
        type: "run.created",
      } satisfies RecordedEventForType<"run.created">,
      {
        ...runEventBase,
        id: EventId.make("recorded-codec:run.updated:round-trip"),
        type: "run.updated",
      } satisfies RecordedEventForType<"run.updated">,
    ];
    for (const input of runEventInputs) {
      const event = yield* compiledDecodeEffect6(input);
      assert.deepStrictEqual(event, input);
      const encoded = yield* compiledEncodeEffect7(event);
      assert.deepStrictEqual(encoded.payload, yield* compiledEncodeEffect8(run));
      assert.deepStrictEqual(yield* compiledDecodeEffect9(encoded), input);
    }
    const threadEventBase = {
      threadId,
      occurredAt: thread.createdAt,
      payload: thread,
    };
    const threadEventInputs = [
      {
        ...threadEventBase,
        id: EventId.make("recorded-codec:thread.created:round-trip"),
        type: "thread.created",
      } satisfies RecordedEventForType<"thread.created">,
      {
        ...threadEventBase,
        id: EventId.make("recorded-codec:thread.archived:round-trip"),
        type: "thread.archived",
      } satisfies RecordedLifecycleEvent & RecordedLifecycleEventJson,
    ];
    for (const input of threadEventInputs) {
      const event = yield* compiledDecodeEffect6(input);
      assert.deepStrictEqual(event, input);
      const encoded = yield* compiledEncodeEffect7(event);
      assert.deepStrictEqual(encoded.payload, yield* compiledEncodeEffect10(thread));
      assert.deepStrictEqual(yield* compiledDecodeEffect9(encoded), input);
    }
    yield* Effect.scoped(
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const sql = yield* SqlClient.SqlClient;
        yield* insertProject(projectId, "/recorded-codec");
        const [claimStored] = yield* sink.write({
          commandId: createCommandId,
          events: [
            {
              id: EventId.make("recorded-codec:thread"),
              threadId,
              type: "thread.created",
              occurredAt: thread.createdAt,
              payload: thread,
            },
          ],
        });
        const [birthStored] = yield* sink.write({
          commandId: policy.birthCommandId,
          events: [
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
        if (claimStored === undefined || birthStored === undefined)
          return yield* Effect.die("Recorded claim and birth positions required");
        // Receiving persisted these values inline in the ledger and projection rows, with their
        // accepted claim and birth receipts; fresh public writes no longer do.
        const threadRow = yield* encodeRecordedThreadRow(thread);
        const runRow = yield* encodeRecordedRunRow(run);
        yield* sql`UPDATE orchestration_events SET payload_json = ${threadRow} WHERE event_id = ${"recorded-codec:thread"}`;
        yield* sql`UPDATE orchestration_events SET payload_json = ${runRow} WHERE event_id = ${"recorded-codec:run"}`;
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${threadRow} WHERE thread_id = ${threadId}`;
        yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json = ${runRow} WHERE run_id = ${runId}`;
        yield* insertHistoricalReceipt({
          commandId: createCommandId,
          threadId,
          commandType: "thread.create",
          sequence: claimStored.sequence,
          acceptedAt: timestamp,
        });
        yield* insertHistoricalReceipt({
          commandId: policy.birthCommandId,
          threadId,
          commandType: "message.dispatch",
          sequence: birthStored.sequence,
          acceptedAt: timestamp,
        });
        assert.isEmpty(yield* sql`SELECT run_id FROM jones_legacy_bootstrap_runs`);
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
        const sink = yield* EventSink.EventSinkV2;
        // The stale ordinary write preserved the inline values into the Jones records first.
        const birthLifecycle = Array.from(
          yield* sink
            .readLifecycleByCommandId({ commandId: policy.birthCommandId })
            .pipe(Stream.runCollect),
        );
        assert.lengthOf(birthLifecycle, 1);
        const birth = birthLifecycle[0]!.event;
        if (birth.type !== "run.created") return yield* Effect.die("Missing recorded birth");
        assert.deepStrictEqual(birth.payload.legacyBootstrap, policy);
        assert.strictEqual(birth.payload.workspaceRunSetupScript, false);
        assert.strictEqual(birth.payload.legacyPreparationFailureKnown, false);
        assert.isTrue((yield* maintenance.rebuild).valid);
        const projection = yield* retainedProjection(threadId);
        assert.deepStrictEqual(
          (yield* projections.getThreadProjection(threadId)).runs[0]?.id,
          projection.runs[0]?.id,
        );
        assert.deepStrictEqual(projection.thread.legacyBootstrapClaim, claimPolicy);
        assert.deepStrictEqual(projection.runs[0]?.legacyBootstrap, policy);
        assert.strictEqual(projection.runs[0]?.workspaceRunSetupScript, false);
        assert.strictEqual(projection.runs[0]?.legacyPreparationFailureKnown, false);
        const publicProjection = projectThreadProjectionForWire(projection);
        assert.isFalse("legacyBootstrapClaim" in publicProjection.thread);
        assert.isFalse("legacyBootstrap" in publicProjection.runs[0]!);
        assert.isFalse("workspaceRunSetupScript" in publicProjection.runs[0]!);
        assert.isFalse("legacyPreparationFailureKnown" in publicProjection.runs[0]!);
        assert.deepStrictEqual(projection.runs[0]?.legacyBootstrap, policy);
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
        // A fresh ordinary write persists only public values; the recorded values stay readable
        // from the explicit Jones record and are not replaced by that write.
        assert.notProperty(raw[0]!.event.payload, "legacyBootstrap");
        const afterLive = yield* retainedProjection(threadId);
        assert.deepStrictEqual(afterLive.thread.legacyBootstrapClaim, claimPolicy);
        assert.deepStrictEqual(afterLive.runs[0]?.legacyBootstrap, policy);
        assert.strictEqual(afterLive.runs[0]?.workspaceRunSetupScript, false);
        assert.strictEqual(afterLive.runs[0]?.legacyPreparationFailureKnown, false);
      }).pipe(Effect.provide(Layer.fresh(layer))),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "receiving inline rows with an in-flight legacy preparation decode, replay and preserve with the same semantics",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "legacy-inflight-" });
      const database = SqlitePersistence.layerFromPath(`${directory}/inflight.sqlite`);
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
      const threadId = ThreadId.make("inflight:T");
      const runId = RunId.make("inflight:R");
      const projectId = ProjectId.make("inflight:P");
      const messageId = MessageId.make("inflight:M");
      const releaseCommandId = CommandId.make("inflight:C");
      const createCommandId = legacyBootstrapCreateCommandId(threadId, releaseCommandId);
      const birthCommandId = CommandId.make(`${createCommandId}:initial-message`);
      const claimPolicy = {
        version: 1 as const,
        createCommandId,
        birthCommandId,
        releaseCommandId,
        projectId,
        threadId,
        messageId,
        payloadHash: "inflight-payload",
        ownsNewThread: true,
      };
      const policy = { ...claimPolicy, runId };
      const modelSelection = {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.1-codex",
      };
      const timestamp = "2026-10-05T00:00:00.000Z";
      const workspaceRoot = "/inflight-repo";
      const claimEventId = EventId.make("inflight:claim");
      const birthEventId = EventId.make("inflight:birth");
      const thread = decodeRecordedThreadJson({
        createdBy: "user",
        creationSource: "web",
        id: threadId,
        projectId,
        title: "In-flight preparation",
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
        legacyBootstrapClaim: claimPolicy,
      });
      const born = decodeRecordedRunJson({
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
        legacyBootstrap: policy,
      });
      const effect = {
        kind: "worktree.add",
        input: {
          cwd: workspaceRoot,
          args: [
            "worktree",
            "add",
            "-b",
            "t3code/inflight",
            "/inflight-worktrees/T",
            "inflight-oid",
          ],
          worktreePath: "/inflight-worktrees/T",
          commonDirectory: `${workspaceRoot}/.git`,
          baseCommitOid: "inflight-oid",
          targetRef: "refs/heads/t3code/inflight",
        },
      };

      // Receiving shape: claim, birth and an accepted worktree.add intent whose outcome was never
      // recorded, all inline in the ledger and projection rows, with no Jones sidecar.
      const fixture = yield* Effect.scoped(
        Effect.gen(function* () {
          const sink = yield* EventSink.EventSinkV2;
          const sql = yield* SqlClient.SqlClient;
          yield* insertProject(projectId, workspaceRoot);
          const [claim] = yield* sink.write({
            commandId: createCommandId,
            events: [
              {
                id: claimEventId,
                threadId,
                type: "thread.created",
                occurredAt: thread.createdAt,
                payload: thread,
              },
            ],
          });
          const [birth] = yield* sink.write({
            commandId: birthCommandId,
            events: [
              {
                id: birthEventId,
                threadId,
                runId,
                type: "run.created",
                occurredAt: born.requestedAt,
                payload: born,
              },
            ],
          });
          if (claim === undefined || birth === undefined)
            return yield* Effect.die("Receiving claim and birth positions required");
          const generation = legacyPreparationGeneration({
            runId,
            birthEventId,
            birthSequence: birth.sequence,
          });
          const initialized = decodeLegacyPreparation({
            version: 1,
            policy,
            claimEventId,
            claimSequence: claim.sequence,
            claimReceiptSequence: claim.sequence,
            birthEventId,
            birthSequence: birth.sequence,
            birthReceiptSequence: birth.sequence,
            generation,
            projectWorkspaceRoot: workspaceRoot,
            commonDirectory: null,
            setup: { status: "no_script" },
            steps: [],
          });
          const effectId = legacyPreparationEffectId({ generation, effect });
          const stem = `${createCommandId}:preparation:${generation}:${effectId}`;
          const inFlight = decodeLegacyPreparation({
            ...initialized,
            commonDirectory: effect.input.commonDirectory,
            steps: [
              {
                effectId,
                inputHash: legacyPayloadHash(canonicalLegacyPayload(effect)),
                effect,
                intentCommandId: `${stem}:intent`,
                intentEventId: `${stem}:intent:event`,
                state: "intent",
              },
            ],
          });
          const intent = inFlight.steps[0]!;
          // The fixture is the released transition's own in-flight state.
          const transitioned = transitionLegacyPreparation({
            current: initialized,
            update: { type: "intent", step: intent },
            commandId: intent.intentCommandId,
          });
          assert.equal(transitioned.type, "accepted");
          if (transitioned.type !== "accepted")
            return yield* Effect.die("Released intent transition required");
          assert.strictEqual(
            canonicalLegacyPayload(transitioned.preparation),
            canonicalLegacyPayload(inFlight),
          );
          const preparing = { ...born, legacyPreparation: inFlight };
          const [intentStored] = yield* sink.write({
            commandId: intent.intentCommandId,
            events: [
              {
                id: intent.intentEventId,
                threadId,
                runId,
                type: "run.updated",
                occurredAt: born.requestedAt,
                payload: preparing,
              },
            ],
          });
          if (intentStored === undefined)
            return yield* Effect.die("Receiving intent position required");
          const threadRow = yield* encodeRecordedThreadRow(thread);
          const bornRow = yield* encodeRecordedRunRow(born);
          const preparingRow = yield* encodeRecordedRunRow(preparing);
          yield* sql`UPDATE orchestration_events SET payload_json = ${threadRow} WHERE event_id = ${claimEventId}`;
          yield* sql`UPDATE orchestration_events SET payload_json = ${bornRow} WHERE event_id = ${birthEventId}`;
          yield* sql`UPDATE orchestration_events SET payload_json = ${preparingRow} WHERE event_id = ${intent.intentEventId}`;
          yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${threadRow} WHERE thread_id = ${threadId}`;
          yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json = ${preparingRow} WHERE run_id = ${runId}`;
          for (const [commandId, commandType, sequence] of [
            [createCommandId, "thread.create", claim.sequence],
            [birthCommandId, "message.dispatch", birth.sequence],
            [intent.intentCommandId, "prepared-run.progress", intentStored.sequence],
          ] as const)
            yield* insertHistoricalReceipt({
              commandId,
              threadId,
              commandType,
              sequence,
              acceptedAt: timestamp,
            });
          assert.isEmpty(yield* sql`SELECT thread_id FROM jones_legacy_bootstrap_threads`);
          assert.isEmpty(yield* sql`SELECT run_id FROM jones_legacy_bootstrap_runs`);
          return {
            claimSequence: claim.sequence,
            birthSequence: birth.sequence,
            intentSequence: intentStored.sequence,
            stem,
            intent,
            inFlight,
            preparing,
          };
        }).pipe(Effect.provide(Layer.fresh(layer))),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const sink = yield* EventSink.EventSinkV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
          const sql = yield* SqlClient.SqlClient;
          const readRetained = yield* RetainedRecords.make;
          const lifecycle = (commandId: CommandId) =>
            sink.readLifecycleByCommandId({ commandId }).pipe(
              Stream.runCollect,
              Effect.map((events) => Array.from(events)),
            );

          // Decode after SQL reopen, before any Jones sidecar exists.
          const records = yield* readRetained(threadId);
          assert.deepStrictEqual(records.thread.legacyBootstrapClaim, claimPolicy);
          assert.lengthOf(records.runs, 1);
          const retainedRun = records.runs[0]!;
          assert.deepStrictEqual(retainedRun.legacyPreparation, fixture.inFlight);
          assert.deepStrictEqual(retainedRun.legacyBootstrap, policy);
          assert.strictEqual(retainedRun.workspaceRunSetupScript, false);
          const publicProjection = yield* projections.getThreadProjection(threadId);
          const wire = projectThreadProjectionForWire(
            yield* overlayRetainedRecords(publicProjection, records),
          );
          assert.notProperty(wire.thread, "legacyBootstrapClaim");
          assert.notProperty(wire.runs[0]!, "legacyPreparation");
          assert.notProperty(wire.runs[0]!, "legacyBootstrap");

          // Replay the receiving ledger through the Jones lifecycle reader.
          const birthCheck = legacyBootstrapBirth({
            policy,
            claimEvents: yield* lifecycle(createCommandId),
            birthEvents: yield* lifecycle(birthCommandId),
          });
          assert.equal(birthCheck.type, "valid");
          if (birthCheck.type !== "valid")
            return yield* Effect.die("Valid receiving birth required");
          assert.equal(birthCheck.claimEventId, claimEventId);
          assert.equal(birthCheck.claimSequence, fixture.claimSequence);
          assert.equal(birthCheck.birthEventId, birthEventId);
          assert.equal(birthCheck.sequence, fixture.birthSequence);
          const intentEvents = yield* lifecycle(fixture.intent.intentCommandId);
          assert.deepEqual(
            intentEvents.map((stored) => [stored.sequence, stored.event.type]),
            [[fixture.intentSequence, "run.updated"]],
          );
          const replayedIntent = intentEvents[0]!.event;
          if (replayedIntent.type !== "run.updated")
            return yield* Effect.die("Replayed preparation intent required");
          assert.deepStrictEqual(replayedIntent.payload.legacyPreparation, fixture.inFlight);

          // Same released decisions as on the receiving object: release stays blocked, the intent
          // cannot repeat, and only its exact outcome can settle it.
          const blocker = legacyPreparationReleaseBlocker({ run: retainedRun });
          assert.isDefined(blocker);
          assert.strictEqual(blocker, legacyPreparationReleaseBlocker({ run: fixture.preparing }));
          const repeated = { type: "intent" as const, step: fixture.intent };
          const repeatedOnRetained = transitionLegacyPreparation({
            current: retainedRun.legacyPreparation,
            update: repeated,
            commandId: fixture.intent.intentCommandId,
          });
          assert.equal(repeatedOnRetained.type, "rejected");
          assert.deepStrictEqual(
            repeatedOnRetained,
            transitionLegacyPreparation({
              current: fixture.inFlight,
              update: repeated,
              commandId: fixture.intent.intentCommandId,
            }),
          );
          const outcomeCommandId = CommandId.make(`${fixture.stem}:outcome`);
          const outcome = {
            type: "outcome" as const,
            step: {
              ...fixture.intent,
              state: "unknown" as const,
              outcomeCommandId,
              outcomeEventId: EventId.make(`${outcomeCommandId}:event`),
              evidence: { type: "unknown" as const, reason: "outcome_lost" as const },
            },
          };
          const settledOnRetained = transitionLegacyPreparation({
            current: retainedRun.legacyPreparation,
            update: outcome,
            commandId: outcomeCommandId,
          });
          assert.equal(settledOnRetained.type, "accepted");
          assert.deepStrictEqual(
            settledOnRetained,
            transitionLegacyPreparation({
              current: fixture.inFlight,
              update: outcome,
              commandId: outcomeCommandId,
            }),
          );

          // A stale ordinary write preserves the inline values into Jones records before replacing
          // the public row, and native rebuild keeps them.
          const {
            legacyPreparation: _preparation,
            legacyBootstrap: _policy,
            workspaceRunSetupScript: _setup,
            ...ordinary
          } = fixture.preparing;
          yield* sink.write({
            events: [
              {
                id: EventId.make("inflight:ordinary"),
                threadId,
                runId,
                type: "run.updated",
                occurredAt: fixture.preparing.requestedAt,
                payload: ordinary,
              },
            ],
          });
          assert.lengthOf(
            yield* sql`SELECT thread_id FROM jones_legacy_bootstrap_threads WHERE thread_id = ${threadId}`,
            1,
          );
          assert.lengthOf(
            yield* sql`SELECT run_id FROM jones_legacy_bootstrap_runs WHERE run_id = ${runId}`,
            1,
          );
          const [publicRow] = yield* sql<{
            readonly payload_json: string;
          }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id = ${runId}`;
          assert.notInclude(publicRow?.payload_json ?? "legacyPreparation", "legacyPreparation");
          assert.deepStrictEqual(
            (yield* readRetained(threadId)).runs[0]?.legacyPreparation,
            fixture.inFlight,
          );
          assert.isTrue((yield* maintenance.rebuild).valid);
          const rebuilt = (yield* readRetained(threadId)).runs[0];
          if (rebuilt === undefined) return yield* Effect.die("Rebuilt retained run required");
          assert.deepStrictEqual(rebuilt.legacyPreparation, fixture.inFlight);
          assert.deepStrictEqual(rebuilt.legacyBootstrap, policy);
          assert.strictEqual(legacyPreparationReleaseBlocker({ run: rebuilt }), blocker);
          const preserved = yield* lifecycle(fixture.intent.intentCommandId);
          const preservedIntent = preserved[0]?.event;
          if (preservedIntent?.type !== "run.updated")
            return yield* Effect.die("Preserved preparation intent required");
          assert.deepStrictEqual(preservedIntent.payload.legacyPreparation, fixture.inFlight);
        }).pipe(Effect.provide(Layer.fresh(layer))),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
