import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  NodeId,
  PlanId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as EventStore from "./EventStore.ts";
import * as EventSink from "./EventSink.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import { providerThreadActivityObservation } from "./ProviderThreadRuntimeObservation.ts";

const threadId = ThreadId.make("thread:control-reads");
const providerThreadId = ProviderThreadId.make("provider-thread:control-reads");
const providerTurnId = ProviderTurnId.make("provider-turn:control-reads");
const providerSessionId = ProviderSessionId.make("session:control-reads");
const attemptId = RunAttemptId.make("attempt:control-reads");
const runId = RunId.make("run:control-reads");
const messageId = MessageId.make("message:control-reads");
const requestId = RuntimeRequestId.make("request:control-reads");
const nodeId = NodeId.make("node:control-reads");
const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId: providerInstanceId, model: "gpt-6" };

function fixtureEvents(now: DateTime.Utc): ReadonlyArray<OrchestrationV2DomainEvent> {
  const common = { threadId, occurredAt: now };
  return [
    {
      ...common,
      id: EventId.make("control:thread"),
      type: "thread.created",
      payload: {
        id: threadId,
        projectId: ProjectId.make("project:control-reads"),
        title: "Control reads",
        providerInstanceId,
        modelSelection,
        createdBy: "user",
        creationSource: "web",
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/workspace",
        activeProviderThreadId: providerThreadId,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
    },
    {
      ...common,
      id: EventId.make("control:provider-thread"),
      type: "provider-thread.updated",
      payload: {
        id: providerThreadId,
        driver,
        providerInstanceId,
        providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "native-control-thread", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "active",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      ...common,
      id: EventId.make("control:run"),
      type: "run.created",
      payload: {
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId,
        modelSelection,
        providerThreadId,
        userMessageId: messageId,
        rootNodeId: nodeId,
        activeAttemptId: attemptId,
        status: "running",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    },
    {
      ...common,
      id: EventId.make("control:attempt"),
      type: "run-attempt.created",
      payload: {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId,
        providerThreadId,
        providerTurnId,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    },
    // A runtime reply resolves the request's recorded node and the run it belongs to.
    {
      ...common,
      id: EventId.make("control:node"),
      type: "node.updated",
      payload: {
        id: nodeId,
        threadId,
        runId,
        parentNodeId: null,
        rootNodeId: nodeId,
        kind: "root_turn",
        status: "running",
        countsForRun: true,
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: now,
        completedAt: null,
      },
    },
    {
      ...common,
      id: EventId.make("control:turn"),
      type: "provider-turn.updated",
      payload: {
        id: providerTurnId,
        providerThreadId,
        nodeId,
        runAttemptId: attemptId,
        nativeTurnRef: { driver, nativeId: "native-control-turn", strength: "strong" },
        ordinal: 1,
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    },
    {
      ...common,
      id: EventId.make("control:message"),
      type: "message.updated",
      payload: {
        id: messageId,
        threadId,
        runId,
        nodeId,
        role: "user",
        text: "Use the smaller fix.",
        attachments: [],
        streaming: false,
        createdBy: "user",
        creationSource: "web",
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      ...common,
      id: EventId.make("control:request"),
      type: "runtime-request.updated",
      payload: {
        id: requestId,
        nodeId,
        providerTurnId,
        nativeRequestRef: null,
        kind: "command",
        status: "resolved",
        responseCapability: { type: "live", providerSessionId },
        createdAt: now,
        resolvedAt: now,
      },
    },
  ];
}

for (const storage of ["sqlite", "memory"] as const) {
  const storeLayer =
    storage === "sqlite"
      ? ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory))
      : ProjectionStore.layerMemory;
  const countsLayer =
    storage === "sqlite"
      ? Layer.merge(storeLayer, EventStore.layer.pipe(Layer.provide(SqlitePersistenceMemory)))
      : storeLayer;
  it.effect(
    `${storage}: operating candidates read only scoped foreground facts and retain completed snoozed threads`,
    () =>
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        // Only the SQLite case has an event store and SQL client beside its projection store.
        const sqliteEventStore = yield* Effect.serviceOption(EventStore.EventStoreV2);
        const sqliteClient = yield* Effect.serviceOption(SqlClient.SqlClient);
        assert.equal(Option.isSome(sqliteEventStore), storage === "sqlite");
        assert.equal(Option.isSome(sqliteClient), storage === "sqlite");
        const now = yield* DateTime.now;
        const threadEvent = fixtureEvents(now).find((event) => event.type === "thread.created")!;
        const runEvent = fixtureEvents(now).find((event) => event.type === "run.created")!;
        const requestEvent = fixtureEvents(now).find(
          (event) => event.type === "runtime-request.updated",
        )!;
        const countsProjectId = ProjectId.make("project:operating-counts");
        const completedId = ThreadId.make("thread:operating-counts:completed-snoozed");
        for (const [name, projectId, archivedAt, deletedAt, completed] of [
          ["running", countsProjectId, null, null, false],
          ["approval", countsProjectId, null, null, false],
          ["input", countsProjectId, null, null, false],
          ["plan", countsProjectId, null, null, true],
          ["completed-snoozed", countsProjectId, null, null, true],
          ["archived", countsProjectId, now, null, false],
          ["deleted", countsProjectId, null, now, false],
          ["other-project", ProjectId.make("project:operating-counts-other"), null, null, false],
        ] as const) {
          const id = ThreadId.make(`thread:operating-counts:${name}`);
          const createdEvent = {
            ...threadEvent,
            id: EventId.make(`event:${id}:thread`),
            threadId: id,
            payload: {
              ...threadEvent.payload,
              id,
              projectId,
              archivedAt,
              deletedAt,
              interactionMode: name === "plan" ? ("plan" as const) : ("default" as const),
              settledAt: completed ? now : null,
              snoozedUntil: completed ? DateTime.add(now, { hours: 1 }) : null,
              lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
            },
          };
          yield* store.apply(createdEvent);
          if (storage === "sqlite") {
            yield* Option.getOrThrow(sqliteEventStore).append({ events: [createdEvent] });
          }
          const scopedRunId = RunId.make(`run:operating-counts:${name}`);
          yield* store.apply({
            ...runEvent,
            id: EventId.make(`event:${id}:run`),
            threadId: id,
            runId: scopedRunId,
            payload: {
              ...runEvent.payload,
              id: scopedRunId,
              threadId: id,
              status: completed ? "completed" : "running",
              completedAt: completed ? now : null,
            },
          });
          if (name === "approval" || name === "input") {
            yield* store.apply({
              ...requestEvent,
              id: EventId.make(`event:${id}:request`),
              threadId: id,
              payload: {
                ...requestEvent.payload,
                id: RuntimeRequestId.make(`request:${id}`),
                status: "pending",
                kind: name === "approval" ? "command" : "user_input",
                resolvedAt: null,
              },
            });
          }
          if (name === "plan") {
            yield* store.apply({
              id: EventId.make(`event:${id}:plan`),
              type: "plan.updated",
              threadId: id,
              occurredAt: now,
              payload: {
                id: PlanId.make(`plan:${id}`),
                threadId: id,
                runId: scopedRunId,
                nodeId,
                status: "active",
                kind: "proposed_plan",
                markdown: "Synthetic plan",
              },
            });
          }
        }
        if (storage === "sqlite") {
          yield* Option.getOrThrow(sqliteEventStore).appendProjectEvent({
            eventId: EventId.make("event:operating-counts:project-update"),
            aggregateKind: "project",
            aggregateId: countsProjectId,
            occurredAt: DateTime.formatIso(now),
            commandId: null,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.meta-updated",
            payload: { projectId: countsProjectId, updatedAt: DateTime.formatIso(now) },
          });
        }
        const before = yield* store.getOperatingCountsCandidates({ projectId: countsProjectId });
        assert.isAbove(before.snapshotSequence, 0);
        if (storage === "sqlite") {
          const eventStore = Option.getOrThrow(sqliteEventStore);
          assert.equal(before.snapshotSequence, yield* eventStore.latestApplicationSequence);
          assert.isAbove(before.snapshotSequence, yield* eventStore.latestSequence());
        }
        assert.deepEqual(
          before.threads.map((thread) => thread.id),
          ["approval", "completed-snoozed", "input", "plan", "running"].map((name) =>
            ThreadId.make(`thread:operating-counts:${name}`),
          ),
        );
        const completed = before.threads.find((thread) => thread.id === completedId)!;
        assert.deepEqual(completed, {
          id: completedId,
          projectId: countsProjectId,
          activeProviderThreadId: providerThreadId,
          archivedAt: null,
          deletedAt: null,
          pendingRuntimeRequest: null,
          activityRunStatus: null,
          interactionMode: "default",
          hasActionableProposedPlan: false,
          latestRunCompletedAt: now,
        });
        assert.equal(
          before.threads.find(
            (thread) => thread.id === ThreadId.make("thread:operating-counts:running"),
          )?.activityRunStatus,
          "running",
        );
        assert.deepEqual(before.threads[0]?.pendingRuntimeRequest, { kind: "command" });
        assert.deepEqual(before.threads[2]?.pendingRuntimeRequest, { kind: "user_input" });
        assert.equal(before.threads[3]?.hasActionableProposedPlan, true);
        assert.equal(before.threads[3]?.interactionMode, "plan");
        assert.deepEqual(
          providerThreadActivityObservation(
            completed,
            {
              status: "monitoring",
              binding: {
                threadId: completedId,
                providerThreadId,
                providerSessionId,
                instanceId: providerInstanceId,
                runtimeGeneration: "synthetic-counts-generation",
              },
              observedAt: DateTime.formatIso(now),
            },
            DateTime.toEpochMillis(now),
          ),
          { foreground: null, background: "monitoring", backgroundStatus: "known" },
        );
        assert.deepEqual(
          providerThreadActivityObservation(
            completed,
            { status: "unknown", reason: "runtime_not_resident" },
            DateTime.toEpochMillis(now),
          ),
          {
            foreground: null,
            background: null,
            backgroundStatus: "known",
            reason: "runtime_not_resident",
          },
        );
        assert.deepEqual(
          (yield* store.getOperatingCountsCandidates({
            projectId: ProjectId.make("project:absent"),
          })).threads,
          [],
        );
        assert.lengthOf((yield* store.getOperatingCountsCandidates()).threads, 6);
        if (storage === "sqlite") {
          const sql = Option.getOrThrow(sqliteClient);
          // Candidate columns remain readable when unrelated shell and history JSON cannot decode.
          yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{broken'`;
          yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json = '{broken'`;
          yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET payload_json = '{broken'`;
          yield* sql`UPDATE orchestration_v2_projection_plans SET payload_json = '{broken'`;
          assert.equal(
            (yield* store.getThreadProjection(completedId).pipe(Effect.result))._tag,
            "Failure",
          );
          assert.deepEqual(
            yield* store.getOperatingCountsCandidates({ projectId: countsProjectId }),
            before,
          );
          yield* sql`UPDATE orchestration_v2_projection_threads SET interaction_mode = 'invalid-mode'
          WHERE thread_id = ${completedId}`;
          assert.equal(
            (yield* store
              .getOperatingCountsCandidates({ projectId: countsProjectId })
              .pipe(Effect.result))._tag,
            "Failure",
          );
        }
      }).pipe(Effect.provide(Layer.fresh(countsLayer))),
  );

  it.effect(`${storage}: finds the active root turn without an attempt reverse link`, () =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const events = fixtureEvents(now).map((event) =>
        event.type === "run-attempt.created"
          ? { ...event, payload: { ...event.payload, providerTurnId: null } }
          : event,
      );
      yield* Effect.forEach(events, (event) => store.apply(event), { discard: true });
      const running = yield* store.getRunningTurnContext(threadId);
      assert.equal(running.providerTurn?.id, providerTurnId);
      const turnEvent = events.find((event) => event.type === "provider-turn.updated")!;
      yield* store.apply({
        ...turnEvent,
        payload: { ...turnEvent.payload, status: "completed", completedAt: now },
      });
      assert.isUndefined((yield* store.getRunningTurnContext(threadId)).providerTurn);
    }).pipe(Effect.provide(storeLayer)),
  );
  it.effect(`${storage}: controls and replies read only their exact durable targets`, () =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const fixtureSql = yield* SqlClient.SqlClient;
      const now = yield* DateTime.now;
      const events = fixtureEvents(now);
      yield* Effect.forEach(events, (event) => store.apply(event), { discard: true });
      if (storage === "sqlite") {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT INTO orchestration_v2_projection_messages
        (message_id, thread_id, run_id, node_id, role, streaming, created_at, updated_at, payload_json)
        VALUES ('message:unrelated-obsolete', ${threadId}, ${runId}, ${nodeId}, 'assistant', 0,
          ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)}, '{"obsolete":"transcript"}')`;
        assert.equal((yield* Effect.exit(store.getThreadProjection(threadId)))._tag, "Failure");
        const queryPlan = yield* sql<{ detail: string }>`EXPLAIN QUERY PLAN
          SELECT payload_json FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${threadId} AND node_id = ${nodeId}
            AND type IN ('approval_request', 'user_input_request')
            AND json_extract(payload_json, '$.requestId') = ${requestId}
          ORDER BY ordinal ASC LIMIT 1`;
        assert.isTrue(queryPlan.some((row) => row.detail.includes("turn_items_node_ordinal_idx")));
      }
      const running = yield* store.getRunningTurnContext(threadId);
      assert.equal(running.run?.id, runId);
      assert.equal(running.providerThread?.id, providerThreadId);
      assert.equal(running.providerTurn?.id, providerTurnId);
      const providerContext = yield* store.getThreadProviderContext(threadId, providerInstanceId);
      assert.equal(providerContext.thread.id, threadId);
      assert.deepEqual(
        providerContext.providerThreads.map((thread) => thread.id),
        [providerThreadId],
      );
      const responseContext = yield* store.getRuntimeResponseContext(threadId, requestId);
      assert.equal(responseContext.request?.id, requestId);
      const target = { providerThreadId, providerTurnId, attemptId, messageId };
      const context = yield* store.getProviderControlContext(threadId, target);
      assert.equal(context.providerThread?.id, providerThreadId);
      assert.equal(context.providerTurn?.id, providerTurnId);
      assert.equal(context.attempt?.id, attemptId);
      assert.equal(context.message?.text, "Use the smaller fix.");
      assert.equal(context.run?.id, runId);
      assert.equal((yield* store.getRuntimeRequest(threadId, requestId))?.status, "resolved");
      assert.isUndefined(
        yield* store.getRuntimeRequest(threadId, RuntimeRequestId.make("request:missing")),
      );
      const absent = yield* store.getProviderControlContext(threadId, {
        providerThreadId: ProviderThreadId.make("provider-thread:missing"),
        providerTurnId: ProviderTurnId.make("provider-turn:missing"),
      });
      assert.isUndefined(absent.providerThread);
      assert.isUndefined(absent.providerTurn);
      const missingThread = yield* store
        .getRuntimeRequest(ThreadId.make("thread:missing"), requestId)
        .pipe(Effect.flip);
      assert.instanceOf(missingThread, ProjectionStore.ProjectionStoreThreadNotFoundError);

      const calls: string[] = [];
      const sessions = Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
        get: () =>
          Effect.succeed(
            Option.some({
              interruptTurn: () =>
                Effect.sync(() => {
                  calls.push("interrupt");
                }),
              steerTurn: (input: { message: { text: string }; runId: RunId }) =>
                Effect.sync(() => {
                  assert.equal(input.runId, runId);
                  calls.push(input.message.text);
                }),
              respondToRuntimeRequest: () =>
                Effect.sync(() => {
                  calls.push("reply");
                }),
            } as never),
          ),
      });
      yield* Effect.gen(function* () {
        const control = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
        const reply = yield* RuntimeRequestService.RuntimeRequestServiceV2;
        yield* control.interrupt({ threadId, providerThreadId, providerTurnId, providerSessionId });
        yield* control.steer({
          threadId,
          providerThreadId,
          providerTurnId,
          providerSessionId,
          messageId,
        });
        yield* reply.respond({ threadId, providerSessionId, requestId, decision: "accept" });
        assert.deepEqual(calls, ["interrupt", "Use the smaller fix.", "reply"]);
        const wrongSession = ProviderSessionId.make("session:wrong");
        assert.equal(
          (yield* Effect.exit(
            control.interrupt({
              threadId,
              providerThreadId,
              providerTurnId,
              providerSessionId: wrongSession,
            }),
          ))._tag,
          "Failure",
        );
        assert.equal(
          (yield* Effect.exit(
            reply.respond({
              threadId,
              providerSessionId: wrongSession,
              requestId,
              decision: "accept",
            }),
          ))._tag,
          "Failure",
        );
        assert.lengthOf(calls, 3);
        const turn = context.providerTurn!;
        yield* store.apply({
          id: EventId.make("control:turn-completed"),
          type: "provider-turn.updated",
          threadId,
          occurredAt: now,
          payload: { ...turn, status: "completed", completedAt: now },
        });
        // A settled turn's Stop still reaches the adapter, which alone knows
        // whether it runs background work for the thread.
        yield* control.interrupt({ threadId, providerThreadId, providerTurnId, providerSessionId });
        assert.deepEqual(calls, ["interrupt", "Use the smaller fix.", "reply", "interrupt"]);
      }).pipe(
        Effect.provide(
          Layer.merge(ProviderTurnControlService.layer, RuntimeRequestService.layer).pipe(
            Layer.provide(
              Layer.merge(
                sessions,
                Layer.mock(EventSink.EventSinkV2)({
                  readOrdinaryCheckoutAdmissionForRun: () =>
                    Effect.gen(function* () {
                      const rows =
                        yield* fixtureSql`SELECT admission_id FROM orchestration_v2_ordinary_checkout_admissions WHERE thread_id = ${threadId}`;
                      assert.deepEqual(rows, []);
                      return null;
                    }).pipe(
                      Effect.mapError(
                        (cause) => new EventSink.EventSinkWriteError({ eventCount: 0, cause }),
                      ),
                    ),
                  revalidateOrdinaryCheckoutExecution: () =>
                    Effect.die("Projection-only control fixture has no checkout execution actor."),
                }),
              ),
            ),
          ),
        ),
      );
    }).pipe(Effect.provide(Layer.merge(storeLayer, SqlitePersistenceMemory))),
  );
}
