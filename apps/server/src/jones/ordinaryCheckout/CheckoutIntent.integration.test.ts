import { assert, it } from "@effect/vitest";
import * as NodePath from "@effect/platform-node/NodePath";
import * as HostProcess from "@t3tools/shared/HostProcess";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  RunId,
  ScheduledTaskId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as ProviderAdapterRegistry from "../../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2Shape } from "@t3tools/provider-core/server/ProviderAdapter";
import * as ImportedFixture from "../importedHistory/ImportedHistoryFixture.testkit.ts";
import * as CheckoutIntent from "./CheckoutIntent.ts";
import * as CheckoutTarget from "./CheckoutTarget.ts";
import * as Repository from "./OrdinaryCheckoutSqlite.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckout.ts";
import {
  CheckoutError,
  operationId,
  type Admission,
  type Binding,
  type Source,
} from "./CheckoutProtocol.ts";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "synthetic" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Provider execution is excluded from accepted-intent tests"),
} as ProviderAdapterV2Shape;
class ObservationState extends Context.Service<
  ObservationState,
  {
    readonly published: Ref.Ref<number>;
    readonly calls: Ref.Ref<number>;
    readonly unavailable: Ref.Ref<boolean>;
  }
>()("t3/jones/ordinaryCheckout/CheckoutIntent.integration.test/ObservationState") {}
const observationState = Layer.effect(
  ObservationState,
  Effect.gen(function* () {
    return {
      published: yield* Ref.make(0),
      calls: yield* Ref.make(0),
      unavailable: yield* Ref.make(false),
    };
  }),
);
const observation = Layer.effect(
  CheckoutTarget.CheckoutTargetObservation,
  Effect.gen(function* () {
    const state = yield* ObservationState;
    return CheckoutTarget.CheckoutTargetObservation.of({
      directory: (path) =>
        Effect.gen(function* () {
          yield* Ref.update(state.calls, (n) => n + 1);
          if (yield* Ref.get(state.unavailable))
            return yield* new CheckoutError({ reason: "unavailable" });
          return { realPath: path, identity: { version: 1, device: "1", inode: path, birth: "1" } };
        }),
      git: () => Effect.succeed(null),
    });
  }),
).pipe(Layer.provide(observationState));
const database = SqlitePersistence.layerMemory;
const repository = Repository.layer.pipe(Layer.provide(database), Layer.provide(NodePath.layer));
const targets = CheckoutTarget.layer.pipe(
  Layer.provide(observation),
  Layer.provide(NodePath.layer),
  Layer.provide(Layer.succeed(HostProcess.HomeDirectory, "/synthetic-home")),
);
const intent = CheckoutIntent.layer.pipe(
  Layer.provide(Layer.mergeAll(database, repository, targets)),
);
const observedStore = Layer.effect(
  EventStore.EventStoreV2,
  Effect.gen(function* () {
    const store = yield* EventStore.EventStoreV2;
    const observation = yield* ObservationState;
    return {
      ...store,
      publishCommitted: (events: Parameters<typeof store.publishCommitted>[0]) =>
        Ref.update(observation.published, (n) => n + events.length).pipe(
          Effect.andThen(store.publishCommitted(events)),
        ),
    };
  }),
).pipe(Layer.provide(EventStore.layer), Layer.provide(observationState));
const testLayer = Layer.mergeAll(
  database,
  observationState,
  repository,
  intent,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  EffectOutbox.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "ordinary-checkout-intent" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    {
      databaseLayer: database,
      runEffectWorker: false,
      checkoutIntentLayer: intent,
      eventStoreLayer: observedStore,
    },
  ),
);
const threadId = ThreadId.make("ordinary-thread");
const now = "2026-10-18T00:00:00.000Z";
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES('ordinary-project','Synthetic','/ordinary','[]',${now},${now})`;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create-ordinary"),
    threadId,
    projectId: ProjectId.make("ordinary-project"),
    title: "Synthetic",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdBy: "user",
    creationSource: "web",
  });
  return orchestrator;
});
const command = {
  type: "message.dispatch",
  commandId: CommandId.make("ordinary-message"),
  threadId,
  messageId: MessageId.make("ordinary-message-id"),
  text: "Synthetic accepted intent",
  attachments: [],
  createdBy: "user",
  creationSource: "web",
  dispatchMode: { type: "start_immediately" },
} as const;
const state = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return {
    events: yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
    receipts: yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
    threads: yield* sql`SELECT * FROM orchestration_v2_projection_threads ORDER BY thread_id`,
    runs: yield* sql`SELECT * FROM orchestration_v2_projection_runs ORDER BY run_id`,
    messages: yield* sql`SELECT * FROM orchestration_v2_projection_messages ORDER BY message_id`,
    providerThreads:
      yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads ORDER BY provider_thread_id`,
    effects: yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
    owners: yield* sql`SELECT * FROM jones_ordinary_checkout_owners ORDER BY checkout_key`,
    admissions: yield* sql`SELECT * FROM jones_ordinary_checkout_admissions ORDER BY admission_id`,
    links: yield* sql`SELECT * FROM jones_ordinary_checkout_effect_links ORDER BY effect_id`,
    uses: yield* sql`SELECT * FROM jones_ordinary_checkout_uses ORDER BY operation_id`,
    lifecycle:
      yield* sql`SELECT * FROM jones_ordinary_checkout_lifecycle_events ORDER BY operation_id,ordinal`,
  };
});

it.effect(
  "real Orchestrator atomically accepts null-path intent and replays before physical observation",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* seed;
      const result = yield* orchestrator.dispatch(command);
      const accepted = yield* state;
      assert.lengthOf(accepted.owners, 1);
      assert.lengthOf(accepted.admissions, 1);
      assert.lengthOf(accepted.links, 1);
      assert.equal(accepted.owners[0]!.checkout_key, "/ordinary");
      const observation = yield* ObservationState;
      const calls = yield* Ref.get(observation.calls);
      yield* Ref.set(observation.unavailable, true);
      assert.deepEqual(yield* orchestrator.dispatch(command), result);
      assert.equal(yield* Ref.get(observation.calls), calls);
      assert.deepEqual(yield* state, accepted);
      assert.isTrue(
        Exit.isFailure(
          yield* orchestrator
            .dispatch({ ...command, text: "Reused ID, changed content" })
            .pipe(Effect.exit),
        ),
      );
      assert.equal(yield* Ref.get(observation.calls), calls);
      assert.deepEqual(yield* state, accepted);
    }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["jones_ordinary_checkout_admissions", "jones_ordinary_checkout_effect_links"])(
  "real EventSink rolls back upstream and Jones writes on %s failure",
  (table) =>
    Effect.gen(function* () {
      const orchestrator = yield* seed;
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe(
        `CREATE TRIGGER synthetic_acceptance_failure BEFORE INSERT ON ${table} BEGIN SELECT RAISE(ABORT, 'synthetic acceptance failure'); END`,
      );
      const before = yield* state;
      const published = yield* Ref.get((yield* ObservationState).published);
      assert.isTrue(Exit.isFailure(yield* orchestrator.dispatch(command).pipe(Effect.exit)));
      assert.deepEqual(yield* state, before);
      assert.equal(yield* Ref.get((yield* ObservationState).published), published);
      yield* sql`DROP TRIGGER synthetic_acceptance_failure`;
      yield* orchestrator.dispatch(command);
      assert.lengthOf((yield* state).admissions, 1);
    }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["queue_after_active", "defer_start"] as const)(
  "records %s reservation before any provider use",
  (mode) =>
    Effect.gen(function* () {
      const orchestrator = yield* seed;
      yield* orchestrator.dispatch({ ...command, dispatchMode: { type: mode } });
      const accepted = yield* state;
      assert.lengthOf(accepted.admissions, 1);
      assert.lengthOf(accepted.owners, 1);
      assert.lengthOf(accepted.uses, 0);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("historical receipt stays replay-only and an orphan Jones acceptance refuses", () =>
  Effect.gen(function* () {
    const orchestrator = yield* seed;
    const sql = yield* SqlClient.SqlClient;
    const historical = {
      type: "thread.metadata.update",
      commandId: CommandId.make("historical-title"),
      threadId,
      title: "Historical title",
    } as const;
    yield* orchestrator.dispatch(historical);
    const before = yield* state;
    yield* Ref.set((yield* ObservationState).unavailable, true);
    yield* orchestrator.dispatch(historical);
    assert.deepEqual(yield* state, before);
    yield* Ref.set((yield* ObservationState).unavailable, false);
    yield* orchestrator.dispatch(command);
    yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id=${command.commandId}`;
    const orphan = yield* state;
    assert.isTrue(Exit.isFailure(yield* orchestrator.dispatch(command).pipe(Effect.exit)));
    assert.deepEqual(yield* state, orphan);
  }).pipe(Effect.provide(testLayer)),
);

it.effect("a direct EventSink retry validates content without target acquisition", () =>
  Effect.gen(function* () {
    const orchestrator = yield* seed;
    const accepted = yield* orchestrator.dispatch(command);
    const sql = yield* SqlClient.SqlClient;
    const sink = yield* EventSink.EventSinkV2;
    const rows = yield* sql<{
      accepted_at: string;
    }>`SELECT accepted_at FROM orchestration_command_receipts WHERE command_id=${command.commandId}`;
    const before = yield* state;
    yield* Ref.set((yield* ObservationState).unavailable, true);
    const input = {
      commandId: command.commandId,
      threadId,
      commandType: command.type,
      acceptedAt: DateTime.makeUnsafe(rows[0]!.accepted_at),
      ordinaryCheckoutCommand: command,
      events: [],
      effects: [],
    };
    const replay = yield* sink.commitCommand(input);
    assert.equal(replay.receipt.resultSequence, accepted.sequence);
    assert.isFalse(replay.committed);
    assert.isTrue(
      Exit.isFailure(
        yield* sink
          .commitCommand({
            ...input,
            ordinaryCheckoutCommand: { ...command, type: "thread.delete" },
          })
          .pipe(Effect.exit),
      ),
    );
    assert.deepEqual(yield* state, before);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "derived checkpoint without a live use refuses and rolls back the real terminal transaction",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* seed;
      yield* orchestrator.dispatch(command);
      const projection = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
        threadId,
      );
      const run = projection.runs[0]!;
      const sink = yield* EventSink.EventSinkV2;
      const before = yield* state;
      const result = yield* sink
        .writeWithEffects({
          commandId: CommandId.make("synthetic-checkpoint"),
          events: [
            {
              id: EventId.make("synthetic-terminal"),
              type: "run.updated",
              threadId,
              occurredAt: yield* DateTime.now,
              payload: { ...run, status: "completed" },
            },
          ],
          effects: [
            {
              id: "synthetic-checkpoint",
              commandId: CommandId.make("synthetic-checkpoint"),
              threadId,
              request: {
                type: "checkpoint.capture",
                runId: run.id,
                scopeId: CheckpointScopeId.make("synthetic-scope"),
              },
            },
          ],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      assert.deepEqual(yield* state, before);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("scheduled dispatch reaches the same transactional acceptance seam", () =>
  Effect.gen(function* () {
    const orchestrator = yield* seed;
    yield* orchestrator.dispatch({
      ...command,
      scheduledTaskId: ScheduledTaskId.make("synthetic-schedule"),
    });
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<Admission>`SELECT * FROM jones_ordinary_checkout_admissions`;
    assert.equal(rows[0]!.origin_kind, "scheduled");
    assert.equal(rows[0]!.request_id, "synthetic-schedule");
  }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["writeWithEffects", "writeIfRunCurrent"] as const)(
  "%s commits checkpoint link and terminal event using an existing synthetic use, without a synthetic command receipt",
  (method) =>
    Effect.gen(function* () {
      const orchestrator = yield* seed;
      yield* orchestrator.dispatch(command);
      const sql = yield* SqlClient.SqlClient;
      const admission =
        (yield* sql<Admission>`SELECT * FROM jones_ordinary_checkout_admissions`)[0]!;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const claim = yield* outbox.claimNext({
        workerId: "synthetic-worker",
        leaseDurationMs: 60000,
      });
      assert.isTrue(Option.isSome(claim));
      if (Option.isNone(claim)) return;
      const source: Source = {
        kind: "outbox",
        effectId: claim.value.id,
        workerId: "synthetic-worker",
        attempt: claim.value.attemptCount,
        claimExpiresAt: claim.value.leaseExpiresAt!,
      };
      const binding: Binding = {
        version: 1,
        operationId: operationId(admission.admission_id, source),
        admissionId: admission.admission_id,
        executor: "outbox",
        issuerId: "synthetic-executor",
        source,
        relation: {
          runId: admission.run_id,
          attemptId: admission.attempt_id,
          nodeId: admission.node_id,
          messageId: admission.message_id,
          checkpointScopeId: admission.checkpoint_scope_id,
        },
        provider: null,
      };
      // This test owns a synthetic executor port; it proves SQL handoff, not DC-C issuance or retirement.
      const observedAt = DateTime.formatIso(yield* DateTime.now);
      yield* (yield* OrdinaryCheckout.OrdinaryCheckout)
        .reserve({
          admissionId: admission.admission_id,
          source,
          now: observedAt,
          binding,
          evidence: {
            version: 1,
            issuerId: "synthetic-executor",
            reference: "synthetic-claim",
            outcome: "active",
            coverage: "complete",
            observedAt,
          },
        })
        .pipe(
          Effect.provideService(OrdinaryCheckout.CheckoutExecutorAuthority, {
            assertCurrent: () => Effect.void,
            reconcile: () => Effect.void,
          }),
        );
      const run = (yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(threadId))
        .runs[0]!;
      const sink = yield* EventSink.EventSinkV2;
      const input = {
        commandId: CommandId.make("terminal-checkpoint"),
        events: [
          {
            id: EventId.make("terminal-checkpoint-event"),
            type: "run.updated" as const,
            threadId,
            occurredAt: yield* DateTime.now,
            payload: { ...run, status: "completed" as const },
          },
        ],
        effects: [
          {
            id: "terminal-checkpoint-effect",
            commandId: CommandId.make("terminal-checkpoint"),
            threadId,
            request: {
              type: "checkpoint.capture" as const,
              runId: run.id,
              scopeId: CheckpointScopeId.make("synthetic-scope"),
            },
          },
        ],
      };
      const write =
        method === "writeWithEffects"
          ? sink.writeWithEffects(input).pipe(Effect.asVoid)
          : Effect.gen(function* () {
              if (run.activeAttemptId === null)
                return yield* Effect.die(
                  "Synthetic run requires an attempt for current-run hook coverage",
                );
              return yield* sink
                .writeIfRunCurrent({
                  ...input,
                  threadId,
                  runId: run.id,
                  activeAttemptId: run.activeAttemptId,
                  expectedStatus: run.status,
                })
                .pipe(Effect.asVoid);
            });
      const before = yield* state;
      const published = yield* Ref.get((yield* ObservationState).published);
      yield* sql`CREATE TRIGGER synthetic_derived_failure AFTER INSERT ON jones_ordinary_checkout_effect_links BEGIN SELECT RAISE(ABORT, 'synthetic derived failure'); END`;
      assert.isTrue(Exit.isFailure(yield* write.pipe(Effect.exit)));
      assert.deepEqual(yield* state, before);
      assert.equal(yield* Ref.get((yield* ObservationState).published), published);
      yield* sql`DROP TRIGGER synthetic_derived_failure`;
      yield* write;
      const after = yield* state;
      assert.lengthOf(after.links, before.links.length + 1);
      assert.lengthOf(after.receipts, before.receipts.length);
      assert.equal(
        (yield* sql<{
          status: string;
        }>`SELECT status FROM orchestration_v2_projection_runs WHERE run_id=${run.id}`)[0]!.status,
        "completed",
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "imported-history acceptance commits its choice and ordinary admission in the same transaction",
  () =>
    Effect.gen(function* () {
      yield* ImportedFixture.setup;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      yield* sink.write({
        events: [
          {
            id: EventId.make("ordinary-import-attempt"),
            type: "run-attempt.created",
            threadId: ImportedFixture.threadId,
            occurredAt: ImportedFixture.now,
            payload: {
              id: ImportedFixture.run.activeAttemptId,
              runId: ImportedFixture.runId,
              attemptOrdinal: 1,
              rootNodeId: ImportedFixture.run.rootNodeId,
              providerInstanceId: instanceId,
              providerThreadId: ImportedFixture.providerThreadId,
              providerTurnId: null,
              reason: "initial",
              status: "pending",
              startedAt: null,
              completedAt: null,
            },
          },
          {
            id: EventId.make("ordinary-import-node"),
            type: "node.updated",
            threadId: ImportedFixture.threadId,
            occurredAt: ImportedFixture.now,
            payload: {
              id: ImportedFixture.run.rootNodeId,
              threadId: ImportedFixture.threadId,
              runId: ImportedFixture.runId,
              parentNodeId: null,
              rootNodeId: ImportedFixture.run.rootNodeId,
              kind: "root_turn",
              status: "pending",
              countsForRun: true,
              providerThreadId: ImportedFixture.providerThreadId,
              providerTurnId: null,
              nativeItemRef: null,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: null,
              completedAt: null,
            },
          },
        ],
      });
      const review = yield* sink.reviewImportedHistory!({
        threadId: ImportedFixture.threadId,
        delivery: ImportedFixture.delivery,
        principal: ImportedFixture.principal,
        targetCapabilities: ImportedFixture.capabilities,
      });
      assert.equal(review.status, "available");
      const importedCommand = {
        type: "thread.imported-history.start" as const,
        commandId: CommandId.make("ordinary-import"),
        threadId: ImportedFixture.threadId,
        delivery: ImportedFixture.delivery,
        reviewedBasis: review.reviewedBasis!,
      };
      const input = {
        commandId: importedCommand.commandId,
        threadId: importedCommand.threadId,
        commandType: importedCommand.type,
        acceptedAt: ImportedFixture.now,
        importedHistory: {
          command: importedCommand,
          principal: ImportedFixture.principal,
          targetCapabilities: ImportedFixture.capabilities,
        },
        events: [
          {
            id: EventId.make("ordinary-import-start"),
            type: "run.updated" as const,
            threadId: importedCommand.threadId,
            occurredAt: ImportedFixture.now,
            payload: { ...ImportedFixture.run, status: "starting" as const, queueHeld: false },
          },
        ],
        effects: [
          {
            id: "ordinary-import-effect",
            commandId: importedCommand.commandId,
            threadId: importedCommand.threadId,
            request: { type: "provider-turn.start" as const, runId: ImportedFixture.runId },
          },
        ],
      };
      const before = yield* state;
      yield* sql`CREATE TRIGGER synthetic_import_failure BEFORE INSERT ON jones_ordinary_checkout_effect_links BEGIN SELECT RAISE(ABORT, 'synthetic import failure'); END`;
      assert.isTrue(Exit.isFailure(yield* sink.commitCommand(input).pipe(Effect.exit)));
      assert.deepEqual(yield* state, before);
      assert.lengthOf(yield* sql`SELECT * FROM jones_imported_history_choices`, 0);
      assert.lengthOf(yield* sql`SELECT * FROM jones_imported_history_outcomes`, 0);
      yield* sql`DROP TRIGGER synthetic_import_failure`;
      const accepted = yield* sink.commitCommand(input);
      assert.equal(accepted.receipt.status, "accepted");
      assert.lengthOf((yield* state).admissions, 1);
      assert.lengthOf(yield* sql`SELECT * FROM jones_imported_history_outcomes`, 1);
      yield* Ref.set((yield* ObservationState).unavailable, true);
      assert.isFalse((yield* sink.commitCommand(input)).committed);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "a pending same-owner admission fences a second command without changing either owner or upstream state",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* seed;
      yield* orchestrator.dispatch(command);
      const before = yield* state;
      assert.isTrue(
        Exit.isFailure(
          yield* orchestrator
            .dispatch({
              ...command,
              commandId: CommandId.make("second-command"),
              messageId: MessageId.make("second-message"),
              dispatchMode: { type: "queue_after_active" },
            })
            .pipe(Effect.exit),
        ),
      );
      assert.deepEqual(yield* state, before);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("transaction authority refuses acceptance outside EventSink's SQL transaction", () =>
  Effect.gen(function* () {
    yield* seed;
    const service = yield* CheckoutIntent.CheckoutIntent;
    const prepared = yield* service.prepare({ command, threadId, events: [], effects: [] });
    assert.isTrue(Exit.isFailure(yield* service.validate(prepared).pipe(Effect.exit)));
  }).pipe(Effect.provide(testLayer)),
);

it.effect("a parent receipt atomically admits a newly born delegated child subject", () =>
  Effect.gen(function* () {
    const orchestrator = yield* seed;
    yield* orchestrator.dispatch(command);
    const sql = yield* SqlClient.SqlClient;
    const projection = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
      threadId,
    );
    const parentRun = projection.runs[0]!;
    const childId = ThreadId.make("delegated-child");
    const childProject = ProjectId.make("delegated-project");
    const childProviderId = ProviderThreadId.make("delegated-provider-thread");
    const childRunId = RunId.make("delegated-run");
    const childMessageId = MessageId.make("delegated-message");
    const at = yield* DateTime.now;
    yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES(${childProject},'Synthetic child','/delegated','[]',${now},${now})`;
    const delegated = {
      type: "delegated_task.request" as const,
      commandId: CommandId.make("delegated-request"),
      parentThreadId: threadId,
      parentRunId: parentRun.id,
      parentNodeId: parentRun.rootNodeId!,
      task: "Synthetic child task",
      modelSelection,
      runtimeMode: "full-access" as const,
      interactionMode: "default" as const,
      createdBy: "agent" as const,
      creationSource: "mcp" as const,
    };
    const child = {
      ...projection.thread,
      id: childId,
      projectId: childProject,
      activeProviderThreadId: null,
      lineage: {
        parentThreadId: threadId,
        relationshipToParent: "subagent" as const,
        rootThreadId: threadId,
      },
      createdAt: at,
      updatedAt: at,
    };
    const run = {
      ...parentRun,
      id: childRunId,
      providerThreadId: childProviderId,
      threadId: childId,
      rootNodeId: null,
      activeAttemptId: null,
      userMessageId: childMessageId,
    };
    const input = {
      commandId: delegated.commandId,
      threadId,
      commandType: delegated.type,
      ordinaryCheckoutCommand: delegated,
      acceptedAt: at,
      events: [
        {
          id: EventId.make("delegated-birth"),
          type: "thread.created" as const,
          threadId: childId,
          occurredAt: at,
          payload: child,
        },
        {
          id: EventId.make("delegated-provider-birth"),
          type: "provider-thread.updated" as const,
          threadId: childId,
          occurredAt: at,
          payload: {
            ...projection.providerThreads[0]!,
            id: childProviderId,
            appThreadId: childId,
            providerSessionId: null,
            ownerNodeId: null,
            nativeThreadRef: null,
            nativeConversationHeadRef: null,
            createdAt: at,
            updatedAt: at,
          },
        },
        {
          id: EventId.make("delegated-message-birth"),
          type: "message.updated" as const,
          threadId: childId,
          occurredAt: at,
          payload: {
            ...projection.messages[0]!,
            id: childMessageId,
            threadId: childId,
            runId: childRunId,
            nodeId: null,
            createdAt: at,
            updatedAt: at,
          },
        },
        {
          id: EventId.make("delegated-run-birth"),
          type: "run.created" as const,
          threadId: childId,
          occurredAt: at,
          payload: run,
        },
      ],
      effects: [
        {
          id: "delegated-effect",
          commandId: delegated.commandId,
          threadId: childId,
          request: { type: "provider-turn.start" as const, runId: childRunId },
        },
      ],
    };
    const sink = yield* EventSink.EventSinkV2;
    const before = yield* state;
    yield* sql`CREATE TRIGGER synthetic_child_failure BEFORE INSERT ON jones_ordinary_checkout_effect_links WHEN NEW.effect_id='delegated-effect' BEGIN SELECT RAISE(ABORT, 'synthetic child failure'); END`;
    assert.isTrue(Exit.isFailure(yield* sink.commitCommand(input).pipe(Effect.exit)));
    assert.deepEqual(yield* state, before);
    yield* sql`DROP TRIGGER synthetic_child_failure`;
    const accepted = yield* sink.commitCommand(input);
    const admissions =
      yield* sql<Admission>`SELECT * FROM jones_ordinary_checkout_admissions WHERE command_id=${delegated.commandId}`;
    assert.lengthOf(admissions, 1);
    assert.equal(admissions[0]!.receipt_thread_id, threadId);
    assert.equal(admissions[0]!.subject_thread_id, childId);
    assert.equal(admissions[0]!.origin_kind, "delegated");
    assert.equal(admissions[0]!.birth_sequence, accepted.storedEvents[0]!.sequence);
    const after = yield* state;
    yield* Ref.set((yield* ObservationState).unavailable, true);
    assert.isFalse((yield* sink.commitCommand(input)).committed);
    assert.deepEqual(yield* state, after);
  }).pipe(Effect.provide(testLayer)),
);
