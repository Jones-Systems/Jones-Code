import { CheckoutTargetObservation } from "../ordinaryCheckout/CheckoutTarget.ts";
import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  NodeId,
  OrchestrationV2AppThreadJson,
  OrchestrationV2DomainEventJson,
  OrchestrationV2SubagentJson,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RawEventId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as Projections from "../../orchestration-v2/ProjectionStore.ts";
import * as Ingestor from "../../orchestration-v2/ProviderEventIngestor.ts";
import * as ThreadCommands from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Instances from "../../provider/ProviderInstanceRegistry.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { pruneEventLogLikeUpstream } from "../persistence/prunedEventLog.testkit.ts";
import {
  captureDeletionGroupPhysical,
  makeDeletionGroupEvidence,
  type DeletionGroupPhysicalV1,
} from "./DeletionGroupWitness.ts";

const database = Sqlite.layerMemory;
const stores = Layer.mergeAll(EventStore.layer, Projections.layer).pipe(
  Layer.provideMerge(database),
);
const sinkLayer = EventSink.layer.pipe(Layer.provideMerge(stores));
const dependencies = Layer.mergeAll(
  sinkLayer,
  IdAllocator.layer,
  ThreadCommands.layer,
  Layer.mock(Instances.ProviderInstanceRegistry)({
    getInstance: () => Effect.sync(() => undefined),
    getCatalogSource: () => undefined,
  }),
  Layer.mock(Git.GitVcsDriver)({
    execute: () => Effect.die("Native identity fixture must not invoke Git"),
  }),
).pipe(Layer.provideMerge(NodeServices.layer), Layer.provideMerge(NodeCrypto.layer));
const layer = Ingestor.layer.pipe(Layer.provideMerge(dependencies));
const at = "2026-10-07T00:00:00.000Z";
const root = ThreadId.make("group:root");
const child = ThreadId.make("group:child");
const projectId = ProjectId.make("group:project");
const target = {
  projectId,
  projectRoot: "/synthetic/project",
  path: "/synthetic/worktree",
  branch: "native",
  force: false,
};
const directoryIdentity = { version: 1 as const, device: "1", inode: "2", birth: "3" };
const physical: DeletionGroupPhysicalV1 = {
  directory: { realPath: target.path, identity: directoryIdentity },
  project: { realPath: target.projectRoot, identity: { ...directoryIdentity, inode: "4" } },
  gitCommonDirectory: "/synthetic/project/.git",
  gitDirectory: "/synthetic/project/.git/worktrees/native",
  gitCommonIdentity: { ...directoryIdentity, inode: "5" },
  gitDirectoryIdentity: { ...directoryIdentity, inode: "6" },
  head: "a".repeat(40),
  branch: "refs/heads/native",
};
const selection = { instanceId: "codex", model: "synthetic" };
const appThread = (id: ThreadId, parent: ThreadId | null) => ({
  id,
  projectId,
  createdBy: parent === null ? "user" : "agent",
  creationSource: parent === null ? "web" : "provider",
  title: "Synthetic native group",
  providerInstanceId: "codex",
  modelSelection: selection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: target.branch,
  worktreePath: target.path,
  activeProviderThreadId: parent === null ? "native:parent" : null,
  lineage: {
    parentThreadId: parent,
    relationshipToParent: parent === null ? null : "subagent",
    rootThreadId: root,
  },
  forkedFrom: parent === null ? null : { type: "node", nodeId: "native:task" },
  createdAt: at,
  updatedAt: at,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
});
const decodeOrchestrationV2DomainEventJson = Schema.decodeUnknownEffect(
  OrchestrationV2DomainEventJson,
);

const event = (type: string, payload: unknown, threadId = root) =>
  decodeOrchestrationV2DomainEventJson({
    id: `group:${type}:${threadId}`,
    threadId,
    type,
    occurredAt: at,
    payload,
  });
const decodeOrchestrationV2AppThreadJson = Schema.decodeUnknownEffect(OrchestrationV2AppThreadJson);
const decodeOrchestrationV2SubagentJson = Schema.decodeEffect(OrchestrationV2SubagentJson);

const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* EventSink.EventSinkV2;
  const ingestor = yield* Ingestor.ProviderEventIngestorV2;
  const owner = yield* decodeOrchestrationV2AppThreadJson(appThread(root, null));
  yield* sink.write({ events: [yield* event("thread.created", appThread(root, null))] });
  const providerSessionId = ProviderSessionId.make("native:session");
  let serial = 0;
  const native = (providerEvent: Ingestor.ProviderEventIngestInput["event"]) =>
    ingestor.ingestNormalized({
      providerSessionId,
      providerInstanceId: ProviderInstanceId.make("codex"),
      threadId: root,
      runId: RunId.make("native:run"),
      nodeId: NodeId.make("native:root-node"),
      rawEventId: RawEventId.make(`native:raw:${++serial}`),
      event: providerEvent,
    });
  const driver = ProviderDriverKind.make("codex");
  const session = {
    id: providerSessionId,
    driver,
    providerInstanceId: ProviderInstanceId.make("codex"),
    status: "ready" as const,
    cwd: target.path,
    model: null,
    capabilities: CodexProviderCapabilitiesV2,
    createdAt: DateTime.makeUnsafe(at),
    updatedAt: DateTime.makeUnsafe(at),
    lastError: null,
  };
  yield* native({ type: "provider_session.updated", driver, providerSession: session });
  yield* sink.write({
    events: [
      yield* event("provider-thread.updated", {
        id: "native:parent",
        driver,
        providerInstanceId: "codex",
        providerSessionId,
        appThreadId: root,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "native-original", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [],
        createdAt: at,
        updatedAt: at,
      }),
      yield* event("run.created", {
        id: "native:run",
        threadId: root,
        ordinal: 1,
        providerInstanceId: "codex",
        modelSelection: selection,
        providerThreadId: "native:parent",
        userMessageId: "native:message",
        rootNodeId: "native:root-node",
        activeAttemptId: "native:attempt",
        status: "completed",
        requestedAt: at,
        startedAt: at,
        completedAt: at,
        checkpointId: null,
        contextHandoffId: null,
      }),
      yield* event("run-attempt.created", {
        id: "native:attempt",
        runId: "native:run",
        attemptOrdinal: 1,
        rootNodeId: "native:root-node",
        providerInstanceId: "codex",
        providerThreadId: "native:parent",
        providerTurnId: "native:turn",
        reason: "initial",
        status: "completed",
        startedAt: at,
        completedAt: at,
        providerSettlement: {
          runAttemptId: "native:attempt",
          providerTurnId: "native:turn",
          status: "completed",
          completedAt: at,
        },
      }),
      yield* event("node.updated", {
        id: "native:root-node",
        threadId: root,
        runId: "native:run",
        parentNodeId: null,
        rootNodeId: "native:root-node",
        kind: "root_turn",
        status: "completed",
        countsForRun: true,
        providerThreadId: "native:parent",
        providerTurnId: "native:turn",
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: at,
        completedAt: at,
      }),
    ],
  });
  const subagent = yield* decodeOrchestrationV2SubagentJson({
    id: "native:task",
    threadId: root,
    runId: "native:run",
    parentNodeId: "native:root-node",
    origin: "provider_native",
    createdBy: "agent",
    driver,
    providerInstanceId: "codex",
    providerThreadId: null,
    childThreadId: child,
    nativeTaskRef: { driver, nativeId: "task", strength: "strong" },
    prompt: "Synthetic native child",
    title: null,
    model: null,
    status: "completed",
    result: "done",
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  });
  yield* native({ type: "subagent.updated", driver, subagent });
  yield* native({
    type: "app_thread.created",
    driver,
    appThread: yield* decodeOrchestrationV2AppThreadJson(appThread(child, root)),
  });
  const deleted = { ...owner, deletedAt: DateTime.makeUnsafe(at) };
  const deletion = yield* sink.commitCommand({
    commandId: CommandId.make("group:delete"),
    threadId: root,
    commandType: "thread.delete",
    acceptedAt: DateTime.makeUnsafe(at),
    events: [
      {
        id: EventId.make("group:deletion"),
        type: "thread.deleted",
        threadId: root,
        occurredAt: DateTime.makeUnsafe(at),
        payload: deleted,
      },
    ],
    effects: [],
  });
  const evidence = yield* makeDeletionGroupEvidence(sql);
  const input = {
    ownerThreadId: root,
    deletionEventId: "group:deletion",
    deletionSequence: deletion.receipt.resultSequence,
    target,
    physical,
  };
  const stop = native({
    type: "provider_session.updated",
    driver,
    providerSession: { ...session, status: "stopped" },
  });
  return { sql, sink, native, session, owner, evidence, input, stop };
});

it.effect(
  "requires the actual persisted session stop before capturing an authenticated original native group",
  () =>
    Effect.gen(function* () {
      const { evidence, input, stop, sql } = yield* setup;
      const live = yield* evidence.capture(input).pipe(Effect.flip);
      assert.strictEqual(live.reason, "original_group_session_not_stopped");
      yield* stop;
      const captured = yield* evidence.capture(input);
      assert.deepEqual(
        captured.members.map((member) => member.threadId),
        [child, root],
      );
      yield* sql.withTransaction(evidence.validate(captured));
      assert.deepEqual(
        yield* sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL`,
        [{ thread_id: child }],
      );
    }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect(
  "admits the versioned completed-native policy only after the real shared-DB session stops",
  () =>
    Effect.gen(function* () {
      const { sink, stop, sql } = yield* setup;
      yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES(${projectId},'Synthetic completed cleanup',${target.projectRoot},'[]',${at},${at})`;
      const rules = {
        worktreeSettledScope: "branch-retained" as const,
        worktreeCoveredDiscard: [],
        worktreeKeepWhen: "uncommitted-changes" as const,
        worktreeAfterDays: null,
        worktreeSettledAfterDays: null,
        worktreeOnDelete: true,
        worktreeOnMerge: false,
        worktreeUnchanged: false,
      };
      const input = {
        threadId: root,
        target,
        rules,
        currentRules: Effect.succeed(rules),
        currentLive: Effect.succeed(true),
      };
      const start = sink.startDeletionWorktreeRemoval;
      if (start === undefined) return yield* Effect.die("Missing actual deletion admission owner");
      const live = yield* start(input).pipe(Effect.result);
      assert.strictEqual(live._tag, "Failure");
      assert.deepStrictEqual(
        yield* sql`SELECT effect_id FROM jones_deletion_worktree_admissions`,
        [],
      );
      yield* stop;
      const admitted = yield* start(input);
      assert.strictEqual(admitted.status, "start_now");
      const rows = yield* sql<{
        binding_json: string;
      }>`SELECT binding_json FROM jones_deletion_worktree_admissions`;
      const binding = JSON.parse(rows[0]!.binding_json);
      assert.strictEqual(binding.schema, "jones.deletion-admission/v3");
      assert.deepStrictEqual(
        binding.completedNativeMembers.map((member: { threadId: string }) => member.threadId),
        [child],
      );
      assert.strictEqual(admitted.start.target.force, false);
      const repeat = yield* start(input);
      assert.strictEqual(repeat.status, "observe_only");
      assert.deepStrictEqual(repeat.start, admitted.start);
    }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect("rejects forged native metadata and preserves the original admitted witness", () =>
  Effect.gen(function* () {
    const { evidence, input, stop, sql } = yield* setup;
    yield* stop;
    const captured = yield* evidence.capture(input);
    const original = JSON.stringify(captured);
    yield* sql`UPDATE orchestration_events SET metadata_json='{}' WHERE event_type='subagent.updated'`;
    const invalid = yield* evidence.validate(captured).pipe(Effect.result);
    assert.strictEqual(invalid._tag, "Failure");
    assert.strictEqual(JSON.stringify(captured), original);
  }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect.each(["lineage", "pending-child", "active-turn"] as const)(
  "the completed-native policy refuses a later %s change without changing its original admission",
  (change) =>
    Effect.gen(function* () {
      const { sink, stop, sql } = yield* setup;
      yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES(${projectId},'Synthetic completed cleanup',${target.projectRoot},'[]',${at},${at})`;
      yield* stop;
      const rules = {
        worktreeSettledScope: "branch-retained" as const,
        worktreeCoveredDiscard: [],
        worktreeKeepWhen: "uncommitted-changes" as const,
        worktreeAfterDays: null,
        worktreeSettledAfterDays: null,
        worktreeOnDelete: true,
        worktreeOnMerge: false,
        worktreeUnchanged: false,
      };
      if (
        sink.startDeletionWorktreeRemoval === undefined ||
        sink.revalidateDeletionWorktreeRemovalStart === undefined
      )
        return yield* Effect.die("Missing actual deletion admission owner");
      const start = yield* sink.startDeletionWorktreeRemoval({
        threadId: root,
        target,
        rules,
        currentRules: Effect.succeed(rules),
        currentLive: Effect.succeed(true),
      });
      const original =
        yield* sql`SELECT binding_json,start_json FROM jones_deletion_worktree_admissions`;
      if (change === "lineage")
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.lineage.parentThreadId','foreign:parent') WHERE thread_id=${child}`;
      else if (change === "pending-child")
        yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,created_at,updated_at) VALUES('cleanup-policy:pending','group:delete',${child},'terminal.cleanup','{"type":"terminal.cleanup"}','pending',0,${at},${at},${at})`;
      else
        yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json=json_set(payload_json,'$.activeTurnId','foreign:turn') WHERE provider_thread_id='native:parent'`;
      const refused = yield* sink
        .revalidateDeletionWorktreeRemovalStart(
          start.start,
          0,
          Effect.succeed(rules),
          Effect.succeed(true),
        )
        .pipe(Effect.result);
      assert.strictEqual(refused._tag, "Failure");
      assert.deepStrictEqual(
        yield* sql`SELECT binding_json,start_json FROM jones_deletion_worktree_admissions`,
        original,
      );
      assert.deepStrictEqual(yield* sql`SELECT state FROM jones_deletion_worktree_admissions`, [
        { state: "started" },
      ]);
      assert.deepStrictEqual(yield* sql`SELECT * FROM jones_deletion_worktree_observations`, []);
    }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect(
  "rejects new overlapping members and changed child lineage instead of inferring new membership",
  () =>
    Effect.gen(function* () {
      const { evidence, input, stop, sql, sink } = yield* setup;
      yield* stop;
      const captured = yield* evidence.capture(input);
      yield* sink.write({
        events: [
          yield* event(
            "thread.created",
            appThread(ThreadId.make("new:unrelated"), null),
            ThreadId.make("new:unrelated"),
          ),
        ],
      });
      const overlap = yield* evidence.validate(captured).pipe(Effect.flip);
      assert.strictEqual(overlap.reason, "original_group_membership_changed");
      yield* sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id='new:unrelated'`;
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.forkedFrom.nodeId','replacement') WHERE thread_id=${child}`;
      assert.strictEqual(
        (yield* evidence.validate(captured).pipe(Effect.flip)).reason,
        "original_group_member_identity_changed",
      );
    }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect("does not make physical observations inside an existing SQL transaction", () =>
  Effect.gen(function* () {
    const { sql } = yield* setup;
    let physicalReads = 0;
    const unexpected = () =>
      Effect.sync(() => {
        physicalReads++;
        throw new Error("No physical read inside SQL");
      });
    const refused = yield* sql.withTransaction(captureDeletionGroupPhysical(target)).pipe(
      Effect.provideService(CheckoutTargetObservation, {
        directory: unexpected,
        git: unexpected,
      }),
      Effect.flip,
    );
    assert.strictEqual(physicalReads, 0);
    assert.strictEqual(refused.reason, "group_physical_observation_inside_transaction");
  }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect(
  "a stopped projection cannot be replaced by a ready or missing session during admission",
  () =>
    Effect.gen(function* () {
      const { evidence, input, stop, sql } = yield* setup;
      yield* stop;
      const captured = yield* evidence.capture(input);
      yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET status='ready' WHERE provider_session_id='native:session'`;
      assert.strictEqual(
        (yield* evidence.validate(captured).pipe(Effect.flip)).reason,
        "original_group_session_not_stopped",
      );
      yield* sql`DELETE FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id='native:session'`;
      assert.strictEqual(
        (yield* evidence.validate(captured).pipe(Effect.flip)).reason,
        "original_group_session_missing",
      );
    }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect("pending, held and unknown run or attempt evidence always retains the checkout", () =>
  Effect.gen(function* () {
    const { evidence, input, stop, sql } = yield* setup;
    yield* stop;
    const captured = yield* evidence.capture(input);
    const runs = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id='native:run'`;
    for (const status of ["queued", "preparing", "starting", "running", "waiting", "unknown"]) {
      yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json=json_set(${runs[0]!.payload_json},'$.status',${status}) WHERE run_id='native:run'`;
      assert.strictEqual((yield* evidence.validate(captured).pipe(Effect.result))._tag, "Failure");
    }
    yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json=json_set(${runs[0]!.payload_json},'$.queueHeld',json('true')) WHERE run_id='native:run'`;
    assert.strictEqual(
      (yield* evidence.validate(captured).pipe(Effect.flip)).reason,
      "original_group_run_unsettled",
    );
    yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json=${runs[0]!.payload_json} WHERE run_id='native:run'`;
    yield* sql`UPDATE orchestration_v2_projection_run_attempts SET payload_json=json_set(payload_json,'$.providerSettlement',NULL) WHERE attempt_id='native:attempt'`;
    assert.strictEqual(
      (yield* evidence.validate(captured).pipe(Effect.flip)).reason,
      "original_group_provider_settlement_unavailable",
    );
  }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect(
  "records execution generations and rejects intervening accepted history even with an equal shell",
  () =>
    Effect.gen(function* () {
      const { evidence, input, stop, sink } = yield* setup;
      yield* stop;
      const captured = yield* evidence.capture(input);
      yield* sink.write({
        events: [yield* event("thread.metadata-updated", appThread(child, root), child)],
      });
      assert.strictEqual(
        (yield* evidence.validate(captured).pipe(Effect.flip)).reason,
        "original_group_generation_changed",
      );
    }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect("rows the upstream pruner removes fail the witness closed, never shrink the group", () =>
  Effect.gen(function* () {
    const { evidence, input, stop } = yield* setup;
    yield* stop;
    const captured = yield* evidence.capture(input);
    assert.isAbove((yield* pruneEventLogLikeUpstream(input.deletionSequence)).length, 0);
    assert.strictEqual(
      (yield* evidence.validate(captured).pipe(Effect.flip)).reason,
      "original_group_deletion_boundary_unavailable",
    );
  }).pipe(Effect.provide(layer), Effect.scoped),
);

it.effect("app-owned or side-chat lineage never authenticates as an original native group", () =>
  Effect.gen(function* () {
    const { evidence, input, stop, sql } = yield* setup;
    yield* stop;
    const original = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_events WHERE event_type='subagent.updated'`;
    yield* sql`UPDATE orchestration_events SET payload_json=json_set(payload_json,'$.origin','app_owned') WHERE event_type='subagent.updated'`;
    assert.strictEqual((yield* evidence.capture(input).pipe(Effect.result))._tag, "Failure");
    yield* sql`UPDATE orchestration_events SET payload_json=${original[0]!.payload_json} WHERE event_type='subagent.updated'`;
    yield* sql`UPDATE orchestration_events SET payload_json=json_set(payload_json,'$.lineage.sideChat',json('true')) WHERE event_type='thread.created' AND stream_id=${child}`;
    assert.strictEqual(
      (yield* evidence.capture(input).pipe(Effect.flip)).reason,
      "original_group_native_parent_unavailable",
    );
  }).pipe(Effect.provide(layer), Effect.scoped),
);
