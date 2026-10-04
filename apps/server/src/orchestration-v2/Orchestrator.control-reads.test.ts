import * as NodeCrypto from "node:crypto";
import conformance from "../../../../packages/contracts/contracts/workstreams-t3-provider/v1/fixtures/conformance.json" with { type: "json" };
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  AuthSessionId,
  CheckpointId,
  CheckpointRef,
  CommandId,
  EnvironmentAuthenticatedPrincipal,
  MessageId,
  EventId,
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
  TurnItemId,
  WorkstreamsNativeContext,
  WorkstreamsNativeSettlementRequest,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { TestClock } from "effect/testing";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { layer as nativeRepositoryLayer } from "../persistence/Layers/NativeCreationRepository.ts";
import { NATIVE_PROVIDER_SCOPES } from "../workstreams/nativeProvider/enrollment.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as EventSink from "./EventSink.ts";
import { NativeCreationAuthority } from "./NativeCreationAuthority.ts";
import { ordinaryCheckoutAdmissionRefV1 } from "./OrdinaryCheckoutOwnership.ts";
import {
  makeWorktreeOwnershipLeaseStore,
  WorktreeOwnershipLease,
} from "./WorktreeOwnershipLease.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";

class CheckoutFixtureFailure extends Schema.TaggedError<CheckoutFixtureFailure>()(
  "CheckoutFixtureFailure",
  { message: Schema.String, cause: Schema.Defect() },
) {}
const fixtureFailure = (message: string) =>
  new CheckoutFixtureFailure({ message, cause: new Error(message) });

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for metadata controls"),
} as ProviderAdapterV2Shape;
const database = SqlitePersistenceMemory;
const nativeRepository = nativeRepositoryLayer.pipe(Layer.provide(database));
const nativeAuthority = Layer.mock(NativeCreationAuthority)({
  authorize: () => Effect.die("Ordinary controls must not invoke native creation authority"),
  isAutomationEnrolled: () => Effect.die("Ordinary controls must not inspect native enrollment"),
  issueExecution: () => Effect.die("The disabled control worker must not issue native execution"),
  authorizeExecution: () => Effect.die("Ordinary controls must not authorize native execution"),
});
const testLayer = Layer.mergeAll(
  database,
  nativeRepository,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  ProjectStore.layer.pipe(Layer.provide(database)),
  makeOrchestratorV2ReplayLayerWithRegistry(
    { name: "control-reads" },
    ProviderAdapterRegistry.makeLayer([adapter]),
    { databaseLayer: database, runEffectWorker: false },
  ).pipe(Layer.provide(Layer.mergeAll(database, nativeRepository, nativeAuthority))),
);

const seedOrdinaryProject = (projectId: ProjectId) =>
  Effect.gen(function* () {
    const projects = yield* ProjectStore.ProjectStoreV2;
    const now = DateTime.formatIso(yield* DateTime.now);
    yield* projects.apply({
      sequence: 0,
      eventId: EventId.make(`created:${projectId}`),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: now,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId,
        title: "Ordinary control fixture",
        workspaceRoot: `/__t3_control_reads_fixture__/${projectId}`,
        defaultModelSelection: modelSelection,
        scripts: [],
        createdAt: now,
        updatedAt: now,
      },
    });
  });

const nativeSettlementInput = (): Orchestrator.NativeWorkstreamSettlementInputV2 => {
  const fixture = (name: string) => conformance.cases.find((entry) => entry.name === name)?.value;
  const context = Schema.decodeUnknownSync(WorkstreamsNativeContext)(fixture("Context"));
  const request = Schema.decodeUnknownSync(WorkstreamsNativeSettlementRequest)(
    fixture("SettlementRequest"),
  );
  const enrollment = {
    ...context,
    session_id: "session-synthetic",
    registry_origin: "https://registry.invalid",
    scopes: Object.values(NATIVE_PROVIDER_SCOPES),
  };
  const digest = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
  const contextJson = Schema.encodeSync(Schema.fromJsonString(WorkstreamsNativeContext))(context);
  const requestJson = Schema.encodeSync(Schema.fromJsonString(WorkstreamsNativeSettlementRequest))(
    request,
  );
  const enrollmentSha256 = digest(
    `${enrollment.registry_origin}\n${enrollment.session_id}\n${contextJson}`,
  );
  return {
    enrollment,
    request,
    attempt: {
      enrollment,
      request,
      requestBytesSha256: digest(requestJson),
      enrollmentSha256,
      nativeCommandId: `workstreams:${digest(`${enrollmentSha256}\n${requestJson}`)}`,
      createdAt: "2026-10-03T00:00:00.000Z",
      dispatchStartedAt: "2026-10-03T00:00:01.000Z",
    },
  };
};

it.effect(
  "reorders a pinned active thread without changing pin placement, snooze, settlement or project scope",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      for (const snoozed of [false, true]) {
        const name = snoozed ? "pinned-snoozed" : "pinned";
        const threadId = ThreadId.make(`thread:active-reorder:${name}`);
        const otherThreadId = ThreadId.make(`thread:active-reorder:${name}:other-project`);
        for (const id of [threadId, otherThreadId]) {
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create:${id}`),
            threadId: id,
            projectId: ProjectId.make(`project:${id}`),
            title: name,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdBy: "user",
            creationSource: "web",
          });
        }
        yield* orchestrator.dispatch({
          type: "thread.pin",
          commandId: CommandId.make(`pin:${threadId}`),
          threadId,
          orderKey: "s",
        });
        if (snoozed) {
          const existing = yield* projections.getThread(threadId);
          const now = yield* DateTime.now;
          yield* projections.apply({
            id: EventId.make(`snoozed-pinned-state:${threadId}`),
            type: "thread.snoozed",
            threadId,
            occurredAt: now,
            payload: { ...existing, snoozedAt: now, snoozedUntil: DateTime.add(now, { days: 1 }) },
          });
        }
        const before = yield* projections.getThread(threadId);
        const otherBefore = yield* projections.getThread(otherThreadId);
        assert.isNotNull(before.pinnedAt);
        assert.equal(before.pinOrderKey, "s");
        assert.notEqual(before.settledOverride, "settled");
        yield* orchestrator.dispatch({
          type: "thread.active.reorder",
          commandId: CommandId.make(`reorder:${threadId}`),
          threadId,
          orderKey: "m",
        });
        const after = yield* projections.getThread(threadId);
        assert.deepEqual(after, { ...before, activeOrderKey: "m" });
        assert.deepEqual(yield* projections.getThread(otherThreadId), otherBefore);
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect("settling clears snooze and pin placement while unsettle preserves parked metadata", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    for (const action of ["settle", "unsettle"] as const) {
      const threadId = ThreadId.make(`thread:parked-metadata:${action}`);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`create:${threadId}`),
        threadId,
        projectId: ProjectId.make(`project:${threadId}`),
        title: "Parked metadata",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "thread.pin",
        commandId: CommandId.make(`pin:${threadId}`),
        threadId,
        orderKey: "s",
      });
      const pinned = yield* projections.getThread(threadId);
      const now = yield* DateTime.now;
      yield* projections.apply({
        id: EventId.make(`snoozed:${threadId}`),
        type: "thread.snoozed",
        threadId,
        occurredAt: now,
        payload: { ...pinned, snoozedAt: now, snoozedUntil: DateTime.add(now, { days: 1 }) },
      });
      const before = yield* projections.getThread(threadId);
      assert.isNotNull(before.snoozedAt);
      assert.isNotNull(before.snoozedUntil);
      yield* orchestrator.dispatch({
        ...(action === "settle"
          ? { type: "thread.settle" as const }
          : { type: "thread.unsettle" as const, reason: "user" as const }),
        commandId: CommandId.make(`${action}:${threadId}`),
        threadId,
      });
      const after = yield* projections.getThread(threadId);
      for (const field of ["snoozedAt", "snoozedUntil", "pinnedAt", "pinOrderKey"] as const) {
        assert.deepEqual(after[field], action === "settle" ? null : before[field]);
      }
      assert.equal(after.projectId, before.projectId);
      assert.equal(after.title, before.title);
      assert.deepEqual(after.modelSelection, before.modelSelection);
      assert.equal(after.settledOverride, action === "settle" ? "settled" : "active");
    }
  }).pipe(Effect.provide(testLayer)),
);

it.effect("observes an absent runtime stop without inventing a target or writing commands", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sql = yield* SqlClient.SqlClient;
    const input = {
      threadId: ThreadId.make("thread:absent-runtime-stop"),
      commandId: CommandId.make("command:absent-runtime-stop"),
    };
    const before =
      yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
    assert.deepEqual(yield* orchestrator.observeCurrentThreadRuntimeStop(input), {
      version: 2,
      ...input,
      target: null,
      commandStatus: "not_found",
      receipt: null,
      queueFence: { status: "not_installed", affectedRunIds: [] },
      runtimeStop: { status: "not_started" },
      reason: null,
    });
    assert.deepEqual(
      yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`,
      before,
    );
    assert.deepEqual(yield* sql`SELECT command_id FROM orchestration_command_receipts`, []);
    assert.deepEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, []);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "keeps native settlement unavailable without effects and rejects changed immutable association",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sql = yield* SqlClient.SqlClient;
      const input = nativeSettlementInput();
      const dispatch = (value: Orchestrator.NativeWorkstreamSettlementInputV2) =>
        orchestrator.dispatchNativeWorkstreamSettlement(value).pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, {
            sessionId: AuthSessionId.make(input.enrollment.session_id),
            subject: `workstreams-native:${input.enrollment.enrollment_id}`,
            method: "bearer-access-token",
            scopes: new Set(input.enrollment.scopes),
          }),
        );
      const unavailable = yield* Effect.flip(dispatch(input));
      assert.instanceOf(unavailable, Orchestrator.NativeWorkstreamSettlementAuthorityError);
      assert.equal(
        unavailable._tag === "NativeWorkstreamSettlementAuthorityError"
          ? unavailable.code
          : undefined,
        "authority_unavailable",
      );
      const conflict = yield* Effect.flip(
        dispatch({
          ...input,
          request: {
            ...input.request,
            native_action: input.request.native_action === "settle" ? "unsettle" : "settle",
          },
        }),
      );
      assert.instanceOf(conflict, Orchestrator.NativeWorkstreamSettlementAuthorityError);
      assert.equal(
        conflict._tag === "NativeWorkstreamSettlementAuthorityError" ? conflict.code : undefined,
        "idempotency_conflict",
      );
      const normalizedId = yield* Effect.flip(
        dispatch({
          ...input,
          attempt: { ...input.attempt, nativeCommandId: ` ${input.attempt.nativeCommandId}` },
        }),
      );
      assert.instanceOf(normalizedId, Orchestrator.NativeWorkstreamSettlementAuthorityError);
      assert.equal(
        normalizedId._tag === "NativeWorkstreamSettlementAuthorityError"
          ? normalizedId.code
          : undefined,
        "invalid_request",
      );
      const unstarted = yield* Effect.flip(
        dispatch({
          ...input,
          attempt: { ...input.attempt, dispatchStartedAt: null },
        }),
      );
      assert.instanceOf(unstarted, Orchestrator.NativeWorkstreamSettlementAuthorityError);
      assert.equal(
        unstarted._tag === "NativeWorkstreamSettlementAuthorityError" ? unstarted.code : undefined,
        "invalid_request",
      );
      const effects = yield* sql<{
        readonly count: number;
      }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox`;
      const events = yield* sql<{
        readonly count: number;
      }>`SELECT COUNT(*) AS count FROM orchestration_events`;
      assert.equal(effects[0]?.count, 0);
      assert.equal(events[0]?.count, 0);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "acquires ordinary terminal ownership for the actual V2 birth and preserves containment and foreign-owner conflicts",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projects = yield* ProjectStore.ProjectStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const now = DateTime.formatIso(yield* DateTime.now);
      const projectId = ProjectId.make("project:terminal-ownership");
      const checkout = "/__t3_terminal_ownership_fixture__/checkout";
      yield* projects.apply({
        sequence: 1,
        eventId: EventId.make("project:terminal-ownership:created"),
        aggregateKind: "project",
        aggregateId: projectId,
        occurredAt: now,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "project.created",
        payload: {
          projectId,
          title: "Terminal ownership",
          workspaceRoot: checkout,
          defaultModelSelection: modelSelection,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      const firstThread = ThreadId.make("thread:terminal-owner");
      const secondThread = ThreadId.make("thread:terminal-contender");
      for (const threadId of [firstThread, secondThread]) {
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create:${threadId}`),
          threadId,
          projectId,
          title: "Terminal thread",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
      }
      const lease = yield* orchestrator.acquireOrdinaryWorktreeOwnership(
        firstThread,
        `${checkout}/nested`,
      );
      const birth = yield* sql<{
        readonly eventId: string;
        readonly sequence: number;
      }>`SELECT event_id AS eventId, sequence FROM orchestration_events WHERE stream_id = ${firstThread} AND event_type = 'thread.created' AND application_event_version = 2`;
      assert.equal(
        lease.ownerIncarnation,
        yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))([
          "t3.orchestration-v2.thread-birth/v1",
          birth[0]?.eventId,
          birth[0]?.sequence,
        ]).pipe(Effect.orDie),
      );
      assert.deepEqual(
        yield* orchestrator.acquireOrdinaryWorktreeOwnership(firstThread, checkout),
        lease,
      );
      const outside = yield* Effect.flip(
        orchestrator.acquireOrdinaryWorktreeOwnership(firstThread, `${checkout}-sibling`),
      );
      assert.instanceOf(outside, Orchestrator.OrchestratorWorktreeOwnershipError);
      const rotated = yield* orchestrator.acquireWorktreeOwnership(firstThread, checkout);
      assert.notEqual(rotated.leaseId, lease.leaseId);
      assert.deepEqual(
        yield* orchestrator.acquireOrdinaryWorktreeOwnership(firstThread, checkout),
        rotated,
      );
      yield* orchestrator.releaseWorktreeOwnership(lease);
      assert.equal((yield* orchestrator.listWorktreeOwnershipLeases)[0]?.leaseId, rotated.leaseId);
      yield* sql`UPDATE worktree_ownership_leases SET expires_at_ms = 0 WHERE resource_path = ${checkout}`;
      const conflict = yield* Effect.flip(
        orchestrator.acquireOrdinaryWorktreeOwnership(secondThread, checkout),
      );
      assert.equal(conflict._tag, "WorktreeOwnershipConflictError");
      yield* orchestrator.releaseWorktreeOwnership(rotated);
      const next = yield* orchestrator.acquireOrdinaryWorktreeOwnership(secondThread, checkout);
      assert.equal(next.ownerThreadId, secondThread);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "captures the original deferred checkout admission and replays without rotating ownership",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:ordinary-deferred-admission");
      const projectId = ProjectId.make("project:ordinary-deferred-admission");
      yield* seedOrdinaryProject(projectId);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create:ordinary-deferred-admission"),
        threadId,
        projectId,
        title: "Deferred admission",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "main",
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const lease = yield* orchestrator.acquireOrdinaryWorktreeOwnership(threadId);
      const command = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("dispatch:ordinary-deferred-admission"),
        threadId,
        messageId: MessageId.make("message:ordinary-deferred-admission"),
        text: "Prepare this exact run",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "defer_start" as const },
        createdBy: "user" as const,
        creationSource: "web" as const,
      };
      const accepted = yield* orchestrator.dispatch(command);
      const admission = yield* sink.readOrdinaryCheckoutAdmission({
        commandId: command.commandId,
        threadId,
      });
      if (admission === null)
        return yield* Effect.die("Deferred message has no original checkout admission");
      assert.deepEqual(admission.capture.lease, lease);
      assert.equal(admission.capture.commandId, command.commandId);
      assert.equal(admission.capture.canonicalCommand.text, command.text);
      assert.deepEqual(admission.capture.origin, { kind: "command" });
      assert.equal(admission.receipt.resultSequence, accepted.sequence);
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const run = projection.runs[0];
      if (run === undefined) return yield* Effect.die("Deferred message has no accepted run");
      if (run.activeAttemptId === null || run.rootNodeId === null)
        return yield* Effect.die("Deferred message has no accepted attempt and root node");
      assert.deepEqual(admission.run, {
        runId: run.id,
        runAttemptId: run.activeAttemptId,
        nodeId: run.rootNodeId,
        messageId: command.messageId,
      });
      assert.equal(
        projection.nodes.find((node) => node.id === run.rootNodeId)?.checkpointScopeId,
        null,
      );
      assert.deepEqual(
        yield* sink.readOrdinaryCheckoutAdmissionForRun({ threadId, runId: run.id }),
        admission,
      );
      const events =
        yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
      assert.equal(
        (yield* sql<{
          readonly count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE command_id = ${command.commandId}`)[0]
          ?.count,
        0,
      );
      let preparations = 0;
      assert.deepEqual(
        yield* orchestrator.dispatch(
          command,
          Effect.sync(() => {
            preparations += 1;
          }),
        ),
        accepted,
      );
      assert.equal(preparations, 0);
      assert.deepEqual(yield* orchestrator.acquireOrdinaryWorktreeOwnership(threadId), lease);
      assert.deepEqual(
        yield* sink.readOrdinaryCheckoutAdmission({ commandId: command.commandId, threadId }),
        admission,
      );
      const conflict = yield* Effect.flip(
        orchestrator.dispatch(
          { ...command, text: "Changed original intent" },
          Effect.sync(() => {
            preparations += 1;
          }),
        ),
      );
      assert.equal(conflict._tag, "DispatchGuardRejectedError");
      assert.equal(
        conflict._tag === "DispatchGuardRejectedError" ? conflict.reason : undefined,
        "identity_conflict",
      );
      assert.equal(preparations, 0);
      assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), projection);
      assert.deepEqual(
        yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`,
        events,
      );
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [lease]);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "rejects an otherwise valid provider rollback on a foreign checkout without accepting it",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("project:rollback-checkout");
      const threadId = ThreadId.make("thread:rollback-checkout");
      const ownerThreadId = ThreadId.make("thread:rollback-checkout-owner");
      yield* seedOrdinaryProject(projectId);
      for (const id of [threadId, ownerThreadId]) {
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create:${id}`),
          threadId: id,
          projectId,
          title: "Rollback checkout",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
      }
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("dispatch:rollback-checkout"),
        threadId,
        messageId: MessageId.make("message:rollback-checkout"),
        text: "Create the real rollback source",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const prepared = yield* orchestrator.getThreadProjection(threadId);
      const scope = prepared.checkpointScopes[0];
      if (scope === undefined)
        return yield* Effect.die("Rollback fixture has no accepted checkpoint scope");
      const checkpointId = CheckpointId.make("checkpoint:rollback-checkout-baseline");
      yield* sink.write({
        events: [
          {
            id: EventId.make("event:rollback-checkout-baseline"),
            type: "checkpoint.captured",
            threadId,
            occurredAt: yield* DateTime.now,
            payload: {
              id: checkpointId,
              threadId,
              scopeId: scope.id,
              runId: null,
              nodeId: scope.nodeId,
              parentCheckpointId: null,
              ordinalWithinScope: 0,
              appRunOrdinal: null,
              ref: CheckpointRef.make("refs/t3/rollback-checkout/baseline"),
              status: "ready",
              files: [],
              capturedAt: yield* DateTime.now,
            },
          },
        ],
      });
      const oldLease = (yield* orchestrator.listWorktreeOwnershipLeases).find(
        (lease) => lease.ownerThreadId === threadId,
      );
      if (oldLease === undefined)
        return yield* Effect.die("Rollback source has no original checkout lease");
      yield* orchestrator.releaseWorktreeOwnership(oldLease);
      const owner = yield* orchestrator.acquireOrdinaryWorktreeOwnership(ownerThreadId);
      const before = yield* orchestrator.getThreadProjection(threadId);
      const events =
        yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
      const effects =
        yield* sql`SELECT effect_id, command_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
      const command = {
        type: "checkpoint.rollback" as const,
        commandId: CommandId.make("rollback:foreign-checkout"),
        threadId,
        scopeId: scope.id,
        checkpointId,
        restoreFiles: false,
      };
      let preparations = 0;
      const conflict = yield* Effect.flip(
        orchestrator.dispatch(
          command,
          Effect.sync(() => {
            preparations += 1;
          }),
        ),
      );
      assert.equal(conflict._tag, "WorktreeOwnershipConflictError");
      assert.equal(preparations, 0);
      assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), before);
      assert.deepEqual(
        yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`,
        events,
      );
      assert.deepEqual(
        yield* sql`SELECT effect_id, command_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        effects,
      );
      assert.equal((yield* sink.readCommandReceiptIdentity(command.commandId)).receipt, null);
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [owner]);
      yield* orchestrator.releaseWorktreeOwnership(owner);
      const accepted = yield* orchestrator.dispatch(command);
      assert.ok(
        accepted.storedEvents.some(
          (stored) => stored.event.type === "checkpoint.rollback-requested",
        ),
      );
      assert.equal(
        (yield* sink.readCommandReceiptIdentity(command.commandId)).receipt?.status,
        "accepted",
      );
      const admission = yield* sink.readOrdinaryCheckoutAdmission({
        commandId: command.commandId,
        threadId,
      });
      assert.equal(admission?.capture.commandType, "checkpoint.rollback");
      assert.equal(admission?.capture.lease.ownerThreadId, threadId);
      const queued = yield* sql<{ readonly effect_type: string; readonly count: number }>`
      SELECT effect_type, COUNT(*) AS count FROM orchestration_v2_effect_outbox
      WHERE command_id = ${command.commandId} GROUP BY effect_type`;
      assert.deepEqual(queued, [{ effect_type: "provider-thread.rollback", count: 1 }]);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "links prepared release to the original deferred checkout admission and preserves exact replay",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("project:prepared-release-admission");
      const threadId = ThreadId.make("thread:prepared-release-admission");
      yield* seedOrdinaryProject(projectId);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create:prepared-release-admission"),
        threadId,
        projectId,
        title: "Prepared release admission",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "main",
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const initial = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("prepare:release-admission"),
        threadId,
        messageId: MessageId.make("message:prepared-release-admission"),
        text: "Release the original prepared run",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "defer_start" as const },
        createdBy: "user" as const,
        creationSource: "web" as const,
      };
      yield* orchestrator.dispatch(initial);
      const original = yield* sink.readOrdinaryCheckoutAdmission({
        commandId: initial.commandId,
        threadId,
      });
      if (original === null || original.run === null)
        return yield* Effect.die("Prepared release lacks an original accepted run admission");
      const lease = original.capture.lease;
      const release = {
        type: "prepared-run.release" as const,
        commandId: CommandId.make("release:original-admission"),
        threadId,
        runId: original.run.runId,
      };
      const accepted = yield* orchestrator.dispatch(release);
      const effectId = `effect:${release.commandId}:provider-turn.start:${release.runId}`;
      const link = yield* sink.readOrdinaryCheckoutEffectLink(effectId);
      assert.isNotNull(link);
      assert.equal(link?.commandId, release.commandId);
      assert.equal(link?.threadId, threadId);
      assert.equal(link?.admission.admissionId, original.admissionId);
      assert.deepEqual(
        yield* sink.readOrdinaryCheckoutAdmissionForRun({ threadId, runId: release.runId }),
        original,
      );
      assert.equal(
        yield* sink.readOrdinaryCheckoutAdmission({ commandId: release.commandId, threadId }),
        null,
      );
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [lease]);
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.runs.find((run) => run.id === release.runId)?.status, "starting");
      const rootNode = projection.nodes.find((node) => node.id === original.run?.nodeId);
      if (rootNode === undefined)
        return yield* Effect.die("Released run has no original root node");
      assert.isNotNull(rootNode.checkpointScopeId);
      const events =
        yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
      const effects =
        yield* sql`SELECT effect_id, command_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
      let preparations = 0;
      assert.deepEqual(
        yield* orchestrator.dispatch(
          release,
          Effect.sync(() => {
            preparations += 1;
          }),
        ),
        accepted,
      );
      assert.equal(preparations, 0);
      const conflict = yield* Effect.flip(
        orchestrator.dispatch({ ...release, runId: RunId.make("run:different-release-target") }),
      );
      assert.equal(conflict._tag, "DispatchGuardRejectedError");
      assert.equal(
        conflict._tag === "DispatchGuardRejectedError" ? conflict.reason : undefined,
        "identity_conflict",
      );
      assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), projection);
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [lease]);
      assert.deepEqual(
        yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`,
        events,
      );
      assert.deepEqual(
        yield* sql`SELECT effect_id, command_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        effects,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "retains the original checkout admission for same-run runtime answers and binds effectless replay",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("project:runtime-answer-admission");
      const threadId = ThreadId.make("thread:runtime-answer-admission");
      yield* seedOrdinaryProject(projectId);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create:runtime-answer-admission"),
        threadId,
        projectId,
        title: "Runtime answer admission",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "main",
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const initial = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("prepare:runtime-answer-admission"),
        threadId,
        messageId: MessageId.make("message:runtime-answer-admission"),
        text: "Prepare the original run",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "defer_start" as const },
        createdBy: "user" as const,
        creationSource: "web" as const,
      };
      yield* orchestrator.dispatch(initial);
      const preparedRun = (yield* orchestrator.getThreadProjection(threadId)).runs[0];
      if (preparedRun === undefined) return yield* Effect.die("Runtime answer has no prepared run");
      const original = yield* sink.readOrdinaryCheckoutAdmissionForRun({
        threadId,
        runId: preparedRun.id,
      });
      if (original === null || original.run === null)
        return yield* Effect.die("Runtime answer has no original admitted run");
      const run = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (candidate) => candidate.id === original.run?.runId,
      );
      if (run === undefined) return yield* Effect.die("Runtime answer has no current original run");
      const now = yield* DateTime.now;
      const sessionId = ProviderSessionId.make("session:runtime-answer-admission");
      yield* projections.apply({
        id: EventId.make("session:runtime-answer-admission"),
        type: "provider-session.attached",
        threadId,
        occurredAt: now,
        payload: {
          id: sessionId,
          driver: adapter.driver,
          providerInstanceId: instanceId,
          status: "ready",
          cwd: original.capture.canonicalCheckoutPath,
          model: modelSelection.model,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      });
      for (const mode of ["live", "effectless"] as const) {
        const requestId = RuntimeRequestId.make(`request:runtime-answer-admission:${mode}`);
        const nodeId = NodeId.make(`node:runtime-answer-admission:${mode}`);
        yield* projections.apply({
          id: EventId.make(`node:runtime-answer-admission:${mode}`),
          type: "node.updated",
          threadId,
          runId: run.id,
          occurredAt: now,
          payload: {
            id: nodeId,
            threadId,
            runId: run.id,
            parentNodeId: run.rootNodeId,
            rootNodeId: NodeId.make(original.run.nodeId),
            kind: "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId: run.providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: requestId,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        });
        yield* projections.apply({
          id: EventId.make(`request:runtime-answer-admission:${mode}`),
          type: "runtime-request.updated",
          threadId,
          runId: run.id,
          occurredAt: now,
          payload: {
            id: requestId,
            nodeId,
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            createdAt: now,
            resolvedAt: null,
            responseCapability:
              mode === "live"
                ? { type: "live", providerSessionId: sessionId }
                : { type: "message" },
          },
        });
        const response = {
          type: "runtime-request.respond" as const,
          commandId: CommandId.make(`respond:runtime-answer-admission:${mode}`),
          threadId,
          requestId,
          decision: mode === "live" ? ("accept" as const) : ("cancel" as const),
        };
        const accepted = yield* orchestrator.dispatch(response);
        assert.equal(
          yield* sink.readOrdinaryCheckoutAdmission({ commandId: response.commandId, threadId }),
          null,
        );
        const identity = yield* sink.readCommandReceiptIdentity(response.commandId);
        assert.equal(
          identity.ordinaryCheckoutCommands[0]?.admission.admissionId,
          original.admissionId,
        );
        assert.equal(
          identity.ordinaryCheckoutCommands[0]?.canonicalCommand.decision,
          response.decision,
        );
        assert.deepEqual(
          yield* sink.readOrdinaryCheckoutAdmissionForRun({ threadId, runId: run.id }),
          original,
        );
        const effectId = `effect:${response.commandId}:runtime-request.respond:${requestId}`;
        if (mode === "live") {
          assert.equal(
            (yield* sink.readOrdinaryCheckoutEffectLink(effectId))?.admission.admissionId,
            original.admissionId,
          );
        } else {
          assert.equal(yield* sink.readOrdinaryCheckoutEffectLink(effectId), null);
          assert.equal(
            (yield* sql<{
              readonly count: number;
            }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox
          WHERE command_id = ${response.commandId}`)[0]?.count,
            0,
          );
        }
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const events =
          yield* sql`SELECT event_id, command_id FROM orchestration_events ORDER BY sequence`;
        const effects =
          yield* sql`SELECT effect_id, command_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
        assert.deepEqual(yield* orchestrator.dispatch(response), accepted);
        const conflict = yield* Effect.flip(
          orchestrator.dispatch({ ...response, decision: "decline" }),
        );
        assert.equal(conflict._tag, "DispatchGuardRejectedError");
        assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), projection);
        assert.deepEqual(
          yield* sql`SELECT event_id, command_id FROM orchestration_events ORDER BY sequence`,
          events,
        );
        assert.deepEqual(
          yield* sql`SELECT effect_id, command_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
          effects,
        );
        assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [original.capture.lease]);
      }
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "reserves an expired prepared checkout and renews only its exact persisted use before entry",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const leases = yield* makeWorktreeOwnershipLeaseStore();
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("project:expired-prepared-use");
      const threadId = ThreadId.make("thread:expired-prepared-use");
      yield* seedOrdinaryProject(projectId);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create:expired-prepared-use"),
        threadId,
        projectId,
        title: "Expired prepared checkout",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "main",
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const originalLease = yield* orchestrator.acquireOrdinaryWorktreeOwnership(threadId);
      assert.isTrue(
        yield* leases.renew({ ...originalLease, nowMs: originalLease.renewedAtMs, expiresAtMs: 0 }),
      );
      const command = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("prepare:expired-prepared-use"),
        threadId,
        messageId: MessageId.make("message:expired-prepared-use"),
        text: "Prepare without physical work",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "defer_start" as const },
        createdBy: "user" as const,
        creationSource: "web" as const,
      };
      yield* orchestrator.dispatch(command);
      const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0];
      if (run === undefined) return yield* Effect.die("Prepared use lacks its accepted run");
      const admission = yield* orchestrator.readOrdinaryCheckoutAdmissionForRun({
        threadId,
        runId: run.id,
      });
      if (admission === null || admission.run === null)
        return yield* Effect.die("Prepared use lacks its real original admission");
      assert.equal(admission.capture.lease.expiresAtMs, 0);
      const ref = ordinaryCheckoutAdmissionRefV1(admission);
      const input = {
        operationId: "operation:expired-prepared-use",
        source: {
          kind: "prepared_run" as const,
          admission: ref,
          preparation: admission.run,
        },
      };
      const reserved = yield* orchestrator.beginOrdinaryPreparedCheckoutUse(admission, input);
      assert.equal(reserved.status, "reserved");
      assert.equal(reserved.record.state, "reserved");
      assert.equal(reserved.record.startedAt, null);
      assert.equal((yield* orchestrator.listWorktreeOwnershipLeases)[0]?.expiresAtMs, 0);
      const entered = yield* orchestrator.revalidateOrdinaryCheckoutUse(
        admission,
        reserved.record.subject.use,
      );
      assert.equal(entered.state, "started");
      assert.isNotNull(entered.startedAt);
      const renewed = (yield* orchestrator.listWorktreeOwnershipLeases)[0];
      if (renewed === undefined)
        return yield* Effect.die("Qualified renewal lost its captured lease");
      assert.equal(renewed.leaseId, originalLease.leaseId);
      assert.equal(renewed.ownerIncarnation, originalLease.ownerIncarnation);
      assert.isAbove(renewed.expiresAtMs, 0);
      assert.deepEqual(ordinaryCheckoutAdmissionRefV1(admission), ref);
      assert.equal(
        (yield* orchestrator.beginOrdinaryPreparedCheckoutUse(admission, input)).status,
        "observe_only",
      );
      const changedUse = {
        ...reserved.record.subject.use,
        source: {
          ...input.source,
          preparation: { ...admission.run, runId: RunId.make("run:unrelated-preparation") },
        },
      };
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(orchestrator.revalidateOrdinaryCheckoutUse(admission, changedUse)),
        ),
      );
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(orchestrator.releaseWorktreeOwnership(renewed))),
      );
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [renewed]);
      assert.deepEqual(
        yield* sink.readOrdinaryCheckoutAdmissionForRun({ threadId, runId: admission.run.runId }),
        admission,
      );
      assert.equal(
        (yield* sql<{
          readonly count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_v2_worktree_path_admissions
      WHERE operation_id = ${input.operationId}`)[0]?.count,
        1,
      );
      assert.equal(
        (yield* sql<{
          readonly count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox
      WHERE command_id = ${command.commandId}`)[0]?.count,
        0,
      );
      const release = {
        type: "prepared-run.release" as const,
        commandId: CommandId.make("release:expired-prepared-use"),
        threadId,
        runId: admission.run.runId,
      };
      const beforeRelease = yield* orchestrator.getThreadProjection(threadId);
      const beforeEvents =
        yield* sql`SELECT event_id, command_id FROM orchestration_events ORDER BY sequence`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(orchestrator.dispatch(release))));
      assert.equal((yield* sink.readCommandReceiptIdentity(release.commandId)).receipt, null);
      assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), beforeRelease);
      assert.deepEqual(
        yield* sql`SELECT event_id, command_id FROM orchestration_events ORDER BY sequence`,
        beforeEvents,
      );
      const released = yield* orchestrator.dispatchOrdinaryPreparedRunRelease(
        release,
        entered.subject.use,
      );
      assert.equal(
        (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.id === run.id,
        )?.status,
        "starting",
      );
      assert.equal(
        (yield* sink.readOrdinaryCheckoutEffectLink(
          `effect:${release.commandId}:provider-turn.start:${run.id}`,
        ))?.admission.admissionId,
        admission.admissionId,
      );
      const replay = yield* sink.readCommandReceiptIdentity(release.commandId);
      assert.deepEqual(replay.ordinaryCheckoutCommands[0]?.joinedUse, entered.subject.use);
      const releasedEvents =
        yield* sql`SELECT event_id, command_id FROM orchestration_events ORDER BY sequence`;
      const releasedEffects =
        yield* sql`SELECT effect_id, command_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
      assert.deepEqual(
        yield* orchestrator.dispatchOrdinaryPreparedRunRelease(release, entered.subject.use),
        released,
      );
      assert.isTrue(Exit.isFailure(yield* Effect.exit(orchestrator.dispatch(release))));
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            orchestrator.dispatchOrdinaryPreparedRunRelease(release, {
              ...entered.subject.use,
              operationId: "operation:different-prepared-use",
            }),
          ),
        ),
      );
      assert.deepEqual(
        yield* sink.readOrdinaryCheckoutAdmissionForRun({ threadId, runId: run.id }),
        admission,
      );
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [renewed]);
      assert.deepEqual(
        yield* sql`SELECT event_id, command_id FROM orchestration_events ORDER BY sequence`,
        releasedEvents,
      );
      assert.deepEqual(
        yield* sql`SELECT effect_id, command_id FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
        releasedEffects,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("renews a locally owned checkout beyond five minutes without an active execution", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const projectId = ProjectId.make("project:local-checkout-owner-heartbeat");
    const threadId = ThreadId.make("thread:local-checkout-owner-heartbeat");
    yield* seedOrdinaryProject(projectId);
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create:local-checkout-owner-heartbeat"),
      threadId,
      projectId,
      title: "Local checkout owner",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "main",
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    const original = yield* orchestrator.acquireOrdinaryWorktreeOwnership(threadId);
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const events =
      yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`;
    const renewals = yield* Queue.unbounded<{
      readonly renewedAtMs: number;
      readonly committedAtMs: number;
      readonly expiresAtMs: number;
    }>();
    const withDeletionWorktreeSqlMutation = sink.withDeletionWorktreeSqlMutation;
    yield* Effect.acquireRelease(
      Effect.sync(() =>
        vi
          .spyOn(sink, "withDeletionWorktreeSqlMutation")
          .mockImplementation(
            <A, E, R>(
              input: Parameters<typeof withDeletionWorktreeSqlMutation>[0],
              mutation: Effect.Effect<A, E, R>,
            ) =>
              withDeletionWorktreeSqlMutation(input, mutation).pipe(
                Effect.tap((result) => {
                  if (
                    input.path !== original.resourcePath ||
                    !Option.isOption(result) ||
                    Option.isNone(result) ||
                    !Schema.is(WorktreeOwnershipLease)(result.value) ||
                    result.value.leaseId !== original.leaseId ||
                    result.value.renewedAtMs <= original.renewedAtMs
                  )
                    return Effect.void;
                  const lease = result.value;
                  // Observe the SQL row only after its enclosing renewal transaction commits.
                  return sink
                    .onCommit(
                      Effect.gen(function* () {
                        yield* Queue.offer(renewals, {
                          renewedAtMs: lease.renewedAtMs,
                          committedAtMs: yield* Clock.currentTimeMillis,
                          expiresAtMs: lease.expiresAtMs,
                        });
                      }),
                    )
                    .pipe(Effect.orDie);
                }),
              ),
          ),
      ),
      (observer) => Effect.sync(() => observer.mockRestore()),
    );
    for (let minute = 0; minute < 6; minute += 1) {
      yield* TestClock.adjust("60 seconds");
      const receipt = yield* Queue.take(renewals);
      const clockNow = yield* Clock.currentTimeMillis;
      const receiptJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(
        receipt,
      ).pipe(Effect.orDie);
      assert.equal(receipt.renewedAtMs, clockNow, receiptJson);
      assert.equal(receipt.committedAtMs, clockNow, receiptJson);
      assert.isAbove(receipt.expiresAtMs, clockNow, receiptJson);
    }
    const now = DateTime.toEpochMillis(yield* DateTime.now);
    const leases = yield* orchestrator.listWorktreeOwnershipLeases;
    assert.equal(leases.length, 1);
    const renewed = leases[0]!;
    assert.equal(renewed.leaseId, original.leaseId);
    assert.equal(renewed.ownerThreadId, original.ownerThreadId);
    assert.equal(renewed.ownerIncarnation, original.ownerIncarnation);
    assert.equal(renewed.branch, original.branch);
    assert.equal(renewed.acquiredAtMs, original.acquiredAtMs);
    assert.isAbove(renewed.renewedAtMs, original.renewedAtMs);
    assert.isAbove(renewed.expiresAtMs, now);
    assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), projection);
    assert.deepEqual(
      yield* sql`SELECT event_id, sequence FROM orchestration_events ORDER BY sequence`,
      events,
    );
    assert.deepEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, []);
    assert.deepEqual(
      yield* sql`SELECT association_id FROM orchestration_v2_ordinary_checkout_execution_associations`,
      [],
    );
    assert.isNotNull(yield* sink.readApplicationBirthRecord(threadId));
    yield* orchestrator.releaseWorktreeOwnership(renewed);
    yield* TestClock.adjust("120 seconds");
    assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, []);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "stops renewing a local checkout when its raw source changes without deleting the lease",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const projectId = ProjectId.make("project:local-checkout-source-loss");
      const threadId = ThreadId.make("thread:local-checkout-source-loss");
      yield* seedOrdinaryProject(projectId);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create:local-checkout-source-loss"),
        threadId,
        projectId,
        title: "Local checkout source loss",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "main",
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const original = yield* orchestrator.acquireOrdinaryWorktreeOwnership(threadId);
      const before = yield* orchestrator.getThreadProjection(threadId);
      const now = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make("event:local-checkout-source-loss"),
            type: "thread.metadata-updated",
            threadId,
            occurredAt: now,
            payload: { ...before.thread, worktreePath: `${original.resourcePath}/.` },
          },
        ],
      });
      const changed = yield* orchestrator.getThreadProjection(threadId);
      // The canonical checkout is unchanged; the original raw source must still fence its heartbeat.
      for (let minute = 0; minute < 6; minute += 1) yield* TestClock.adjust("60 seconds");
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [original]);
      assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), changed);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "leaves a replacement lease unchanged when a stale local owner heartbeat loses its lease",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const leases = yield* makeWorktreeOwnershipLeaseStore();
      const projectId = ProjectId.make("project:local-checkout-replacement");
      const threadId = ThreadId.make("thread:local-checkout-replacement");
      const replacementThreadId = ThreadId.make("thread:local-checkout-replacement-owner");
      yield* seedOrdinaryProject(projectId);
      for (const id of [threadId, replacementThreadId])
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create:${id}`),
          threadId: id,
          projectId,
          title: "Local checkout replacement",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
      const original = yield* orchestrator.acquireOrdinaryWorktreeOwnership(threadId);
      yield* leases.release(original);
      const birth = yield* orchestrator.getOrdinaryThreadOwnershipIncarnation(replacementThreadId);
      if (Option.isNone(birth)) return yield* Effect.die("Replacement thread has no actual birth");
      const acquired = yield* leases.ensureOrdinaryOwnership({
        ...original,
        leaseId: "lease:actual-local-replacement",
        ownerThreadId: replacementThreadId,
        ownerIncarnation: birth.value,
        nowMs: original.renewedAtMs,
      });
      if (Option.isNone(acquired))
        return yield* Effect.die("Replacement checkout was not acquired");
      const replacement = acquired.value;
      for (let minute = 0; minute < 6; minute += 1) yield* TestClock.adjust("60 seconds");
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [replacement]);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "keeps an entered checkout execution live beyond five minutes and fences captured loss once",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const projectId = ProjectId.make("project:checkout-execution-keeper");
      const threadId = ThreadId.make("thread:checkout-execution-keeper");
      yield* seedOrdinaryProject(projectId);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create:checkout-execution-keeper"),
        threadId,
        projectId,
        title: "Checkout execution keeper",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "main",
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const command = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("prepare:checkout-execution-keeper"),
        threadId,
        messageId: MessageId.make("message:checkout-execution-keeper"),
        text: "Prepare an owned execution",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "defer_start" as const },
        createdBy: "user" as const,
        creationSource: "web" as const,
      };
      yield* orchestrator.dispatch(command);
      const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0];
      if (run === undefined) return yield* Effect.die("Keeper has no actual prepared run");
      const admission = yield* orchestrator.readOrdinaryCheckoutAdmissionForRun({
        threadId,
        runId: run.id,
      });
      if (admission === null || admission.run === null)
        return yield* Effect.die("Keeper has no original checkout admission");
      const source = {
        kind: "prepared_run" as const,
        admission: ordinaryCheckoutAdmissionRefV1(admission),
        preparation: admission.run,
      };
      const reserved = yield* orchestrator.beginOrdinaryPreparedCheckoutUse(admission, {
        operationId: "operation:checkout-execution-keeper",
        source,
      });
      const capturedProducer = { current: true, losses: 0 };
      const lossSignal = yield* Deferred.make<void>();
      const revalidateCaptured = Effect.suspend(() =>
        capturedProducer.current
          ? Effect.void
          : Effect.fail(fixtureFailure("Captured preparation ended")),
      );
      const ref = yield* sink.bindOrdinaryCheckoutExecution({
        originalUse: reserved.record.subject.use,
        executor: {
          kind: "actual_prepared_producer",
          producerId: "producer:checkout-execution-keeper",
          source,
        },
        targetSource: {
          projectWorkspaceRoot: admission.capture.canonicalProjectRoot,
          worktreePath: null,
        },
        revalidateProducer: revalidateCaptured,
      });
      const callbacks = {
        revalidateCaptured,
        onLoss: Effect.gen(function* () {
          capturedProducer.losses += 1;
          yield* Deferred.succeed(lossSignal, undefined);
        }),
      };
      const beforeRegistration = yield* sink.readOrdinaryCheckoutExecutionAssociations(
        ref.originalUse,
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sink.withTransaction(
              orchestrator
                .registerOrdinaryCheckoutExecution(ref, callbacks)
                .pipe(
                  Effect.andThen(
                    Effect.fail(fixtureFailure("Rollback registration before publication")),
                  ),
                ),
            ),
          ),
        ),
      );
      yield* TestClock.adjust("60 seconds");
      assert.deepEqual(
        yield* sink.readOrdinaryCheckoutExecutionAssociations(ref.originalUse),
        beforeRegistration,
      );
      assert.equal(
        (yield* sink.readOrdinaryCheckoutUse(ref.originalUse.operationId))?.state,
        "reserved",
      );
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [admission.capture.lease]);
      assert.equal(capturedProducer.losses, 0);
      yield* orchestrator.registerOrdinaryCheckoutExecution(ref, callbacks);
      const renewals = yield* Queue.unbounded<{
        readonly ref: typeof ref;
        readonly renewedAtMs: number;
      }>();
      const renewOrdinaryCheckoutExecution = sink.renewOrdinaryCheckoutExecution;
      yield* Effect.acquireRelease(
        Effect.sync(() =>
          vi.spyOn(sink, "renewOrdinaryCheckoutExecution").mockImplementation((input) =>
            renewOrdinaryCheckoutExecution(input).pipe(
              Effect.tap((renewedRef) =>
                input.ref.associationId !== ref.associationId
                  ? Effect.void
                  : sink
                      .onCommit(
                        Queue.offer(renewals, {
                          ref: renewedRef,
                          renewedAtMs: DateTime.toEpochMillis(input.now),
                        }).pipe(Effect.asVoid),
                      )
                      .pipe(Effect.orDie),
              ),
            ),
          ),
        ),
        (observer) => Effect.sync(() => observer.mockRestore()),
      );
      // SQL renewal must commit before the next clock tick can move its validation horizon.
      for (let minute = 0; minute < 6; minute += 1) {
        yield* TestClock.adjust("60 seconds");
        const renewed = yield* Queue.take(renewals);
        assert.deepEqual(renewed.ref, ref);
        assert.equal(renewed.renewedAtMs, DateTime.toEpochMillis(yield* DateTime.now));
      }
      const alive = yield* sink.readOrdinaryCheckoutExecutionAssociations(ref.originalUse);
      const participant = alive.participants.find(
        (item) => item.ref.associationId === ref.associationId,
      );
      assert.equal(participant?.state, "active");
      assert.deepEqual(participant?.ref, ref);
      assert.isAbove(alive.latestOrdinal, beforeRegistration.latestOrdinal);
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      const expiry = DateTime.toEpochMillis(
        yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
          participant?.expiresAt,
        ).pipe(Effect.orDie),
      );
      assert.isAbove(expiry, now);
      assert.isAtMost(expiry, now + 5 * 60_000);
      const lease = (yield* orchestrator.listWorktreeOwnershipLeases)[0];
      if (lease === undefined) return yield* Effect.die("Keeper lost its immutable lease");
      assert.equal(lease.leaseId, admission.capture.lease.leaseId);
      assert.equal(lease.ownerIncarnation, admission.capture.lease.ownerIncarnation);
      assert.isAbove(lease.expiresAtMs, now);
      assert.deepEqual(yield* orchestrator.revalidateOrdinaryCheckoutExecution(ref), ref);
      capturedProducer.current = false;
      yield* TestClock.adjust("60 seconds");
      yield* Deferred.await(lossSignal);
      const lost = yield* sink.readOrdinaryCheckoutExecutionAssociations(ref.originalUse);
      assert.equal(
        lost.participants.find((item) => item.ref.associationId === ref.associationId)?.state,
        "unknown",
      );
      assert.equal(
        (yield* sink.readOrdinaryCheckoutUse(ref.originalUse.operationId))?.state,
        "unknown",
      );
      assert.equal(capturedProducer.losses, 1);
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(orchestrator.revalidateOrdinaryCheckoutExecution(ref))),
      );
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(orchestrator.releaseWorktreeOwnership(lease))),
      );
      yield* TestClock.adjust("120 seconds");
      assert.equal(capturedProducer.losses, 1);
      assert.deepEqual(
        yield* sink.readOrdinaryCheckoutExecutionAssociations(ref.originalUse),
        lost,
      );
      assert.deepEqual(yield* orchestrator.listWorktreeOwnershipLeases, [lease]);
      assert.deepEqual(
        yield* sink.readOrdinaryCheckoutAdmissionForRun({ threadId, runId: run.id }),
        admission,
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("holds unknown imported continuation before allocating provider work", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const eventSink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:unknown-import");
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create:unknown-import"),
      threadId,
      projectId: ProjectId.make("project:unknown-import"),
      title: "Imported",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* eventSink.recordLegacyContinuationDisposition({
      threadId,
      provenance: "native_import",
      qualification: { type: "unknown", reason: "source_not_stopped" },
      evidence: null,
      importedAt: DateTime.formatIso(yield* DateTime.now),
    });
    const commandId = CommandId.make("dispatch:unknown-import");
    const failure = yield* Effect.flip(
      orchestrator.dispatch({
        type: "message.dispatch",
        commandId,
        threadId,
        messageId: MessageId.make("message:unknown-import"),
        text: "Continue",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      }),
    );
    assert.equal(
      failure._tag === "OrchestratorImportedContinuationHeldError" ? failure.reason : undefined,
      "continuation_unknown",
    );
    const providers = yield* sql<{
      readonly count: number;
    }>`SELECT COUNT(*) AS count FROM orchestration_v2_projection_provider_threads WHERE thread_id = ${threadId}`;
    const effects = yield* sql<{
      readonly count: number;
    }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE command_id = ${commandId}`;
    assert.equal(providers[0]?.count, 0);
    assert.equal(effects[0]?.count, 0);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "dispatches metadata, queue resume and request controls without hydrating unrelated history",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread:control-dispatch");
      const projectId = ProjectId.make("project:control-dispatch");
      const now = yield* DateTime.now;
      yield* seedOrdinaryProject(projectId);
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create-control"),
        threadId,
        projectId,
        title: "Before",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      for (const enabled of [false, true]) {
        yield* orchestrator.dispatch({
          type: "thread.auto-settle.set",
          commandId: CommandId.make(`auto-settle-${enabled}`),
          threadId,
          enabled,
        });
        const updated = yield* projections.getThreadProjection(threadId);
        assert.equal(updated.thread.autoSettleDisabledAt == null, enabled);
        const shell = yield* projections.getThreadShell(threadId);
        assert.ok(shell);
        assert.equal(shell.autoSettleDisabledAt == null, enabled);
      }
      yield* sql`INSERT INTO orchestration_v2_projection_messages
      (message_id, thread_id, run_id, node_id, role, streaming, created_at, updated_at, payload_json)
      VALUES ('obsolete', ${threadId}, NULL, NULL, 'assistant', 0, ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)}, '{"obsolete":true}')`;
      assert.equal((yield* Effect.exit(projections.getThreadProjection(threadId)))._tag, "Failure");
      yield* orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("resume-empty-queue"),
        threadId,
      });
      const rename = {
        type: "thread.metadata.update",
        commandId: CommandId.make("rename-control"),
        threadId,
        title: "After",
      } as const;
      const renamed = yield* orchestrator.dispatch(rename);
      assert.deepEqual(yield* orchestrator.dispatch(rename), renamed);
      yield* orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("mode-control"),
        threadId,
        runtimeMode: "approval-required",
      });
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("model-control"),
        threadId,
        modelSelection: { ...modelSelection, model: "gpt-6" },
      });
      const sessionId = ProviderSessionId.make("session:control-dispatch");
      yield* projections.apply({
        id: EventId.make("attach-control"),
        type: "provider-session.attached",
        threadId,
        occurredAt: now,
        payload: {
          id: sessionId,
          driver: adapter.driver,
          providerInstanceId: instanceId,
          status: "ready",
          cwd: "/repo",
          model: "gpt-6",
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      });
      for (const mode of ["live", "message"] as const) {
        const requestId = RuntimeRequestId.make(`request:${mode}`);
        yield* projections.apply({
          id: EventId.make(`request:${mode}`),
          type: "runtime-request.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: requestId,
            nodeId: NodeId.make(`node:${mode}`),
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability:
              mode === "live"
                ? { type: "live", providerSessionId: sessionId }
                : { type: "message" },
            createdAt: now,
            resolvedAt: null,
          },
        });
        const nodeId = NodeId.make(`node:${mode}`);
        yield* projections.apply({
          id: EventId.make(`node:${mode}`),
          type: "node.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: nodeId,
            threadId,
            runId: null,
            parentNodeId: null,
            rootNodeId: nodeId,
            kind: "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: requestId,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          },
        });
        yield* projections.apply({
          id: EventId.make(`item:${mode}`),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: TurnItemId.make(`item:${mode}`),
            threadId,
            runId: null,
            nodeId,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: mode === "live" ? 1 : 2,
            status: "waiting",
            title: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "user_input_request",
            requestId,
            questions: [],
          },
        });
        yield* orchestrator.dispatch(
          mode === "live"
            ? {
                type: "runtime-request.respond",
                commandId: CommandId.make(`respond:${mode}`),
                threadId,
                requestId,
                decision: "accept",
              }
            : {
                type: "thread.user-input.dismiss",
                commandId: CommandId.make(`respond:${mode}`),
                threadId,
                requestId,
              },
        );
        assert.equal(
          (yield* projections.getRuntimeRequest(threadId, requestId))?.status,
          "resolved",
        );
        const response = yield* projections.getRuntimeResponseContext(threadId, requestId);
        assert.equal(response.node?.status, mode === "live" ? "completed" : "cancelled");
        assert.equal(response.item?.status, mode === "live" ? "completed" : "cancelled");
      }
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("workspace-control"),
        threadId,
        worktreePath: "/new-repo",
      });
      const thread = yield* projections.getThread(threadId);
      assert.equal(thread.title, "After");
      assert.equal(thread.modelSelection.model, "gpt-6");
      assert.equal(thread.runtimeMode, "approval-required");
      assert.deepEqual(
        (yield* projections.getThreadProviderContext(threadId)).providerSessions,
        [],
      );
      yield* sql`INSERT INTO orchestration_v2_projection_turn_items
        (turn_item_id, thread_id, run_id, node_id, provider_thread_id, provider_turn_id,
          type, status, ordinal, updated_at, payload_json)
        VALUES ('obsolete-output', ${threadId}, NULL, NULL, NULL, NULL,
          'command_execution', 'completed', 900, ${DateTime.formatIso(now)}, '{"obsolete":true}')`;
      yield* sql`INSERT INTO orchestration_v2_projection_plans
        (plan_id, thread_id, run_id, node_id, kind, status, payload_json)
        VALUES ('obsolete-plan', ${threadId}, NULL, 'old-node', 'proposed', 'completed', '{"obsolete":true}')`;
      yield* sql`INSERT INTO orchestration_v2_projection_context_handoffs
        (context_handoff_id, thread_id, target_run_id, to_provider_thread_id, strategy, status, updated_at, payload_json)
        VALUES ('obsolete-handoff', ${threadId}, 'old-run', 'old-provider-thread', 'full_thread_summary', 'ready', ${DateTime.formatIso(now)}, '{"obsolete":true}')`;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("dispatch-with-old-history"),
        threadId,
        messageId: MessageId.make("fresh-input"),
        text: "Continue",
        attachments: [],
        dispatchMode: { type: "defer_start" },
        createdBy: "user",
        creationSource: "web",
      });
      const fresh = yield* projections.getThreadRecords(threadId, ["turnItems"], {
        turnItemTypes: ["user_message"],
      });
      assert.isAbove(fresh.turnItems.at(-1)!.ordinal, 900);
      yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("archive-with-old-history"),
        threadId,
      });
      yield* orchestrator.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("delete-with-old-history"),
        threadId,
      });
      assert.isNotNull((yield* projections.getThread(threadId)).deletedAt);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("implements a proposed plan that the command projection leaves out", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:implement-plan");
    const projectId = ProjectId.make("project:implement-plan");
    const planId = PlanId.make("plan:implement-plan");
    const now = yield* DateTime.now;
    yield* seedOrdinaryProject(projectId);
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-implement-plan"),
      threadId,
      projectId,
      title: "Plan",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "plan",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* projections.apply({
      id: EventId.make("plan:implement-plan"),
      type: "plan.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: planId,
        threadId,
        runId: null,
        nodeId: NodeId.make("node:implement-plan"),
        kind: "proposed_plan",
        status: "active",
        markdown: "# Plan\n\n1. Do the thing.",
      },
    });

    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("implement-plan"),
      threadId,
      messageId: MessageId.make("implement-plan-input"),
      text: "Implement the plan.",
      attachments: [],
      sourcePlanRef: { threadId, planId },
      dispatchMode: { type: "defer_start" },
      createdBy: "user",
      creationSource: "web",
    });

    assert.equal((yield* projections.getPlan(threadId, planId))?.status, "completed");
  }).pipe(Effect.provide(testLayer)),
);

// Stop's settle follow-up runs after the provider interrupt returns, possibly
// long after the Stop (retries) or again (an effect replayed after a crash).
// A later run's background work is not that Stop's to end.
it.effect("settles only the stopped run's background work, once", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:settle-binding");
    const providerThreadId = ProviderThreadId.make("provider-thread:settle-binding");
    const now = yield* DateTime.now;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-settle-binding"),
      threadId,
      projectId: ProjectId.make("project:settle-binding"),
      title: "Settle binding",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* projections.apply({
      id: EventId.make("settle-binding:provider-thread"),
      type: "provider-thread.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: providerThreadId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 2,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    });
    const commandItem = (ordinal: number) => TurnItemId.make(`turn-item:settle-binding:${ordinal}`);
    for (const ordinal of [1, 2]) {
      const runId = RunId.make(`run:settle-binding:${ordinal}`);
      const attemptId = RunAttemptId.make(`attempt:settle-binding:${ordinal}`);
      const nodeId = NodeId.make(`node:settle-binding:${ordinal}`);
      const providerTurnId = ProviderTurnId.make(`provider-turn:settle-binding:${ordinal}`);
      yield* projections.apply({
        id: EventId.make(`settle-binding:run:${ordinal}`),
        type: "run.created",
        threadId,
        occurredAt: now,
        payload: {
          id: runId,
          threadId,
          ordinal,
          providerInstanceId: instanceId,
          modelSelection,
          providerThreadId,
          userMessageId: MessageId.make(`message:settle-binding:${ordinal}`),
          rootNodeId: nodeId,
          activeAttemptId: attemptId,
          status: "completed",
          requestedAt: now,
          startedAt: now,
          completedAt: now,
          checkpointId: null,
          contextHandoffId: null,
        },
      });
      yield* projections.apply({
        id: EventId.make(`settle-binding:attempt:${ordinal}`),
        type: "run-attempt.created",
        threadId,
        occurredAt: now,
        payload: {
          id: attemptId,
          runId,
          attemptOrdinal: 1,
          rootNodeId: nodeId,
          providerInstanceId: instanceId,
          providerThreadId,
          providerTurnId,
          reason: "initial",
          status: "completed",
          startedAt: now,
          completedAt: now,
        },
      });
      yield* projections.apply({
        id: EventId.make(`settle-binding:turn:${ordinal}`),
        type: "provider-turn.updated",
        threadId,
        occurredAt: now,
        payload: {
          id: providerTurnId,
          providerThreadId,
          nodeId,
          runAttemptId: attemptId,
          nativeTurnRef: null,
          ordinal,
          status: "completed",
          startedAt: now,
          completedAt: now,
        },
      });
      yield* projections.apply({
        id: EventId.make(`settle-binding:item:${ordinal}`),
        type: "turn-item.updated",
        threadId,
        runId,
        occurredAt: now,
        payload: {
          id: commandItem(ordinal),
          threadId,
          runId,
          nodeId,
          providerThreadId,
          providerTurnId,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: ordinal * 10,
          status: "running",
          title: `Background command ${ordinal}`,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
          type: "command_execution",
          input: `sleep ${ordinal}`,
        },
      });
    }
    const itemStatuses = Effect.map(projections.getThreadProjection(threadId), (projection) =>
      projection.turnItems
        .flatMap((item) => (item.type === "command_execution" ? [`${item.id}:${item.status}`] : []))
        .toSorted(),
    );
    // The settle that followed a Stop of run 1's turn, dispatched only after
    // run 2 had settled with work of its own.
    const settle = {
      type: "thread.background-work.settle",
      commandId: CommandId.make("stop-run-1:background-work-settled"),
      threadId,
      providerThreadId,
      providerTurnId: ProviderTurnId.make("provider-turn:settle-binding:1"),
    } as const;
    yield* orchestrator.dispatch(settle);
    assert.deepEqual(yield* itemStatuses, [
      `${commandItem(1)}:interrupted`,
      `${commandItem(2)}:running`,
    ]);

    // A settle that found nothing to end replays as a no-op, even after work
    // it would match appears: its receipt is recorded with no events.
    const emptySettle = {
      ...settle,
      commandId: CommandId.make("stop-run-1-again:background-work-settled"),
    };
    const first = yield* orchestrator.dispatch(emptySettle);
    assert.lengthOf(first.storedEvents, 0);
    yield* projections.apply({
      id: EventId.make("settle-binding:item:late"),
      type: "turn-item.updated",
      threadId,
      runId: RunId.make("run:settle-binding:1"),
      occurredAt: now,
      payload: {
        id: commandItem(3),
        threadId,
        runId: RunId.make("run:settle-binding:1"),
        nodeId: NodeId.make("node:settle-binding:1"),
        providerThreadId,
        providerTurnId: ProviderTurnId.make("provider-turn:settle-binding:1"),
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 30,
        status: "running",
        title: "Late background command",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "command_execution",
        input: "sleep 3",
      },
    });
    const replayed = yield* orchestrator.dispatch(emptySettle);
    assert.lengthOf(replayed.storedEvents, 0);
    assert.deepEqual(yield* itemStatuses, [
      `${commandItem(1)}:interrupted`,
      `${commandItem(2)}:running`,
      `${commandItem(3)}:running`,
    ]);
  }).pipe(Effect.provide(testLayer)),
);
