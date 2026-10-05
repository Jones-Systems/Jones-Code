import { planDelegatedCheckout } from "./DelegatedCheckoutPolicy.ts";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  RunAttemptId,
  OrchestrationV2Command,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderThreadId,
  ProviderSessionId,
  ProviderTurnId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Ordinary from "./OrdinaryCheckoutOwnership.ts";
import { makeWorktreeOwnershipLeaseStore } from "./WorktreeOwnershipLease.ts";
import { canonicalJson } from "./CanonicalJson.ts";
import { makeOrdinaryCheckoutStore } from "./OrdinaryCheckoutStore.ts";

const database = SqlitePersistenceMemory;
const infrastructure = Layer.mergeAll(
  database,
  EventStore.layer.pipe(Layer.provide(database)),
  CommandReceiptStore.layer.pipe(Layer.provide(database)),
  ProjectionStore.layer.pipe(Layer.provide(database)),
  EffectOutbox.layer.pipe(Layer.provide(database)),
);
const layer = EventSink.layer.pipe(Layer.provideMerge(infrastructure));
const now = DateTime.makeUnsafe("2026-10-03T00:00:00.000Z");
const projectId = ProjectId.make("project:checkout-admission");
const providerInstanceId = ProviderInstanceId.make("checkout-admission-provider");
const modelSelection = { instanceId: providerInstanceId, model: "fixture" };
const decodeDeadline = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ expiresAt: Schema.String })),
);
const encodeExecutionRef = Schema.encodeSync(Ordinary.OrdinaryCheckoutExecutionRefV1);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function thread(threadId: ThreadId): OrchestrationV2AppThread {
  return {
    id: threadId,
    projectId,
    title: "Ownership fixture",
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "fixture/owner",
    worktreePath: "/fixture/worktree",
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

const createFixture = Effect.fn("createCheckoutFixture")(function* (id: string) {
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT OR IGNORE INTO projection_projects
    (project_id, title, workspace_root, scripts_json, created_at, updated_at)
    VALUES (${projectId}, 'Fixture', '/fixture/project', '[]', ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)})`;
  const owner = thread(ThreadId.make(id));
  yield* sink.write({
    events: [
      {
        id: EventId.make(`event:${id}:created`),
        type: "thread.created",
        threadId: owner.id,
        providerInstanceId,
        occurredAt: now,
        payload: owner,
      },
    ],
  });
  return owner;
});

const prepare = Effect.fn("prepareCheckoutFixture")(function* (
  owner: OrchestrationV2AppThread,
  id: string,
) {
  const store = yield* makeOrdinaryCheckoutStore();
  const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
    type: "message.dispatch",
    commandId: id,
    threadId: owner.id,
    messageId: `message:${id}`,
    text: "Mutate the owned checkout",
    attachments: [],
    modelSelection,
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  const capture = yield* store.capture({
    command,
    threadId: owner.id,
    projectId,
    branch: owner.branch,
    canonicalProjectRoot: "/fixture/project",
    canonicalCheckoutPath: "/fixture/worktree",
    source: { projectWorkspaceRoot: "/fixture/project", worktreePath: owner.worktreePath },
    leaseId: `lease:${id}`,
  });
  const event: OrchestrationV2DomainEvent = {
    id: EventId.make(`event:${id}:accepted`),
    type: "thread.metadata-updated",
    threadId: owner.id,
    providerInstanceId,
    occurredAt: now,
    payload: { ...owner, title: id },
  };
  return { command, capture, event };
});

const claimFixture = Effect.fn("claimCheckoutFixture")(function* (id: string) {
  const owner = yield* createFixture(`owner:${id}`);
  const fixture = yield* prepare(owner, `command:${id}`);
  const sink = yield* EventSink.EventSinkV2;
  const acceptedAt = yield* DateTime.now;
  if (fixture.command.type !== "message.dispatch")
    return yield* Effect.die("Unexpected fixture command");
  const providerThreadId = ProviderThreadId.make(`provider-thread:${id}`);
  const providerSessionId = ProviderSessionId.make(`provider-session:${id}`);
  const runId = RunId.make(`run:${id}`);
  const rootNodeId = NodeId.make(`node:${id}`);
  const attemptId = RunAttemptId.make(`attempt:${id}`);
  const run = {
    id: runId,
    threadId: owner.id,
    ordinal: 1,
    providerInstanceId,
    modelSelection,
    providerThreadId,
    userMessageId: fixture.command.messageId,
    rootNodeId,
    activeAttemptId: attemptId,
    status: "starting" as const,
    queuePosition: null,
    requestedAt: acceptedAt,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const eventScope = {
    threadId: owner.id,
    runId,
    nodeId: rootNodeId,
    providerInstanceId,
    occurredAt: acceptedAt,
  };
  const runEvents: Array<OrchestrationV2DomainEvent> = [
    {
      ...eventScope,
      id: EventId.make(`event:${id}:provider`),
      type: "provider-thread.updated",
      payload: {
        id: providerThreadId,
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId,
        providerSessionId,
        appThreadId: owner.id,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: acceptedAt,
        updatedAt: acceptedAt,
      },
    },
    { ...eventScope, id: EventId.make(`event:${id}:run`), type: "run.created", payload: run },
    {
      ...eventScope,
      id: EventId.make(`event:${id}:attempt`),
      type: "run-attempt.created",
      payload: {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId,
        providerInstanceId,
        providerThreadId,
        providerTurnId: null,
        reason: "initial",
        status: "pending",
        startedAt: null,
        completedAt: null,
      },
    },
    {
      ...eventScope,
      id: EventId.make(`event:${id}:node`),
      type: "node.updated",
      payload: {
        id: rootNodeId,
        threadId: owner.id,
        runId,
        parentNodeId: null,
        rootNodeId,
        kind: "root_turn",
        status: "pending",
        countsForRun: true,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: null,
        completedAt: null,
      },
    },
    {
      ...eventScope,
      id: EventId.make(`event:${id}:message`),
      type: "message.updated",
      payload: {
        id: fixture.command.messageId,
        threadId: owner.id,
        runId,
        nodeId: rootNodeId,
        role: "user",
        text: fixture.command.text,
        attachments: [],
        streaming: false,
        createdBy: "user",
        creationSource: "web",
        createdAt: acceptedAt,
        updatedAt: acceptedAt,
      },
    },
  ];
  yield* sink.commitCommand({
    commandId: fixture.command.commandId,
    threadId: owner.id,
    commandType: fixture.command.type,
    acceptedAt,
    events: [fixture.event, ...runEvents],
    ordinaryCheckout: fixture.capture,
    effects: [
      {
        id: `effect:${id}`,
        commandId: fixture.command.commandId,
        threadId: owner.id,
        request: { type: "provider-turn.start", runId: RunId.make(`run:${id}`) },
      },
    ],
  });
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const claimed = yield* outbox.claimNext({ workerId: `worker:${id}`, leaseDurationMs: 60000 });
  if (Option.isNone(claimed) || claimed.value.leaseExpiresAt === null)
    return yield* Effect.die("The fixture did not acquire a physical effect claim.");
  const effect = claimed.value;
  const store = yield* makeOrdinaryCheckoutStore();
  const linked = yield* store.readEffectLink(effect);
  if (linked === null) return yield* Effect.die("The fixture effect has no accepted admission.");
  const source: Ordinary.OrdinaryCheckoutUseSourceV1 = {
    kind: "outbox",
    link: linked.link,
    workerId: `worker:${id}`,
    expectedAttempt: effect.attemptCount,
    leaseExpiresAt: DateTime.makeUnsafe(effect.leaseExpiresAt!),
  };
  const input = {
    operationId: Ordinary.ordinaryCheckoutOutboxOperationIdV1(effect.id, effect.attemptCount),
    admission: linked.link.admission,
    source,
    targetSource: fixture.capture.source,
  };
  return { owner, fixture, store, input, outbox, effect, run };
});

describe("Ordinary checkout physical use", () => {
  it.effect("binds actual system restart effects to their immutable original admission", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("system-restart");
      const sink = yield* EventSink.EventSinkV2;
      const effect = {
        id: "effect:system-restart:later",
        commandId: CommandId.make("command:system-restart:later"),
        threadId: fixture.owner.id,
        request: {
          type: "provider-turn.restart" as const,
          runId: fixture.run.id,
          providerThreadId: fixture.run.providerThreadId,
          providerSessionId: ProviderSessionId.make("provider-session:system-restart"),
          providerTurnId: ProviderTurnId.make("provider-turn:system-restart"),
          interruptedAttemptId: fixture.run.activeAttemptId,
        },
      };
      yield* sink.writeWithEffects({
        events: [],
        effects: [effect],
        ordinaryCheckoutEffects: [
          {
            runId: fixture.run.id,
            admission: fixture.input.admission,
            source: fixture.input.targetSource,
          },
        ],
      });
      const actual = Option.getOrThrow(yield* fixture.outbox.get(effect.id));
      const linked = yield* fixture.store.readEffectLink(actual);
      assert.isNotNull(linked);
      assert.deepEqual(linked!.link.admission, fixture.input.admission);
      assert.equal(linked!.link.effectId, effect.id);
      assert.equal(linked!.link.commandId, effect.commandId);
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = ${effect.id}`)
          .length,
        1,
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect("rolls back an enqueued system effect when its original run association differs", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("system-refusal");
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const before = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
      const effect = {
        id: "effect:system-refusal:later",
        commandId: CommandId.make("command:system-refusal:later"),
        threadId: fixture.owner.id,
        request: {
          type: "provider-turn.restart" as const,
          runId: fixture.run.id,
          providerThreadId: fixture.run.providerThreadId,
          providerSessionId: ProviderSessionId.make("provider-session:system-refusal"),
          providerTurnId: ProviderTurnId.make("provider-turn:system-refusal"),
          interruptedAttemptId: fixture.run.activeAttemptId,
        },
      };
      const refusal = yield* Effect.exit(
        sink.writeWithEffects({
          events: [],
          effects: [effect],
          ordinaryCheckoutEffects: [
            {
              runId: RunId.make("run:system-refusal:replacement"),
              admission: fixture.input.admission,
              source: fixture.input.targetSource,
            },
          ],
        }),
      );
      assert.isTrue(Exit.isFailure(refusal));
      assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, before);
      assert.isTrue(Option.isNone(yield* fixture.outbox.get(effect.id)));
      assert.equal(
        (yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = ${effect.id}`)
          .length,
        0,
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "commits a delegated child's birth, pinned preparation, admission and lease atomically",
    () =>
      Effect.gen(function* () {
        const parent = yield* claimFixture("delegated-parent");
        const sink = yield* EventSink.EventSinkV2;
        const sql = yield* SqlClient.SqlClient;
        const acceptedAt = yield* DateTime.now;
        const childId = ThreadId.make("child:isolated-checkout");
        const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
          type: "delegated_task.request",
          commandId: "command:isolated-checkout",
          parentThreadId: parent.owner.id,
          parentRunId: parent.run.id,
          parentNodeId: parent.run.rootNodeId,
          task: "Inspect the committed source",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          createdBy: "agent",
          creationSource: "mcp",
        });
        if (command.type !== "delegated_task.request")
          return yield* Effect.die("Unexpected fixture command");
        const plan = planDelegatedCheckout({
          commandId: command.commandId,
          parentThreadId: parent.owner.id,
          parentCheckoutPath: parent.owner.worktreePath!,
          parentCommit: "a".repeat(40),
          childThreadId: childId,
          canonicalProjectRoot: "/fixture/project",
          canonicalWorktreesDir: "/fixture/worktrees",
        });
        const child = { ...thread(childId), worktreePath: plan.worktreePath, branch: plan.branch };
        const runId = RunId.make("run:isolated-checkout");
        const attemptId = RunAttemptId.make("attempt:isolated-checkout");
        const nodeId = NodeId.make("node:isolated-checkout");
        const messageId = MessageId.make("message:isolated-checkout");
        const providerThreadId = ProviderThreadId.make("provider-thread:isolated-checkout");
        const scope = {
          threadId: childId,
          runId,
          nodeId,
          providerInstanceId,
          occurredAt: acceptedAt,
        };
        const run = {
          ...parent.run,
          id: runId,
          threadId: childId,
          providerThreadId,
          rootNodeId: nodeId,
          activeAttemptId: attemptId,
          userMessageId: messageId,
          status: "preparing" as const,
          workspacePreparation: plan.workspaceStrategy,
        };
        const events: Array<OrchestrationV2DomainEvent> = [
          {
            id: EventId.make("event:isolated-checkout:birth"),
            type: "thread.created",
            threadId: childId,
            providerInstanceId,
            occurredAt: acceptedAt,
            payload: child,
          },
          {
            ...scope,
            id: EventId.make("event:isolated-checkout:run"),
            type: "run.created",
            payload: run,
          },
          {
            ...scope,
            id: EventId.make("event:isolated-checkout:attempt"),
            type: "run-attempt.created",
            payload: {
              id: attemptId,
              runId,
              attemptOrdinal: 1,
              rootNodeId: nodeId,
              providerInstanceId,
              providerThreadId,
              providerTurnId: null,
              reason: "initial",
              status: "pending",
              startedAt: null,
              completedAt: null,
            },
          },
          {
            ...scope,
            id: EventId.make("event:isolated-checkout:node"),
            type: "node.updated",
            payload: {
              id: nodeId,
              threadId: childId,
              runId,
              parentNodeId: null,
              rootNodeId: nodeId,
              kind: "root_turn",
              status: "pending",
              countsForRun: true,
              providerThreadId,
              providerTurnId: null,
              nativeItemRef: null,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: null,
              completedAt: null,
            },
          },
          {
            ...scope,
            id: EventId.make("event:isolated-checkout:message"),
            type: "message.updated",
            payload: {
              id: messageId,
              threadId: childId,
              runId,
              nodeId,
              role: "user",
              text: command.task,
              attachments: [],
              streaming: false,
              createdBy: "agent",
              creationSource: "mcp",
              createdAt: acceptedAt,
              updatedAt: acceptedAt,
            },
          },
        ];
        const input = {
          commandId: command.commandId,
          threadId: parent.owner.id,
          commandType: command.type,
          acceptedAt,
          ordinaryDelegatedCommand: command,
          events,
          effects: [
            {
              id: "effect:isolated-checkout:prepare",
              commandId: command.commandId,
              threadId: childId,
              request: { type: "delegated-workspace.prepare" as const, runId, plan },
            },
          ],
        };
        const originalParentLease =
          yield* sql`SELECT * FROM worktree_ownership_leases WHERE resource_path = ${parent.owner.worktreePath}`;
        const committed = yield* sink.commitCommand(input);
        assert.isTrue(committed.committed);
        const admission = yield* sink.ordinaryCheckoutLifetime!.readAdmission(
          command.commandId,
          childId,
        );
        assert.isNotNull(admission);
        assert.deepEqual(admission!.capture.origin, {
          kind: "delegated_child",
          parentThreadId: parent.owner.id,
        });
        assert.equal(admission!.capture.canonicalCheckoutPath, plan.worktreePath);
        assert.equal(admission!.capture.applicationBirth.eventId, "event:isolated-checkout:birth");
        assert.equal(admission!.run!.runId, runId);
        assert.deepEqual(
          yield* sink.ordinaryCheckoutLifetime!.readAdmissionForRun({ threadId: childId, runId }),
          admission,
        );
        const childLease = yield* sql<{
          readonly owner_thread_id: string;
          readonly branch: string;
        }>`SELECT * FROM worktree_ownership_leases WHERE resource_path = ${plan.worktreePath}`;
        assert.equal(childLease.length, 1);
        assert.equal(childLease[0]!.owner_thread_id, childId);
        assert.equal(childLease[0]!.branch, plan.branch);
        assert.deepEqual(
          yield* sql`SELECT * FROM worktree_ownership_leases WHERE resource_path = ${parent.owner.worktreePath}`,
          originalParentLease,
        );
        const replay = yield* sink.commitCommand(input);
        assert.isFalse(replay.committed);
        assert.deepEqual(replay.storedEvents, committed.storedEvents);
        assert.equal(
          (yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions WHERE command_id = ${command.commandId}`)
            .length,
          1,
        );
        yield* sink.validateOrdinaryCheckoutCommandReplay!(command, childId);
      }).pipe(Effect.provide(layer)),
  );

  it.effect("renews only the current original claim and preserves each immutable deadline", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-renewal");
      if (fixture.input.source.kind !== "outbox")
        return yield* Effect.die("Unexpected fixture source");
      const begun = yield* fixture.store.beginUse(fixture.input);
      const ref = yield* fixture.store.bindExecution({
        originalUse: begun.record.subject.use,
        executor: { kind: "actual_outbox_claim", source: fixture.input.source },
        targetSource: fixture.input.targetSource,
      });
      yield* fixture.store.validateExecution(ref, true);
      const newExpiry = DateTime.add(fixture.input.source.leaseExpiresAt, { seconds: 30 });
      yield* fixture.store.renewExecution({
        ref,
        now: yield* DateTime.now,
        newExpiry,
        expectedClaimExpiry: fixture.input.source.leaseExpiresAt,
      });
      const history = yield* fixture.store.readExecutionHistory(ref.originalUse);
      assert.deepEqual(
        history.facts.map((fact) => fact.eventKind),
        ["bind", "renew"],
      );
      assert.equal(
        history.facts[0]!.evidence.expiresAt,
        DateTime.formatIso(fixture.input.source.leaseExpiresAt),
      );
      assert.equal(history.facts[1]!.evidence.expiresAt, DateTime.formatIso(newExpiry));
      assert.equal(history.participants[0]!.expiresAt, DateTime.formatIso(newExpiry));
      const updated = Option.getOrThrow(yield* fixture.outbox.get(fixture.effect.id));
      assert.equal(updated.leaseExpiresAt, DateTime.formatIso(newExpiry));
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            fixture.store.renewExecution({
              ref,
              now: yield* DateTime.now,
              newExpiry: DateTime.add(newExpiry, { seconds: 30 }),
              expectedClaimExpiry: fixture.input.source.leaseExpiresAt,
            }),
          ),
        ),
      );
      assert.equal((yield* fixture.store.readExecutionHistory(ref.originalUse)).latestOrdinal, 1);
      yield* fixture.store.validateExecution(ref);
    }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "retains an uncertain native operation across cancellation, completion, renewal and process-loss recovery",
    () =>
      Effect.gen(function* () {
        const fixture = yield* claimFixture("physical-native-hold");
        if (fixture.input.source.kind !== "outbox")
          return yield* Effect.die("Unexpected fixture source");
        const begun = yield* fixture.store.beginUse(fixture.input);
        const ref = yield* fixture.store.bindExecution({
          originalUse: begun.record.subject.use,
          executor: { kind: "actual_outbox_claim", source: fixture.input.source },
          targetSource: fixture.input.targetSource,
        });
        yield* fixture.store.validateExecution(ref, true);
        assert.isTrue(
          yield* fixture.outbox.holdUnknown({
            effectId: fixture.effect.id,
            workerId: fixture.input.source.workerId,
            expectedAttempt: fixture.effect.attemptCount,
            operationId: "native:physical-native-hold",
            evidence: {
              operationId: "native:physical-native-hold",
              operation: "start_turn",
              threadId: fixture.owner.id,
              instanceId: providerInstanceId,
              outcome: "unknown",
            },
          }),
        );
        yield* fixture.store.retainExecutionUnknown(
          ref,
          "Original endpoint has no completion evidence",
        );
        assert.deepEqual(
          yield* fixture.outbox.cancelUnsettled({
            threadId: fixture.owner.id,
            effectTypes: ["provider-turn.start"],
            reason: "cancel unknown",
          }),
          [],
        );
        assert.isFalse(
          yield* fixture.outbox.succeed({
            effectId: fixture.effect.id,
            workerId: fixture.input.source.workerId,
          }),
        );
        assert.isFalse(
          yield* fixture.outbox.retry({
            effectId: fixture.effect.id,
            workerId: fixture.input.source.workerId,
            error: "unknown",
            delayMs: 0,
          }),
        );
        assert.isFalse(
          yield* fixture.outbox.fail({
            effectId: fixture.effect.id,
            workerId: fixture.input.source.workerId,
            error: "unknown",
          }),
        );
        assert.isFalse(
          yield* fixture.outbox.renewClaim({
            effectId: fixture.effect.id,
            workerId: fixture.input.source.workerId,
            expectedAttempt: fixture.effect.attemptCount,
            expectedLeaseExpiresAt: fixture.effect.leaseExpiresAt!,
            leaseExpiresAt: DateTime.formatIso(
              DateTime.add(fixture.input.source.leaseExpiresAt, { seconds: 30 }),
            ),
          }),
        );
        assert.isTrue(Exit.isFailure(yield* Effect.exit(fixture.store.validateExecution(ref))));
        const holds = yield* fixture.outbox.listHeldByThreadId(fixture.owner.id);
        assert.equal(holds.length, 1);
        assert.equal(holds[0]!.operationId, "native:physical-native-hold");
        assert.equal((yield* fixture.store.readUse(ref.originalUse.operationId))!.state, "unknown");
        const sql = yield* SqlClient.SqlClient;
        yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = '2000-01-01T00:00:00.000Z' WHERE effect_id = ${fixture.effect.id}`;
        yield* fixture.outbox.reconcileAfterProcessLoss;
        assert.equal(
          Option.getOrThrow(yield* fixture.outbox.get(fixture.effect.id)).status,
          "running",
        );
        assert.isTrue(
          Option.isNone(
            yield* fixture.outbox.claimNext({ workerId: "foreign", leaseDurationMs: 60000 }),
          ),
        );
        assert.equal(
          (yield* fixture.store.readExecutionHistory(ref.originalUse)).participants[0]!.state,
          "unknown",
        );
      }).pipe(Effect.provide(layer)),
  );

  it.effect("captures only the exact accepted runless birth and retains its immutable replay", () =>
    Effect.gen(function* () {
      yield* createFixture("owner:runless:project-seed");
      const owner = thread(ThreadId.make("owner:runless"));
      const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
        type: "thread.create",
        commandId: "command:runless",
        threadId: owner.id,
        projectId,
        title: owner.title,
        modelSelection,
        runtimeMode: owner.runtimeMode,
        interactionMode: owner.interactionMode,
        branch: owner.branch,
        worktreePath: owner.worktreePath,
        createdBy: "user",
        creationSource: "web",
      });
      if (command.type !== "thread.create")
        return yield* Effect.die("Unexpected runless fixture command");
      const sink = yield* EventSink.EventSinkV2;
      const committed = yield* sink.commitCommand({
        commandId: command.commandId,
        threadId: owner.id,
        commandType: command.type,
        acceptedAt: yield* DateTime.now,
        events: [
          {
            id: EventId.make("event:runless:birth"),
            type: "thread.created",
            threadId: owner.id,
            providerInstanceId,
            occurredAt: yield* DateTime.now,
            payload: owner,
          },
        ],
        effects: [],
      });
      const event = committed.storedEvents[0]!;
      const store = yield* makeOrdinaryCheckoutStore();
      const input = {
        command,
        preparationEvent: {
          eventId: event.event.id,
          sequence: event.sequence,
          threadId: owner.id,
          commandId: event.commandId,
          eventType: event.event.type,
        },
        target: {
          threadId: owner.id,
          projectId,
          branch: owner.branch,
          canonicalProjectRoot: "/fixture/project",
          canonicalCheckoutPath: "/fixture/worktree",
          source: { projectWorkspaceRoot: "/fixture/project", worktreePath: owner.worktreePath },
        },
      };
      const original = yield* store.capturePreparedLaunch(input);
      assert.isNull(original.run);
      assert.equal(original.capture.applicationBirth.eventId, event.event.id);
      assert.equal(original.capture.applicationBirth.sequence, event.sequence);
      assert.deepEqual(yield* store.capturePreparedLaunch(input), original);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            store.capturePreparedLaunch({
              ...input,
              preparationEvent: { ...input.preparationEvent, sequence: event.sequence + 1 },
            }),
          ),
        ),
      );
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql`SELECT admission_id FROM orchestration_v2_ordinary_checkout_admissions`).length,
        1,
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect("accepts another durable intent without granting a second physical actor", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-intent-reuse");
      const original = yield* fixture.store.beginUse(fixture.input);
      const originalUse = original.record.subject.use;
      yield* fixture.store.revalidateUse(originalUse);
      const pending = yield* prepare(fixture.owner, "command:physical-intent-reuse:pending");
      const sink = yield* EventSink.EventSinkV2;
      const committed = yield* sink.commitCommand({
        commandId: pending.command.commandId,
        threadId: fixture.owner.id,
        commandType: pending.command.type,
        acceptedAt: yield* DateTime.now,
        ordinaryCheckout: pending.capture,
        events: [pending.event],
        effects: [],
      });
      assert.isTrue(committed.committed);
      const admission = yield* fixture.store.readAdmission(
        pending.command.commandId,
        fixture.owner.id,
      );
      assert.isNotNull(admission);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            fixture.store.beginUse({
              ...fixture.input,
              operationId: "physical-intent-reuse:second",
              admission: Ordinary.ordinaryCheckoutAdmissionRefV1(admission!),
            }),
          ),
        ),
      );
      assert.equal((yield* fixture.store.revalidateUse(originalUse)).state, "started");
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql`SELECT operation_id FROM orchestration_v2_worktree_path_admissions`).length,
        1,
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect("reserves once and makes a repeated begin observation-only", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-once");
      const first = yield* fixture.store.beginUse(fixture.input);
      assert.equal(first.status, "reserved");
      const repeated = yield* fixture.store.beginUse(fixture.input);
      assert.equal(repeated.status, "observe_only");
      const entered = yield* fixture.store.revalidateUse(first.record.subject.use);
      assert.equal(entered.state, "started");
      assert.isNotNull(entered.startedAt);
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql`SELECT operation_id FROM orchestration_v2_worktree_path_admissions`).length,
        1,
      );
    }).pipe(Effect.provide(layer)),
  );
  it.effect("refuses an altered worker before writing a physical reservation", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-stale");
      if (fixture.input.source.kind !== "outbox")
        return yield* Effect.die("Unexpected fixture source");
      const result = yield* Effect.exit(
        fixture.store.beginUse({
          ...fixture.input,
          source: { ...fixture.input.source, workerId: "another-worker" },
        }),
      );
      assert.isTrue(Exit.isFailure(result));
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql`SELECT operation_id FROM orchestration_v2_worktree_path_admissions`).length,
        0,
      );
    }).pipe(Effect.provide(layer)),
  );
  it.effect("retains an unknown entered use and refuses lease expiry as takeover evidence", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-unknown");
      const first = yield* fixture.store.beginUse(fixture.input);
      const use = first.record.subject.use;
      yield* fixture.store.revalidateUse(use);
      const unknown = yield* fixture.store.markUnknown(
        use,
        "Captured producer endpoint unavailable",
      );
      assert.equal(unknown.state, "unknown");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(fixture.store.revalidateUse(use))));
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE worktree_ownership_leases SET expires_at_ms = 1`;
      const other = yield* createFixture("owner:physical-foreign");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(prepare(other, "command:physical-foreign"))));
      const row = (yield* sql<{
        readonly state: string;
        readonly started_at: string;
        readonly outcome_json: string;
      }>`
        SELECT state, started_at, outcome_json FROM orchestration_v2_worktree_path_admissions`)[0]!;
      assert.equal(row.state, "unknown");
      assert.equal(row.started_at, unknown.startedAt);
      assert.include(row.outcome_json, "Captured producer endpoint unavailable");
      assert.equal((yield* fixture.store.beginUse(fixture.input)).status, "observe_only");
    }).pipe(Effect.provide(layer)),
  );
  it.effect("binds only the original executor and preserves its append-only fact", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("physical-binding");
      const begun = yield* fixture.store.beginUse(fixture.input);
      const use = begun.record.subject.use;
      const binding = yield* fixture.store.bindOutboxExecution(use);
      assert.equal(binding.executor.kind, "actual_outbox_claim");
      assert.equal(
        (yield* fixture.store.bindOutboxExecution(use)).associationId,
        binding.associationId,
      );
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{
        readonly ordinal: number;
        readonly event_kind: string;
        readonly evidence_json: string;
      }>`
        SELECT ordinal, event_kind, evidence_json FROM orchestration_v2_ordinary_checkout_execution_associations`;
      assert.equal(rows.length, 1);
      assert.equal(rows[0]!.ordinal, 0);
      assert.equal(rows[0]!.event_kind, "bind");
      assert.include(rows[0]!.evidence_json, "t3.ordinary-checkout-execution-liveness/v1");
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE orchestration_v2_ordinary_checkout_execution_associations SET event_kind = 'retire'`,
          ),
        ),
      );
      yield* fixture.store.markUnknown(use, "No issued managed actor closure");
      assert.isTrue(Exit.isFailure(yield* Effect.exit(fixture.store.bindOutboxExecution(use))));
      assert.equal(
        (yield* sql`SELECT ordinal FROM orchestration_v2_ordinary_checkout_execution_associations`)
          .length,
        1,
      );
    }).pipe(Effect.provide(layer)),
  );
});

describe("Ordinary checkout executor entry", () => {
  it.effect("requires the original accepted run and real live worker before native entry", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("entry-owner");
      const use = (yield* fixture.store.beginUse(fixture.input)).record.subject.use;
      if (use.source.kind !== "outbox") return yield* Effect.die("Unexpected fixture source");
      const ref = yield* fixture.store.bindExecution({
        originalUse: use,
        executor: { kind: "actual_outbox_claim", source: use.source },
        targetSource: fixture.input.targetSource,
      });
      const entered = yield* fixture.store.validateExecution(ref, true);
      assert.equal(entered.admission.run!.runId, fixture.run.id);
      assert.equal((yield* fixture.store.readUse(use.operationId))!.state, "started");
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.write({
        events: [
          {
            id: EventId.make("event:entry-owner:replaced"),
            type: "run.updated",
            threadId: fixture.owner.id,
            runId: fixture.run.id,
            occurredAt: yield* DateTime.now,
            payload: { ...fixture.run, activeAttemptId: RunAttemptId.make("attempt:replacement") },
          },
        ],
      });
      assert.isTrue(Exit.isFailure(yield* Effect.exit(fixture.store.validateExecution(ref, true))));
      assert.equal((yield* fixture.store.readUse(use.operationId))!.state, "started");
    }).pipe(Effect.provide(layer)),
  );
});

describe("Ordinary checkout execution succession", () => {
  it.effect("refuses native entry after only the original checkout lease expires", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("lease-only-entry-refusal");
      const use = (yield* fixture.store.beginUse(fixture.input)).record.subject.use;
      const ref = yield* fixture.store.bindOutboxExecution(use);
      yield* fixture.store.revalidateExecution(ref);
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE worktree_ownership_leases SET expires_at_ms = 1`;
      yield* TestClock.adjust("2 millis");
      const before = yield* sql`SELECT * FROM worktree_ownership_leases`;
      assert.equal(
        (yield* fixture.store.validateExecution(ref, true, false)).ref.associationId,
        ref.associationId,
      );
      assert.isTrue(Exit.isFailure(yield* Effect.exit(fixture.store.revalidateExecution(ref))));
      assert.deepEqual(yield* sql`SELECT * FROM worktree_ownership_leases`, before);
      assert.equal((yield* fixture.store.readUse(use.operationId))!.state, "started");
      assert.equal((yield* fixture.store.readExecutionHistory(use)).latestOrdinal, 0);
      assert.equal(
        Option.getOrThrow(yield* fixture.outbox.get(fixture.effect.id)).status,
        "running",
      );
    }).pipe(Effect.provide(layer)),
  );

  it.effect("retains executor liveness after its deadline and fences ordinary lease mutation", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("lifetime-fence");
      const use = (yield* fixture.store.beginUse(fixture.input)).record.subject.use;
      const ref = yield* fixture.store.bindOutboxExecution(use);
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE worktree_ownership_leases SET expires_at_ms = 1`;
      const before = yield* sql`SELECT * FROM worktree_ownership_leases`;
      const leases = yield* makeWorktreeOwnershipLeaseStore();
      assert.isTrue(
        Option.isNone(
          yield* leases.acquire({
            ...use.lease,
            leaseId: "replacement",
            nowMs: 100,
            expiresAtMs: 1000,
          }),
        ),
      );
      assert.isFalse(yield* leases.renew({ ...use.lease, nowMs: 100, expiresAtMs: 1000 }));
      yield* leases.release(use.lease);
      assert.deepEqual(yield* sql`SELECT * FROM worktree_ownership_leases`, before);
      const history = yield* fixture.store.readExecutionHistory(use);
      assert.equal(history.participants[0]!.state, "active");
      assert.equal(history.participants[0]!.ref.associationId, ref.associationId);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("records an unknown executor without releasing or reviving its physical use", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("lifetime-unknown");
      const use = (yield* fixture.store.beginUse(fixture.input)).record.subject.use;
      const ref = yield* fixture.store.bindOutboxExecution(use);
      const unknown = yield* fixture.store.retainExecutionUnknown(
        ref,
        "Original actor endpoint unavailable",
      );
      assert.equal(unknown.state, "unknown");
      const history = yield* fixture.store.readExecutionHistory(use);
      assert.equal(history.latestOrdinal, 1);
      assert.equal(history.participants[0]!.state, "unknown");
      yield* fixture.store.retainExecutionUnknown(
        ref,
        "Later observer cannot replace the original unknown fact",
      );
      assert.equal((yield* fixture.store.readExecutionHistory(use)).latestOrdinal, 1);
      assert.isTrue(Exit.isFailure(yield* Effect.exit(fixture.store.revalidateUse(use))));
    }).pipe(Effect.provide(layer)),
  );

  it.effect("refuses an appended renewal that changes its exact predecessor deadline", () =>
    Effect.gen(function* () {
      const fixture = yield* claimFixture("lifetime-predecessor");
      const use = (yield* fixture.store.beginUse(fixture.input)).record.subject.use;
      const ref = yield* fixture.store.bindOutboxExecution(use);
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{
        readonly recorded_at: string;
        readonly evidence_json: string;
      }>`SELECT recorded_at, evidence_json FROM orchestration_v2_ordinary_checkout_execution_associations`;
      const evidence = yield* decodeDeadline(rows[0]!.evidence_json);
      const invalid = canonicalJson({
        version: 1,
        schema: "t3.ordinary-checkout-execution-liveness/v1",
        kind: "renew",
        expiresAt: evidence.expiresAt,
        previousExpiry: "1970-01-01T00:00:00.001Z",
      });
      const encoded = canonicalJson(encodeExecutionRef(ref));
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations
      (operation_id, ordinal, predecessor_ordinal, association_id, admission_id, executor_kind, effect_id, event_kind, association_json, evidence_json, recorded_at)
      VALUES (${use.operationId}, 1, 0, ${ref.associationId}, ${use.admission.admissionId}, 'actual_outbox_claim', ${fixture.effect.id}, 'renew', ${encoded}, ${invalid}, ${rows[0]!.recorded_at})`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(fixture.store.readExecutionHistory(use))));
    }).pipe(Effect.provide(layer)),
  );
});

describe("Ordinary checkout EventSink admission", () => {
  it.effect(
    "commits the lease, admission, receipt, projection and exact effect association together",
    () =>
      Effect.gen(function* () {
        const owner = yield* createFixture("owner:atomic");
        const fixture = yield* prepare(owner, "command:atomic");
        const sink = yield* EventSink.EventSinkV2;
        const result = yield* sink.commitCommand({
          commandId: fixture.command.commandId,
          threadId: owner.id,
          commandType: fixture.command.type,
          acceptedAt: now,
          events: [fixture.event],
          ordinaryCheckout: fixture.capture,
          effects: [
            {
              id: "effect:atomic",
              commandId: fixture.command.commandId,
              threadId: owner.id,
              request: { type: "provider-turn.start", runId: RunId.make("run:atomic") },
            },
          ],
        });
        const store = yield* makeOrdinaryCheckoutStore();
        const admission = yield* store.readAdmission(fixture.command.commandId, owner.id);
        assert.isTrue(result.committed);
        assert.isNotNull(admission);
        assert.equal(admission!.receipt.resultSequence, result.receipt.resultSequence);
        assert.equal(admission!.eventBasis[0]!.eventId, fixture.event.id);
        const sql = yield* SqlClient.SqlClient;
        const links =
          yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = 'effect:atomic'`;
        assert.equal(links.length, 1);
        const titles = yield* sql<{
          readonly title: string;
        }>`SELECT title FROM orchestration_v2_projection_threads WHERE thread_id = ${owner.id}`;
        assert.equal(titles[0]!.title, "command:atomic");
      }).pipe(Effect.provide(layer)),
  );

  it.effect("rolls back all acceptance facts when admission attribution fails", () =>
    Effect.gen(function* () {
      const owner = yield* createFixture("owner:rollback");
      const fixture = yield* prepare(owner, "command:rollback");
      const sink = yield* EventSink.EventSinkV2;
      const result = yield* Effect.exit(
        sink.commitCommand({
          commandId: CommandId.make("command:other-body"),
          threadId: owner.id,
          commandType: fixture.command.type,
          acceptedAt: now,
          events: [fixture.event],
          ordinaryCheckout: fixture.capture,
          effects: [
            {
              id: "effect:rollback",
              commandId: CommandId.make("command:other-body"),
              threadId: owner.id,
              request: { type: "provider-turn.start", runId: RunId.make("run:rollback") },
            },
          ],
        }),
      );
      assert.isTrue(Exit.isFailure(result));
      const sql = yield* SqlClient.SqlClient;
      for (const table of [
        "worktree_ownership_leases",
        "orchestration_command_receipts",
        "orchestration_v2_ordinary_checkout_admissions",
        "orchestration_v2_ordinary_checkout_effect_links",
        "orchestration_v2_effect_outbox",
      ])
        assert.equal((yield* sql.unsafe(`SELECT * FROM ${table}`)).length, 0);
      const events =
        yield* sql`SELECT * FROM orchestration_events WHERE event_id = ${fixture.event.id}`;
      assert.equal(events.length, 0);
      const titles = yield* sql<{
        readonly title: string;
      }>`SELECT title FROM orchestration_v2_projection_threads WHERE thread_id = ${owner.id}`;
      assert.equal(titles[0]!.title, "Ownership fixture");
    }).pipe(Effect.provide(layer)),
  );

  it.effect(
    "refuses another thread on the same canonical path without changing the owner's lease",
    () =>
      Effect.gen(function* () {
        const owner = yield* createFixture("owner:exclusive");
        const fixture = yield* prepare(owner, "command:exclusive");
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.commitCommand({
          commandId: fixture.command.commandId,
          threadId: owner.id,
          commandType: fixture.command.type,
          acceptedAt: now,
          events: [fixture.event],
          effects: [],
          ordinaryCheckout: fixture.capture,
        });
        const sql = yield* SqlClient.SqlClient;
        const before = yield* sql`SELECT * FROM worktree_ownership_leases`;
        const contender = yield* createFixture("owner:contender");
        const refused = yield* Effect.exit(prepare(contender, "command:contender"));
        assert.isTrue(Exit.isFailure(refused));
        assert.deepEqual(yield* sql`SELECT * FROM worktree_ownership_leases`, before);
        assert.equal((yield* sql`SELECT * FROM orchestration_command_receipts`).length, 1);
      }).pipe(Effect.provide(layer)),
  );

  it.effect("replays acceptance without renewing or rotating the original lease", () =>
    Effect.gen(function* () {
      const owner = yield* createFixture("owner:replay-lease");
      const fixture = yield* prepare(owner, "command:replay-lease");
      const sink = yield* EventSink.EventSinkV2;
      const input = {
        commandId: fixture.command.commandId,
        threadId: owner.id,
        commandType: fixture.command.type,
        acceptedAt: now,
        events: [fixture.event],
        effects: [],
        ordinaryCheckout: fixture.capture,
      };
      yield* sink.commitCommand(input);
      const sql = yield* SqlClient.SqlClient;
      const before = yield* sql`SELECT * FROM worktree_ownership_leases`;
      const repeated = yield* sink.commitCommand({
        ...input,
        acceptedAt: DateTime.add(now, { minutes: 10 }),
      });
      assert.isFalse(repeated.committed);
      assert.deepEqual(yield* sql`SELECT * FROM worktree_ownership_leases`, before);
      assert.equal(
        (yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`).length,
        1,
      );
      yield* sink.validateOrdinaryCheckoutCommandReplay!(fixture.command, owner.id);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("rejects a changed command on replay and retains immutable admission bytes", () =>
    Effect.gen(function* () {
      const owner = yield* createFixture("owner:replay");
      const fixture = yield* prepare(owner, "command:replay");
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.commitCommand({
        commandId: fixture.command.commandId,
        threadId: owner.id,
        commandType: fixture.command.type,
        acceptedAt: now,
        events: [fixture.event],
        effects: [],
        ordinaryCheckout: fixture.capture,
      });
      const sql = yield* SqlClient.SqlClient;
      const before = yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`;
      if (fixture.command.type !== "message.dispatch")
        return yield* Effect.die("Unexpected fixture command");
      const changed = yield* (yield* makeOrdinaryCheckoutStore()).capture({
        command: {
          ...fixture.command,
          text: "Changed payload",
          messageId: MessageId.make("message:changed"),
        },
        threadId: owner.id,
        projectId,
        branch: owner.branch,
        canonicalProjectRoot: "/fixture/project",
        canonicalCheckoutPath: "/fixture/worktree",
        source: fixture.capture.source,
        leaseId: fixture.capture.capture.lease.leaseId,
      });
      const refusal = yield* Effect.exit(sink.validateOrdinaryCheckoutReplay!(changed));
      assert.isTrue(Exit.isFailure(refusal));
      assert.equal(
        encodeJson(yield* sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`),
        encodeJson(before),
      );
    }).pipe(Effect.provide(layer)),
  );
});
