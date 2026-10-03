import { assert, it } from "@effect/vitest";
import {
  type AgentSessionImportSource, AuthSessionId, CheckpointScopeId, CommandId, EventId, MessageId, NativeCreationHistoricalBinding, NodeId, OrchestrationV2Command, ProjectId, ProviderDriverKind, ProviderInstanceId,
  type OrchestrationV2ImportedHistoryDelivery, type OrchestrationV2StartWithImportedHistoryCommand,
  ProviderSessionId, ProviderThreadId, ProviderTurnId, RunAttemptId, RunId, ThreadId, TurnItemId,
  type OrchestrationV2AppThread, type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { makeLegacyStoppedRuntimeProofV1, makeLegacyProviderContinuationEvidenceV1 } from "../persistence/ProviderSessionRuntime.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import { NativeCreationRepository } from "../persistence/Services/NativeCreationRepository.ts";
import { layer as NativeCreationRepositoryLayer } from "../persistence/Layers/NativeCreationRepository.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import { makeWorktreeOwnershipLeaseStore } from "./WorktreeOwnershipLease.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { NativePreparationBinding, nativeCreationCanonicalJson, nativeCreationSha256, nativeCreationV2CommandDigest,
  nativePreparationCommand, validateNativeCreationPreparation } from "./NativeCreationPreparation.ts";
import { AuthSessionRepository, make as makeAuthSessions } from "../persistence/AuthSessions.ts";
import { NativeCreationBindingResolver, NativeCreationGrantResolver, makeNativeCreationAuthority,
  type NativeCreationGrant } from "./NativeCreationAuthority.ts";

const database = SqlitePersistenceMemory;
const stores = Layer.mergeAll(database,
  EventStore.layer.pipe(Layer.provideMerge(database)),
  ProjectionStore.layer.pipe(Layer.provideMerge(database)));
const testLayer = Layer.mergeAll(stores,
  EventSink.layer.pipe(Layer.provide(stores)),
  ProjectionMaintenance.layer.pipe(Layer.provide(stores)),
  EffectOutbox.layer.pipe(Layer.provide(database)));
const instanceId = ProviderInstanceId.make("fixture-codex");
const driver = ProviderDriverKind.make("codex");
const threadId = ThreadId.make("thread:store-native");
const projectId = ProjectId.make("project:store-native");
const commandId = CommandId.make("command:store-native");

function thread(now: DateTime.Utc): OrchestrationV2AppThread {
  return { createdBy: "user", creationSource: "web", id: threadId, projectId, title: "Fixture",
    providerInstanceId: instanceId, modelSelection: { instanceId, model: "fixture-model" },
    runtimeMode: "full-access", interactionMode: "default", branch: null, worktreePath: null,
    activeProviderThreadId: null, lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null, createdAt: now, updatedAt: now, archivedAt: null, settledOverride: null,
    settledAt: null, lastVisitedAt: null, deletedAt: null };
}
const seed = Effect.fnUntraced(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const value = thread(now);
  yield* sink.write({ events: [{ id: EventId.make("event:store-native:birth"), threadId,
    type: "thread.created", providerInstanceId: instanceId, occurredAt: now, payload: value }] });
  return { sink, now, thread: value };
});
const acceptance = Effect.fnUntraced(function* () {
  const value = yield* seed();
  const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId });
  return { ...value, input: { commandId, threadId, commandType: "message.dispatch", acceptedAt: value.now,
    nativeContext: { identity: { kind: "guarded_message_dispatch" as const, version: 2 as const,
      commandId, commandType: "message.dispatch", aggregateKind: "thread" as const, aggregateId: threadId,
      normalizedCommandDigest: "a".repeat(64), bindingDigest: "b".repeat(64) },
      snapshot: facts.commitSnapshot, revalidateAuthority: Effect.void },
    events: [{ id: EventId.make("event:store-native:accepted"), threadId,
      type: "thread.metadata-updated" as const, providerInstanceId: instanceId, occurredAt: value.now,
      payload: { ...value.thread, title: "Accepted" } }],
    effects: [{ id: "effect:store-native", commandId, threadId, request: { type: "terminal.cleanup" as const } }] } };
});

it.effect("native acceptance rolls back its identity, receipt, projection, effect and commit callbacks together", () =>
  Effect.gen(function* () {
    const { sink, input } = yield* acceptance();
    const callbackCount = yield* Ref.make(0);
    const result = yield* sink.withTransaction(Effect.gen(function* () {
      yield* sink.commitCommand(input);
      yield* sink.onCommit(Ref.update(callbackCount, (count) => count + 1));
      return yield* Effect.fail("fixture-rollback");
    })).pipe(Effect.result);
    assert.strictEqual(result._tag, "Failure");
    const facts = yield* sink.readNativeCommandFacts({ threadId, commandId });
    assert.isNull(facts.receipt);
    assert.isNull(facts.identity);
    assert.deepEqual(facts.events, []);
    assert.strictEqual(facts.projection?.thread.title, "Fixture");
    assert.strictEqual(yield* Ref.get(callbackCount), 0);
    assert.deepEqual(yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(commandId), []);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("exact native replay cannot strip identity or repeat effects", () =>
  Effect.gen(function* () {
    const { sink, input } = yield* acceptance();
    const first = yield* sink.commitCommand(input);
    const replay = yield* sink.commitCommand(input);
    assert.isTrue(first.committed);
    assert.isFalse(replay.committed);
    assert.deepEqual(replay.receipt, first.receipt);
    const { nativeContext: _context, ...ordinary } = input;
    assert.strictEqual((yield* sink.commitCommand(ordinary).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    const conflict = yield* sink.commitCommand({ ...input, nativeContext: { ...input.nativeContext,
      identity: { ...input.nativeContext.identity, bindingDigest: "c".repeat(64) } } }).pipe(Effect.flip);
    assert.strictEqual(conflict._tag, "NativeCommandPreconditionError");
    assert.strictEqual((yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(commandId)).length, 1);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("a direct contributing projection write invalidates the acceptance snapshot without an event", () =>
  Effect.gen(function* () {
    const { sink, input } = yield* acceptance();
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_threads SET title = 'interleaved-provider-write' WHERE thread_id = ${threadId}`;
    const error = yield* sink.commitCommand(input).pipe(Effect.flip);
    assert.strictEqual(error._tag, "NativeCommandPreconditionError");
    if (error._tag === "NativeCommandPreconditionError") assert.strictEqual(error.reason, "stale_target");
    assert.isNull((yield* sink.readNativeCommandFacts({ threadId, commandId })).receipt);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("an unbuffered enclosing SQL transaction rejects a sink write before appending", () =>
  Effect.gen(function* () {
    const { sink, now, thread: value } = yield* seed();
    const sql = yield* SqlClient.SqlClient;
    const before = yield* sink.latestSequence({ threadId });
    const result = yield* sql.withTransaction(sink.write({ events: [{
      id: EventId.make("event:store-native:unbuffered"), type: "thread.metadata-updated", threadId,
      providerInstanceId: instanceId, occurredAt: now, payload: { ...value, title: "Uncommitted" },
    }] })).pipe(Effect.result);
    assert.strictEqual(result._tag, "Failure");
    assert.strictEqual(yield* sink.latestSequence({ threadId }), before);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("unknown operation holds preserve their claim fence across cancellation, retry and process loss", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue([{ id: "held-effect", commandId, threadId, request: { type: "terminal.cleanup" } }]);
    const claimed = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker-a", leaseDurationMs: 60_000 }));
    const input = { effectId: claimed.id, workerId: "worker-a", operationId: "native-operation",
      evidence: { operationId: "native-operation", operation: "close_session" as const, outcome: "unknown" as const, threadId },
      expectedAttempt: claimed.attemptCount };
    assert.isFalse(yield* outbox.holdUnknown({ ...input, expectedAttempt: input.expectedAttempt + 1 }));
    assert.isTrue(yield* outbox.holdUnknown(input));
    assert.isFalse(yield* outbox.succeed({ effectId: claimed.id, workerId: "worker-a" }));
    assert.isFalse(yield* outbox.retry({ effectId: claimed.id, workerId: "worker-a", error: "unproved", delayMs: 0 }));
    assert.deepEqual(yield* outbox.cancelUnsettled({ threadId, effectTypes: ["terminal.cleanup"], reason: "restart" }), []);
    assert.deepEqual(yield* outbox.reconcileAfterProcessLoss, { requeued: 0, cancelled: 0 });
    assert.strictEqual((yield* outbox.listHeldByThreadId(threadId))[0]?.operationId, "native-operation");
    assert.isTrue(Option.isNone(yield* outbox.claimNext({ workerId: "worker-b", leaseDurationMs: 60_000 })));
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

const seedBinding = Effect.fnUntraced(function* () {
  const value = yield* seed();
  const providerThreadId = ProviderThreadId.make("provider-thread:store-native:first");
  const providerSessionId = ProviderSessionId.make("provider-session:store-native:first");
  const updatedThread = { ...value.thread, activeProviderThreadId: providerThreadId };
  const providerThread = { id: providerThreadId, driver, providerInstanceId: instanceId, providerSessionId,
    appThreadId: threadId, ownerNodeId: null, nativeThreadRef: { driver, nativeId: "native-first", strength: "strong" as const },
    nativeConversationHeadRef: null, status: "idle" as const, firstRunOrdinal: 1, lastRunOrdinal: 1,
    handoffIds: [], forkedFrom: null, createdAt: value.now, updatedAt: value.now };
  const session = { id: providerSessionId, driver, providerInstanceId: instanceId, status: "ready" as const,
    cwd: "/fixture", model: "fixture-model", capabilities: CodexProviderCapabilitiesV2,
    createdAt: value.now, updatedAt: value.now, lastError: null };
  const events: ReadonlyArray<OrchestrationV2DomainEvent> = [
    { id: EventId.make("event:store-native:binding-thread"), threadId, type: "thread.metadata-updated", occurredAt: value.now, payload: updatedThread },
    { id: EventId.make("event:store-native:session"), threadId, type: "provider-session.attached", driver, providerInstanceId: instanceId, occurredAt: value.now, payload: session },
    { id: EventId.make("event:store-native:provider-thread"), threadId, type: "provider-thread.updated", driver, providerInstanceId: instanceId, occurredAt: value.now, payload: providerThread },
  ];
  yield* value.sink.write({ events });
  const binding = { threadId, providerThreadId, providerSessionId, instanceId, driver, nativeThreadId: "native-first", runtimeGeneration: null };
  const registered = yield* value.sink.registerProviderRuntime({ expectedBinding: binding, expectedEvidenceRevision: 0,
    actualBinding: { ...binding, runtimeGeneration: "actual-generation-a" } });
  if (!registered.committed) return yield* Effect.die(registered.rejection);
  return { ...value, binding: { ...binding, runtimeGeneration: "actual-generation-a" }, revision: registered.evidenceRevision,
    providerThread, session, thread: updatedThread };
});

it.effect("current owner and application birth survive selection changes without weakening native START", () =>
  Effect.gen(function* () {
    const value = yield* seedBinding();
    const sql = yield* SqlClient.SqlClient;
    const birth = yield* value.sink.readApplicationThreadBirth(threadId);
    assert.isNotNull(birth);
    yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json,
      '$.modelSelection.instanceId', 'selected-next-owner') WHERE thread_id = ${threadId}`;
    assert.deepEqual((yield* value.sink.readCurrentProviderRuntimeOwner(threadId))?.binding, value.binding);
    assert.deepEqual(yield* value.sink.readApplicationThreadBirth(threadId), birth);
    assert.isFalse((yield* value.sink.writeIfProviderBindingCurrent({ expectedBinding: value.binding,
      expectedEvidenceRevision: value.revision, events: [] })).committed);
    const { driver: _driver, ...runtimeBinding } = value.binding;
    const observation = { status: "working" as const, binding: runtimeBinding, observedAt: DateTime.formatIso(value.now) };
    const published = yield* value.sink.writeIfCurrentProviderRuntimeOwner({ expectedBinding: value.binding,
      expectedEvidenceRevision: value.revision, events: [], observation, revalidateCurrentOwner: Effect.void });
    assert.isTrue(published.committed);
    assert.deepEqual((yield* value.sink.readProviderRuntimeEvidence(threadId))?.observation, observation);
    const revision = (yield* value.sink.readProviderRuntimeEvidence(threadId))!.evidenceRevision;
    const rejected = yield* value.sink.writeIfCurrentProviderRuntimeOwner({ expectedBinding: value.binding,
      expectedEvidenceRevision: revision, events: [], observation,
      revalidateCurrentOwner: sql`UPDATE orchestration_v2_provider_runtime_evidence SET evidence_revision = evidence_revision + 1 WHERE thread_id = ${threadId}`.pipe(Effect.asVoid) });
    assert.isFalse(rejected.committed);
    yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET provider_instance_id = 'wrong-owner' WHERE provider_session_id = ${value.binding.providerSessionId}`;
    assert.isNull(yield* value.sink.readCurrentProviderRuntimeOwner(threadId));
    assert.isNull(yield* value.sink.readApplicationThreadBirth(threadId));
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

const leaseCleanupFixture = Effect.fnUntraced(function* () {
  const value = yield* seedBinding();
  const sql = yield* SqlClient.SqlClient;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const ownerBirth = (yield* value.sink.readApplicationBirthRecord(threadId))!;
  const ownedThread = { ...value.thread, worktreePath: "/fixture/owned-worktree", branch: "fixture-owned" };
  yield* value.sink.write({ events: [{ id: EventId.make("event:lease-cleanup:worktree"), type: "thread.metadata-updated",
    threadId, occurredAt: value.now, payload: ownedThread }] });
  const lease = { resourcePath: ownedThread.worktreePath, leaseId: "lease:cleanup:original", ownerThreadId: threadId,
    ownerIncarnation: JSON.stringify(["t3.orchestration-v2.thread-birth/v1", ownerBirth.eventId, ownerBirth.sequence]),
    branch: ownedThread.branch, acquiredAtMs: 1, renewedAtMs: 1, expiresAtMs: 100_000 };
  yield* sql`INSERT INTO worktree_ownership_leases
    (resource_path, lease_id, owner_thread_id, owner_incarnation, branch, acquired_at_ms, renewed_at_ms, expires_at_ms)
    VALUES (${lease.resourcePath}, ${lease.leaseId}, ${threadId}, ${lease.ownerIncarnation}, ${lease.branch}, 1, 1, 100000)`;
  const deleteCommandId = CommandId.make("command:lease-cleanup:delete");
  const providerEffectId = `effect:${deleteCommandId}:provider-session.detach:${value.session.id}`;
  const input = { commandId: deleteCommandId, threadId, commandType: "thread.delete", acceptedAt: value.now,
    events: [
      { id: EventId.make("event:lease-cleanup:delete"), type: "thread.deleted" as const, threadId, occurredAt: value.now,
        payload: { ...ownedThread, deletedAt: value.now } },
      { id: EventId.make("event:lease-cleanup:detach"), type: "provider-session.detached" as const, threadId, occurredAt: value.now,
        payload: { providerSessionId: value.session.id, detachedAt: value.now, reason: "Thread deleted." } },
    ], effects: [
      { id: providerEffectId, commandId: deleteCommandId, threadId, request: { type: "provider-session.detach" as const,
        providerSessionId: value.session.id, detail: "Thread deleted.", revokeMcpCredential: true } },
      { id: `effect:${deleteCommandId}:terminal.cleanup`, commandId: deleteCommandId, threadId, request: { type: "terminal.cleanup" as const } },
      { id: `effect:${deleteCommandId}:attachment.cleanup`, commandId: deleteCommandId, threadId,
        request: { type: "attachment.cleanup" as const, attachmentIds: ["attachment:lease-cleanup"] } },
    ] };
  return { ...value, sql, outbox, ownerBirth, lease, input, providerEffectId };
});

const deletionWorktreeFixture = Effect.fnUntraced(function* (leased = true) {
  const value = yield* leaseCleanupFixture();
  yield* value.sql`INSERT INTO projection_projects
    (project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at)
    VALUES (${projectId}, 'Deletion project', '/fixture/repo', '[]', ${DateTime.formatIso(value.now)}, ${DateTime.formatIso(value.now)}, NULL)`;
  if (!leased) yield* value.sql`DELETE FROM worktree_ownership_leases WHERE resource_path = ${value.lease.resourcePath}`;
  const command: Extract<OrchestrationV2Command, { readonly type: "thread.delete" }> = {
    type: "thread.delete", commandId: value.input.commandId, threadId,
    worktreeRemoval: { projectId, path: value.lease.resourcePath, branch: value.lease.branch, force: true },
  };
  return { ...value, command, input: { ...value.input, deletionCommand: command },
    worktreeEffectId: EventSink.deletionWorktreeEffectIdV1(command.commandId, threadId) };
});

const deletionObservedFixture = Effect.fnUntraced(function* (leased = true) {
  const value = yield* seed();
  const sql = yield* SqlClient.SqlClient;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const ownerBirth = (yield* value.sink.readApplicationBirthRecord(threadId))!;
  const appThread = { ...value.thread, worktreePath: "/fixture/repo/observed-worktree", branch: "observed-branch" };
  yield* value.sink.write({ events: [{ id: EventId.make("event:observed-cleanup:path"), threadId,
    type: "thread.metadata-updated", occurredAt: value.now, payload: appThread }] });
  yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at)
    VALUES (${projectId}, 'Observed cleanup project', '/fixture/repo', '[]', ${DateTime.formatIso(value.now)}, ${DateTime.formatIso(value.now)}, NULL)`;
  const lease = { resourcePath: appThread.worktreePath, leaseId: "lease:observed-cleanup", ownerThreadId: threadId,
    ownerIncarnation: JSON.stringify(["t3.orchestration-v2.thread-birth/v1", ownerBirth.eventId, ownerBirth.sequence]),
    branch: appThread.branch, acquiredAtMs: 1, renewedAtMs: 1, expiresAtMs: 100_000 };
  if (leased) yield* sql`INSERT INTO worktree_ownership_leases
    (resource_path, lease_id, owner_thread_id, owner_incarnation, branch, acquired_at_ms, renewed_at_ms, expires_at_ms)
    VALUES (${lease.resourcePath}, ${lease.leaseId}, ${threadId}, ${lease.ownerIncarnation}, ${lease.branch}, 1, 1, 100000)`;
  const command: Extract<OrchestrationV2Command, { readonly type: "thread.delete" }> = { type: "thread.delete",
    commandId: CommandId.make("command:observed-cleanup:delete"), threadId,
    worktreeRemoval: { projectId, path: lease.resourcePath, branch: lease.branch, force: true } };
  const terminalEffectId = `effect:${command.commandId}:terminal.cleanup`;
  yield* value.sink.commitCommand({ commandId: command.commandId, threadId, commandType: command.type,
    deletionCommand: command, acceptedAt: value.now, events: [{ id: EventId.make("event:observed-cleanup:delete"), threadId,
      type: "thread.deleted", occurredAt: value.now, payload: { ...appThread, deletedAt: value.now } }],
    effects: [{ id: terminalEffectId, commandId: command.commandId, threadId, request: { type: "terminal.cleanup" } }] });
  const capture = { managerId: "manager:observed-cleanup", threadId, ownerBirth, status: "captured" as const,
    managedTargetsOnly: true as const, targets: [] };
  const tasks = yield* value.sink.prepareDeletionCleanupTaskBindings({ commandId: command.commandId, terminalCapture: capture });
  const terminal = tasks.find((task) => task.effectId === terminalEffectId)!;
  const worktreeEffectId = EventSink.deletionWorktreeEffectIdV1(command.commandId, threadId);
  yield* sql`UPDATE orchestration_v2_effect_outbox SET available_at = '2099-01-01T00:00:00.000Z' WHERE effect_id = ${worktreeEffectId}`;
  const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker:observed-terminal", leaseDurationMs: 60_000 }));
  assert.strictEqual(claim.id, terminalEffectId);
  const observation: EventSink.ManagedTerminalDeletionObservationV1 = { version: 1, kind: "managed_terminal",
    effectId: terminalEffectId, bindingSha256: terminal.bindingSha256, workerId: claim.leaseOwner!, expectedAttempt: claim.attemptCount,
    capture, result: { status: "observed_absent", managedTargetsOnly: true, processExitObserved: false,
      descendantsQuiescence: "unavailable", futureWakeClosure: "unavailable" }, observedAt: DateTime.formatIso(value.now) };
  return { ...value, sql, outbox, ownerBirth, lease, command, capture, terminal, terminalEffectId, observation, worktreeEffectId };
});

const providerDeletionObservedFixture = Effect.fnUntraced(function* () {
  const value = yield* leaseCleanupFixture();
  yield* value.sink.commitCommand({ ...value.input,
    deletionCommand: { type: "thread.delete", commandId: value.input.commandId, threadId } });
  const task = (yield* value.sink.readDeletionCleanupTask(value.providerEffectId))!;
  if (task.task.kind !== "provider") return yield* Effect.die("Provider deletion fixture lost its original task");
  yield* value.sql`UPDATE orchestration_v2_effect_outbox SET available_at = '2099-01-01T00:00:00.000Z'
    WHERE thread_id = ${threadId} AND effect_id <> ${value.providerEffectId}`;
  const claim = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:observed-provider", leaseDurationMs: 60_000 }));
  assert.strictEqual(claim.id, value.providerEffectId);
  const nativeOperation = { operationId: claim.id, operation: "close_session" as const, threadId,
    providerThreadId: task.task.expectedBinding.providerThreadId, providerSessionId: task.task.expectedBinding.providerSessionId,
    instanceId: task.task.expectedBinding.instanceId, runtimeGeneration: task.task.expectedBinding.runtimeGeneration };
  const observation: EventSink.ManagedProviderDeletionObservationV1 = { version: 1, kind: "managed_provider",
    effectId: claim.id, bindingSha256: task.bindingSha256, workerId: claim.leaseOwner!, expectedAttempt: claim.attemptCount,
    binding: task.task.expectedBinding, evidenceRevision: task.task.evidenceRevision, nativeOperation,
    result: { status: "stopped", operationId: claim.id, binding: task.task.expectedBinding,
      cancelledPendingStart: false, interruptedProviderTurnIds: [], readback: { threadAttached: false } },
    observedAt: DateTime.formatIso(value.now) };
  const input = { effectId: claim.id, bindingSha256: task.bindingSha256, expectedLatestOrdinal: -1, observation, coveredHolds: [] };
  return { ...value, task, claim, observation, input, deletionCommandId: value.input.commandId };
});

it.effect("managed provider completion covers its exact native hold without changing generic retry or immutable audit", () =>
  Effect.gen(function* () {
    const value = yield* providerDeletionObservedFixture();
    const unknown = yield* value.sink.recordObservedDeletionCleanupOutcome({ ...value.input,
      observation: { ...value.observation, result: { status: "unknown", reason: "original managed close not yet observed" } } });
    assert.strictEqual(unknown.ordinal, 0);
    assert.deepEqual(unknown.outcome, { taskId: value.providerEffectId, result: null, effect: "unknown" });
    const holds = yield* value.outbox.listHeldByThreadId(threadId);
    assert.strictEqual(holds.length, 1);
    assert.deepEqual(holds[0]!.evidence, { ...value.observation.nativeOperation, outcome: "unknown" });
    assert.isFalse(yield* value.outbox.succeed({ effectId: value.providerEffectId, workerId: value.observation.workerId }));
    assert.isFalse(yield* value.outbox.retry({ effectId: value.providerEffectId, workerId: value.observation.workerId,
      error: "generic retry remains blocked", delayMs: 0 }));
    const confirmed = yield* value.sink.recordObservedDeletionCleanupOutcome({ ...value.input, expectedLatestOrdinal: unknown.ordinal, coveredHolds: holds });
    assert.deepEqual(confirmed.outcome, { taskId: value.providerEffectId, result: "succeeded", effect: "confirmed" });
    assert.deepEqual(yield* value.sink.readUnresolvedDeletionCleanupHolds(threadId), []);
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(threadId), holds);
    assert.isTrue(yield* value.sink.completeObservedDeletionCleanup({ effectId: value.providerEffectId,
      bindingSha256: value.task.bindingSha256, expectedLatestOrdinal: confirmed.ordinal }));
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.providerEffectId)).status, "succeeded");
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(`effect:${value.deletionCommandId}:terminal.cleanup`)).status, "pending");
    yield* value.sink.write({ events: [{ id: EventId.make("event:provider-observation:later-birth"), type: "thread.created", threadId,
      occurredAt: value.now, payload: value.thread }] });
    assert.isFalse((yield* value.sink.readDeletionCleanupTaskOwnerBirth(value.providerEffectId))!.matchesOriginal);
    assert.deepEqual(yield* value.sink.readUnresolvedDeletionCleanupHolds(threadId), []);
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(threadId), holds);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("managed provider outcome rejects changed operation, revision, runtime and hold coverage", () =>
  Effect.gen(function* () {
    const value = yield* providerDeletionObservedFixture();
    const unknown = yield* value.sink.recordObservedDeletionCleanupOutcome({ ...value.input,
      observation: { ...value.observation, result: { status: "unknown", reason: "awaiting matched managed stop" } } });
    const holds = yield* value.outbox.listHeldByThreadId(threadId);
    const input = { ...value.input, expectedLatestOrdinal: unknown.ordinal, coveredHolds: holds };
    const result = value.observation.result;
    if (result.status !== "stopped") return yield* Effect.die("Provider fixture has no typed managed result");
    for (const candidate of [
      { ...input, observation: { ...value.observation, evidenceRevision: value.observation.evidenceRevision + 1 } },
      { ...input, observation: { ...value.observation, binding: { ...value.observation.binding, runtimeGeneration: "replacement-generation" } } },
      { ...input, observation: { ...value.observation, nativeOperation: { ...value.observation.nativeOperation, operationId: "unrelated-operation" } } },
      { ...input, observation: { ...value.observation, result: { ...result, operationId: "unrelated-stop-result" } } },
      { ...input, observation: { ...value.observation, expectedAttempt: value.observation.expectedAttempt + 1 } },
      { ...input, coveredHolds: [] },
      { ...input, coveredHolds: [{ ...holds[0]!, heldAt: new Date(Date.parse(holds[0]!.heldAt) + 1).toISOString() }] },
    ]) assert.strictEqual((yield* value.sink.recordObservedDeletionCleanupOutcome(candidate).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual((yield* value.sink.readDeletionCleanupTaskOutcome(value.providerEffectId))?.ordinal, unknown.ordinal);
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.providerEffectId)).status, "running");
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(threadId), holds);
    assert.isFalse(yield* value.sink.completeObservedDeletionCleanup({ effectId: value.providerEffectId,
      bindingSha256: value.task.bindingSha256, expectedLatestOrdinal: unknown.ordinal }));
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("new managed provider completion cannot use an original task after a same-time replacement is also deleted", () =>
  Effect.gen(function* () {
    const value = yield* providerDeletionObservedFixture();
    const unknown = yield* value.sink.recordObservedDeletionCleanupOutcome({ ...value.input,
      observation: { ...value.observation, result: { status: "unknown", reason: "before replaced owner" } } });
    const holds = yield* value.outbox.listHeldByThreadId(threadId);
    yield* value.sink.write({ events: [
      { id: EventId.make("event:provider-observation:replacement"), type: "thread.created", threadId, occurredAt: value.now, payload: value.thread },
      { id: EventId.make("event:provider-observation:replacement-deleted"), type: "thread.deleted", threadId, occurredAt: value.now,
        payload: { ...value.thread, deletedAt: value.now } },
    ] });
    assert.isNull(yield* value.sink.readApplicationBirthRecord(threadId));
    assert.isFalse((yield* value.sink.readDeletionCleanupTaskOwnerBirth(value.providerEffectId))!.matchesOriginal);
    assert.strictEqual((yield* value.sink.recordObservedDeletionCleanupOutcome({ ...value.input,
      expectedLatestOrdinal: unknown.ordinal, coveredHolds: holds }).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual((yield* value.sink.readDeletionCleanupTaskOutcome(value.providerEffectId))?.ordinal, unknown.ordinal);
    assert.deepEqual(yield* value.sink.readUnresolvedDeletionCleanupHolds(threadId), holds);
    assert.deepEqual(yield* value.sink.readDeletionCleanupTask(value.providerEffectId), value.task);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

const deletionObservedStartFixture = Effect.fnUntraced(function* (leased = true) {
  const value = yield* deletionObservedFixture(leased);
  const terminal = yield* value.sink.recordObservedDeletionCleanupOutcome({ effectId: value.terminalEffectId,
    bindingSha256: value.terminal.bindingSha256, expectedLatestOrdinal: -1, observation: value.observation, coveredHolds: [] });
  assert.strictEqual(terminal.outcome.effect, "absent");
  assert.isTrue(yield* value.sink.completeObservedDeletionCleanup({ effectId: value.terminalEffectId,
    bindingSha256: value.terminal.bindingSha256, expectedLatestOrdinal: terminal.ordinal }));
  yield* value.sql`UPDATE orchestration_v2_effect_outbox SET available_at = ${DateTime.formatIso(value.now)} WHERE effect_id = ${value.worktreeEffectId}`;
  const claim = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:observed-worktree", leaseDurationMs: 60_000 }));
  assert.strictEqual(claim.id, value.worktreeEffectId);
  const binding = (yield* value.sink.readDeletionWorktreeTask(claim.id))!;
  const target = EventSink.deletionWorktreeRemovalTargetV1(binding)!;
  const started = yield* value.sink.startDeletionWorktreeRemoval({ effectId: claim.id, bindingSha256: binding.bindingSha256,
    workerId: claim.leaseOwner!, expectedAttempt: claim.attemptCount, target });
  assert.strictEqual(started.status, "start_now");
  if (started.status !== "start_now") return yield* Effect.die("Recorded fixture start was not admitted");
  const observation: typeof EventSink.DeletionWorktreeRemovalObservationSchemaV1.Type = { version: 1, start: started.start, startOrdinal: 0,
    operation: { kind: "executed", exitCode: 0, completion: "exited" },
    before: { registration: { status: "complete", projectRoot: target.projectRoot, gitCommonDirectory: "/fixture/repo/.git",
      entries: [{ path: target.projectRoot, head: "b".repeat(40), branch: "refs/heads/main", bare: false },
        { path: target.path, head: "a".repeat(40), branch: `refs/heads/${target.branch}`, bare: false }] },
      filesystem: { status: "present", path: target.path } },
    after: { registration: { status: "complete", projectRoot: target.projectRoot, gitCommonDirectory: "/fixture/repo/.git",
      entries: [{ path: target.projectRoot, head: "b".repeat(40), branch: "refs/heads/main", bare: false }] },
      filesystem: { status: "absent", path: target.path } }, observedAt: DateTime.formatIso(value.now) };
  return { ...value, binding, claim, started, observation };
});

it.effect("original lease absence creates truthful cleanup tasks and replacement leases do not rewrite that capture", () =>
  Effect.gen(function* () {
    const value = yield* deletionObservedFixture(false);
    assert.strictEqual(value.terminal.version, 1);
    assert.isNull(yield* value.sink.readLeaseCleanupTask(value.terminalEffectId));
    assert.deepEqual(yield* value.sink.readDeletionCleanupTask(value.terminalEffectId), value.terminal);
    assert.isNull(yield* value.sink.readDeletionCleanupTaskOutcome(value.terminalEffectId));
    assert.deepEqual(yield* value.sink.readDeletionCleanupTaskOwnerBirth(value.terminalEffectId),
      { latestApplicationBirth: value.ownerBirth, matchesOriginal: true });
    if (value.terminal.version === 1) assert.deepEqual(value.terminal.leaseInventory,
      { status: "absent", resourcePath: value.lease.resourcePath });
    const command = yield* value.sink.readThreadDeletionCommand(value.command.commandId);
    assert.deepEqual(command?.inventory.leaseInventory, { status: "absent" });
    assert.deepEqual((yield* value.sink.prepareDeletionCleanupTaskBindings({ commandId: value.command.commandId,
      terminalCapture: null }))[0], value.terminal);
    yield* value.sql`INSERT INTO worktree_ownership_leases
      (resource_path, lease_id, owner_thread_id, owner_incarnation, branch, acquired_at_ms, renewed_at_ms, expires_at_ms)
      VALUES (${value.lease.resourcePath}, 'replacement-lease', ${threadId}, ${value.lease.ownerIncarnation}, ${value.lease.branch}, 1, 1, 100000)`;
    assert.deepEqual(yield* value.sink.readDeletionCleanupTask(value.terminalEffectId), value.terminal);
    assert.strictEqual((yield* value.sink.readDeletionWorktreeExecutionBasis(value.worktreeEffectId))?.currentLease, "replacement");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("later lease absence cannot promote a full captured lease into an originally unleased cleanup task", () =>
  Effect.gen(function* () {
    const value = yield* deletionObservedFixture();
    assert.strictEqual(value.terminal.version, 2);
    yield* value.sql`DELETE FROM worktree_ownership_leases WHERE resource_path = ${value.lease.resourcePath}`;
    assert.deepEqual(yield* value.sink.readDeletionCleanupTask(value.terminalEffectId), value.terminal);
    assert.deepEqual(yield* value.sink.readLeaseCleanupTask(value.terminalEffectId), value.terminal);
    assert.strictEqual((yield* value.sink.readDeletionWorktreeExecutionBasis(value.worktreeEffectId))?.reason, "original_lease_identity_changed");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("pinned cleanup distinguishes a later same-ID same-time application birth even after its replacement is deleted", () =>
  Effect.gen(function* () {
    const value = yield* deletionObservedFixture();
    assert.isNull(yield* value.sink.readApplicationBirthRecord(threadId));
    assert.deepEqual(yield* value.sink.readDeletionCleanupTaskOwnerBirth(value.terminalEffectId),
      { latestApplicationBirth: value.ownerBirth, matchesOriginal: true });
    const replacementThread = { ...value.thread, worktreePath: "/fixture/replacement-worktree" };
    assert.strictEqual((yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath })).status, "reserved");
    yield* value.sink.write({ events: [{ id: EventId.make("event:observed-cleanup:replacement-birth"), threadId,
      type: "thread.created", occurredAt: value.now, payload: replacementThread }] });
    const replacement = (yield* value.sink.readApplicationBirthRecord(threadId))!;
    assert.notStrictEqual(replacement.eventId, value.ownerBirth.eventId);
    assert.deepEqual(yield* value.sink.readDeletionCleanupTaskOwnerBirth(value.terminalEffectId),
      { latestApplicationBirth: replacement, matchesOriginal: false });
    yield* value.sink.write({ events: [{ id: EventId.make("event:observed-cleanup:replacement-deleted"), threadId,
      type: "thread.deleted", occurredAt: value.now, payload: { ...replacementThread, deletedAt: value.now } }] });
    assert.isNull(yield* value.sink.readApplicationBirthRecord(threadId));
    assert.deepEqual(yield* value.sink.readDeletionCleanupTaskOwnerBirth(value.terminalEffectId),
      { latestApplicationBirth: replacement, matchesOriginal: false });
    assert.deepEqual(yield* value.sink.readDeletionCleanupTask(value.terminalEffectId), value.terminal);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("pinned cleanup birth currentness stays unknown without a matching current projection", () =>
  Effect.gen(function* () {
    const value = yield* deletionObservedFixture(false);
    yield* value.sql`UPDATE orchestration_v2_projection_threads
      SET payload_json = json_set(payload_json, '$.projectId', 'project:unattributed-replacement') WHERE thread_id = ${threadId}`;
    assert.isNull(yield* value.sink.readDeletionCleanupTaskOwnerBirth(value.terminalEffectId));
    assert.deepEqual(yield* value.sink.readDeletionCleanupTask(value.terminalEffectId), value.terminal);
    assert.isNull(yield* value.sink.readDeletionCleanupTaskOwnerBirth("effect:missing-cleanup-task"));
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("qualified managed completion covers only its exact immutable hold while generic retry and audit semantics remain fenced", () =>
  Effect.gen(function* () {
    const value = yield* deletionObservedFixture();
    yield* value.sink.recordLeaseCleanupTaskOutcome({ effectId: value.terminalEffectId, workerId: value.observation.workerId,
      expectedAttempt: value.observation.expectedAttempt, outcome: { taskId: value.terminalEffectId, result: null, effect: "unknown" },
      evidence: { reason: "awaiting_managed_readback" } });
    const awaiting = yield* value.sink.readDeletionCleanupTaskOutcome(value.terminalEffectId);
    assert.strictEqual(awaiting?.ordinal, 0);
    assert.deepEqual(awaiting?.outcome, { taskId: value.terminalEffectId, result: null, effect: "unknown" });
    const held = yield* value.outbox.listHeldByThreadId(threadId);
    assert.strictEqual(held.length, 1);
    const changedHeldAt = new Date(Date.parse(held[0]!.heldAt) + 1).toISOString();
    assert.notStrictEqual(changedHeldAt, held[0]!.heldAt);
    const input = { effectId: value.terminalEffectId, bindingSha256: value.terminal.bindingSha256,
      expectedLatestOrdinal: 0, observation: value.observation, coveredHolds: held };
    for (const candidate of [
      { ...input, observation: { ...input.observation, expectedAttempt: input.observation.expectedAttempt + 1 } },
      { ...input, coveredHolds: [{ ...held[0]!, heldAt: changedHeldAt }] },
      { ...input, coveredHolds: [] },
      { ...input, observation: { ...input.observation, capture: { ...value.capture, managerId: "wrong-manager" } } },
    ]) assert.strictEqual((yield* value.sink.recordObservedDeletionCleanupOutcome(candidate).pipe(Effect.result))._tag, "Failure");
    const accepted = yield* value.sink.recordObservedDeletionCleanupOutcome(input);
    assert.strictEqual(accepted.ordinal, 1);
    assert.strictEqual(accepted.outcome.effect, "absent");
    const latest = yield* value.sink.readDeletionCleanupTaskOutcome(value.terminalEffectId);
    assert.strictEqual(latest?.ordinal, accepted.ordinal);
    assert.deepEqual(latest?.outcome, accepted.outcome);
    assert.deepEqual(latest?.correlation, { workerId: value.observation.workerId,
      expectedAttempt: value.observation.expectedAttempt, bindingSha256: value.terminal.bindingSha256, evidence: accepted.evidence });
    assert.deepEqual(yield* value.sink.readUnresolvedDeletionCleanupHolds(threadId), []);
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(threadId), held);
    assert.isFalse(yield* value.outbox.succeed({ effectId: value.terminalEffectId, workerId: value.observation.workerId }));
    assert.isFalse(yield* value.outbox.retry({ effectId: value.terminalEffectId, workerId: value.observation.workerId, error: "old hold", delayMs: 0 }));
    assert.isFalse(yield* value.sink.completeObservedDeletionCleanup({ effectId: value.terminalEffectId,
      bindingSha256: value.terminal.bindingSha256, expectedLatestOrdinal: 0 }));
    assert.isTrue(yield* value.sink.completeObservedDeletionCleanup({ effectId: value.terminalEffectId,
      bindingSha256: value.terminal.bindingSha256, expectedLatestOrdinal: 1 }));
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.terminalEffectId)).status, "succeeded");
    assert.deepEqual(yield* value.sink.readDeletionCleanupTaskOutcome(value.terminalEffectId), latest);
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(threadId), held);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

for (const leased of [true, false]) {
it.effect(`qualified worktree retirement ${leased ? "releases only the complete captured lease atomically" : "rereads original absence without calling lease release"}`, () =>
  Effect.gen(function* () {
    const value = yield* deletionObservedStartFixture(leased);
    const startOutcome = yield* value.sink.readDeletionCleanupTaskOutcome(value.worktreeEffectId);
    assert.strictEqual(startOutcome?.ordinal, value.observation.startOrdinal);
    assert.deepEqual(startOutcome?.correlation.evidence, value.started.start);
    yield* value.sink.revalidateDeletionWorktreeRemovalStart(value.started.start, 0);
    assert.strictEqual((yield* value.sink.startDeletionWorktreeRemoval({ effectId: value.worktreeEffectId, bindingSha256: value.binding.bindingSha256,
      workerId: value.claim.leaseOwner!, expectedAttempt: value.claim.attemptCount, target: value.started.start.target })).status, "observe_only");
    const input = { effectId: value.worktreeEffectId, bindingSha256: value.binding.bindingSha256,
      expectedLatestOrdinal: 0, observation: value.observation, coveredHolds: [] };
    for (const observation of [
      { ...value.observation, startOrdinal: 1 },
      { ...value.observation, start: { ...value.observation.start, expectedAttempt: value.claim.attemptCount + 1 } },
      { ...value.observation, after: { ...value.observation.after, filesystem: { status: "absent" as const, path: "/fixture/wrong" } } },
    ]) assert.strictEqual((yield* value.sink.recordObservedDeletionCleanupOutcome({ ...input, observation }).pipe(Effect.result))._tag, "Failure");
    const qualified = yield* value.sink.recordObservedDeletionCleanupOutcome(input);
    assert.strictEqual(qualified.outcome.effect, "confirmed");
    assert.strictEqual((yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: value.command.commandId })).state, "pending");
    const ownership = yield* makeWorktreeOwnershipLeaseStore();
    const finalize = { effectId: value.worktreeEffectId, bindingSha256: value.binding.bindingSha256, expectedLatestOrdinal: qualified.ordinal };
    if (leased) yield* value.sql`UPDATE worktree_ownership_leases SET lease_id = 'replacement-before-finalization' WHERE resource_path = ${value.lease.resourcePath}`;
    else yield* value.sql`INSERT INTO worktree_ownership_leases
      (resource_path, lease_id, owner_thread_id, owner_incarnation, branch, acquired_at_ms, renewed_at_ms, expires_at_ms)
      VALUES (${value.lease.resourcePath}, 'replacement-before-finalization', ${threadId}, ${value.lease.ownerIncarnation}, ${value.lease.branch}, 1, 1, 100000)`;
    assert.strictEqual((yield* ownership.finalizeDeletionWorktreeCleanup(finalize)).status, "retained");
    assert.strictEqual((yield* value.sql<{ readonly lease_id: string }>`SELECT lease_id FROM worktree_ownership_leases
      WHERE resource_path = ${value.lease.resourcePath}`)[0]?.lease_id, "replacement-before-finalization");
    if (leased) yield* value.sql`UPDATE worktree_ownership_leases SET lease_id = ${value.lease.leaseId} WHERE resource_path = ${value.lease.resourcePath}`;
    else yield* value.sql`DELETE FROM worktree_ownership_leases WHERE resource_path = ${value.lease.resourcePath}`;
    if (leased) {
      yield* value.sql`CREATE TRIGGER fixture_change_full_lease_before_release AFTER UPDATE ON orchestration_v2_effect_outbox
        WHEN NEW.status = 'succeeded' AND EXISTS (SELECT 1 FROM orchestration_v2_lease_cleanup_task_bindings
          WHERE effect_id = NEW.effect_id AND json_extract(task_json, '$.kind') = 'worktree')
        BEGIN UPDATE worktree_ownership_leases SET renewed_at_ms = renewed_at_ms + 1 WHERE resource_path =
          (SELECT json_extract(task_json, '$.worktree.path') FROM orchestration_v2_lease_cleanup_task_bindings WHERE effect_id = NEW.effect_id); END`;
      assert.strictEqual((yield* ownership.finalizeDeletionWorktreeCleanup(finalize).pipe(Effect.result))._tag, "Failure");
      yield* value.sql`DROP TRIGGER fixture_change_full_lease_before_release`;
      assert.deepEqual(Option.getOrThrow(yield* ownership.getByResourcePath(value.lease.resourcePath)), value.lease);
      assert.strictEqual((yield* value.sink.readDeletionWorktreeExecutionBasis(value.worktreeEffectId))?.currentLease, "original");
      assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.worktreeEffectId)).status, "running");
      assert.strictEqual((yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath })).status, "reserved");
    }
    assert.strictEqual((yield* ownership.finalizeDeletionWorktreeCleanup(finalize)).status, "completed");
    assert.isTrue(Option.isNone(yield* ownership.getByResourcePath(value.lease.resourcePath)));
    yield* value.sql`INSERT INTO worktree_ownership_leases
      (resource_path, lease_id, owner_thread_id, owner_incarnation, branch, acquired_at_ms, renewed_at_ms, expires_at_ms)
      VALUES (${value.lease.resourcePath}, 'lawful-later-fresh-lease', ${threadId}, ${value.lease.ownerIncarnation}, ${value.lease.branch}, 2, 2, 100000)`;
    const historical = yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: value.command.commandId });
    assert.strictEqual(historical.state, "completed");
    assert.strictEqual(historical.currentLease, "replacement");
    assert.strictEqual((yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath })).status, "available");
    assert.strictEqual((yield* value.sink.revalidateDeletionWorktreeRemovalStart(value.started.start, 0).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual((yield* value.sink.startDeletionWorktreeRemoval({ effectId: value.worktreeEffectId, bindingSha256: value.binding.bindingSha256,
      workerId: value.claim.leaseOwner!, expectedAttempt: value.claim.attemptCount, target: value.started.start.target })).status, "observe_only");
    assert.strictEqual((yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: value.command.commandId })).state, "completed");
    assert.strictEqual((yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath })).status, "available");
    assert.isNotNull(yield* value.sink.readDeletionWorktreeTask(value.worktreeEffectId));
    assert.strictEqual((yield* ownership.finalizeDeletionWorktreeCleanup(finalize)).status, "completed");
    assert.strictEqual(Option.getOrThrow(yield* ownership.getByResourcePath(value.lease.resourcePath)).leaseId, "lawful-later-fresh-lease");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);
}

it.effect("exit zero with incomplete readback stays unknown and recovered qualified absence covers the preserved original hold", () =>
  Effect.gen(function* () {
    const value = yield* deletionObservedStartFixture();
    const incomplete = { ...value.observation, after: { ...value.observation.after,
      registration: { status: "unavailable" as const, reason: "truncated-registration" } } };
    const input = { effectId: value.worktreeEffectId, bindingSha256: value.binding.bindingSha256,
      expectedLatestOrdinal: 0, observation: incomplete, coveredHolds: [] };
    const unknown = yield* value.sink.recordObservedDeletionCleanupOutcome(input);
    assert.deepEqual(unknown.outcome, { taskId: value.worktreeEffectId, result: null, effect: "unknown" });
    const held = yield* value.outbox.listHeldByThreadId(threadId);
    assert.strictEqual(held.length, 1);
    assert.strictEqual((yield* value.sink.readDeletionWorktreeExecutionBasis(value.worktreeEffectId))?.admission?.state, "unknown");
    assert.isFalse(yield* value.sink.completeObservedDeletionCleanup({ effectId: value.worktreeEffectId,
      bindingSha256: value.binding.bindingSha256, expectedLatestOrdinal: unknown.ordinal }));
    const recovered = yield* value.sink.recordObservedDeletionCleanupOutcome({ ...input, expectedLatestOrdinal: unknown.ordinal,
      observation: { ...value.observation, operation: { kind: "reconciled", completion: "unknown" } }, coveredHolds: held });
    assert.strictEqual(recovered.outcome.effect, "absent");
    assert.deepEqual(yield* value.sink.readUnresolvedDeletionCleanupHolds(threadId), []);
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(threadId), held);
    yield* value.sink.recordLeaseCleanupTaskOutcome({ effectId: value.worktreeEffectId, workerId: value.claim.leaseOwner!,
      expectedAttempt: value.claim.attemptCount, outcome: { taskId: value.worktreeEffectId, result: null, effect: "unknown" },
      evidence: { reason: "later_readback_unavailable" } });
    const latest = yield* value.sink.readDeletionCleanupTaskOutcome(value.worktreeEffectId);
    assert.strictEqual(latest?.ordinal, recovered.ordinal + 1);
    assert.deepEqual(latest?.outcome, { taskId: value.worktreeEffectId, result: null, effect: "unknown" });
    assert.deepEqual(yield* value.sink.readUnresolvedDeletionCleanupHolds(threadId), held);
    assert.isFalse(yield* value.sink.completeObservedDeletionCleanup({ effectId: value.worktreeEffectId,
      bindingSha256: value.binding.bindingSha256, expectedLatestOrdinal: recovered.ordinal }));
    assert.strictEqual((yield* value.sink.finalizeDeletionWorktreeCleanup({ effectId: value.worktreeEffectId,
      bindingSha256: value.binding.bindingSha256, expectedLatestOrdinal: recovered.ordinal })).status, "retained");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

for (const leased of [true, false]) {
it.effect(`requested worktree cleanup captures the original ${leased ? "complete lease" : "positive lease absence"} and observes without mutation`, () =>
  Effect.gen(function* () {
    const value = yield* deletionWorktreeFixture(leased);
    const accepted = yield* value.sink.commitCommand(value.input);
    const task = (yield* value.sink.readDeletionWorktreeTask(value.worktreeEffectId))!;
    assert.isNotNull(task);
    assert.deepEqual(task.ownerBirth, value.ownerBirth);
    assert.deepEqual(task.task.canonicalCommand, value.command);
    assert.deepEqual(task.leaseInventory, leased ? { status: "original", lease: value.lease } : { status: "absent" });
    assert.deepEqual(task.task.prerequisiteEffectIds, value.input.effects.map((effect) => effect.id).sort());
    assert.strictEqual(task.task.captureStatus, "captured");
    assert.strictEqual(task.task.worktree.path, value.lease.resourcePath);
    assert.isTrue(task.deletion.sequence < accepted.receipt.resultSequence);
    assert.deepEqual(Option.getOrThrow(yield* value.outbox.get(value.worktreeEffectId)).request, { type: "worktree.cleanup" });
    assert.isNull(yield* value.sink.readLeaseCleanupTask(value.worktreeEffectId));
    const sequence = yield* value.sink.latestSequence({ threadId });
    const effects = yield* value.outbox.listByCommandId(value.command.commandId);
    const observation = yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: value.command.commandId });
    assert.strictEqual(observation.state, "pending");
    assert.strictEqual(observation.currentLease, leased ? "original" : "absent");
    assert.deepEqual(observation.deletion, { eventId: task.deletion.eventId, sequence: task.deletion.sequence, resultSequence: accepted.receipt.resultSequence });
    assert.deepEqual(yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: value.command.commandId }), observation);
    assert.strictEqual(yield* value.sink.latestSequence({ threadId }), sequence);
    assert.deepEqual(yield* value.outbox.listByCommandId(value.command.commandId), effects);
    assert.isFalse((yield* value.sink.commitCommand(value.input)).committed);
    for (const candidate of [
      { ...value.input, deletionCommand: { type: "thread.delete" as const, commandId: value.command.commandId, threadId } },
      { ...value.input, deletionCommand: { ...value.command, worktreeRemoval: { ...value.command.worktreeRemoval!, path: "/fixture/replaced" } } },
      { ...value.input, deletionCommand: { ...value.command, worktreeRemoval: { ...value.command.worktreeRemoval!, branch: "replacement-branch" } } },
    ]) assert.strictEqual((yield* value.sink.commitCommand(candidate).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).threadDeletion?.command, value.command);
    assert.deepEqual(yield* value.outbox.listByCommandId(value.command.commandId), effects);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);
}

it.effect("worktree cleanup capture failure and enclosing rollback preserve the original path, lease, events, receipt and notifications", () =>
  Effect.gen(function* () {
    const value = yield* deletionWorktreeFixture();
    const sequence = yield* value.sink.latestSequence({ threadId });
    const notified = yield* Ref.make(0);
    yield* value.sql`CREATE TRIGGER fixture_deletion_command_failure BEFORE INSERT ON orchestration_v2_thread_deletion_commands
      BEGIN SELECT RAISE(ABORT, 'fixture deletion capture rollback'); END`;
    assert.strictEqual((yield* value.sink.commitCommand(value.input).pipe(Effect.result))._tag, "Failure");
    yield* value.sql`DROP TRIGGER fixture_deletion_command_failure`;
    assert.strictEqual((yield* value.sink.withTransaction(Effect.gen(function* () {
      yield* value.sink.commitCommand(value.input);
      yield* value.sink.onCommit(Ref.update(notified, (count) => count + 1));
      return yield* Effect.fail("fixture enclosing deletion rollback");
    })).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual(yield* Ref.get(notified), 0);
    assert.strictEqual(yield* value.sink.latestSequence({ threadId }), sequence);
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).receipt);
    assert.deepEqual(yield* value.outbox.listByCommandId(value.command.commandId), []);
    assert.isNull(yield* value.sink.readDeletionWorktreeTask(value.worktreeEffectId));
    assert.strictEqual((yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath })).status, "available");
    assert.isTrue((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).leaseCurrent);
    assert.isNotNull(yield* value.sink.readApplicationBirthRecord(threadId));
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
    assert.strictEqual((yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath })).status, "reserved");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("a pending worktree reservation fences same-owner lease SQL and actual path recreation before mutation", () =>
  Effect.gen(function* () {
    const value = yield* deletionWorktreeFixture();
    yield* value.sink.commitCommand(value.input);
    const admission = yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath });
    assert.strictEqual(admission.status, "reserved");
    assert.strictEqual(admission.reservations.length, 1);
    assert.strictEqual(admission.admissions.length, 1);
    for (const mutation of [
      value.sql`UPDATE worktree_ownership_leases SET lease_id = 'same-owner-reacquired' WHERE resource_path = ${value.lease.resourcePath}`,
      value.sql`UPDATE worktree_ownership_leases SET renewed_at_ms = 200000 WHERE resource_path = ${value.lease.resourcePath}`,
      value.sql`DELETE FROM worktree_ownership_leases WHERE resource_path = ${value.lease.resourcePath}`,
    ]) assert.strictEqual((yield* value.sink.withDeletionWorktreeSqlMutation({ path: value.lease.resourcePath }, mutation).pipe(Effect.result))._tag, "Failure");
    assert.isTrue((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).leaseCurrent);
    const replacementThreadId = ThreadId.make("thread:replacement-path");
    const replacement = { ...thread(value.now), id: replacementThreadId, worktreePath: value.lease.resourcePath, branch: value.lease.branch,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: replacementThreadId } };
    const sequence = yield* value.sink.latestSequence();
    assert.strictEqual((yield* value.sink.write({ events: [{ id: EventId.make("event:replacement-path:birth"),
      type: "thread.created", threadId: replacementThreadId, occurredAt: value.now, payload: replacement }] }).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual(yield* value.sink.latestSequence(), sequence);
    assert.isNull(yield* value.sink.readApplicationBirthRecord(replacementThreadId));
    const callback = yield* Ref.make(0);
    yield* value.sink.withDeletionWorktreeSqlMutation({ path: "/fixture/unrelated-worktree" }, Ref.update(callback, (count) => count + 1));
    assert.strictEqual(yield* Ref.get(callback), 1);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("new no-consent deletion is observable while unavailable historical consent cannot be promoted", () =>
  Effect.gen(function* () {
    const value = yield* deletionWorktreeFixture();
    const command = { type: "thread.delete" as const, commandId: value.command.commandId, threadId };
    yield* value.sink.commitCommand({ ...value.input, deletionCommand: command });
    assert.isNull(yield* value.sink.readDeletionWorktreeTask(value.worktreeEffectId));
    assert.strictEqual((yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: command.commandId })).state, "not_requested");
    assert.strictEqual((yield* value.sink.commitCommand(value.input).pipe(Effect.result))._tag, "Failure");
    yield* value.sql`DROP TRIGGER orchestration_v2_thread_deletion_commands_no_delete`;
    yield* value.sql`DELETE FROM orchestration_v2_thread_deletion_commands WHERE command_id = ${command.commandId}`;
    const historical = yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: command.commandId });
    assert.strictEqual(historical.state, "unknown");
    assert.strictEqual(historical.reason, "original_worktree_consent_unavailable");
    assert.strictEqual((yield* value.sink.commitCommand({ ...value.input, deletionCommand: command }).pipe(Effect.result))._tag, "Failure");
    assert.isNull(yield* value.sink.readDeletionWorktreeTask(value.worktreeEffectId));
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("an owned worktree unknown outcome retains the path admission, exact claim and latest observation through process loss", () =>
  Effect.gen(function* () {
    const value = yield* deletionWorktreeFixture();
    const deletionCommand = { ...value.command, worktreeRemoval: { ...value.command.worktreeRemoval!, branch: "unmatched-consent-branch" } };
    yield* value.sink.commitCommand({ ...value.input, deletionCommand });
    assert.strictEqual((yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: deletionCommand.commandId })).state, "retained");
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET available_at = '2099-01-01T00:00:00.000Z'
      WHERE command_id = ${deletionCommand.commandId} AND effect_id <> ${value.worktreeEffectId}`;
    const claim = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:worktree-unknown", leaseDurationMs: 60_000 }));
    assert.strictEqual(claim.id, value.worktreeEffectId);
    assert.strictEqual((yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: deletionCommand.commandId })).state, "retained");
    const input = { effectId: value.worktreeEffectId, workerId: "worker:worktree-unknown", expectedAttempt: claim.attemptCount,
      outcome: { taskId: value.worktreeEffectId, result: null, effect: "unknown" as const }, evidence: { reason: "complete_removal_preconditions_unavailable" } };
    for (const candidate of [{ ...input, workerId: "unowned-worker" }, { ...input, expectedAttempt: claim.attemptCount + 1 },
      { ...input, outcome: { ...input.outcome, result: "succeeded" as const, effect: "confirmed" as const } }])
      assert.strictEqual((yield* value.sink.recordLeaseCleanupTaskOutcome(candidate).pipe(Effect.result))._tag, "Failure");
    yield* value.sql`CREATE TRIGGER fixture_worktree_hold_failure BEFORE INSERT ON orchestration_v2_unknown_effect_holds
      BEGIN SELECT RAISE(ABORT, 'fixture worktree unknown rollback'); END`;
    assert.strictEqual((yield* value.sink.recordLeaseCleanupTaskOutcome(input).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* value.sql`SELECT ordinal FROM orchestration_v2_lease_cleanup_task_outcomes WHERE effect_id = ${value.worktreeEffectId}`, []);
    assert.strictEqual((yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath })).admissions[0]?.state, "reserved");
    yield* value.sql`DROP TRIGGER fixture_worktree_hold_failure`;
    assert.deepEqual(yield* value.sink.recordLeaseCleanupTaskOutcome(input), input.outcome);
    assert.deepEqual(yield* value.sink.recordLeaseCleanupTaskOutcome(input), input.outcome);
    assert.strictEqual((yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath })).admissions[0]?.state, "unknown");
    const observation = yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: deletionCommand.commandId });
    assert.strictEqual(observation.state, "unknown");
    assert.deepEqual(observation.removalOutcome, { result: null, effect: "unknown" });
    assert.isFalse(yield* value.outbox.succeed({ effectId: claim.id, workerId: input.workerId }));
    assert.isFalse(yield* value.outbox.retry({ effectId: claim.id, workerId: input.workerId, error: "uncertain removal", delayMs: 0 }));
    assert.deepEqual(yield* value.outbox.cancelUnsettled({ threadId, effectTypes: ["worktree.cleanup"], reason: "restart" }), []);
    yield* value.outbox.reconcileAfterProcessLoss;
    assert.deepEqual(yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: deletionCommand.commandId }), observation);
    assert.strictEqual((yield* value.outbox.listHeldByThreadId(threadId))[0]?.effectId, claim.id);
    assert.strictEqual((yield* value.sink.withDeletionWorktreeSqlMutation({ path: value.lease.resourcePath },
      value.sql`DELETE FROM worktree_ownership_leases WHERE resource_path = ${value.lease.resourcePath}`).pipe(Effect.result))._tag, "Failure");
    assert.isTrue((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).leaseCurrent);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("worktree native start retains pending prerequisites and rejects changed targets without recording an operation", () =>
  Effect.gen(function* () {
    const value = yield* deletionWorktreeFixture();
    yield* value.sink.commitCommand(value.input);
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET available_at = '2099-01-01T00:00:00.000Z'
      WHERE command_id = ${value.command.commandId} AND effect_id <> ${value.worktreeEffectId}`;
    const claim = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:worktree-start", leaseDurationMs: 60_000 }));
    const basis = (yield* value.sink.readDeletionWorktreeExecutionBasis(value.worktreeEffectId))!;
    assert.strictEqual(claim.id, value.worktreeEffectId);
    assert.isFalse(basis.inventoryComplete);
    assert.isFalse(basis.prerequisitesReady);
    assert.strictEqual(basis.prerequisites.length, value.input.effects.length);
    assert.isNull(basis.start);
    const target = EventSink.deletionWorktreeRemovalTargetV1(basis.binding)!;
    const input = { effectId: claim.id, bindingSha256: basis.binding.bindingSha256,
      workerId: "worker:worktree-start", expectedAttempt: claim.attemptCount, target };
    for (const candidate of [input, { ...input, workerId: "unowned-worker" },
      { ...input, expectedAttempt: claim.attemptCount + 1 }, { ...input, target: { ...target, path: "/fixture/replacement-path" } }])
      assert.strictEqual((yield* value.sink.startDeletionWorktreeRemoval(candidate)).status, "retained");
    assert.deepEqual(yield* value.sql`SELECT ordinal FROM orchestration_v2_lease_cleanup_task_outcomes WHERE effect_id = ${claim.id}`, []);
    assert.strictEqual((yield* value.sink.readDeletionWorktreeExecutionBasis(claim.id))?.admission?.state, "reserved");
    assert.deepEqual(Option.getOrThrow(yield* value.outbox.get(claim.id)), claim);
    assert.strictEqual((yield* value.sink.recordLeaseCleanupTaskOutcome({ ...input,
      outcome: { taskId: claim.id, result: null, effect: "unknown" }, evidence: {
        schema: "t3.deletion-worktree-removal-start/v1", ...input, startedAt: DateTime.formatIso(value.now),
      } }).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(threadId), []);
    assert.strictEqual((yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: value.command.commandId })).state, "pending");
    assert.isTrue((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).leaseCurrent);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("server policy cleanup captures force-false origin without changing client deletion identity or replaying policy", () =>
  Effect.gen(function* () {
    const value = yield* deletionWorktreeFixture();
    const command = { type: "thread.delete" as const, commandId: value.command.commandId, threadId };
    const rules = { worktreeAfterDays: null, worktreeOnMerge: false, worktreeOnDelete: true, worktreeUnchanged: true };
    const request = { origin: "policy" as const, projectId, path: value.lease.resourcePath, branch: value.lease.branch,
      force: false as const, rules };
    const input = { ...value.input, deletionCommand: command, deletionWorktreePolicy: { status: "captured" as const,
      request, revalidate: Effect.succeed(rules) } };
    assert.isTrue((yield* value.sink.commitCommand(input)).committed);
    const original = (yield* value.sink.readThreadDeletionCommand(command.commandId))!;
    const task = (yield* value.sink.readDeletionWorktreeTask(value.worktreeEffectId))!;
    assert.deepEqual(original.command, command);
    assert.deepEqual(task.task.canonicalCommand, command);
    assert.deepEqual(EventSink.deletionWorktreeCleanupRequestV1(task), request);
    assert.isUndefined(task.task.consent);
    assert.isFalse(EventSink.deletionWorktreeRemovalTargetV1(task)!.force);
    assert.strictEqual((yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: command.commandId })).state, "pending");
    const calls = yield* Ref.make(0);
    const replay = yield* value.sink.commitCommand({ ...input, deletionWorktreePolicy: { ...input.deletionWorktreePolicy,
      revalidate: Ref.update(calls, (count) => count + 1).pipe(Effect.as({ ...rules, worktreeOnDelete: false })) } });
    assert.isFalse(replay.committed);
    assert.strictEqual(yield* Ref.get(calls), 0);
    assert.deepEqual(yield* value.sink.readDeletionWorktreeTask(value.worktreeEffectId), task);
    assert.strictEqual((yield* value.sink.commitCommand(value.input).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual((yield* value.sink.readCommandReceiptIdentity(command.commandId)).threadDeletion?.command, command);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("unavailable policy capture preserves logical deletion and retains its original path without claiming no request", () =>
  Effect.gen(function* () {
    const value = yield* deletionWorktreeFixture();
    const command = { type: "thread.delete" as const, commandId: value.command.commandId, threadId };
    const accepted = yield* value.sink.commitCommand({ ...value.input, deletionCommand: command,
      deletionWorktreePolicy: { status: "unavailable" } });
    assert.isTrue(accepted.committed);
    assert.isNull(yield* value.sink.readDeletionWorktreeTask(value.worktreeEffectId));
    const observation = yield* value.sink.observeThreadDeletionCleanup({ threadId, commandId: command.commandId });
    assert.strictEqual(observation.state, "unknown");
    assert.strictEqual(observation.reason, "worktree_policy_capture_unavailable");
    assert.strictEqual((yield* value.sink.readDeletionWorktreePathAdmission({ path: value.lease.resourcePath })).status, "unavailable");
    assert.strictEqual((yield* value.sink.withDeletionWorktreeSqlMutation({ path: value.lease.resourcePath },
      value.sql`DELETE FROM worktree_ownership_leases WHERE resource_path = ${value.lease.resourcePath}`).pipe(Effect.result))._tag, "Failure");
    assert.isTrue((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).leaseCurrent);
    assert.strictEqual((yield* value.sink.readCommandReceiptIdentity(command.commandId)).receipt?.status, "accepted");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("deletion pins its pre-detach provider cleanup and an unknown close retains the exact lease and operation through restart", () =>
  Effect.gen(function* () {
    const value = yield* leaseCleanupFixture();
    assert.strictEqual((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).ownerPresence, "current");
    yield* value.sink.commitCommand(value.input);
    assert.isNull(yield* value.sink.readCurrentProviderRuntimeOwner(threadId));
    const task = (yield* value.sink.readLeaseCleanupTask(value.providerEffectId))!;
    assert.strictEqual(task.task.kind, "provider");
    if (task.task.kind !== "provider") return yield* Effect.die("Wrong cleanup task kind");
    assert.deepEqual(task.task.expectedBinding, value.binding);
    assert.strictEqual(task.task.evidenceRevision, value.revision);
    assert.deepEqual(task.ownerBirth, value.ownerBirth);
    assert.deepEqual(task.lease, value.lease);
    const prepared = yield* value.sink.prepareLeaseCleanupTaskBindings({ lease: value.lease, terminalCapture: null });
    assert.strictEqual(prepared.ownerPresence, "absent");
    assert.isFalse(prepared.inventoryComplete);
    assert.strictEqual(prepared.tasks.length, 2);
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET available_at = '2099-01-01T00:00:00.000Z'
      WHERE command_id = ${value.input.commandId} AND effect_id <> ${value.providerEffectId}`;
    const providerClaim = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:lease-cleanup", leaseDurationMs: 60_000 }));
    assert.strictEqual(providerClaim.id, value.providerEffectId);
    const outcome = { taskId: value.providerEffectId, result: null, effect: "unknown" as const };
    const evidence = { operationId: value.providerEffectId, operation: "close_session" as const, outcome: "unknown" as const,
      threadId, providerThreadId: value.binding.providerThreadId, providerSessionId: value.binding.providerSessionId,
      instanceId: value.binding.instanceId, runtimeGeneration: value.binding.runtimeGeneration };
    const input = { effectId: value.providerEffectId, workerId: "worker:lease-cleanup", expectedAttempt: providerClaim.attemptCount, outcome, evidence };
    yield* value.sql`CREATE TRIGGER fixture_cleanup_outcome_failure BEFORE INSERT ON orchestration_v2_lease_cleanup_task_outcomes
      BEGIN SELECT RAISE(ABORT, 'fixture outcome rollback'); END`;
    assert.strictEqual((yield* value.sink.recordLeaseCleanupTaskOutcome(input).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(threadId), []);
    yield* value.sql`DROP TRIGGER fixture_cleanup_outcome_failure`;
    assert.strictEqual((yield* value.sink.recordLeaseCleanupTaskOutcome({ ...input, workerId: "stale-worker" }).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* value.sink.recordLeaseCleanupTaskOutcome(input), outcome);
    assert.deepEqual(yield* value.sink.recordLeaseCleanupTaskOutcome(input), outcome);
    const held = yield* value.outbox.listHeldByThreadId(threadId);
    assert.strictEqual(held.length, 1);
    assert.deepEqual(held[0]!.evidence, evidence);
    assert.strictEqual(held[0]!.effectId, value.providerEffectId);
    assert.strictEqual(held[0]!.workerId, input.workerId);
    assert.strictEqual(held[0]!.expectedAttempt, input.expectedAttempt);
    assert.isFalse(yield* value.outbox.holdUnknown({ ...input, operationId: value.providerEffectId }));
    assert.strictEqual((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).outcomes.length, 1);
    assert.isFalse(yield* value.outbox.succeed({ effectId: value.providerEffectId, workerId: input.workerId }));
    yield* value.outbox.reconcileAfterProcessLoss;
    assert.strictEqual((yield* value.outbox.listHeldByThreadId(threadId)).length, 1);
    assert.isTrue((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).leaseCurrent);
    assert.strictEqual((yield* value.sink.recordLeaseCleanupTaskOutcome({ ...input,
      outcome: { ...outcome, result: "succeeded", effect: "confirmed" } }).pipe(Effect.result))._tag, "Failure");
    yield* value.sql`UPDATE orchestration_v2_provider_runtime_evidence SET provider_session_id = 'replacement-session',
      runtime_generation = 'replacement-generation', evidence_revision = evidence_revision + 1 WHERE thread_id = ${threadId}`;
    assert.deepEqual((yield* value.sink.readLeaseCleanupTask(value.providerEffectId))?.task, task.task);
    assert.isTrue((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).leaseCurrent);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("a failed pinned cleanup insertion rolls back the deletion receipt, detach, events and effects while retaining its lease", () =>
  Effect.gen(function* () {
    const value = yield* leaseCleanupFixture();
    const sequence = yield* value.sink.latestSequence({ threadId });
    yield* value.sql`CREATE TRIGGER fixture_cleanup_binding_failure BEFORE INSERT ON orchestration_v2_lease_cleanup_task_bindings
      BEGIN SELECT RAISE(ABORT, 'fixture binding rollback'); END`;
    assert.strictEqual((yield* value.sink.commitCommand(value.input).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual(yield* value.sink.latestSequence({ threadId }), sequence);
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.input.commandId)).receipt);
    assert.deepEqual(yield* value.outbox.listByCommandId(value.input.commandId), []);
    assert.isNotNull(yield* value.sink.readCurrentProviderRuntimeOwner(threadId));
    assert.strictEqual((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).ownerPresence, "current");
    yield* value.sql`DROP TRIGGER fixture_cleanup_binding_failure`;
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
    assert.isNotNull(yield* value.sink.readLeaseCleanupTask(value.providerEffectId));
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("resource cleanup unknowns retain the exact attachment task and claim without fabricating a provider operation", () =>
  Effect.gen(function* () {
    const value = yield* leaseCleanupFixture();
    yield* value.sink.commitCommand(value.input);
    yield* value.sink.prepareLeaseCleanupTaskBindings({ lease: value.lease, terminalCapture: null });
    const effectId = value.input.effects[2]!.id;
    const task = (yield* value.sink.readLeaseCleanupTask(effectId))!;
    assert.deepEqual(task.task, { kind: "attachment", attachmentIds: ["attachment:lease-cleanup"] });
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET available_at = '2099-01-01T00:00:00.000Z'
      WHERE command_id = ${value.input.commandId} AND effect_id <> ${effectId}`;
    const claim = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:resource-cleanup", leaseDurationMs: 60_000 }));
    assert.strictEqual(claim.id, effectId);
    const evidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1 = { version: 1, kind: "resource_cleanup",
      operationId: effectId, threadId, taskKind: "attachment", bindingSha256: task.bindingSha256, outcome: "unknown" };
    const hold = { effectId, workerId: "worker:resource-cleanup", expectedAttempt: claim.attemptCount, evidence };
    for (const candidate of [
      { ...hold, workerId: "stale-worker" }, { ...hold, expectedAttempt: claim.attemptCount + 1 },
      { ...hold, evidence: { ...evidence, bindingSha256: "f".repeat(64) } },
      { ...hold, evidence: { ...evidence, taskKind: "terminal" as const } },
      { ...hold, evidence: { ...evidence, threadId: ThreadId.make("unrelated-thread") } },
    ]) assert.isFalse(yield* value.outbox.holdResourceCleanupUnknown(candidate));
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = '1970-01-01T00:00:00.000Z' WHERE effect_id = ${effectId}`;
    assert.isFalse(yield* value.outbox.holdResourceCleanupUnknown(hold));
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = ${claim.leaseExpiresAt} WHERE effect_id = ${effectId}`;
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(threadId), []);
    const outcome = { taskId: effectId, result: null, effect: "unknown" as const };
    const input = { ...hold, outcome, evidence: { reason: "attachment_reference_proof_unavailable" } };
    yield* value.sql`CREATE TRIGGER fixture_resource_hold_failure BEFORE INSERT ON orchestration_v2_unknown_effect_holds
      BEGIN SELECT RAISE(ABORT, 'fixture resource hold rollback'); END`;
    assert.strictEqual((yield* value.sink.recordLeaseCleanupTaskOutcome(input).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).outcomes.length, 0);
    yield* value.sql`DROP TRIGGER fixture_resource_hold_failure`;
    assert.deepEqual(yield* value.sink.recordLeaseCleanupTaskOutcome(input), outcome);
    assert.deepEqual(yield* value.sink.recordLeaseCleanupTaskOutcome(input), outcome);
    const held = yield* value.outbox.listHeldByThreadId(threadId);
    assert.strictEqual(held.length, 1);
    assert.deepEqual(held[0]!.evidence, evidence);
    assert.strictEqual(held[0]!.expectedAttempt, claim.attemptCount);
    assert.isFalse(yield* value.outbox.holdResourceCleanupUnknown(hold));
    assert.isFalse(yield* value.outbox.succeed({ effectId, workerId: hold.workerId }));
    assert.isFalse(yield* value.outbox.retry({ effectId, workerId: hold.workerId, error: "unresolved resource cleanup", delayMs: 0 }));
    assert.deepEqual(yield* value.outbox.cancelUnsettled({ threadId, effectTypes: ["attachment.cleanup"], reason: "restart" }), []);
    const remaining = yield* value.outbox.listByCommandId(value.input.commandId);
    assert.isTrue(remaining.filter((effect) => effect.id !== effectId).every((effect) => effect.status === "pending"));
    yield* value.outbox.reconcileAfterProcessLoss;
    assert.deepEqual((yield* value.outbox.listHeldByThreadId(threadId))[0]!.evidence, evidence);
    assert.isTrue(Option.isNone(yield* value.outbox.claimNext({ workerId: "replacement-worker", leaseDurationMs: 60_000 })));
    assert.isTrue((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).leaseCurrent);
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.input.effects[1]!.id)).status, "pending");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("a missing cleanup task can only retain an exact resource unknown claim without granting target access", () =>
  Effect.gen(function* () {
    const value = yield* leaseCleanupFixture();
    yield* value.sink.commitCommand(value.input);
    const effectId = value.input.effects[1]!.id;
    assert.isNull(yield* value.sink.readLeaseCleanupTask(effectId));
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET available_at = '2099-01-01T00:00:00.000Z'
      WHERE command_id = ${value.input.commandId} AND effect_id <> ${effectId}`;
    const claim = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:missing-cleanup-task", leaseDurationMs: 60_000 }));
    assert.strictEqual(claim.id, effectId);
    const evidence: EffectOutbox.ResourceCleanupUnknownEvidenceV1 = { version: 1, kind: "resource_cleanup",
      operationId: effectId, threadId, taskKind: "terminal", bindingSha256: null, reason: "task_binding_unavailable", outcome: "unknown" };
    const input = { effectId, workerId: "worker:missing-cleanup-task", expectedAttempt: claim.attemptCount, evidence };
    for (const candidate of [
      { ...input, workerId: "different-worker" }, { ...input, expectedAttempt: claim.attemptCount + 1 },
      { ...input, evidence: { ...evidence, threadId: ThreadId.make("different-thread") } },
      { ...input, evidence: { ...evidence, taskKind: "attachment" as const } },
    ]) assert.isFalse(yield* value.outbox.holdResourceCleanupUnknown(candidate));
    assert.strictEqual((yield* value.outbox.holdResourceCleanupUnknown({ ...input,
      evidence: { ...evidence, reason: "invented_reason" } as unknown as EffectOutbox.ResourceCleanupUnknownEvidenceV1 }).pipe(Effect.result))._tag, "Failure");
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = '1970-01-01T00:00:00.000Z' WHERE effect_id = ${effectId}`;
    assert.isFalse(yield* value.outbox.holdResourceCleanupUnknown(input));
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = ${claim.leaseExpiresAt} WHERE effect_id = ${effectId}`;
    assert.isTrue(yield* value.outbox.holdResourceCleanupUnknown(input));
    assert.isFalse(yield* value.outbox.holdResourceCleanupUnknown(input));
    const held = yield* value.outbox.listHeldByThreadId(threadId);
    assert.strictEqual(held.length, 1);
    assert.deepEqual(held[0]!.evidence, evidence);
    assert.strictEqual(held[0]!.workerId, input.workerId);
    assert.strictEqual(held[0]!.expectedAttempt, input.expectedAttempt);
    assert.isNull(yield* value.sink.readLeaseCleanupTask(effectId));
    assert.strictEqual((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).outcomes.length, 0);
    assert.isFalse(yield* value.outbox.succeed({ effectId, workerId: input.workerId }));
    assert.isFalse(yield* value.outbox.retry({ effectId, workerId: input.workerId, error: "unavailable task", delayMs: 0 }));
    yield* value.outbox.reconcileAfterProcessLoss;
    assert.deepEqual((yield* value.outbox.listHeldByThreadId(threadId))[0]!.evidence, evidence);
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(effectId)).status, "running");
    assert.isFalse((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).inventoryComplete);
    assert.isTrue((yield* value.sink.readLeaseCleanupStoreBasis(value.lease)).leaseCurrent);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("current owner identity publication retains the exact session and registered generation after target selection changes", () =>
  Effect.gen(function* () {
    const value = yield* seedBinding();
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json,
      '$.modelSelection.instanceId', 'selected-next-owner') WHERE thread_id = ${threadId}`;
    const runtimeIdentity = { runtimeGeneration: value.binding.runtimeGeneration,
      requested: { providerInstanceId: instanceId, providerDriver: driver, model: "fixture-model", serviceTier: null },
      observed: { backend: { status: "unknown" as const }, model: { status: "unknown" as const },
        account: { status: "unknown" as const }, serviceTier: { status: "unknown" as const } } };
    const event: OrchestrationV2DomainEvent = { id: EventId.make("event:owner-runtime-identity"),
      type: "provider-session.updated", threadId, driver, providerInstanceId: instanceId,
      occurredAt: value.now, payload: { ...value.session, runtimeIdentity } };
    const input = { expectedBinding: value.binding, expectedEvidenceRevision: value.revision,
      events: [event], revalidateCurrentOwner: Effect.void };
    for (const candidate of [
      { ...event, payload: { ...event.payload, model: "unrelated-model-change" } },
      { ...event, payload: { ...event.payload, runtimeIdentity: { ...runtimeIdentity, runtimeGeneration: "unregistered-generation" } } },
      { ...event, payload: { ...event.payload, runtimeIdentity: { ...runtimeIdentity,
        requested: { ...runtimeIdentity.requested, providerInstanceId: ProviderInstanceId.make("wrong-runtime-owner") } } } },
    ]) {
      assert.isFalse((yield* value.sink.writeIfCurrentProviderRuntimeOwner({ ...input, events: [candidate] })).committed);
      assert.strictEqual((yield* value.sink.readProviderRuntimeEvidence(threadId))?.evidenceRevision, value.revision);
    }
    assert.deepEqual(yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${event.id}`, []);
    const result = yield* value.sink.writeIfCurrentProviderRuntimeOwner(input);
    assert.isTrue(result.committed);
    assert.strictEqual(result.storedEvents.length, 1);
    const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(threadId, ["providerSessions"]);
    assert.deepEqual(records.providerSessions.find((session) => session.id === value.session.id)?.runtimeIdentity, runtimeIdentity);
    assert.strictEqual((yield* value.sink.readProviderRuntimeEvidence(threadId))?.evidenceRevision, value.revision + 1);
    const changedOwnerEvent = { ...event, id: EventId.make("event:owner-runtime-identity:stale") };
    const stale = yield* value.sink.writeIfCurrentProviderRuntimeOwner({ ...input,
      expectedEvidenceRevision: value.revision + 1, events: [changedOwnerEvent],
      revalidateCurrentOwner: sql`UPDATE orchestration_v2_projection_provider_sessions SET provider_instance_id = 'replaced-owner'
        WHERE provider_session_id = ${value.binding.providerSessionId}`.pipe(Effect.asVoid) });
    assert.isFalse(stale.committed);
    assert.deepEqual(yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${changedOwnerEvent.id}`, []);
    assert.strictEqual((yield* value.sink.readProviderRuntimeEvidence(threadId))?.evidenceRevision, value.revision + 1);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

const outputOwnerFixture = Effect.fnUntraced(function* () {
  const value = yield* seedBinding();
  const runId = RunId.make("run:output-owner");
  const attemptId = RunAttemptId.make("attempt:output-owner");
  const rootNodeId = NodeId.make("node:output-owner:root");
  const providerTurnId = ProviderTurnId.make("turn:output-owner");
  const node = { id: rootNodeId, threadId, runId, parentNodeId: null, rootNodeId, kind: "root_turn" as const,
    status: "running" as const, countsForRun: true, providerThreadId: value.binding.providerThreadId, providerTurnId,
    nativeItemRef: null, runtimeRequestId: null, checkpointScopeId: null, startedAt: value.now, completedAt: null };
  yield* value.sink.write({ events: [
    { id: EventId.make("event:output-owner:run"), type: "run.created", threadId, occurredAt: value.now,
      payload: { id: runId, threadId, ordinal: 1, providerInstanceId: instanceId, modelSelection: value.thread.modelSelection,
        providerThreadId: value.binding.providerThreadId, userMessageId: MessageId.make("message:output-owner:user"), rootNodeId,
        activeAttemptId: attemptId, status: "running", requestedAt: value.now, startedAt: value.now,
        completedAt: null, checkpointId: null, contextHandoffId: null } },
    { id: EventId.make("event:output-owner:attempt"), type: "run-attempt.created", threadId, occurredAt: value.now,
      payload: { id: attemptId, runId, attemptOrdinal: 1, rootNodeId, providerInstanceId: instanceId,
        providerThreadId: value.binding.providerThreadId, providerTurnId, reason: "initial", status: "running",
        startedAt: value.now, completedAt: null } },
    { id: EventId.make("event:output-owner:root"), type: "node.updated", threadId, runId, nodeId: rootNodeId, occurredAt: value.now, payload: node },
    { id: EventId.make("event:output-owner:turn"), type: "provider-turn.updated", threadId, runId, nodeId: rootNodeId, occurredAt: value.now,
      payload: { id: providerTurnId, providerThreadId: value.binding.providerThreadId, nodeId: rootNodeId,
        runAttemptId: attemptId, nativeTurnRef: null, ordinal: 1, status: "running", startedAt: value.now, completedAt: null } },
  ] });
  const assistantNodeId = NodeId.make("node:output-owner:assistant");
  const reasoningNodeId = NodeId.make("node:output-owner:reasoning");
  const messageId = MessageId.make("message:output-owner:assistant");
  const envelope = { threadId, runId, driver, providerInstanceId: instanceId, occurredAt: value.now };
  const companionNodes: ReadonlyArray<Extract<OrchestrationV2DomainEvent, { readonly type: "node.updated" }>> = [
    { ...envelope, id: EventId.make("event:output-owner:assistant-node"), type: "node.updated", nodeId: assistantNodeId,
      payload: { ...node, id: assistantNodeId, parentNodeId: rootNodeId, kind: "assistant_message", countsForRun: false } },
    { ...envelope, id: EventId.make("event:output-owner:reasoning-node"), type: "node.updated", nodeId: reasoningNodeId,
      payload: { ...node, id: reasoningNodeId, parentNodeId: rootNodeId, kind: "reasoning", countsForRun: false } },
  ];
  const messageEvent: Extract<OrchestrationV2DomainEvent, { readonly type: "message.updated" }> = {
    ...envelope, id: EventId.make("event:output-owner:message"), type: "message.updated", nodeId: assistantNodeId,
    payload: { createdBy: "agent", creationSource: "provider", id: messageId, threadId, runId, nodeId: assistantNodeId,
      role: "assistant", text: "retained assistant text", attachments: [], streaming: false, createdAt: value.now, updatedAt: value.now } };
  const itemFields = { threadId, runId, providerThreadId: value.binding.providerThreadId, providerTurnId,
    nativeItemRef: null, parentItemId: null, status: "running" as const, title: null, startedAt: value.now,
    completedAt: null, updatedAt: value.now, streaming: false };
  const events: ReadonlyArray<OrchestrationV2DomainEvent> = [messageEvent,
    { ...envelope, id: EventId.make("event:output-owner:assistant-item"), type: "turn-item.updated", nodeId: assistantNodeId,
      payload: { ...itemFields, id: TurnItemId.make("item:output-owner:assistant"), nodeId: assistantNodeId, ordinal: 1,
        type: "assistant_message", messageId, text: messageEvent.payload.text } },
    { ...envelope, id: EventId.make("event:output-owner:reasoning-item"), type: "turn-item.updated", nodeId: reasoningNodeId,
      payload: { ...itemFields, id: TurnItemId.make("item:output-owner:reasoning"), nodeId: reasoningNodeId, ordinal: 2,
        type: "reasoning", text: "retained reasoning text" } },
  ];
  const input = { expectedBinding: value.binding, expectedEvidenceRevision: value.revision, expectedRunId: runId,
    expectedRunAttemptId: attemptId, expectedProviderTurnId: providerTurnId,
    revalidateCurrentOwner: Effect.void, companionNodes, events };
  return { ...value, runId, attemptId, providerTurnId, rootNodeId, assistantNodeId, reasoningNodeId, messageEvent, input };
});

const ordinaryCreationFixture = Effect.fnUntraced(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const now = yield* DateTime.now;
  const ownership = yield* makeWorktreeOwnershipLeaseStore();
  const calls = yield* Ref.make(0);
  const app = { ...thread(now), branch: "ordinary-branch", worktreePath: "/fixture/ordinary-checkout" };
  yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at)
    VALUES (${projectId}, 'Ordinary project', '/fixture/repo', '[]', ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)}, NULL)`;
  const command: Extract<OrchestrationV2Command, { readonly type: "thread.create" }> = { type: "thread.create",
    createdBy: app.createdBy, creationSource: app.creationSource,
    commandId: CommandId.make("command:ordinary:create"), threadId, projectId, title: app.title,
    modelSelection: app.modelSelection, runtimeMode: app.runtimeMode, interactionMode: app.interactionMode,
    branch: app.branch, worktreePath: app.worktreePath };
  const context: EventSink.OrdinaryCheckoutCommitContextV1 = { command, captureAfterProjection: () => Effect.gen(function* () {
    yield* Ref.update(calls, (count) => count + 1);
    const birth = (yield* sink.readApplicationBirthRecord(threadId))!;
    assert.isNotNull(birth);
    const lease = Option.getOrThrow(yield* ownership.ensureOrdinaryOwnership({ resourcePath: app.worktreePath,
      leaseId: "lease:ordinary:create", ownerThreadId: threadId, ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(birth),
      branch: app.branch, nowMs: DateTime.toEpochMillis(now), expiresAtMs: DateTime.toEpochMillis(now) + 300_000 }));
    return [{ capture: { version: 1, commandId: command.commandId, commandType: command.type,
      canonicalCommand: JSON.parse(nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(command))),
      commandDigest: nativeCreationSha256(nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(command))),
      origin: { kind: "command" }, threadId, applicationBirth: birth, projectId, canonicalProjectRoot: "/fixture/repo",
      canonicalCheckoutPath: app.worktreePath, branch: app.branch, lease }, originalAdmission: null,
      source: { projectWorkspaceRoot: "/fixture/repo", worktreePath: app.worktreePath } }] satisfies ReadonlyArray<EventSink.OrdinaryCheckoutSqlCaptureV1>;
  }) };
  const input = { commandId: command.commandId, threadId, commandType: command.type, acceptedAt: now, ordinaryCheckoutContext: context,
    events: [{ id: EventId.make("event:ordinary:create"), threadId, type: "thread.created" as const, occurredAt: now, payload: app }], effects: [] };
  return { sink, sql, now, ownership, calls, app, command, context, input };
});

it.effect("ordinary prepared launch acceptance captures its actual birth and replay cannot acquire another lease", () =>
  Effect.gen(function* () {
    const value = yield* ordinaryCreationFixture();
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
    const admission = (yield* value.sink.readOrdinaryCheckoutAdmission({ commandId: value.command.commandId, threadId }))!;
    assert.isNotNull(admission);
    assert.deepEqual(admission.capture.applicationBirth, yield* value.sink.readApplicationBirthRecord(threadId));
    assert.strictEqual(admission.capture.applicationBirth.sequence, admission.eventBasis[0]!.sequence);
    assert.isNull(admission.run);
    assert.deepEqual((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).ordinaryCheckoutAdmissions, [admission]);
    assert.isFalse((yield* value.sink.commitCommand(value.input)).committed);
    assert.strictEqual(yield* Ref.get(value.calls), 1);
    assert.deepEqual(Option.getOrThrow(yield* value.ownership.getByResourcePath(value.app.worktreePath)), admission.capture.lease);
    const { ordinaryCheckoutContext: _context, ...stripped } = value.input;
    assert.strictEqual((yield* value.sink.commitCommand(stripped).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.strictEqual((yield* value.sink.commitCommand({ ...value.input, ordinaryCheckoutContext: { ...value.context,
      command: { ...value.command, title: "Changed replay" } } }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.strictEqual((yield* value.sink.commitRejectedCommand({ commandId: value.command.commandId, threadId,
      commandType: value.command.type, rejectedAt: value.now, error: "stripped replay" }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.strictEqual((yield* value.sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`).length, 1);
    assert.strictEqual((yield* value.sql`SELECT * FROM worktree_ownership_leases`).length, 1);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("ordinary checkout capture and accepted creation roll back together on enclosing SQL failure", () =>
  Effect.gen(function* () {
    const value = yield* ordinaryCreationFixture();
    const publications = yield* Ref.make(0);
    const result = yield* value.sink.withTransaction(Effect.gen(function* () {
      yield* value.sink.commitCommand(value.input);
      yield* value.sink.onCommit(Ref.update(publications, (count) => count + 1));
      return yield* Effect.fail("rollback ordinary acceptance");
    })).pipe(Effect.result);
    assert.strictEqual(result._tag, "Failure");
    assert.strictEqual(yield* Ref.get(publications), 0);
    assert.strictEqual(yield* Ref.get(value.calls), 1);
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).receipt);
    assert.isNull(yield* value.sink.readApplicationBirthRecord(threadId));
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_events`, []);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_ordinary_checkout_admissions`, []);
    assert.deepEqual(yield* value.sql`SELECT * FROM worktree_ownership_leases`, []);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
    assert.isNotNull(yield* value.sink.readOrdinaryCheckoutAdmission({ commandId: value.command.commandId, threadId }));
    assert.strictEqual(yield* Ref.get(value.calls), 2);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("ordinary final checkout guard preserves the foreign owner and rolls back a fresh delegated child", () =>
  Effect.gen(function* () {
    const value = yield* outputOwnerFixture();
    const sql = yield* SqlClient.SqlClient;
    const ownership = yield* makeWorktreeOwnershipLeaseStore();
    yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at)
      VALUES (${projectId}, 'Delegated project', '/fixture/repo', '[]', ${DateTime.formatIso(value.now)}, ${DateTime.formatIso(value.now)}, NULL)`;
    const parentBirth = (yield* value.sink.readApplicationBirthRecord(threadId))!;
    const parentLease = Option.getOrThrow(yield* ownership.ensureOrdinaryOwnership({ resourcePath: "/fixture/repo", leaseId: "lease:ordinary:parent",
      ownerThreadId: threadId, ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(parentBirth), branch: null, nowMs: 0, expiresAtMs: 300_000 }));
    const childId = ThreadId.make("thread:ordinary:delegated-child");
    const child = { ...thread(value.now), id: childId, title: "Delegated child", lineage: { parentThreadId: threadId,
      relationshipToParent: "subagent" as const, rootThreadId: threadId } };
    const command: Extract<OrchestrationV2Command, { readonly type: "delegated_task.request" }> = { type: "delegated_task.request",
      createdBy: "agent", creationSource: "server",
      commandId: CommandId.make("command:ordinary:delegated"), parentThreadId: threadId, parentRunId: value.runId, parentNodeId: value.rootNodeId,
      task: "Work in inherited checkout", modelSelection: value.thread.modelSelection, runtimeMode: value.thread.runtimeMode,
      interactionMode: value.thread.interactionMode, createdAt: value.now };
    const before = yield* value.sink.latestSequence();
    const seenBirth = yield* Ref.make<EventSink.OrdinaryCheckoutSqlCaptureV1["capture"]["applicationBirth"] | null>(null);
    const context: EventSink.OrdinaryCheckoutCommitContextV1 = { command, captureAfterProjection: () => Effect.gen(function* () {
      const birth = (yield* value.sink.readApplicationBirthRecord(childId))!;
      assert.isNotNull(birth);
      yield* Ref.set(seenBirth, birth);
      const canonicalCommand = JSON.parse(nativeCreationCanonicalJson(Schema.encodeSync(OrchestrationV2Command)(command)));
      return [{ capture: { version: 1, commandId: command.commandId, commandType: command.type, canonicalCommand,
        commandDigest: nativeCreationSha256(nativeCreationCanonicalJson(canonicalCommand)), origin: { kind: "delegated_child", parentThreadId: threadId },
        threadId: childId, applicationBirth: birth, projectId, canonicalProjectRoot: "/fixture/repo", canonicalCheckoutPath: "/fixture/repo", branch: null,
        lease: { ...parentLease, leaseId: "lease:ordinary:child", ownerThreadId: childId,
          ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(birth) } }, originalAdmission: null,
        source: { projectWorkspaceRoot: "/fixture/repo", worktreePath: null } }] satisfies ReadonlyArray<EventSink.OrdinaryCheckoutSqlCaptureV1>;
    }) };
    const error = yield* value.sink.commitCommand({ commandId: command.commandId, threadId, commandType: command.type,
      acceptedAt: value.now, ordinaryCheckoutContext: context, effects: [], events: [
        { id: EventId.make("event:ordinary:delegated-parent"), threadId, type: "thread.metadata-updated", occurredAt: value.now,
          payload: { ...value.thread, title: "Delegated accepted" } },
        { id: EventId.make("event:ordinary:delegated-child"), threadId: childId, type: "thread.created", occurredAt: value.now, payload: child },
      ] }).pipe(Effect.flip);
    assert.strictEqual(error._tag, "WorktreeOwnershipConflictError");
    if (error._tag === "WorktreeOwnershipConflictError") {
      assert.strictEqual(error.ownerThreadId, threadId);
      assert.strictEqual(error.requestingThreadId, childId);
      assert.strictEqual(error.resourcePath, parentLease.resourcePath);
      assert.strictEqual(error.expiresAtMs, parentLease.expiresAtMs);
    }
    assert.isNotNull(yield* Ref.get(seenBirth));
    assert.isNull(yield* value.sink.readApplicationBirthRecord(childId));
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(command.commandId)).receipt);
    assert.deepEqual((yield* value.sink.readCommandReceiptIdentity(command.commandId)).ordinaryCheckoutAdmissions, []);
    assert.strictEqual(yield* value.sink.latestSequence(), before);
    assert.strictEqual((yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(threadId, [])).thread.title, value.thread.title);
    assert.deepEqual(Option.getOrThrow(yield* ownership.getByResourcePath(parentLease.resourcePath)), parentLease);
    assert.deepEqual(yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(command.commandId), []);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("buffered assistant output persists its actual companion nodes without changing native evidence", () =>
  Effect.gen(function* () {
    const value = yield* outputOwnerFixture();
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json,
      '$.modelSelection.instanceId', 'selected-next-owner') WHERE thread_id = ${threadId}`;
    const evidence = yield* value.sink.readProviderRuntimeEvidence(threadId);
    assert.isFalse((yield* value.sink.writeIfCurrentProviderRuntimeOwner(value.input)).committed);
    const result = yield* value.sink.writeIfCurrentProviderRuntimeOutputOwner(value.input);
    assert.isTrue(result.committed);
    if (!result.committed) return yield* Effect.die(result.rejection);
    assert.strictEqual(result.evidenceRevision, value.revision);
    assert.deepEqual(result.storedEvents.map((stored) => stored.event.type),
      ["node.updated", "node.updated", "message.updated", "turn-item.updated", "turn-item.updated"]);
    assert.deepEqual(yield* value.sink.readProviderRuntimeEvidence(threadId), evidence);
    const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(threadId, ["nodes", "messages", "turnItems"]);
    assert.strictEqual(records.messages.find((message) => message.id === value.messageEvent.payload.id)?.text, "retained assistant text");
    assert.strictEqual(records.nodes.find((node) => node.id === value.assistantNodeId)?.parentNodeId, value.rootNodeId);
    assert.strictEqual(records.turnItems.find((item) => item.type === "reasoning")?.nodeId, value.reasoningNodeId);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("buffered output rejects stale owners and callback replacements before storing any snapshot", () =>
  Effect.gen(function* () {
    const value = yield* outputOwnerFixture();
    const sql = yield* SqlClient.SqlClient;
    for (const input of [
      { ...value.input, expectedBinding: { ...value.binding, runtimeGeneration: "replaced-generation" } },
      { ...value.input, expectedEvidenceRevision: value.revision + 1 },
      { ...value.input, expectedRunAttemptId: RunAttemptId.make("replaced-attempt") },
      { ...value.input, expectedProviderTurnId: ProviderTurnId.make("unrelated-provider-turn") },
    ]) assert.isFalse((yield* value.sink.writeIfCurrentProviderRuntimeOutputOwner(input)).committed);
    const result = yield* value.sink.writeIfCurrentProviderRuntimeOutputOwner({ ...value.input,
      revalidateCurrentOwner: sql`UPDATE orchestration_v2_projection_provider_sessions SET provider_instance_id = 'replacement-owner'
        WHERE provider_session_id = ${value.binding.providerSessionId}`.pipe(Effect.asVoid) });
    assert.isFalse(result.committed);
    assert.deepEqual(yield* sql`SELECT event_id FROM orchestration_events WHERE event_id IN
      (${value.messageEvent.id}, ${value.input.companionNodes[0]!.id}, ${value.input.companionNodes[1]!.id})`, []);
    const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(threadId, ["messages", "turnItems", "nodes"]);
    assert.deepEqual(records.messages, []);
    assert.deepEqual(records.turnItems, []);
    assert.deepEqual(records.nodes.map((node) => node.id), [value.rootNodeId]);
    assert.strictEqual((yield* value.sink.readProviderRuntimeEvidence(threadId))?.evidenceRevision, value.revision);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("buffered output rejects unrelated events, missing ancestors and changed durable attribution", () =>
  Effect.gen(function* () {
    const value = yield* outputOwnerFixture();
    const sql = yield* SqlClient.SqlClient;
    const invalid: ReadonlyArray<Parameters<EventSink.EventSinkV2Shape["writeIfCurrentProviderRuntimeOutputOwner"]>[0]> = [
      { ...value.input, events: [{ id: EventId.make("event:output-owner:forbidden"), type: "thread.metadata-updated", threadId,
        occurredAt: value.now, payload: { ...value.thread, title: "forbidden output mutation" } }] },
      { ...value.input, companionNodes: [] },
      { ...value.input, companionNodes: value.input.companionNodes.map((event) => ({ ...event,
        payload: { ...event.payload, parentNodeId: NodeId.make("missing-output-ancestor") } })) },
      { ...value.input, companionNodes: [...value.input.companionNodes, { ...value.input.companionNodes[0]!,
        id: EventId.make("event:output-owner:unreferenced"), nodeId: NodeId.make("unreferenced-output-node"),
        payload: { ...value.input.companionNodes[0]!.payload, id: NodeId.make("unreferenced-output-node") } }] },
      { ...value.input, events: [{ ...value.messageEvent, payload: { ...value.messageEvent.payload, role: "user" } }] },
    ];
    for (const input of invalid) assert.isFalse((yield* value.sink.writeIfCurrentProviderRuntimeOutputOwner(input)).committed);
    assert.deepEqual(yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = ${value.messageEvent.id}`, []);
    assert.isTrue((yield* value.sink.writeIfCurrentProviderRuntimeOutputOwner(value.input)).committed);
    const changed = yield* value.sink.writeIfCurrentProviderRuntimeOutputOwner({ ...value.input, companionNodes: [],
      events: [{ ...value.messageEvent, id: EventId.make("event:output-owner:changed-creation"),
        payload: { ...value.messageEvent.payload, createdAt: DateTime.add(value.now, { seconds: 1 }) } }] });
    assert.isFalse(changed.committed);
    assert.deepEqual(yield* sql`SELECT event_id FROM orchestration_events WHERE event_id = 'event:output-owner:changed-creation'`, []);
    assert.strictEqual((yield* value.sink.readProviderRuntimeEvidence(threadId))?.evidenceRevision, value.revision);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

const stopFixture = Effect.fnUntraced(function* (claim = false) {
  const value = yield* seedBinding();
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const runId = RunId.make("run:stop-basis");
  const runAttemptId = RunAttemptId.make("attempt:stop-basis");
  const messageId = MessageId.make("message:stop-basis");
  const startCommandId = CommandId.make("command:system:start-queued:run:stop-basis");
  const executionIntent = { kind: "queued" as const, commandId: startCommandId, runId, runAttemptId,
    effectId: `effect:${startCommandId}:provider-turn.start:${runId}`, reviewedBasis: null };
  yield* value.sink.write({ events: [
    { id: EventId.make("event:stop-basis:run"), type: "run.created", threadId, occurredAt: value.now,
      payload: { id: runId, threadId, ordinal: 1, providerInstanceId: instanceId, modelSelection: value.thread.modelSelection,
        providerThreadId: value.binding.providerThreadId, userMessageId: messageId, rootNodeId: NodeId.make("node:stop-basis"),
        activeAttemptId: runAttemptId, status: "queued", queueHeld: true, requestedAt: value.now, startedAt: null,
        completedAt: null, checkpointId: null, contextHandoffId: null } },
    { id: EventId.make("event:stop-basis:attempt"), type: "run-attempt.created", threadId, occurredAt: value.now,
      payload: { id: runAttemptId, runId, attemptOrdinal: 1, rootNodeId: NodeId.make("node:stop-basis"),
        providerInstanceId: instanceId, providerThreadId: value.binding.providerThreadId, providerTurnId: null,
        reason: "initial", status: "pending", startedAt: null, completedAt: null } },
  ] });
  yield* outbox.enqueue([{ id: executionIntent.effectId, commandId: startCommandId, threadId,
    request: { type: "provider-turn.start", runId } }]);
  if (claim) yield* outbox.claimNext({ workerId: "worker:stop-basis", leaseDurationMs: 60_000 });
  const basisFields = { runId, messageId, queuedProviderThreadId: value.binding.providerThreadId, runAttemptId,
    executionIntent, switchPlan: null, sourceMode: "queued_thread" as const,
    sourceBinding: value.binding, sourceEvidenceRevision: value.revision };
  const basis = { ...basisFields, basisDigest: EventSink.queuedRunContinuationBasisDigestV2(basisFields) };
  const stopCommandId = CommandId.make("command:stop-current");
  const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: stopCommandId });
  const incarnation = yield* value.sink.readApplicationThreadBirth(threadId);
  if (incarnation === null) return yield* Effect.die("Fixture owner birth unavailable");
  const input = { commandId: stopCommandId, threadId, commandType: "provider-session.detach", acceptedAt: value.now,
    events: [{ id: EventId.make("event:stop-current"), type: "provider-session.detach-requested" as const, threadId,
      occurredAt: DateTime.add(value.now, { minutes: 1 }), payload: { providerSessionId: value.binding.providerSessionId, reason: "explicit_stop" } }], effects: [],
    stopContext: { snapshot: facts.commitSnapshot, incarnation, canonicalRequestDigest: "d".repeat(64), actorBindingDigest: "e".repeat(64),
      targetBinding: value.binding, targetEvidenceRevision: value.revision, queuedBases: [basis], affectedRunIds: [runId], revalidateCurrentTarget: Effect.void } };
  return { ...value, outbox, executionIntent, basis, incarnation, input };
});

const workstreamWitnessFixture = Effect.fnUntraced(function* (withOwner = true) {
  const value = withOwner ? yield* seedBinding() : yield* seed();
  const sql = yield* SqlClient.SqlClient;
  const workstreamCommandId = CommandId.make("command:workstream-witness");
  const actorSessionId = AuthSessionId.make("actor:workstream-witness");
  const command = { type: "thread.settle" as const, commandId: workstreamCommandId, threadId };
  const attemptKey = { owner_id: "owner:fixture", principal_id: "principal:fixture", command_id: "workstream:fixture" };
  yield* sql`INSERT INTO workstreams_native_attempts
    (owner_id, principal_id, command_id, request_json, request_bytes_sha256, enrollment_sha256, enrollment_json,
      native_command_id, created_at, dispatch_started_at)
    VALUES (${attemptKey.owner_id}, ${attemptKey.principal_id}, ${attemptKey.command_id}, '{}', ${"a".repeat(64)},
      ${"b".repeat(64)}, '{}', ${workstreamCommandId}, ${DateTime.formatIso(value.now)}, ${DateTime.formatIso(value.now)})`;
  const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: workstreamCommandId, authority: { actorSessionId } });
  if (facts.incarnation === null) return yield* Effect.die("Fixture native incarnation unavailable");
  const owner = yield* value.sink.readCurrentProviderRuntimeOwner(threadId);
  const witness: EventSink.NativeWorkstreamSettlementWitnessV2 = { version: 2, command, attemptKey,
    dispatchStartedAt: DateTime.formatIso(value.now), actorSessionId, enrollmentSha256: "b".repeat(64), requestBytesSha256: "a".repeat(64),
    authority: { environmentId: "fixture-environment", authorityNamespace: "fixture-namespace", storeGeneration: 1 },
    incarnation: facts.incarnation, targetEventSequence: facts.targetEventSequence,
    provider: owner === null ? null : { binding: owner.binding, evidenceRevision: owner.evidenceRevision } };
  const identity = { kind: "workstream_settlement" as const, version: 2 as const, commandId: workstreamCommandId,
    commandType: command.type, aggregateKind: "thread" as const, aggregateId: threadId,
    normalizedCommandDigest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
    bindingDigest: EventSink.nativeWorkstreamSettlementWitnessBindingDigestV2(witness) };
  const input = { commandId: workstreamCommandId, threadId, commandType: command.type, acceptedAt: value.now,
    nativeContext: { identity, snapshot: facts.commitSnapshot, revalidateAuthority: Effect.void, workstreamWitness: witness },
    events: [{ id: EventId.make("event:workstream-witness:settled"), type: "thread.settled" as const, threadId, occurredAt: value.now,
      payload: { ...value.thread, settledOverride: "settled" as const, settledAt: value.now } }], effects: [] };
  return { ...value, sql, witness, identity, input };
});

it.effect("workstream witness preserves its original provider and raw attempt association on replay and later owner changes", () =>
  Effect.gen(function* () {
    const value = yield* workstreamWitnessFixture();
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
    assert.isFalse((yield* value.sink.commitCommand(value.input)).committed);
    const read = () => value.sink.readNativeCommandFacts({ threadId, commandId: value.input.commandId });
    assert.deepEqual((yield* read()).workstreamWitness, value.witness);
    yield* value.sql`UPDATE orchestration_v2_provider_runtime_evidence SET runtime_generation = 'later-owner-generation',
      evidence_revision = evidence_revision + 1 WHERE thread_id = ${threadId}`;
    assert.deepEqual((yield* read()).workstreamWitness, value.witness);
    assert.deepEqual((yield* read()).identity, value.identity);
    const { workstreamWitness: _witness, ...stripped } = value.input.nativeContext;
    assert.strictEqual((yield* value.sink.commitCommand({ ...value.input, nativeContext: stripped }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.strictEqual((yield* value.sql`SELECT * FROM orchestration_v2_workstream_settlement_witnesses`).length, 1);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("workstream companion failure rolls back every accepted field and a witnessed rejected receipt remains exact", () =>
  Effect.gen(function* () {
    const value = yield* workstreamWitnessFixture(false);
    const before = yield* value.sink.latestSequence({ threadId });
    yield* value.sql`CREATE TRIGGER reject_witness_fixture BEFORE INSERT ON orchestration_v2_workstream_settlement_witnesses BEGIN SELECT RAISE(ABORT, 'fixture witness failure'); END`;
    assert.strictEqual((yield* value.sink.commitCommand(value.input).pipe(Effect.result))._tag, "Failure");
    const failed = yield* value.sink.readNativeCommandFacts({ threadId, commandId: value.input.commandId });
    assert.isNull(failed.receipt);
    assert.isNull(failed.identity);
    assert.isNull(failed.workstreamWitness);
    assert.deepEqual(failed.events, []);
    assert.strictEqual(yield* value.sink.latestSequence({ threadId }), before);
    yield* value.sql`DROP TRIGGER reject_witness_fixture`;
    const rejected = yield* value.sink.commitRejectedCommand({ commandId: value.input.commandId, threadId, commandType: value.input.commandType,
      rejectedAt: value.now, error: "fixture denied after qualified association", nativeIdentity: value.identity, workstreamWitness: value.witness });
    assert.strictEqual(rejected.status, "rejected");
    assert.deepEqual((yield* value.sink.readNativeCommandFacts({ threadId, commandId: value.input.commandId })).workstreamWitness, value.witness);
    assert.deepEqual((yield* value.sink.readNativeCommandFacts({ threadId, commandId: value.input.commandId })).events, []);
    assert.strictEqual((yield* value.sink.commitRejectedCommand({ commandId: value.input.commandId, threadId, commandType: value.input.commandType,
      rejectedAt: value.now, error: "stripped", nativeIdentity: value.identity }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("STOP persists all reviewed bases and exact old-intent fences before ordinary queue changes", () =>
  Effect.gen(function* () {
    const value = yield* stopFixture();
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
    const intent = yield* value.sink.readCurrentThreadRuntimeStopIntent({ threadId, commandId: value.input.commandId });
    assert.deepEqual(intent?.queuedBases, [value.basis]);
    const fences = yield* value.sink.readQueuedRunRuntimeStopFences({ threadId, runId: value.basis.runId, incarnation: value.incarnation });
    assert.strictEqual(fences.length, 1);
    assert.deepEqual(fences[0]!.executionIntent, value.executionIntent);
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json = json_set(payload_json, '$.queueHeld', json('false')) WHERE run_id = ${value.basis.runId}`;
    const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: value.executionIntent.commandId });
    assert.deepEqual(yield* value.sink.reserveQueuedRunStart({ snapshot: facts.commitSnapshot, incarnation: value.incarnation,
      basis: value.basis, executionIntent: value.executionIntent, revalidateCurrentSource: Effect.void }), { status: "rejected", reason: "fenced" });
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_queued_start_reservations`, []);
    assert.deepEqual(yield* value.sink.readQueuedRunRuntimeStopFences({ threadId, runId: value.basis.runId, incarnation: value.incarnation }), fences);
    const { stopContext: _stop, ...ordinary } = value.input;
    assert.strictEqual((yield* value.sink.commitCommand(ordinary).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.isNotNull((yield* value.sink.readCommandReceiptIdentity(value.input.commandId)).currentRuntimeStopIdentity);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("STOP acceptance preserves a running claim and lease until actual quiescence", () =>
  Effect.gen(function* () {
    const value = yield* stopFixture(true);
    const before = Option.getOrThrow(yield* value.outbox.get(value.executionIntent.effectId));
    const result = yield* value.sink.commitCommand({ ...value.input,
      cancelUnsettledEffects: { effectTypes: ["provider-turn.start"], reason: "stop-current-runtime" } });
    assert.isTrue(result.committed);
    assert.strictEqual(result.cancelledEffectCount, 0);
    assert.deepEqual(Option.getOrThrow(yield* value.outbox.get(value.executionIntent.effectId)), before);
    assert.strictEqual((yield* value.sink.readQueuedRunRuntimeStopFences({ threadId, runId: value.basis.runId, incarnation: value.incarnation })).length, 1);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("durable detach requests survive compaction and rebuild without detaching or changing thread metadata", () =>
  Effect.gen(function* () {
    const value = yield* stopFixture();
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    const before = yield* projections.getThreadRecords(threadId, ["providerThreads", "providerSessions"]);
    yield* value.sink.commitCommand(value.input);
    const original = yield* value.sink.readCurrentThreadRuntimeStopIntent({ threadId, commandId: value.input.commandId });
    const snapshot = yield* projections.getThreadRecords(threadId, ["providerThreads", "providerSessions"]);
    assert.deepEqual(snapshot, before);
    yield* maintenance.compactEventStore;
    assert.deepEqual(yield* value.sink.readCurrentThreadRuntimeStopIntent({ threadId, commandId: value.input.commandId }), original);
    yield* maintenance.rebuild;
    assert.deepEqual(yield* projections.getThreadRecords(threadId, ["providerThreads", "providerSessions"]), before);
    assert.deepEqual(yield* value.sink.readCurrentThreadRuntimeStopIntent({ threadId, commandId: value.input.commandId }), original);
    assert.deepEqual((yield* value.sink.readCurrentProviderRuntimeOwner(threadId))?.binding, value.binding);
    assert.strictEqual((yield* value.sink.readQueuedRunRuntimeStopFences({ threadId, runId: value.basis.runId,
      incarnation: value.incarnation })).length, 1);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("reviewed unknown STOP sources hold the old queued intent without requiring a fabricated fence", () =>
  Effect.gen(function* () {
    const value = yield* stopFixture();
    const { basisDigest: _digest, ...fields } = value.basis;
    const unknownFields = { ...fields, sourceMode: "unknown" as const, reason: "prestop source association unavailable" };
    const unknownBasis = { ...unknownFields, basisDigest: EventSink.queuedRunContinuationBasisDigestV2(unknownFields) };
    yield* value.sink.commitCommand({ ...value.input, stopContext: { ...value.input.stopContext,
      queuedBases: [unknownBasis], affectedRunIds: [] } });
    assert.deepEqual(yield* value.sink.readQueuedRunRuntimeStopFences({ threadId, runId: value.basis.runId, incarnation: value.incarnation }), []);
    const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: value.executionIntent.commandId });
    assert.deepEqual(yield* value.sink.reserveQueuedRunStart({ snapshot: facts.commitSnapshot, incarnation: value.incarnation,
      basis: value.basis, executionIntent: value.executionIntent, revalidateCurrentSource: Effect.void }), { status: "rejected", reason: "source_unknown" });
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.executionIntent.effectId)).status, "pending");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("real pending starts reserve before preparation and outer rollback leaves no outbox reservation or notification", () =>
  Effect.gen(function* () {
    const value = yield* stopFixture();
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = ${value.executionIntent.effectId}`;
    const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: value.executionIntent.commandId });
    const pendingEffect = { id: value.executionIntent.effectId, commandId: value.executionIntent.commandId, threadId,
      request: { type: "provider-turn.start" as const, runId: value.basis.runId } };
    const input = { snapshot: facts.commitSnapshot, incarnation: value.incarnation, basis: value.basis,
      executionIntent: value.executionIntent, pendingEffect, revalidateCurrentSource: Effect.void };
    assert.deepEqual(yield* value.sink.reserveQueuedRunStart(input), { status: "rejected", reason: "intent_conflict" });
    const publications = yield* Ref.make(0);
    const rolled = yield* value.sink.withTransaction(Effect.gen(function* () {
      assert.strictEqual((yield* value.sink.reserveQueuedRunStart(input)).status, "reserved");
      assert.strictEqual((yield* sql`SELECT effect_id FROM orchestration_v2_queued_start_reservations`).length, 1);
      assert.strictEqual((yield* value.outbox.enqueue([pendingEffect]).pipe(Effect.result))._tag, "Success");
      assert.deepEqual(yield* value.sink.reserveQueuedRunStart({ ...input, pendingEffect: { ...pendingEffect,
        request: { type: "provider-turn.start", runId: RunId.make("wrong-real-run") } } }),
        { status: "rejected", reason: "intent_conflict" });
      yield* value.sink.onCommit(Ref.update(publications, (count) => count + 1));
      return yield* Effect.fail("rollback before actual preparation commit");
    })).pipe(Effect.result);
    assert.strictEqual(rolled._tag, "Failure");
    assert.isTrue(Option.isNone(yield* value.outbox.get(pendingEffect.id)));
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_queued_start_reservations`, []);
    assert.strictEqual(yield* Ref.get(publications), 0);
    yield* value.sink.withTransaction(Effect.gen(function* () {
      assert.strictEqual((yield* value.sink.reserveQueuedRunStart(input)).status, "reserved");
      assert.strictEqual((yield* value.sink.reserveQueuedRunStart(input)).status, "already_started");
    }));
    assert.strictEqual((yield* sql`SELECT effect_id FROM orchestration_v2_queued_start_reservations`).length, 1);
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(pendingEffect.id)).status, "pending");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

const firstOrdinaryQueueFixture = Effect.fnUntraced(function* (gui = false) {
  const value = yield* seed();
  const sql = yield* SqlClient.SqlClient;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const runId = RunId.make("run:first-ordinary-queue");
  const runAttemptId = RunAttemptId.make("attempt:first-ordinary-queue");
  const messageId = MessageId.make("message:first-ordinary-queue");
  const nodeId = NodeId.make("node:first-ordinary-queue");
  const providerThreadId = ProviderThreadId.make("provider-thread:first-ordinary-queue");
  const queueCommandId = CommandId.make("command:first-ordinary-queue");
  const startCommandId = CommandId.make(`command:system:start-queued:${runId}`);
  const executionIntent = { kind: "queued" as const, commandId: startCommandId, runId, runAttemptId,
    effectId: `effect:${startCommandId}:provider-turn.start:${runId}`, reviewedBasis: null };
  const provider = { id: providerThreadId, driver, providerInstanceId: instanceId,
    providerSessionId: gui ? ProviderSessionId.make("session:planned-never-opened") : null,
    appThreadId: threadId, ownerNodeId: nodeId, nativeThreadRef: null, nativeConversationHeadRef: null,
    status: "idle" as const, firstRunOrdinal: 1, lastRunOrdinal: 1, handoffIds: [], forkedFrom: null,
    createdAt: value.now, updatedAt: value.now };
  const events: OrchestrationV2DomainEvent[] = [
    { id: EventId.make("event:first-ordinary:provider"), type: "provider-thread.updated", threadId, occurredAt: value.now, payload: provider },
    { id: EventId.make("event:first-ordinary:run"), type: "run.created", threadId, occurredAt: value.now,
      payload: { id: runId, threadId, ordinal: gui ? 2 : 1, providerInstanceId: instanceId, modelSelection: value.thread.modelSelection,
        providerThreadId, userMessageId: messageId, rootNodeId: nodeId, activeAttemptId: runAttemptId, status: "queued",
        queueHeld: false, queuePosition: 3, requestedAt: value.now, startedAt: null, completedAt: null, checkpointId: null, contextHandoffId: null } },
    { id: EventId.make("event:first-ordinary:attempt"), type: "run-attempt.created", threadId, occurredAt: value.now,
      payload: { id: runAttemptId, runId, attemptOrdinal: 1, rootNodeId: nodeId, providerInstanceId: instanceId, providerThreadId,
        providerTurnId: null, reason: "initial", status: "pending", startedAt: null, completedAt: null } },
    { id: EventId.make("event:first-ordinary:node"), type: "node.updated", threadId, occurredAt: value.now,
      payload: { id: nodeId, threadId, runId, parentNodeId: null, rootNodeId: nodeId, kind: "root_turn", status: "pending", countsForRun: true,
        providerThreadId, providerTurnId: null, nativeItemRef: null, runtimeRequestId: null, checkpointScopeId: null, startedAt: null, completedAt: null } },
    { id: EventId.make("event:first-ordinary:message"), type: "message.updated", threadId, occurredAt: value.now,
      payload: { id: messageId, threadId, runId, nodeId, createdBy: "user", creationSource: "web",
        role: "user", text: "First queued turn", attachments: [], streaming: false,
        createdAt: value.now, updatedAt: value.now } },
  ];
  if (gui) {
    const blockerId = RunId.make("run:gui-preparing-blocker");
    const blockerAttemptId = RunAttemptId.make("attempt:gui-preparing-blocker");
    const blockerNodeId = NodeId.make("node:gui-preparing-blocker");
    const blockerMessageId = MessageId.make("message:gui-preparing-blocker");
    const blockerEvents = events.map((event): OrchestrationV2DomainEvent => {
      const id = EventId.make(`${event.id}:blocker`);
      switch (event.type) {
        case "provider-thread.updated": return { ...event, id, payload: { ...event.payload, ownerNodeId: blockerNodeId } };
        case "run.created": return { ...event, id, payload: { ...event.payload, id: blockerId, ordinal: 1, userMessageId: blockerMessageId,
          rootNodeId: blockerNodeId, activeAttemptId: blockerAttemptId, status: "preparing" } };
        case "run-attempt.created": return { ...event, id, payload: { ...event.payload, id: blockerAttemptId, runId: blockerId, rootNodeId: blockerNodeId } };
        case "node.updated": return { ...event, id, payload: { ...event.payload, id: blockerNodeId, runId: blockerId, rootNodeId: blockerNodeId } };
        case "message.updated": return { ...event, id, payload: { ...event.payload, id: blockerMessageId, runId: blockerId, nodeId: blockerNodeId, text: "Deferred blocker" } };
        default: return event;
      }
    });
    yield* value.sink.commitCommand({ commandId: CommandId.make("command:gui-defer-start"), commandType: "message.dispatch", threadId,
      acceptedAt: value.now, events: blockerEvents, effects: [] });
    const pooledThreadId = ThreadId.make("thread:gui-unused-pool-association");
    const pooledRunId = RunId.make("run:gui-unused-pool-association");
    const pooledAttemptId = RunAttemptId.make("attempt:gui-unused-pool-association");
    const pooledNodeId = NodeId.make("node:gui-unused-pool-association");
    const pooledProviderId = ProviderThreadId.make("provider:gui-unused-pool-association");
    const pooledMessageId = MessageId.make("message:gui-unused-pool-association");
    yield* value.sink.write({ events: [{ id: EventId.make("event:gui-pooled-birth"), type: "thread.created", threadId: pooledThreadId,
      occurredAt: value.now, payload: { ...value.thread, id: pooledThreadId,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: pooledThreadId } } }] });
    const pooledEvents = blockerEvents.map((event): OrchestrationV2DomainEvent => {
      const base = { ...event, id: EventId.make(`${event.id}:pooled`), threadId: pooledThreadId };
      switch (event.type) {
        case "provider-thread.updated": return { ...base, type: event.type, payload: { ...event.payload, id: pooledProviderId, appThreadId: pooledThreadId, ownerNodeId: pooledNodeId } };
        case "run.created": return { ...base, type: event.type, payload: { ...event.payload, id: pooledRunId, threadId: pooledThreadId,
          providerThreadId: pooledProviderId, rootNodeId: pooledNodeId, activeAttemptId: pooledAttemptId, userMessageId: pooledMessageId } };
        case "run-attempt.created": return { ...base, type: event.type, payload: { ...event.payload, id: pooledAttemptId, runId: pooledRunId,
          providerThreadId: pooledProviderId, rootNodeId: pooledNodeId } };
        case "node.updated": return { ...base, type: event.type, payload: { ...event.payload, id: pooledNodeId, threadId: pooledThreadId, runId: pooledRunId,
          providerThreadId: pooledProviderId, rootNodeId: pooledNodeId } };
        case "message.updated": return { ...base, type: event.type, payload: { ...event.payload, id: pooledMessageId, threadId: pooledThreadId,
          runId: pooledRunId, nodeId: pooledNodeId } };
        default: return event;
      }
    });
    yield* value.sink.commitCommand({ commandId: CommandId.make("command:gui-pooled-defer-start"), commandType: "message.dispatch", threadId: pooledThreadId,
      acceptedAt: value.now, events: pooledEvents, effects: [] });
    yield* value.sink.commitCommand({ commandId: queueCommandId, commandType: "message.dispatch", threadId,
      acceptedAt: value.now, events: events.filter((event) => event.type !== "provider-thread.updated"), effects: [] });
    const requestId = TurnItemId.make("item:gui-interrupt-request");
    const signal = { threadId, runId: blockerId, nodeId: blockerNodeId, providerThreadId, providerTurnId: null, nativeItemRef: null,
      startedAt: value.now, completedAt: value.now, updatedAt: value.now };
    const terminalEvents = blockerEvents.flatMap((event): OrchestrationV2DomainEvent[] => {
      switch (event.type) {
        case "run.created": return [{ ...event, id: EventId.make("event:gui-interrupted-run"), type: "run.updated",
          payload: { ...event.payload, status: "interrupted", completedAt: value.now } }];
        case "run-attempt.created": return [{ ...event, id: EventId.make("event:gui-interrupted-attempt"), type: "run-attempt.updated",
          payload: { ...event.payload, status: "interrupted", completedAt: value.now } }];
        case "node.updated": return [{ ...event, id: EventId.make("event:gui-interrupted-node"), payload: { ...event.payload, status: "interrupted", completedAt: value.now } }];
        default: return [];
      }
    });
    yield* value.sink.commitCommand({ commandId: CommandId.make("command:gui-run-interrupt"), commandType: "run.interrupt", threadId,
      acceptedAt: value.now, events: [
        { id: EventId.make("event:gui-interrupt-request"), type: "turn-item.updated", threadId, occurredAt: value.now,
          payload: { ...signal, id: requestId, parentItemId: null, ordinal: 1, status: "completed", title: "Interrupt requested", type: "run_interrupt_request", message: "Interrupt requested" } },
        { id: EventId.make("event:gui-interrupt-result"), type: "turn-item.updated", threadId, occurredAt: value.now,
          payload: { ...signal, id: TurnItemId.make("item:gui-interrupt-result"), parentItemId: requestId, ordinal: 2, status: "interrupted",
            title: "Interrupted", type: "run_interrupt_result", message: "Run interrupted before provider start" } },
        ...terminalEvents,
      ], effects: [] });
  } else yield* value.sink.commitCommand({ commandId: queueCommandId, commandType: "message.dispatch", threadId,
    acceptedAt: value.now, events, effects: [] });
  const incarnation = yield* value.sink.readApplicationBirthRecord(threadId);
  if (incarnation === null) return yield* Effect.die("Fixture application birth unavailable");
  const fields = { runId, messageId, queuedProviderThreadId: providerThreadId, runAttemptId, executionIntent,
    switchPlan: null, sourceMode: "new_context" as const, sourceBinding: null, sourceEvidenceRevision: null };
  const basis = { ...fields, basisDigest: EventSink.queuedRunContinuationBasisDigestV2(fields) };
  const pendingEffect = { id: executionIntent.effectId, commandId: startCommandId, threadId,
    request: { type: "provider-turn.start" as const, runId } };
  const makeInput = Effect.gen(function* () {
    const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: startCommandId });
    return { snapshot: facts.commitSnapshot, incarnation, basis, executionIntent, pendingEffect, revalidateCurrentSource: Effect.void };
  });
  return { ...value, sql, outbox, runId, runAttemptId, nodeId, messageId, provider, providerThreadId, basis, executionIntent, incarnation, makeInput };
});

it.effect("GUI defer-start interruption preserves the same queued head and proves its claimed first start without opening a session", () =>
  Effect.gen(function* () {
    const value = yield* firstOrdinaryQueueFixture(true);
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const before = yield* projections.getThreadRecords(threadId, ["runs", "attempts", "nodes", "messages", "providerThreads"]);
    const run = before.runs.find((candidate) => candidate.id === value.runId)!;
    const node = before.nodes.find((candidate) => candidate.id === value.nodeId)!;
    const provider = before.providerThreads.find((candidate) => candidate.id === value.providerThreadId)!;
    const scopeId = CheckpointScopeId.make("scope:gui-queued-head");
    const presenceInput = { effectId: value.executionIntent.effectId, threadId, runId: run.id };
    const readPresence = Effect.gen(function* () {
      const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: value.executionIntent.commandId });
      return facts.commitSnapshot.records.start_reservations!.filter((row) => row.effect_id === presenceInput.effectId &&
        row.command_id === value.executionIntent.commandId && row.thread_id === threadId && row.run_id === run.id);
    });
    assert.deepEqual(yield* readPresence, []);
    yield* value.sink.withTransaction(Effect.gen(function* () {
      assert.strictEqual((yield* value.sink.reserveQueuedRunStart(yield* value.makeInput)).status, "reserved");
      const presence = yield* readPresence;
      assert.strictEqual(presence.length, 1);
      assert.deepEqual(JSON.parse(String(presence[0]!.basis_json)), value.basis);
      assert.deepEqual(JSON.parse(String(presence[0]!.execution_intent_json)), value.executionIntent);
      assert.isNull(yield* value.sink.readClaimedQueuedRunStart({ ...presenceInput, workerId: "worker:gui-head", expectedAttempt: 1 }));
      assert.strictEqual((yield* projections.getThreadRecords(threadId, ["runs"])).runs.find((candidate) => candidate.id === run.id)!.status, "queued");
      yield* value.sink.writeWithEffects({ commandId: value.executionIntent.commandId, events: [
        { id: EventId.make("event:gui-start-provider"), type: "provider-thread.updated", threadId, occurredAt: value.now, payload: provider },
        { id: EventId.make("event:gui-start-run"), type: "run.updated", threadId, occurredAt: value.now, payload: { ...run, status: "starting" } },
        { id: EventId.make("event:gui-start-scope"), type: "checkpoint-scope.created", threadId, occurredAt: value.now,
          payload: { id: scopeId, threadId, runId: run.id, nodeId: node.id, providerThreadId: provider.id, parentScopeId: null,
            kind: "root_run", ordinalWithinParent: 0, advancesAppRunCount: true, cwd: "/fixture/gui-project", createdAt: value.now } },
        { id: EventId.make("event:gui-start-node"), type: "node.updated", threadId, occurredAt: value.now, payload: { ...node, checkpointScopeId: scopeId } },
        { id: EventId.make("event:gui-start-active"), type: "thread.metadata-updated", threadId, occurredAt: value.now,
          payload: { ...value.thread, activeProviderThreadId: provider.id } },
      ], effects: [{ id: value.executionIntent.effectId, commandId: value.executionIntent.commandId, threadId,
        request: { type: "provider-turn.start", runId: run.id } }] });
    }));
    const claimed = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:gui-head", leaseDurationMs: 60_000 }));
    const input = { effectId: claimed.id, threadId, runId: run.id, workerId: "worker:gui-head", expectedAttempt: claimed.attemptCount };
    assert.deepEqual(yield* value.sink.readClaimedQueuedRunStart(input), { incarnation: value.incarnation, basis: value.basis, executionIntent: value.executionIntent });
    assert.isNull(yield* value.sink.readClaimedQueuedRunStart({ ...input, workerId: "worker:stale" }));
    assert.isNull(yield* value.sink.readClaimedQueuedRunStart({ ...input, expectedAttempt: input.expectedAttempt + 1 }));
    const after = yield* projections.getThreadRecords(threadId, ["runs", "messages"]);
    assert.deepEqual(after.messages, before.messages);
    assert.deepEqual(after.runs.find((candidate) => candidate.id === run.id), { ...run, status: "starting" });
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_projection_provider_sessions`, []);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`, []);
    const pooledOpening = yield* value.sink.withTransaction(Effect.gen(function* () {
      yield* value.sink.write({ events: [{ id: EventId.make("event:gui-pooled-actual-session"), type: "provider-session.attached",
        threadId: ThreadId.make("thread:gui-unused-pool-association"), occurredAt: value.now,
        payload: { id: provider.providerSessionId!, driver, providerInstanceId: instanceId, status: "ready", cwd: "/fixture/gui-project",
          model: "fixture-model", capabilities: CodexProviderCapabilitiesV2, createdAt: value.now, updatedAt: value.now, lastError: null } }] });
      assert.isNull(yield* value.sink.readClaimedQueuedRunStart(input));
      return yield* Effect.fail("rollback fixture pooled opening");
    })).pipe(Effect.result);
    assert.strictEqual(pooledOpening._tag, "Failure");
    assert.isNotNull(yield* value.sink.readClaimedQueuedRunStart(input));
    yield* value.sink.write({ events: [{ id: EventId.make("event:gui-prior-native"), type: "provider-thread.updated", threadId,
      occurredAt: value.now, payload: { ...provider, nativeThreadRef: { driver, nativeId: "unproved-prior-native", strength: "strong" } } }] });
    assert.isNull(yield* value.sink.readClaimedQueuedRunStart(input));
    const retainedPresence = yield* readPresence;
    assert.strictEqual(retainedPresence.length, 1);
    assert.deepEqual(JSON.parse(String(retainedPresence[0]!.basis_json)), value.basis);
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(claimed.id)).status, "running");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("claimed cross-instance queue preparation retains the original registered source after the active pointer moves to its target", () =>
  Effect.gen(function* () {
    const value = yield* seedBinding();
    const sql = yield* SqlClient.SqlClient;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const targetInstance = ProviderInstanceId.make("fixture-codex-next");
    const targetProviderId = ProviderThreadId.make("provider:queued-native-copy-target");
    const targetSessionId = ProviderSessionId.make("session:queued-native-copy-planned");
    const runId = RunId.make("run:queued-native-copy");
    const attemptId = RunAttemptId.make("attempt:queued-native-copy");
    const nodeId = NodeId.make("node:queued-native-copy");
    const messageId = MessageId.make("message:queued-native-copy");
    const targetModel = { instanceId: targetInstance, model: "fixture-model" };
    const provider = { ...value.providerThread, id: targetProviderId, providerInstanceId: targetInstance,
      providerSessionId: null, ownerNodeId: null, nativeThreadRef: null, status: "not_loaded" as const,
      firstRunOrdinal: null, lastRunOrdinal: null };
    const run = { id: runId, threadId, ordinal: 2, providerInstanceId: targetInstance, modelSelection: targetModel,
      providerThreadId: targetProviderId, userMessageId: messageId, rootNodeId: nodeId, activeAttemptId: attemptId,
      status: "queued" as const, queueHeld: false, queuePosition: 7, requestedAt: value.now, startedAt: null,
      completedAt: null, checkpointId: null, contextHandoffId: null };
    const node = { id: nodeId, threadId, runId, parentNodeId: null, rootNodeId: nodeId, kind: "root_turn" as const,
      status: "pending" as const, countsForRun: true, providerThreadId: targetProviderId, providerTurnId: null,
      nativeItemRef: null, runtimeRequestId: null, checkpointScopeId: null, startedAt: null, completedAt: null };
    yield* value.sink.commitCommand({ commandId: CommandId.make("command:queued-native-copy-original"), commandType: "message.dispatch",
      threadId, acceptedAt: value.now, effects: [], events: [
        { id: EventId.make("event:queued-copy:target"), type: "provider-thread.updated", threadId, occurredAt: value.now, payload: provider },
        { id: EventId.make("event:queued-copy:run"), type: "run.created", threadId, occurredAt: value.now, payload: run },
        { id: EventId.make("event:queued-copy:attempt"), type: "run-attempt.created", threadId, occurredAt: value.now,
          payload: { id: attemptId, runId, attemptOrdinal: 1, rootNodeId: nodeId, providerInstanceId: targetInstance,
            providerThreadId: targetProviderId, providerTurnId: null, reason: "initial", status: "pending", startedAt: null, completedAt: null } },
        { id: EventId.make("event:queued-copy:node"), type: "node.updated", threadId, occurredAt: value.now, payload: node },
        { id: EventId.make("event:queued-copy:message"), type: "message.updated", threadId, occurredAt: value.now,
          payload: { id: messageId, threadId, runId, nodeId, createdBy: "user", creationSource: "web",
            role: "user", text: "Switch account on this queued turn", attachments: [],
            streaming: false, createdAt: value.now, updatedAt: value.now } },
      ] });
    const commandId = CommandId.make(`command:system:start-queued:${runId}`);
    const intent = { kind: "queued" as const, commandId, runId, runAttemptId: attemptId,
      effectId: `effect:${commandId}:provider-turn.start:${runId}`, reviewedBasis: null };
    const fields = { runId, messageId, queuedProviderThreadId: targetProviderId, runAttemptId: attemptId, executionIntent: intent,
      sourceMode: "active_native_copy" as const, sourceBinding: value.binding, sourceEvidenceRevision: value.revision,
      switchPlan: { instanceChanged: true, modelChanged: false, targetProviderThreadId: targetProviderId,
        releaseProviderSessionIds: [value.binding.providerSessionId], transition: { type: "restart_and_resume" as const } } };
    const basis = { ...fields, basisDigest: EventSink.queuedRunContinuationBasisDigestV2(fields) };
    const incarnation = (yield* value.sink.readApplicationBirthRecord(threadId))!;
    const scopeId = CheckpointScopeId.make("scope:queued-native-copy");
    const preparedProvider = { ...provider, providerSessionId: targetSessionId,
      firstRunOrdinal: run.ordinal, lastRunOrdinal: run.ordinal, nativeThreadRef: value.providerThread.nativeThreadRef,
      nativeConversationHeadRef: value.providerThread.nativeConversationHeadRef };
    const pendingEffect = { id: intent.effectId, commandId, threadId, request: { type: "provider-turn.start" as const, runId } };
    yield* value.sink.withTransaction(Effect.gen(function* () {
      const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId });
      assert.strictEqual((yield* value.sink.reserveQueuedRunStart({ snapshot: facts.commitSnapshot, incarnation, basis,
        executionIntent: intent, pendingEffect, revalidateCurrentSource: Effect.void })).status, "reserved");
      yield* value.sink.writeWithEffects({ commandId, effects: [pendingEffect], events: [
        { id: EventId.make("event:queued-copy:prepared-provider"), type: "provider-thread.updated", threadId, occurredAt: value.now, payload: preparedProvider },
        { id: EventId.make("event:queued-copy:prepared-run"), type: "run.updated", threadId, occurredAt: value.now, payload: { ...run, status: "starting" } },
        { id: EventId.make("event:queued-copy:prepared-node"), type: "node.updated", threadId, occurredAt: value.now, payload: { ...node, checkpointScopeId: scopeId } },
        { id: EventId.make("event:queued-copy:scope"), type: "checkpoint-scope.created", threadId, occurredAt: value.now,
          payload: { id: scopeId, threadId, runId, nodeId, providerThreadId: targetProviderId, parentScopeId: null,
            kind: "root_run", ordinalWithinParent: 0, advancesAppRunCount: true, cwd: "/fixture", createdAt: value.now } },
        { id: EventId.make("event:queued-copy:pointer"), type: "thread.metadata-updated", threadId, occurredAt: value.now,
          payload: { ...value.thread, activeProviderThreadId: targetProviderId, providerInstanceId: targetInstance, modelSelection: targetModel } },
      ] });
    }));
    assert.isNull(yield* value.sink.readCurrentProviderRuntimeOwner(threadId));
    const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker:queued-native-copy", leaseDurationMs: 60_000 }));
    const input = { effectId: claim.id, threadId, runId, workerId: "worker:queued-native-copy", expectedAttempt: claim.attemptCount };
    assert.deepEqual(yield* value.sink.readClaimedQueuedRunStart(input), { incarnation, basis, executionIntent: intent });
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_provider_continuation_sources`, []);
    assert.isNull(yield* value.sink.readClaimedQueuedRunStart({ ...input, workerId: "stale-worker" }));
    assert.isNull(yield* value.sink.readClaimedQueuedRunStart({ ...input, expectedAttempt: input.expectedAttempt + 1 }));
    for (const mutation of [
      sql`UPDATE orchestration_v2_provider_runtime_evidence SET evidence_revision = evidence_revision + 1 WHERE thread_id = ${threadId}`,
      sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json = json_set(payload_json, '$.nativeThreadRef.nativeId', 'changed-source') WHERE provider_thread_id = ${value.binding.providerThreadId}`,
      sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json = json_set(payload_json, '$.nativeMetadata', json('{"changed":true}')) WHERE provider_thread_id = ${targetProviderId}`,
      sql`UPDATE orchestration_v2_projection_nodes SET payload_json = json_set(payload_json, '$.checkpointScopeId', 'wrong-scope') WHERE node_id = ${nodeId}`,
    ]) {
      const result = yield* value.sink.withTransaction(Effect.gen(function* () {
        yield* mutation;
        assert.isNull(yield* value.sink.readClaimedQueuedRunStart(input));
        return yield* Effect.fail("rollback exact source/target mismatch fixture");
      })).pipe(Effect.result);
      assert.strictEqual(result._tag, "Failure");
      assert.isNotNull(yield* value.sink.readClaimedQueuedRunStart(input));
    }
    const after = yield* projections.getThreadRecords(threadId, ["runs", "messages", "providerThreads"]);
    assert.strictEqual(after.runs.find((item) => item.id === runId)?.queuePosition, 7);
    assert.strictEqual(after.messages.find((item) => item.id === messageId)?.text, "Switch account on this queued turn");
    assert.deepEqual(after.providerThreads.find((item) => item.id === value.binding.providerThreadId), {
      ...value.providerThread,
      contextUsage: null,
      nativeMetadata: null,
      pendingBackgroundTasks: [],
    });
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("a first ordinary queue reserves its real stable effect with positive application proof before source preparation", () =>
  Effect.gen(function* () {
    const value = yield* firstOrdinaryQueueFixture();
    assert.isNull(yield* value.sink.readCurrentProviderRuntimeOwner(threadId));
    assert.isNull(yield* value.sink.readApplicationThreadBirth(threadId));
    assert.isNotNull(yield* value.sink.readApplicationBirthRecord(threadId));
    yield* value.sink.withTransaction(Effect.gen(function* () {
      const input = yield* value.makeInput;
      assert.strictEqual((yield* value.sink.reserveQueuedRunStart(input)).status, "reserved");
      const rows = yield* value.sql<{ readonly basis_json: string }>`SELECT basis_json FROM orchestration_v2_queued_start_reservations`;
      assert.strictEqual(rows.length, 1);
      assert.deepEqual(JSON.parse(rows[0]!.basis_json), value.basis);
      assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.executionIntent.effectId)).status, "pending");
      assert.strictEqual((yield* value.sink.reserveQueuedRunStart(input)).status, "already_started");
    }));
    yield* value.sql`UPDATE orchestration_v2_projection_runs SET status = 'starting', payload_json = json_set(payload_json, '$.status', 'starting')
      WHERE run_id = ${value.runId}`;
    assert.deepEqual(yield* value.sink.readInFlightQueuedRunStartBases({ threadId, incarnation: value.incarnation }), [
      { incarnation: value.incarnation, basis: value.basis, executionIntent: value.executionIntent },
    ]);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_provider_runtime_evidence`, []);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_projection_provider_sessions`, []);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

for (const variant of ["detached", "legacy", "unknown"] as const) {
  it.effect(`missing owner cannot admit ${variant} history as a first ordinary queue`, () =>
    Effect.gen(function* () {
      const value = yield* firstOrdinaryQueueFixture();
      if (variant === "detached") {
        yield* value.sink.write({ events: [
          { id: EventId.make("event:first-ordinary:old-native"), type: "provider-thread.updated", threadId, occurredAt: value.now,
            payload: { ...value.provider, nativeThreadRef: { driver, nativeId: "detached-old-native", strength: "strong" } } },
          { id: EventId.make("event:first-ordinary:cleared-native"), type: "provider-thread.updated", threadId, occurredAt: value.now, payload: value.provider },
        ] });
      } else if (variant === "legacy") {
        yield* value.sink.recordLegacyContinuationDisposition({ threadId, provenance: "legacy_row",
          qualification: { type: "unknown", reason: "No historical binding" }, evidence: null, importedAt: DateTime.formatIso(value.now) });
      } else {
        yield* value.outbox.enqueue([{ id: "effect:first-ordinary:unresolved", commandId, threadId, request: { type: "terminal.cleanup" } }]);
        const claimed = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:unresolved", leaseDurationMs: 60_000 }));
        assert.isTrue(yield* value.outbox.holdUnknown({ effectId: claimed.id, workerId: "worker:unresolved", expectedAttempt: claimed.attemptCount,
          operationId: "operation:unresolved", evidence: { operationId: "operation:unresolved", operation: "close_session", outcome: "unknown", threadId } }));
      }
      assert.isNull(yield* value.sink.readCurrentProviderRuntimeOwner(threadId));
      const input = yield* value.makeInput;
      assert.strictEqual((yield* value.sink.withTransaction(value.sink.reserveQueuedRunStart(input))).status, "rejected");
      assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_queued_start_reservations`, []);
      assert.isTrue(Option.isNone(yield* value.outbox.get(value.executionIntent.effectId)));
      if (variant === "unknown") assert.strictEqual((yield* value.outbox.listHeldByThreadId(threadId)).length, 1);
    }).pipe(Effect.provide(Layer.fresh(testLayer))),
  );
}

it.effect("an old STOP intent cannot be bypassed by clearing its current owner and native pointer", () =>
  Effect.gen(function* () {
    const value = yield* stopFixture();
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
    yield* value.sink.write({ events: [
      { id: EventId.make("event:stop-basis:actually-detached"), type: "provider-session.detached", threadId, occurredAt: value.now,
        payload: { providerSessionId: value.binding.providerSessionId, detachedAt: value.now } },
      { id: EventId.make("event:stop-basis:old-provider-cleared"), type: "provider-thread.updated", threadId, occurredAt: value.now,
        payload: { ...value.providerThread, providerSessionId: null, nativeThreadRef: null, nativeConversationHeadRef: null } },
    ] });
    assert.isNull(yield* value.sink.readCurrentProviderRuntimeOwner(threadId));
    const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: value.executionIntent.commandId });
    const fields = { ...value.basis, sourceMode: "new_context" as const, sourceBinding: null, sourceEvidenceRevision: null };
    const basis = { ...fields, basisDigest: EventSink.queuedRunContinuationBasisDigestV2(fields) };
    assert.strictEqual((yield* value.sink.reserveQueuedRunStart({ snapshot: facts.commitSnapshot, incarnation: value.incarnation,
      basis, executionIntent: value.executionIntent, revalidateCurrentSource: Effect.void })).status, "rejected");
    assert.strictEqual((yield* value.sink.readQueuedRunRuntimeStopFences({ threadId, runId: basis.runId, incarnation: value.incarnation })).length, 1);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("STOP companion failure and enclosing rollback leave no receipt event fence or publication", () =>
  Effect.gen(function* () {
    const value = yield* stopFixture();
    const sql = yield* SqlClient.SqlClient;
    const before = yield* value.sink.latestSequence({ threadId });
    yield* sql`CREATE TRIGGER reject_stop_fixture BEFORE INSERT ON orchestration_v2_queued_runtime_stop_fences BEGIN SELECT RAISE(ABORT, 'fixture stop fence failure'); END`;
    assert.strictEqual((yield* value.sink.commitCommand(value.input).pipe(Effect.result))._tag, "Failure");
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.input.commandId)).receipt);
    assert.isNull(yield* value.sink.readCurrentThreadRuntimeStopIntent({ threadId, commandId: value.input.commandId }));
    assert.strictEqual(yield* value.sink.latestSequence({ threadId }), before);
    yield* sql`DROP TRIGGER reject_stop_fixture`;
    const publications = yield* Ref.make(0);
    assert.strictEqual((yield* value.sink.withTransaction(Effect.gen(function* () {
      yield* value.sink.commitCommand(value.input);
      yield* value.sink.onCommit(Ref.update(publications, (count) => count + 1));
      return yield* Effect.fail("rollback after STOP acceptance");
    })).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual(yield* Ref.get(publications), 0);
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.input.commandId)).receipt);
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_queued_runtime_stop_fences`, []);
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("a replacement registration compares old evidence separately from the new projected owner tuple", () =>
  Effect.gen(function* () {
    const value = yield* seedBinding();
    const providerThreadId = ProviderThreadId.make("provider-thread:store-native:replacement");
    const providerSessionId = ProviderSessionId.make("provider-session:store-native:replacement");
    const nextBinding = { ...value.binding, providerThreadId, providerSessionId, nativeThreadId: "native-replacement" };
    yield* value.sink.write({ events: [
      { id: EventId.make("event:store-native:replacement-thread"), threadId, type: "thread.metadata-updated", occurredAt: value.now,
        payload: { ...value.thread, activeProviderThreadId: providerThreadId } },
      { id: EventId.make("event:store-native:replacement-session"), threadId, type: "provider-session.attached", driver, providerInstanceId: instanceId,
        occurredAt: value.now, payload: { ...value.session, id: providerSessionId } },
      { id: EventId.make("event:store-native:replacement-native"), threadId, type: "provider-thread.updated", driver, providerInstanceId: instanceId,
        occurredAt: value.now, payload: { ...value.providerThread, id: providerThreadId, providerSessionId,
          nativeThreadRef: { driver, nativeId: "native-replacement", strength: "strong" } } },
    ] });
    const result = yield* value.sink.registerProviderRuntime({ expectedBinding: nextBinding, expectedRegisteredBinding: value.binding,
      expectedEvidenceRevision: value.revision, actualBinding: { ...nextBinding, runtimeGeneration: "actual-generation-b" } });
    assert.isTrue(result.committed);
    const current = yield* value.sink.readProviderRuntimeEvidence(threadId);
    assert.strictEqual(current?.binding.nativeThreadId, "native-replacement");
    assert.strictEqual(current?.binding.runtimeGeneration, "actual-generation-b");
    assert.isNull(current?.observation);
    const stale = yield* value.sink.writeIfProviderBindingCurrent({ expectedBinding: value.binding,
      expectedEvidenceRevision: value.revision, events: [] });
    assert.isFalse(stale.committed);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("dormant preparation creates no runnable effect and exact clear cannot clear a later marker", () =>
  Effect.gen(function* () {
    const value = yield* seedBinding();
    const sourceRunId = RunId.make("run:store-native");
    const sourceRunAttemptId = RunAttemptId.make("attempt:store-native");
    yield* value.sink.write({ events: [
      { id: EventId.make("event:store-native:run"), type: "run.created", threadId, runId: sourceRunId, occurredAt: value.now,
        payload: { id: sourceRunId, threadId, ordinal: 1, providerInstanceId: instanceId,
          modelSelection: value.thread.modelSelection, providerThreadId: value.binding.providerThreadId,
          userMessageId: MessageId.make("message:store-native"), rootNodeId: null, activeAttemptId: sourceRunAttemptId,
          status: "running", requestedAt: value.now, startedAt: value.now, completedAt: null, checkpointId: null, contextHandoffId: null } },
      { id: EventId.make("event:store-native:attempt"), type: "run-attempt.created", threadId, runId: sourceRunId, occurredAt: value.now,
        payload: { id: sourceRunAttemptId, runId: sourceRunId, attemptOrdinal: 1, rootNodeId: NodeId.make("node:store-native"),
          providerInstanceId: instanceId, providerThreadId: value.binding.providerThreadId, providerTurnId: null,
          reason: "initial", status: "running", startedAt: value.now, completedAt: null } },
    ] });
    const input = { threadId, projectId, sourceRunId, sourceRunAttemptId, expectedBinding: value.binding, expectedEvidenceRevision: value.revision };
    const first = yield* value.sink.prepareRestartContinuation({ ...input, markerId: "marker-first" });
    assert.deepEqual(yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(commandId), []);
    assert.isTrue(yield* value.sink.clearRestartContinuation(first));
    const second = yield* value.sink.prepareRestartContinuation({ ...input, markerId: "marker-second" });
    assert.isFalse(yield* value.sink.clearRestartContinuation(first));
    assert.deepEqual(yield* value.sink.readDormantRestartContinuations, [second]);
    const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId });
    const denied = yield* value.sink.releaseRestartContinuation({ marker: second, currentSnapshot: facts.commitSnapshot,
      revalidateAfterTrial: Effect.fail("trial-not-committed") }).pipe(Effect.result);
    assert.strictEqual(denied._tag, "Failure");
    assert.deepEqual(yield* value.sink.readDormantRestartContinuations, [second]);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

const capturedRestartFixture = Effect.fnUntraced(function* (earlierStop = false) {
  const value = yield* seedBinding();
  const sql = yield* SqlClient.SqlClient;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const sourceRunId = RunId.make("run:captured-restart-source");
  const sourceRunAttemptId = RunAttemptId.make("attempt:captured-restart-source");
  if (earlierStop) {
    const stopCommandId = CommandId.make("command:captured-restart:earlier-stop");
    const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: stopCommandId });
    yield* value.sink.commitCommand({ commandId: stopCommandId, threadId, commandType: "provider-session.detach", acceptedAt: value.now,
      events: [{ id: EventId.make("event:captured-restart:earlier-stop"), type: "provider-session.detach-requested", threadId,
        occurredAt: value.now, payload: { providerSessionId: value.binding.providerSessionId, reason: "earlier explicit stop" } }], effects: [],
      stopContext: { snapshot: facts.commitSnapshot, incarnation: (yield* value.sink.readApplicationThreadBirth(threadId))!,
        canonicalRequestDigest: "c".repeat(64), actorBindingDigest: "d".repeat(64), targetBinding: value.binding,
        targetEvidenceRevision: value.revision, queuedBases: [], affectedRunIds: [], revalidateCurrentTarget: Effect.void } });
  }
  yield* value.sink.write({ events: [
    { id: EventId.make("event:captured-restart:source"), type: "run.created", threadId, occurredAt: value.now,
      payload: { id: sourceRunId, threadId, ordinal: 1, providerInstanceId: instanceId, modelSelection: value.thread.modelSelection,
        providerThreadId: value.binding.providerThreadId, userMessageId: MessageId.make("message:captured-source"), rootNodeId: null,
        activeAttemptId: sourceRunAttemptId, status: "running", requestedAt: value.now, startedAt: value.now,
        completedAt: null, checkpointId: null, contextHandoffId: null } },
    { id: EventId.make("event:captured-restart:attempt"), type: "run-attempt.created", threadId, occurredAt: value.now,
      payload: { id: sourceRunAttemptId, runId: sourceRunId, attemptOrdinal: 1, rootNodeId: NodeId.make("node:captured-source"),
        providerInstanceId: instanceId, providerThreadId: value.binding.providerThreadId, providerTurnId: null,
        reason: "initial", status: "running", startedAt: value.now, completedAt: null } },
  ] });
  const marker = yield* value.sink.prepareRestartContinuation({ markerId: "marker:captured-restart", threadId, projectId,
    sourceRunId, sourceRunAttemptId, expectedBinding: value.binding, expectedEvidenceRevision: value.revision });
  const releaseCommandId = CommandId.make("command:captured-restart-release");
  const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: releaseCommandId });
  assert.isTrue(yield* value.sink.releaseRestartContinuation({ marker, currentSnapshot: facts.commitSnapshot, revalidateAfterTrial: Effect.void }));
  const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker:captured-restart", leaseDurationMs: 60_000 }));
  const ids = EventSink.capturedRestartContinuationIdsV1({ effectId: claim.id, marker });
  const command = { type: "message.dispatch" as const, ...ids, threadId, text: "Continue where you left off.", attachments: [],
    modelSelection: value.thread.modelSelection, dispatchMode: { type: "start_immediately" as const }, createdBy: "agent" as const,
    creationSource: "server" as const, restartContinuationOfRunId: sourceRunId };
  const context = { effectId: claim.id, marker, workerId: "worker:captured-restart", expectedAttempt: claim.attemptCount };
  const input = { commandId: command.commandId, threadId, commandType: command.type, acceptedAt: value.now,
    capturedRestartContext: { ...context, command }, events: [{ id: EventId.make("event:captured-restart:message"),
      type: "message.updated" as const, threadId, occurredAt: value.now, payload: { id: command.messageId, threadId, runId: null,
        nodeId: null, createdBy: command.createdBy, creationSource: command.creationSource, role: "user" as const,
        text: command.text, attachments: [], streaming: false, createdAt: value.now, updatedAt: value.now } }], effects: [] };
  return { ...value, sql, outbox, marker, claim, command, context, input };
});

it.effect("captured restart is blocked by a later idle STOP using the immutable source-attempt creation anchor", () =>
  Effect.gen(function* () {
    const value = yield* capturedRestartFixture();
    const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(threadId, ["runs", "attempts"]);
    const run = records.runs.find((item) => item.id === value.marker.sourceRunId)!;
    const attempt = records.attempts.find((item) => item.id === value.marker.sourceRunAttemptId)!;
    yield* value.sink.write({ events: [
      { id: EventId.make("event:captured-restart:completed-attempt"), type: "run-attempt.updated", threadId, occurredAt: value.now,
        payload: { ...attempt, status: "completed", completedAt: value.now } },
      { id: EventId.make("event:captured-restart:completed-run"), type: "run.updated", threadId, occurredAt: value.now,
        payload: { ...run, status: "completed", completedAt: value.now } },
    ] });
    const stopCommandId = CommandId.make("command:captured-restart:later-idle-stop");
    const facts = yield* value.sink.readNativeCommandFacts({ threadId, commandId: stopCommandId });
    yield* value.sink.commitCommand({ commandId: stopCommandId, threadId, commandType: "provider-session.detach", acceptedAt: value.now,
      events: [{ id: EventId.make("event:captured-restart:later-idle-stop"), type: "provider-session.detach-requested", threadId,
        occurredAt: value.now, payload: { providerSessionId: value.binding.providerSessionId, reason: "idle explicit stop" } }], effects: [],
      stopContext: { snapshot: facts.commitSnapshot, incarnation: (yield* value.sink.readApplicationThreadBirth(threadId))!,
        canonicalRequestDigest: "c".repeat(64), actorBindingDigest: "d".repeat(64), targetBinding: value.binding,
        targetEvidenceRevision: value.revision, queuedBases: [], affectedRunIds: [], revalidateCurrentTarget: Effect.void } });
    assert.strictEqual((yield* value.sink.readCapturedRestartCommandOrigin({ command: value.command, context: value.context }).pipe(Effect.flip))._tag,
      "NativeCommandPreconditionError");
    assert.strictEqual((yield* value.sink.commitCommand(value.input).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).receipt);
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.claim.id)).status, "running");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("an earlier STOP cannot be relabeled as stopping a newly created source attempt", () =>
  Effect.gen(function* () {
    const value = yield* capturedRestartFixture(true);
    assert.isNull(yield* value.sink.readCapturedRestartCommandOrigin({ command: value.command, context: value.context }));
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("ambiguous or unrelated source-attempt creation cannot prove captured restart ordering", () =>
  Effect.gen(function* () {
    const value = yield* capturedRestartFixture(true);
    const sql = value.sql;
    const wrongSource = yield* value.sink.withTransaction(Effect.gen(function* () {
      yield* sql`UPDATE orchestration_v2_projection_run_attempts SET payload_json = json_set(payload_json, '$.providerThreadId', 'unrelated-provider')
        WHERE attempt_id = ${value.marker.sourceRunAttemptId}`;
      assert.strictEqual((yield* value.sink.readCapturedRestartCommandOrigin({ command: value.command, context: value.context }).pipe(Effect.result))._tag, "Failure");
      return yield* Effect.fail("rollback unrelated source fixture");
    })).pipe(Effect.result);
    assert.strictEqual(wrongSource._tag, "Failure");
    yield* sql`INSERT INTO orchestration_events
      (event_id, stream_id, aggregate_kind, stream_version, event_type, payload_json, occurred_at, application_event_version, actor_kind, metadata_json)
      SELECT 'event:captured-restart:ambiguous-created', stream_id, aggregate_kind,
        (SELECT MAX(stream_version) + 1 FROM orchestration_events WHERE stream_id = ${threadId} AND aggregate_kind = 'thread'),
        event_type, payload_json, occurred_at, application_event_version, actor_kind, metadata_json
      FROM orchestration_events WHERE event_id = 'event:captured-restart:attempt'`;
    assert.strictEqual((yield* value.sink.readCapturedRestartCommandOrigin({ command: value.command, context: value.context }).pipe(Effect.result))._tag, "Failure");
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).receipt);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("captured restart receipt binds the full marker and dispatch body while replay checks the new claim separately", () =>
  Effect.gen(function* () {
    const value = yield* capturedRestartFixture();
    assert.isNull(yield* value.sink.readCapturedRestartCommandOrigin({ command: value.command, context: value.context }));
    assert.isTrue((yield* value.sink.commitCommand(value.input)).committed);
    const origin = (yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).capturedRestartOrigin!;
    assert.deepEqual(origin.marker, value.marker);
    assert.deepEqual(origin.command, value.command);
    assert.strictEqual(origin.originalClaim.expectedAttempt, value.claim.attemptCount);
    assert.isFalse((yield* value.sink.commitCommand(value.input)).committed);
    const { capturedRestartContext: _private, ...ordinary } = value.input;
    assert.strictEqual((yield* value.sink.commitCommand(ordinary).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.strictEqual((yield* value.sink.readCapturedRestartCommandOrigin({ command: { ...value.command, text: "Changed request" },
      context: value.context }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.isTrue(yield* value.outbox.retry({ effectId: value.claim.id, workerId: value.context.workerId, error: "known SQL-only scheduling retry", delayMs: 0 }));
    const next = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:captured-restart-next", leaseDurationMs: 60_000 }));
    const currentContext = { ...value.context, workerId: "worker:captured-restart-next", expectedAttempt: next.attemptCount };
    assert.strictEqual(next.attemptCount, origin.originalClaim.expectedAttempt + 1);
    assert.deepEqual(yield* value.sink.readCapturedRestartCommandOrigin({ command: value.command, context: currentContext }), origin);
    assert.isFalse((yield* value.sink.commitCommand({ ...value.input, capturedRestartContext: { ...currentContext, command: value.command } })).committed);
    assert.deepEqual((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).capturedRestartOrigin, origin);
    assert.strictEqual((yield* value.sink.readCapturedRestartCommandOrigin({ command: value.command, context: value.context }).pipe(Effect.flip))._tag,
      "NativeCommandPreconditionError");
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

it.effect("captured restart companion failure rolls back acceptance and rejected attribution keeps the same immutable origin", () =>
  Effect.gen(function* () {
    const value = yield* capturedRestartFixture();
    const before = yield* value.sink.latestSequence({ threadId });
    const published = yield* Ref.make(0);
    const wrongMessage = { ...value.input.events[0]!, payload: { ...value.input.events[0]!.payload, text: "Unbound message body" } };
    assert.strictEqual((yield* value.sink.commitCommand({ ...value.input, events: [wrongMessage] }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).receipt);
    yield* value.sql`CREATE TRIGGER fixture_captured_origin_failure BEFORE INSERT ON orchestration_v2_captured_restart_command_origins
      BEGIN SELECT RAISE(ABORT, 'fixture captured origin write failure'); END`;
    const result = yield* value.sink.withTransaction(Effect.gen(function* () {
      yield* value.sink.commitCommand(value.input);
      yield* value.sink.onCommit(Ref.update(published, (count) => count + 1));
    })).pipe(Effect.result);
    assert.strictEqual(result._tag, "Failure");
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).receipt);
    assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).capturedRestartOrigin);
    assert.strictEqual(yield* value.sink.latestSequence({ threadId }), before);
    assert.strictEqual(yield* Ref.get(published), 0);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_captured_restart_command_origins`, []);
    yield* value.sql`DROP TRIGGER fixture_captured_origin_failure`;
    const rejectedInput = { commandId: value.command.commandId, threadId, commandType: value.command.type,
      rejectedAt: value.now, error: "Current source no longer eligible", capturedRestartContext: { ...value.context, command: value.command } };
    assert.strictEqual((yield* value.sink.commitRejectedCommand(rejectedInput)).status, "rejected");
    assert.deepEqual((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).capturedRestartOrigin?.command, value.command);
    assert.strictEqual((yield* value.sink.commitRejectedCommand(rejectedInput)).status, "rejected");
    const { capturedRestartContext: _private, ...stripped } = rejectedInput;
    assert.strictEqual((yield* value.sink.commitRejectedCommand(stripped).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
    assert.strictEqual(yield* value.sink.latestSequence({ threadId }), before);
  }).pipe(Effect.provide(Layer.fresh(testLayer))),
);

for (const variant of ["old_unbound", "expired", "unknown_hold"] as const) {
  it.effect(`captured restart ${variant} evidence cannot promote an ordinary receipt or authorize a dispatch`, () =>
    Effect.gen(function* () {
      const value = yield* capturedRestartFixture();
      if (variant === "old_unbound") {
        const { capturedRestartContext: _private, ...ordinary } = value.input;
        yield* value.sink.commitCommand(ordinary);
      } else if (variant === "expired") yield* value.sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = '1970-01-01T00:00:00.000Z'
        WHERE effect_id = ${value.claim.id}`;
      else assert.isTrue(yield* value.outbox.holdUnknown({ effectId: value.claim.id, workerId: value.context.workerId,
        expectedAttempt: value.claim.attemptCount, operationId: "unknown:captured-restart", evidence: { operationId: "unknown:captured-restart",
          operation: "close_session", outcome: "unknown", threadId } }));
      assert.strictEqual((yield* value.sink.commitCommand(value.input).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
      assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).capturedRestartOrigin);
      assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_captured_restart_command_origins`, []);
    }).pipe(Effect.provide(Layer.fresh(testLayer))),
  );
}

it("only genuine stopped source rows produce correlated historical continuation proof", () => {
  const sourceRow = { threadId, providerName: "codex", providerInstanceId: null, adapterKey: "codex",
    runtimeMode: "full-access" as const, status: "stopped" as const, lastSeenAt: "2026-10-03T00:00:00.000Z",
    resumeCursor: { threadId: "historical-native" }, runtimePayload: null };
  const input = { sourceRow, driver, nativeThreadId: "historical-native" };
  assert.isNull(makeLegacyStoppedRuntimeProofV1({ ...input, source: "synthetic_import" }));
  assert.isNull(makeLegacyStoppedRuntimeProofV1({ ...input, source: "persisted_runtime_row", sourceRow: { ...sourceRow, status: "running" } }));
  const stoppedProof = makeLegacyStoppedRuntimeProofV1({ ...input, source: "persisted_runtime_row" });
  const evidence = makeLegacyProviderContinuationEvidenceV1({ sourceRow, provenance: "legacy_row", driver,
    nativeThreadId: "historical-native", continuationKey: null, historicalSourceIdentity: null, stoppedProof, accessibility: null });
  assert.isNull(evidence.providerInstanceId);
  assert.isNull(evidence.continuationKey);
  assert.isNull(evidence.historicalSourceIdentity);
  assert.strictEqual(evidence.stoppedProof?.nativeThreadId, "historical-native");
  assert.throws(() => makeLegacyProviderContinuationEvidenceV1({ sourceRow, provenance: "legacy_row", driver,
    nativeThreadId: "wrong-native", continuationKey: null, historicalSourceIdentity: null, stoppedProof, accessibility: null }));
});

const sealTestLayer = Layer.mergeAll(testLayer, ProviderSessionRuntime.layer.pipe(Layer.provide(database)),
  NativeCreationRepositoryLayer.pipe(Layer.provide(database)));
const nativeRecoveryRaceFixture = Effect.fnUntraced(function* (beforeCreate?: (
  sink: EventSink.EventSinkV2Shape, input: Parameters<EventSink.EventSinkV2Shape["commitCommand"]>[0],
) => Effect.Effect<void, unknown>) {
  const repository = yield* NativeCreationRepository;
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const sessions = yield* makeAuthSessions;
  const now = yield* DateTime.now;
  const timestamp = DateTime.formatIso(now);
  const binding = Schema.decodeUnknownSync(NativePreparationBinding)({ backend_instance: "fixture-backend",
    environment_id: "fixture-environment", project_id: projectId, project_cwd: "/fixture/project",
    account_ref: "fixture-account", runtime_mode: "full-access", interaction_mode: "default", base_branch: "main",
    start_from_origin: false, run_setup_script: false, provider_model_selection: { instanceId, model: "fixture-model" } });
  const original = nativePreparationCommand("fixture-recovery-race", binding, "Synthetic prompt", "Recovery race", timestamp);
  const preparation = yield* validateNativeCreationPreparation(new TextEncoder().encode(nativeCreationCanonicalJson({
    schema: "voice.t3-bootstrap-preparation/v1", operation_id: "fixture-recovery-race", binding, command: original,
    preparation_id: original.commandId.replace("voice-command-", "voice-bootstrap-"),
    binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)), prompt_digest: nativeCreationSha256("Synthetic prompt"),
    command_digest: nativeCreationSha256(nativeCreationCanonicalJson(original)),
  })));
  const historical = Schema.decodeUnknownSync(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance, environmentId: binding.environment_id, projectId: binding.project_id,
    projectCwd: binding.project_cwd, accountRef: binding.account_ref, accountBindingId: "qualified-fixture-account", accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection, runtimeMode: binding.runtime_mode, interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch, startFromOrigin: false, runSetupScript: false, requestedBranch: original.bootstrap.prepareWorktree.branch,
  });
  const actorSessionId = AuthSessionId.make("session:recovery-race");
  yield* sessions.create({ sessionId: actorSessionId, subject: "fixture-recovery-actor", method: "bearer-access-token",
    scopes: ["orchestration:read", "orchestration:operate"], issuedAt: now, expiresAt: DateTime.add(now, { days: 1 }),
    client: { label: null, ipAddress: null, userAgent: null, deviceType: "bot", os: null, browser: null } });
  yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${actorSessionId}, ${timestamp})`;
  const claimId = "claim:recovery-race";
  const resources = { projectCwd: binding.project_cwd, branch: historical.requestedBranch, worktreePath: "/fixture/recovery-race" };
  const guard = { schema: "t3.native-creation-guard/v1" as const, grantId: "grant:recovery-race", grantRevision: 1 };
  const authorization = { actorSessionId, preparation, guard, resources };
  let grant: NativeCreationGrant = { grantId: guard.grantId, revision: guard.grantRevision, actorSessionId,
    issuerId: "fixture-qualified-issuer", expiresAt: DateTime.add(now, { days: 1 }), revoked: false,
    operationId: preparation.operationId, preparationId: preparation.preparationId, preparationSha256: preparation.preparationSha256,
    bindingDigest: preparation.bindingDigest, binding: historical, resources,
    allowedStages: ["claim", "native_command", "cleanup"], recoveryScopes: [] };
  const authority = yield* makeNativeCreationAuthority.pipe(
    Effect.provideService(AuthSessionRepository, sessions),
    Effect.provideService(NativeCreationGrantResolver, {
      resolveCurrent: () => Effect.sync(() => ({ enrolledSessionId: actorSessionId, trustedIssuerId: grant.issuerId, grant })),
    }),
    Effect.provideService(NativeCreationBindingResolver, { resolveCurrent: () => Effect.succeed(historical) }),
  );
  yield* repository.claim({ preparation, claimId, resources, claimedBootId: "fixture-boot", claimedAt: timestamp,
    actorSessionId, grantId: guard.grantId, grantRevision: guard.grantRevision }, authority.authorize({ ...authorization, stage: "claim" }));
  const create = Schema.decodeUnknownSync(OrchestrationV2Command)({ type: "thread.create",
    commandId: `${original.commandId}:native:v2:create`, threadId: original.threadId, projectId,
    title: original.bootstrap.createThread.title, modelSelection: original.bootstrap.createThread.modelSelection,
    runtimeMode: historical.runtimeMode, interactionMode: historical.interactionMode,
    branch: resources.branch, worktreePath: resources.worktreePath, createdBy: "user", creationSource: "server" });
  if (create.type !== "thread.create") return yield* Effect.die("Expected native creation stage");
  const release = { type: "prepared-run.release" as const, commandId: CommandId.make(original.commandId),
    threadId: create.threadId, runId: RunId.make("run:recovery-race") };
  yield* repository.reserveCommandIdentities(claimId, [create.commandId, `${original.commandId}:native:v2:message`, release.commandId]);
  yield* repository.reserveCommand(claimId, create);
  yield* repository.recordNormalizedCommand(claimId, release);
  const appThread = { ...thread(now), id: create.threadId, title: create.title, branch: create.branch, worktreePath: create.worktreePath,
    creationSource: create.creationSource, lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: create.threadId } };
  const nativeAuthorityRead = { actorSessionId, claimId, projectId, resourcePaths: [resources.projectCwd, resources.worktreePath] };
  const facts = yield* sink.readNativeCommandFacts({ threadId: create.threadId, commandId: create.commandId, authority: nativeAuthorityRead });
  const revalidateAuthority = authority.authorize({ ...authorization, stage: "native_command" }).pipe(Effect.asVoid);
  const createAcceptance: Parameters<EventSink.EventSinkV2Shape["commitCommand"]>[0] = {
    commandId: create.commandId, threadId: create.threadId, commandType: create.type, acceptedAt: now,
    events: [{ id: EventId.make("event:recovery-race:birth"), type: "thread.created", threadId: create.threadId, occurredAt: now, payload: appThread }], effects: [],
    nativeContext: { identity: { kind: "native_creation_stage", version: 2, commandId: create.commandId, commandType: create.type,
      aggregateKind: "thread", aggregateId: create.threadId, normalizedCommandDigest: nativeCreationV2CommandDigest(create), bindingDigest: preparation.bindingDigest },
      snapshot: facts.commitSnapshot, revalidateAuthority } };
  if (beforeCreate !== undefined) yield* beforeCreate(sink, createAcceptance);
  yield* sink.commitCommand(createAcceptance);
  const incarnation = yield* sink.getThreadIncarnation(create.threadId);
  if (incarnation === null) return yield* Effect.die("Expected committed native V2 birth");
  const resource = { kind: "thread" as const, threadId: create.threadId, incarnation };
  const recoveryScopeId = `${preparation.operationId}:cleanup:thread`;
  grant = { ...grant, recoveryScopes: [{ scopeId: recoveryScopeId, resource }] };
  const deleteCommand = { type: "thread.delete" as const, commandId: CommandId.make(`${original.commandId}:bootstrap-thread-delete`), threadId: create.threadId };
  const cleanup = yield* repository.startEffect(claimId, { kind: "cleanup", phase: "started", effectId: `${deleteCommand.commandId}:cleanup`,
    timestamp, recoveryScopeId, resource }, authority.authorize({ ...authorization, stage: "cleanup", recoveryScopeId, recoveryResource: resource }));
  const reference = { version: 2 as const, claimId, commandId: deleteCommand.commandId, threadId: create.threadId, commandType: deleteCommand.type,
    canonicalCommand: deleteCommand, commandDigest: nativeCreationV2CommandDigest(deleteCommand), commandStartEffectId: `${deleteCommand.commandId}:command`,
    cleanupStartEffectId: cleanup.effectId, cleanupStartOrdinal: cleanup.ordinal, recoveryScopeId, resource };
  yield* repository.reserveThreadRecoveryCommand(reference);
  yield* repository.startEffect(claimId, { kind: "native_command", phase: "started", effectId: reference.commandStartEffectId, timestamp,
    commandId: deleteCommand.commandId, threadId: create.threadId, commandType: deleteCommand.type, commandDigest: reference.commandDigest },
    authority.authorize({ ...authorization, stage: "native_command" }));
  const context = yield* authority.issueThreadRecovery({ claimId, commandStartEffectId: reference.commandStartEffectId,
    cleanupStartEffectId: reference.cleanupStartEffectId,
    authorization: { ...authorization, stage: "cleanup", recoveryScopeId, recoveryResource: resource } });
  const acceptance = { commandId: deleteCommand.commandId, threadId: create.threadId, commandType: deleteCommand.type, acceptedAt: now,
    recoveryContext: context, effects: [], events: [{ id: EventId.make("event:recovery-race:deleted"), type: "thread.deleted" as const,
      threadId: create.threadId, occurredAt: now, payload: { ...appThread, deletedAt: now, updatedAt: now } }] };
  return { sink, sql, repository, now, timestamp, claimId, create, release, deleteCommand, reference, context, acceptance, appThread,
    nativeAuthorityRead, revalidateAuthority, preparation, authorizeCommand: authority.authorize({ ...authorization, stage: "native_command" }) };
});

it.effect("native recovery rechecks an interleaved accepted final release or foreign unresolved command inside deletion SQL", () =>
  Effect.gen(function* () {
    const value = yield* nativeRecoveryRaceFixture();
    assert.deepEqual(yield* value.sink.readNativeThreadRecovery({ command: value.deleteCommand, context: value.context }), value.reference);
    const beforeHistory = yield* value.repository.readHistoryByClaim(value.claimId);
    const beforeEvents = yield* value.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
    for (const variant of ["accepted_release", "foreign_command_start"] as const) {
      const rejectionVerified = yield* Ref.make(false);
      const result = yield* value.sink.withTransaction(Effect.gen(function* () {
        if (variant === "accepted_release") {
          const facts = yield* value.sink.readNativeCommandFacts({ threadId: value.create.threadId, commandId: value.release.commandId, authority: value.nativeAuthorityRead });
          const effectId = `effect:${value.release.commandId}:provider-turn.start:${value.release.runId}`;
          const accepted = yield* value.sink.commitCommand({ commandId: value.release.commandId, threadId: value.create.threadId,
            commandType: value.release.type, acceptedAt: value.now,
            events: [{ id: EventId.make("event:recovery-race:released"), type: "run.updated", threadId: value.create.threadId, occurredAt: value.now,
              payload: { id: value.release.runId, threadId: value.create.threadId, ordinal: 1, providerInstanceId: instanceId,
                modelSelection: value.appThread.modelSelection, providerThreadId: ProviderThreadId.make("provider:recovery-race"),
                userMessageId: MessageId.make("message:recovery-race"), rootNodeId: NodeId.make("node:recovery-race"),
                activeAttemptId: RunAttemptId.make("attempt:recovery-race"), status: "starting", queueHeld: false, queuePosition: null,
                requestedAt: value.now, startedAt: null, completedAt: null, checkpointId: null, contextHandoffId: null } }],
            effects: [{ id: effectId, commandId: value.release.commandId, threadId: value.create.threadId,
              request: { type: "provider-turn.start", runId: value.release.runId }, nativeCreationExecutionReference: {
                version: 2, claimId: value.claimId, stageCommandId: value.release.commandId, effectId, stage: "native_command" } }],
            nativeContext: { identity: { kind: "native_creation_stage", version: 2, commandId: value.release.commandId, commandType: value.release.type,
              aggregateKind: "thread", aggregateId: value.create.threadId, normalizedCommandDigest: nativeCreationV2CommandDigest(value.release), bindingDigest: value.preparation.bindingDigest },
              snapshot: facts.commitSnapshot, revalidateAuthority: value.revalidateAuthority } });
          assert.isTrue(accepted.committed);
          assert.strictEqual((yield* value.sink.readCommandReceiptIdentity(value.release.commandId)).receipt?.status, "accepted");
        } else {
          yield* value.repository.startEffect(value.claimId, { kind: "native_command", phase: "started", effectId: "foreign:recovery-race:create",
            timestamp: value.timestamp, commandId: value.create.commandId, threadId: value.create.threadId,
            commandType: value.create.type, commandDigest: nativeCreationV2CommandDigest(value.create) }, value.authorizeCommand);
        }
        const rejected = yield* value.sink.commitCommand(value.acceptance).pipe(Effect.flip);
        assert.strictEqual(rejected._tag, "NativeCommandPreconditionError");
        if (rejected._tag === "NativeCommandPreconditionError") assert.strictEqual(rejected.reason, "unknown_evidence");
        assert.isNull((yield* value.sink.readCommandReceiptIdentity(value.deleteCommand.commandId)).receipt);
        assert.strictEqual((yield* value.sql`SELECT event_id FROM orchestration_events WHERE event_type = 'thread.deleted'`).length, 0);
        assert.strictEqual((yield* value.sql`SELECT effect_id FROM native_creation_effect_facts WHERE phase = 'completed'`).length, 0);
        yield* Ref.set(rejectionVerified, true);
        return yield* Effect.fail("rollback interleaved recovery fixture");
      })).pipe(Effect.result);
      assert.strictEqual(result._tag, "Failure");
      assert.isTrue(yield* Ref.get(rejectionVerified));
      assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_events ORDER BY sequence`, beforeEvents);
      assert.deepEqual(yield* value.repository.readHistoryByClaim(value.claimId), beforeHistory);
    }
    const accepted = yield* value.sink.commitCommand(value.acceptance);
    assert.isTrue(accepted.committed);
    const history = yield* value.repository.readHistoryByClaim(value.claimId);
    assert.deepEqual(history.effects.filter((fact) => fact.phase === "completed").map((fact) => fact.effectId), [value.reference.commandStartEffectId]);
    assert.deepEqual(history.effectsV2, []);
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("a fresh native creation reservation fences accepted and rejected writes before any receipt or event", () =>
  Effect.gen(function* () {
    const value = yield* nativeRecoveryRaceFixture((sink, input) => Effect.gen(function* () {
      const context = input.nativeContext;
      if (context === undefined) return yield* Effect.die("Expected actual qualified native creation context");
      const initial = yield* sink.readCommandReceiptIdentity(input.commandId);
      assert.deepEqual(initial.nativeCreationReservation, { commandId: input.commandId, threadId: input.threadId,
        claimId: context.snapshot.authority.claimId });
      assert.isNull(initial.receipt);
      assert.isNull(initial.identity);
      const { nativeContext: _context, ...ordinary } = input;
      const rejectedInput = { commandId: input.commandId, threadId: input.threadId, commandType: input.commandType,
        rejectedAt: input.acceptedAt, error: "fixture-reserved-rejection" };
      assert.strictEqual((yield* sink.commitCommand(ordinary).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
      assert.strictEqual((yield* sink.commitRejectedCommand(rejectedInput).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
      assert.strictEqual((yield* sink.commitRejectedCommand({ ...rejectedInput, nativeIdentity: context.identity }).pipe(Effect.flip))._tag,
        "NativeCommandPreconditionError");
      for (const kind of ["guarded_message_dispatch", "workstream_settlement"] as const) {
        assert.strictEqual((yield* sink.commitCommand({ ...input, nativeContext: { ...context,
          identity: { ...context.identity, kind } } }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
        assert.strictEqual((yield* sink.commitRejectedCommand({ ...rejectedInput, nativeContext: { ...context,
          identity: { ...context.identity, kind } } }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
      }
      for (const nativeContext of [
        { ...context, identity: { ...context.identity, normalizedCommandDigest: "f".repeat(64) } },
        { ...context, snapshot: { ...context.snapshot, authority: { ...context.snapshot.authority, claimId: "foreign-claim" } } },
        { ...context, snapshot: { ...context.snapshot, authority: { ...context.snapshot.authority, actorSessionId: AuthSessionId.make("foreign-session") } } },
      ]) {
        assert.strictEqual((yield* sink.commitCommand({ ...input, nativeContext }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
        assert.strictEqual((yield* sink.commitRejectedCommand({ ...rejectedInput, nativeContext }).pipe(Effect.flip))._tag, "NativeCommandPreconditionError");
      }
      const untouched = yield* sink.readNativeCommandFacts({ threadId: input.threadId, commandId: input.commandId,
        authority: context.snapshot.authority });
      assert.isNull(untouched.receipt);
      assert.isNull(untouched.identity);
      assert.isNull(untouched.projection);
      assert.deepEqual(untouched.events, []);
      assert.deepEqual(untouched.commitSnapshot.records.effects, []);
      assert.deepEqual(untouched.commitSnapshot.authorityRecords.effect_facts, []);
      const rejectionVerified = yield* Ref.make(false);
      const rollback = yield* sink.withTransaction(Effect.gen(function* () {
        const receipt = yield* sink.commitRejectedCommand({ ...rejectedInput, nativeContext: context });
        assert.strictEqual(receipt.status, "rejected");
        assert.deepEqual((yield* sink.readCommandReceiptIdentity(input.commandId)).identity, context.identity);
        assert.deepEqual(yield* sink.commitRejectedCommand({ ...rejectedInput, nativeContext: context }), receipt);
        yield* Ref.set(rejectionVerified, true);
        return yield* Effect.fail("rollback qualified native rejection fixture");
      })).pipe(Effect.result);
      assert.strictEqual(rollback._tag, "Failure");
      assert.isTrue(yield* Ref.get(rejectionVerified));
      assert.deepEqual(yield* sink.readCommandReceiptIdentity(input.commandId), initial);
    }));
    assert.strictEqual((yield* value.sink.readCommandReceiptIdentity(value.create.commandId)).receipt?.status, "accepted");
    assert.deepEqual((yield* value.repository.readHistoryByClaim(value.claimId)).effects.map((fact) => [fact.kind, fact.phase]),
      [["cleanup", "started"], ["native_command", "started"]]);
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

function nativeImportFixture(provider: "codex" | "claudeAgent", count: number) {
  const source: AgentSessionImportSource = { provider, providerInstanceId: instanceId, providerSessionId: `source-${provider}-${count}`,
    filePath: `/fixture/${provider}-${count}.jsonl`, size: 1000, mtimeMs: 100, device: 1, inode: 2, birthtimeMs: null };
  const importedThreadId = ThreadId.make(`import:${instanceId}:${source.providerSessionId}`);
  const now = DateTime.makeUnsafe("2026-10-03T12:00:00-04:00");
  const value: OrchestrationV2AppThread = { ...thread(now), id: importedThreadId, createdBy: "system", creationSource: "server",
    historyOrigin: "v1_import", lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: importedThreadId } };
  const messageEvents: OrchestrationV2DomainEvent[] = [];
  for (let index = 0; index < count; index++) {
    const suffix = String(index).padStart(6, "0");
    const messageId = MessageId.make(`${importedThreadId}:${suffix}`);
    const turnItemId = TurnItemId.make(`agent-session-import:v2:turn-item:${importedThreadId}:${suffix}`);
    const role = index % 2 === 0 ? "user" as const : "assistant" as const;
    const text = `Exact visible text ${index}\n preserved`;
    messageEvents.push({ id: EventId.make(`agent-session-import:v2:message:${importedThreadId}:${suffix}`),
      type: "message.updated", threadId: importedThreadId, occurredAt: now,
      payload: { id: messageId, threadId: importedThreadId, createdBy: role === "user" ? "user" : "agent", creationSource: "server",
        role, text, runId: null, nodeId: null, attachments: [], streaming: false, createdAt: now, updatedAt: now } });
    const common = { id: turnItemId, threadId: importedThreadId, runId: null, nodeId: null, providerThreadId: null,
      providerTurnId: null, nativeItemRef: null, parentItemId: null, ordinal: index + 1, status: "completed" as const,
      title: null, startedAt: now, completedAt: now, updatedAt: now };
    messageEvents.push({ id: EventId.make(`agent-session-import:v2:turn-item:${importedThreadId}:${suffix}`),
      type: "turn-item.updated", threadId: importedThreadId, occurredAt: now,
      payload: role === "user" ? { ...common, createdBy: "user", creationSource: "server", type: "user_message", messageId,
        inputIntent: "turn_start", text, attachments: [] } : { ...common, type: "assistant_message", messageId, text, streaming: false } });
  }
  const input = { threadId: importedThreadId, projectId, source, parserPolicy: "agent_session_visible_messages_v1" as const,
    messageEvents, importedAt: DateTime.formatIso(now) };
  const write = Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    yield* sink.recordLegacyContinuationDisposition({ threadId: importedThreadId, provenance: "native_import",
      qualification: { type: "unknown", reason: "source_not_qualified" }, evidence: null, importedAt: input.importedAt });
    yield* runtimes.upsert({ threadId: importedThreadId, providerName: provider, providerInstanceId: instanceId,
      adapterKey: provider, runtimeMode: "full-access", status: "stopped", lastSeenAt: input.importedAt,
      resumeCursor: provider === "codex" ? { threadId: source.providerSessionId } : { threadId: importedThreadId, resume: source.providerSessionId },
      runtimePayload: { importOrigin: "native_import" } });
    yield* sink.write({ events: [{ id: EventId.make(`agent-session-import:v2:thread:${importedThreadId}:created`),
      type: "thread.created", threadId: importedThreadId, providerInstanceId: instanceId, occurredAt: now, payload: value }, ...messageEvents] });
    yield* runtimes.recordImportedTranscript({ threadId: importedThreadId, source });
  });
  return { input, write };
}

it.effect("future native seals preserve the exact Codex and Claude visible snapshot at bounds 1 and 200", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    for (const provider of ["codex", "claudeAgent"] as const) for (const count of [1, 200]) {
      const fixture = nativeImportFixture(provider, count);
      const seal = yield* sink.withTransaction(fixture.write.pipe(Effect.andThen(sink.recordNativeImportTranscriptSeal(fixture.input))));
      assert.strictEqual(seal.messageCount, count);
      assert.strictEqual(seal.eventBasis.length, count * 2);
      assert.match(seal.eventsSha256, /^[0-9a-f]{64}$/);
      assert.deepEqual(yield* sink.readNativeImportTranscriptSeal(fixture.input.threadId), seal);
      assert.isNull(yield* sink.getThreadIncarnation(fixture.input.threadId));
    }
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("old native markers and a new outer transaction cannot manufacture or replace a snapshot seal", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const fixture = nativeImportFixture("codex", 1);
    yield* sink.withTransaction(fixture.write);
    assert.isNull(yield* sink.readNativeImportTranscriptSeal(fixture.input.threadId));
    assert.strictEqual((yield* sink.withTransaction(sink.recordNativeImportTranscriptSeal(fixture.input)).pipe(Effect.result))._tag, "Failure");
    assert.isNull(yield* sink.readNativeImportTranscriptSeal(fixture.input.threadId));
    const sql = yield* SqlClient.SqlClient;
    assert.deepEqual(yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals`, []);
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("seal insertion and nested outer rollback discard events runtime markers and publications before exact retry", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const fixture = nativeImportFixture("codex", 1);
    const published = yield* Ref.make(0);
    const importAndSeal = fixture.write.pipe(Effect.andThen(sink.recordNativeImportTranscriptSeal(fixture.input)),
      Effect.tap(() => sink.onCommit(Ref.update(published, (value) => value + 1))));
    yield* sql`CREATE TRIGGER fixture_fail_seal BEFORE INSERT ON orchestration_v2_native_import_transcript_seals
      BEGIN SELECT RAISE(ABORT, 'injected seal failure'); END`;
    assert.strictEqual((yield* sink.withTransaction(importAndSeal).pipe(Effect.result))._tag, "Failure");
    yield* sql`DROP TRIGGER fixture_fail_seal`;
    assert.strictEqual((yield* sink.withTransaction(sink.withTransaction(importAndSeal).pipe(
      Effect.andThen(Effect.fail("outer rollback")))).pipe(Effect.result))._tag, "Failure");
    for (const rows of [yield* sql`SELECT * FROM orchestration_events WHERE stream_id = ${fixture.input.threadId}`,
      yield* sql`SELECT * FROM provider_session_runtime WHERE thread_id = ${fixture.input.threadId}`,
      yield* sql`SELECT * FROM orchestration_v2_legacy_continuation_dispositions WHERE thread_id = ${fixture.input.threadId}`,
      yield* sql`SELECT * FROM orchestration_v2_native_import_transcript_seals WHERE thread_id = ${fixture.input.threadId}`]) assert.deepEqual(rows, []);
    assert.strictEqual(yield* Ref.get(published), 0);
    const seal = yield* sink.withTransaction(importAndSeal);
    assert.deepEqual(yield* sink.readNativeImportTranscriptSeal(fixture.input.threadId), seal);
    assert.strictEqual(yield* Ref.get(published), 1);
    assert.strictEqual((yield* sink.withTransaction(sink.recordNativeImportTranscriptSeal(fixture.input)).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* sink.readNativeImportTranscriptSeal(fixture.input.threadId), seal);
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("native snapshot digest and source identity mismatches stay unknown without native IO", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const fixture = nativeImportFixture("codex", 1);
    const seal = yield* sink.withTransaction(fixture.write.pipe(Effect.andThen(sink.recordNativeImportTranscriptSeal(fixture.input))));
    yield* sql`UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.text', 'same-count changed text')
      WHERE event_id = ${seal.eventBasis[0]!.eventId}`;
    assert.isNull(yield* sink.readNativeImportTranscriptSeal(fixture.input.threadId));
    yield* sql`UPDATE provider_session_runtime SET runtime_payload_json = json_set(runtime_payload_json,
      '$.importedTranscripts[0].size', 9999) WHERE thread_id = ${fixture.input.threadId}`;
    assert.isNull(yield* sink.readNativeImportTranscriptSeal(fixture.input.threadId));
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("wrong-source incomplete reordered and over-limit native event arrays cannot seal a new import", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const fixture = nativeImportFixture("claudeAgent", 1);
    const invalid = [
      { ...fixture.input, source: { ...fixture.input.source, providerSessionId: "wrong-source" } },
      { ...fixture.input, messageEvents: fixture.input.messageEvents.slice(0, 1) },
      { ...fixture.input, messageEvents: [...fixture.input.messageEvents].reverse() },
      { ...fixture.input, messageEvents: nativeImportFixture("claudeAgent", 201).input.messageEvents },
    ];
    for (const input of invalid) {
      assert.strictEqual((yield* sink.withTransaction(fixture.write.pipe(Effect.andThen(sink.recordNativeImportTranscriptSeal(input))))
        .pipe(Effect.result))._tag, "Failure");
      assert.isNull(yield* sink.readNativeImportTranscriptSeal(fixture.input.threadId));
    }
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

const importedReviewFixture = Effect.fnUntraced(function* (withProject = false) {
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const fixture = nativeImportFixture("codex", 1);
  yield* sink.withTransaction(fixture.write.pipe(Effect.andThen(sink.recordNativeImportTranscriptSeal(fixture.input))));
  if (withProject) yield* sink.commitProjectCommand({ commandId: CommandId.make("command:imported-project"), projectId,
    commandType: "project.create", acceptedAt: DateTime.makeUnsafe(fixture.input.importedAt), event: {
      eventId: EventId.make("event:imported-project"), aggregateKind: "project", aggregateId: projectId,
      occurredAt: fixture.input.importedAt, commandId: null, causationEventId: null, correlationId: null, metadata: {},
      type: "project.created", payload: { projectId, title: "Imported project", workspaceRoot: "/fixture/imported-project",
        defaultModelSelection: { instanceId, model: "fixture-model" }, scripts: [], createdAt: fixture.input.importedAt, updatedAt: fixture.input.importedAt },
    } });
  const delivery: OrchestrationV2ImportedHistoryDelivery = { type: "message", messageId: MessageId.make("message:explicit-imported"),
    text: "Continue with this exact imported snapshot", attachments: [], runtimeMode: "full-access", interactionMode: "default",
    dispatchMode: { type: "start_immediately" } };
  const reviewContext: EventSink.ImportedHistoryStartReviewContextV2 = { actorSessionId: AuthSessionId.make("actor:imported"),
    threadId: fixture.input.threadId, delivery, readTargetCapability: (selected) => Effect.succeed({ instanceId: selected,
      driver, enabled: true, declared: { canConsumeHandoffSummaries: true, supportsFullThreadHandoff: true,
        supportsProviderSwitchingViaHandoff: true } }) };
  const facts = yield* sink.readImportedHistoryStartReview(reviewContext);
  assert.isNotNull(facts.review.reviewedBasis);
  const command: OrchestrationV2StartWithImportedHistoryCommand = { type: "thread.imported-history.start",
    commandId: CommandId.make("command:explicit-imported"), threadId: fixture.input.threadId,
    reviewedBasis: facts.review.reviewedBasis!, delivery };
  const runId = RunId.make("run:explicit-imported");
  const now = DateTime.makeUnsafe(fixture.input.importedAt);
  const events: OrchestrationV2DomainEvent[] = [
    { id: EventId.make("event:explicit-imported:message"), type: "message.updated", threadId: command.threadId, runId, occurredAt: now,
      payload: { id: delivery.messageId, threadId: command.threadId, runId, nodeId: null, createdBy: "user", creationSource: "web",
        role: "user", text: delivery.text, attachments: [], streaming: false, createdAt: now, updatedAt: now } },
    { id: EventId.make("event:explicit-imported:run"), type: "run.created", threadId: command.threadId, runId, occurredAt: now,
      payload: { id: runId, threadId: command.threadId, ordinal: 1, providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "fixture-model" }, providerThreadId: null, userMessageId: delivery.messageId,
        rootNodeId: null, activeAttemptId: null, status: "queued", requestedAt: now, startedAt: null, completedAt: null,
        checkpointId: null, contextHandoffId: null } },
  ];
  return { sink, sql, fixture, reviewContext, facts, command, runId, events, now };
});

it.effect("imported choice reserves its entire canonical command before allocation and exact replay never plans again", () =>
  Effect.gen(function* () {
    const value = yield* importedReviewFixture();
    const allocations = yield* Ref.make(0);
    const input = { command: value.command, reviewContext: value.reviewContext, revalidateAuthority: Effect.void,
      plan: () => Effect.gen(function* () {
        assert.strictEqual((yield* value.sql`SELECT command_id FROM orchestration_v2_imported_history_start_choices
          WHERE command_id = ${value.command.commandId}`).length, 1);
        yield* Ref.update(allocations, (count) => count + 1);
        return { runId: value.runId, messageId: value.command.delivery.messageId, events: value.events, effects: [] };
      }) };
    const first = yield* value.sink.commitImportedHistoryStart(input);
    assert.isTrue(first.committed);
    assert.strictEqual(first.outcome.receipt.status, "accepted");
    const replay = yield* value.sink.commitImportedHistoryStart(input);
    assert.isFalse(replay.committed);
    assert.deepEqual(replay.outcome, first.outcome);
    assert.strictEqual(yield* Ref.get(allocations), 1);
    const observation = yield* value.sink.observeImportedHistoryStart({ threadId: value.command.threadId, commandId: value.command.commandId });
    assert.strictEqual(observation.intentStatus, "accepted");
    assert.strictEqual(observation.execution.status, "pending");
    assert.strictEqual(observation.execution.effectOutcome, null);
    assert.isFalse(JSON.stringify(observation).includes("canonical_command_json"));
    assert.strictEqual((yield* value.sink.commitCommand({ commandId: value.command.commandId, threadId: value.command.threadId,
      commandType: "message.dispatch", acceptedAt: value.now, events: value.events, effects: [] }).pipe(Effect.result))._tag, "Failure");
    const changedDelivery = { ...value.reviewContext.delivery, text: "changed full command" };
    const changed = { ...input, command: { ...value.command, delivery: changedDelivery },
      reviewContext: { ...value.reviewContext, delivery: changedDelivery } };
    assert.strictEqual((yield* value.sink.commitImportedHistoryStart(changed).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual(yield* Ref.get(allocations), 1);
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("changed imported basis rejects durably before allocation while a failed plan rolls reservation back", () =>
  Effect.gen(function* () {
    const value = yield* importedReviewFixture();
    const allocations = yield* Ref.make(0);
    const input = { command: value.command, reviewContext: value.reviewContext, revalidateAuthority: Effect.void,
      plan: () => Ref.update(allocations, (count) => count + 1).pipe(Effect.andThen(Effect.fail("plan failure"))) };
    assert.strictEqual((yield* value.sink.commitImportedHistoryStart(input).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_imported_history_start_choices`, []);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_imported_history_start_outcomes`, []);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${value.command.commandId}`, []);
    yield* value.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.title', 'changed basis')
      WHERE thread_id = ${value.command.threadId}`;
    const rejected = yield* value.sink.commitImportedHistoryStart(input);
    assert.strictEqual(rejected.outcome.receipt.status, "rejected");
    assert.strictEqual(rejected.outcome.rejectionReason, "imported_history_review_changed");
    assert.strictEqual(yield* Ref.get(allocations), 1);
    assert.deepEqual((yield* value.sink.commitImportedHistoryStart(input)).outcome, rejected.outcome);
    assert.strictEqual(yield* Ref.get(allocations), 1);
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("missing imported owner capability or uncertain prior native effects yields no reviewed basis", () =>
  Effect.gen(function* () {
    const value = yield* importedReviewFixture();
    assert.isNull((yield* value.sink.readImportedHistoryStartReview({ ...value.reviewContext,
      readTargetCapability: () => Effect.succeed(null) })).review.reviewedBasis);
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue([{ id: "effect:prior-unknown", commandId: CommandId.make("command:prior-unknown"),
      threadId: value.command.threadId, request: { type: "provider-turn.start", runId: RunId.make("run:prior-unknown") } }]);
    const claimed = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker:prior", leaseDurationMs: 60_000 }));
    assert.isTrue(yield* outbox.holdUnknown({ effectId: claimed.id, workerId: "worker:prior", expectedAttempt: claimed.attemptCount,
      operationId: "operation:prior", evidence: { operationId: "operation:prior", operation: "start_turn",
        threadId: value.command.threadId, outcome: "unknown" } }));
    const review = yield* value.sink.readImportedHistoryStartReview(value.reviewContext);
    assert.isNull(review.review.reviewedBasis);
    assert.strictEqual(review.review.nativeEffects.type, "unknown");
    assert.strictEqual((yield* outbox.listHeldByThreadId(value.command.threadId)).length, 1);
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("an explicit imported queued choice preserves the same held run message payload and order", () =>
  Effect.gen(function* () {
    const value = yield* importedReviewFixture();
    const events = value.events.map((event): OrchestrationV2DomainEvent => event.type === "run.created"
      ? { ...event, payload: { ...event.payload, queueHeld: true, queuePosition: 7 } } : event);
    yield* value.sink.write({ events });
    const delivery: OrchestrationV2ImportedHistoryDelivery = { type: "queued_run", runId: value.runId,
      messageId: value.command.delivery.messageId };
    const reviewContext = { ...value.reviewContext, delivery };
    const facts = yield* value.sink.readImportedHistoryStartReview(reviewContext);
    assert.isNotNull(facts.review.reviewedBasis);
    const command: OrchestrationV2StartWithImportedHistoryCommand = { ...value.command, commandId: CommandId.make("command:queued-new-intent"),
      delivery, reviewedBasis: facts.review.reviewedBasis! };
    const beforeRuns = yield* value.sql`SELECT * FROM orchestration_v2_projection_runs WHERE thread_id = ${command.threadId}`;
    const beforeMessages = yield* value.sql`SELECT * FROM orchestration_v2_projection_messages WHERE thread_id = ${command.threadId}`;
    const currentThread = yield* (yield* ProjectionStore.ProjectionStoreV2).getThread(command.threadId);
    const result = yield* value.sink.commitImportedHistoryStart({ command, reviewContext, revalidateAuthority: Effect.void,
      plan: () => Effect.succeed({ runId: value.runId, messageId: delivery.messageId,
        effects: [{ id: `effect:${command.commandId}:provider-turn.start:${value.runId}`, commandId: command.commandId,
          threadId: command.threadId, request: { type: "provider-turn.start", runId: value.runId } }], events: [{
        id: EventId.make("event:queued-new-intent:accepted"), type: "thread.metadata-updated", threadId: command.threadId,
        occurredAt: value.now, payload: currentThread,
      } satisfies OrchestrationV2DomainEvent] }) });
    assert.strictEqual(result.outcome.runId, value.runId);
    assert.strictEqual(result.outcome.messageId, delivery.messageId);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_projection_runs WHERE thread_id = ${command.threadId}`, beforeRuns);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_projection_messages WHERE thread_id = ${command.threadId}`, beforeMessages);
    assert.strictEqual((yield* value.sink.observeImportedHistoryStart({ threadId: command.threadId, commandId: command.commandId })).execution.status, "pending");
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

const claimedImportedFixture = Effect.fnUntraced(function* (queued = false) {
  const value = yield* importedReviewFixture(true);
  const providerThreadId = ProviderThreadId.make("provider-thread:explicit-fresh");
  const providerSessionId = ProviderSessionId.make("provider-session:explicit-fresh");
  const attemptId = RunAttemptId.make("attempt:explicit-fresh");
  const rootNodeId = NodeId.make("node:explicit-fresh");
  const scopeId = CheckpointScopeId.make("scope:explicit-fresh");
  const provider = { id: providerThreadId, driver, providerInstanceId: instanceId, providerSessionId: null,
    appThreadId: value.command.threadId, ownerNodeId: rootNodeId,
    nativeThreadRef: queued ? { driver, nativeId: "historical-queued-source", strength: "strong" as const } : null, nativeConversationHeadRef: null,
    status: "idle" as const, firstRunOrdinal: 1, lastRunOrdinal: 1, handoffIds: [], forkedFrom: null, createdAt: value.now, updatedAt: value.now };
  const node = { id: rootNodeId, threadId: value.command.threadId, runId: value.runId, parentNodeId: null, rootNodeId,
    kind: "root_turn" as const, status: "pending" as const, countsForRun: true, providerThreadId, providerTurnId: null,
    nativeItemRef: null, runtimeRequestId: null, checkpointScopeId: null, startedAt: null, completedAt: null };
  const initial: OrchestrationV2DomainEvent[] = [
    ...value.events.map((event): OrchestrationV2DomainEvent => event.type === "run.created" ? { ...event,
      payload: { ...event.payload, providerThreadId, rootNodeId, activeAttemptId: attemptId,
        status: queued ? "queued" : "starting", ...(queued ? { queueHeld: true, queuePosition: 7 } : {}) } } : event),
    { id: EventId.make("event:explicit-fresh:provider"), type: "provider-thread.updated", threadId: value.command.threadId,
      occurredAt: value.now, payload: provider },
    { id: EventId.make("event:explicit-fresh:node"), type: "node.updated", threadId: value.command.threadId, occurredAt: value.now, payload: node },
    { id: EventId.make("event:explicit-fresh:attempt"), type: "run-attempt.created", threadId: value.command.threadId, occurredAt: value.now,
      payload: { id: attemptId, runId: value.runId, attemptOrdinal: 1, rootNodeId, providerInstanceId: instanceId, providerThreadId,
        providerTurnId: null, reason: "initial", status: "pending", startedAt: null, completedAt: null } },
  ];
  let command = value.command;
  let reviewContext = value.reviewContext;
  let acceptanceEvents = initial;
  if (queued) {
    yield* value.sink.write({ events: initial });
    reviewContext = { ...reviewContext, delivery: { type: "queued_run", runId: value.runId, messageId: command.delivery.messageId } };
    const reviewed = yield* value.sink.readImportedHistoryStartReview(reviewContext);
    assert.isNotNull(reviewed.review.reviewedBasis);
    command = { ...command, delivery: reviewContext.delivery, reviewedBasis: reviewed.review.reviewedBasis! };
    const importedThread = yield* (yield* ProjectionStore.ProjectionStoreV2).getThread(command.threadId);
    acceptanceEvents = [{ id: EventId.make("event:explicit-queued:accepted"), type: "thread.metadata-updated",
      threadId: command.threadId, occurredAt: value.now, payload: importedThread }];
  }
  const effectId = `effect:${command.commandId}:provider-turn.start:${value.runId}`;
  yield* value.sink.commitImportedHistoryStart({ command, reviewContext, revalidateAuthority: Effect.void,
    plan: () => Effect.succeed({ runId: value.runId, messageId: command.delivery.messageId, events: acceptanceEvents,
      effects: [{ id: effectId, commandId: command.commandId, threadId: command.threadId,
        request: { type: "provider-turn.start", runId: value.runId } }] }) });
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const claimed = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker:explicit-fresh", leaseDurationMs: 60_000 }));
  assert.strictEqual(claimed.id, effectId);
  const preparedProviderId = queued ? ProviderThreadId.make("provider-thread:explicit-prepared-queued") : providerThreadId;
  const current = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(command.threadId, ["runs", "attempts"]);
  const currentThread = current.thread;
  const events: OrchestrationV2DomainEvent[] = [
    { id: EventId.make("event:explicit-fresh:planned-session"), type: "provider-thread.updated", threadId: value.command.threadId,
      occurredAt: value.now, payload: { ...provider, id: preparedProviderId, providerSessionId, nativeThreadRef: null } },
    { id: EventId.make("event:explicit-fresh:scope"), type: "checkpoint-scope.created", threadId: value.command.threadId, occurredAt: value.now,
      payload: { id: scopeId, threadId: value.command.threadId, runId: value.runId, nodeId: rootNodeId, parentScopeId: null,
        providerThreadId: preparedProviderId, kind: "root_run", ordinalWithinParent: 1, advancesAppRunCount: true, cwd: "/fixture/imported-project", createdAt: value.now } },
    { id: EventId.make("event:explicit-fresh:prepared-node"), type: "node.updated", threadId: value.command.threadId,
      occurredAt: value.now, payload: { ...node, providerThreadId: preparedProviderId, checkpointScopeId: scopeId } },
    { id: EventId.make("event:explicit-fresh:current-pointer"), type: "thread.provider-switched", threadId: value.command.threadId,
      occurredAt: value.now, payload: { ...currentThread, activeProviderThreadId: preparedProviderId } },
  ];
  if (queued) events.push(
    { id: EventId.make("event:explicit-queued:prepared-run"), type: "run.updated", threadId: command.threadId, occurredAt: value.now,
      payload: { ...current.runs.find((run) => run.id === value.runId)!, providerThreadId: preparedProviderId, status: "starting" } },
    { id: EventId.make("event:explicit-queued:prepared-attempt"), type: "run-attempt.updated", threadId: command.threadId, occurredAt: value.now,
      payload: { ...current.attempts.find((attempt) => attempt.id === attemptId)!, providerThreadId: preparedProviderId } },
  );
  const makeInput = Effect.gen(function* () {
    const facts = yield* value.sink.readNativeCommandFacts({ threadId: command.threadId, commandId: command.commandId,
      authority: { actorSessionId: value.reviewContext.actorSessionId } });
    return { reference: { commandId: command.commandId, threadId: command.threadId, runId: value.runId, effectId },
      workerId: "worker:explicit-fresh", expectedAttempt: claimed.attemptCount, currentSnapshot: facts.commitSnapshot,
      reviewSource: { readTargetCapability: reviewContext.readTargetCapability }, revalidateAuthority: () => Effect.void,
      prepare: () => Effect.succeed(events) };
  });
  return { ...value, command, reviewContext, provider, providerThreadId: preparedProviderId, originalProviderThreadId: providerThreadId,
    providerSessionId, attemptId, rootNodeId, scopeId, node, events, outbox, claimed, effectId, makeInput };
});

const addHeldQueuedSibling = Effect.fnUntraced(function* (value: Effect.Success<ReturnType<typeof claimedImportedFixture>>, position: number) {
  const current = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(value.command.threadId, ["runs"]);
  const original = current.runs.find((run) => run.id === value.runId)!;
  const runId = RunId.make("run:other-held-position");
  const messageId = MessageId.make("message:other-held-position");
  yield* value.sink.write({ events: [
    { id: EventId.make("event:other-held-position:run"), type: "run.created", threadId: value.command.threadId, occurredAt: value.now,
      payload: { ...original, id: runId, ordinal: original.ordinal + 1, queuePosition: position, queueHeld: true,
        userMessageId: messageId, rootNodeId: null, activeAttemptId: null } },
    { id: EventId.make("event:other-held-position:message"), type: "message.updated", threadId: value.command.threadId, occurredAt: value.now,
      payload: { id: messageId, threadId: value.command.threadId, runId, nodeId: null, createdBy: "user", creationSource: "web",
        role: "user", text: "Other held payload",
        attachments: [], streaming: false, createdAt: value.now, updatedAt: value.now } },
  ] });
  return { runId, messageId };
});

it.effect("claimed queued choice keeps its original reservation and payload while preparing a fresh target ahead of later held work", () =>
  Effect.gen(function* () {
    const value = yield* claimedImportedFixture(true);
    const sibling = yield* addHeldQueuedSibling(value, 9);
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const before = yield* projections.getThreadRecords(value.command.threadId, ["runs", "attempts", "providerThreads", "messages"]);
    const messages = yield* value.sql`SELECT * FROM orchestration_v2_projection_messages WHERE thread_id = ${value.command.threadId} ORDER BY message_id`;
    const originalRun = before.runs.find((run) => run.id === value.runId)!;
    const originalProvider = before.providerThreads.find((provider) => provider.id === value.originalProviderThreadId)!;
    const siblingRun = before.runs.find((run) => run.id === sibling.runId)!;
    assert.strictEqual((yield* value.sink.prepareImportedHistoryStartExecution(yield* value.makeInput)).status, "prepared");
    const after = yield* projections.getThreadRecords(value.command.threadId, ["runs", "attempts", "providerThreads", "nodes"]);
    const preparedRun = after.runs.find((run) => run.id === value.runId)!;
    assert.deepEqual({ ...preparedRun, providerThreadId: originalRun.providerThreadId, status: originalRun.status }, originalRun);
    assert.strictEqual(preparedRun.providerThreadId, value.providerThreadId);
    assert.strictEqual(preparedRun.status, "starting");
    assert.deepEqual(after.runs.find((run) => run.id === sibling.runId), siblingRun);
    assert.deepEqual(after.providerThreads.find((provider) => provider.id === value.originalProviderThreadId), originalProvider);
    assert.strictEqual(after.attempts.find((attempt) => attempt.id === value.attemptId)?.providerThreadId, value.providerThreadId);
    assert.strictEqual(after.nodes.find((node) => node.id === value.rootNodeId)?.providerThreadId, value.providerThreadId);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_projection_messages WHERE thread_id = ${value.command.threadId} ORDER BY message_id`, messages);
    const reservation = yield* value.sql<{ readonly basis_json: string }>`SELECT basis_json FROM orchestration_v2_queued_start_reservations WHERE effect_id = ${value.effectId}`;
    const basis = JSON.parse(reservation[0]!.basis_json);
    assert.strictEqual(basis.queuedProviderThreadId, value.originalProviderThreadId);
    assert.strictEqual(basis.sourceMode, "new_context");
    assert.strictEqual(basis.executionIntent.kind, "imported_history_choice");
    assert.notStrictEqual(basis.queuedProviderThreadId, preparedRun.providerThreadId);
    assert.strictEqual((yield* value.sink.prepareImportedHistoryStartExecution(yield* value.makeInput)).status, "already_prepared");
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.effectId)).status, "running");
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("a non-head accepted queued choice performs no preparation or reservation and retains its original pending delivery position", () =>
  Effect.gen(function* () {
    const value = yield* claimedImportedFixture(true);
    yield* addHeldQueuedSibling(value, 1);
    const before = yield* value.sql`SELECT * FROM orchestration_v2_projection_runs WHERE thread_id = ${value.command.threadId} ORDER BY run_id`;
    const calls = yield* Ref.make(0);
    const input = yield* value.makeInput;
    assert.deepEqual(yield* value.sink.prepareImportedHistoryStartExecution({ ...input,
      prepare: () => Ref.update(calls, (count) => count + 1).pipe(Effect.as(value.events)) }),
      { status: "rejected", reason: "queued_delivery_not_first" });
    assert.strictEqual(yield* Ref.get(calls), 0);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_queued_start_reservations`, []);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_projection_runs WHERE thread_id = ${value.command.threadId} ORDER BY run_id`, before);
    assert.strictEqual((yield* value.sink.readImportedHistoryStartChoice(value.command))?.receipt.status, "accepted");
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.effectId)).status, "running");
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("a proven non-head delivery parks the same effect without spending attempts and reclaims only after actual queue advancement", () =>
  Effect.gen(function* () {
    const value = yield* claimedImportedFixture(true);
    const sibling = yield* addHeldQueuedSibling(value, 1);
    const beforeRuns = yield* value.sql`SELECT * FROM orchestration_v2_projection_runs WHERE thread_id = ${value.command.threadId} ORDER BY run_id`;
    const beforeAttempts = yield* value.sql`SELECT * FROM orchestration_v2_projection_run_attempts WHERE thread_id = ${value.command.threadId} ORDER BY attempt_id`;
    const input = { effectId: value.effectId, commandId: value.command.commandId, threadId: value.command.threadId,
      runId: value.runId, runAttemptId: value.attemptId, workerId: "worker:explicit-fresh", expectedAttempt: value.claimed.attemptCount };
    assert.deepEqual(yield* value.outbox.parkImportedHistoryDelivery({ ...input, workerId: "wrong-worker" }), { status: "rejected", reason: "claim_changed" });
    assert.deepEqual(yield* value.outbox.parkImportedHistoryDelivery({ ...input, expectedAttempt: input.expectedAttempt + 1 }), { status: "rejected", reason: "claim_changed" });
    const parked = yield* value.outbox.parkImportedHistoryDelivery(input);
    assert.strictEqual(parked.status, "parked");
    assert.deepEqual(yield* value.outbox.parkImportedHistoryDelivery(input), parked);
    const pending = Option.getOrThrow(yield* value.outbox.get(value.effectId));
    assert.strictEqual(pending.status, "pending");
    assert.strictEqual(pending.attemptCount, value.claimed.attemptCount - 1);
    assert.isNull(pending.leaseOwner);
    assert.isNull(pending.leaseExpiresAt);
    assert.strictEqual(pending.lastError, "imported-history.waiting-for-head/v1");
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_projection_runs WHERE thread_id = ${value.command.threadId} ORDER BY run_id`, beforeRuns);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_projection_run_attempts WHERE thread_id = ${value.command.threadId} ORDER BY attempt_id`, beforeAttempts);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_queued_start_reservations`, []);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_native_effect_confirmations`, []);
    assert.deepEqual(yield* value.outbox.listHeldByThreadId(value.command.threadId), []);
    assert.isTrue(Option.isNone(yield* value.outbox.nextClaimableAt));
    assert.isTrue(Option.isNone(yield* value.outbox.claimNext({ workerId: "worker:next-head", leaseDurationMs: 60_000 })));
    const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(value.command.threadId, ["runs"]);
    const head = records.runs.find((run) => run.id === sibling.runId)!;
    yield* value.sink.write({ events: [{ id: EventId.make("event:other-held-position:advanced"), type: "run.updated", threadId: value.command.threadId,
      occurredAt: value.now, payload: { ...head, status: "cancelled", completedAt: value.now } }] });
    assert.isTrue(Option.isSome(yield* value.outbox.nextClaimableAt));
    const reclaimed = Option.getOrThrow(yield* value.outbox.claimNext({ workerId: "worker:next-head", leaseDurationMs: 60_000 }));
    assert.strictEqual(reclaimed.id, value.effectId);
    assert.strictEqual(reclaimed.attemptCount, value.claimed.attemptCount);
    assert.deepEqual(reclaimed.request, value.claimed.request);
    assert.strictEqual((yield* value.sink.prepareImportedHistoryStartExecution({ ...(yield* value.makeInput), workerId: "worker:next-head" })).status, "prepared");
    assert.deepEqual(yield* value.outbox.parkImportedHistoryDelivery({ ...input, workerId: "worker:next-head" }), { status: "rejected", reason: "queue_changed" });
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

for (const variant of ["expired", "changed_payload", "unknown"] as const) {
  it.effect(`HEAD park rejects ${variant} proof without resetting its claim or native uncertainty`, () =>
    Effect.gen(function* () {
      const value = yield* claimedImportedFixture(true);
      yield* addHeldQueuedSibling(value, 1);
      if (variant === "expired") {
        yield* value.sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = ${DateTime.formatIso(yield* DateTime.now)} WHERE effect_id = ${value.effectId}`;
      } else if (variant === "changed_payload") {
        const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(value.command.threadId, ["runs"]);
        const run = records.runs.find((run) => run.id === value.runId)!;
        yield* value.sink.write({ events: [{ id: EventId.make("event:explicit-queued:changed-position"), type: "run.updated",
          threadId: value.command.threadId, occurredAt: value.now, payload: { ...run, queuePosition: 2 } }] });
      } else {
        assert.isTrue(yield* value.outbox.holdUnknown({ effectId: value.effectId, workerId: "worker:explicit-fresh",
          expectedAttempt: value.claimed.attemptCount, operationId: value.effectId,
          evidence: { operationId: value.effectId, operation: "start_turn", outcome: "unknown", threadId: value.command.threadId } }));
      }
      const before = Option.getOrThrow(yield* value.outbox.get(value.effectId));
      const result = yield* value.outbox.parkImportedHistoryDelivery({ effectId: value.effectId, commandId: value.command.commandId,
        threadId: value.command.threadId, runId: value.runId, runAttemptId: value.attemptId, workerId: "worker:explicit-fresh",
        expectedAttempt: value.claimed.attemptCount });
      assert.strictEqual(result.status, "rejected");
      assert.deepEqual(Option.getOrThrow(yield* value.outbox.get(value.effectId)), before);
      if (variant === "unknown") assert.strictEqual((yield* value.outbox.listHeldByThreadId(value.command.threadId)).length, 1);
    }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
  );
}

it.effect("claimed imported preparation reserves before SQL IDs and leaves real capabilities to manager open", () =>
  Effect.gen(function* () {
    const value = yield* claimedImportedFixture();
    const callbacks = yield* Ref.make(0);
    const input = yield* value.makeInput;
    const prepare = () => Effect.gen(function* () {
      assert.strictEqual((yield* value.sql`SELECT effect_id FROM orchestration_v2_queued_start_reservations WHERE effect_id = ${value.effectId}`).length, 1);
      assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id = ${value.providerSessionId}`, []);
      yield* Ref.update(callbacks, (count) => count + 1);
      return value.events;
    });
    const result = yield* value.sink.prepareImportedHistoryStartExecution({ ...input, prepare });
    assert.strictEqual(result.status, "prepared");
    const records = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(value.command.threadId, ["providerThreads", "providerSessions", "nodes", "checkpointScopes"]);
    assert.strictEqual(records.thread.activeProviderThreadId, value.providerThreadId);
    assert.strictEqual(records.providerThreads.find((provider) => provider.id === value.providerThreadId)?.providerSessionId, value.providerSessionId);
    assert.strictEqual(records.nodes.find((node) => node.id === value.rootNodeId)?.checkpointScopeId, value.scopeId);
    assert.deepEqual(records.providerSessions, []);
    assert.isNull(yield* value.sink.readCurrentProviderRuntimeOwner(value.command.threadId));
    const replay = yield* value.sink.prepareImportedHistoryStartExecution({ ...(yield* value.makeInput), prepare });
    assert.strictEqual(replay.status, "already_prepared");
    assert.strictEqual(yield* Ref.get(callbacks), 1);
    assert.strictEqual((yield* value.sink.observeImportedHistoryStart({ threadId: value.command.threadId, commandId: value.command.commandId })).execution.status, "unknown");
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.effectId)).status, "running");
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

it.effect("claimed preparation rejects historical session IDs and rolls reservation back on invalid scope or outer failure", () =>
  Effect.gen(function* () {
    const value = yield* claimedImportedFixture();
    const scopeMismatch = value.events.map((event): OrchestrationV2DomainEvent => event.type === "checkpoint-scope.created"
      ? { ...event, payload: { ...event.payload, cwd: "/different-workspace" } } : event);
    assert.strictEqual((yield* value.sink.prepareImportedHistoryStartExecution({ ...(yield* value.makeInput),
      prepare: () => Effect.succeed(scopeMismatch) }).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_queued_start_reservations`, []);
    const notifications = yield* Ref.make(0);
    assert.strictEqual((yield* value.sink.withTransaction(Effect.gen(function* () {
      assert.strictEqual((yield* value.sink.prepareImportedHistoryStartExecution(yield* value.makeInput)).status, "prepared");
      yield* value.sink.onCommit(Ref.update(notifications, (count) => count + 1));
      return yield* Effect.fail("outer claimed preparation rollback");
    })).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_queued_start_reservations`, []);
    assert.strictEqual(yield* Ref.get(notifications), 0);
    const historicalId = ProviderSessionId.make("session:historical-reused");
    yield* value.sink.write({ events: [{ id: EventId.make("event:historical-reused-session"), type: "provider-session.attached",
      threadId: value.command.threadId, occurredAt: value.now, payload: { id: historicalId, driver, providerInstanceId: instanceId,
        status: "stopped", cwd: "/fixture/imported-project", model: "fixture-model", capabilities: CodexProviderCapabilitiesV2,
        createdAt: value.now, updatedAt: value.now, lastError: null } }] });
    yield* value.sql`DELETE FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id = ${historicalId}`;
    yield* value.sql`DELETE FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id = ${historicalId}`;
    const reusedEvents = value.events.map((event): OrchestrationV2DomainEvent => event.type === "provider-thread.updated"
      ? { ...event, payload: { ...event.payload, providerSessionId: historicalId } } : event);
    assert.strictEqual((yield* value.sink.prepareImportedHistoryStartExecution({ ...(yield* value.makeInput),
      prepare: () => Effect.succeed(reusedEvents) }).pipe(Effect.result))._tag, "Failure");
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_queued_start_reservations`, []);
    const before = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(value.command.threadId, ["providerThreads", "nodes", "checkpointScopes"]);
    assert.strictEqual(before.providerThreads.find((provider) => provider.id === value.providerThreadId)?.providerSessionId, null);
    assert.strictEqual(before.nodes.find((node) => node.id === value.rootNodeId)?.checkpointScopeId, null);
    assert.deepEqual(before.checkpointScopes, []);
    assert.strictEqual((yield* value.sink.prepareImportedHistoryStartExecution(yield* value.makeInput)).status, "prepared");
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);

for (const queued of [false, true]) {
it.effect(`imported ${queued ? "same queued" : "immediate"} ACK confirmation retains exact application lineage across observation revisions without minting native authority`, () =>
  Effect.gen(function* () {
    const value = yield* claimedImportedFixture(queued);
    const repository = yield* NativeCreationRepository;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    assert.strictEqual((yield* value.sink.prepareImportedHistoryStartExecution(yield* value.makeInput)).status, "prepared");
    const before = yield* projections.getThreadRecords(value.command.threadId, ["runs", "attempts", "providerThreads"]);
    const run = before.runs.find((candidate) => candidate.id === value.runId)!;
    const attempt = before.attempts.find((candidate) => candidate.id === value.attemptId)!;
    const provider = before.providerThreads.find((candidate) => candidate.id === value.providerThreadId)!;
    const nativeThreadId = "native:explicit-fresh";
    yield* value.sink.write({ events: [
      { id: EventId.make("event:explicit-fresh:actual-session"), type: "provider-session.attached", threadId: value.command.threadId,
        occurredAt: value.now, payload: { id: value.providerSessionId, driver, providerInstanceId: instanceId, status: "ready",
          cwd: "/fixture/imported-project", model: run.modelSelection.model, capabilities: CodexProviderCapabilitiesV2,
          createdAt: value.now, updatedAt: value.now, lastError: null } },
      { id: EventId.make("event:explicit-fresh:actual-native"), type: "provider-thread.updated", threadId: value.command.threadId,
        occurredAt: value.now, payload: { ...provider, nativeThreadRef: { driver, nativeId: nativeThreadId, strength: "strong" }, status: "active" } },
      { id: EventId.make("event:explicit-fresh:actual-run"), type: "run.updated", threadId: value.command.threadId,
        occurredAt: value.now, payload: { ...run, status: "running", startedAt: value.now } },
      { id: EventId.make("event:explicit-fresh:actual-attempt"), type: "run-attempt.updated", threadId: value.command.threadId,
        occurredAt: value.now, payload: { ...attempt, status: "running", startedAt: value.now } },
    ] });
    const binding = { threadId: value.command.threadId, providerThreadId: value.providerThreadId, providerSessionId: value.providerSessionId,
      instanceId, nativeThreadId, runtimeGeneration: "actual:explicit-fresh-generation" };
    const registered = yield* value.sink.registerProviderRuntime({ expectedBinding: { ...binding, driver, runtimeGeneration: null },
      expectedEvidenceRevision: 0, actualBinding: binding, expectedRunId: value.runId, expectedRunAttemptId: value.attemptId,
      actualContinuationSourceIdentity: { driverKind: driver, continuationKey: "codex:actual-fixture-home", runtimeGeneration: binding.runtimeGeneration } });
    if (!registered.committed) return yield* Effect.die(registered.rejection);
    assert.isTrue(registered.committed);
    const expected = { expectedBinding: { ...binding, driver }, expectedEvidenceRevision: registered.evidenceRevision };
    assert.isNull(yield* value.sink.readConfirmedImportedHistoryBinding(expected));
    const input = { effectId: value.effectId, workerId: "worker:explicit-fresh", expectedAttempt: value.claimed.attemptCount,
      runId: value.runId, attemptId: value.attemptId, binding, expectedEvidenceRevision: registered.evidenceRevision,
      evidence: { operationId: value.effectId, operation: "start_turn" as const, outcome: "confirmed_success" as const,
        threadId: binding.threadId, providerThreadId: binding.providerThreadId, providerSessionId: binding.providerSessionId,
        instanceId, runtimeGeneration: binding.runtimeGeneration, attemptId: value.attemptId } };
    yield* value.sql`CREATE TRIGGER fixture_fail_application_confirmation BEFORE INSERT ON orchestration_v2_native_effect_confirmations
      BEGIN SELECT RAISE(ABORT, 'fixture complete application ACK failure'); END`;
    assert.strictEqual((yield* repository.recordNativeEffectConfirmation(input).pipe(Effect.result))._tag, "Failure");
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.effectId)).status, "running");
    assert.isNull(yield* value.sink.readConfirmedImportedHistoryBinding(expected));
    yield* value.sql`DROP TRIGGER fixture_fail_application_confirmation`;
    const proof = yield* repository.recordNativeEffectConfirmation(input);
    assert.isNull(proof.nativeExecutionReference);
    assert.deepEqual(yield* value.sink.readConfirmedImportedHistoryBinding(expected), proof);
    assert.strictEqual((yield* value.sink.observeImportedHistoryStart({ threadId: value.command.threadId, commandId: value.command.commandId })).execution.status, "started");
    assert.deepEqual(yield* value.sql`SELECT * FROM native_creation_effect_facts`, []);
    const observation = { status: "working" as const, binding, observedAt: DateTime.formatIso(value.now) };
    const advanced = yield* value.sink.writeIfCurrentProviderRuntimeOwner({ ...expected, events: [], observation, revalidateCurrentOwner: Effect.void });
    if (!advanced.committed) return yield* Effect.die(advanced.rejection);
    assert.isTrue(advanced.committed);
    assert.isNull(yield* value.sink.readConfirmedImportedHistoryBinding(expected));
    assert.deepEqual(yield* value.sink.readConfirmedImportedHistoryBinding({ ...expected, expectedEvidenceRevision: advanced.evidenceRevision }), proof);
    assert.isNull(yield* value.sink.readConfirmedImportedHistoryBinding({ expectedBinding: { ...expected.expectedBinding,
      runtimeGeneration: "replacement-generation" }, expectedEvidenceRevision: advanced.evidenceRevision }));
    const initialLineage = yield* value.sink.readConfirmedImportedHistoryContinuation({ ...expected, expectedEvidenceRevision: advanced.evidenceRevision });
    assert.deepEqual(initialLineage?.confirmation, proof);
    assert.strictEqual(initialLineage?.currentSource.runtimeGeneration, binding.runtimeGeneration);
    const replacementSessionId = ProviderSessionId.make("session:explicit-fresh-replacement");
    const replacementBinding = { ...binding, providerSessionId: replacementSessionId, runtimeGeneration: "actual:replacement-generation" };
    yield* value.sink.write({ events: [
      { id: EventId.make("event:explicit-fresh:replacement-session"), type: "provider-session.attached", threadId: value.command.threadId,
        occurredAt: value.now, payload: { id: replacementSessionId, driver, providerInstanceId: instanceId, status: "ready",
          cwd: "/fixture/imported-project", model: run.modelSelection.model, capabilities: CodexProviderCapabilitiesV2,
          createdAt: value.now, updatedAt: value.now, lastError: null } },
      { id: EventId.make("event:explicit-fresh:replacement-binding"), type: "provider-thread.updated", threadId: value.command.threadId,
        occurredAt: value.now, payload: { ...provider, providerSessionId: replacementSessionId,
          nativeThreadRef: { driver, nativeId: nativeThreadId, strength: "strong" }, status: "active" } },
    ] });
    const replaced = yield* value.sink.registerProviderRuntime({ expectedBinding: { ...replacementBinding, driver },
      expectedRegisteredBinding: expected.expectedBinding, expectedEvidenceRevision: advanced.evidenceRevision,
      actualBinding: replacementBinding, expectedRunId: value.runId, expectedRunAttemptId: value.attemptId });
    if (!replaced.committed) return yield* Effect.die(replaced.rejection);
    assert.isTrue(replaced.committed);
    const currentExpected = { expectedBinding: { ...replacementBinding, driver }, expectedEvidenceRevision: replaced.evidenceRevision };
    assert.isNull(yield* value.sink.readConfirmedImportedHistoryBinding(currentExpected));
    assert.isNull(yield* value.sink.readConfirmedImportedHistoryContinuation(currentExpected));
    const observedReplacement = yield* value.sink.registerProviderRuntime({ ...currentExpected,
      actualBinding: replacementBinding, actualContinuationSourceIdentity: { driverKind: driver,
        continuationKey: "codex:actual-fixture-home", runtimeGeneration: replacementBinding.runtimeGeneration },
      expectedRunId: value.runId, expectedRunAttemptId: value.attemptId });
    if (!observedReplacement.committed) return yield* Effect.die(observedReplacement.rejection);
    assert.isTrue(observedReplacement.committed);
    const continued = yield* value.sink.readConfirmedImportedHistoryContinuation({ ...currentExpected,
      expectedEvidenceRevision: observedReplacement.evidenceRevision });
    assert.deepEqual(continued?.confirmation, proof);
    assert.strictEqual(continued?.historicalSource.runtimeGeneration, binding.runtimeGeneration);
    assert.strictEqual(continued?.currentSource.runtimeGeneration, replacementBinding.runtimeGeneration);
    assert.strictEqual(continued?.historicalSource.continuationKey, continued?.currentSource.continuationKey);
    assert.isNull(yield* value.sink.readConfirmedImportedHistoryContinuation({ ...currentExpected,
      expectedEvidenceRevision: observedReplacement.evidenceRevision - 1 }));
    const wrongHomeBinding = { ...replacementBinding, runtimeGeneration: "actual:another-store-generation" };
    const wrongHome = yield* value.sink.registerProviderRuntime({ expectedBinding: currentExpected.expectedBinding,
      expectedEvidenceRevision: observedReplacement.evidenceRevision, actualBinding: wrongHomeBinding,
      actualContinuationSourceIdentity: { driverKind: driver, continuationKey: "codex:different-actual-home",
        runtimeGeneration: wrongHomeBinding.runtimeGeneration }, expectedRunId: value.runId, expectedRunAttemptId: value.attemptId });
    if (!wrongHome.committed) return yield* Effect.die(wrongHome.rejection);
    assert.isTrue(wrongHome.committed);
    assert.isNull(yield* value.sink.readConfirmedImportedHistoryContinuation({ expectedBinding: { ...wrongHomeBinding, driver },
      expectedEvidenceRevision: wrongHome.evidenceRevision }));
    assert.deepEqual(yield* repository.readNativeEffectConfirmation(value.effectId), proof);
    assert.strictEqual((yield* value.sink.readLegacyContinuationDisposition(value.command.threadId))?.qualification.type, "unknown");
    assert.strictEqual(Option.getOrThrow(yield* value.outbox.get(value.effectId)).status, "succeeded");
  }).pipe(Effect.provide(Layer.fresh(sealTestLayer))),
);
}
