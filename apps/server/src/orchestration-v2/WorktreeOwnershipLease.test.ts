import { CheckpointScopeId, CommandId, EventId, OrchestrationV2Command, ProjectId, ProviderInstanceId, RunId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import { makeWorktreeOwnershipLeaseStore } from "./WorktreeOwnershipLease.ts";

const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
);
const TestLayer = Layer.mergeAll(EventSink.layer, EffectOutbox.layer).pipe(Layer.provideMerge(stores));

const insertBirth = Effect.fn("WorktreeOwnershipLease.test.insertBirth")(function* (input: {
  readonly threadId: ThreadId;
  readonly eventId: string;
  readonly applicationEventVersion?: 1 | 2;
  readonly streamVersion?: number;
  readonly includeProjection?: boolean;
  readonly projectionId?: ThreadId;
  readonly historyOrigin?: "native" | "v1_import";
  readonly fullApplicationIdentity?: boolean;
}) {
  const sql = yield* SqlClient.SqlClient;
  const payload = JSON.stringify({
    id: input.projectionId ?? input.threadId,
    ...(input.historyOrigin === undefined ? {} : { historyOrigin: input.historyOrigin }),
    ...(input.fullApplicationIdentity === true ? {
      projectId: "project-lease-fixture", createdAt: "2026-10-03T00:00:00Z",
    } : {}),
  });
  const rows = yield* sql<{ readonly sequence: number }>`
    INSERT INTO orchestration_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, actor_kind, payload_json, metadata_json, application_event_version
    ) VALUES (
      ${input.eventId}, 'thread', ${input.threadId}, ${input.streamVersion ?? 0},
      'thread.created', '2026-10-03T00:00:00Z', 'server', ${payload}, '{}',
      ${input.applicationEventVersion ?? 2}
    ) RETURNING sequence
  `;
  if (input.includeProjection !== false) {
    yield* sql`
      INSERT INTO orchestration_v2_projection_threads (
        thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
        created_at, updated_at, payload_json
      ) VALUES (
        ${input.threadId}, 'project-lease-fixture', 'Lease fixture', 'codex',
        'full-access', 'default', '2026-10-03T00:00:00Z', '2026-10-03T00:00:00Z', ${payload}
      ) ON CONFLICT(thread_id) DO NOTHING
    `;
  }
  return rows[0]!.sequence;
});

const insertPathAdmission = Effect.fn("WorktreeOwnershipLease.test.insertPathAdmission")(function* (
  resourcePath: string,
  state: "reserved" | "started" | "unknown" | "no_effect",
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT INTO orchestration_v2_worktree_path_admissions (
      operation_id, canonical_path, kind, subject_json, state,
      started_at, outcome_json, recorded_at, updated_at
    ) VALUES (
      ${`removal:${resourcePath}`}, ${resourcePath}, 'worktree_removal', '{}', ${state},
      ${state === "started" ? "2026-10-03T09:00:01.000Z" : null},
      ${state === "unknown" ? '{"effect":"unknown"}' : state === "no_effect" ? '{"effect":"no_effect"}' : null},
      '2026-10-03T09:00:00.000Z', '2026-10-03T09:00:02.000Z'
    )
  `;
});

const ordinaryOwnUseFixture = Effect.fn("WorktreeOwnershipLease.test.ordinaryOwnUseFixture")(function* (
  key: string,
  outboxUse = false,
  expired = false,
) {
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const store = yield* makeWorktreeOwnershipLeaseStore();
  const now = yield* DateTime.now;
  const nowMs = DateTime.toEpochMillis(now);
  const threadId = ThreadId.make(`ordinary-use:${key}`);
  const projectId = ProjectId.make(`ordinary-use-project:${key}`);
  const instanceId = ProviderInstanceId.make(`ordinary-use-instance-${key}`);
  const commandId = CommandId.make(`ordinary-use-command:${key}`);
  const projectRoot = `/workspace/ordinary-use-${key}`;
  const resourcePath = `${projectRoot}/checkout`;
  const app = { createdBy: "user" as const, creationSource: "web" as const, id: threadId, projectId, title: "Own-use fixture",
    providerInstanceId: instanceId, modelSelection: { instanceId, model: "fixture-model" }, runtimeMode: "full-access" as const,
    interactionMode: "default" as const, branch: "feature/own-use", worktreePath: resourcePath, activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId }, forkedFrom: null,
    createdAt: now, updatedAt: now, archivedAt: null, settledOverride: null, settledAt: null, lastVisitedAt: null, deletedAt: null };
  yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at, deleted_at)
    VALUES (${projectId}, 'Own-use project', ${projectRoot}, '[]', ${DateTime.formatIso(now)}, ${DateTime.formatIso(now)}, NULL)`;
  const command: Extract<OrchestrationV2Command, { readonly type: "thread.create" }> = { type: "thread.create",
    createdBy: app.createdBy, creationSource: app.creationSource, commandId, threadId, projectId, title: app.title,
    modelSelection: app.modelSelection, runtimeMode: app.runtimeMode, interactionMode: app.interactionMode,
    branch: app.branch, worktreePath: app.worktreePath };
  const canonicalCommand = JSON.parse(JSON.stringify(Schema.encodeSync(OrchestrationV2Command)(command)));
  const context: EventSink.OrdinaryCheckoutCommitContextV1 = { command, captureAfterProjection: () => Effect.gen(function* () {
    const birth = (yield* sink.readApplicationBirthRecord(threadId))!;
    const lease = Option.getOrThrow(yield* store.ensureOrdinaryOwnership({ resourcePath, leaseId: `ordinary-use-lease:${key}`,
      ownerThreadId: threadId, ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(birth),
      branch: app.branch, nowMs, expiresAtMs: expired ? nowMs : nowMs + 300_000 }));
    return [{ capture: { version: 1, commandId, commandType: command.type, canonicalCommand,
      commandDigest: OrdinaryCheckout.ordinaryCheckoutCommandDigestV1(canonicalCommand), origin: { kind: "command" },
      threadId, applicationBirth: birth, projectId, canonicalProjectRoot: projectRoot, canonicalCheckoutPath: resourcePath,
      branch: app.branch, lease }, originalAdmission: null, source: { projectWorkspaceRoot: projectRoot, worktreePath: resourcePath } }];
  }) };
  const effectId = `ordinary-use-effect:${key}`;
  yield* sink.commitCommand({ commandId, threadId, commandType: command.type, acceptedAt: now, ordinaryCheckoutContext: context,
    events: [{ id: EventId.make(`ordinary-use-birth:${key}`), type: "thread.created", threadId, occurredAt: now, payload: app }],
    effects: outboxUse ? [{ id: effectId, commandId, threadId, request: { type: "checkpoint.capture",
      runId: RunId.make(`ordinary-use-run:${key}`), scopeId: CheckpointScopeId.make(`ordinary-use-scope:${key}`) } }] : [] });
  const admission = (yield* sink.readOrdinaryCheckoutAdmission({ commandId, threadId }))!;
  const reference = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
  let source: OrdinaryCheckout.OrdinaryCheckoutUseSourceV1;
  if (outboxUse) {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId: `ordinary-use-worker:${key}`, leaseDurationMs: 60_000 }));
    assert.equal(claim.id, effectId);
    const link = (yield* sink.readOrdinaryCheckoutEffectLink(effectId))!;
    source = { kind: "outbox", link, workerId: claim.leaseOwner!, expectedAttempt: claim.attemptCount,
      leaseExpiresAt: DateTime.makeUnsafe(claim.leaseExpiresAt!) };
  } else {
    source = { kind: "prepared_launch", admission: reference, preparationCommandId: commandId,
      preparationEvent: admission.eventBasis[0]!, applicationBirth: admission.capture.applicationBirth, projectId,
      canonicalProjectRoot: projectRoot, canonicalCheckoutPath: resourcePath, branch: app.branch };
  }
  const reserved = yield* sink.beginOrdinaryCheckoutUse({ operationId: outboxUse ? effectId : `ordinary-use-operation:${key}`,
    admission: reference, source, targetSource: { projectWorkspaceRoot: projectRoot, worktreePath: resourcePath } });
  assert.equal(reserved.status, "reserved");
  return { sink, sql, store, nowMs, resourcePath, admission, use: reserved.record.subject.use };
});

it.layer(TestLayer)("WorktreeOwnershipLeaseStore", (it) => {
  it.effect("an expired exact owner reservation renews before live entry while generic mutations remain fenced", () =>
    Effect.gen(function* () {
      const value = yield* ordinaryOwnUseFixture("expired-positive", false, true);
      const before = Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath));
      assert.equal((yield* value.sink.readOrdinaryCheckoutUse(value.use.operationId))?.state, "reserved");
      assert.equal((yield* value.sink.revalidateOrdinaryCheckoutUse(value.use).pipe(Effect.result))._tag, "Failure");
      for (const mutation of [value.store.renew({ ...before, nowMs: value.nowMs + 1_000, expiresAtMs: value.nowMs + 301_000 }),
        value.store.acquire({ ...before, leaseId: "forbidden-own-rotation", nowMs: value.nowMs + 1_000, expiresAtMs: value.nowMs + 301_000 }),
        value.store.release(before)]) assert.equal((yield* mutation.pipe(Effect.result))._tag, "Failure");
      assert.deepEqual(Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath)), before);
      assert.isTrue(yield* value.store.renewOrdinaryOwnUse({ ordinaryUse: value.use, nowMs: value.nowMs + 1_000, expiresAtMs: value.nowMs + 301_000 }));
      const renewed = Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath));
      assert.deepEqual(OrdinaryCheckout.ordinaryCheckoutLeaseIdentityV1(renewed), OrdinaryCheckout.ordinaryCheckoutLeaseIdentityV1(before));
      assert.equal(renewed.renewedAtMs, value.nowMs + 1_000);
      assert.equal(renewed.expiresAtMs, value.nowMs + 301_000);
      assert.equal((yield* value.sink.revalidateOrdinaryCheckoutUse(value.use)).state, "started");
      assert.isTrue(yield* value.store.renewOrdinaryOwnUse({ ordinaryUse: value.use, nowMs: value.nowMs + 2_000, expiresAtMs: value.nowMs + 302_000 }));
    }),
  );

  it.effect("qualified renewal rejects a foreign or rotated captured identity without changing the lease", () =>
    Effect.gen(function* () {
      const value = yield* ordinaryOwnUseFixture("foreign-capture");
      const before = Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath));
      for (const lease of [{ ...value.use.lease, ownerThreadId: ThreadId.make("foreign-own-use-owner") },
        { ...value.use.lease, leaseId: "rotated-own-use-generation" }]) {
        const error = yield* value.store.renewOrdinaryOwnUse({ ordinaryUse: { ...value.use, lease },
          nowMs: value.nowMs + 1_000, expiresAtMs: value.nowMs + 301_000 }).pipe(Effect.flip);
        assert.equal(error._tag, "PersistenceSqlError");
        assert.equal(error.operation, "WorktreeOwnershipLeaseStore.renewOrdinaryOwnUse:query");
        assert.deepEqual(Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath)), before);
      }
    }),
  );

  it.effect("qualified outbox renewal checks the actual worker and attempt claim before mutation", () =>
    Effect.gen(function* () {
      for (const variant of ["worker", "attempt"] as const) {
        const value = yield* ordinaryOwnUseFixture(`claim-${variant}`, true);
        assert.isTrue(yield* value.store.renewOrdinaryOwnUse({ ordinaryUse: value.use,
          nowMs: value.nowMs + 1_000, expiresAtMs: value.nowMs + 301_000 }));
        const before = Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath));
        if (variant === "worker") yield* value.sql`UPDATE orchestration_v2_effect_outbox SET lease_owner = 'replacement-worker'
          WHERE effect_id = ${value.use.operationId}`;
        else yield* value.sql`UPDATE orchestration_v2_effect_outbox SET attempt_count = attempt_count + 1
          WHERE effect_id = ${value.use.operationId}`;
        assert.equal((yield* value.store.renewOrdinaryOwnUse({ ordinaryUse: value.use,
          nowMs: value.nowMs + 2_000, expiresAtMs: value.nowMs + 302_000 }).pipe(Effect.result))._tag, "Failure");
        assert.deepEqual(Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath)), before);
      }
    }),
  );

  it.effect("qualified renewal rolls back decreasing liveness and rejects an unknown reservation", () =>
    Effect.gen(function* () {
      const value = yield* ordinaryOwnUseFixture("unknown-and-monotonic");
      assert.isTrue(yield* value.store.renewOrdinaryOwnUse({ ordinaryUse: value.use,
        nowMs: value.nowMs + 1_000, expiresAtMs: value.nowMs + 301_000 }));
      const before = Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath));
      for (const times of [{ nowMs: value.nowMs + 999, expiresAtMs: value.nowMs + 301_000 },
        { nowMs: value.nowMs + 1_001, expiresAtMs: value.nowMs + 300_999 }]) {
        assert.equal((yield* value.store.renewOrdinaryOwnUse({ ordinaryUse: value.use, ...times }).pipe(Effect.result))._tag, "Failure");
        assert.deepEqual(Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath)), before);
      }
      yield* value.sink.holdOrdinaryCheckoutUseUnknown({ use: value.use, reason: "original physical completion unavailable" });
      assert.equal((yield* value.store.renewOrdinaryOwnUse({ ordinaryUse: value.use,
        nowMs: value.nowMs + 2_000, expiresAtMs: value.nowMs + 302_000 }).pipe(Effect.result))._tag, "Failure");
      assert.equal((yield* value.sink.readOrdinaryCheckoutUse(value.use.operationId))?.state, "unknown");
      assert.deepEqual(Option.getOrThrow(yield* value.store.getByResourcePath(value.resourcePath)), before);
    }),
  );

  it.effect("cleanup finalization cannot release a retained lease without its qualified immutable task", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const resourcePath = "/workspace/unqualified-cleanup-finalizer";
      const retained = Option.getOrThrow(yield* store.acquire({
        resourcePath, leaseId: "unqualified-cleanup-lease", ownerThreadId: ThreadId.make("unqualified-cleanup-owner"),
        ownerIncarnation: "unqualified-cleanup-birth", branch: null, nowMs: 1_000, expiresAtMs: 2_000,
      }));
      yield* insertPathAdmission(resourcePath, "unknown");
      assert.deepEqual(yield* store.finalizeDeletionWorktreeCleanup({
        effectId: `removal:${resourcePath}`, bindingSha256: "a".repeat(64), expectedLatestOrdinal: 0,
      }), { status: "retained", reason: "original_worktree_task_unavailable" });
      assert.deepEqual(Option.getOrThrow(yield* store.getByResourcePath(resourcePath)), retained);
      const error = yield* store.release(retained).pipe(Effect.flip);
      assert.equal(error._tag, "PersistenceSqlError");
      assert.equal(error.operation, "WorktreeOwnershipLeaseStore.release:query");
      assert.deepEqual(Option.getOrThrow(yield* store.getByResourcePath(resourcePath)), retained);
    }),
  );

  it.effect("reads ordinary imported application birth without weakening the strict native getter", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const threadId = ThreadId.make("ordinary-imported-owner");
      const eventId = "migration:v1:thread:ordinary-imported-owner:created";
      const sequence = yield* insertBirth({ threadId, eventId, historyOrigin: "v1_import", fullApplicationIdentity: true });
      assert.equal(Option.getOrThrow(yield* store.getOrdinaryThreadIncarnation(threadId)), JSON.stringify([
        "t3.orchestration-v2.thread-birth/v1", eventId, sequence,
      ]));
      assert.isTrue(Option.isNone(yield* store.getThreadIncarnation(threadId)));
      assert.isTrue(Option.isNone(yield* store.getOrdinaryThreadIncarnation(ThreadId.make("ordinary-birth-missing"))));
      const ownerIncarnation = Option.getOrThrow(yield* store.getOrdinaryThreadIncarnation(threadId));
      const input = { resourcePath: "/workspace/ordinary-imported", leaseId: "ordinary-imported-lease", ownerThreadId: threadId,
        ownerIncarnation, branch: null, nowMs: 1_000, expiresAtMs: 2_000 };
      assert.isTrue(Option.isSome(yield* store.ensureOrdinaryOwnership(input)));
      const retained = Option.getOrThrow(yield* store.acquire({ ...input,
        resourcePath: "/workspace/ordinary-imported-historical", ownerIncarnation: "historical-legacy-import-birth",
      }));
      assert.isTrue(Option.isNone(yield* store.ensureOrdinaryOwnership({ ...input, resourcePath: retained.resourcePath })));
      assert.deepEqual(Option.getOrThrow(yield* store.getByResourcePath(retained.resourcePath)), retained);
    }),
  );

  it.effect("stable ordinary ownership retains the exact generation while low-level acquire still rotates", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const ownerThreadId = ThreadId.make("ordinary-stable-owner");
      yield* insertBirth({ threadId: ownerThreadId, eventId: "ordinary-stable-birth", fullApplicationIdentity: true });
      const ownerIncarnation = Option.getOrThrow(yield* store.getOrdinaryThreadIncarnation(ownerThreadId));
      const input = { resourcePath: "/workspace/ordinary-stable", leaseId: "ordinary-stable-first", ownerThreadId,
        ownerIncarnation, branch: "feature/stable", nowMs: 1_000, expiresAtMs: 2_000 };
      assert.isTrue(Option.isNone(yield* store.getByResourcePath(input.resourcePath)));
      const first = Option.getOrThrow(yield* store.ensureOrdinaryOwnership(input));
      assert.deepEqual(Option.getOrThrow(yield* store.getByResourcePath(input.resourcePath)), first);
      const repeated = Option.getOrThrow(yield* store.ensureOrdinaryOwnership({
        ...input, leaseId: "ordinary-unused-candidate", nowMs: 10_000, expiresAtMs: 11_000,
      }));
      assert.deepEqual(repeated, first);
      const rotated = Option.getOrThrow(yield* store.acquire({ ...input,
        leaseId: "ordinary-explicit-rotation", nowMs: 12_000, expiresAtMs: 13_000,
      }));
      assert.equal(rotated.leaseId, "ordinary-explicit-rotation");
      assert.equal(rotated.acquiredAtMs, first.acquiredAtMs);
      assert.deepEqual(Option.getOrThrow(yield* store.ensureOrdinaryOwnership({
        ...input, leaseId: "ordinary-another-unused-candidate", nowMs: 14_000, expiresAtMs: 15_000,
      })), rotated);
      assert.isTrue(Option.isNone(yield* store.getByResourcePath("/workspace/ordinary-stable-unrelated")));
    }),
  );

  it.effect("ordinary ensure rejects foreign, changed-branch and recreated births without inheritance", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const ownerThreadId = ThreadId.make("ordinary-recreated-owner");
      yield* insertBirth({ threadId: ownerThreadId, eventId: "ordinary-original-birth", fullApplicationIdentity: true });
      const ownerIncarnation = Option.getOrThrow(yield* store.getOrdinaryThreadIncarnation(ownerThreadId));
      const input = { resourcePath: "/workspace/ordinary-recreated", leaseId: "ordinary-original-lease", ownerThreadId,
        ownerIncarnation, branch: "feature/original", nowMs: 1_000, expiresAtMs: 2_000 };
      const retained = Option.getOrThrow(yield* store.ensureOrdinaryOwnership(input));
      assert.isTrue(Option.isNone(yield* store.ensureOrdinaryOwnership({ ...input, branch: "feature/changed" })));
      const contender = ThreadId.make("ordinary-expired-contender");
      yield* insertBirth({ threadId: contender, eventId: "ordinary-contender-birth", fullApplicationIdentity: true });
      assert.isTrue(Option.isNone(yield* store.ensureOrdinaryOwnership({ ...input,
        ownerThreadId: contender, ownerIncarnation: Option.getOrThrow(yield* store.getOrdinaryThreadIncarnation(contender)),
        leaseId: "ordinary-expired-takeover", nowMs: 10_000, expiresAtMs: 11_000,
      })));
      yield* insertBirth({ threadId: ownerThreadId, eventId: "ordinary-recreated-birth", streamVersion: 1, fullApplicationIdentity: true });
      const recreated = Option.getOrThrow(yield* store.getOrdinaryThreadIncarnation(ownerThreadId));
      assert.notEqual(recreated, ownerIncarnation);
      assert.isTrue(Option.isNone(yield* store.ensureOrdinaryOwnership({ ...input, ownerIncarnation: recreated,
        leaseId: "ordinary-recreated-lease", nowMs: 12_000, expiresAtMs: 13_000,
      })));
      assert.isTrue(Option.isNone(yield* store.ensureOrdinaryOwnership(input)));
      assert.deepEqual(Option.getOrThrow(yield* store.getByResourcePath(input.resourcePath)), retained);
    }),
  );

  it.effect("ordinary ensure requires current committed birth before inserting a lease", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const ownerThreadId = ThreadId.make("ordinary-missing-insert-birth");
      const input = { resourcePath: "/workspace/ordinary-missing-insert", leaseId: "ordinary-missing-insert-lease",
        ownerThreadId, ownerIncarnation: "invented-birth", branch: null, nowMs: 1_000, expiresAtMs: 2_000 };
      assert.isTrue(Option.isNone(yield* store.ensureOrdinaryOwnership(input)));
      yield* insertBirth({ threadId: ownerThreadId, eventId: "ordinary-real-insert-birth", fullApplicationIdentity: true });
      assert.isTrue(Option.isNone(yield* store.ensureOrdinaryOwnership(input)));
      assert.isTrue(Option.isNone(yield* store.getByResourcePath(input.resourcePath)));
    }),
  );

  it.effect("ordinary stable ensure preserves pending and unknown deletion fences while targeted reads remain available", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const ownerThreadId = ThreadId.make("ordinary-deletion-fenced-owner");
      yield* insertBirth({ threadId: ownerThreadId, eventId: "ordinary-deletion-fenced-birth", fullApplicationIdentity: true });
      const ownerIncarnation = Option.getOrThrow(yield* store.getOrdinaryThreadIncarnation(ownerThreadId));
      for (const state of ["reserved", "unknown"] as const) {
        const resourcePath = `/workspace/ordinary-ensure-fence-${state}`;
        const input = { resourcePath, leaseId: `ordinary-ensure-${state}`, ownerThreadId,
          ownerIncarnation, branch: null, nowMs: 1_000, expiresAtMs: 2_000 };
        const retained = Option.getOrThrow(yield* store.ensureOrdinaryOwnership(input));
        yield* insertPathAdmission(resourcePath, state);
        const error = yield* store.ensureOrdinaryOwnership({ ...input, leaseId: "ordinary-unused-fenced-id" }).pipe(Effect.flip);
        assert.equal(error._tag, "PersistenceSqlError");
        assert.equal(error.operation, "WorktreeOwnershipLeaseStore.ensureOrdinaryOwnership:query");
        assert.deepEqual(Option.getOrThrow(yield* store.getByResourcePath(resourcePath)), retained);
      }
    }),
  );

  it.effect("pending and unknown removal admissions prevent all same-owner lease mutations", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const sink = yield* EventSink.EventSinkV2;
      for (const state of ["reserved", "started", "unknown"] as const) {
        const resourcePath = `/workspace/deletion-lease-${state}`;
        const retained = Option.getOrThrow(yield* store.acquire({
          resourcePath,
          leaseId: `lease-before-${state}`,
          ownerThreadId: ThreadId.make(`thread-deletion-lease-${state}`),
          ownerIncarnation: `birth-${state}`,
          branch: "feature/retained",
          nowMs: 1_000,
          expiresAtMs: 2_000,
        }));
        yield* insertPathAdmission(resourcePath, state);
        const admission = yield* sink.readDeletionWorktreePathAdmission({ path: resourcePath });
        assert.equal(admission.status, "reserved");
        assert.equal(admission.admissions[0]?.state, state);

        const acquireError = yield* store.acquire({
          ...retained, leaseId: `lease-after-${state}`, branch: "feature/changed",
          nowMs: 10_000, expiresAtMs: 11_000,
        }).pipe(Effect.flip);
        assert.equal(acquireError._tag, "PersistenceSqlError");
        assert.equal(acquireError.operation, "WorktreeOwnershipLeaseStore.acquire:query");
        assert.equal(acquireError.cause instanceof EventSink.EventSinkWriteError, true);
        assert.deepEqual((yield* store.listAll()).find((lease) => lease.resourcePath === resourcePath), retained);

        const renewError = yield* store.renew({ ...retained, nowMs: 10_001, expiresAtMs: 11_001 }).pipe(Effect.flip);
        assert.equal(renewError._tag, "PersistenceSqlError");
        assert.equal(renewError.operation, "WorktreeOwnershipLeaseStore.renew:query");
        assert.equal(renewError.cause instanceof EventSink.EventSinkWriteError, true);
        assert.deepEqual((yield* store.listAll()).find((lease) => lease.resourcePath === resourcePath), retained);

        const releaseError = yield* store.release(retained).pipe(Effect.flip);
        assert.equal(releaseError._tag, "PersistenceSqlError");
        assert.equal(releaseError.operation, "WorktreeOwnershipLeaseStore.release:query");
        assert.equal(releaseError.cause instanceof EventSink.EventSinkWriteError, true);
        assert.deepEqual((yield* store.listAll()).find((lease) => lease.resourcePath === resourcePath), retained);
      }
    }),
  );

  it.effect("healthy path admission permits lease mutation independently of a pending removal", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const sink = yield* EventSink.EventSinkV2;
      const resourcePath = "/workspace/deletion-lease-healthy";
      yield* insertPathAdmission("/workspace/deletion-lease-unrelated", "reserved");
      yield* insertPathAdmission(resourcePath, "no_effect");
      assert.equal((yield* sink.readDeletionWorktreePathAdmission({ path: resourcePath })).status, "available");
      const first = Option.getOrThrow(yield* store.acquire({
        resourcePath, leaseId: "lease-healthy-first", ownerThreadId: ThreadId.make("thread-lease-healthy"),
        ownerIncarnation: "birth-healthy", branch: null, nowMs: 1_000, expiresAtMs: 2_000,
      }));
      assert.isTrue(yield* store.renew({ ...first, nowMs: 1_500, expiresAtMs: 2_500 }));
      const renewed = (yield* store.listAll()).find((lease) => lease.resourcePath === resourcePath);
      assert.isDefined(renewed);
      assert.equal(renewed?.renewedAtMs, 1_500);
      assert.equal(renewed?.expiresAtMs, 2_500);
      const rotated = Option.getOrThrow(yield* store.acquire({
        ...first, leaseId: "lease-healthy-next", nowMs: 1_600, expiresAtMs: 2_600,
      }));
      assert.equal(rotated.acquiredAtMs, first.acquiredAtMs);
      assert.equal(rotated.leaseId, "lease-healthy-next");
      yield* store.release(rotated);
      assert.equal((yield* store.listAll()).some((lease) => lease.resourcePath === resourcePath), false);
      assert.equal((yield* sink.readDeletionWorktreePathAdmission({ path: "/workspace/deletion-lease-unrelated" })).status, "reserved");
    }),
  );

  it.effect("binds the lease incarnation to the committed V2 birth event id and sequence", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread-v2-birth");
      const eventId = 'birth/"native"';
      const sequence = yield* insertBirth({ threadId, eventId, historyOrigin: "native" });
      const expected = JSON.stringify([
        "t3.orchestration-v2.thread-birth/v1",
        eventId,
        sequence,
      ]);
      assert.equal(Option.getOrThrow(yield* store.getThreadIncarnation(threadId)), expected);

      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          'event-legacy-birth', 'thread', ${threadId}, 1, 'thread.created',
          '2026-10-03T00:00:01Z', 'server', '{}', '{}', 1
        )
      `;
      assert.equal(Option.getOrThrow(yield* store.getThreadIncarnation(threadId)), expected);
    }),
  );

  it.effect("reports missing, legacy-only, unmatched and ambiguous birth lineage unavailable", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const sql = yield* SqlClient.SqlClient;
      assert.isTrue(Option.isNone(yield* store.getThreadIncarnation(ThreadId.make("missing"))));

      const legacyOnly = ThreadId.make("thread-legacy-only");
      yield* insertBirth({
        threadId: legacyOnly,
        eventId: "event-legacy-only",
        applicationEventVersion: 1,
      });
      assert.isTrue(Option.isNone(yield* store.getThreadIncarnation(legacyOnly)));
      yield* sql`
        INSERT INTO orchestration_v2_events (
          event_id, thread_id, event_type, occurred_at, payload_json
        ) VALUES (
          'event-historical-preview-birth', ${legacyOnly}, 'thread.created',
          '2026-10-03T00:00:00Z', '{}'
        )
      `;
      assert.isTrue(Option.isNone(yield* store.getThreadIncarnation(legacyOnly)));

      const missingProjection = ThreadId.make("thread-missing-projection");
      yield* insertBirth({
        threadId: missingProjection,
        eventId: "event-missing-projection",
        includeProjection: false,
      });
      assert.isTrue(Option.isNone(yield* store.getThreadIncarnation(missingProjection)));

      const mismatchedProjection = ThreadId.make("thread-mismatched-projection");
      yield* insertBirth({
        threadId: mismatchedProjection,
        eventId: "event-mismatched-projection",
        projectionId: ThreadId.make("thread-other-projection"),
      });
      assert.isTrue(Option.isNone(yield* store.getThreadIncarnation(mismatchedProjection)));

      const ambiguous = ThreadId.make("thread-ambiguous-birth");
      yield* insertBirth({ threadId: ambiguous, eventId: "event-ambiguous-first" });
      yield* insertBirth({
        threadId: ambiguous,
        eventId: "event-ambiguous-second",
        streamVersion: 1,
      });
      assert.isTrue(Option.isNone(yield* store.getThreadIncarnation(ambiguous)));
    }),
  );

  it.effect("does not alias imported V2 shells to retained historical lease identities", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const sql = yield* SqlClient.SqlClient;
      const importedThreads = [
        ThreadId.make("thread-import-marker"),
        ThreadId.make("thread-import-origin"),
        ThreadId.make("thread-import-birth"),
      ];
      yield* insertBirth({ threadId: importedThreads[0]!, eventId: "event-import-marker" });
      yield* sql`
        INSERT INTO orchestration_v2_legacy_imports (
          thread_id, source_updated_at, shell_imported_at
        ) VALUES (${importedThreads[0]!}, '2026-10-03T00:00:00Z', '2026-10-03T00:00:00Z')
      `;
      yield* insertBirth({
        threadId: importedThreads[1]!,
        eventId: "event-import-origin",
        historyOrigin: "v1_import",
      });
      yield* insertBirth({
        threadId: importedThreads[2]!,
        eventId: "migration:v1:thread:imported:created",
      });

      for (const ownerThreadId of importedThreads) {
        const retained = Option.getOrThrow(yield* store.acquire({
          resourcePath: `/workspace/${ownerThreadId}`,
          leaseId: `lease:${ownerThreadId}`,
          ownerThreadId,
          ownerIncarnation: `historical-event:${ownerThreadId}`,
          branch: "feature/imported",
          nowMs: 1_000,
          expiresAtMs: 2_000,
        }));
        assert.isTrue(Option.isNone(yield* store.getThreadIncarnation(ownerThreadId)));
        assert.deepEqual(
          (yield* store.listAll()).find((lease) => lease.resourcePath === retained.resourcePath),
          retained,
        );
      }
    }),
  );

  it.effect("does not rekey a historical lease when a new V2 birth is available", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const ownerThreadId = ThreadId.make("thread-historical-lease");
      const retained = Option.getOrThrow(yield* store.acquire({
        resourcePath: "/workspace/historical-lease",
        leaseId: "lease-historical",
        ownerThreadId,
        ownerIncarnation: "legacy-created-event",
        branch: "feature/historical",
        nowMs: 1_000,
        expiresAtMs: 2_000,
      }));
      yield* insertBirth({ threadId: ownerThreadId, eventId: "event-new-v2-birth" });
      const ownerIncarnation = Option.getOrThrow(yield* store.getThreadIncarnation(ownerThreadId));
      assert.isTrue(Option.isNone(yield* store.acquire({
        ...retained,
        leaseId: "lease-new-v2",
        ownerIncarnation,
        nowMs: 10_000,
        expiresAtMs: 11_000,
      })));
      assert.isFalse(yield* store.renew({
        ...retained,
        ownerIncarnation,
        nowMs: 10_000,
        expiresAtMs: 11_000,
      }));
      yield* store.release({ ...retained, ownerIncarnation });
      const retainedAfterV2Birth = (yield* store.listAll()).find(
        (lease) => lease.resourcePath === retained.resourcePath,
      );
      assert.isDefined(retainedAfterV2Birth);
      assert.deepEqual(retainedAfterV2Birth, retained);
    }),
  );

  it.effect("preserves the persistence error contract when current birth evidence cannot decode", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread-malformed-birth-evidence");
      yield* insertBirth({ threadId, eventId: "event-malformed-birth-evidence" });
      yield* sql`
        UPDATE orchestration_v2_projection_threads
        SET payload_json = '{invalid-json'
        WHERE thread_id = ${threadId}
      `;
      const error = yield* store.getThreadIncarnation(threadId).pipe(Effect.flip);
      assert.equal(error._tag, "PersistenceSqlError");
      assert.equal(error.operation, "WorktreeOwnershipLeaseStore.getThreadIncarnation:query");
    }),
  );

  it.effect("rotates same-owner generations and never grants expiry-only takeover", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const resourcePath = "/workspace/project";
      const ownerThreadId = ThreadId.make("thread-owner");
      const contenderThreadId = ThreadId.make("thread-contender");
      const ownerIncarnation = "event-owner-created";

      const first = Option.getOrThrow(
        yield* store.acquire({
          resourcePath,
          leaseId: "lease-1",
          ownerThreadId,
          ownerIncarnation,
          branch: "feature/owner",
          nowMs: 1_000,
          expiresAtMs: 2_000,
        }),
      );
      const recovered = Option.getOrThrow(
        yield* store.acquire({
          resourcePath,
          leaseId: "lease-2",
          ownerThreadId,
          ownerIncarnation,
          branch: "feature/owner",
          nowMs: 1_500,
          expiresAtMs: 2_500,
        }),
      );

      assert.equal(recovered.acquiredAtMs, first.acquiredAtMs);
      assert.equal(recovered.leaseId, "lease-2");
      assert.isTrue(
        yield* store.renew({
          resourcePath,
          leaseId: recovered.leaseId,
          ownerThreadId,
          ownerIncarnation,
          nowMs: 1_550,
          expiresAtMs: 2_550,
        }),
      );
      const renewed = (yield* store.listAll()).find((lease) => lease.resourcePath === resourcePath);
      assert.isDefined(renewed);
      assert.equal(renewed?.acquiredAtMs, first.acquiredAtMs);
      assert.equal(renewed?.renewedAtMs, 1_550);
      assert.equal(renewed?.expiresAtMs, 2_550);
      assert.isFalse(
        yield* store.renew({
          resourcePath,
          leaseId: "lease-1",
          ownerThreadId,
          ownerIncarnation,
          nowMs: 1_600,
          expiresAtMs: 2_600,
        }),
      );

      yield* store.release(first);
      const retainedAfterStaleRelease = (yield* store.listAll()).find(
        (lease) => lease.resourcePath === resourcePath,
      );
      assert.isDefined(retainedAfterStaleRelease);
      assert.equal(retainedAfterStaleRelease?.leaseId, "lease-2");

      const expiredTakeover = yield* store.acquire({
        resourcePath,
        leaseId: "lease-3",
        ownerThreadId: contenderThreadId,
        ownerIncarnation: "event-contender-created",
        branch: "feature/contender",
        nowMs: 10_000,
        expiresAtMs: 11_000,
      });
      assert.isTrue(Option.isNone(expiredTakeover));

      yield* store.release(recovered);
      const acquiredAfterRelease = yield* store.acquire({
        resourcePath,
        leaseId: "lease-4",
        ownerThreadId: contenderThreadId,
        ownerIncarnation: "event-contender-created",
        branch: "feature/contender",
        nowMs: 10_001,
        expiresAtMs: 11_001,
      });
      assert.isTrue(Option.isSome(acquiredAfterRelease));
    }),
  );

  it.effect("does not recover a retained lease for a recreated thread id", () =>
    Effect.gen(function* () {
      const store = yield* makeWorktreeOwnershipLeaseStore();
      const resourcePath = "/workspace/recreated";
      const ownerThreadId = ThreadId.make("thread-reused-id");

      const retained = yield* store.acquire({
        resourcePath,
        leaseId: "lease-old-incarnation",
        ownerThreadId,
        ownerIncarnation: "event-old-incarnation",
        branch: "feature/old",
        nowMs: 1_000,
        expiresAtMs: 2_000,
      });
      assert.isTrue(Option.isSome(retained));

      const recreated = yield* store.acquire({
        resourcePath,
        leaseId: "lease-new-incarnation",
        ownerThreadId,
        ownerIncarnation: "event-new-incarnation",
        branch: "feature/new",
        nowMs: 10_000,
        expiresAtMs: 11_000,
      });
      assert.isTrue(Option.isNone(recreated));

      assert.isFalse(
        yield* store.renew({
          resourcePath,
          leaseId: "lease-old-incarnation",
          ownerThreadId,
          ownerIncarnation: "event-new-incarnation",
          nowMs: 10_001,
          expiresAtMs: 11_001,
        }),
      );
      yield* store.release({
        resourcePath,
        leaseId: "lease-old-incarnation",
        ownerThreadId,
        ownerIncarnation: "event-new-incarnation",
      });
      const retainedAfterMismatch = (yield* store.listAll()).find(
        (lease) => lease.resourcePath === resourcePath,
      );
      assert.isDefined(retainedAfterMismatch);
      assert.deepEqual(retainedAfterMismatch, Option.getOrThrow(retained));

      assert.isFalse(
        yield* store.renew({
          resourcePath,
          leaseId: "lease-old-incarnation",
          ownerThreadId: ThreadId.make("thread-foreign-owner"),
          ownerIncarnation: "event-old-incarnation",
          nowMs: 10_002,
          expiresAtMs: 11_002,
        }),
      );
      yield* store.release({
        resourcePath,
        leaseId: "lease-old-incarnation",
        ownerThreadId: ThreadId.make("thread-foreign-owner"),
        ownerIncarnation: "event-old-incarnation",
      });
      const retainedAfterForeignRelease = (yield* store.listAll()).find(
        (lease) => lease.resourcePath === resourcePath,
      );
      assert.isDefined(retainedAfterForeignRelease);
      assert.deepEqual(retainedAfterForeignRelease, Option.getOrThrow(retained));
    }),
  );
});
