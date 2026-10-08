import * as NodePath from "@effect/platform-node/NodePath";
import { assert, it } from "@effect/vitest";
import { CommandId, ProjectId, ThreadId, type WorktreeCleanupRules } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import { makeDeletionAdmission } from "./DeletionAdmission.ts";
import type { DeletionWorktreeRemovalObservationV1 } from "./DeletionWorktreeRemoval.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const testLayer = Receipts.layer.pipe(
  Layer.provideMerge(memory),
  Layer.provideMerge(NodePath.layer),
);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const rules: WorktreeCleanupRules = {
  worktreeAfterDays: null,
  worktreeOnDelete: true,
  worktreeOnMerge: false,
  worktreeUnchanged: false,
};
const threadId = ThreadId.make("thread:deletion-original");
const projectId = ProjectId.make("project:deletion-original");
const commandId = CommandId.make("command:deletion-original");
const timestamp = "2026-10-07T00:00:00.000Z";
const target = {
  projectId,
  projectRoot: "/synthetic/project",
  path: "/synthetic/worktrees/original",
  branch: "original",
  force: false,
};
const input = {
  threadId,
  target,
  rules,
  currentRules: Effect.succeed(rules),
  currentLive: Effect.succeed(true),
};
const setup = Effect.gen(function* () {
  yield* runMigrations();
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const payload = {
    id: threadId,
    projectId,
    createdAt: timestamp,
    deletedAt: timestamp,
    worktreePath: target.path,
    branch: target.branch,
  };
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES(${projectId},'synthetic',${target.projectRoot},'[]',${timestamp},${timestamp})`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,deleted_at,payload_json) VALUES(${threadId},${projectId},'synthetic','codex','local','default',${timestamp},${timestamp},${timestamp},${encodeJson(payload)})`;
  for (const [sequence, eventId, eventType, body] of [
    [1, "event:original-birth", "thread.created", { ...payload, deletedAt: null }],
    [2, "event:original-deletion", "thread.deleted", payload],
  ] as const) {
    yield* sql`INSERT INTO orchestration_events(sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES(${sequence},${eventId},'thread',${threadId},${sequence},${eventType},${timestamp},${eventType === "thread.deleted" ? commandId : null},'client',${encodeJson(body)},'{}',2)`;
  }
  yield* receipts.upsert({
    commandId,
    threadId,
    commandType: "thread.delete",
    acceptedAt: DateTime.makeUnsafe(timestamp),
    resultSequence: 2,
    status: "accepted",
    error: null,
  });
  const owner = yield* makeDeletionAdmission;
  return { sql, receipts, owner, payload };
});

it.effect(
  "records the original task and unknown-start fence before returning one execution permit",
  () =>
    Effect.gen(function* () {
      const { owner, sql } = yield* setup;
      const admitted = yield* owner.start(input);
      assert.strictEqual(admitted.status, "start_now");
      assert.deepStrictEqual(
        yield* sql`SELECT state,worktree_path FROM jones_deletion_worktree_admissions`,
        [{ state: "started", worktree_path: target.path }],
      );
      yield* owner.revalidate(
        admitted.start,
        admitted.ordinal,
        input.currentRules,
        input.currentLive,
      );
      const repeated = yield* owner
        .revalidate(admitted.start, admitted.ordinal, input.currentRules, input.currentLive)
        .pipe(Effect.flip);
      assert.strictEqual(repeated.reason, "original_start_observation_only");
      assert.strictEqual((yield* owner.start(input)).status, "observe_only");
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "process recovery reads the original start and refuses execution even before an observation exists",
  () =>
    Effect.gen(function* () {
      const { owner } = yield* setup;
      const first = yield* owner.start(input);
      const recoveredOwner = yield* makeDeletionAdmission;
      const recovered = yield* recoveredOwner.read(first.start.effectId);
      assert.deepStrictEqual(recovered, { start: first.start, ordinal: 0 });
      const denial = yield* recoveredOwner
        .revalidate(first.start, 0, input.currentRules, input.currentLive)
        .pipe(Effect.flip);
      assert.strictEqual(denial.reason, "original_start_observation_only");
    }).pipe(Effect.provide(testLayer)),
);

it.effect("policy changes and new live activity cannot consume the original execution permit", () =>
  Effect.gen(function* () {
    const { owner } = yield* setup;
    const first = yield* owner.start(input);
    const changed = yield* owner
      .revalidate(
        first.start,
        0,
        Effect.succeed({ ...rules, worktreeOnMerge: true }),
        input.currentLive,
      )
      .pipe(Effect.flip);
    assert.strictEqual(changed.reason, "original_worktree_policy_changed");
    const live = yield* owner
      .revalidate(first.start, 0, input.currentRules, Effect.succeed(false))
      .pipe(Effect.flip);
    assert.strictEqual(live.reason, "current_cleanup_owner_or_live_prerequisites_changed");
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "permanent workspace101 admission and native claim require explicit original-owner handoff",
  () =>
    Effect.gen(function* () {
      const { owner, sql } = yield* setup;
      yield* sql`INSERT INTO native_creation_intents(claim_id,operation_id,preparation_id,command_id,thread_id,message_id,project_cwd,branch,worktree_path,canonical_preparation,intent_json) VALUES('claim:original','op:original','prep:original','cmd:native','thread:native','message:native',${target.projectRoot},${target.branch},${target.path},'{}','{}')`;
      const claim = yield* owner.start(input).pipe(Effect.flip);
      assert.strictEqual(claim.reason, "native_creation_original_owner_handoff_required");
      yield* sql`INSERT INTO jones_native_workspace_admissions(claim_id,worktree_path,basis_json) VALUES('claim:original',${target.path},'{}')`;
      const admission = yield* owner.start(input).pipe(Effect.flip);
      assert.strictEqual(admission.reason, "native_workspace_original_owner_handoff_required");
      assert.deepStrictEqual(yield* sql`SELECT * FROM jones_deletion_worktree_admissions`, []);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("expired foreign path lease does not authorize deletion", () =>
  Effect.gen(function* () {
    const { owner, sql } = yield* setup;
    yield* sql`INSERT INTO worktree_ownership_leases(resource_path,lease_id,owner_thread_id,owner_incarnation,acquired_at_ms,renewed_at_ms,expires_at_ms) VALUES(${target.path},'lease:original','thread:other','incarnation:original',0,0,1)`;
    const denial = yield* owner.start(input).pipe(Effect.flip);
    assert.strictEqual(denial.reason, "worktree_original_lease_owner_unavailable");
  }).pipe(Effect.provide(testLayer)),
);

it.effect("unsettled cleanup and mismatched deletion receipt refuse start", () =>
  Effect.gen(function* () {
    const { owner, sql, receipts } = yield* setup;
    yield* receipts.upsert({
      commandId,
      threadId,
      commandType: "thread.delete",
      acceptedAt: DateTime.makeUnsafe(timestamp),
      resultSequence: 2,
      status: "rejected",
      error: "synthetic",
    });
    assert.strictEqual(
      (yield* owner.start(input).pipe(Effect.flip)).reason,
      "original_deletion_receipt_unavailable",
    );
    yield* receipts.upsert({
      commandId,
      threadId,
      commandType: "thread.delete",
      acceptedAt: DateTime.makeUnsafe(timestamp),
      resultSequence: 2,
      status: "accepted",
      error: null,
    });
    yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,created_at,updated_at) VALUES('effect:cleanup',${commandId},${threadId},'terminal.cleanup','{"type":"terminal.cleanup"}','pending',0,${timestamp},${timestamp},${timestamp})`;
    assert.strictEqual(
      (yield* owner.start(input).pipe(Effect.flip)).reason,
      "original_cleanup_prerequisites_unsettled",
    );
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "unknown observation remains retained and recovery can prove absence without another invocation",
  () =>
    Effect.gen(function* () {
      const { owner, sql } = yield* setup;
      const first = yield* owner.start(input);
      yield* owner.revalidate(first.start, 0, input.currentRules, input.currentLive);
      const before: DeletionWorktreeRemovalObservationV1["before"] = {
        registration: {
          status: "complete",
          projectRoot: target.projectRoot,
          gitCommonDirectory: "/synthetic/common",
          entries: [
            { path: target.path, head: "a".repeat(40), branch: "refs/heads/original", bare: false },
          ],
        },
        filesystem: { status: "present", path: target.path },
      };
      const observedAt = DateTime.formatIso(yield* DateTime.now);
      const unknown: DeletionWorktreeRemovalObservationV1 = {
        version: 1,
        start: first.start,
        startOrdinal: 0,
        operation: { kind: "executed", completion: "unknown", exitCode: null },
        before,
        after: before,
        observedAt,
      };
      yield* owner.qualify(unknown);
      assert.deepStrictEqual(yield* sql`SELECT state FROM jones_deletion_worktree_admissions`, [
        { state: "unknown" },
      ]);
      const recovered = yield* makeDeletionAdmission;
      const absence = {
        registration: { ...before.registration, entries: [] },
        filesystem: { status: "absent" as const, path: target.path },
      };
      yield* recovered.qualify({
        ...unknown,
        operation: { kind: "reconciled", completion: "unknown" },
        after: absence,
      });
      assert.deepStrictEqual(yield* sql`SELECT state FROM jones_deletion_worktree_admissions`, [
        { state: "completed" },
      ]);
      assert.deepStrictEqual(
        yield* sql`SELECT result FROM jones_deletion_worktree_observations ORDER BY ordinal`,
        [{ result: "unknown" }, { result: "absent" }],
      );
      assert.strictEqual((yield* recovered.start(input)).status, "observe_only");
    }).pipe(Effect.provide(testLayer)),
);

it.effect("observation cannot substitute another original target or Git common directory", () =>
  Effect.gen(function* () {
    const { owner, sql } = yield* setup;
    const first = yield* owner.start(input);
    const registration = {
      status: "complete" as const,
      projectRoot: target.projectRoot,
      gitCommonDirectory: "/synthetic/one",
      entries: [],
    };
    const readback: DeletionWorktreeRemovalObservationV1["before"] = {
      registration,
      filesystem: { status: "absent", path: target.path },
    };
    const observed: DeletionWorktreeRemovalObservationV1 = {
      version: 1,
      start: { ...first.start, target: { ...target, path: "/synthetic/another" } },
      startOrdinal: 0,
      operation: { kind: "reconciled", completion: "unknown" },
      before: readback,
      after: readback,
      observedAt: DateTime.formatIso(yield* DateTime.now),
    };
    assert.isTrue(Exit.isFailure(yield* owner.qualify(observed).pipe(Effect.exit)));
    yield* owner.qualify({
      ...observed,
      start: first.start,
      after: {
        ...readback,
        registration: { ...registration, gitCommonDirectory: "/synthetic/two" },
      },
    });
    assert.deepStrictEqual(yield* sql`SELECT state FROM jones_deletion_worktree_admissions`, [
      { state: "unknown" },
    ]);
  }).pipe(Effect.provide(testLayer)),
);
