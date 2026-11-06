import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import {
  AuthSessionId,
  EventId,
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";
import { migrationManifest, runMigrations } from "../../persistence/Migrations.ts";
import { runJonesMigrations } from "../persistence/JonesMigrationGuard.ts";
import { jonesMigrationEntries } from "../persistence/JonesMigrations.ts";
import migrate from "../persistence/Migrations/003_JonesNativeCreationIntents.ts";
import {
  NativeCreationExecutionReferenceV2,
  NativeCreationWholeOperationEvidence,
} from "./NativeCreationExecutionTypes.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as RepositorySqlite from "./NativeCreationRepositorySqlite.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
it.effect("records additive fork migration 3 without changing upstream migration identity", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    assert.deepEqual(yield* runMigrations(), migrationManifest);
    assert.deepEqual(yield* runMigrations(), []);
    assert.deepEqual(
      yield* sql`SELECT migration_id, name FROM jones_sql_migrations ORDER BY migration_id`,
      [
        { migration_id: 1, name: "WorktreeOwnershipLeases" },
        { migration_id: 2, name: "ProjectionThreadRuntimeIdentity" },
        { migration_id: 3, name: "NativeCreationIntents" },
        { migration_id: 4, name: "NativeCreationCommandIdentities" },
        { migration_id: 5, name: "WorkstreamsNativeAttempts" },
        { migration_id: 6, name: "WorkstreamsProviderEnrollments" },
        { migration_id: 100, name: "NativeCreationExecution" },
        { migration_id: 101, name: "NativeWorkspacePreparation" },
        { migration_id: 102, name: "ImportedHistoryChoices" },
        { migration_id: 103, name: "RuntimeStop" },
        { migration_id: 104, name: "DeletionAdmission" },
        { migration_id: 105, name: "UpstreamReferenceIsolation" },
        { migration_id: 106, name: "PlannedUpdateContinuity" },
        { migration_id: 107, name: "WorkstreamsSettlementProofs" },
        { migration_id: 108, name: "PhysicalProcessEvidence" },
        { migration_id: 109, name: "OrdinaryCheckoutLifecycle" },
        { migration_id: 110, name: "ThreadLookupIndexes" },
        { migration_id: 111, name: "ClientSettlementCommands" },
        { migration_id: 112, name: "ClientLaunchRequests" },
        { migration_id: 114, name: "SelfSettlementProjectItems" },
        { migration_id: 115, name: "ScheduledWebhookPendingRuns" },
        { migration_id: 116, name: "RemoteAdmission" },
        { migration_id: 117, name: "RemoteDelegationIntents" },
        { migration_id: 118, name: "RemoteAdmissionIntegration" },
        { migration_id: 119, name: "PeerEnvironmentLinks" },
        { migration_id: 120, name: "SettlementRestores" },
        { migration_id: 121, name: "RemoteTaskObservations" },
        { migration_id: 122, name: "ThreadLinks" },
        { migration_id: 123, name: "WorkspaceLifecycle" },
        { migration_id: 124, name: "TaskContinuations" },
        { migration_id: 129, name: "CodexFailover" },
        { migration_id: 130, name: "ThreadTitleRefinement" },
        { migration_id: 132, name: "OwnerSelections" },
        { migration_id: 133, name: "ThreadResultDependencies" },
        { migration_id: 134, name: "GoalContinuity" },
        { migration_id: 135, name: "IdleWorktreeAdmission" },
        { migration_id: 136, name: "WorkspaceProvenance" },
        { migration_id: 137, name: "NativeCreationLeaseRenewal" },
        { migration_id: 139, name: "ConversationWorkspace" },
      ],
    );
    assert.deepEqual(
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
      migrationManifest.map(([migration_id, name]) => ({ migration_id, name })),
    );
    const tables = yield* sql<{
      name: string;
    }>`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'native_creation_%' ORDER BY name`;
    assert.deepEqual(
      tables.map((row) => row.name),
      [
        "native_creation_automation_enrollments",
        "native_creation_effect_facts",
        "native_creation_intents",
        "native_creation_normalized_commands",
        "native_creation_reserved_command_identities",
        "native_creation_reserved_commands",
      ],
    );
  }).pipe(Effect.provide(memory)),
);

const released136Database = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* runMigrations({ toMigrationInclusive: migrationManifest.at(-1)![0] });
    yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id <= 136));
  }),
).pipe(Layer.provideMerge(memory));
const upgradeRepository = RepositorySqlite.layer.pipe(Layer.provideMerge(released136Database));

const nativeUpgradeFixture = Effect.gen(function* () {
  const now = "2026-11-05T12:00:00.000Z";
  yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(now)));
  const sql = yield* SqlClient.SqlClient;
  const repository = yield* Repository.NativeCreationRepository;
  const outbox = yield* Outbox.EffectOutboxV2;
  const binding = yield* Schema.decodeEffect(NativePreparationBinding)({
    backend_instance: "lease-upgrade-backend",
    environment_id: "lease-upgrade-environment",
    project_id: "lease-upgrade-project",
    project_cwd: "/synthetic/project",
    account_ref: "lease-upgrade-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
  });
  const prepared = nativePreparationCommand(
    "lease-upgrade",
    binding,
    "Synthetic prompt",
    "Synthetic title",
    now,
  );
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: "lease-upgrade",
        binding,
        command: prepared,
        preparation_id: prepared.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
        prompt_digest: nativeCreationSha256(prepared.message.text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(prepared)),
      }),
    ),
  );
  const historical = yield* Schema.decodeEffect(NativeCreationHistoricalBinding)({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "lease-upgrade-account-binding",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: prepared.bootstrap.prepareWorktree.branch,
  });
  const claimId = "lease-upgrade-claim";
  yield* repository.claim(
    {
      preparation,
      resources: {
        projectCwd: binding.project_cwd,
        branch: historical.requestedBranch,
        worktreePath: "/synthetic/worktree",
      },
      claimId,
      claimedBootId: "lease-upgrade-boot",
      claimedAt: now,
      actorSessionId: AuthSessionId.make("lease-upgrade-actor"),
      grantId: "lease-upgrade-grant",
      grantRevision: 1,
    },
    Effect.succeed(historical),
  );
  const command = yield* Schema.decodeEffect(OrchestrationV2Command)({
    type: "prepared-run.release",
    commandId: `${prepared.commandId}:native:v2:message`,
    threadId: prepared.threadId,
    runId: "lease-upgrade-run",
  });
  if (command.type !== "prepared-run.release")
    return yield* Effect.die("Unexpected synthetic release type");
  yield* repository.reserveExecutionCommandIdentities!(claimId, [
    prepared.commandId,
    command.commandId,
  ]);
  const normalized = yield* Schema.decodeEffect(OrchestrationV2Command)({
    type: "message.dispatch",
    createdBy: "user",
    creationSource: "server",
    commandId: prepared.commandId,
    threadId: prepared.threadId,
    messageId: prepared.message.messageId,
    text: prepared.message.text,
    attachments: [],
    modelSelection: binding.provider_model_selection,
    dispatchMode: { type: "start_immediately" },
  });
  yield* repository.recordNormalizedCommand(claimId, normalized);
  yield* repository.reserveCommand(claimId, command);
  const reference = yield* Schema.decodeEffect(NativeCreationExecutionReferenceV2)({
    version: 2,
    claimId,
    stageCommandId: command.commandId,
    effectId: `effect:${command.commandId}:provider-turn.start:${command.runId}`,
    stage: "native_command",
  });
  yield* sql.withTransaction(
    Effect.gen(function* () {
      const eventId = EventId.make("lease-upgrade-release-event");
      const events = yield* sql<{ readonly sequence: number }>`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id,
       actor_kind, payload_json, metadata_json, application_event_version)
      VALUES (${eventId}, 'thread', ${command.threadId}, 1, 'run.updated', ${now}, ${command.commandId},
        'system', '{}', '{}', 2) RETURNING sequence`;
      const sequence = events[0]!.sequence;
      yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status)
      VALUES (${command.commandId}, 'thread', ${command.threadId}, ${command.type}, ${now}, ${sequence}, 'accepted')`;
      yield* repository.recordExecutionAcceptance!({ claimId, command, eventId, sequence });
    }),
  );
  yield* sql`INSERT INTO orchestration_v2_projection_threads
    (thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
     active_provider_thread_id, created_at, updated_at, payload_json)
    VALUES (${command.threadId}, 'lease-upgrade-project', 'Synthetic', 'codex', 'full-access', 'default',
      'lease-provider-thread', ${now}, ${now}, '{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_runs
    (run_id, thread_id, ordinal, provider, provider_thread_id, status, requested_at, payload_json)
    VALUES (${command.runId}, ${command.threadId}, 1, 'codex', 'lease-provider-thread', 'running', ${now}, '{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_run_attempts
    (attempt_id, thread_id, run_id, attempt_ordinal, root_node_id, provider, provider_thread_id, status, payload_json)
    VALUES ('lease-attempt', ${command.threadId}, ${command.runId}, 1, 'lease-root', 'codex', 'lease-provider-thread', 'running', '{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_provider_threads
    (provider_thread_id, thread_id, provider, provider_session_id, status, updated_at, payload_json)
    VALUES ('lease-provider-thread', ${command.threadId}, 'codex', 'lease-provider-session', 'active', ${now},
      '{"runtimeIdentity":{"runtimeGeneration":"lease-generation"}}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings
    (provider_session_id, thread_id) VALUES ('lease-provider-session', ${command.threadId})`;
  yield* outbox.enqueue([
    {
      id: reference.effectId,
      commandId: command.commandId,
      threadId: command.threadId,
      request: { type: "provider-turn.start", runId: command.runId },
      nativeCreationExecutionReference: reference,
    },
  ]);
  const claim = Option.getOrThrow(
    yield* outbox.claimNext({ workerId: "lease-worker", leaseDurationMs: 30_000 }),
  );
  const start = yield* repository.startEffectV2!(reference, now, Effect.succeed(historical));
  assert.strictEqual(start.status, "started");
  const evidence = yield* Schema.decodeEffect(NativeCreationWholeOperationEvidence)({
    version: 1,
    outcome: "confirmed_success",
    effectId: reference.effectId,
    threadId: command.threadId,
    commandId: command.commandId,
    providerSessionId: "lease-provider-session",
    providerThreadId: "lease-provider-thread",
    runtimeGeneration: "lease-generation",
    runId: command.runId,
    attemptId: "lease-attempt",
    coverage: "whole_operation",
  });
  const readLeases = Effect.gen(function* () {
    return {
      outbox: yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
      native:
        yield* sql`SELECT rowid AS retained_rowid, *, typeof(lease_expires_at) AS expiry_storage
        FROM jones_native_creation_execution_starts ORDER BY effect_id`,
    };
  });
  return { sql, repository, outbox, reference, claim, evidence, readLeases };
});

it.effect(
  "real outbox renewal rolls back before 137, then confirms and settles the same native start",
  () =>
    Effect.gen(function* () {
      const f = yield* nativeUpgradeFixture;
      const before = yield* f.readLeases;
      yield* TestClock.adjust("10 seconds");
      const failed = yield* Effect.exit(
        f.outbox.renew({ claim: f.claim, leaseDurationMs: 30_000 }),
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.deepStrictEqual(yield* f.readLeases, before);
      assert.deepStrictEqual(
        yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id <= 137)),
        [[137, "NativeCreationLeaseRenewal"]],
      );
      assert.deepStrictEqual(yield* f.readLeases, before);
      const expiry = yield* f.outbox.renew({ claim: f.claim, leaseDurationMs: 30_000 });
      assert.strictEqual(expiry, "2026-11-05T12:00:40.000Z");
      const confirmation = {
        reference: f.reference,
        workerId: "lease-worker",
        expectedAttempt: 1,
        leaseExpiresAt: expiry,
        evidence: f.evidence,
      };
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            f.repository.confirmExecution!({
              ...confirmation,
              leaseExpiresAt: f.claim.leaseExpiresAt!,
            }),
          ),
        ),
      );
      yield* f.repository.confirmExecution!(confirmation);
      const proof =
        yield* f.sql`SELECT rowid AS retained_rowid, * FROM jones_native_creation_execution_confirmations`;
      yield* TestClock.adjust("10 seconds");
      const next = yield* f.outbox.renew({
        claim: { ...f.claim, leaseExpiresAt: expiry },
        leaseDurationMs: 30_000,
      });
      assert.strictEqual(next, "2026-11-05T12:00:50.000Z");
      assert.deepStrictEqual(
        yield* f.sql`SELECT rowid AS retained_rowid, *, typeof(lease_expires_at) AS expiry_storage
      FROM jones_native_creation_execution_starts ORDER BY effect_id`,
        before.native.map((row) => ({ ...row, lease_expires_at: next })),
      );
      assert.deepStrictEqual(
        yield* f.sql`SELECT rowid AS retained_rowid, * FROM jones_native_creation_execution_confirmations`,
        proof,
      );
      const current = { ...f.claim, leaseExpiresAt: next };
      yield* TestClock.adjust("15 seconds");
      assert.strictEqual(f.claim.leaseExpiresAt, "2026-11-05T12:00:30.000Z");
      const live = yield* f.readLeases;
      assert.isFalse(
        yield* f.outbox.succeed({ effectId: f.claim.id, workerId: "lease-worker", claim: f.claim }),
      );
      assert.deepStrictEqual(yield* f.readLeases, live);
      assert.isTrue(
        yield* f.outbox.succeed({ effectId: f.claim.id, workerId: "lease-worker", claim: current }),
      );
      assert.isTrue(
        Option.exists(
          yield* f.outbox.get(f.claim.id),
          (row) => row.status === "succeeded" && row.attemptCount === 1,
        ),
      );
      const settled = yield* f.readLeases;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(f.outbox.renew({ claim: current, leaseDurationMs: 30_000 })),
        ),
      );
      assert.deepStrictEqual(yield* f.readLeases, settled);
      assert.deepStrictEqual(
        yield* runJonesMigrations(jonesMigrationEntries.filter(([id]) => id <= 137)),
        [],
      );
    }).pipe(Effect.provide(Outbox.layer.pipe(Layer.provideMerge(upgradeRepository)))),
);

it.effect(
  "native session enrollment markers survive attempted update, deletion and replacement",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migrate;
      yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at)
    VALUES ('fixture-native-session', '2026-10-02T12:00:00Z')`;
      for (const mutation of [
        sql`UPDATE native_creation_automation_enrollments SET enrolled_at = '2099-01-01T00:00:00Z' WHERE session_id = 'fixture-native-session'`,
        sql`UPDATE native_creation_automation_enrollments SET session_id = 'other-session' WHERE session_id = 'fixture-native-session'`,
        sql`DELETE FROM native_creation_automation_enrollments WHERE session_id = 'fixture-native-session'`,
        sql`INSERT OR REPLACE INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES ('fixture-native-session', '2099-01-01T00:00:00Z')`,
      ]) {
        assert.isTrue((yield* mutation.pipe(Effect.result))._tag === "Failure");
      }
      assert.deepEqual(
        yield* sql`SELECT session_id, enrolled_at FROM native_creation_automation_enrollments`,
        [{ session_id: "fixture-native-session", enrolled_at: "2026-10-02T12:00:00Z" }],
      );
    }).pipe(Effect.provide(memory)),
);

it.effect("bounded upstream replay leaves creation tables absent and migration is idempotent", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 54 });
    assert.deepEqual(
      yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'native_creation_%'`,
      [],
    );
    yield* migrate;
    yield* migrate;
    yield* sql`INSERT INTO native_creation_intents
    (claim_id, operation_id, preparation_id, command_id, thread_id, message_id, project_cwd, branch, worktree_path, canonical_preparation, intent_json)
    VALUES ('claim', 'operation', 'preparation', 'command', 'thread', 'message', '/fixture/project', 'fixture-branch', '/fixture/path', '{}', '{}')`;
    for (const mutation of [
      sql`UPDATE native_creation_intents SET intent_json = 'changed' WHERE claim_id = 'claim'`,
      sql`DELETE FROM native_creation_intents WHERE claim_id = 'claim'`,
    ]) {
      assert.isTrue((yield* mutation.pipe(Effect.result))._tag === "Failure");
    }
    assert.deepEqual(yield* sql`SELECT claim_id, intent_json FROM native_creation_intents`, [
      { claim_id: "claim", intent_json: "{}" },
    ]);
  }).pipe(Effect.provide(memory)),
);

it.effect(
  "all resource identity constraints reject collisions and effect facts cannot replace history",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migrate;
      const insert = (id: string, column?: string) =>
        sql`INSERT INTO native_creation_intents ${sql.insert({
          claim_id: id,
          operation_id: id,
          preparation_id: id,
          command_id: id,
          thread_id: id,
          message_id: id,
          project_cwd: "/fixture/project",
          branch: id,
          worktree_path: `/fixture/${id}`,
          canonical_preparation: "{}",
          intent_json: "{}",
          ...(column === undefined
            ? {}
            : { [column]: column === "worktree_path" ? "/fixture/original" : "original" }),
        })}`;
      yield* insert("original");
      for (const column of [
        "claim_id",
        "operation_id",
        "preparation_id",
        "command_id",
        "thread_id",
        "message_id",
        "branch",
        "worktree_path",
      ]) {
        assert.isTrue(
          (yield* insert(`other-${column}`, column).pipe(Effect.result))._tag === "Failure",
        );
      }
      yield* sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json) VALUES ('original', 'effect', 'started', 0, '{}')`;
      for (const mutation of [
        sql`UPDATE native_creation_effect_facts SET fact_json = 'changed'`,
        sql`DELETE FROM native_creation_effect_facts`,
        sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json) VALUES ('original', 'effect', 'started', 1, 'changed')`,
        sql`INSERT OR REPLACE INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json) VALUES ('original', 'effect', 'started', 0, 'changed')`,
      ]) {
        assert.isTrue((yield* mutation.pipe(Effect.result))._tag === "Failure");
      }
      assert.deepEqual(yield* sql`SELECT ordinal, fact_json FROM native_creation_effect_facts`, [
        { ordinal: 0, fact_json: "{}" },
      ]);
    }).pipe(Effect.provide(memory)),
);
