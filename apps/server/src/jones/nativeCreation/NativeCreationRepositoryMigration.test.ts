import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import {
  AuthSessionId,
  EventId,
  NativeCreationHistoricalBinding,
  ProviderRuntimeBinding,
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

const decodePreparationBinding = Schema.decodeEffect(NativePreparationBinding);
const decodeHistoricalBinding = Schema.decodeEffect(NativeCreationHistoricalBinding);
const decodeCommand = Schema.decodeEffect(OrchestrationV2Command);
const decodeExecutionReference = Schema.decodeEffect(NativeCreationExecutionReferenceV2);
const decodeWholeOperationEvidence = Schema.decodeEffect(NativeCreationWholeOperationEvidence);

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
        { migration_id: 138, name: "WorkspaceRetirement" },
        { migration_id: 139, name: "ConversationWorkspace" },
        { migration_id: 140, name: "WorkstreamsNative" },
        { migration_id: 141, name: "WorkstreamsSourceEnrollments" },
        { migration_id: 142, name: "WorkstreamsRollupAttempts" },
        { migration_id: 143, name: "WorkstreamsPeerSessionPins" },
        { migration_id: 144, name: "WorkstreamsMemberBindings" },
        { migration_id: 145, name: "WorkstreamsForwardReceipts" },
        { migration_id: 146, name: "SettlementOriginRetention" },
        { migration_id: 147, name: "NativeExecutionProcessCustody" },
        { migration_id: 148, name: "OrdinaryCheckoutProcessCustody" },
        { migration_id: 149, name: "AuthSessionClientMetadata" },
        { migration_id: 150, name: "WorkspaceRuntimeUses" },
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

const compiledMainSchema11 = Schema.decodeEffect(ProviderRuntimeBinding);

const nativeUpgradeFixture = Effect.gen(function* () {
  const now = "2026-11-05T12:00:00.000Z";
  yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(now)));
  const sql = yield* SqlClient.SqlClient;
  const repository = yield* Repository.NativeCreationRepository;
  const outbox = yield* Outbox.EffectOutboxV2;
  const binding = yield* decodePreparationBinding({
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
  const historical = yield* decodeHistoricalBinding({
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
  const command = yield* decodeCommand({
    type: "prepared-run.release",
    commandId: `${prepared.commandId}:native:v2:message`,
    threadId: prepared.threadId,
    runId: "lease-upgrade-run",
  });
  if (command.type !== "prepared-run.release")
    return yield* Effect.die("Unexpected synthetic release type");
  const beforeUpgrade = yield* sql`SELECT * FROM native_creation_reserved_command_identities`;
  const unavailable = yield* repository.reserveExecutionCommandIdentities!(claimId, [
    prepared.commandId,
    command.commandId,
  ]).pipe(Effect.flip);
  assert.strictEqual(unavailable.code, "unresolved_claim");
  assert.deepStrictEqual(
    yield* sql`SELECT * FROM native_creation_reserved_command_identities`,
    beforeUpgrade,
  );
  const legacyBefore = yield* sql`SELECT * FROM jones_native_creation_execution_starts`;
  // Every Jones migration after the released 136, in ledger order.
  assert.deepStrictEqual(
    yield* runJonesMigrations(jonesMigrationEntries),
    jonesMigrationEntries.filter(([id]) => id >= 137).map(([id, name]) => [id, name]),
  );
  assert.deepStrictEqual(
    yield* sql`SELECT * FROM jones_native_creation_execution_starts`,
    legacyBefore,
  );
  yield* repository.reserveExecutionCommandIdentities!(claimId, [
    prepared.commandId,
    command.commandId,
  ]);
  const normalized = yield* decodeCommand({
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
  const reference = yield* decodeExecutionReference({
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
  const claim = Option.getOrThrow(yield* outbox.claimNext({ workerId: "lease-worker" }));
  const expectedClaim: Repository.NativeExecutionProcessClaim = {
    workerId: "lease-worker",
    expectedAttempt: claim.attemptCount,
    claimedAt: claim.updatedAt,
    commandId: command.commandId,
    threadId: command.threadId,
    runId: command.runId,
    requestDigest: nativeCreationSha256(nativeCreationCanonicalJson(claim.request)),
  };
  const start = yield* repository.startEffectV2!(
    reference,
    now,
    Effect.succeed(historical),
    expectedClaim,
  );
  assert.strictEqual(start.status, "started");
  const evidence = yield* decodeWholeOperationEvidence({
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
  yield* repository.recordExecutionBinding!({
    reference,
    expectedClaim,
    binding: yield* compiledMainSchema11({
      threadId: command.threadId,
      providerThreadId: "lease-provider-thread",
      providerSessionId: "lease-provider-session",
      providerInstanceId: historical.providerModelSelection.instanceId,
      driver: "codex",
      nativeThreadId: "lease-native-thread",
      runtimeGeneration: "lease-generation",
    }),
  });
  const readLeases = Effect.gen(function* () {
    return {
      outbox: yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`,
      native:
        yield* sql`SELECT rowid AS retained_rowid, * FROM jones_native_creation_execution_process_claims ORDER BY effect_id`,
    };
  });
  return { sql, repository, outbox, reference, claim, evidence, readLeases };
});

it.effect(
  "released136 upgrades without adopting legacy starts and current process custody remains immutable until once-only completion",
  () =>
    Effect.gen(function* () {
      const f = yield* nativeUpgradeFixture;
      const before = yield* f.readLeases;
      yield* TestClock.adjust("10 seconds");
      assert.deepStrictEqual(yield* f.readLeases, before);
      assert.deepStrictEqual(yield* runJonesMigrations(jonesMigrationEntries), []);
      assert.deepStrictEqual(yield* f.readLeases, before);
      const confirmation = {
        reference: f.reference,
        workerId: "lease-worker",
        expectedAttempt: 1,
        claimedAt: f.claim.updatedAt,
        evidence: f.evidence,
      };
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            f.repository.confirmExecution!({ ...confirmation, claimedAt: "stale-generation" }),
          ),
        ),
      );
      yield* f.repository.confirmExecution!(confirmation);
      const proof =
        yield* f.sql`SELECT rowid AS retained_rowid,* FROM jones_native_creation_execution_process_confirmations`;
      yield* TestClock.adjust("10 seconds");
      assert.deepStrictEqual((yield* f.readLeases).native, before.native);
      assert.deepStrictEqual(
        yield* f.sql`SELECT rowid AS retained_rowid,* FROM jones_native_creation_execution_process_confirmations`,
        proof,
      );
      yield* TestClock.adjust("15 seconds");
      const live = yield* f.readLeases;
      assert.isFalse(
        yield* f.outbox.succeed({
          effectId: f.claim.id,
          workerId: "lease-worker",
          claim: { ...f.claim, updatedAt: "stale-generation" },
        }),
      );
      assert.deepStrictEqual(yield* f.readLeases, live);
      assert.isTrue(
        yield* f.outbox.succeed({ effectId: f.claim.id, workerId: "lease-worker", claim: f.claim }),
      );
      assert.isTrue(
        Option.exists(
          yield* f.outbox.get(f.claim.id),
          (row) => row.status === "succeeded" && row.attemptCount === 1,
        ),
      );
      const settled = yield* f.readLeases;
      assert.isFalse(
        yield* f.outbox.succeed({ effectId: f.claim.id, workerId: "lease-worker", claim: f.claim }),
      );
      assert.deepStrictEqual(
        yield* f.sql`SELECT * FROM jones_native_creation_execution_starts`,
        [],
      );
      assert.deepStrictEqual(yield* f.sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* f.readLeases, settled);
    }).pipe(Effect.provide(Outbox.layer.pipe(Layer.provideMerge(upgradeRepository)))),
);
