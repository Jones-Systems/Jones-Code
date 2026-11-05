import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import { makeNativeExecutionMethods } from "./NativeCreationExecutionSqlite.ts";
import { NativeCreationRepositoryError } from "./NativeCreationRepository.ts";
import { NativeCreationExecutionReferenceV2 } from "./NativeCreationExecutionTypes.ts";
const memory = NodeSqliteClient.layer({ filename: ":memory:" });

const decodeEffectNativeCreationExecutionReferenceV2 = Schema.decodeEffect(
  NativeCreationExecutionReferenceV2,
);

it.effect("foreign ledger history cannot reconstruct execution authority from old claims", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE jones_sql_migrations(migration_id INTEGER PRIMARY KEY,name TEXT NOT NULL)`;
    yield* sql`INSERT INTO jones_sql_migrations VALUES(7,'V2NativeAcceptance'),(100,'NativeCreationExecution')`;
    let reads = 0;
    const owner = {
      readHistory: () =>
        Effect.gen(function* () {
          reads += 1;
          return yield* new NativeCreationRepositoryError({
            code: "unresolved_claim",
            message: "Old claim must not be read",
          });
        }),
      getReservedCommand: () =>
        Effect.fail(
          new NativeCreationRepositoryError({
            code: "unresolved_claim",
            message: "Old reservation must not be read",
          }),
        ),
    };
    const methods = makeNativeExecutionMethods(sql, owner);
    const reference = yield* decodeEffectNativeCreationExecutionReferenceV2({
      version: 2,
      claimId: "old-claim",
      stageCommandId: "old-command",
      effectId: "old-effect",
      stage: "native_command",
    });
    const result = yield* methods.readExecutionReference(reference).pipe(Effect.flip);
    assert.strictEqual(result.code, "unresolved_claim");
    assert.strictEqual(reads, 0);
  }).pipe(Effect.provide(memory)),
);

import {
  AuthSessionId,
  EventId,
  NativeCreationHistoricalBinding,
  ProviderRuntimeBinding,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";

import * as TestClock from "effect/testing/TestClock";
import * as AuthSessions from "../../persistence/AuthSessions.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as Repository from "./NativeCreationRepository.ts";
import * as RepositorySqlite from "./NativeCreationRepositorySqlite.ts";
import * as Authority from "./NativeCreationAuthority.ts";
import {
  NativeCreationProviderExecutor,
  NativeCreationProviderExecutionError,
  executeNativeProviderEffect,
} from "./NativeCreationProviderExecution.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";
import { NativeCreationWholeOperationEvidence } from "./NativeCreationExecutionTypes.ts";
import * as Outbox from "../../orchestration-v2/EffectOutbox.ts";

const now = "2026-10-02T12:34:56Z";
const deadline = "2099-01-01T00:00:00.000Z";
const actor = AuthSessionId.make("sql-fixture-actor");
const database = Layer.effectDiscard(runMigrations().pipe(Effect.map(() => undefined))).pipe(
  Layer.provideMerge(memory),
);
const realRepository = RepositorySqlite.layer.pipe(Layer.provideMerge(database));

const decodeEffectNativePreparationBinding = Schema.decodeEffect(NativePreparationBinding);
const decodeEffectNativeCreationHistoricalBinding = Schema.decodeEffect(
  NativeCreationHistoricalBinding,
);
const decodeEffectOrchestrationV2Command = Schema.decodeEffect(OrchestrationV2Command);
const encodeEffectFromJsonStringOutboxNativeOrchestrationEffectPayloadV2 = Schema.encodeEffect(
  Schema.fromJsonString(Outbox.NativeOrchestrationEffectPayloadV2),
);
const decodeEffectNativeCreationWholeOperationEvidence = Schema.decodeEffect(
  NativeCreationWholeOperationEvidence,
);
const decodeEffectAuthSessionsAuthSessionRecord = Schema.decodeEffect(
  AuthSessions.AuthSessionRecord,
);
const decodeProviderRuntimeBinding = Schema.decodeEffect(ProviderRuntimeBinding);

const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(now)));
  const repository = yield* Repository.NativeCreationRepository;
  const sql = yield* SqlClient.SqlClient;
  const binding = yield* decodeEffectNativePreparationBinding({
    backend_instance: "sql-fixture-backend",
    environment_id: "sql-fixture-environment",
    project_id: "sql-fixture-project",
    project_cwd: "/synthetic/project",
    account_ref: "sql-fixture-account",
    runtime_mode: "full-access",
    interaction_mode: "default",
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "synthetic-model" },
  });
  const prepared = nativePreparationCommand(
    "sql-native-execution",
    binding,
    "Synthetic prompt",
    "Synthetic title",
    now,
  );
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: "sql-native-execution",
        binding,
        command: prepared,
        preparation_id: prepared.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
        prompt_digest: nativeCreationSha256(prepared.message.text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(prepared)),
      }),
    ),
  );
  const historical = yield* decodeEffectNativeCreationHistoricalBinding({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "sql-qualified-account",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: prepared.bootstrap.prepareWorktree.branch,
  });
  const resources = {
    projectCwd: binding.project_cwd,
    branch: historical.requestedBranch,
    worktreePath: "/synthetic/worktree",
  };
  const claimId = "sql-native-claim";
  yield* repository.claim(
    {
      preparation,
      resources,
      claimId,
      claimedBootId: "sql-boot",
      claimedAt: now,
      actorSessionId: actor,
      grantId: "sql-grant",
      grantRevision: 1,
    },
    Effect.succeed(historical),
  );
  yield* sql`INSERT INTO native_creation_automation_enrollments(session_id,enrolled_at) VALUES(${actor},${now})`;
  const command = yield* decodeEffectOrchestrationV2Command({
    type: "prepared-run.release",
    commandId: `${prepared.commandId}:native:v2:message`,
    threadId: prepared.threadId,
    runId: "sql-run",
  });
  if (command.type !== "prepared-run.release")
    return yield* Effect.die(new Error("Synthetic release has wrong type"));
  yield* repository.reserveExecutionCommandIdentities!(claimId, [
    prepared.commandId,
    command.commandId,
  ]);
  const normalized = yield* decodeEffectOrchestrationV2Command({
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
  const reference = yield* decodeEffectNativeCreationExecutionReferenceV2({
    version: 2,
    claimId,
    stageCommandId: command.commandId,
    effectId: `effect:${command.commandId}:provider-turn.start:${command.runId}`,
    stage: "native_command",
  });
  const eventId = EventId.make("sql-release-event");
  const appendReceipt = Effect.gen(function* () {
    const events = yield* sql<{
      sequence: number;
    }>`INSERT INTO orchestration_events(event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES(${eventId},'thread',${command.threadId},1,'run.updated',${now},${command.commandId},'system','{}','{}',2) RETURNING sequence`;
    const sequence = events[0]!.sequence;
    yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type)
      VALUES(${command.commandId},'thread',${command.threadId},${now},${sequence},'accepted',NULL,${command.type})`;
    return sequence;
  });
  const accept = (actual: OrchestrationV2Command = command) =>
    sql.withTransaction(
      Effect.gen(function* () {
        const sequence = yield* appendReceipt;
        yield* repository.recordExecutionAcceptance!({
          claimId,
          command: actual,
          eventId,
          sequence,
        });
        return sequence;
      }),
    );
  const enqueue = Effect.gen(function* () {
    const payload = yield* encodeEffectFromJsonStringOutboxNativeOrchestrationEffectPayloadV2({
      request: { type: "provider-turn.start", runId: command.runId },
      nativeCreationExecutionReference: reference,
    });
    yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,lease_owner,created_at,updated_at)
      VALUES(${reference.effectId},${command.commandId},${command.threadId},'provider-turn.start',${payload},'running',1,${now},'sql-worker',${now},${now})`;
  });
  const seedRuntime = Effect.gen(function* () {
    const providerPayload = nativeCreationCanonicalJson({
      runtimeIdentity: { runtimeGeneration: "sql-generation" },
    });
    yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,active_provider_thread_id,created_at,updated_at,payload_json)
      VALUES(${command.threadId},'sql-fixture-project','Synthetic','codex','full-access','default','sql-provider-thread',${now},${now},'{}')`;
    yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,provider_thread_id,status,requested_at,payload_json)
      VALUES(${command.runId},${command.threadId},1,'codex','sql-provider-thread','running',${now},'{}')`;
    yield* sql`INSERT INTO orchestration_v2_projection_run_attempts(attempt_id,thread_id,run_id,attempt_ordinal,root_node_id,provider,provider_thread_id,status,payload_json)
      VALUES('sql-attempt',${command.threadId},${command.runId},1,'sql-root','codex','sql-provider-thread','running','{}')`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads(provider_thread_id,thread_id,provider,provider_session_id,status,updated_at,payload_json)
      VALUES('sql-provider-thread',${command.threadId},'codex','sql-provider-session','active',${now},${providerPayload})`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings(provider_session_id,thread_id) VALUES('sql-provider-session',${command.threadId})`;
  });
  const evidence = yield* decodeEffectNativeCreationWholeOperationEvidence({
    version: 1,
    outcome: "confirmed_success",
    effectId: reference.effectId,
    threadId: command.threadId,
    commandId: command.commandId,
    providerSessionId: "sql-provider-session",
    providerThreadId: "sql-provider-thread",
    runtimeGeneration: "sql-generation",
    runId: command.runId,
    attemptId: "sql-attempt",
    coverage: "whole_operation",
  });
  const session = yield* decodeEffectAuthSessionsAuthSessionRecord({
    sessionId: actor,
    subject: "Synthetic native actor",
    scopes: ["orchestration:operate"],
    method: "bearer-access-token",
    client: {
      label: null,
      ipAddress: null,
      userAgent: null,
      deviceType: "bot",
      os: null,
      browser: null,
    },
    issuedAt: now,
    expiresAt: deadline,
    revokedAt: null,
    lastConnectedAt: null,
  });
  const grant: Authority.NativeCreationGrant = {
    grantId: "sql-grant",
    revision: 1,
    actorSessionId: actor,
    issuerId: "sql-issuer",
    expiresAt: DateTime.makeUnsafe(deadline),
    revoked: false,
    operationId: preparation.operationId,
    preparationId: preparation.preparationId,
    preparationSha256: preparation.preparationSha256,
    bindingDigest: preparation.bindingDigest,
    binding: historical,
    resources,
    allowedStages: ["native_command", "fetch"],
    recoveryScopes: [],
  };
  const authorityLayer = Authority.NativeCreationAuthorityLive.pipe(
    Layer.provide(Layer.succeed(Repository.NativeCreationRepository, repository)),
    Layer.provide(
      Layer.succeed(
        AuthSessions.AuthSessionRepository,
        AuthSessions.AuthSessionRepository.of({
          getById: () => Effect.succeedSome(session),
          create: () => Effect.void,
          createReplacingActive: () => Effect.succeed([]),
          createIfAbsent: () => Effect.void,
          listActive: () => Effect.succeed([]),
          revoke: () => Effect.succeed(false),
          revokeAllExcept: () => Effect.succeed([]),
          setLastConnectedAt: () => Effect.void,
        }),
      ),
    ),
    Layer.provide(
      Layer.succeed(Authority.NativeCreationGrantResolver, {
        resolveCurrent: () =>
          Effect.succeed({ enrolledSessionId: actor, trustedIssuerId: "sql-issuer", grant }),
      }),
    ),
    Layer.provide(
      Layer.succeed(Authority.NativeCreationBindingResolver, {
        resolveCurrent: () => Effect.succeed(historical),
      }),
    ),
  );
  const issue = Effect.gen(function* () {
    const authority = yield* Authority.NativeCreationAuthority;
    const context = yield* authority.issueExecution!({
      reference,
      timestamp: now,
      expectedClaim: {
        workerId: "sql-worker",
        expectedAttempt: 1,
        claimedAt: now,
        commandId: command.commandId,
        threadId: command.threadId,
        runId: command.runId,
        requestDigest: nativeCreationSha256(
          nativeCreationCanonicalJson({ type: "provider-turn.start", runId: command.runId }),
        ),
      },
    });
    const physical = yield* decodeProviderRuntimeBinding({
      threadId: command.threadId,
      providerThreadId: "sql-provider-thread",
      providerSessionId: "sql-provider-session",
      providerInstanceId: historical.providerModelSelection.instanceId,
      driver: "codex",
      nativeThreadId: "sql-native-thread",
      runtimeGeneration: "sql-generation",
    }).pipe(
      Effect.mapError(
        (cause) =>
          new NativeCreationRepositoryError({ code: "unresolved_claim", message: String(cause) }),
      ),
    );
    yield* Authority.recordNativeCreationExecutionBinding(context, physical);
    return context;
  }).pipe(Effect.provide(authorityLayer));
  const confirmation = {
    reference,
    workerId: "sql-worker",
    expectedAttempt: 1,
    claimedAt: now,
    evidence,
  };
  const effect: Outbox.OrchestrationEffectV2 = {
    id: reference.effectId,
    commandId: command.commandId,
    threadId: command.threadId,
    request: { type: "provider-turn.start", runId: command.runId },
    nativeCreationExecutionReference: reference,
    status: "running",
    attemptCount: 1,
    availableAt: now,
    leaseOwner: "sql-worker",
    createdAt: now,
    updatedAt: now,
    completedAt: null,
    lastError: null,
  };
  return {
    sql,
    repository,
    command,
    reference,
    eventId,
    accept,
    enqueue,
    seedRuntime,
    evidence,
    issue,
    confirmation,
    authorityLayer,
    effect,
  };
});

it.effect(
  "real receiving owner commits acceptance, issues one matching start and confirms only the exact claim",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const wrongCommand = { ...f.command, runId: f.command.runId };
      const mismatched = yield* decodeEffectOrchestrationV2Command({
        ...wrongCommand,
        runId: "other-run",
      });
      yield* f.accept(mismatched).pipe(Effect.flip);
      assert.deepEqual(
        yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id=${f.command.commandId}`,
        [],
      );
      assert.deepEqual(
        yield* f.sql`SELECT event_id FROM orchestration_events WHERE event_id=${f.eventId}`,
        [],
      );
      const sequence = yield* f.accept();
      yield* f.sql.withTransaction(
        f.repository.recordExecutionAcceptance!({
          claimId: f.reference.claimId,
          command: f.command,
          eventId: f.eventId,
          sequence,
        }),
      );
      assert.deepEqual(
        yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_acceptances`,
        [{ count: 1 }],
      );
      yield* f.enqueue;
      yield* f.seedRuntime;
      const context = yield* f.issue;
      assert.deepEqual(Authority.getNativeCreationExecutionReference(context), f.reference);
      assert.strictEqual((yield* f.issue.pipe(Effect.flip)).code, "unresolved_claim");
      assert.strictEqual(
        (yield* f.repository.confirmExecution!({ ...f.confirmation, expectedAttempt: 2 }).pipe(
          Effect.flip,
        )).code,
        "unresolved_claim",
      );
      assert.strictEqual(
        (yield* f.repository.confirmExecution!({
          ...f.confirmation,
          evidence: { ...f.evidence, runtimeGeneration: "other-generation" },
        }).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.deepEqual(
        yield* f.sql`SELECT effect_id FROM jones_native_creation_execution_process_confirmations`,
        [],
      );
      yield* f.repository.confirmExecution!(f.confirmation);
      yield* f.repository.confirmExecution!(f.confirmation);
      assert.deepEqual(
        yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_process_claims`,
        [{ count: 1 }],
      );
      assert.deepEqual(
        yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_process_confirmations`,
        [{ count: 1 }],
      );
      assert.isTrue(
        yield* Effect.gen(function* () {
          const outbox = yield* Outbox.EffectOutboxV2;
          return yield* outbox.succeed({ effectId: f.reference.effectId, workerId: "sql-worker" });
        }).pipe(Effect.provide(Outbox.layer)),
      );
    }).pipe(Effect.provide(realRepository)),
);

it.effect(
  "uncertain started execution retains an immutable hold and cannot rerun or acknowledge",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.accept();
      yield* f.enqueue;
      yield* f.seedRuntime;
      yield* f.issue;
      yield* f.repository.holdExecution!(f.reference, "uncertain native outcome");
      yield* f.repository.holdExecution!(f.reference, "later diagnostic");
      assert.strictEqual((yield* f.issue.pipe(Effect.flip)).code, "unresolved_claim");
      assert.strictEqual(
        (yield* f.repository.confirmExecution!(f.confirmation).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.isFalse(
        yield* Effect.gen(function* () {
          const outbox = yield* Outbox.EffectOutboxV2;
          return yield* outbox.succeed({ effectId: f.reference.effectId, workerId: "sql-worker" });
        }).pipe(Effect.provide(Outbox.layer)),
      );
      assert.deepEqual(
        yield* f.sql`SELECT reason FROM jones_native_creation_execution_process_holds`,
        [{ reason: "uncertain native outcome" }],
      );
    }).pipe(Effect.provide(realRepository)),
);

it.effect("qualified whole-operation port confirms its success through the real owner", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.accept();
    yield* f.enqueue;
    yield* f.seedRuntime;
    let invocations = 0;
    yield* executeNativeProviderEffect(f.effect).pipe(
      Effect.provide(f.authorityLayer),
      Effect.provideService(NativeCreationProviderExecutor, {
        executeWholeOperation: ({ context }) =>
          Effect.gen(function* () {
            invocations += 1;
            assert.deepEqual(Authority.getNativeCreationExecutionReference(context), f.reference);
            yield* Authority.recordNativeCreationExecutionBinding(
              context,
              yield* decodeProviderRuntimeBinding({
                threadId: f.command.threadId,
                providerThreadId: "sql-provider-thread",
                providerSessionId: "sql-provider-session",
                providerInstanceId: "codex",
                driver: "codex",
                nativeThreadId: "sql-native-thread",
                runtimeGeneration: "sql-generation",
              }),
            );
            return f.evidence;
          }).pipe(
            Effect.mapError(
              (cause) => new NativeCreationProviderExecutionError({ message: String(cause) }),
            ),
          ),
      }),
    );
    assert.strictEqual(invocations, 1);
    assert.deepEqual(
      yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_process_confirmations`,
      [{ count: 1 }],
    );
  }).pipe(Effect.provide(realRepository)),
);

it.effect("qualified whole-operation failure retains its durable unknown hold", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.accept();
    yield* f.enqueue;
    yield* f.seedRuntime;
    yield* executeNativeProviderEffect(f.effect).pipe(
      Effect.provide(f.authorityLayer),
      Effect.provideService(NativeCreationProviderExecutor, {
        executeWholeOperation: () =>
          Effect.fail(
            new NativeCreationProviderExecutionError({ message: "Synthetic uncertain failure" }),
          ),
      }),
      Effect.flip,
    );
    assert.deepEqual(
      yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_process_holds`,
      [{ count: 1 }],
    );
    assert.deepEqual(
      yield* f.sql`SELECT effect_id FROM jones_native_creation_execution_process_confirmations`,
      [],
    );
  }).pipe(Effect.provide(realRepository)),
);

it.effect("cancellation after native invocation retains a hold before releasing the fiber", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.accept();
    yield* f.enqueue;
    yield* f.seedRuntime;
    const entered = yield* Deferred.make<void>();
    const blocked = executeNativeProviderEffect(f.effect).pipe(
      Effect.provide(f.authorityLayer),
      Effect.provideService(NativeCreationProviderExecutor, {
        executeWholeOperation: () =>
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      }),
    );
    const fiber = yield* blocked.pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(fiber);
    assert.deepEqual(
      yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_process_holds`,
      [{ count: 1 }],
    );
    assert.deepEqual(
      yield* f.sql`SELECT effect_id FROM jones_native_creation_execution_process_confirmations`,
      [],
    );
  }).pipe(Effect.provide(realRepository)),
);

it.effect(
  "native completion needs a strict envelope and the exact confirmed worker, process claim and attempt",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.accept();
      yield* f.enqueue;
      yield* f.seedRuntime;
      yield* f.issue;
      const outbox = yield* Outbox.EffectOutboxV2;
      const succeed = (workerId = "sql-worker") =>
        outbox.succeed({ effectId: f.reference.effectId, workerId });
      const status = Effect.map(
        f.sql<{
          readonly status: string;
        }>`SELECT status FROM orchestration_v2_effect_outbox WHERE effect_id=${f.reference.effectId}`,
        (rows) => rows[0]?.status,
      );
      const row = (yield* f.sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_effect_outbox WHERE effect_id=${f.reference.effectId}`)[0]!;
      // Changes one fact for the check and always puts it back.
      const withRow = <A, E>(
        change: Effect.Effect<unknown, SqlError>,
        restore: Effect.Effect<unknown, SqlError>,
        check: Effect.Effect<A, E>,
      ) =>
        Effect.acquireUseRelease(
          Effect.orDie(change),
          () => check,
          () => Effect.orDie(restore),
        );

      // Started but unconfirmed.
      assert.isFalse(yield* succeed());
      yield* f.repository.confirmExecution!(f.confirmation);

      assert.isFalse(yield* succeed("other-worker"));
      assert.isFalse(
        yield* withRow(
          f.sql`UPDATE orchestration_v2_effect_outbox SET attempt_count=2 WHERE effect_id=${f.reference.effectId}`,
          f.sql`UPDATE orchestration_v2_effect_outbox SET attempt_count=1 WHERE effect_id=${f.reference.effectId}`,
          succeed(),
        ),
      );
      assert.isFalse(
        yield* withRow(
          f.sql`UPDATE orchestration_v2_effect_outbox SET updated_at='2026-10-02T12:34:57Z' WHERE effect_id=${f.reference.effectId}`,
          f.sql`UPDATE orchestration_v2_effect_outbox SET updated_at=${now} WHERE effect_id=${f.reference.effectId}`,
          succeed(),
        ),
      );
      // Time alone neither loses the process claim nor proves provider completion.
      yield* withRow(
        TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2099-06-01T00:00:00Z"))),
        TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(now))),
        f.repository.confirmExecution!(f.confirmation),
      );
      assert.strictEqual(yield* status, "running");
      assert.strictEqual(
        (yield* f.sql`SELECT effect_id FROM jones_native_creation_execution_process_confirmations WHERE effect_id=${f.reference.effectId}`)
          .length,
        1,
      );
      // The immutable start is never replayed, even with the same operation identity.
      assert.strictEqual((yield* f.issue.pipe(Effect.result))._tag, "Failure");
      for (const [change, restore] of [
        [
          f.sql`UPDATE orchestration_v2_effect_outbox SET status='failed' WHERE effect_id=${f.reference.effectId}`,
          f.sql`UPDATE orchestration_v2_effect_outbox SET status='running' WHERE effect_id=${f.reference.effectId}`,
        ],
        [
          f.sql`UPDATE orchestration_v2_effect_outbox SET command_id='changed-command' WHERE effect_id=${f.reference.effectId}`,
          f.sql`UPDATE orchestration_v2_effect_outbox SET command_id=${f.effect.commandId} WHERE effect_id=${f.reference.effectId}`,
        ],
      ] as const) {
        const confirmed = yield* withRow(
          change,
          restore,
          f.repository.confirmExecution!(f.confirmation).pipe(Effect.result),
        );
        assert.strictEqual(confirmed._tag, "Failure");
      }

      const staleReference = yield* f.repository.confirmExecution!({
        ...f.confirmation,
        reference: { ...f.confirmation.reference, claimId: "changed-claim" },
      }).pipe(Effect.result);
      assert.strictEqual(staleReference._tag, "Failure");

      // A matching proof never admits an envelope the strict decoder rejects.
      const payload = JSON.parse(row.payload_json) as Record<string, unknown>;
      const reference = payload.nativeCreationExecutionReference as Record<string, unknown>;
      for (const malformed of [
        { ...payload, nativeCreationExecutionReference: { ...reference, stage: "unknown" } },
        { ...payload, nativeCreationExecutionReference: null },
        { ...payload, extra: true },
        { nativeCreationExecutionReference: reference },
      ]) {
        const failed = yield* withRow(
          f.sql`UPDATE orchestration_v2_effect_outbox SET payload_json=${JSON.stringify(malformed)} WHERE effect_id=${f.reference.effectId}`,
          f.sql`UPDATE orchestration_v2_effect_outbox SET payload_json=${row.payload_json} WHERE effect_id=${f.reference.effectId}`,
          succeed().pipe(Effect.result),
        );
        assert.strictEqual(failed._tag, "Failure", JSON.stringify(malformed));
        assert.strictEqual(yield* status, "running");
      }

      assert.isTrue(yield* succeed());
      assert.strictEqual(yield* status, "succeeded");
      assert.isFalse(yield* succeed());
    }).pipe(Effect.provide(Outbox.layer.pipe(Layer.provideMerge(realRepository)))),
);

it.effect(
  "blocks a legacy started-but-unconfirmed native effect without adopting or replaying it",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.accept();
      yield* f.enqueue;
      yield* f.sql`INSERT INTO jones_native_creation_execution_starts(effect_id,claim_id,command_id,worker_id,expected_attempt,lease_expires_at,reference_json,fact_json)
      VALUES(${f.reference.effectId},${f.reference.claimId},${f.reference.stageCommandId},'historical-worker',1,${deadline},${nativeCreationCanonicalJson(f.reference)},'{}')`;
      const started = yield* f.issue.pipe(Effect.result);
      assert.strictEqual(started._tag, "Failure");
      assert.strictEqual(
        (yield* f.sql`SELECT effect_id FROM jones_native_creation_execution_process_claims WHERE effect_id=${f.reference.effectId}`)
          .length,
        0,
      );
      assert.strictEqual(
        (yield* f.sql`SELECT effect_id FROM jones_native_creation_execution_starts WHERE effect_id=${f.reference.effectId}`)
          .length,
        1,
      );
    }).pipe(Effect.provide(realRepository)),
);

it.effect.each([
  "worker",
  "attempt",
  "claimed-at",
  "command",
  "thread",
  "run",
  "request-digest",
] as const)("native start rejects a stale caller's expected %s before recording custody", (field) =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.accept();
    yield* f.enqueue;
    const expected: Repository.NativeExecutionProcessClaim = {
      workerId: "sql-worker",
      expectedAttempt: 1,
      claimedAt: now,
      commandId: f.command.commandId,
      threadId: f.command.threadId,
      runId: f.command.runId,
      requestDigest: nativeCreationSha256(nativeCreationCanonicalJson(f.effect.request)),
      ...(field === "worker" ? { workerId: "stale-worker" } : {}),
      ...(field === "attempt" ? { expectedAttempt: 2 } : {}),
      ...(field === "claimed-at" ? { claimedAt: "2026-10-02T12:34:57Z" } : {}),
      ...(field === "command" ? { commandId: "other-command" } : {}),
      ...(field === "thread" ? { threadId: "other-thread" } : {}),
      ...(field === "run" ? { runId: "other-run" } : {}),
      ...(field === "request-digest" ? { requestDigest: "a".repeat(64) } : {}),
    };
    const before = yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox`;
    const failure = yield* Effect.gen(function* () {
      const authority = yield* Authority.NativeCreationAuthority;
      return yield* authority.issueExecution!({
        reference: f.reference,
        timestamp: now,
        expectedClaim: expected,
      });
    }).pipe(Effect.provide(f.authorityLayer), Effect.flip);
    assert.strictEqual(failure.code, "unresolved_claim");
    assert.deepEqual(yield* f.sql`SELECT * FROM orchestration_v2_effect_outbox`, before);
    assert.deepEqual(
      yield* f.sql`SELECT * FROM jones_native_creation_execution_process_claims`,
      [],
    );
  }).pipe(Effect.provide(realRepository)),
);

it.effect("native hold before any start blocks issuance and leaves no process claim", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.accept();
    yield* f.enqueue;
    yield* f.repository.holdExecution!(f.reference, "uncertain before start");
    assert.strictEqual((yield* f.issue.pipe(Effect.flip)).code, "unresolved_claim");
    assert.deepEqual(
      yield* f.sql`SELECT * FROM jones_native_creation_execution_process_claims`,
      [],
    );
    assert.strictEqual(
      (yield* f.sql`SELECT * FROM jones_native_creation_execution_process_holds`).length,
      1,
    );
  }).pipe(Effect.provide(realRepository)),
);

it.effect("a legacy orphan hold blocks native issuance even without a legacy start", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.accept();
    yield* f.enqueue;
    // Reproduce readable legacy orphan history inside a rollback-only synthetic transaction.
    // Deferred FK validation lets the start guard encounter it before commit; no orphan is persisted.
    const rollback = yield* f.sql
      .withTransaction(
        Effect.gen(function* () {
          yield* f.sql`PRAGMA defer_foreign_keys=ON`;
          yield* f.sql`INSERT INTO jones_native_creation_execution_holds(effect_id,reason,held_at) VALUES(${f.reference.effectId},'legacy uncertainty',${now})`;
          assert.deepEqual(yield* f.sql`SELECT * FROM jones_native_creation_execution_starts`, []);
          assert.strictEqual((yield* f.issue.pipe(Effect.flip)).code, "unresolved_claim");
          assert.deepEqual(
            yield* f.sql`SELECT * FROM jones_native_creation_execution_process_claims`,
            [],
          );
          return yield* Effect.fail("rollback synthetic orphan");
        }),
      )
      .pipe(Effect.flip);
    assert.strictEqual(rollback, "rollback synthetic orphan");
    assert.deepEqual(yield* f.sql`SELECT * FROM jones_native_creation_execution_holds`, []);
  }).pipe(Effect.provide(realRepository)),
);
