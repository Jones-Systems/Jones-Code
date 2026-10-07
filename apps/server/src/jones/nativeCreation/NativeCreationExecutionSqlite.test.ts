import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { makeNativeExecutionMethods } from "./NativeCreationExecutionSqlite.ts";
import { NativeCreationRepositoryError } from "./NativeCreationRepository.ts";
import { NativeCreationExecutionReferenceV2 } from "./NativeCreationExecutionTypes.ts";
const memory = NodeSqliteClient.layer({ filename: ":memory:" });
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
    const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)({
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
  OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
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
const fixture = Effect.gen(function* () {
  yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(now)));
  const repository = yield* Repository.NativeCreationRepository;
  const sql = yield* SqlClient.SqlClient;
  const binding = yield* Schema.decodeUnknownEffect(NativePreparationBinding)({
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
  const historical = yield* Schema.decodeUnknownEffect(NativeCreationHistoricalBinding)({
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
  const command = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
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
  const normalized = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
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
  const reference = yield* Schema.decodeUnknownEffect(NativeCreationExecutionReferenceV2)({
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
    const payload = yield* Schema.encodeEffect(
      Schema.fromJsonString(Outbox.NativeOrchestrationEffectPayloadV2),
    )({
      request: { type: "provider-turn.start", runId: command.runId },
      nativeCreationExecutionReference: reference,
    });
    yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,lease_owner,lease_expires_at,created_at,updated_at)
      VALUES(${reference.effectId},${command.commandId},${command.threadId},'provider-turn.start',${payload},'running',1,${now},'sql-worker',${deadline},${now},${now})`;
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
  const evidence = yield* Schema.decodeUnknownEffect(NativeCreationWholeOperationEvidence)({
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
  const session = yield* Schema.decodeUnknownEffect(AuthSessions.AuthSessionRecord)({
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
          getById: () => Effect.succeed(Option.some(session)),
          create: () => Effect.void,
          createReplacingActive: () => Effect.succeed([]),
          createIfAbsent: () => Effect.void,
          listActive: () => Effect.succeed([]),
          revoke: () => Effect.succeed(false),
          revokeAllExcept: () => Effect.succeed([]),
          setLastConnectedAt: () => Effect.void,
          setClientConnection: () => Effect.void,
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
    return yield* authority.issueExecution!({ reference, timestamp: now });
  }).pipe(Effect.provide(authorityLayer));
  const confirmation = {
    reference,
    workerId: "sql-worker",
    expectedAttempt: 1,
    leaseExpiresAt: deadline,
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
    leaseExpiresAt: deadline,
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
      const mismatched = yield* Schema.decodeUnknownEffect(OrchestrationV2Command)({
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
        yield* f.sql`SELECT effect_id FROM jones_native_creation_execution_confirmations`,
        [],
      );
      yield* f.repository.confirmExecution!(f.confirmation);
      yield* f.repository.confirmExecution!(f.confirmation);
      assert.deepEqual(
        yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_starts`,
        [{ count: 1 }],
      );
      assert.deepEqual(
        yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_confirmations`,
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
      assert.deepEqual(yield* f.sql`SELECT reason FROM jones_native_creation_execution_holds`, [
        { reason: "uncertain native outcome" },
      ]);
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
            return f.evidence;
          }),
      }),
    );
    assert.strictEqual(invocations, 1);
    assert.deepEqual(
      yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_confirmations`,
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
      yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_holds`,
      [{ count: 1 }],
    );
    assert.deepEqual(
      yield* f.sql`SELECT effect_id FROM jones_native_creation_execution_confirmations`,
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
      yield* f.sql`SELECT COUNT(*) AS count FROM jones_native_creation_execution_holds`,
      [{ count: 1 }],
    );
    assert.deepEqual(
      yield* f.sql`SELECT effect_id FROM jones_native_creation_execution_confirmations`,
      [],
    );
  }).pipe(Effect.provide(realRepository)),
);
