import { assert, it } from "@effect/vitest";
import {
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
  CommandId,
  RunId,
  ThreadId,
  AuthSessionId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderThreadId,
  ProviderSessionId,
  RunAttemptId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import * as EventStore from "../../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { CodexProviderCapabilitiesV2 } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";
import {
  NativeCreationAuthorityError,
  NativeCreationBindingResolver,
  NativeCreationGrantResolver,
  makeNativeCreationAuthority,
  type NativeCreationGrant,
} from "../../orchestration-v2/NativeCreationAuthority.ts";
import { AuthSessionRepository, make as makeAuthSessions } from "../AuthSessions.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativeCreationV2CommandDigest,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "../../orchestration-v2/NativeCreationPreparation.ts";
import migration from "../Migrations/003_JonesNativeCreationIntents.ts";
import identityMigration from "../Migrations/004_JonesNativeCreationCommandIdentities.ts";
import receiptMigration from "../Migrations/002_OrchestrationCommandReceipts.ts";
import nativeAcceptanceMigration from "../Migrations/007_JonesV2NativeAcceptance.ts";
import {
  NativeCreationRepository,
  type NativeCreationClaimInput,
} from "../Services/NativeCreationRepository.ts";
import { layer, make } from "./NativeCreationRepository.ts";

const memory = NodeSqliteClient.layer({ filename: ":memory:" });
const database = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* migration;
    yield* receiptMigration;
    yield* identityMigration;
    yield* nativeAcceptanceMigration;
  }),
).pipe(Layer.provideMerge(memory));
const repositoryLayer = layer.pipe(Layer.provideMerge(database));
const v2RepositoryLayer = Layer.mergeAll(
  layer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
  EffectOutbox.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);
const confirmationStores = Layer.mergeAll(
  SqlitePersistenceMemory,
  EventStore.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
  ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);
const confirmationLayer = Layer.mergeAll(
  v2RepositoryLayer,
  confirmationStores,
  EventSink.layer.pipe(Layer.provide(confirmationStores)),
);
const timestamp = "2026-10-02T12:34:56Z";
const decodeFixtureBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeFixtureHistory = Schema.decodeUnknownSync(NativeCreationHistoricalBinding);
const decodeFixtureCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
// Same bytes as JSON.stringify; a failure stays a defect as the native throw was.
const encodeJson = (value: unknown) =>
  Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(value).pipe(Effect.orDie);
const releaseCommand = (preparation: {
  readonly command: { readonly commandId: string; readonly threadId: string };
}) =>
  decodeFixtureCommand({
    type: "prepared-run.release",
    commandId: CommandId.make(preparation.command.commandId),
    threadId: ThreadId.make(preparation.command.threadId),
    runId: RunId.make("fixture-prepared-run"),
  });
const enrolledSessionId = AuthSessionId.make("fixture-enrolled-session");
const fixture = Effect.fnUntraced(function* (
  operationId = "fixture-operation",
  text = "Synthetic prompt",
  path = "/fixture/worktree",
) {
  const binding = decodeFixtureBinding({
    backend_instance: "fixture-backend",
    environment_id: "fixture-environment",
    project_id: "fixture-project",
    project_cwd: "/fixture/project",
    account_ref: "fixture-account",
    runtime_mode: "full-access" as const,
    interaction_mode: "default" as const,
    base_branch: "main",
    start_from_origin: false,
    run_setup_script: false,
    provider_model_selection: { instanceId: "codex", model: "fixture-model" },
  });
  const command = nativePreparationCommand(
    operationId,
    binding,
    text,
    "Synthetic thread",
    timestamp,
  );
  const preparation = yield* validateNativeCreationPreparation(
    new TextEncoder().encode(
      nativeCreationCanonicalJson({
        schema: "voice.t3-bootstrap-preparation/v1",
        operation_id: operationId,
        binding,
        command,
        preparation_id: command.commandId.replace("voice-command-", "voice-bootstrap-"),
        binding_digest: nativeCreationSha256(nativeCreationCanonicalJson(binding)),
        prompt_digest: nativeCreationSha256(text),
        command_digest: nativeCreationSha256(nativeCreationCanonicalJson(command)),
      }),
    ),
  );
  const historical = decodeFixtureHistory({
    backendInstance: binding.backend_instance,
    environmentId: binding.environment_id,
    projectId: binding.project_id,
    projectCwd: binding.project_cwd,
    accountRef: binding.account_ref,
    accountBindingId: "qualified-fixture-account",
    accountBindingRevision: 1,
    providerModelSelection: binding.provider_model_selection,
    runtimeMode: binding.runtime_mode,
    interactionMode: binding.interaction_mode,
    baseBranch: binding.base_branch,
    startFromOrigin: false,
    runSetupScript: false,
    requestedBranch: command.bootstrap.prepareWorktree.branch,
  });
  const input: NativeCreationClaimInput = {
    preparation,
    resources: {
      projectCwd: binding.project_cwd,
      branch: historical.requestedBranch,
      worktreePath: path,
    },
    claimId: `claim-${operationId}`,
    claimedBootId: "fixture-boot",
    claimedAt: timestamp,
    actorSessionId: "fixture-session",
    grantId: "fixture-grant",
    grantRevision: 1,
  };
  return { input, historical, preparation, authorize: Effect.succeed(historical) };
});

const acceptedV2Release = Effect.fnUntraced(function* (messageText = "Synthetic prompt") {
  const repository = yield* NativeCreationRepository;
  const sql = yield* SqlClient.SqlClient;
  const value = yield* fixture("fixture-v2-execution", messageText);
  yield* repository.claim(value.input, value.authorize);
  const command = releaseCommand(value.preparation);
  if (command.type !== "prepared-run.release")
    return yield* Effect.die("Fixture release type changed");
  yield* repository.reserveCommandIdentities(value.input.claimId, [command.commandId]);
  yield* repository.recordNormalizedCommand(value.input.claimId, command);
  const digest = Option.getOrThrow(
    yield* repository.getReservedCommand(command.commandId),
  ).commandDigest;
  yield* sql`INSERT INTO orchestration_command_receipts
    (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status)
    VALUES (${command.commandId}, 'thread', ${command.threadId}, ${command.type}, ${timestamp}, 0, 'accepted')`;
  yield* sql`INSERT INTO orchestration_v2_native_command_identities
    (command_id, kind, version, command_type, aggregate_kind, aggregate_id, normalized_command_digest, binding_digest)
    VALUES (${command.commandId}, 'native_creation_stage', 2, ${command.type}, 'thread', ${command.threadId},
      ${digest}, ${value.preparation.bindingDigest})`;
  const id = `effect:${command.commandId}:provider-turn.start:${command.runId}`;
  const reference = {
    version: 2 as const,
    claimId: value.input.claimId,
    stageCommandId: command.commandId,
    effectId: id,
    stage: "native_command" as const,
  };
  return {
    ...value,
    repository,
    sql,
    command,
    digest,
    reference,
    pending: {
      id,
      commandId: command.commandId,
      threadId: command.threadId,
      request: { type: "provider-turn.start" as const, runId: command.runId },
      nativeCreationExecutionReference: reference,
    },
  };
});

const nativeConfirmationFixture = Effect.fnUntraced(function* (messageText = "Synthetic prompt") {
  yield* TestClock.setTime(Date.parse(timestamp));
  const value = yield* acceptedV2Release(messageText);
  const sink = yield* EventSink.EventSinkV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const now = DateTime.makeUnsafe(timestamp);
  const threadId = value.command.threadId;
  const instanceId = ProviderInstanceId.make("codex");
  const driver = ProviderDriverKind.make("codex");
  const providerThreadId = ProviderThreadId.make("provider-thread:native-confirmation");
  const providerSessionId = ProviderSessionId.make("session:native-confirmation");
  const attemptId = RunAttemptId.make("attempt:native-confirmation");
  const history = yield* value.repository.readHistoryByClaim(value.reference.claimId);
  const messageId = MessageId.make(history.intent.messageId);
  const modelSelection = { instanceId, model: "fixture-model" };
  const thread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make("fixture-project"),
    title: "Fixture",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThreadId,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const events: OrchestrationV2DomainEvent[] = [
    {
      id: EventId.make("event:native-confirmation:birth"),
      type: "thread.created",
      threadId,
      occurredAt: now,
      payload: thread,
    },
    {
      id: EventId.make("event:native-confirmation:session"),
      type: "provider-session.attached",
      threadId,
      occurredAt: now,
      payload: {
        id: providerSessionId,
        driver,
        providerInstanceId: instanceId,
        status: "ready",
        cwd: "/fixture/project",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
    },
    {
      id: EventId.make("event:native-confirmation:provider"),
      type: "provider-thread.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: providerThreadId,
        driver,
        providerInstanceId: instanceId,
        providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "native-confirmed", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "active",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      id: EventId.make("event:native-confirmation:message"),
      type: "message.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: messageId,
        threadId,
        runId: value.command.runId,
        nodeId: null,
        createdBy: "user",
        creationSource: "web",
        role: "user",
        text: messageText,
        attachments: [],
        streaming: false,
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      id: EventId.make("event:native-confirmation:run"),
      type: "run.created",
      threadId,
      occurredAt: now,
      payload: {
        id: value.command.runId,
        threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId,
        userMessageId: messageId,
        rootNodeId: NodeId.make("node:native-confirmation"),
        activeAttemptId: attemptId,
        status: "running",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    },
    {
      id: EventId.make("event:native-confirmation:attempt"),
      type: "run-attempt.created",
      threadId,
      occurredAt: now,
      payload: {
        id: attemptId,
        runId: value.command.runId,
        attemptOrdinal: 1,
        rootNodeId: NodeId.make("node:native-confirmation"),
        providerInstanceId: instanceId,
        providerThreadId,
        providerTurnId: null,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    },
  ];
  const stored = yield* sink.write({ commandId: value.command.commandId, events });
  yield* value.sql`UPDATE orchestration_command_receipts SET result_sequence = ${stored.at(-1)!.sequence} WHERE command_id = ${value.command.commandId}`;
  const binding = {
    threadId,
    providerThreadId,
    providerSessionId,
    instanceId,
    runtimeGeneration: "actual-confirmation-generation",
    nativeThreadId: "native-confirmed",
  };
  const registered = yield* sink.registerProviderRuntime({
    expectedBinding: { ...binding, driver, runtimeGeneration: null },
    expectedEvidenceRevision: 0,
    actualBinding: binding,
    expectedRunId: value.command.runId,
    expectedRunAttemptId: attemptId,
  });
  if (!registered.committed) return yield* Effect.die(registered.rejection);
  yield* outbox.enqueue([value.pending]);
  const claimed = Option.getOrThrow(
    yield* outbox.claimNext({ workerId: "worker:native-confirmation", leaseDurationMs: 60_000 }),
  );
  yield* value.repository.startEffectV2(value.reference, timestamp, value.authorize);
  const input = {
    effectId: claimed.id,
    workerId: "worker:native-confirmation",
    expectedAttempt: claimed.attemptCount,
    runId: value.command.runId,
    attemptId,
    binding,
    expectedEvidenceRevision: registered.evidenceRevision,
    evidence: {
      operationId: claimed.id,
      operation:
        messageText.trim().toLowerCase() === "/compact"
          ? ("compact_thread" as const)
          : ("start_turn" as const),
      outcome: "confirmed_success" as const,
      threadId,
      providerThreadId,
      providerSessionId,
      instanceId,
      runtimeGeneration: binding.runtimeGeneration,
      attemptId,
    },
  };
  return { ...value, sink, outbox, input, stored };
});

it.effect(
  "native complete-operation confirmation commits immutable proof external completion and terminal outbox together",
  () =>
    Effect.gen(function* () {
      const value = yield* nativeConfirmationFixture();
      assert.isNull(yield* value.repository.readNativeEffectConfirmation(value.input.effectId));
      const proof = yield* value.repository.recordNativeEffectConfirmation(value.input);
      assert.deepEqual(
        yield* value.repository.readNativeEffectConfirmation(value.input.effectId),
        proof,
      );
      assert.deepEqual(proof.binding, value.input.binding);
      assert.deepEqual(proof.nativeExecutionReference, value.reference);
      assert.deepEqual(proof.commandEvent, {
        eventId: value.stored.at(-1)!.event.id,
        sequence: value.stored.at(-1)!.sequence,
      });
      assert.strictEqual(
        Option.getOrThrow(yield* value.outbox.get(value.input.effectId)).status,
        "succeeded",
      );
      assert.deepEqual(
        (yield* value.repository.readHistoryByClaim(value.reference.claimId)).effectsV2.map(
          (fact) => fact.phase,
        ),
        ["started", "completed"],
      );
      assert.strictEqual(
        (yield* value.repository.recordNativeEffectConfirmation(value.input).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      assert.strictEqual(
        (yield* value.sql`SELECT * FROM orchestration_v2_native_effect_confirmations`).length,
        1,
      );
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "native confirmation rejects stale claims binding revisions attempts and incomplete evidence before terminal success",
  () =>
    Effect.gen(function* () {
      const value = yield* nativeConfirmationFixture();
      const invalid = [
        { ...value.input, workerId: "different-worker" },
        { ...value.input, expectedAttempt: value.input.expectedAttempt + 1 },
        { ...value.input, expectedEvidenceRevision: value.input.expectedEvidenceRevision + 1 },
        {
          ...value.input,
          binding: { ...value.input.binding, runtimeGeneration: "old-generation" },
        },
        {
          ...value.input,
          evidence: { ...value.input.evidence, operationId: "different-operation" },
        },
        { ...value.input, evidence: { ...value.input.evidence, outcome: "unknown" as const } },
        {
          ...value.input,
          evidence: { ...value.input.evidence, operation: "compact_thread" as const },
        },
      ];
      for (const input of invalid) {
        assert.strictEqual(
          (yield* value.repository.recordNativeEffectConfirmation(input).pipe(Effect.flip)).code,
          "unresolved_claim",
        );
        assert.isNull(yield* value.repository.readNativeEffectConfirmation(input.effectId));
        assert.strictEqual(
          Option.getOrThrow(yield* value.outbox.get(input.effectId)).status,
          "running",
        );
      }
      yield* value.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.deletedAt', ${timestamp})
      WHERE thread_id = ${value.input.binding.threadId}`;
      assert.strictEqual(
        (yield* value.repository.recordNativeEffectConfirmation(value.input).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      assert.isNull(yield* value.repository.readNativeEffectConfirmation(value.input.effectId));
      assert.strictEqual(
        Option.getOrThrow(yield* value.outbox.get(value.input.effectId)).status,
        "running",
      );
      yield* value.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.deletedAt', NULL)
      WHERE thread_id = ${value.input.binding.threadId}`;
      yield* value.sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = '2000-01-01T00:00:00Z' WHERE effect_id = ${value.input.effectId}`;
      assert.strictEqual(
        (yield* value.repository.recordNativeEffectConfirmation(value.input).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_native_effect_confirmations`,
        [],
      );
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "native completion failure rolls confirmation and terminal settlement back and exact retry remains fenced",
  () =>
    Effect.gen(function* () {
      const value = yield* nativeConfirmationFixture();
      yield* value.sql`CREATE TRIGGER fixture_fail_native_completion BEFORE INSERT ON native_creation_effect_facts
      WHEN NEW.phase = 'completed' BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END`;
      assert.strictEqual(
        (yield* value.repository.recordNativeEffectConfirmation(value.input).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_native_effect_confirmations`,
        [],
      );
      assert.deepEqual(
        (yield* value.repository.readHistoryByClaim(value.reference.claimId)).effectsV2.map(
          (fact) => fact.phase,
        ),
        ["started"],
      );
      assert.strictEqual(
        Option.getOrThrow(yield* value.outbox.get(value.input.effectId)).status,
        "running",
      );
      yield* value.sql`DROP TRIGGER fixture_fail_native_completion`;
      const proof = yield* value.repository.recordNativeEffectConfirmation(value.input);
      assert.deepEqual(
        yield* value.repository.readNativeEffectConfirmation(value.input.effectId),
        proof,
      );
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "native compact confirmation requires the actual attachment-free compact message operation",
  () =>
    Effect.gen(function* () {
      const value = yield* nativeConfirmationFixture(" /compact ");
      assert.strictEqual(
        (yield* value.repository
          .recordNativeEffectConfirmation({
            ...value.input,
            evidence: { ...value.input.evidence, operation: "start_turn" },
          })
          .pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      const proof = yield* value.repository.recordNativeEffectConfirmation(value.input);
      assert.strictEqual(proof.evidence.operation, "compact_thread");
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

const threadRecoveryFixture = Effect.fnUntraced(function* (name: string) {
  yield* TestClock.setTime(Date.parse(timestamp));
  const value = yield* fixture(`fixture-recovery-${name}`);
  const repository = yield* NativeCreationRepository;
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const sessions = yield* makeAuthSessions;
  const now = yield* DateTime.now;
  const sessionId = AuthSessionId.make(value.input.actorSessionId);
  yield* sessions.create({
    sessionId,
    subject: "fixture-recovery-actor",
    method: "bearer-access-token",
    scopes: ["orchestration:read", "orchestration:operate"],
    issuedAt: now,
    expiresAt: DateTime.add(now, { days: 1 }),
    client: {
      label: null,
      ipAddress: null,
      userAgent: null,
      deviceType: "bot",
      os: null,
      browser: null,
    },
  });
  yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at) VALUES (${sessionId}, ${timestamp})`;
  const guard = {
    schema: "t3.native-creation-guard/v1" as const,
    grantId: value.input.grantId,
    grantRevision: value.input.grantRevision,
  };
  const input = {
    actorSessionId: sessionId,
    preparation: value.preparation,
    guard,
    resources: value.input.resources,
  };
  let grant: NativeCreationGrant = {
    grantId: guard.grantId,
    revision: guard.grantRevision,
    actorSessionId: sessionId,
    issuerId: "fixture-qualified-issuer",
    expiresAt: DateTime.add(now, { days: 1 }),
    revoked: false,
    operationId: value.preparation.operationId,
    preparationId: value.preparation.preparationId,
    preparationSha256: value.preparation.preparationSha256,
    bindingDigest: value.preparation.bindingDigest,
    binding: value.historical,
    resources: input.resources,
    allowedStages: ["claim", "native_command"],
    recoveryScopes: [],
  };
  const authority = yield* makeNativeCreationAuthority.pipe(
    Effect.provideService(AuthSessionRepository, sessions),
    Effect.provideService(NativeCreationGrantResolver, {
      resolveCurrent: () =>
        Effect.sync(() => ({
          enrolledSessionId: sessionId,
          trustedIssuerId: grant.issuerId,
          grant,
        })),
    }),
    Effect.provideService(NativeCreationBindingResolver, {
      resolveCurrent: () => Effect.succeed(value.historical),
    }),
  );
  yield* repository.claim(value.input, authority.authorize({ ...input, stage: "claim" }));
  const original = value.preparation.command;
  const create = decodeFixtureCommand({
    type: "thread.create",
    commandId: `${original.commandId}:native:v2:create`,
    threadId: original.threadId,
    projectId: original.bootstrap.createThread.projectId,
    title: original.bootstrap.createThread.title,
    modelSelection: original.bootstrap.createThread.modelSelection,
    runtimeMode: value.historical.runtimeMode,
    interactionMode: value.historical.interactionMode,
    branch: input.resources.branch,
    worktreePath: input.resources.worktreePath,
    createdBy: "user",
    creationSource: "server",
  });
  if (create.type !== "thread.create") return yield* Effect.die("Expected canonical native create");
  const inventory = [
    original.commandId,
    create.commandId,
    `${original.commandId}:native:v2:message`,
  ];
  yield* repository.reserveCommandIdentities(value.input.claimId, inventory);
  yield* repository.reserveCommand(value.input.claimId, create);
  const thread: OrchestrationV2AppThread = {
    id: create.threadId,
    projectId: create.projectId,
    title: create.title,
    createdBy: create.createdBy,
    creationSource: create.creationSource,
    providerInstanceId: create.modelSelection.instanceId,
    modelSelection: create.modelSelection,
    runtimeMode: create.runtimeMode,
    interactionMode: create.interactionMode,
    branch: create.branch,
    worktreePath: create.worktreePath,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: create.threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const facts = yield* sink.readNativeCommandFacts({
    threadId: create.threadId,
    commandId: create.commandId,
    authority: {
      actorSessionId: sessionId,
      claimId: value.input.claimId,
      projectId: create.projectId,
      resourcePaths: [input.resources.projectCwd, input.resources.worktreePath],
    },
  });
  const acceptedCreate = yield* sink.commitCommand({
    commandId: create.commandId,
    threadId: create.threadId,
    commandType: create.type,
    acceptedAt: now,
    effects: [],
    events: [
      {
        id: EventId.make(`event:recovery:${name}:birth`),
        type: "thread.created",
        threadId: create.threadId,
        occurredAt: now,
        payload: thread,
      },
    ],
    nativeContext: {
      identity: {
        kind: "native_creation_stage",
        version: 2,
        commandId: create.commandId,
        commandType: create.type,
        aggregateKind: "thread",
        aggregateId: create.threadId,
        normalizedCommandDigest: nativeCreationV2CommandDigest(create),
        bindingDigest: value.preparation.bindingDigest,
      },
      snapshot: facts.commitSnapshot,
      revalidateAuthority: authority
        .authorize({ ...input, stage: "native_command" })
        .pipe(Effect.asVoid),
    },
  });
  assert.isTrue(acceptedCreate.committed);
  const incarnation = yield* sink.getThreadIncarnation(create.threadId);
  if (incarnation === null) return yield* Effect.die("Expected actual accepted V2 birth");
  assert.deepEqual(incarnation, {
    eventId: acceptedCreate.storedEvents[0]!.event.id,
    sequence: acceptedCreate.storedEvents[0]!.sequence,
  });
  const resource = { kind: "thread" as const, threadId: create.threadId, incarnation };
  const recoveryScopeId = `scope:recovery:${name}`;
  grant = {
    ...grant,
    allowedStages: ["claim", "native_command", "cleanup"],
    recoveryScopes: [{ scopeId: recoveryScopeId, resource }],
  };
  const authorization = {
    ...input,
    stage: "cleanup" as const,
    recoveryScopeId,
    recoveryResource: resource,
  };
  const cleanup = yield* repository.startEffect(
    value.input.claimId,
    {
      kind: "cleanup",
      phase: "started",
      effectId: `cleanup:recovery:${name}`,
      timestamp,
      recoveryScopeId,
      resource,
    },
    authority.authorize(authorization),
  );
  const command = {
    type: "thread.delete" as const,
    commandId: CommandId.make(`${original.commandId}:bootstrap-thread-delete`),
    threadId: create.threadId,
  };
  const reference = {
    version: 2 as const,
    claimId: value.input.claimId,
    commandId: command.commandId,
    threadId: command.threadId,
    commandType: command.type,
    canonicalCommand: command,
    commandDigest: nativeCreationV2CommandDigest(command),
    commandStartEffectId: `command-start:recovery:${name}`,
    cleanupStartEffectId: cleanup.effectId,
    cleanupStartOrdinal: cleanup.ordinal,
    recoveryScopeId,
    resource,
  };
  yield* repository.reserveThreadRecoveryCommand(reference);
  yield* repository.startEffect(
    value.input.claimId,
    {
      kind: "native_command",
      phase: "started",
      effectId: reference.commandStartEffectId,
      timestamp,
      commandId: command.commandId,
      threadId: command.threadId,
      commandType: command.type,
      commandDigest: reference.commandDigest,
    },
    authority.authorize({ ...input, stage: "native_command" }),
  );
  const context = yield* authority.issueThreadRecovery({
    claimId: value.input.claimId,
    commandStartEffectId: reference.commandStartEffectId,
    cleanupStartEffectId: reference.cleanupStartEffectId,
    authorization,
  });
  const acceptance = {
    commandId: command.commandId,
    threadId: command.threadId,
    commandType: command.type,
    acceptedAt: now,
    effects: [],
    recoveryContext: context,
    events: [
      {
        id: EventId.make(`event:recovery:${name}:deleted`),
        type: "thread.deleted" as const,
        threadId: command.threadId,
        occurredAt: now,
        payload: { ...thread, deletedAt: now, updatedAt: now },
      },
    ],
  };
  return {
    repository,
    sink,
    projections,
    sql,
    command,
    reference,
    context,
    acceptance,
    inventory,
    revokeGrant: () =>
      Effect.sync(() => {
        grant = { ...grant, revoked: true };
      }),
  };
});

it.effect(
  "thread recovery acceptance completes only its original command start and preserves immutable inventory on exact replay",
  () =>
    Effect.gen(function* () {
      const value = yield* threadRecoveryFixture("completion-replay");
      assert.deepEqual(
        yield* value.sink.readNativeThreadRecovery({
          command: value.command,
          context: value.context,
        }),
        value.reference,
      );
      const historyBefore = yield* value.repository.readHistoryByClaim(value.reference.claimId);
      assert.deepEqual(
        historyBefore.effects.map((fact) => [fact.kind, fact.phase, fact.ordinal]),
        [
          ["cleanup", "started", 0],
          ["native_command", "started", 1],
        ],
      );
      const accepted = yield* value.sink.commitCommand(value.acceptance);
      assert.isTrue(accepted.committed);
      const identity = yield* value.sink.readCommandReceiptIdentity(value.command.commandId);
      assert.deepEqual(identity.threadRecovery, value.reference);
      assert.strictEqual(identity.receipt?.status, "accepted");
      assert.isNull(identity.identity);
      const history = yield* value.repository.readHistoryByClaim(value.reference.claimId);
      const completion = history.effects.find(
        (fact) =>
          fact.effectId === value.reference.commandStartEffectId && fact.phase === "completed",
      );
      assert.strictEqual(completion?.kind, "native_command");
      if (completion?.kind !== "native_command" || completion.phase !== "completed")
        return yield* Effect.die("Missing original command completion");
      assert.strictEqual(completion.eventId, accepted.storedEvents.at(-1)!.event.id);
      assert.strictEqual(completion.sequence, accepted.receipt.resultSequence);
      assert.strictEqual(completion.commandDigest, value.reference.commandDigest);
      assert.strictEqual(completion.ordinal, 2);
      assert.isFalse(
        history.effects.some(
          (fact) =>
            fact.effectId === value.reference.cleanupStartEffectId && fact.phase === "completed",
        ),
      );
      assert.deepEqual(history.effectsV2, []);
      assert.deepEqual(history.intent, historyBefore.intent);
      assert.isNotNull(
        (yield* value.projections.getThreadShell(value.command.threadId))?.deletedAt,
      );
      const before = yield* value.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
      yield* value.revokeGrant();
      assert.deepEqual(
        yield* value.sink.readNativeThreadRecovery({
          command: value.command,
          context: value.context,
        }),
        value.reference,
      );
      const replayed = yield* value.sink.commitCommand(value.acceptance);
      assert.isFalse(replayed.committed);
      assert.deepEqual(replayed.receipt, accepted.receipt);
      assert.deepEqual(replayed.storedEvents, accepted.storedEvents);
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_events ORDER BY sequence`,
        before,
      );
      assert.deepEqual(
        yield* value.repository.readHistoryByClaim(value.reference.claimId),
        history,
      );
      assert.deepEqual(
        (yield* value.sql<{
          command_id: string;
        }>`SELECT command_id FROM native_creation_reserved_command_identities ORDER BY command_id`).map(
          (row) => row.command_id,
        ),
        [...value.inventory].sort(),
      );
      assert.isTrue(
        Option.isNone(yield* value.repository.getReservedCommand(value.command.commandId)),
      );
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "thread recovery completion insertion failure rolls deletion receipt and projection back while retaining both original starts",
  () =>
    Effect.gen(function* () {
      const value = yield* threadRecoveryFixture("completion-rollback");
      const history = yield* value.repository.readHistoryByClaim(value.reference.claimId);
      const projection = yield* value.projections.getThreadShell(value.command.threadId);
      const before = yield* value.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
      yield* value.sql`CREATE TRIGGER fixture_fail_recovery_completion BEFORE INSERT ON native_creation_effect_facts
      WHEN NEW.phase = 'completed' BEGIN SELECT RAISE(ABORT, 'injected recovery completion failure'); END`;
      assert.strictEqual(
        (yield* value.sink.commitCommand(value.acceptance).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.isNull(
        (yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).receipt,
      );
      assert.deepEqual(yield* value.projections.getThreadShell(value.command.threadId), projection);
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_events ORDER BY sequence`,
        before,
      );
      assert.deepEqual(
        yield* value.repository.readHistoryByClaim(value.reference.claimId),
        history,
      );
      assert.deepEqual(
        yield* value.repository.readThreadRecoveryCommand(value.command.commandId),
        value.reference,
      );
      yield* value.sql`DROP TRIGGER fixture_fail_recovery_completion`;
      assert.isTrue((yield* value.sink.commitCommand(value.acceptance)).committed);
      const completed = yield* value.repository.readHistoryByClaim(value.reference.claimId);
      assert.strictEqual(
        completed.effects.filter(
          (fact) =>
            fact.effectId === value.reference.commandStartEffectId && fact.phase === "completed",
        ).length,
        1,
      );
      assert.isFalse(
        completed.effects.some(
          (fact) =>
            fact.effectId === value.reference.cleanupStartEffectId && fact.phase === "completed",
        ),
      );
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "thread recovery rejects copied authority and a revoked fresh grant without completing or deleting",
  () =>
    Effect.gen(function* () {
      const value = yield* threadRecoveryFixture("authority-fences");
      const history = yield* value.repository.readHistoryByClaim(value.reference.claimId);
      const projection = yield* value.projections.getThreadShell(value.command.threadId);
      const before = yield* value.sql`SELECT * FROM orchestration_events ORDER BY sequence`;
      const copied = yield* value.sink
        .commitCommand({ ...value.acceptance, recoveryContext: structuredClone(value.context) })
        .pipe(Effect.flip);
      assert.instanceOf(copied, EventSink.NativeCommandPreconditionError);
      assert.strictEqual(
        copied._tag === "NativeCommandPreconditionError" ? copied.reason : undefined,
        "identity_conflict",
      );
      const { recoveryContext: _context, ...stripped } = value.acceptance;
      const ordinary = yield* value.sink.commitCommand(stripped).pipe(Effect.flip);
      assert.instanceOf(ordinary, EventSink.NativeCommandPreconditionError);
      assert.strictEqual(
        ordinary._tag === "NativeCommandPreconditionError" ? ordinary.reason : undefined,
        "identity_conflict",
      );
      yield* value.revokeGrant();
      assert.strictEqual(
        (yield* value.sink.commitCommand(value.acceptance).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.isNull(
        (yield* value.sink.readCommandReceiptIdentity(value.command.commandId)).receipt,
      );
      assert.deepEqual(
        yield* value.repository.readHistoryByClaim(value.reference.claimId),
        history,
      );
      assert.deepEqual(yield* value.projections.getThreadShell(value.command.threadId), projection);
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_events ORDER BY sequence`,
        before,
      );
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "thread recovery reserves outside the immutable creation inventory before its original V1 command start",
  () =>
    Effect.gen(function* () {
      const value = yield* fixture("fixture-separated-recovery");
      const repository = yield* NativeCreationRepository;
      const sql = yield* SqlClient.SqlClient;
      yield* repository.claim(value.input, value.authorize);
      const originalId = value.preparation.command.commandId;
      const inventory = [
        originalId,
        `${originalId}:native:v2:create`,
        `${originalId}:native:v2:message`,
      ];
      yield* repository.reserveCommandIdentities(value.input.claimId, inventory);
      const threadId = ThreadId.make(value.preparation.command.threadId);
      const command = {
        type: "thread.delete" as const,
        commandId: CommandId.make(`${originalId}:bootstrap-thread-delete`),
        threadId,
      };
      const resource = {
        kind: "thread" as const,
        threadId,
        incarnation: { eventId: EventId.make("event:recovery:birth"), sequence: 4 },
      };
      const cleanup = yield* repository.startEffect(
        value.input.claimId,
        {
          kind: "cleanup",
          phase: "started",
          effectId: "cleanup:separated-recovery",
          timestamp,
          resource,
          recoveryScopeId: "scope:separated-recovery",
        },
        value.authorize,
      );
      assert.strictEqual(cleanup.ordinal, 0);
      const reference = {
        version: 2 as const,
        claimId: value.input.claimId,
        commandId: command.commandId,
        threadId,
        commandType: command.type,
        canonicalCommand: command,
        commandDigest: nativeCreationV2CommandDigest(command),
        commandStartEffectId: "command-start:separated-recovery",
        cleanupStartEffectId: cleanup.effectId,
        cleanupStartOrdinal: cleanup.ordinal,
        recoveryScopeId: "scope:separated-recovery",
        resource,
      };
      const rolled = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* repository.reserveThreadRecoveryCommand(reference);
            return yield* Effect.fail("rollback recovery reservation before start");
          }),
        )
        .pipe(Effect.result);
      assert.strictEqual(rolled._tag, "Failure");
      assert.isNull(yield* repository.readThreadRecoveryCommand(command.commandId));
      assert.deepEqual(yield* repository.reserveThreadRecoveryCommand(reference), reference);
      assert.deepEqual(yield* repository.reserveThreadRecoveryCommand(reference), reference);
      assert.isTrue(Option.isNone(yield* repository.getReservedCommandIdentity(command.commandId)));
      assert.isTrue(Option.isNone(yield* repository.getReservedCommand(command.commandId)));
      assert.strictEqual(
        (yield* repository
          .reserveCommandIdentities(value.input.claimId, [...inventory, command.commandId])
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(
        (yield* repository
          .reserveThreadRecoveryCommand({ ...reference, commandStartEffectId: "changed-start" })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      const started = yield* repository.startEffect(
        value.input.claimId,
        {
          kind: "native_command",
          phase: "started",
          effectId: reference.commandStartEffectId,
          timestamp,
          commandId: command.commandId,
          threadId,
          commandType: "thread.delete",
          commandDigest: reference.commandDigest,
        },
        value.authorize,
      );
      assert.strictEqual(started.kind, "native_command");
      assert.strictEqual(started.ordinal, cleanup.ordinal + 1);
      assert.deepEqual(yield* repository.readThreadRecoveryCommand(command.commandId), reference);
      assert.deepEqual(
        (yield* sql<{
          command_id: string;
        }>`SELECT command_id FROM native_creation_reserved_command_identities ORDER BY command_id`).map(
          (row) => row.command_id,
        ),
        [...inventory].sort(),
      );
      assert.strictEqual(
        (yield* repository.readHistoryByClaim(value.input.claimId)).effects.length,
        2,
      );
    }).pipe(Effect.provide(Layer.fresh(repositoryLayer))),
);

it.effect(
  "V2 execution requires an accepted exact native outbox claim and creates only one new start",
  () =>
    Effect.gen(function* () {
      const value = yield* acceptedV2Release();
      const { repository, sql, reference } = value;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const resolved = yield* repository.readExecutionReference(reference);
      assert.deepEqual(resolved.command, value.command);
      assert.strictEqual(
        resolved.preparation.preparationSha256,
        value.preparation.preparationSha256,
      );
      assert.strictEqual(
        (yield* repository.startEffectV2(reference, timestamp, value.authorize).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      yield* outbox.enqueue([value.pending]);
      assert.strictEqual(
        (yield* repository.startEffectV2(reference, timestamp, value.authorize).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      const claimed = Option.getOrThrow(
        yield* outbox.claimNext({ workerId: "native-worker", leaseDurationMs: 60_000 }),
      );
      assert.deepEqual(claimed.nativeCreationExecutionReference, reference);
      const denied = Effect.fail(
        new NativeCreationAuthorityError({ code: "stale_grant", message: "Revoked fixture" }),
      );
      assert.strictEqual(
        (yield* repository.startEffectV2(reference, timestamp, denied).pipe(Effect.flip)).code,
        "stale_grant",
      );
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_effect_facts`, []);
      const started = yield* repository.startEffectV2(reference, timestamp, value.authorize);
      assert.strictEqual(started.status, "started");
      assert.strictEqual(started.fact.version, 2);
      assert.strictEqual(started.fact.commandDigest, value.digest);
      assert.strictEqual(
        (yield* repository.startEffectV2(reference, timestamp, value.authorize).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      assert.strictEqual(
        (yield* repository
          .startEffectV2(
            { ...reference, effectId: "replacement-effect" },
            timestamp,
            value.authorize,
          )
          .pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.deepEqual((yield* repository.readHistoryByClaim(reference.claimId)).effectsV2, [
        started.fact,
      ]);
    }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
);

it.effect(
  "V2 execution rejects stripped, changed, expired and held outbox references before recording a start",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse(timestamp));
      const value = yield* acceptedV2Release();
      const { repository, sql, reference } = value;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      yield* outbox.enqueue([value.pending]);
      const claimed = Option.getOrThrow(
        yield* outbox.claimNext({ workerId: "native-worker", leaseDurationMs: 60_000 }),
      );
      for (const payload of [
        value.pending.request,
        { request: value.pending.request },
        {
          request: value.pending.request,
          nativeCreationExecutionReference: { ...reference, claimId: "wrong-claim" },
        },
        {
          request: { ...value.pending.request, runId: "wrong-run" },
          nativeCreationExecutionReference: reference,
        },
      ]) {
        const payloadJson = yield* encodeJson(payload);
        yield* sql`UPDATE orchestration_v2_effect_outbox SET payload_json = ${payloadJson} WHERE effect_id = ${reference.effectId}`;
        assert.strictEqual(
          (yield* repository.startEffectV2(reference, timestamp, value.authorize).pipe(Effect.flip))
            .code,
          "unresolved_claim",
        );
      }
      const boundPayloadJson = yield* encodeJson({
        request: value.pending.request,
        nativeCreationExecutionReference: reference,
      });
      yield* sql`UPDATE orchestration_v2_effect_outbox SET payload_json = ${boundPayloadJson}, lease_expires_at = '2000-01-01T00:00:00Z' WHERE effect_id = ${reference.effectId}`;
      assert.strictEqual(
        (yield* repository.startEffectV2(reference, timestamp, value.authorize).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      yield* sql`UPDATE orchestration_v2_effect_outbox SET lease_expires_at = '2099-01-01T00:00:00Z' WHERE effect_id = ${reference.effectId}`;
      assert.isTrue(
        yield* outbox.holdUnknown({
          effectId: claimed.id,
          workerId: "native-worker",
          operationId: "unknown-native",
          expectedAttempt: claimed.attemptCount,
          evidence: {
            outcome: "unknown",
            operation: "start_turn",
            operationId: "unknown-native",
            threadId: value.command.threadId,
          },
        }),
      );
      assert.strictEqual(
        (yield* repository.startEffectV2(reference, timestamp, value.authorize).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      assert.deepEqual(yield* sql`SELECT * FROM native_creation_effect_facts`, []);
    }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
);

it.effect(
  "V2 completion requires the exact committed command event and bounded history exposes safe fields only",
  () =>
    Effect.gen(function* () {
      const value = yield* nativeConfirmationFixture();
      const { repository, reference } = value;
      const started = (yield* repository.readHistoryByClaim(reference.claimId)).effectsV2[0]!;
      const eventId = value.stored.at(-1)!.event.id;
      const sequence = value.stored.at(-1)!.sequence;
      assert.strictEqual(
        (yield* repository
          .completeEffectV2(reference, {
            timestamp,
            eventId: EventId.make("event:unrelated"),
            sequence,
          })
          .pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      yield* repository.recordNativeEffectConfirmation(value.input);
      const completed = (yield* repository.readHistoryByClaim(reference.claimId)).effectsV2[1]!;
      assert.strictEqual(completed.phase, "completed");
      assert.strictEqual(
        (yield* repository
          .completeEffectV2(reference, { timestamp, eventId, sequence })
          .pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      const history = yield* repository.readBoundedHistoryByThread(value.command.threadId);
      assert.isNotNull(history);
      assert.strictEqual(history?.originalCommandId, value.command.commandId);
      assert.strictEqual(history?.threadId, value.command.threadId);
      assert.deepEqual(history?.effectsV2, [started, completed]);
      assert.deepEqual(history?.unresolvedEffects, []);
      assert.isFalse(history?.overflow);
      assert.deepEqual(history?.stageCommands[0]?.event, { eventId, sequence });
      for (const key of [
        "intent",
        "canonicalPreparation",
        "canonicalCommand",
        "preparation",
        "command",
      ]) {
        assert.isFalse(Object.hasOwn(history!, key));
      }
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "a command-attributed completed ledger fact without whole-operation confirmation stays unknown",
  () =>
    Effect.gen(function* () {
      const value = yield* nativeConfirmationFixture();
      const event = value.stored.at(-1)!;
      yield* value.repository.completeEffectV2(value.reference, {
        timestamp,
        eventId: event.event.id,
        sequence: event.sequence,
      });
      assert.strictEqual(
        (yield* value.repository
          .readBoundedHistoryByThread(value.command.threadId)
          .pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.isNull(yield* value.repository.readNativeEffectConfirmation(value.input.effectId));
      assert.strictEqual(
        Option.getOrThrow(yield* value.outbox.get(value.input.effectId)).status,
        "running",
      );
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "owning history retains every immutable fact while bounded observation reports overflow",
  () =>
    Effect.gen(function* () {
      const value = yield* acceptedV2Release();
      const { repository, sql } = value;
      yield* sql`WITH RECURSIVE facts(ordinal) AS (
      SELECT 0 UNION ALL SELECT ordinal + 1 FROM facts WHERE ordinal < 256
    ) INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json)
      SELECT ${value.input.claimId}, 'overflow-fetch:' || ordinal, 'started', ordinal,
        json_object('kind', 'fetch', 'phase', 'started', 'effectId', 'overflow-fetch:' || ordinal,
          'ordinal', ordinal, 'timestamp', ${timestamp}, 'projectCwd', ${value.input.resources.projectCwd}, 'baseRef', 'main')
      FROM facts`;
      const complete = yield* repository.readHistoryByClaim(value.input.claimId);
      assert.strictEqual(complete.effects.length, 257);
      assert.strictEqual(complete.effectsV2.length, 0);
      assert.isTrue(complete.effectOverflow);
      const bounded = yield* repository.readBoundedHistoryByThread(value.command.threadId);
      assert.isTrue(bounded?.overflow);
      assert.strictEqual(bounded?.effectsV1.length, 256);
      assert.strictEqual(bounded?.unresolvedEffects.length, 256);
      assert.isTrue((bounded?.stageCommands.length ?? 4) <= 3);
      assert.isFalse(Object.hasOwn(bounded!, "canonicalPreparation"));
    }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
);

for (const defect of ["effect-identity", "command-digest", "unmatched-completion"] as const) {
  it.effect(`bounded history rejects a persisted V2 execution with invalid ${defect}`, () =>
    Effect.gen(function* () {
      const value = yield* acceptedV2Release();
      const { repository, sql, reference } = value;
      const fact = {
        version: 2,
        kind: "native_command",
        ordinal: 0,
        timestamp,
        effectId: defect === "effect-identity" ? "fabricated-effect" : reference.effectId,
        commandId: reference.stageCommandId,
        threadId: value.command.threadId,
        commandType: "prepared-run.release",
        commandDigest: defect === "command-digest" ? "a".repeat(64) : value.digest,
        ...(defect === "unmatched-completion"
          ? { phase: "completed", eventId: "event:absent-completion", sequence: 1 }
          : { phase: "started" }),
      };
      yield* sql`INSERT INTO native_creation_effect_facts (claim_id, effect_id, phase, ordinal, fact_json)
        VALUES (${reference.claimId}, ${fact.effectId}, ${fact.phase}, 0, ${nativeCreationCanonicalJson(fact)})`;
      assert.strictEqual(
        (yield* repository.readBoundedHistoryByThread(value.command.threadId).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
    }).pipe(Effect.provide(Layer.fresh(v2RepositoryLayer))),
  );
}

it.effect(
  "commits one invocation claim under concurrent identical submissions and never replaces it",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const value = yield* fixture();
      const results = yield* Effect.all(
        Array.from({ length: 8 }, (_, index) =>
          repository.claim({ ...value.input, claimId: `request-${index}` }, value.authorize),
        ),
        { concurrency: "unbounded" },
      );
      assert.strictEqual(results.filter((result) => result.status === "claimed").length, 1);
      assert.strictEqual(results.filter((result) => result.status === "duplicate").length, 7);
      assert.strictEqual(new Set(results.map((result) => result.history.intent.claimId)).size, 1);
      assert.isNull(results[0]!.history.normalizedCommandDigest);
      const old = results[0]!.history.intent;
      const duplicate = yield* repository.claim(
        {
          ...value.input,
          claimedBootId: "later-boot",
          claimedAt: "2099-01-01T00:00:00Z",
          grantRevision: 2,
        },
        value.authorize,
      );
      assert.deepEqual(duplicate.history.intent, old);
      assert.strictEqual(duplicate.status, "duplicate");
      const observed = yield* repository.readHistory(value.preparation.command.commandId);
      assert.isTrue(Option.isSome(observed));
      assert.deepEqual(Option.getOrThrow(observed).intent, old);
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("reads exact permanent enrollment membership and rejects malformed present markers", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    assert.isFalse(yield* repository.hasAutomationEnrollment(enrolledSessionId));
    yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at)
    VALUES (${enrolledSessionId}, ${timestamp})`;
    assert.isTrue(yield* repository.hasAutomationEnrollment(enrolledSessionId));
    assert.isFalse(
      yield* repository.hasAutomationEnrollment(AuthSessionId.make("other-native-session")),
    );
    yield* sql`INSERT INTO native_creation_automation_enrollments (session_id, enrolled_at)
    VALUES ('malformed-native-session', 'not-a-timestamp')`;
    assert.strictEqual(
      (yield* repository
        .hasAutomationEnrollment(AuthSessionId.make("malformed-native-session"))
        .pipe(Effect.flip)).code,
      "unresolved_claim",
    );
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("missing native membership table is unknown rather than an absent marker", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    assert.strictEqual(
      (yield* repository.hasAutomationEnrollment(enrolledSessionId).pipe(Effect.flip)).code,
      "unresolved_claim",
    );
  }).pipe(Effect.provide(layer.pipe(Layer.provide(memory)))),
);

it.effect(
  "rejects changed immutable intent, competing path and duplicate identities without partial claims",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const sql = yield* SqlClient.SqlClient;
      const value = yield* fixture();
      yield* repository.claim(value.input, value.authorize);
      const changed = yield* fixture("fixture-operation", "Changed prompt");
      const otherPath = yield* fixture("other-operation");
      const otherClaimId = yield* fixture(
        "third-operation",
        "Synthetic prompt",
        "/fixture/other-worktree",
      );
      const wrongBranch = {
        ...value.input,
        claimId: "other-claim",
        resources: { ...value.input.resources, branch: "other-branch" },
      };
      for (const [input, authorize] of [
        [changed.input, changed.authorize],
        [otherPath.input, otherPath.authorize],
        [{ ...otherClaimId.input, claimId: value.input.claimId }, otherClaimId.authorize],
        [wrongBranch, value.authorize],
      ] as const) {
        assert.strictEqual(
          (yield* repository.claim(input, authorize).pipe(Effect.flip)).code,
          "conflict",
        );
      }
      assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM native_creation_intents`, [
        { count: 1 },
      ]);
      assert.isTrue(Option.isNone(yield* repository.readHistory("missing-command")));
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("authority denial creates no claim or started effect", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    const denied = Effect.fail(
      new NativeCreationAuthorityError({ code: "stale_grant", message: "Synthetic revoked grant" }),
    );
    assert.strictEqual(
      (yield* repository.claim(value.input, denied).pipe(Effect.flip)).code,
      "stale_grant",
    );
    for (const changed of [
      { backendInstance: "other-backend" },
      { environmentId: "other-environment" },
      { accountRef: "other-account" },
      { runSetupScript: true },
      {
        providerModelSelection: {
          ...value.historical.providerModelSelection,
          model: "other-model",
        },
      },
    ]) {
      assert.strictEqual(
        (yield* repository
          .claim(value.input, Effect.succeed({ ...value.historical, ...changed }))
          .pipe(Effect.flip)).code,
        "conflict",
      );
    }
    assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM native_creation_intents`, [
      { count: 0 },
    ]);
    yield* repository.claim(value.input, value.authorize);
    const fact = {
      kind: "fetch" as const,
      phase: "started" as const,
      effectId: "fixture-fetch",
      timestamp,
      projectCwd: value.input.resources.projectCwd,
      baseRef: "main",
    };
    assert.strictEqual(
      (yield* repository.startEffect(value.input.claimId, fact, denied).pipe(Effect.flip)).code,
      "stale_grant",
    );
    assert.deepEqual(yield* sql`SELECT COUNT(*) AS count FROM native_creation_effect_facts`, [
      { count: 0 },
    ]);
    const persisted = yield* repository.startEffect(value.input.claimId, fact, value.authorize);
    const history = Option.getOrThrow(
      yield* repository.readHistory(value.preparation.command.commandId),
    );
    assert.deepEqual(history.effects, [persisted]);
    assert.strictEqual(persisted.ordinal, 0);
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("keeps original digest separate and reserves immutable native commands", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const command = releaseCommand(value.preparation);
    yield* repository.reserveCommandIdentities(value.input.claimId, [command.commandId]);
    yield* repository.recordNormalizedCommand(value.input.claimId, command);
    yield* repository.recordNormalizedCommand(value.input.claimId, command);
    const history = Option.getOrThrow(
      yield* repository.readHistory(value.preparation.command.commandId),
    );
    assert.strictEqual(history.intent.commandDigest, value.preparation.commandDigest);
    assert.strictEqual(
      history.normalizedCommandDigest,
      nativeCreationSha256(nativeCreationCanonicalJson(command)),
    );
    assert.notStrictEqual(history.normalizedCommandDigest, history.intent.commandDigest);
    const reserved = Option.getOrThrow(yield* repository.getReservedCommand(command.commandId));
    assert.strictEqual(reserved.claimId, value.input.claimId);
    assert.strictEqual(reserved.commandDigest, history.normalizedCommandDigest);
    if (command.type !== "prepared-run.release")
      return yield* Effect.die("Fixture command type changed");
    assert.strictEqual(
      (yield* repository
        .recordNormalizedCommand(value.input.claimId, {
          ...command,
          runId: RunId.make("changed-prepared-run"),
        })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    const second = yield* fixture("other-operation", "Synthetic prompt", "/fixture/other-worktree");
    yield* repository.claim(second.input, second.authorize);
    assert.strictEqual(
      (yield* repository
        .reserveCommand(second.input.claimId, {
          ...command,
          threadId: ThreadId.make(second.preparation.command.threadId),
        })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    const future = yield* fixture(
      "future-operation",
      "Synthetic prompt",
      "/fixture/future-worktree",
    );
    assert.strictEqual(
      (yield* repository
        .reserveCommand(value.input.claimId, {
          ...command,
          commandId: CommandId.make(future.preparation.command.commandId),
        })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual(
      (yield* repository
        .reserveCommandIdentities(value.input.claimId, [
          command.commandId,
          future.preparation.command.commandId,
        ])
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual((yield* repository.claim(future.input, future.authorize)).status, "claimed");
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("orders append-only typed facts and preserves external gaps and failed results", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const started = {
      kind: "fetch" as const,
      phase: "started" as const,
      effectId: "fixture-fetch",
      timestamp,
      projectCwd: value.input.resources.projectCwd,
      baseRef: "main",
    };
    const completed = { ...started, phase: "completed" as const, result: "failed" as const };
    assert.strictEqual(
      (yield* repository.completeEffect(value.input.claimId, completed).pipe(Effect.flip)).code,
      "conflict",
    );
    const persistedStart = yield* repository.startEffect(
      value.input.claimId,
      started,
      value.authorize,
    );
    assert.strictEqual(
      (yield* repository
        .startEffect(value.input.claimId, started, value.authorize)
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual(
      (yield* repository
        .completeEffect(value.input.claimId, { ...completed, baseRef: "other" })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.deepEqual(
      Option.getOrThrow(yield* repository.readHistory(value.preparation.command.commandId)).effects,
      [persistedStart],
    );
    const persistedCompletion = yield* repository.completeEffect(value.input.claimId, completed);
    assert.deepEqual(
      Option.getOrThrow(yield* repository.readHistory(value.preparation.command.commandId)).effects,
      [persistedStart, persistedCompletion],
    );
    assert.strictEqual(persistedStart.ordinal, 0);
    assert.strictEqual(persistedCompletion.ordinal, 1);
    assert.strictEqual(
      (yield* repository.completeEffect(value.input.claimId, completed).pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual(
      (yield* repository
        .startEffect(
          value.input.claimId,
          { ...started, effectId: "next", projectCwd: "/other/project" },
          value.authorize,
        )
        .pipe(Effect.flip)).code,
      "conflict",
    );
    yield* repository.startEffect(
      value.input.claimId,
      {
        kind: "setup",
        phase: "started",
        effectId: "fixture-setup",
        timestamp,
        worktreePath: value.input.resources.worktreePath,
        terminalId: "known-terminal",
      },
      value.authorize,
    );
    assert.strictEqual(
      (yield* repository
        .completeEffect(value.input.claimId, {
          kind: "setup",
          phase: "completed",
          effectId: "fixture-setup",
          timestamp,
          worktreePath: value.input.resources.worktreePath,
          terminalId: "other-terminal",
          exitCode: 0,
          result: "succeeded",
        })
        .pipe(Effect.flip)).code,
      "conflict",
    );
    yield* repository.completeEffect(value.input.claimId, {
      kind: "setup",
      phase: "completed",
      effectId: "fixture-setup",
      timestamp,
      worktreePath: value.input.resources.worktreePath,
      terminalId: "known-terminal",
      exitCode: 0,
      result: "succeeded",
    });
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("fact completion joins an enclosing engine transaction and rolls back with it", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const started = {
      kind: "lifecycle" as const,
      phase: "started" as const,
      effectId: "fixture-normalization",
      timestamp,
      threadId: ThreadId.make(value.preparation.command.threadId),
      action: "normalization" as const,
    };
    const persistedStart = yield* repository.startEffect(
      value.input.claimId,
      started,
      value.authorize,
    );
    const completed = { ...started, phase: "completed" as const, result: "succeeded" as const };
    yield* sql`CREATE TABLE synthetic_engine_receipts (command_id TEXT PRIMARY KEY)`;
    const aborted = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO synthetic_engine_receipts (command_id) VALUES ('synthetic-command')`;
          yield* repository.completeEffect(value.input.claimId, completed);
          return yield* Effect.fail("synthetic-transaction-abort");
        }),
      )
      .pipe(Effect.flip);
    assert.strictEqual(aborted, "synthetic-transaction-abort");
    assert.deepEqual(yield* sql`SELECT * FROM synthetic_engine_receipts`, []);
    assert.deepEqual(
      Option.getOrThrow(yield* repository.readHistory(value.preparation.command.commandId)).effects,
      [persistedStart],
    );
    yield* repository.completeEffect(value.input.claimId, completed);
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("assigns distinct fact ordinals when asynchronous completions arrive concurrently", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const starts = yield* Effect.all(
      ["first", "second"].map((effectId) =>
        repository.startEffect(
          value.input.claimId,
          {
            kind: "lifecycle",
            phase: "started",
            effectId,
            timestamp,
            threadId: ThreadId.make(value.preparation.command.threadId),
            action: "git_status_refresh",
          },
          value.authorize,
        ),
      ),
      { concurrency: "unbounded" },
    );
    const completions = yield* Effect.all(
      starts.map((fact) => {
        if (fact.kind !== "lifecycle") return Effect.die("Fixture lifecycle fact changed");
        const { ordinal: _ordinal, ...started } = fact;
        return repository.completeEffect(value.input.claimId, {
          ...started,
          phase: "completed",
          result: "succeeded",
        });
      }),
      { concurrency: "unbounded" },
    );
    assert.deepEqual(starts.map((fact) => fact.ordinal).sort(), [0, 1]);
    assert.deepEqual(completions.map((fact) => fact.ordinal).sort(), [2, 3]);
    assert.deepEqual(
      Option.getOrThrow(
        yield* repository.readHistory(value.preparation.command.commandId),
      ).effects.map((fact) => fact.ordinal),
      [0, 1, 2, 3],
    );
  }).pipe(Effect.provide(repositoryLayer)),
);

const identityInventory = (commandId: string) => [
  commandId,
  ...[
    "bootstrap-thread-create",
    "bootstrap-thread-message",
    "bootstrap-thread-preparing",
    "bootstrap-thread-meta-update",
    "bootstrap-thread-preparing-failed",
    "bootstrap-thread-delete",
    "setup-script-requested",
    "setup-script-started",
    "setup-script-failed",
    "worktree-setup-running",
    "worktree-setup-done",
    "worktree-setup-failed",
    "worktree-setup-cancelled",
  ].map((suffix) => `${commandId}:${suffix}`),
];

it.effect(
  "reserves the whole closed inventory without inventing bodies and binds real bodies later",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const value = yield* fixture();
      yield* repository.claim(value.input, value.authorize);
      const ids = identityInventory(value.preparation.command.commandId);
      yield* repository.reserveCommandIdentities(value.input.claimId, ids);
      yield* repository.reserveCommandIdentities(value.input.claimId, [...ids].reverse());
      for (const commandId of ids) {
        assert.deepEqual(
          Option.getOrThrow(yield* repository.getReservedCommandIdentity(commandId)),
          {
            claimId: value.input.claimId,
            commandId,
            threadId: value.preparation.command.threadId,
          },
        );
        assert.isTrue(Option.isNone(yield* repository.getReservedCommand(commandId)));
      }
      const command = releaseCommand(value.preparation);
      yield* repository.recordNormalizedCommand(value.input.claimId, command);
      const body = Option.getOrThrow(yield* repository.getReservedCommand(command.commandId));
      assert.strictEqual(body.canonicalCommand, nativeCreationCanonicalJson(command));
      const history = yield* repository.readHistoryByClaim(value.input.claimId);
      assert.deepEqual(
        history,
        Option.getOrThrow(yield* repository.readHistory(command.commandId)),
      );
      assert.strictEqual(history.normalizedCommandDigest, body.commandDigest);
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("invalid inventories and unowned bodies reject without reserving any IDs", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const id = value.preparation.command.commandId;
    for (const ids of [
      [],
      [id, id],
      [id, ""],
      [id, " padded "],
      [id, `${id}:bootstrap-arbitrary`],
      [`${id}:bootstrap-thread-create`],
    ])
      assert.strictEqual(
        (yield* repository.reserveCommandIdentities(value.input.claimId, ids).pipe(Effect.result))
          ._tag,
        "Failure",
      );
    assert.strictEqual(
      (yield* repository
        .reserveCommand(
          value.input.claimId,
          yield* Schema.decodeEffect(OrchestrationV2Command)(releaseCommand(value.preparation)),
        )
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.deepEqual(
      yield* sql`SELECT command_id FROM native_creation_reserved_command_identities`,
      [],
    );
    assert.deepEqual(yield* sql`SELECT command_id FROM native_creation_reserved_commands`, []);
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("native receipts collide before whole-inventory reservation or claim insertion", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    const ids = identityInventory(value.preparation.command.commandId);
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
      VALUES (${ids[ids.length - 1]!}, 'thread', ${value.preparation.command.threadId}, ${timestamp}, 1, 'accepted')`;
    assert.strictEqual(
      (yield* repository.reserveCommandIdentities(value.input.claimId, ids).pipe(Effect.flip)).code,
      "conflict",
    );
    assert.deepEqual(
      yield* sql`SELECT command_id FROM native_creation_reserved_command_identities`,
      [],
    );
    const other = yield* fixture(
      "receipt-operation",
      "Synthetic prompt",
      "/fixture/receipt-worktree",
    );
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
      VALUES (${other.preparation.command.commandId}, 'thread', ${other.preparation.command.threadId}, ${timestamp}, 2, 'accepted')`;
    assert.strictEqual(
      (yield* repository.claim(other.input, other.authorize).pipe(Effect.flip)).code,
      "conflict",
    );
    assert.isTrue(
      Option.isNone(yield* repository.readHistory(other.preparation.command.commandId)),
    );
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("different claims cannot bind or reserve another owner's commands", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const first = yield* fixture();
    const second = yield* fixture(
      "second-operation",
      "Synthetic prompt",
      "/fixture/second-worktree",
    );
    yield* repository.claim(first.input, first.authorize);
    yield* repository.claim(second.input, second.authorize);
    const ids = identityInventory(first.preparation.command.commandId);
    yield* repository.reserveCommandIdentities(first.input.claimId, ids);
    assert.strictEqual(
      (yield* repository
        .reserveCommandIdentities(second.input.claimId, [
          second.preparation.command.commandId,
          ids[1]!,
        ])
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.strictEqual(
      (yield* repository
        .reserveCommand(
          second.input.claimId,
          releaseCommand({
            command: {
              commandId: first.preparation.command.commandId,
              threadId: second.preparation.command.threadId,
            },
          }),
        )
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.deepEqual(
      yield* sql`SELECT COUNT(*) AS count FROM native_creation_reserved_command_identities`,
      [{ count: ids.length }],
    );
  }).pipe(Effect.provide(repositoryLayer)),
);

it.effect(
  "a new repository instance preserves identity ownership and cannot extend partial reservations",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const sql = yield* SqlClient.SqlClient;
      const value = yield* fixture();
      const claimed = yield* repository.claim(value.input, value.authorize);
      const ids = identityInventory(value.preparation.command.commandId);
      yield* sql`INSERT INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id)
      VALUES (${ids[0]!}, ${value.input.claimId}, ${value.preparation.command.threadId})`;
      const restarted = yield* make;
      assert.strictEqual(
        (yield* restarted.reserveCommandIdentities(value.input.claimId, ids).pipe(Effect.flip))
          .code,
        "unresolved_claim",
      );
      const duplicate = yield* restarted.claim(
        { ...value.input, claimId: "takeover-claim", claimedBootId: "later-boot" },
        value.authorize,
      );
      assert.strictEqual(duplicate.status, "duplicate");
      assert.deepEqual(duplicate.history.intent, claimed.history.intent);
      assert.deepEqual(
        yield* sql`SELECT claim_id FROM native_creation_reserved_command_identities`,
        [{ claim_id: value.input.claimId }],
      );
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect(
  "missing or malformed identity lookup remains unresolved, while a valid absence is empty",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const sql = yield* SqlClient.SqlClient;
      const value = yield* fixture();
      yield* repository.claim(value.input, value.authorize);
      const id = value.preparation.command.commandId;
      assert.isTrue(Option.isNone(yield* repository.getReservedCommandIdentity(id)));
      yield* sql`INSERT INTO native_creation_reserved_command_identities (command_id, claim_id, thread_id)
      VALUES (${id}, ${value.input.claimId}, 'wrong-thread')`;
      assert.strictEqual(
        (yield* repository.getReservedCommandIdentity(id).pipe(Effect.flip)).code,
        "unresolved_claim",
      );
      assert.strictEqual(
        (yield* repository.readHistoryByClaim("missing-claim").pipe(Effect.flip)).code,
        "unresolved_claim",
      );
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("an unavailable identity table fails closed", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    assert.strictEqual(
      (yield* repository.getReservedCommandIdentity("fixture-command").pipe(Effect.flip)).code,
      "unresolved_claim",
    );
  }).pipe(Effect.provide(layer.pipe(Layer.provide(memory)))),
);

it.effect(
  "historical activity command facts bind immutable V1 bodies and preserve repeated fact rejection",
  () =>
    Effect.gen(function* () {
      const repository = yield* NativeCreationRepository;
      const value = yield* fixture();
      yield* repository.claim(value.input, value.authorize);
      yield* repository.reserveCommandIdentities(
        value.input.claimId,
        identityInventory(value.preparation.command.commandId),
      );
      const command = {
        type: "thread.activity.append" as const,
        commandId: CommandId.make(`${value.preparation.command.commandId}:worktree-setup-done`),
        threadId: ThreadId.make(value.preparation.command.threadId),
        activity: {
          id: "fixture-native-activity",
          tone: "info",
          kind: "worktree.setup.done",
          summary: "Setup completed",
          payload: { exitCode: 0, terminalId: "real-fixture-terminal" },
          turnId: null,
          createdAt: "2026-10-02T12:36:00Z",
        },
        createdAt: "2026-10-02T12:36:00Z",
      };
      const sql = yield* SqlClient.SqlClient;
      // Stored V1 bodies remain historical evidence; the live reserve API only accepts V2.
      const canonical = nativeCreationCanonicalJson(command);
      yield* sql`INSERT INTO native_creation_reserved_commands
        (command_id, claim_id, thread_id, command_type, command_digest, canonical_command)
        VALUES (${command.commandId}, ${value.input.claimId}, ${command.threadId}, ${command.type},
          ${nativeCreationSha256(canonical)}, ${canonical})`;
      const reserved = Option.getOrThrow(yield* repository.getReservedCommand(command.commandId));
      const start = {
        kind: "native_command" as const,
        phase: "started" as const,
        effectId: "fixture-activity-command",
        timestamp: "2026-10-02T12:36:01Z",
        commandId: command.commandId,
        threadId: ThreadId.make(value.preparation.command.threadId),
        commandType: "thread.activity.append" as const,
        commandDigest: reserved.commandDigest,
      };
      yield* repository.startEffect(value.input.claimId, start, value.authorize);
      assert.strictEqual(
        (yield* repository
          .startEffect(value.input.claimId, start, value.authorize)
          .pipe(Effect.flip)).code,
        "conflict",
      );
      const completed = {
        ...start,
        phase: "completed" as const,
        timestamp: "2026-10-02T12:36:02Z",
        eventId: EventId.make("fixture-native-event"),
        sequence: 5,
      };
      assert.strictEqual(
        (yield* repository
          .completeEffect(value.input.claimId, { ...completed, commandDigest: "0".repeat(64) })
          .pipe(Effect.flip)).code,
        "conflict",
      );
      yield* repository.completeEffect(value.input.claimId, completed);
      assert.strictEqual(
        (yield* repository.completeEffect(value.input.claimId, completed).pipe(Effect.flip)).code,
        "conflict",
      );
      assert.strictEqual(
        (yield* repository
          .reserveCommand(
            value.input.claimId,
            // @ts-expect-error Historical V1 activity bodies cannot enter live V2 reservation.
            { ...command, createdAt: "2026-10-02T12:37:00Z" },
          )
          .pipe(Effect.flip)).code,
        "conflict",
      );
      const history = yield* repository.readHistoryByClaim(value.input.claimId);
      assert.strictEqual(history.effects.length, 2);
      assert.deepEqual(
        yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(
          reserved.canonicalCommand,
        ),
        command,
      );
    }).pipe(Effect.provide(repositoryLayer)),
);

it.effect("a receipt appearing after identity reservation prevents later body binding", () =>
  Effect.gen(function* () {
    const repository = yield* NativeCreationRepository;
    const sql = yield* SqlClient.SqlClient;
    const value = yield* fixture();
    yield* repository.claim(value.input, value.authorize);
    yield* repository.reserveCommandIdentities(
      value.input.claimId,
      identityInventory(value.preparation.command.commandId),
    );
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
      VALUES (${value.preparation.command.commandId}, 'thread', ${value.preparation.command.threadId}, ${timestamp}, 1, 'accepted')`;
    assert.strictEqual(
      (yield* repository
        .recordNormalizedCommand(
          value.input.claimId,
          yield* Schema.decodeEffect(OrchestrationV2Command)(releaseCommand(value.preparation)),
        )
        .pipe(Effect.flip)).code,
      "conflict",
    );
    assert.isTrue(
      Option.isNone(yield* repository.getReservedCommand(value.preparation.command.commandId)),
    );
    assert.isNull(
      (yield* repository.readHistoryByClaim(value.input.claimId)).normalizedCommandDigest,
    );
  }).pipe(Effect.provide(repositoryLayer)),
);
