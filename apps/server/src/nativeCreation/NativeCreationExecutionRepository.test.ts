import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeOS from "node:os";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../persistence/Migrations.ts";
import { assert, it } from "@effect/vitest";
import {
  NativeCreationHistoricalBinding,
  OrchestrationV2Command,
  CommandId,
  RunId,
  ThreadId,
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
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import {
  NativePreparationBinding,
  nativeCreationCanonicalJson,
  nativeCreationSha256,
  nativePreparationCommand,
  validateNativeCreationPreparation,
} from "./NativeCreationPreparation.ts";
import {
  NativeCreationRepository,
  type NativeCreationClaimInput,
} from "./NativeCreationRepository.ts";
import * as LegacyRepositorySqlite from "./NativeCreationRepositorySqlite.ts";
import {
  NativeCreationExecutionRepository,
  layer as executionLayer,
} from "./NativeCreationExecutionRepository.ts";
const layerForDatabase = (database: typeof SqlitePersistenceMemory) => {
  const confirmationStores = Layer.mergeAll(
    database,
    EventStore.layer.pipe(Layer.provide(database)),
    ProjectionStore.layer.pipe(Layer.provide(database)),
    CommandReceiptStore.layer.pipe(Layer.provide(database)),
    EffectOutbox.layer.pipe(Layer.provide(database)),
    LegacyRepositorySqlite.layer.pipe(Layer.provide(database)),
    executionLayer.pipe(Layer.provide(database)),
  );
  return EventSink.layer.pipe(Layer.provideMerge(confirmationStores));
};
const confirmationLayer = layerForDatabase(SqlitePersistenceMemory);
const timestamp = "2026-10-02T12:34:56Z";
const decodeFixtureBinding = Schema.decodeUnknownSync(NativePreparationBinding);
const decodeFixtureHistory = Schema.decodeUnknownSync(NativeCreationHistoricalBinding);
const decodeFixtureCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
const releaseCommand = (preparation: {
  readonly command: { readonly commandId: string; readonly threadId: string };
}) =>
  decodeFixtureCommand({
    type: "prepared-run.release",
    commandId: CommandId.make(preparation.command.commandId),
    threadId: ThreadId.make(preparation.command.threadId),
    runId: RunId.make("fixture-prepared-run"),
  });
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
  const repository = yield* NativeCreationExecutionRepository;
  const legacyRepository = yield* NativeCreationRepository;
  const sql = yield* SqlClient.SqlClient;
  const value = yield* fixture("fixture-v2-execution", messageText);
  yield* legacyRepository.claim(value.input, value.authorize);
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
  if (sink.registerProviderRuntime === undefined)
    return yield* Effect.die("Actual EventSink native runtime registration capability missing");
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

it.effect("generic terminal outbox success cannot manufacture native confirmation", () =>
  Effect.gen(function* () {
    const value = yield* nativeConfirmationFixture();
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded', lease_owner = NULL,
      lease_expires_at = NULL, completed_at = ${timestamp} WHERE effect_id = ${value.input.effectId}`;
    assert.isNull(yield* value.repository.readNativeEffectConfirmation(value.input.effectId));
    assert.deepEqual(
      (yield* value.repository.readHistoryByClaim(value.reference.claimId)).effectsV2.map(
        (fact) => fact.phase,
      ),
      ["started"],
    );
    assert.strictEqual(
      (yield* value.repository.recordNativeEffectConfirmation(value.input).pipe(Effect.flip)).code,
      "unresolved_claim",
    );
  }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect("confirmation reader denies a terminal claim with a different original attempt", () =>
  Effect.gen(function* () {
    const value = yield* nativeConfirmationFixture();
    yield* value.repository.recordNativeEffectConfirmation(value.input);
    yield* value.sql`UPDATE orchestration_v2_effect_outbox SET attempt_count = attempt_count + 1 WHERE effect_id = ${value.input.effectId}`;
    assert.strictEqual(
      (yield* value.repository.readNativeEffectConfirmation(value.input.effectId).pipe(Effect.flip))
        .code,
      "unresolved_claim",
    );
  }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "registered runtime generation rotates only through its exact original revision and attempt",
  () =>
    Effect.gen(function* () {
      const value = yield* nativeConfirmationFixture();
      const sink = yield* EventSink.EventSinkV2;
      if (
        sink.registerProviderRuntime === undefined ||
        sink.readProviderRuntimeEvidence === undefined
      )
        return yield* Effect.die("Actual EventSink runtime registration capability missing");
      const previous = Option.getOrThrow(
        Option.fromNullishOr(yield* sink.readProviderRuntimeEvidence(value.input.binding.threadId)),
      );
      const rotated = {
        expectedBinding: previous.binding,
        expectedEvidenceRevision: previous.evidenceRevision,
        actualBinding: { ...value.input.binding, runtimeGeneration: "fixture-next-generation" },
        expectedRunId: value.input.runId,
        expectedRunAttemptId: value.input.attemptId,
      };
      for (const [input, rejection] of [
        [
          { ...rotated, expectedEvidenceRevision: previous.evidenceRevision + 1 },
          "evidence_revision_mismatch",
        ],
        [
          {
            ...rotated,
            expectedRegisteredBinding: {
              ...previous.binding,
              runtimeGeneration: "fixture-wrong-generation",
            },
          },
          "unregistered_generation",
        ],
        [
          { ...rotated, expectedRunAttemptId: RunAttemptId.make("fixture-wrong-attempt") },
          "attempt_mismatch",
        ],
      ] as const) {
        assert.deepEqual(yield* sink.registerProviderRuntime(input), {
          committed: false,
          rejection,
          storedEvents: [],
        });
        assert.deepEqual(
          yield* sink.readProviderRuntimeEvidence(value.input.binding.threadId),
          previous,
        );
      }
      assert.deepEqual(yield* sink.registerProviderRuntime(rotated), {
        committed: true,
        evidenceRevision: previous.evidenceRevision + 1,
        storedEvents: [],
      });
      const current = Option.getOrThrow(
        Option.fromNullishOr(yield* sink.readProviderRuntimeEvidence(value.input.binding.threadId)),
      );
      assert.strictEqual(current.binding.runtimeGeneration, "fixture-next-generation");
      assert.strictEqual(current.evidenceRevision, previous.evidenceRevision + 1);
      assert.deepEqual(yield* sink.registerProviderRuntime(rotated), {
        committed: false,
        rejection: "evidence_revision_mismatch",
        storedEvents: [],
      });
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
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "runtime continuation provenance is immutable and cannot be rebound to another source",
  () =>
    Effect.gen(function* () {
      const value = yield* nativeConfirmationFixture();
      const sink = yield* EventSink.EventSinkV2;
      if (
        sink.registerProviderRuntime === undefined ||
        sink.readProviderRuntimeEvidence === undefined
      )
        return yield* Effect.die("Actual EventSink runtime registration capability missing");
      const previous = Option.getOrThrow(
        Option.fromNullishOr(yield* sink.readProviderRuntimeEvidence(value.input.binding.threadId)),
      );
      const input = {
        expectedBinding: previous.binding,
        expectedEvidenceRevision: previous.evidenceRevision,
        actualBinding: value.input.binding,
        actualContinuationSourceIdentity: {
          driverKind: previous.binding.driver,
          continuationKey: "fixture-continuation-key",
          runtimeGeneration: value.input.binding.runtimeGeneration,
        },
      };
      assert.deepEqual(yield* sink.registerProviderRuntime(input), {
        committed: true,
        evidenceRevision: previous.evidenceRevision + 1,
        storedEvents: [],
      });
      const current = Option.getOrThrow(
        Option.fromNullishOr(yield* sink.readProviderRuntimeEvidence(value.input.binding.threadId)),
      );
      const before = yield* value.sql`SELECT * FROM orchestration_v2_provider_continuation_sources`;
      assert.strictEqual(before.length, 1);
      assert.deepEqual(
        yield* sink.registerProviderRuntime({
          ...input,
          expectedEvidenceRevision: current.evidenceRevision,
          actualContinuationSourceIdentity: {
            ...input.actualContinuationSourceIdentity,
            continuationKey: "fixture-other-key",
          },
        }),
        { committed: false, rejection: "binding_mismatch", storedEvents: [] },
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_provider_continuation_sources`,
        before,
      );
      assert.deepEqual(
        yield* sink.readProviderRuntimeEvidence(value.input.binding.threadId),
        current,
      );
    }).pipe(Effect.provide(Layer.fresh(confirmationLayer))),
);

it.effect(
  "native confirmation and its exact lineage reopen from persisted bytes without new authority",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          directory: process.env.TMPDIR ?? NodeOS.tmpdir(),
          prefix: "native-confirmation-fixture-",
        });
        const database = Layer.effectDiscard(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql`PRAGMA foreign_keys = ON`;
            yield* runMigrations();
          }),
        ).pipe(
          Layer.provideMerge(
            NodeSqliteClient.layer({ filename: path.join(directory, "fixture.sqlite") }),
          ),
        );
        const reopenedLayer = layerForDatabase(database);
        const recorded = yield* Effect.gen(function* () {
          const value = yield* nativeConfirmationFixture();
          const before = yield* value.sql<{
            intent_json: string;
          }>`SELECT intent_json FROM native_creation_intents WHERE claim_id = ${value.reference.claimId}`;
          const proof = yield* value.repository.recordNativeEffectConfirmation(value.input);
          assert.deepEqual(
            yield* value.sql`SELECT intent_json FROM native_creation_intents WHERE claim_id = ${value.reference.claimId}`,
            before,
          );
          return {
            proof,
            intentBytes: before[0]!.intent_json,
            claimId: value.reference.claimId,
            reference: value.reference,
          };
        }).pipe(Effect.provide(Layer.fresh(reopenedLayer)));
        yield* Effect.gen(function* () {
          const repository = yield* NativeCreationExecutionRepository;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(
            yield* repository.readNativeEffectConfirmation(recorded.proof.effectId),
            recorded.proof,
          );
          const effect = Option.getOrThrow(yield* outbox.get(recorded.proof.effectId));
          assert.strictEqual(effect.status, "succeeded");
          assert.isNull(effect.leaseOwner);
          assert.isNull(effect.leaseExpiresAt);
          assert.deepEqual(effect.nativeCreationExecutionReference, recorded.reference);
          assert.deepEqual(
            (yield* repository.readHistoryByClaim(recorded.claimId)).effectsV2.map(
              (fact) => fact.phase,
            ),
            ["started", "completed"],
          );
          assert.deepEqual(
            yield* sql`SELECT intent_json FROM native_creation_intents WHERE claim_id = ${recorded.claimId}`,
            [{ intent_json: recorded.intentBytes }],
          );
        }).pipe(Effect.provide(Layer.fresh(reopenedLayer)));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
