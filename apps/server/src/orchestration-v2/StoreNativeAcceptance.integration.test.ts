import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";
import { toSafeThreadAttachmentSegment } from "../attachmentStore.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ImportedAttachments from "./ImportedApplicationAttachmentInventory.ts";

const database = SqlitePersistenceMemory;
const stores = Layer.mergeAll(
  database,
  EventStore.layer.pipe(Layer.provide(database)),
  ProjectionStore.layer.pipe(Layer.provide(database)),
);
const persistence = EventSink.layer.pipe(Layer.provideMerge(stores));
const timestamp = "2026-10-05T00:00:00.000Z";
const threadId = ThreadId.make("reader-namespace");
const commandId = CommandId.make("command:reader:delete");
const effectId = `effect:${commandId}:attachment.cleanup`;
const workerId = "worker:reader:original";
const laterWorkerId = "worker:reader:reconcile";
const digest = (value: unknown) => nativeCreationSha256(nativeCreationCanonicalJson(value));
const json = nativeCreationCanonicalJson;

// These SQL carriers qualify persisted readers; they do not issue a cleanup producer capability.
const fixture = Effect.fnUntraced(function* () {
  yield* TestClock.setTime(Date.parse(timestamp));
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const instanceId = ProviderInstanceId.make("codex");
  const thread: OrchestrationV2AppThread = {
    id: threadId,
    projectId: ProjectId.make("project:reader"),
    title: "Reader fixture",
    providerInstanceId: instanceId,
    modelSelection: { instanceId, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
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
  const birthId = EventId.make("event:reader:birth");
  const birth = (yield* sink.write({
    events: [{ id: birthId, type: "thread.created", threadId, occurredAt: now, payload: thread }],
  }))[0]!;
  const ownerBirth = {
    kind: "application_v2_thread_birth" as const,
    threadId,
    eventId: birthId,
    sequence: birth.sequence,
  };
  const triggerEventId = EventId.make("event:reader:deleted");
  const deletion = yield* sink.commitCommand({
    commandId,
    commandType: "thread.delete",
    threadId,
    acceptedAt: now,
    effects: [],
    events: [
      {
        id: triggerEventId,
        type: "thread.deleted",
        threadId,
        occurredAt: now,
        payload: { ...thread, deletedAt: now },
      },
    ],
  });
  const reference = {
    version: 1 as const,
    mode: "delete_thread" as const,
    ownerBirth,
    triggerEventId,
  };
  yield* sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count, available_at,
     lease_owner, lease_expires_at, created_at, updated_at)
    VALUES (${effectId}, ${commandId}, ${threadId}, 'attachment.cleanup',
    ${json({ request: { type: "attachment.cleanup", attachmentIds: [] }, attachmentNamespaceCleanup: reference })},
    'running', 1, ${timestamp}, ${workerId}, '2026-10-05T00:01:00.000Z', ${timestamp}, ${timestamp})`;
  const taskSubject = {
    version: 1 as const,
    effectId,
    commandId,
    threadId,
    reference,
    triggerSequence: deletion.storedEvents[0]!.sequence,
  };
  const task = { ...taskSubject, bindingSha256: digest(taskSubject) };
  const segment = toSafeThreadAttachmentSegment(threadId)!;
  const firstPath = `${segment}-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.bin`;
  const secondPath = `${segment}-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.bin`;
  const basis = {
    status: "ready" as const,
    task,
    claim: { workerId, expectedAttempt: 1, leaseExpiresAt: "2026-10-05T00:01:00.000Z" },
    basisEventSequence: deletion.storedEvents[0]!.sequence,
    retainedRelativePaths: [] as ReadonlyArray<string>,
  };
  const observation = {
    version: 1 as const,
    producer: "attachment_namespace" as const,
    effectId,
    bindingSha256: task.bindingSha256,
    workerId,
    expectedAttempt: 1,
    basisEventSequence: basis.basisEventSequence,
    configuredRoot: "/synthetic/reader-attachments",
    namespaceSegment: segment,
    observedAt: timestamp,
    outcome: {
      status: "completed" as const,
      matchingPaths: [firstPath],
      removedPaths: [firstPath],
      retainedPaths: [] as ReadonlyArray<string>,
      rootAbsent: false,
    },
  };
  const row = {
    effect_id: effectId,
    ordinal: 0,
    binding_sha256: task.bindingSha256,
    canonical_task_json: json(task),
    observation_json: json(observation),
    correlation_json: json({ version: 1, basis, observationSha256: digest(observation) }),
    recorded_at: timestamp,
  };
  const read = sink.readAttachmentNamespaceCleanupObservation;
  const unresolved = sink.readUnresolvedDeletionCleanupHolds;
  if (read === undefined || unresolved === undefined)
    return yield* Effect.die("Actual private readers missing");
  return { sql, sink, now, task, basis, observation, row, firstPath, secondPath, read, unresolved };
});

const insertHold = (
  bindingSha256: string,
  options: { readonly heldWorker?: string; readonly taskKind?: string } = {},
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_v2_unknown_effect_holds
      (effect_id, worker_id, operation_id, evidence_json, expected_attempt, held_at)
      VALUES (${effectId}, ${options.heldWorker ?? workerId}, ${effectId}, ${json({
        version: 1,
        kind: "resource_cleanup",
        operationId: effectId,
        threadId,
        taskKind: options.taskKind ?? "attachment",
        bindingSha256,
        outcome: "unknown",
      })}, 1, ${timestamp})`;
  });

const terminalFixture = Effect.fnUntraced(function* () {
  const value = yield* fixture();
  const terminalEffectId = `effect:${commandId}:terminal.cleanup`;
  const ownerBirth = value.task.reference.ownerBirth;
  const capture = {
    managerId: "manager:original",
    threadId,
    ownerBirth,
    status: "captured",
    managedTargetsOnly: true,
    targets: [
      { threadId, terminalId: "terminal:original", handleId: "handle:original", ownerBirth },
    ],
  };
  const lease = {
    resourcePath: "/synthetic/original-checkout",
    leaseId: "lease:original",
    ownerThreadId: threadId,
    ownerIncarnation: json([
      "t3.orchestration-v2.thread-birth/v1",
      ownerBirth.eventId,
      ownerBirth.sequence,
    ]),
    branch: null,
    acquiredAtMs: Date.parse(timestamp),
    renewedAtMs: Date.parse(timestamp),
    expiresAtMs: Date.parse(timestamp) + 60_000,
  };
  const deletion = {
    commandId,
    eventId: value.task.reference.triggerEventId,
    sequence: value.task.triggerSequence,
  };
  const task = { kind: "terminal", capture };
  const subject = {
    version: 2,
    effectId: terminalEffectId,
    threadId,
    lease,
    ownerBirth,
    deletion,
    task,
  };
  const bindingSha256 = digest(subject);
  yield* value.sql`INSERT INTO orchestration_v2_effect_outbox
    (effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,created_at,updated_at,completed_at)
    VALUES (${terminalEffectId},${commandId},${threadId},'terminal.cleanup','{"type":"terminal.cleanup"}','succeeded',1,${timestamp},${timestamp},${timestamp},${timestamp})`;
  const bindingRow = {
    effect_id: terminalEffectId,
    thread_id: threadId,
    lease_json: json(lease),
    owner_birth_json: json(ownerBirth),
    deletion_json: json(deletion),
    task_json: json(task),
    binding_sha256: bindingSha256,
    recorded_at: timestamp,
  };
  const evidence = {
    version: 1,
    kind: "resource_cleanup",
    operationId: terminalEffectId,
    threadId,
    taskKind: "terminal",
    bindingSha256,
    outcome: "unknown",
  };
  yield* value.sql`INSERT INTO orchestration_v2_unknown_effect_holds
    (effect_id,worker_id,operation_id,evidence_json,expected_attempt,held_at)
    VALUES (${terminalEffectId},${workerId},${terminalEffectId},${json(evidence)},1,${timestamp})`;
  const hold = {
    effectId: terminalEffectId,
    threadId,
    workerId,
    operationId: terminalEffectId,
    evidence,
    expectedAttempt: 1,
    heldAt: timestamp,
  };
  const observation = {
    version: 1,
    kind: "managed_terminal",
    effectId: terminalEffectId,
    bindingSha256,
    workerId,
    expectedAttempt: 1,
    capture,
    result: {
      status: "closed",
      managedTargetsOnly: true,
      processExitObserved: true,
      descendantsQuiescence: "unavailable",
      futureWakeClosure: "unavailable",
    },
    observedAt: timestamp,
  };
  const outcome = { taskId: terminalEffectId, result: "succeeded", effect: "confirmed" };
  const correlation = {
    workerId,
    expectedAttempt: 1,
    bindingSha256,
    evidence: {
      version: 1,
      schema: "t3.deletion-cleanup-observation/v1",
      producer: "managed_terminal",
      observation,
      coveredHolds: [hold],
    },
  };
  const outcomeRow = {
    effect_id: terminalEffectId,
    ordinal: 0,
    outcome_json: json(outcome),
    correlation_json: json(correlation),
    recorded_at: timestamp,
  };
  return { ...value, terminalEffectId, bindingRow, outcomeRow, correlation, observation, hold };
});

it.effect(
  "finite terminal read requalifies the original lease/task and exact stored hold without clearing audit",
  () =>
    Effect.gen(function* () {
      const value = yield* terminalFixture();
      yield* value.sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${value.sql.insert(value.bindingRow)}`;
      yield* value.sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${value.sql.insert(value.outcomeRow)}`;
      const holds = yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`;
      assert.deepEqual(yield* value.unresolved(threadId), []);
      yield* TestClock.adjust("2 minutes");
      assert.deepEqual(yield* value.unresolved(threadId), []);
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`,
        holds,
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_lease_cleanup_task_outcomes`,
        [value.outcomeRow],
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect.each([
  "binding digest",
  "task owner",
  "task handle",
  "birth",
  "receipt",
  "observation binding",
  "observation capture",
  "producer",
  "worker",
  "attempt",
  "outcome",
  "covered hold",
])("finite terminal read denies %s mismatch and preserves the exact hold", (variant) =>
  Effect.gen(function* () {
    const value = yield* terminalFixture();
    const binding = {
      ...value.bindingRow,
      ...(variant === "binding digest" ? { binding_sha256: "f".repeat(64) } : {}),
      ...(variant === "task owner"
        ? { owner_birth_json: json({ ...value.task.reference.ownerBirth, eventId: "event:other" }) }
        : {}),
      ...(variant === "task handle"
        ? {
            task_json: json({
              kind: "terminal",
              capture: {
                ...value.observation.capture,
                targets: [
                  { ...value.observation.capture.targets[0], handleId: "handle:replacement" },
                ],
              },
            }),
          }
        : {}),
    };
    const observation = {
      ...value.observation,
      ...(variant === "observation binding" ? { bindingSha256: "b".repeat(64) } : {}),
      ...(variant === "observation capture"
        ? { capture: { ...value.observation.capture, managerId: "manager:replacement" } }
        : {}),
    };
    const correlation = {
      ...value.correlation,
      ...(variant === "worker" ? { workerId: "worker:copied" } : {}),
      ...(variant === "attempt" ? { expectedAttempt: 2 } : {}),
      evidence: {
        ...value.correlation.evidence,
        observation,
        ...(variant === "producer" ? { producer: "managed_provider" } : {}),
        ...(variant === "covered hold"
          ? { coveredHolds: [{ ...value.hold, heldAt: "2026-10-05T00:00:01.000Z" }] }
          : {}),
      },
    };
    const outcome = {
      ...value.outcomeRow,
      correlation_json: json(correlation),
      ...(variant === "outcome"
        ? {
            outcome_json: json({
              taskId: value.terminalEffectId,
              result: "succeeded",
              effect: "absent",
            }),
          }
        : {}),
    };
    yield* value.sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${value.sql.insert(binding)}`;
    yield* value.sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${value.sql.insert(outcome)}`;
    if (variant === "birth")
      yield* value.sql`UPDATE orchestration_events SET event_type = 'thread.updated' WHERE event_id = ${value.task.reference.ownerBirth.eventId}`;
    if (variant === "receipt")
      yield* value.sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
    const holds = yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`;
    assert.equal((yield* Effect.result(value.unresolved(threadId)))._tag, "Failure");
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`, holds);
  }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect.each(["missing outcome", "unknown observation", "uncovered hold"])(
  "finite terminal read retains %s instead of inferring generic terminal success",
  (variant) =>
    Effect.gen(function* () {
      const value = yield* terminalFixture();
      yield* value.sql`INSERT INTO orchestration_v2_lease_cleanup_task_bindings ${value.sql.insert(value.bindingRow)}`;
      if (variant !== "missing outcome") {
        const observation =
          variant === "unknown observation"
            ? {
                ...value.observation,
                result: {
                  ...value.observation.result,
                  status: "unknown",
                  processExitObserved: false,
                },
              }
            : value.observation;
        const correlation = {
          ...value.correlation,
          evidence: {
            ...value.correlation.evidence,
            observation,
            ...(variant === "uncovered hold" ? { coveredHolds: [] } : {}),
          },
        };
        yield* value.sql`INSERT INTO orchestration_v2_lease_cleanup_task_outcomes ${value.sql.insert(
          {
            ...value.outcomeRow,
            correlation_json: json(correlation),
            ...(variant === "unknown observation"
              ? {
                  outcome_json: json({
                    taskId: value.terminalEffectId,
                    result: null,
                    effect: "unknown",
                  }),
                }
              : {}),
          },
        )}`;
      }
      assert.deepEqual(
        (yield* value.unresolved(threadId)).map((hold) => hold.effectId),
        [value.terminalEffectId],
      );
      assert.equal(
        (yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`).length,
        1,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect("namespace reader reopens exact history without changing its immutable carriers", () =>
  Effect.gen(function* () {
    const value = yield* fixture();
    yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(value.row)}`;
    const before = yield* value.sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`;
    const recorded = yield* value.read(effectId);
    assert.equal(recorded?.status, "completed");
    assert.deepEqual(recorded?.task, value.task);
    assert.deepEqual(recorded?.observation, value.observation);
    yield* TestClock.adjust("2 minutes");
    assert.deepEqual(yield* value.read(effectId), recorded);
    assert.deepEqual(
      yield* value.sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`,
      before,
    );
  }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect.each([
  "missing receipt",
  "wrong receipt",
  "wrong birth",
  "wrong trigger",
  "native envelope",
  "wrong effect subject",
])("namespace reader refuses original task with %s", (variant) =>
  Effect.gen(function* () {
    const value = yield* fixture();
    yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(value.row)}`;
    if (variant === "missing receipt")
      yield* value.sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${commandId}`;
    if (variant === "wrong receipt")
      yield* value.sql`UPDATE orchestration_command_receipts SET command_type = 'checkpoint.rollback' WHERE command_id = ${commandId}`;
    if (variant === "wrong birth")
      yield* value.sql`UPDATE orchestration_events SET event_type = 'thread.updated' WHERE event_id = ${value.task.reference.ownerBirth.eventId}`;
    if (variant === "wrong trigger")
      yield* value.sql`UPDATE orchestration_events SET command_id = 'command:unrelated' WHERE event_id = ${value.task.reference.triggerEventId}`;
    if (variant === "native envelope")
      yield* value.sql`UPDATE orchestration_v2_effect_outbox SET payload_json = ${json({
        request: { type: "attachment.cleanup", attachmentIds: [] },
        attachmentNamespaceCleanup: value.task.reference,
        nativeCreationExecutionReference: {},
      })} WHERE effect_id = ${effectId}`;
    if (variant === "wrong effect subject")
      yield* value.sql`UPDATE orchestration_v2_effect_outbox SET thread_id = 'unrelated' WHERE effect_id = ${effectId}`;
    const audit = yield* value.sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`;
    assert.equal((yield* Effect.result(value.read(effectId)))._tag, "Failure");
    assert.deepEqual(
      yield* value.sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`,
      audit,
    );
  }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect.each([
  "ordinal gap",
  "task",
  "basis task",
  "binding",
  "recorded timestamp",
  "digest",
  "correlation excess",
])("namespace reader refuses immutable %s mismatch", (variant) =>
  Effect.gen(function* () {
    const value = yield* fixture();
    const row = {
      ...value.row,
      ...(variant === "ordinal gap" ? { ordinal: 1 } : {}),
      ...(variant === "task"
        ? { canonical_task_json: json({ ...value.task, commandId: "command:other" }) }
        : {}),
      ...(variant === "binding" ? { binding_sha256: "b".repeat(64) } : {}),
      ...(variant === "recorded timestamp" ? { recorded_at: "2026-10-05T00:00:01.000Z" } : {}),
      ...(variant === "basis task"
        ? {
            correlation_json: json({
              version: 1,
              basis: { ...value.basis, task: { ...value.task, triggerSequence: 9 } },
              observationSha256: digest(value.observation),
            }),
          }
        : {}),
      ...(variant === "digest"
        ? {
            correlation_json: json({
              version: 1,
              basis: value.basis,
              observationSha256: "c".repeat(64),
            }),
          }
        : {}),
      ...(variant === "correlation excess"
        ? {
            correlation_json: json({
              version: 1,
              basis: value.basis,
              observationSha256: digest(value.observation),
              grant: true,
            }),
          }
        : {}),
    };
    yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(row)}`;
    assert.equal((yield* Effect.result(value.read(effectId)))._tag, "Failure");
    assert.deepEqual(
      yield* value.sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`,
      [row],
    );
  }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect.each([
  "worker",
  "attempt",
  "sequence",
  "root",
  "namespace",
  "duplicate scan",
  "unscanned removal",
  "retained removal",
  "root absent",
  "stale status",
  "unknown retained removal",
  "retry retained removal",
])("namespace reader refuses finite scan %s mismatch", (variant) =>
  Effect.gen(function* () {
    const value = yield* fixture();
    const retained = [value.firstPath];
    const basis = {
      ...value.basis,
      retainedRelativePaths: retained,
      retentionSourceEvidence:
        ImportedAttachments.makeImportedApplicationAttachmentRetentionEvidenceV1({
          segments: [],
          visibleV2CarrierSetSha256: digest([]),
          relativePaths: retained,
        }),
    };
    const observation = {
      ...value.observation,
      ...(variant === "worker" ? { workerId: "worker:other" } : {}),
      ...(variant === "attempt" ? { expectedAttempt: 2 } : {}),
      ...(variant === "sequence" ? { basisEventSequence: value.basis.basisEventSequence + 1 } : {}),
      ...(variant === "root" ? { configuredRoot: "/synthetic/../reader" } : {}),
      ...(variant === "namespace" ? { namespaceSegment: "another" } : {}),
      outcome:
        variant === "duplicate scan"
          ? { ...value.observation.outcome, matchingPaths: [value.firstPath, value.firstPath] }
          : variant === "unscanned removal"
            ? { ...value.observation.outcome, removedPaths: [value.secondPath] }
            : variant === "root absent"
              ? { ...value.observation.outcome, rootAbsent: true }
              : variant === "stale status"
                ? { status: "stale" }
                : variant === "unknown retained removal"
                  ? { status: "unknown", removedPaths: [value.firstPath], reason: "lost" }
                  : variant === "retry retained removal"
                    ? {
                        status: "retryable_failure",
                        removedPaths: [value.firstPath],
                        remainingPaths: [value.secondPath],
                        reason: "partial",
                      }
                    : value.observation.outcome,
    };
    const retainedBasis = [
      "retained removal",
      "unknown retained removal",
      "retry retained removal",
    ].includes(variant)
      ? basis
      : value.basis;
    const row = {
      ...value.row,
      observation_json: json(observation),
      correlation_json: json({
        version: 1,
        basis: retainedBasis,
        observationSha256: digest(observation),
      }),
    };
    yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(row)}`;
    assert.equal((yield* Effect.result(value.read(effectId)))._tag, "Failure");
  }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "namespace reader rehashes exact retained references and preserves valid retained paths",
  () =>
    Effect.gen(function* () {
      const value = yield* fixture();
      const basis = {
        ...value.basis,
        retainedRelativePaths: [value.firstPath],
        retentionSourceEvidence:
          ImportedAttachments.makeImportedApplicationAttachmentRetentionEvidenceV1({
            segments: [],
            visibleV2CarrierSetSha256: digest([]),
            relativePaths: [value.firstPath],
          }),
      };
      const observation = {
        ...value.observation,
        outcome: {
          status: "completed" as const,
          matchingPaths: [value.firstPath, value.secondPath],
          removedPaths: [value.secondPath],
          retainedPaths: [value.firstPath],
          rootAbsent: false,
        },
      };
      const row = {
        ...value.row,
        observation_json: json(observation),
        correlation_json: json({ version: 1, basis, observationSha256: digest(observation) }),
      };
      yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(row)}`;
      assert.deepEqual((yield* value.read(effectId))?.observation.outcome, observation.outcome);
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "namespace reader denies changed retained-reference evidence even with a matching observation digest",
  () =>
    Effect.gen(function* () {
      const value = yield* fixture();
      const basis = {
        ...value.basis,
        retainedRelativePaths: [value.firstPath],
        retentionSourceEvidence:
          ImportedAttachments.makeImportedApplicationAttachmentRetentionEvidenceV1({
            segments: [],
            visibleV2CarrierSetSha256: digest([]),
            relativePaths: [],
          }),
      };
      const observation = {
        ...value.observation,
        outcome: {
          status: "completed" as const,
          matchingPaths: [value.firstPath],
          removedPaths: [],
          retainedPaths: [value.firstPath],
          rootAbsent: false,
        },
      };
      yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(
        {
          ...value.row,
          observation_json: json(observation),
          correlation_json: json({ version: 1, basis, observationSha256: digest(observation) }),
        },
      )}`;
      assert.equal((yield* Effect.result(value.read(effectId)))._tag, "Failure");
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect.each([
  "none",
  "pending",
  "no completion",
  "wrong worker",
  "wrong binding",
  "wrong kind",
  "no original unknown",
])("namespace cleanup eligibility conservatively retains %s proof", (variant) =>
  Effect.gen(function* () {
    const value = yield* fixture();
    yield* insertHold(variant === "wrong binding" ? "a".repeat(64) : value.task.bindingSha256, {
      heldWorker: variant === "wrong worker" ? "worker:copied" : workerId,
      taskKind: variant === "wrong kind" ? "terminal" : "attachment",
    });
    const before = yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`;
    if (variant !== "none") {
      if (variant !== "no original unknown") {
        const unknown = {
          ...value.observation,
          outcome: { status: "unknown", removedPaths: [], reason: "lost original response" },
        };
        yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(
          {
            ...value.row,
            observation_json: json(unknown),
            correlation_json: json({
              version: 1,
              basis: value.basis,
              observationSha256: digest(unknown),
            }),
          },
        )}`;
      }
      const basis = {
        ...value.basis,
        claim: { ...value.basis.claim, workerId: laterWorkerId, expectedAttempt: 2 },
      };
      const observed = { ...value.observation, workerId: laterWorkerId, expectedAttempt: 2 };
      yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(
        {
          ...value.row,
          ordinal: variant === "no original unknown" ? 0 : 1,
          observation_json: json(observed),
          correlation_json: json({ version: 1, basis, observationSha256: digest(observed) }),
        },
      )}`;
      yield* value.sql`UPDATE orchestration_v2_effect_outbox SET status = ${variant === "pending" ? "pending" : "succeeded"}, completed_at = ${variant === "no completion" ? null : timestamp} WHERE effect_id = ${effectId}`;
    }
    assert.equal((yield* value.unresolved(threadId)).length, 1);
    assert.deepEqual(yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`, before);
  }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "namespace eligible completion keeps immutable unknown audit and all unrelated holds",
  () =>
    Effect.gen(function* () {
      const value = yield* fixture();
      yield* insertHold(value.task.bindingSha256);
      const unknown = {
        ...value.observation,
        outcome: { status: "unknown", removedPaths: [], reason: "original result lost" },
      };
      yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(
        {
          ...value.row,
          observation_json: json(unknown),
          correlation_json: json({
            version: 1,
            basis: value.basis,
            observationSha256: digest(unknown),
          }),
        },
      )}`;
      const basis = {
        ...value.basis,
        claim: { ...value.basis.claim, workerId: laterWorkerId, expectedAttempt: 2 },
      };
      const observation = { ...value.observation, workerId: laterWorkerId, expectedAttempt: 2 };
      yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert(
        {
          ...value.row,
          ordinal: 1,
          observation_json: json(observation),
          correlation_json: json({ version: 1, basis, observationSha256: digest(observation) }),
        },
      )}`;
      yield* value.sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded', completed_at = ${timestamp} WHERE effect_id = ${effectId}`;
      const before = yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`;
      assert.deepEqual(yield* value.unresolved(threadId), []);
      const unrelatedId = "effect:unrelated:terminal.cleanup";
      yield* value.sql`INSERT INTO orchestration_v2_effect_outbox
      (effect_id,command_id,thread_id,effect_type,payload_json,status,attempt_count,available_at,created_at,updated_at)
      VALUES (${unrelatedId},'command:unrelated',${threadId},'terminal.cleanup','{"type":"terminal.cleanup"}','succeeded',1,${timestamp},${timestamp},${timestamp})`;
      yield* value.sql`INSERT INTO orchestration_v2_unknown_effect_holds
      (effect_id,worker_id,operation_id,evidence_json,expected_attempt,held_at)
      VALUES (${unrelatedId},${workerId},${unrelatedId},${json({
        version: 1,
        kind: "resource_cleanup",
        operationId: unrelatedId,
        threadId,
        taskKind: "terminal",
        bindingSha256: null,
        reason: "task_binding_unavailable",
        outcome: "unknown",
      })},1,${timestamp})`;
      assert.deepEqual(
        (yield* value.unresolved(threadId)).map((hold) => hold.effectId),
        [unrelatedId],
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds WHERE effect_id = ${effectId}`,
        before,
      );
      assert.equal(
        (yield* value.sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`).length,
        2,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);
