import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
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
import {
  SqlitePersistenceMemory,
  makeSqlitePersistenceLive,
} from "../persistence/Layers/Sqlite.ts";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeOS from "node:os";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";
import { toSafeThreadAttachmentSegment } from "../attachmentStore.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ImportedAttachments from "./ImportedApplicationAttachmentInventory.ts";
import { makeCommitTransaction } from "./CommitTransaction.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";

const database = SqlitePersistenceMemory;
const stores = Layer.mergeAll(
  database,
  EventStore.layer.pipe(Layer.provide(database)),
  ProjectionStore.layer.pipe(Layer.provide(database)),
);
const currentSourceReader = Layer.effect(
  EventSink.LegacyCurrentSourceReader,
  LegacyV1ThreadImporter.makeLegacyCurrentSourceReader,
).pipe(Layer.provide(database));
const persistence = EventSink.layer.pipe(
  Layer.provideMerge(Layer.merge(stores, currentSourceReader)),
);
const timestamp = "2026-10-05T00:00:00.000Z";
const threadId = ThreadId.make("reader-namespace");
const commandId = CommandId.make("command:reader:delete");
const effectId = `effect:${commandId}:attachment.cleanup`;
const workerId = "worker:reader:original";
const laterWorkerId = "worker:reader:reconcile";
const digest = (value: unknown) => nativeCreationSha256(nativeCreationCanonicalJson(value));
const json = nativeCreationCanonicalJson;

// These SQL carriers qualify persisted readers; they do not issue a cleanup producer capability.
const fixture = Effect.fnUntraced(function* (options: { readonly retainFirst?: boolean } = {}) {
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
  if (options.retainFirst) {
    const attachmentId = `${toSafeThreadAttachmentSegment(threadId)}-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa`;
    yield* sink.write({
      events: [
        {
          id: EventId.make("event:reader:retained-message"),
          type: "message.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: MessageId.make("message:reader:retained"),
            threadId,
            runId: null,
            nodeId: null,
            role: "user",
            text: "Actually retained reference",
            attachments: [
              {
                type: "file",
                id: attachmentId,
                name: "retained.bin",
                mimeType: "application/octet-stream",
                sizeBytes: 10,
              },
            ],
            streaming: false,
            createdBy: "user",
            creationSource: "web",
            createdAt: now,
            updatedAt: now,
          },
        },
      ],
    });
  }
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
  const retainedReader = sink.readThreadRetainedAttachmentPaths;
  if (retainedReader === undefined) return yield* Effect.die("Authentic retention reader missing");
  const retained = yield* retainedReader(threadId);
  if (retained.status !== "complete")
    return yield* Effect.die(`Actual retention fixture unavailable: ${retained.reason}`);
  const basis = {
    status: "ready" as const,
    task,
    claim: { workerId, expectedAttempt: 1, leaseExpiresAt: "2026-10-05T00:01:00.000Z" },
    basisEventSequence: deletion.storedEvents[0]!.sequence,
    retainedRelativePaths: retained.relativePaths,
    retentionSourceEvidence: retained.sourceEvidence,
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
    const value = yield* fixture({ retainFirst: true });
    const retained = [value.firstPath];
    const basis = {
      ...value.basis,
      retainedRelativePaths: retained,
      retentionSourceEvidence:
        ImportedAttachments.makeImportedApplicationAttachmentRetentionEvidenceV1({
          segments: [],
          visibleV2CarrierSetSha256: value.basis.retentionSourceEvidence.visibleV2CarrierSetSha256,
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
      const value = yield* fixture({ retainFirst: true });
      const basis = {
        ...value.basis,
        retainedRelativePaths: [value.firstPath],
        retentionSourceEvidence:
          ImportedAttachments.makeImportedApplicationAttachmentRetentionEvidenceV1({
            segments: [],
            visibleV2CarrierSetSha256:
              value.basis.retentionSourceEvidence.visibleV2CarrierSetSha256,
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
      const value = yield* fixture({ retainFirst: true });
      const basis = {
        ...value.basis,
        retainedRelativePaths: [value.firstPath],
        retentionSourceEvidence:
          ImportedAttachments.makeImportedApplicationAttachmentRetentionEvidenceV1({
            segments: [],
            visibleV2CarrierSetSha256:
              value.basis.retentionSourceEvidence.visibleV2CarrierSetSha256,
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

const importedApplicationInventoryFixture = Effect.fnUntraced(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const sql = yield* SqlClient.SqlClient;
  const now = yield* DateTime.now;
  const projectId = ProjectId.make("project:reader:inventory");
  const instanceId = ProviderInstanceId.make("codex");
  const owner = yield* makeCommitTransaction();
  const prepare = sink.prepareImportedApplicationAttachmentInventory;
  const read = sink.readImportedApplicationAttachmentInventory;
  const readBirth = sink.readApplicationBirthRecord;
  if (prepare === undefined || read === undefined || readBirth === undefined)
    return yield* Effect.die("Authentic inventory owner missing");
  const createdAt = DateTime.formatIso(now);
  const legacyBirthId = EventId.make("event:application-inventory:legacy-birth");
  const legacyPayload = {
    threadId,
    projectId,
    title: "Imported application",
    modelSelection: { instanceId, model: "fixture-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt,
    updatedAt: createdAt,
  };
  yield* sql`INSERT INTO orchestration_events
    (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
    VALUES (${legacyBirthId}, 'thread', ${threadId}, 1, 'thread.created', ${createdAt}, 'user', ${json(legacyPayload)}, '{}', 1)`;
  yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, created_at, updated_at, deleted_at)
    VALUES (${threadId}, ${projectId}, 'Imported application', ${createdAt}, ${createdAt}, NULL)`;
  const app: OrchestrationV2AppThread = {
    id: threadId,
    projectId,
    title: "Imported application",
    providerInstanceId: instanceId,
    modelSelection: { instanceId, model: "fixture-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "server",
    historyOrigin: "v1_import",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make(`migration:v1:thread:${threadId}:created`),
        threadId,
        type: "thread.created",
        occurredAt: now,
        payload: app,
      },
    ],
  });
  const birth = (yield* readBirth(threadId))!;
  assert.isNotNull(birth);
  const legacySequence = (yield* sql<{
    readonly sequence: number;
  }>`SELECT sequence FROM orchestration_events WHERE event_id = ${legacyBirthId}`)[0]!.sequence;
  for (const projector of [
    "projection.threads",
    "projection.thread-messages",
    "projection.thread-activities",
    "projection.thread-turns",
  ])
    yield* sql`INSERT INTO projection_state (projector, last_applied_sequence, updated_at) VALUES (${projector}, ${legacySequence}, ${createdAt})
      ON CONFLICT(projector) DO UPDATE SET last_applied_sequence = excluded.last_applied_sequence, updated_at = excluded.updated_at`;
  const file = {
    type: "file",
    id: "application-retained-file",
    name: "notes.TXT",
    mimeType: "text/plain",
    sizeBytes: 10,
  } as const;
  for (const role of ["system", "user", "assistant"])
    yield* sql`INSERT INTO projection_thread_messages
    (message_id, thread_id, turn_id, role, text, is_streaming, created_at, updated_at, attachments_json)
    VALUES (${`message:application:${role}`}, ${threadId}, NULL, ${role}, 'Original text stays in the source row', 0,
      ${createdAt}, ${createdAt}, ${role === "system" ? json([file]) : null})`;
  const answer = {
    requestId: "request:application:duplicate",
    questionTextById: { first: "Preserved question" },
    answers: { first: "yes" },
    attachmentsByQuestionId: { first: [{ ...file, id: "application-answer-file" }] },
  };
  for (const [id, payload] of [
    ["answer:application:one", answer],
    ["answer:application:two", { ...answer, attachmentsByQuestionId: {} }],
  ] as const)
    yield* sql`INSERT INTO projection_thread_activities (activity_id, thread_id, turn_id, tone, kind, summary, payload_json, created_at, sequence)
      VALUES (${id}, ${threadId}, NULL, 'info', 'user-input.answer-submitted', 'Answered', ${json(payload)}, ${createdAt}, NULL)`;
  return {
    sink,
    owner,
    prepare,
    read,
    sql,
    birth,
    app,
    now,
    createdAt,
    legacySequence,
    input: { threadId, expectedBirth: birth },
  };
});

it.effect(
  "adopts all retained imported message roles and distinct answer rows with exact immutable SQL parity",
  () =>
    Effect.gen(function* () {
      const value = yield* importedApplicationInventoryFixture();
      const result = yield* value.prepare(value.input);
      assert.strictEqual(result.status, "complete");
      if (result.status !== "complete") return;
      assert.strictEqual(result.inventory.header.messageCarrierCount, 3);
      assert.strictEqual(result.inventory.header.answerCarrierCount, 2);
      assert.strictEqual(result.inventory.header.attachmentReferenceCount, 2);
      assert.deepEqual(
        result.inventory.carriers
          .filter((row) => row.kind === "legacy_message")
          .map((row) => row.role)
          .sort(),
        ["assistant", "system", "user"],
      );
      const answers = result.inventory.carriers.filter((row) => row.kind === "legacy_answer");
      assert.deepEqual(
        answers.map((row) => row.answer.requestId),
        ["request:application:duplicate", "request:application:duplicate"],
      );
      assert.deepEqual(answers[0]!.answer.questionTextById, { first: "Preserved question" });
      const paths = ImportedAttachments.collectImportedApplicationAttachmentPathsV1(
        result.inventory.carriers,
      );
      assert.strictEqual(paths.status, "complete");
      if (paths.status === "complete")
        assert.deepEqual(paths.relativePaths, [
          "application-answer-file.txt",
          "application-retained-file.txt",
        ]);
      assert.deepEqual(yield* value.read(value.input), result);
      assert.strictEqual(
        (yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`)
          .length,
        5,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "inventory adoption rolls back all rows and preserves its original header across unrelated cursor churn",
  () =>
    Effect.gen(function* () {
      const value = yield* importedApplicationInventoryFixture();
      const aborted = yield* value.owner
        .withTransaction(
          Effect.gen(function* () {
            assert.strictEqual((yield* value.prepare(value.input)).status, "complete");
            return yield* Effect.fail("abort inventory adoption");
          }),
        )
        .pipe(Effect.result);
      assert.strictEqual(aborted._tag, "Failure");
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
        [],
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`,
        [],
      );
      const original = yield* value.prepare(value.input);
      yield* TestClock.adjust("1 minute");
      yield* value.sql`UPDATE projection_state SET last_applied_sequence = last_applied_sequence + 20`;
      assert.deepEqual(yield* value.prepare(value.input), original);
      assert.strictEqual(
        (yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`)
          .length,
        1,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "materialized legacy removal appends an adoption while malformed and partial sources never become complete zero",
  () =>
    Effect.gen(function* () {
      const value = yield* importedApplicationInventoryFixture();
      yield* value.sql`UPDATE projection_state SET last_applied_sequence = 0 WHERE projector = 'projection.thread-activities'`;
      assert.deepEqual(yield* value.prepare(value.input), {
        status: "unavailable",
        reason: "legacy_application_source_cut_incomplete",
      });
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
        [],
      );
      yield* value.sql`UPDATE projection_state SET last_applied_sequence = ${value.legacySequence}`;
      const original = yield* value.prepare(value.input);
      assert.strictEqual(original.status, "complete");
      yield* value.sql`DELETE FROM projection_thread_activities WHERE activity_id = 'answer:application:one'`;
      const changed = yield* value.prepare(value.input);
      assert.strictEqual(changed.status, "complete");
      if (original.status === "complete" && changed.status === "complete") {
        assert.notStrictEqual(
          changed.inventory.header.inventoryId,
          original.inventory.header.inventoryId,
        );
        assert.strictEqual(changed.inventory.header.answerCarrierCount, 1);
        assert.strictEqual(changed.inventory.header.attachmentReferenceCount, 1);
        assert.deepEqual(
          yield* value.read({
            ...value.input,
            inventoryId: original.inventory.header.inventoryId,
          }),
          original,
        );
      }
      yield* value.sql`UPDATE projection_thread_activities SET payload_json = '{malformed' WHERE activity_id = 'answer:application:two'`;
      assert.deepEqual(yield* value.prepare(value.input), {
        status: "unavailable",
        reason: "carrier_decode_unavailable",
      });
      assert.strictEqual(
        (yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`)
          .length,
        2,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "same-time legacy recreation cannot replace the source birth of a retained imported inventory",
  () =>
    Effect.gen(function* () {
      const value = yield* importedApplicationInventoryFixture();
      const original = yield* value.prepare(value.input);
      yield* value.sql`INSERT INTO orchestration_events
      (event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version)
      SELECT 'event:application-inventory:replacement', aggregate_kind, stream_id,
        (SELECT max(stream_version) + 1 FROM orchestration_events WHERE stream_id = ${threadId} AND aggregate_kind = 'thread'),
        event_type, occurred_at, actor_kind, payload_json, metadata_json, application_event_version FROM orchestration_events
        WHERE event_id = 'event:application-inventory:legacy-birth'`;
      assert.deepEqual(yield* value.prepare(value.input), {
        status: "unavailable",
        reason: "legacy_application_source_birth_unavailable",
      });
      assert.deepEqual(yield* value.read(value.input), original);
      assert.strictEqual(
        (yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`)
          .length,
        1,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "inventory readers deny a copied header row without rewriting either immutable adoption",
  () =>
    Effect.gen(function* () {
      const value = yield* importedApplicationInventoryFixture();
      const original = yield* value.prepare(value.input);
      assert.equal(original.status, "complete");
      if (original.status !== "complete") return;
      const rows =
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`;
      const copiedId = "a".repeat(64);
      yield* value.sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories ${value.sql.insert({ ...rows[0]!, inventory_id: copiedId, adoption_ordinal: 1 })}`;
      const before =
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories ORDER BY adoption_ordinal`;
      assert.deepEqual(yield* value.read({ ...value.input, inventoryId: copiedId }), {
        status: "unavailable",
        reason: "inventory_header_row_parity_unavailable",
      });
      assert.deepEqual(yield* value.prepare(value.input), {
        status: "unavailable",
        reason: "inventory_header_row_parity_unavailable",
      });
      assert.deepEqual(
        yield* value.read({ ...value.input, inventoryId: original.inventory.header.inventoryId }),
        original,
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories ORDER BY adoption_ordinal`,
        before,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect.each([142, 144])(
  "inventory reader and issuer refuse invalid own migration %s provenance before any feature mutation",
  (missing) =>
    Effect.gen(function* () {
      const value = yield* importedApplicationInventoryFixture();
      assert.equal((yield* value.prepare(value.input)).status, "complete");
      const headers =
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`;
      const carriers =
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`;
      // Deliberately corrupt only this disposable registered loader's ledger; it must confer no authority.
      yield* value.sql`DELETE FROM jones_sql_migrations WHERE migration_id=${missing}`;
      const ledger = yield* value.sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`;
      assert.deepEqual(yield* value.read(value.input), {
        status: "unavailable",
        reason: "imported_application_inventory_schema_unavailable",
      });
      assert.deepEqual(yield* value.prepare(value.input), {
        status: "unavailable",
        reason: "imported_application_inventory_schema_unavailable",
      });
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
        headers,
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`,
        carriers,
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
        ledger,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "syntactically matching native inventory storage cannot substitute for the missing authentic native seal producer",
  () =>
    Effect.gen(function* () {
      const value = yield* importedApplicationInventoryFixture();
      const legacy = yield* value.prepare(value.input);
      assert.equal(legacy.status, "complete");
      if (legacy.status !== "complete") return;
      const source: ImportedAttachments.ImportedApplicationAttachmentSourceV1 = {
        kind: "native_import_batch",
        parserPolicy: "agent_session_visible_messages_v1",
        birth: value.birth,
        source: {
          provider: "codex",
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerSessionId: "session:unissued",
          filePath: "/synthetic/unissued.jsonl",
          size: 100,
          mtimeMs: null,
          device: 1,
          inode: null,
          birthtimeMs: null,
        },
        eventsSha256: digest("unissued stored bytes"),
        messageCount: 2,
        eventBasis: [0, 1, 2, 3].map((index) => ({
          eventId: EventId.make(`event:unissued:${index}`),
          sequence: value.birth.sequence + index + 1,
        })),
      };
      const empty = ImportedAttachments.collectImportedApplicationAttachmentPathsV1([]);
      if (empty.status !== "complete") return yield* Effect.die("Empty carrier paths failed");
      const { inventoryId: _id, recordedAt, ...prior } = legacy.inventory.header;
      const identity = {
        ...prior,
        source,
        sourceHistoryCoverage: "native_visible_message_subset" as const,
        messageCarrierCount: 2,
        answerCarrierCount: 0,
        attachmentReferenceCount: 0,
        carrierSetSha256: empty.carrierSetSha256,
      };
      const header = {
        ...identity,
        inventoryId: ImportedAttachments.makeImportedApplicationAttachmentInventoryIdV1(identity),
        recordedAt,
      };
      assert.equal(
        ImportedAttachments.qualifyImportedApplicationAttachmentSnapshotV1({ header, carriers: [] })
          .status,
        "complete",
      );
      const original =
        (yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`)[0]!;
      yield* value.sql`INSERT INTO orchestration_v2_imported_application_attachment_inventories ${value.sql.insert(
        {
          ...original,
          inventory_id: header.inventoryId,
          adoption_ordinal: 1,
          source_kind: source.kind,
          canonical_header_json: json(header),
          canonical_source_json: json(source),
          message_carrier_count: 2,
          answer_carrier_count: 0,
          attachment_reference_count: 0,
          carrier_set_sha256: empty.carrierSetSha256,
        },
      )}`;
      const before =
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories ORDER BY adoption_ordinal`;
      assert.deepEqual(yield* value.read(value.input), {
        status: "unavailable",
        reason: "native_import_transcript_seal_producer_unavailable",
      });
      assert.deepEqual(yield* value.prepare(value.input), {
        status: "unavailable",
        reason: "native_import_transcript_seal_producer_unavailable",
      });
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories ORDER BY adoption_ordinal`,
        before,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "a matching imported retention digest cannot resolve a namespace hold without authentic current-source inventory evidence",
  () =>
    Effect.gen(function* () {
      const value = yield* fixture();
      const basis = {
        ...value.basis,
        retentionSourceEvidence:
          ImportedAttachments.makeImportedApplicationAttachmentRetentionEvidenceV1({
            segments: [
              {
                inventoryId: "a".repeat(64),
                applicationBirth: value.task.reference.ownerBirth,
                carrierSetSha256: digest([]),
                forkBasis: [],
              },
            ],
            visibleV2CarrierSetSha256: digest([]),
            relativePaths: [],
          }),
      };
      yield* insertHold(value.task.bindingSha256);
      yield* value.sql`INSERT INTO orchestration_v2_attachment_cleanup_observations ${value.sql.insert({ ...value.row, correlation_json: json({ version: 1, basis, observationSha256: digest(value.observation) }) })}`;
      yield* value.sql`UPDATE orchestration_v2_effect_outbox SET status='succeeded',completed_at=${timestamp} WHERE effect_id=${effectId}`;
      const holds = yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`;
      const history =
        yield* value.sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`;
      assert.equal((yield* Effect.result(value.read(effectId)))._tag, "Failure");
      assert.equal((yield* Effect.result(value.unresolved(threadId)))._tag, "Failure");
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_unknown_effect_holds`,
        holds,
      );
      assert.deepEqual(
        yield* value.sql`SELECT * FROM orchestration_v2_attachment_cleanup_observations`,
        history,
      );
    }).pipe(Effect.provide(Layer.fresh(persistence))),
);

it.effect(
  "actual registered inventory and whole-source retention reopen and refuse a changed legacy source without rewriting history",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          directory: process.env.TMPDIR ?? NodeOS.tmpdir(),
          prefix: "application-retention-fixture-",
        });
        const database = makeSqlitePersistenceLive(path.join(directory, "fixture.sqlite"));
        const reopenedStores = Layer.mergeAll(
          database,
          EventStore.layer.pipe(Layer.provide(database)),
          ProjectionStore.layer.pipe(Layer.provide(database)),
        );
        const reopenedSourceReader = Layer.effect(
          EventSink.LegacyCurrentSourceReader,
          LegacyV1ThreadImporter.makeLegacyCurrentSourceReader,
        ).pipe(Layer.provide(database));
        const reopenedLayer = EventSink.layer.pipe(
          Layer.provideMerge(Layer.merge(reopenedStores, reopenedSourceReader)),
        );
        const recorded = yield* Effect.gen(function* () {
          const value = yield* importedApplicationInventoryFixture();
          const inventory = yield* value.prepare(value.input);
          assert.equal(inventory.status, "complete");
          if (inventory.status !== "complete")
            return yield* Effect.die("Authentic adoption missing");
          const retentionReader = value.sink.readThreadRetainedAttachmentPaths;
          if (retentionReader === undefined)
            return yield* Effect.die("Authentic retention reader missing");
          const retention = yield* retentionReader(threadId);
          assert.equal(retention.status, "complete");
          if (retention.status !== "complete") return yield* Effect.die(retention.reason);
          assert.deepEqual(retention.relativePaths, [
            "application-answer-file.txt",
            "application-retained-file.txt",
          ]);
          assert.deepEqual(retention.sourceEvidence.segments, [
            {
              inventoryId: inventory.inventory.header.inventoryId,
              applicationBirth: value.birth,
              carrierSetSha256: inventory.inventory.header.carrierSetSha256,
              forkBasis: [],
            },
          ]);
          return {
            input: value.input,
            inventory,
            retention,
            headers:
              yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
            carriers:
              yield* value.sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`,
            ledger: yield* value.sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
          };
        }).pipe(Effect.provide(Layer.fresh(reopenedLayer)));
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const sink = yield* EventSink.EventSinkV2;
          const read = sink.readImportedApplicationAttachmentInventory;
          const prepare = sink.prepareImportedApplicationAttachmentInventory;
          const retain = sink.readThreadRetainedAttachmentPaths;
          if (read === undefined || prepare === undefined || retain === undefined)
            return yield* Effect.die("Authentic reopened owners missing");
          assert.deepEqual(yield* read(recorded.input), recorded.inventory);
          assert.deepEqual(yield* retain(threadId), recorded.retention);
          assert.deepEqual(
            yield* sql`SELECT * FROM jones_sql_migrations ORDER BY migration_id`,
            recorded.ledger,
          );
          yield* sql`UPDATE projection_thread_messages SET attachments_json='[]' WHERE message_id='message:application:system'`;
          assert.deepEqual(yield* retain(threadId), {
            status: "unavailable",
            reason: "imported_application_inventory_source_changed",
          });
          assert.deepEqual(yield* read(recorded.input), recorded.inventory);
          assert.deepEqual(
            yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_inventories`,
            recorded.headers,
          );
          assert.deepEqual(
            yield* sql`SELECT * FROM orchestration_v2_imported_application_attachment_carriers`,
            recorded.carriers,
          );
          const current = yield* prepare(recorded.input);
          assert.equal(current.status, "complete");
          if (current.status !== "complete") return yield* Effect.die(current.reason);
          assert.notEqual(
            current.inventory.header.inventoryId,
            recorded.inventory.inventory.header.inventoryId,
          );
          const retention = yield* retain(threadId);
          assert.equal(retention.status, "complete");
          if (retention.status !== "complete") return yield* Effect.die(retention.reason);
          assert.deepEqual(retention.relativePaths, ["application-answer-file.txt"]);
          assert.equal(
            retention.sourceEvidence.segments[0]!.inventoryId,
            current.inventory.header.inventoryId,
          );
          assert.notEqual(
            retention.sourceEvidence.retentionBasisSha256,
            recorded.retention.sourceEvidence.retentionBasisSha256,
          );
          assert.deepEqual(
            yield* read({
              ...recorded.input,
              inventoryId: recorded.inventory.inventory.header.inventoryId,
            }),
            recorded.inventory,
          );
        }).pipe(Effect.provide(Layer.fresh(reopenedLayer)));
      }).pipe(Effect.provide(NodeServices.layer)),
    ),
);
