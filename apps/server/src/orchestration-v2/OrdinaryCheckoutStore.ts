import {
  CommandId,
  OrchestrationV2Command,
  type OrchestrationV2StoredEvent,
  OrchestrationV2StoredEventJson,
  OrchestrationV2CheckpointJson,
  WorktreeOwnershipConflictError,
  type ProjectId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  OrdinaryCheckoutExecutionEvidenceV1,
  OrdinaryCheckoutCompletionEvidenceV1,
  OrdinaryManagedStartObservationV1,
  OrdinaryCheckoutExecutorOutcomeV1,
  OrdinaryCheckpointProducerObservationV1,
  OrdinaryFinalCheckpointCompletionBasisV1,
  OrdinaryPreparedBranchTransitionV1,
  OrdinaryPreparedBranchObservationV1,
  type OrdinaryCheckoutExecutionAssociationFactV1,
  type OrdinaryCheckoutExecutionAssociationsV1,
} from "./OrdinaryCheckoutExecution.ts";
import { ProviderManagedActorClosureV1 } from "./ProviderManagedActorCompletion.ts";
import { readApplicationThreadBirth } from "./ApplicationThreadBirth.ts";
import { canonicalJson, sha256 } from "./CanonicalJson.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventStore from "./EventStore.ts";
import { makeCommitTransaction } from "./CommitTransaction.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as Ordinary from "./OrdinaryCheckoutOwnership.ts";
import {
  WORKTREE_OWNERSHIP_LEASE_DURATION_MS,
  WorktreeOwnershipLease,
} from "./WorktreeOwnershipLease.ts";

export interface OrdinaryCheckoutCommitCapture {
  readonly capture: Ordinary.OrdinaryCheckoutCaptureV1;
  readonly source: {
    readonly projectWorkspaceRoot: string;
    readonly worktreePath: string | null;
  };
  readonly joinedUse?: Ordinary.OrdinaryCheckoutUseV1;
  readonly ordinaryCheckoutExecution?: Ordinary.OrdinaryCheckoutExecutionRefV1;
  readonly command?: OrchestrationV2Command;
}

export interface OrdinaryCheckoutSystemEffectsV1 {
  readonly runId: RunId;
  readonly admission: Ordinary.OrdinaryCheckoutAdmissionRefV1;
  readonly source: OrdinaryCheckoutCommitCapture["source"];
  readonly joinedUse?: Ordinary.OrdinaryCheckoutUseV1;
  readonly ordinaryCheckoutExecution?: Ordinary.OrdinaryCheckoutExecutionRefV1;
}

export interface OrdinaryCheckoutUseRecordV1 {
  readonly subject: typeof OrdinaryCheckoutUseSubjectV1.Type;
  readonly state: "reserved" | "started" | "unknown" | "completed" | "no_effect" | "released";
  readonly startedAt: string | null;
}

class OrdinaryCheckoutHistoryError extends Schema.TaggedError<OrdinaryCheckoutHistoryError>()(
  "OrdinaryCheckoutHistoryError",
  { message: Schema.String, cause: Schema.optional(Schema.Unknown) },
) {}

class OrdinaryCheckoutRecordError extends Schema.TaggedError<OrdinaryCheckoutRecordError>()(
  "OrdinaryCheckoutRecordError",
  {
    recordKind: Schema.Literals(["admission", "use"]),
    recordId: Schema.String,
    message: Schema.String,
  },
) {}

class OrdinaryCheckoutEvidenceWriteError extends Schema.TaggedError<OrdinaryCheckoutEvidenceWriteError>()(
  "OrdinaryCheckoutEvidenceWriteError",
  { eventCount: Schema.Number, commandId: Schema.optional(Schema.String), cause: Schema.Unknown },
) {}
class OrdinaryCheckoutCommandEvidenceError extends Schema.TaggedError<OrdinaryCheckoutCommandEvidenceError>()(
  "OrdinaryCheckoutCommandEvidenceError",
  { commandId: Schema.String, reason: Schema.Literal("unknown_evidence") },
) {}

const admissionJson = Schema.fromJsonString(Ordinary.OrdinaryCheckoutAdmissionV1);
const decodeAdmission = Schema.decodeUnknownEffect(admissionJson);
const encodeAdmission = Schema.encodeEffect(admissionJson);
const OrdinaryCheckoutCommandLinkPayloadV1 = Schema.Struct({
  schema: Schema.Literal("t3.ordinary-checkout-command-link/v1"),
  version: Schema.Literal(1),
  link: Ordinary.OrdinaryCheckoutEffectLinkV1,
  command: Schema.Record(Schema.String, Schema.Unknown),
  commandDigest: Schema.String.check(Schema.makeFilter((value) => /^[0-9a-f]{64}$/.test(value))),
  joinedUse: Schema.optional(Ordinary.OrdinaryCheckoutUseV1),
  ordinaryCheckoutExecution: Schema.optional(Ordinary.OrdinaryCheckoutExecutionRefV1),
});
const OrdinaryCheckoutCommandPayloadV1 = Schema.Struct({
  schema: Schema.Literal("t3.ordinary-checkout-command/v1"),
  version: Schema.Literal(1),
  command: Schema.Record(Schema.String, Schema.Unknown),
  joinedUse: Schema.optional(Ordinary.OrdinaryCheckoutUseV1),
  ordinaryCheckoutExecution: Schema.optional(Ordinary.OrdinaryCheckoutExecutionRefV1),
});
const OrdinaryCheckoutSystemLinkPayloadV1 = Schema.Struct({
  schema: Schema.Literal("t3.ordinary-checkout-system-link/v1"),
  version: Schema.Literal(1),
  link: Ordinary.OrdinaryCheckoutEffectLinkV1,
  ordinaryCheckoutExecution: Ordinary.OrdinaryCheckoutExecutionRefV1,
});
export interface OrdinaryCheckoutCommandBindingV1 {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly admission: Ordinary.OrdinaryCheckoutAdmissionRefV1;
  readonly canonicalCommand: Readonly<Record<string, unknown>>;
  readonly commandDigest: string;
  readonly joinedUse: Ordinary.OrdinaryCheckoutUseV1 | null;
  readonly ordinaryCheckoutExecution: Ordinary.OrdinaryCheckoutExecutionRefV1 | null;
}

const linkJson = Schema.fromJsonString(OrdinaryCheckoutCommandLinkPayloadV1);
const decodeLink = Schema.decodeUnknownEffect(linkJson);
const encodeLink = Schema.encodeEffect(linkJson);
const OrdinaryCheckoutUseSubjectV1 = Schema.Struct({
  schema: Schema.Literal("t3.ordinary-checkout-use/v1"),
  use: Ordinary.OrdinaryCheckoutUseV1,
  source: Schema.Struct({
    projectWorkspaceRoot: Schema.NonEmptyString,
    worktreePath: Schema.NullOr(Schema.NonEmptyString),
  }),
});
const decodeUseSubject = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrdinaryCheckoutUseSubjectV1),
);
const encodeUseSubject = Schema.encodeEffect(Schema.fromJsonString(OrdinaryCheckoutUseSubjectV1));
const encodeExecution = Schema.encodeSync(Ordinary.OrdinaryCheckoutExecutionRefV1);
const encodeUse = Schema.encodeSync(Ordinary.OrdinaryCheckoutUseV1);
const encodeSubject = Schema.encodeSync(OrdinaryCheckoutUseSubjectV1);
const encodeEffectLink = Schema.encodeSync(Ordinary.OrdinaryCheckoutEffectLinkV1);
const useBytes = (use: Ordinary.OrdinaryCheckoutUseV1) => canonicalJson(encodeUse(use));
const subjectBytes = (subject: typeof OrdinaryCheckoutUseSubjectV1.Type) =>
  canonicalJson(encodeSubject(subject));
const decodeLease = Schema.decodeUnknownEffect(WorktreeOwnershipLease);
const encodeRequest = Schema.encodeSync(EffectOutbox.OrchestrationEffectRequestV2);
const encodeCommand = Schema.encodeEffect(OrchestrationV2Command);

export interface OrdinaryCheckoutCaptureInput {
  readonly command: OrchestrationV2Command;
  readonly threadId: ThreadId;
  readonly projectId: ProjectId;
  readonly branch: string | null;
  readonly canonicalProjectRoot: string;
  readonly canonicalCheckoutPath: string;
  readonly source: OrdinaryCheckoutCommitCapture["source"];
  readonly leaseId: string;
  readonly origin?: Ordinary.OrdinaryCheckoutOriginV1;
}

/** These methods run inside the EventSink transaction that owns acceptance. */
export const makeOrdinaryCheckoutStore = Effect.fn("makeOrdinaryCheckoutStore")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const transactions = yield* makeCommitTransaction();
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
  const commandReceipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const eventStore = yield* EventStore.EventStoreV2;
  const ordinaryExecutionCallbacks = new Map<
    string,
    Effect.Effect<void, OrdinaryCheckoutHistoryError>
  >();
  const readBirth = (threadId: ThreadId) =>
    readApplicationThreadBirth(threadId).pipe(Effect.provideService(SqlClient.SqlClient, sql));
  const failure = (
    capture: Ordinary.OrdinaryCheckoutCaptureV1,
    reason: Ordinary.OrdinaryCheckoutOwnershipError["reason"],
    message: string,
  ) =>
    new Ordinary.OrdinaryCheckoutOwnershipError({
      reason,
      threadId: capture.threadId,
      path: capture.canonicalCheckoutPath,
      message,
    });

  const capture = Effect.fn("OrdinaryCheckoutStore.capture")(function* (
    input: OrdinaryCheckoutCaptureInput,
  ) {
    const birth = yield* readBirth(input.threadId);
    if (birth === null)
      return yield* new Ordinary.OrdinaryCheckoutOwnershipError({
        reason: "unavailable",
        threadId: input.threadId,
        path: input.canonicalCheckoutPath,
        message: "An authoritative current application thread birth is required.",
      });
    const rows = yield* sql`SELECT resource_path AS "resourcePath", lease_id AS "leaseId",
      owner_thread_id AS "ownerThreadId", owner_incarnation AS "ownerIncarnation", branch,
      acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"
      FROM worktree_ownership_leases WHERE resource_path = ${input.canonicalCheckoutPath}`;
    const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
    const lease =
      rows.length === 1
        ? yield* decodeLease(rows[0])
        : {
            resourcePath: input.canonicalCheckoutPath,
            leaseId: input.leaseId,
            ownerThreadId: input.threadId,
            ownerIncarnation: Ordinary.ordinaryApplicationIncarnationV1(birth),
            branch: input.branch,
            acquiredAtMs: nowMs,
            renewedAtMs: nowMs,
            expiresAtMs: nowMs + WORKTREE_OWNERSHIP_LEASE_DURATION_MS,
          };
    if (lease.ownerThreadId !== input.threadId)
      return yield* new WorktreeOwnershipConflictError({
        resourcePath: lease.resourcePath,
        ownerThreadId: lease.ownerThreadId,
        requestingThreadId: input.threadId,
        ownerBranch: lease.branch,
        expiresAtMs: lease.expiresAtMs,
      });
    const canonicalCommand = yield* encodeCommand(input.command);
    return {
      capture: {
        version: 1 as const,
        commandId: input.command.commandId,
        commandType: input.command.type,
        canonicalCommand,
        commandDigest: Ordinary.ordinaryCheckoutCommandDigestV1(canonicalCommand),
        origin:
          input.origin ??
          (input.command.type === "runtime-request.respond"
            ? { kind: "runtime_request_answer" as const, requestId: input.command.requestId }
            : { kind: "command" as const }),
        threadId: input.threadId,
        applicationBirth: birth,
        projectId: input.projectId,
        canonicalProjectRoot: input.canonicalProjectRoot,
        canonicalCheckoutPath: input.canonicalCheckoutPath,
        branch: input.branch,
        lease,
      },
      source: input.source,
    } satisfies OrdinaryCheckoutCommitCapture;
  });

  const readOrdinaryCheckoutAdmissionForRunEffect = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
  }) {
    const rows = yield* sql<{
      readonly command_id: string;
    }>`SELECT admission.command_id FROM orchestration_v2_ordinary_checkout_admissions admission
      JOIN orchestration_events event ON event.command_id = admission.command_id AND event.stream_id = admission.thread_id
      WHERE admission.thread_id = ${input.threadId} AND json_extract(admission.admission_json, '$.run.runId') = ${input.runId}
        AND event.application_event_version = 2 AND event.aggregate_kind = 'thread' AND event.event_type = 'run.created'
        AND json_extract(event.payload_json, '$.id') = ${input.runId}`;
    if (rows.length === 0) return null;
    if (rows.length !== 1)
      return yield* new OrdinaryCheckoutEvidenceWriteError({
        eventCount: 0,
        cause: "Original checkout run admission is ambiguous",
      });
    const admission = (yield* readOrdinaryCheckoutAdmissionsEffect(
      CommandId.make(rows[0]!.command_id),
    )).find((item) => item.capture.threadId === input.threadId && item.run?.runId === input.runId);
    if (admission === undefined)
      return yield* new OrdinaryCheckoutEvidenceWriteError({
        eventCount: 0,
        cause: "Original checkout run admission is unavailable",
      });
    return admission;
  });
  const resolveOrdinaryCheckoutLease = Effect.fnUntraced(function* (
    original: WorktreeOwnershipLease,
  ) {
    const rows =
      yield* sql`SELECT resource_path AS "resourcePath", lease_id AS "leaseId", owner_thread_id AS "ownerThreadId",
      owner_incarnation AS "ownerIncarnation", branch, acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"
      FROM worktree_ownership_leases WHERE resource_path = ${original.resourcePath}`;
    const current =
      rows.length === 1
        ? yield* Schema.decodeUnknownEffect(Ordinary.OrdinaryCheckoutLeaseV1)(rows[0])
        : null;
    const unavailable = () =>
      new OrdinaryCheckoutEvidenceWriteError({
        eventCount: 0,
        cause: "Original checkout lease has no qualified current branch transition",
      });
    if (current === null) return yield* unavailable();
    if (current.ownerThreadId !== original.ownerThreadId)
      return yield* new WorktreeOwnershipConflictError({
        resourcePath: current.resourcePath,
        ownerThreadId: current.ownerThreadId,
        requestingThreadId: original.ownerThreadId,
        ownerBranch: current.branch,
        expiresAtMs: current.expiresAtMs,
      });
    if (
      canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(current)) !==
      canonicalJson(
        Ordinary.ordinaryCheckoutLeaseIdentityV1({ ...original, branch: current.branch }),
      )
    )
      return yield* unavailable();
    if (current.branch === original.branch) return current;
    const candidates = yield* sql<{
      readonly operation_id: string;
      readonly admission_id: string;
      readonly transition_json: string;
      readonly recorded_at: string;
    }>`SELECT * FROM orchestration_v2_ordinary_checkout_target_transitions
      WHERE json_extract(transition_json, '$.leaseId') = ${original.leaseId}
        AND json_extract(transition_json, '$.canonicalCheckoutPath') = ${original.resourcePath}`;
    const transitions: Array<typeof OrdinaryPreparedBranchTransitionV1.Type> = [];
    for (const candidate of candidates) {
      const transition = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(Ordinary.OrdinaryCheckoutTargetTransitionV1),
      )(candidate.transition_json, { onExcessProperty: "error" });
      const evidence = yield* Schema.decodeUnknownEffect(OrdinaryPreparedBranchTransitionV1)(
        transition.evidence,
        { onExcessProperty: "error" },
      );
      const observation = evidence.observation;
      const ref = observation.execution;
      const record = yield* exactUse(ref.originalUse);
      const admission = yield* resolveAdmission(ref.originalUse.admission);
      const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(ref.originalUse);
      const predecessor = history.facts[evidence.associationOrdinal];
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(evidence.commandId));
      const events = yield* eventStore
        .readByCommandId({ commandId: evidence.commandId })
        .pipe(Stream.runCollect);
      const event = events.find(
        (item) => item.event.id === evidence.eventId && item.sequence === evidence.sequence,
      );
      const observedAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
        observation.observedAt,
      );
      if (
        candidate.operation_id !== ref.originalUse.operationId ||
        transition.operationId !== candidate.operation_id ||
        candidate.admission_id !== ref.originalUse.admission.admissionId ||
        canonicalJson(transition.admission) !== canonicalJson(ref.originalUse.admission) ||
        canonicalJson(
          yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutUseSourceV1)(transition.source),
        ) !==
          canonicalJson(
            yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutUseSourceV1)(
              ref.originalUse.source,
            ),
          ) ||
        canonicalJson(transition.applicationBirth) !==
          canonicalJson(admission.capture.applicationBirth) ||
        canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(transition.beforeLease)) !==
          canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(ref.originalUse.lease)) ||
        canonicalJson(transition.afterLease) !==
          canonicalJson({
            ...transition.beforeLease,
            branch: observation.renamedBranch,
          }) ||
        transition.leaseId !== original.leaseId ||
        transition.canonicalCheckoutPath !== original.resourcePath ||
        transition.fromBranch !== observation.oldBranch ||
        transition.toBranch !== observation.renamedBranch ||
        DateTime.formatIso(transition.recordedAt) !== candidate.recorded_at ||
        canonicalJson(
          Ordinary.ordinaryCheckoutLeaseIdentityV1({
            ...transition.beforeLease,
            branch: original.branch,
          }),
        ) !== canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(original)) ||
        ref.executor.kind !== "actual_prepared_producer" ||
        observation.producerId !== ref.executor.producerId ||
        predecessor === undefined ||
        ordinaryExecutionBytes(predecessor.ref) !== ordinaryExecutionBytes(ref) ||
        predecessor.eventKind === "retire" ||
        predecessor.eventKind === "unknown" ||
        predecessor.recordedAt > candidate.recorded_at ||
        predecessor.evidence.expiresAt <= candidate.recorded_at ||
        observation.checkoutPath !== original.resourcePath ||
        observation.oldBranch !== ref.originalUse.lease.branch ||
        canonicalJson(observation.targetSource) !== canonicalJson(record.subject.source) ||
        observation.readback.cwd !== observation.checkoutPath ||
        observation.readback.refName !== observation.renamedBranch ||
        observation.oldBranch === observation.renamedBranch ||
        record.startedAt === null ||
        observation.observedAt < record.startedAt ||
        DateTime.formatIso(observedAt) !== observation.observedAt ||
        observation.observedAt > candidate.recorded_at ||
        receipt?.status !== "accepted" ||
        receipt.threadId !== ref.originalUse.lease.ownerThreadId ||
        receipt.commandType !== "thread.metadata.update" ||
        receipt.resultSequence !== evidence.sequence ||
        event?.event.type !== "thread.metadata-updated" ||
        event.event.threadId !== receipt.threadId ||
        event.event.payload.branch !== observation.renamedBranch ||
        event.event.payload.worktreePath !== record.subject.source.worktreePath
      )
        return yield* unavailable();
      transitions.push(evidence);
    }
    // A captured target may advance only along the committed physical observations.
    let branch = original.branch;
    for (const transition of transitions.toSorted((a, b) => a.sequence - b.sequence)) {
      if (transition.observation.oldBranch === branch)
        branch = transition.observation.renamedBranch;
    }
    if (branch !== current.branch) return yield* unavailable();
    return current;
  });

  const current = Effect.fn("OrdinaryCheckoutStore.current")(function* (
    input: OrdinaryCheckoutCommitCapture,
  ) {
    const { capture } = input;
    const birth = yield* readBirth(capture.threadId);
    const targets = yield* sql<{
      readonly project_id: string;
      readonly worktree_path: string | null;
      readonly branch: string | null;
      readonly workspace_root: string;
    }>`SELECT t.project_id, json_extract(t.payload_json, '$.worktreePath') AS worktree_path,
        json_extract(t.payload_json, '$.branch') AS branch, p.workspace_root
      FROM orchestration_v2_projection_threads t
      JOIN projection_projects p ON p.project_id = t.project_id AND p.deleted_at IS NULL
      WHERE t.thread_id = ${capture.threadId}
        AND json_extract(t.payload_json, '$.deletedAt') IS NULL`;
    const target = targets[0];
    const leases = yield* sql`SELECT resource_path AS "resourcePath", lease_id AS "leaseId",
      owner_thread_id AS "ownerThreadId", owner_incarnation AS "ownerIncarnation", branch,
      acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"
      FROM worktree_ownership_leases WHERE resource_path = ${capture.canonicalCheckoutPath}`;
    const lease = leases.length === 1 ? yield* resolveOrdinaryCheckoutLease(capture.lease) : null;
    if (
      !Ordinary.ordinaryCheckoutCaptureMatchesV1(capture) ||
      birth === null ||
      canonicalJson(birth) !== canonicalJson(capture.applicationBirth) ||
      target === undefined ||
      target.project_id !== capture.projectId ||
      target.workspace_root !== input.source.projectWorkspaceRoot ||
      target.worktree_path !== input.source.worktreePath ||
      target.branch !== lease?.branch ||
      lease === null ||
      canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(lease)) !==
        canonicalJson(
          Ordinary.ordinaryCheckoutLeaseIdentityV1({ ...capture.lease, branch: lease.branch }),
        )
    )
      return yield* failure(
        capture,
        "target_changed",
        "Captured checkout ownership is no longer current.",
      );
    return lease;
  });

  const acquireBeforeRead = Effect.fn("OrdinaryCheckoutStore.acquireBeforeRead")(function* (
    input: OrdinaryCheckoutCommitCapture,
    acceptedAt: DateTime.Utc,
  ) {
    const { capture } = input;
    if (input.joinedUse !== undefined || input.ordinaryCheckoutExecution !== undefined) {
      yield* transactions.requireOwned;
      return input;
    }
    const nowMs = DateTime.toEpochMillis(acceptedAt);
    // Keep this the transaction's first SQL statement. A prior read can turn a
    // concurrent WAL commit into SQLITE_BUSY_SNAPSHOT instead of a guarded refusal.
    const rows = yield* sql`INSERT INTO worktree_ownership_leases
      (resource_path, lease_id, owner_thread_id, owner_incarnation, branch,
        acquired_at_ms, renewed_at_ms, expires_at_ms)
      SELECT ${capture.canonicalCheckoutPath}, ${capture.lease.leaseId}, ${capture.threadId},
        json_array('t3.orchestration-v2.thread-birth/v1', event_id, sequence),
        ${capture.branch}, ${nowMs}, ${nowMs}, ${nowMs + WORKTREE_OWNERSHIP_LEASE_DURATION_MS}
      FROM orchestration_events
      WHERE aggregate_kind = 'thread' AND application_event_version = 2
        AND stream_id = ${capture.threadId} AND event_type = 'thread.created'
        AND event_id = ${capture.applicationBirth.eventId} AND sequence = ${capture.applicationBirth.sequence}
        AND NOT EXISTS (SELECT 1 FROM orchestration_command_receipts WHERE command_id = ${capture.commandId})
      ON CONFLICT(resource_path) DO UPDATE SET renewed_at_ms = excluded.renewed_at_ms,
        expires_at_ms = excluded.expires_at_ms
      WHERE worktree_ownership_leases.owner_thread_id = excluded.owner_thread_id
        AND worktree_ownership_leases.owner_incarnation = excluded.owner_incarnation
        AND worktree_ownership_leases.lease_id = excluded.lease_id
        AND worktree_ownership_leases.branch IS excluded.branch
      RETURNING resource_path AS "resourcePath", lease_id AS "leaseId",
        owner_thread_id AS "ownerThreadId", owner_incarnation AS "ownerIncarnation", branch,
        acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"`;
    if (rows.length !== 1) {
      const existing =
        yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${capture.commandId}`;
      if (existing.length === 1) {
        const original = yield* readAdmission(capture.commandId, capture.threadId);
        if (original !== null && original.capture.commandDigest === capture.commandDigest)
          return input;
        return yield* failure(
          capture,
          "stale_admission",
          "The replay has another permanent original command.",
        );
      }
      return yield* failure(
        capture,
        "target_changed",
        "The captured checkout lease cannot be acquired.",
      );
    }
    const lease = yield* decodeLease(rows[0]);
    const captured = { ...input, capture: { ...capture, lease } };
    yield* current(captured);
    const active = yield* sql<{
      readonly kind: string;
      readonly state: string;
      readonly subject_json: string;
    }>`
      SELECT kind, state, subject_json FROM orchestration_v2_worktree_path_admissions
      WHERE canonical_path = ${capture.canonicalCheckoutPath} AND state NOT IN ('no_effect', 'released')`;
    for (const row of active) {
      if (row.kind !== "native_operation" || (row.state !== "reserved" && row.state !== "started"))
        return yield* failure(
          capture,
          "unknown_use",
          "Acceptance cannot reuse a removal, unknown or completed physical reservation.",
        );
      const original = yield* decodeUseSubject(row.subject_json);
      const originalAdmission = yield* resolveAdmission(original.use.admission);
      const originalLease = yield* current({
        capture: originalAdmission.capture,
        source: original.source,
      });
      if (
        canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(originalLease)) !==
        canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(lease))
      )
        return yield* failure(
          capture,
          "target_changed",
          "Acceptance cannot reuse another original owner's physical reservation.",
        );
    }
    return captured;
  });

  const acquireNewborn = Effect.fn("OrdinaryCheckoutStore.acquireNewborn")(function* (
    captured: OrdinaryCheckoutCommitCapture,
    receipt: CommandReceiptStore.CommandReceiptV2,
    events: ReadonlyArray<OrchestrationV2StoredEvent>,
  ) {
    yield* transactions.requireOwned;
    const capture = captured.capture;
    const birth = events.find(
      (stored) =>
        stored.event.type === "thread.created" && stored.event.threadId === capture.threadId,
    );
    if (
      capture.origin.kind !== "delegated_child" ||
      receipt.status !== "accepted" ||
      receipt.error !== null ||
      receipt.commandId !== capture.commandId ||
      receipt.commandType !== "delegated_task.request" ||
      receipt.threadId !== capture.origin.parentThreadId ||
      birth?.commandId !== capture.commandId ||
      birth.sequence !== capture.applicationBirth.sequence ||
      birth.event.id !== capture.applicationBirth.eventId ||
      birth.event.type !== "thread.created" ||
      birth.event.payload.branch !== capture.branch ||
      birth.event.payload.worktreePath !== captured.source.worktreePath ||
      birth.event.payload.projectId !== capture.projectId
    )
      return yield* failure(
        capture,
        "stale_admission",
        "A delegated checkout requires its exact newly committed child birth and outer receipt.",
      );
    const nowMs = DateTime.toEpochMillis(receipt.acceptedAt);
    const rows = yield* sql`INSERT INTO worktree_ownership_leases
      (resource_path, lease_id, owner_thread_id, owner_incarnation, branch, acquired_at_ms, renewed_at_ms, expires_at_ms)
      SELECT ${capture.canonicalCheckoutPath}, ${capture.lease.leaseId}, ${capture.threadId},
        ${capture.lease.ownerIncarnation}, ${capture.branch}, ${nowMs}, ${nowMs}, ${nowMs + WORKTREE_OWNERSHIP_LEASE_DURATION_MS}
      WHERE NOT EXISTS (SELECT 1 FROM worktree_ownership_leases WHERE resource_path = ${capture.canonicalCheckoutPath})
        AND NOT EXISTS (SELECT 1 FROM orchestration_v2_worktree_path_admissions
          WHERE canonical_path = ${capture.canonicalCheckoutPath} AND state NOT IN ('no_effect', 'released'))
      RETURNING resource_path AS "resourcePath", lease_id AS "leaseId", owner_thread_id AS "ownerThreadId",
        owner_incarnation AS "ownerIncarnation", branch, acquired_at_ms AS "acquiredAtMs",
        renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"`;
    if (rows.length !== 1)
      return yield* failure(
        capture,
        "target_changed",
        "A delegated child cannot adopt an existing path or lease.",
      );
    const lease = yield* decodeLease(rows[0]);
    const acquired = { ...captured, capture: { ...capture, lease } };
    yield* current(acquired);
    return acquired;
  });

  const recordAcceptance = Effect.fn("OrdinaryCheckoutStore.recordAcceptance")(function* (input: {
    readonly captured: OrdinaryCheckoutCommitCapture;
    readonly receipt: CommandReceiptStore.CommandReceiptV2;
    readonly events: ReadonlyArray<OrchestrationV2StoredEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
  }) {
    const { capture } = input.captured;
    const joinedUse =
      input.captured.ordinaryCheckoutExecution?.originalUse ?? input.captured.joinedUse;
    const canonicalCommand =
      input.captured.command === undefined
        ? capture.canonicalCommand
        : yield* encodeCommand(input.captured.command);
    const commandDigest = Ordinary.ordinaryCheckoutCommandDigestV1(canonicalCommand);
    const original = joinedUse === undefined ? null : yield* resolveAdmission(joinedUse.admission);
    if (original !== null) {
      if (
        original.run === null ||
        original.capture.threadId !== capture.threadId ||
        canonicalJson(original.capture) !== canonicalJson(capture) ||
        canonicalCommand.commandId !== input.receipt.commandId ||
        canonicalCommand.type !== input.receipt.commandType ||
        Reflect.get(canonicalCommand, "threadId") !== capture.threadId ||
        Reflect.get(canonicalCommand, "runId") !== original.run.runId ||
        input.events.some(
          (stored) =>
            stored.event.threadId === capture.threadId &&
            stored.event.runId !== undefined &&
            stored.event.runId !== original.run!.runId,
        )
      )
        return yield* failure(
          capture,
          "stale_admission",
          "Joined acceptance must retain the exact original checkout, run, command and source.",
        );
      if (input.captured.ordinaryCheckoutExecution === undefined)
        yield* validateOrdinaryCheckoutJoin(joinedUse!, original, input.captured.source, true);
      else {
        yield* validateOrdinaryCheckoutExecutionAttribution(
          input.captured.ordinaryCheckoutExecution,
          original,
          input.captured.joinedUse,
        );
        const execution = yield* validateOrdinaryCheckoutExecutionEffect(
          input.captured.ordinaryCheckoutExecution,
        );
        if (
          execution.record.state !== "started" ||
          canonicalJson(execution.record.subject.source) !== canonicalJson(input.captured.source)
        )
          return yield* failure(
            capture,
            "unknown_use",
            "Joined acceptance lost its entered original producer and physical source.",
          );
      }
      yield* validateCapture(capture, input.captured.source, {
        operationId: joinedUse!.operationId,
        requireLiveLease: true,
      });
    } else if (
      input.receipt.commandId !== capture.commandId ||
      canonicalJson(canonicalCommand) !== canonicalJson(capture.canonicalCommand)
    )
      return yield* failure(
        capture,
        "stale_admission",
        "Initial acceptance differs from its captured outer command.",
      );
    const runEvent = input.events.findLast(
      (stored) =>
        (stored.event.type === "run.created" || stored.event.type === "run.updated") &&
        stored.event.threadId === capture.threadId,
    );
    const run =
      runEvent?.event.type === "run.updated" || runEvent?.event.type === "run.created"
        ? runEvent.event.payload
        : null;
    const admission: Ordinary.OrdinaryCheckoutAdmissionV1 = original ?? {
      version: 1,
      admissionId: Ordinary.ordinaryCheckoutAdmissionIdV1(capture),
      capture,
      receipt: { ...input.receipt, status: "accepted", error: null },
      eventBasis: input.events.map((stored) => ({
        eventId: stored.event.id,
        sequence: stored.sequence,
        threadId: stored.event.threadId,
        commandId: stored.commandId,
        eventType: stored.event.type,
      })),
      run:
        run === null || run.activeAttemptId === null || run.rootNodeId === null
          ? null
          : {
              runId: run.id,
              runAttemptId: run.activeAttemptId,
              nodeId: run.rootNodeId,
              messageId: run.userMessageId,
            },
      recordedAt: input.receipt.acceptedAt,
    };
    if (!Ordinary.ordinaryCheckoutAdmissionMatchesV1(admission))
      return yield* failure(
        capture,
        "claim_mismatch",
        "Accepted events do not establish the captured admission.",
      );
    const ref = Ordinary.ordinaryCheckoutAdmissionRefV1(admission);
    const recordedAt = DateTime.formatIso(admission.recordedAt);
    if (original === null)
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions
      (admission_id, command_id, thread_id, admission_sha256, admission_json, recorded_at)
      VALUES (${ref.admissionId}, ${capture.commandId}, ${capture.threadId}, ${ref.admissionSha256},
        ${yield* encodeAdmission(admission)}, ${recordedAt})`;
    yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_commands
      (command_id, thread_id, admission_id, canonical_command_json, command_digest, recorded_at)
      VALUES (${input.receipt.commandId}, ${capture.threadId}, ${ref.admissionId},
        ${canonicalJson(
          joinedUse === undefined
            ? canonicalCommand
            : {
                schema: "t3.ordinary-checkout-command/v1",
                version: 1,
                command: canonicalCommand,
                ...(input.captured.joinedUse === undefined
                  ? {}
                  : { joinedUse: input.captured.joinedUse }),
                ...(input.captured.ordinaryCheckoutExecution === undefined
                  ? {}
                  : { ordinaryCheckoutExecution: input.captured.ordinaryCheckoutExecution }),
              },
        )}, ${commandDigest}, ${DateTime.formatIso(input.receipt.acceptedAt)})`;
    for (const effect of input.effects.filter(
      (item) =>
        item.threadId === capture.threadId &&
        [
          "delegated-workspace.prepare",
          "provider-turn.start",
          "provider-turn.restart",
          "provider-turn.steer",
          "provider-thread.rollback",
          "provider-runtime.continue",
          "runtime-request.respond",
          "checkpoint.capture",
        ].includes(item.request.type),
    )) {
      if (effect.commandId !== input.receipt.commandId)
        return yield* failure(
          capture,
          "claim_mismatch",
          "The effect is attributed to another outer command.",
        );
      const link: Ordinary.OrdinaryCheckoutEffectLinkV1 = {
        version: 1,
        effectId: effect.id,
        commandId: input.receipt.commandId,
        threadId: capture.threadId,
        requestSha256: sha256(canonicalJson(encodeRequest(effect.request))),
        admission: ref,
        recordedAt: input.receipt.acceptedAt,
      };
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_effect_links
        (effect_id, admission_id, link_json, recorded_at)
        VALUES (${effect.id}, ${ref.admissionId}, ${yield* encodeLink({
          schema: "t3.ordinary-checkout-command-link/v1",
          version: 1,
          link,
          command: canonicalCommand,
          commandDigest,
          ...(input.captured.joinedUse === undefined
            ? {}
            : { joinedUse: input.captured.joinedUse }),
          ...(input.captured.ordinaryCheckoutExecution === undefined
            ? {}
            : { ordinaryCheckoutExecution: input.captured.ordinaryCheckoutExecution }),
        })}, ${DateTime.formatIso(input.receipt.acceptedAt)})`;
    }
    yield* readOrdinaryCheckoutCurrentCommandsEffect(input.receipt.commandId);
    return admission;
  });

  const ordinaryEventBasis = (
    events: ReadonlyArray<OrchestrationV2StoredEvent>,
    commandId: CommandId,
  ) =>
    events.map((stored) => ({
      eventId: stored.event.id,
      sequence: stored.sequence,
      threadId: stored.event.threadId,
      commandId,
      eventType: stored.event.type,
    }));
  const readOrdinaryCheckoutAdmissionsEffect = Effect.fnUntraced(function* (commandId: CommandId) {
    const rows = yield* sql<{
      readonly admission_id: string;
      readonly command_id: string;
      readonly thread_id: string;
      readonly admission_sha256: string;
      readonly admission_json: string;
      readonly recorded_at: string;
    }>`
      SELECT * FROM orchestration_v2_ordinary_checkout_admissions WHERE command_id = ${commandId} ORDER BY thread_id`;
    const admissions: Ordinary.OrdinaryCheckoutAdmissionV1[] = [];
    for (const row of rows) {
      const admission = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(Ordinary.OrdinaryCheckoutAdmissionV1),
      )(row.admission_json, { onExcessProperty: "error" });
      const capture = admission.capture;
      const command = yield* Schema.decodeUnknownEffect(Schema.toType(OrchestrationV2Command))(
        {
          ...capture.canonicalCommand,
          ...(typeof capture.canonicalCommand.createdAt === "string"
            ? {
                createdAt: yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
                  capture.canonicalCommand.createdAt,
                ),
              }
            : {}),
        },
        { onExcessProperty: "error" },
      );
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(commandId));
      const events = yield* eventStore.readByCommandId({ commandId }).pipe(Stream.runCollect);
      const birth =
        yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2
        AND aggregate_kind = 'thread' AND stream_id = ${capture.threadId} AND event_type = 'thread.created'
        AND event_id = ${capture.applicationBirth.eventId} AND sequence = ${capture.applicationBirth.sequence}
        AND json_extract(payload_json, '$.id') = ${capture.threadId} AND json_extract(payload_json, '$.projectId') = ${capture.projectId}`;
      const ref = Ordinary.ordinaryCheckoutAdmissionRefV1(admission);
      if (
        row.command_id !== commandId ||
        row.thread_id !== capture.threadId ||
        row.admission_id !== admission.admissionId ||
        row.admission_sha256 !== ref.admissionSha256 ||
        row.recorded_at !== DateTime.formatIso(admission.recordedAt) ||
        !Ordinary.ordinaryCheckoutAdmissionMatchesV1(admission) ||
        birth.length !== 1 ||
        canonicalJson(
          yield* Schema.encodeEffect(OrchestrationV2Command)(command).pipe(Effect.orDie),
        ) !== canonicalJson(capture.canonicalCommand) ||
        receipt?.status !== "accepted" ||
        canonicalJson(
          yield* Schema.encodeEffect(Ordinary.OrdinaryAcceptedReceiptV1)(admission.receipt).pipe(
            Effect.orDie,
          ),
        ) !==
          canonicalJson({
            ...receipt,
            acceptedAt: DateTime.formatIso(receipt.acceptedAt),
          }) ||
        canonicalJson(admission.eventBasis) !== canonicalJson(ordinaryEventBasis(events, commandId))
      )
        return yield* failure(
          capture,
          "stale_admission",
          "Original checkout admission lost its command, receipt or event association",
        );
      if (admission.run !== null) {
        const run = admission.run;
        const messages = events.filter(
          (stored) =>
            stored.event.type === "message.updated" &&
            stored.event.threadId === capture.threadId &&
            stored.event.payload.id === run.messageId &&
            stored.event.payload.role === "user" &&
            stored.event.payload.runId === run.runId,
        );
        const attempts =
          yield* sql`SELECT event_id FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'
          AND stream_id = ${capture.threadId} AND event_type = 'run-attempt.created' AND json_extract(payload_json, '$.id') = ${run.runAttemptId}
          AND json_extract(payload_json, '$.runId') = ${run.runId} AND json_extract(payload_json, '$.rootNodeId') = ${run.nodeId}`;
        if (messages.length !== 1 || attempts.length !== 1)
          return yield* failure(
            capture,
            "stale_admission",
            "Original checkout admission lost its accepted message or attempt",
          );
      }
      admissions.push(admission);
    }
    return admissions;
  });
  const readAdmission = Effect.fn("OrdinaryCheckoutStore.readAdmission")(function* (
    commandId: CommandId,
    threadId: ThreadId,
  ) {
    return (
      (yield* readOrdinaryCheckoutAdmissionsEffect(commandId)).find(
        (item) => item.capture.threadId === threadId,
      ) ?? null
    );
  });

  const readOrdinaryCheckoutEffectLinkEffect = Effect.fnUntraced(function* (effectId: string) {
    const rows = yield* sql<{
      readonly admission_id: string;
      readonly link_json: string;
      readonly recorded_at: string;
    }>`
      SELECT * FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = ${effectId}`;
    if (rows.length === 0) return null;
    const row = rows[0]!;
    const raw = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
    )(row.link_json);
    const wrapped =
      raw.schema === "t3.ordinary-checkout-command-link/v1"
        ? yield* Schema.decodeUnknownEffect(OrdinaryCheckoutCommandLinkPayloadV1)(raw, {
            onExcessProperty: "error",
          })
        : null;
    const system =
      raw.schema === "t3.ordinary-checkout-system-link/v1"
        ? yield* Schema.decodeUnknownEffect(OrdinaryCheckoutSystemLinkPayloadV1)(raw, {
            onExcessProperty: "error",
          })
        : null;
    const link =
      wrapped?.link ??
      system?.link ??
      (yield* Schema.decodeUnknownEffect(Ordinary.OrdinaryCheckoutEffectLinkV1)(raw, {
        onExcessProperty: "error",
      }));
    const owners = yield* sql<{
      readonly command_id: string;
    }>`SELECT command_id FROM orchestration_v2_ordinary_checkout_admissions
      WHERE admission_id = ${row.admission_id}`;
    const admission =
      owners.length === 1
        ? (yield* readOrdinaryCheckoutAdmissionsEffect(CommandId.make(owners[0]!.command_id))).find(
            (item) => item.admissionId === row.admission_id,
          )
        : undefined;
    const effect = Option.getOrNull(yield* outbox.get(effectId));
    if (
      admission === undefined ||
      rows.length !== 1 ||
      link.effectId !== effectId ||
      link.threadId !== admission.capture.threadId ||
      row.recorded_at !== DateTime.formatIso(link.recordedAt) ||
      canonicalJson(link.admission) !==
        canonicalJson(Ordinary.ordinaryCheckoutAdmissionRefV1(admission)) ||
      effect === null ||
      effect.commandId !== link.commandId ||
      effect.threadId !== link.threadId ||
      link.requestSha256 !==
        sha256(
          canonicalJson(
            yield* Schema.encodeEffect(EffectOutbox.OrchestrationEffectRequestV2)(
              effect.request,
            ).pipe(Effect.orDie),
          ),
        )
    )
      return yield* new OrdinaryCheckoutEvidenceWriteError({
        eventCount: 0,
        cause: "Original checkout effect link lost its actual effect association",
      });
    if (wrapped !== null) {
      const command = yield* decodeOrdinaryCanonicalCommand(wrapped.command);
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(link.commandId));
      if (
        command.commandId !== link.commandId ||
        wrapped.commandDigest !== sha256(canonicalJson(wrapped.command)) ||
        receipt?.status !== "accepted" ||
        receipt.commandType !== command.type ||
        Reflect.get(
          wrapped.command,
          command.type === "delegated_task.request" ? "parentThreadId" : "threadId",
        ) !== receipt.threadId ||
        (wrapped.joinedUse !== undefined &&
          (canonicalJson(wrapped.joinedUse.admission) !== canonicalJson(link.admission) ||
            canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(wrapped.joinedUse.lease)) !==
              canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(admission.capture.lease))))
      )
        return yield* new OrdinaryCheckoutCommandEvidenceError({
          commandId: link.commandId,
          reason: "unknown_evidence",
        });
      if (wrapped.ordinaryCheckoutExecution !== undefined)
        yield* validateOrdinaryCheckoutExecutionAttribution(
          wrapped.ordinaryCheckoutExecution,
          admission,
          wrapped.joinedUse,
        );
    }
    if (system !== null)
      yield* validateOrdinaryCheckoutExecutionAttribution(
        system.ordinaryCheckoutExecution,
        admission,
      );
    return link;
  });
  const decodeOrdinaryCanonicalCommand = Effect.fnUntraced(function* (
    canonical: Readonly<Record<string, unknown>>,
  ) {
    const command = yield* Schema.decodeUnknownEffect(Schema.toType(OrchestrationV2Command))(
      {
        ...canonical,
        ...(typeof canonical.createdAt === "string"
          ? {
              createdAt: yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
                canonical.createdAt,
              ),
            }
          : {}),
      },
      { onExcessProperty: "error" },
    );
    if (
      canonicalJson(
        yield* Schema.encodeEffect(OrchestrationV2Command)(command).pipe(Effect.orDie),
      ) !== canonicalJson(canonical)
    )
      return yield* new OrdinaryCheckoutEvidenceWriteError({
        eventCount: 0,
        commandId: command.commandId,
        cause: "Ordinary command bytes are not canonical",
      });
    return command;
  });
  const readOrdinaryCheckoutCurrentCommandsEffect = Effect.fnUntraced(function* (
    commandId: CommandId,
  ) {
    const bindings = new Map<ThreadId, OrdinaryCheckoutCommandBindingV1>();
    const validate = Effect.fnUntraced(function* (input: {
      readonly threadId: ThreadId;
      readonly admissionId: string;
      readonly canonicalCommand: Readonly<Record<string, unknown>>;
      readonly commandDigest: string;
      readonly joinedUse?: Ordinary.OrdinaryCheckoutUseV1;
      readonly ordinaryCheckoutExecution?: Ordinary.OrdinaryCheckoutExecutionRefV1;
    }) {
      const owners = yield* sql<{
        readonly command_id: string;
      }>`SELECT command_id FROM orchestration_v2_ordinary_checkout_admissions WHERE admission_id = ${input.admissionId}`;
      const original =
        owners.length === 1
          ? (yield* readOrdinaryCheckoutAdmissionsEffect(
              CommandId.make(owners[0]!.command_id),
            )).find((item) => item.admissionId === input.admissionId)
          : undefined;
      const command = yield* decodeOrdinaryCanonicalCommand(input.canonicalCommand);
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(commandId));
      const ref = original === undefined ? null : Ordinary.ordinaryCheckoutAdmissionRefV1(original);
      if (
        original === undefined ||
        original.capture.threadId !== input.threadId ||
        command.commandId !== commandId ||
        receipt?.status !== "accepted" ||
        receipt.commandType !== command.type ||
        Reflect.get(
          input.canonicalCommand,
          command.type === "delegated_task.request" ? "parentThreadId" : "threadId",
        ) !== receipt.threadId ||
        input.commandDigest !== sha256(canonicalJson(input.canonicalCommand)) ||
        (input.joinedUse !== undefined &&
          (ref === null ||
            canonicalJson(input.joinedUse.admission) !== canonicalJson(ref) ||
            canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(input.joinedUse.lease)) !==
              canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(original.capture.lease))))
      )
        return yield* new OrdinaryCheckoutCommandEvidenceError({
          commandId,
          reason: "unknown_evidence",
        });
      if (input.ordinaryCheckoutExecution !== undefined)
        yield* validateOrdinaryCheckoutExecutionAttribution(
          input.ordinaryCheckoutExecution,
          original,
          input.joinedUse,
        );
      const binding: OrdinaryCheckoutCommandBindingV1 = {
        commandId,
        threadId: input.threadId,
        admission: Ordinary.ordinaryCheckoutAdmissionRefV1(original),
        canonicalCommand: input.canonicalCommand,
        commandDigest: input.commandDigest,
        joinedUse: input.joinedUse ?? null,
        ordinaryCheckoutExecution: input.ordinaryCheckoutExecution ?? null,
      };
      const existing = bindings.get(input.threadId);
      if (existing !== undefined && canonicalJson(existing) !== canonicalJson(binding))
        return yield* new OrdinaryCheckoutCommandEvidenceError({
          commandId,
          reason: "unknown_evidence",
        });
      bindings.set(input.threadId, binding);
    });
    const rows = yield* sql<{
      readonly thread_id: string;
      readonly admission_id: string;
      readonly canonical_command_json: string;
      readonly command_digest: string;
      readonly recorded_at: string;
    }>`SELECT * FROM orchestration_v2_ordinary_checkout_commands WHERE command_id = ${commandId} ORDER BY thread_id`;
    for (const row of rows) {
      const raw = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
      )(row.canonical_command_json);
      const payload =
        raw.schema === "t3.ordinary-checkout-command/v1"
          ? yield* Schema.decodeUnknownEffect(OrdinaryCheckoutCommandPayloadV1)(raw, {
              onExcessProperty: "error",
            })
          : null;
      const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(commandId));
      if (receipt === null || row.recorded_at !== DateTime.formatIso(receipt.acceptedAt))
        return yield* new OrdinaryCheckoutCommandEvidenceError({
          commandId,
          reason: "unknown_evidence",
        });
      yield* validate({
        threadId: ThreadId.make(row.thread_id),
        admissionId: row.admission_id,
        canonicalCommand: payload?.command ?? raw,
        commandDigest: row.command_digest,
        ...(payload?.joinedUse === undefined ? {} : { joinedUse: payload.joinedUse }),
        ...(payload?.ordinaryCheckoutExecution === undefined
          ? {}
          : { ordinaryCheckoutExecution: payload.ordinaryCheckoutExecution }),
      });
    }
    const links = yield* sql<{
      readonly effect_id: string;
      readonly admission_id: string;
      readonly link_json: string;
    }>`
      SELECT link.effect_id, link.admission_id, link.link_json FROM orchestration_v2_ordinary_checkout_effect_links link
      JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = link.effect_id WHERE effect.command_id = ${commandId} ORDER BY link.effect_id`;
    for (const row of links) {
      const link = yield* readOrdinaryCheckoutEffectLinkEffect(row.effect_id);
      if (link === null)
        return yield* new OrdinaryCheckoutCommandEvidenceError({
          commandId,
          reason: "unknown_evidence",
        });
      const raw = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
      )(row.link_json);
      if (raw.schema === "t3.ordinary-checkout-command-link/v1") {
        const payload = yield* Schema.decodeUnknownEffect(OrdinaryCheckoutCommandLinkPayloadV1)(
          raw,
          { onExcessProperty: "error" },
        );
        yield* validate({
          threadId: link.threadId,
          admissionId: row.admission_id,
          canonicalCommand: payload.command,
          commandDigest: payload.commandDigest,
          ...(payload.joinedUse === undefined ? {} : { joinedUse: payload.joinedUse }),
          ...(payload.ordinaryCheckoutExecution === undefined
            ? {}
            : { ordinaryCheckoutExecution: payload.ordinaryCheckoutExecution }),
        });
      } else {
        const originals = yield* readOrdinaryCheckoutAdmissionsEffect(commandId);
        const original = originals.find(
          (item) =>
            item.admissionId === row.admission_id && item.capture.threadId === link.threadId,
        );
        if (original !== undefined)
          yield* validate({
            threadId: link.threadId,
            admissionId: row.admission_id,
            canonicalCommand: original.capture.canonicalCommand,
            commandDigest: original.capture.commandDigest,
          });
        else {
          const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(commandId));
          const effect = Option.getOrNull(yield* outbox.get(link.effectId));
          const owner = yield* sql<{
            readonly command_id: string;
          }>`SELECT command_id FROM orchestration_v2_ordinary_checkout_admissions
            WHERE admission_id = ${row.admission_id}`;
          const admitted =
            owner.length === 1
              ? (yield* readOrdinaryCheckoutAdmissionsEffect(
                  CommandId.make(owner[0]!.command_id),
                )).find((item) => item.admissionId === row.admission_id)
              : undefined;
          if (
            receipt?.status === "accepted" &&
            receipt.commandType === "prepared-run.release" &&
            effect?.request.type === "provider-turn.start" &&
            receipt.threadId === link.threadId &&
            admitted?.run?.runId === effect.request.runId &&
            effect.id === `effect:${commandId}:provider-turn.start:${effect.request.runId}`
          ) {
            const canonicalCommand = {
              type: "prepared-run.release",
              commandId,
              threadId: link.threadId,
              runId: effect.request.runId,
            };
            yield* validate({
              threadId: link.threadId,
              admissionId: row.admission_id,
              canonicalCommand,
              commandDigest: sha256(canonicalJson(canonicalCommand)),
            });
          }
        }
      }
    }
    return [...bindings.values()].sort((left, right) =>
      left.threadId.localeCompare(right.threadId),
    );
  });
  const readEffectLink = Effect.fn("OrdinaryCheckoutStore.readEffectLink")(function* (
    effect: EffectOutbox.OrchestrationEffectV2,
  ) {
    const link = yield* readOrdinaryCheckoutEffectLinkEffect(effect.id);
    if (link === null) return null;
    const admission = yield* resolveAdmission(link.admission);
    const rows = yield* sql<{
      readonly link_json: string;
    }>`SELECT link_json FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = ${effect.id}`;
    const raw = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
    )(rows[0]!.link_json);
    const payload =
      raw.schema === "t3.ordinary-checkout-command-link/v1"
        ? yield* Schema.decodeUnknownEffect(OrdinaryCheckoutCommandLinkPayloadV1)(raw)
        : null;
    return { link, admission, payload };
  });

  const capturePreparedLaunch = Effect.fnUntraced(function* (input: {
    readonly command: Extract<
      OrchestrationV2Command,
      { readonly type: "thread.create" | "thread.metadata.update" }
    >;
    readonly preparationEvent: Ordinary.OrdinaryAcceptedEventV1;
    readonly target: Omit<OrdinaryCheckoutCaptureInput, "command" | "leaseId">;
  }) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const command = yield* Schema.decodeUnknownEffect(Schema.toType(OrchestrationV2Command))(
          input.command,
          { onExcessProperty: "error" },
        );
        if (command.type !== "thread.create" && command.type !== "thread.metadata.update")
          return yield* new OrdinaryCheckoutEvidenceWriteError({
            eventCount: 0,
            cause: "Runless preparation has no accepted creation or empty-thread update",
          });
        const encoded = yield* Schema.encodeEffect(OrchestrationV2Command)(command).pipe(
          Effect.orDie,
        );
        const preparation = yield* Schema.decodeUnknownEffect(Ordinary.OrdinaryAcceptedEventV1)(
          input.preparationEvent,
          { onExcessProperty: "error" },
        );
        const receipt = Option.getOrNull(yield* commandReceipts.getByCommandId(command.commandId));
        const events = yield* eventStore
          .readByCommandId({ commandId: command.commandId })
          .pipe(Stream.runCollect);
        const prepared = events.find(
          (stored) =>
            stored.event.id === preparation.eventId && stored.sequence === preparation.sequence,
        );
        const identities =
          yield* sql`SELECT command_id FROM orchestration_v2_native_command_identities WHERE command_id = ${command.commandId}`;
        const identity = identities[0] ?? null;
        const reservations =
          yield* sql`SELECT command_id FROM native_creation_reserved_command_identities WHERE command_id = ${command.commandId}`;
        const reservation = reservations[0] ?? null;
        const local = yield* projectionStore.getThreadRecords(command.threadId, [
          "messages",
          "runs",
        ]);
        if (
          receipt?.status !== "accepted" ||
          receipt.threadId !== command.threadId ||
          receipt.commandType !== command.type ||
          reservation !== null ||
          identity !== null ||
          local.messages.length > 0 ||
          local.runs.length > 0 ||
          (command.type === "thread.metadata.update" && command.expectedEmpty !== true) ||
          prepared === undefined ||
          preparation.commandId !== command.commandId ||
          preparation.threadId !== command.threadId ||
          preparation.eventType !== prepared.event.type ||
          prepared.event.threadId !== command.threadId ||
          prepared.event.type !==
            (command.type === "thread.create" ? "thread.created" : "thread.metadata-updated")
        )
          return yield* new OrdinaryCheckoutEvidenceWriteError({
            commandId: command.commandId,
            eventCount: 0,
            cause: "Original runless preparation association is unavailable",
          });
        const original = (yield* readOrdinaryCheckoutAdmissionsEffect(command.commandId)).find(
          (item) => item.capture.threadId === command.threadId,
        );
        if (original !== undefined) {
          if (
            original.run !== null ||
            canonicalJson(original.capture.canonicalCommand) !== canonicalJson(encoded)
          )
            return yield* failure(
              original.capture,
              "stale_admission",
              "Runless preparation differs from its original admission",
            );
          return original;
        }
        const contract = yield* capture({
          ...input.target,
          command,
          leaseId: `lease:${command.commandId}:preparation`,
        });
        if (
          contract.capture.threadId !== command.threadId ||
          contract.capture.origin.kind !== "command" ||
          !(
            (prepared.event.type === "thread.created" ||
              prepared.event.type === "thread.metadata-updated") &&
            prepared.event.payload.projectId === contract.capture.projectId &&
            prepared.event.payload.branch === contract.capture.branch &&
            prepared.event.payload.worktreePath === contract.source.worktreePath
          )
        )
          return yield* failure(
            contract.capture,
            "stale_admission",
            "Runless capture does not retain its actual preparation target",
          );
        const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
        const rows = yield* sql`INSERT INTO worktree_ownership_leases
    (resource_path, lease_id, owner_thread_id, owner_incarnation, branch,
      acquired_at_ms, renewed_at_ms, expires_at_ms)
    SELECT ${contract.capture.canonicalCheckoutPath}, ${contract.capture.lease.leaseId}, ${contract.capture.threadId},
      json_array('t3.orchestration-v2.thread-birth/v1', event_id, sequence),
      ${contract.capture.branch}, ${nowMs}, ${nowMs}, ${nowMs + WORKTREE_OWNERSHIP_LEASE_DURATION_MS}
    FROM orchestration_events
    WHERE aggregate_kind = 'thread' AND application_event_version = 2
      AND stream_id = ${contract.capture.threadId} AND event_type = 'thread.created'
      AND event_id = ${contract.capture.applicationBirth.eventId} AND sequence = ${contract.capture.applicationBirth.sequence}
      AND EXISTS (SELECT 1 FROM orchestration_command_receipts WHERE command_id = ${command.commandId}
        AND aggregate_kind = 'thread' AND aggregate_id = ${command.threadId} AND command_type = ${command.type} AND status = 'accepted')
    ON CONFLICT(resource_path) DO UPDATE SET renewed_at_ms = excluded.renewed_at_ms,
      expires_at_ms = excluded.expires_at_ms
    WHERE worktree_ownership_leases.owner_thread_id = excluded.owner_thread_id
      AND worktree_ownership_leases.owner_incarnation = excluded.owner_incarnation
      AND worktree_ownership_leases.lease_id = excluded.lease_id
      AND worktree_ownership_leases.branch IS excluded.branch
    RETURNING resource_path AS "resourcePath", lease_id AS "leaseId",
      owner_thread_id AS "ownerThreadId", owner_incarnation AS "ownerIncarnation", branch,
      acquired_at_ms AS "acquiredAtMs", renewed_at_ms AS "renewedAtMs", expires_at_ms AS "expiresAtMs"`;
        if (rows.length !== 1)
          return yield* failure(
            contract.capture,
            "target_changed",
            "The accepted runless birth lost its exact physical lease.",
          );
        const acquired = {
          ...contract,
          capture: { ...contract.capture, lease: yield* decodeLease(rows[0]) },
        };
        yield* current(acquired);
        const active = yield* sql`SELECT operation_id FROM orchestration_v2_worktree_path_admissions
          WHERE canonical_path = ${contract.capture.canonicalCheckoutPath} AND state NOT IN ('no_effect', 'released')`;
        if (active.length !== 0)
          return yield* failure(
            contract.capture,
            "unknown_use",
            "Another physical operation retains the runless preparation target.",
          );
        yield* recordAcceptance({
          captured: acquired,
          receipt,
          events: Array.from(events),
          effects: [],
        });
        const admission = (yield* readOrdinaryCheckoutAdmissionsEffect(command.commandId)).find(
          (item) => item.capture.threadId === command.threadId,
        );
        if (admission === undefined)
          return yield* new OrdinaryCheckoutEvidenceWriteError({
            commandId: command.commandId,
            eventCount: 0,
            cause: "Runless capture was not stored",
          });
        return admission;
      }),
    );
  });
  const resolveAdmission = Effect.fn("OrdinaryCheckoutStore.resolveAdmission")(function* (
    ref: Ordinary.OrdinaryCheckoutAdmissionRefV1,
  ) {
    const rows = yield* sql<{ readonly command_id: CommandId; readonly thread_id: ThreadId }>`
      SELECT command_id, thread_id FROM orchestration_v2_ordinary_checkout_admissions
      WHERE admission_id = ${ref.admissionId} AND admission_sha256 = ${ref.admissionSha256}`;
    const row = rows[0];
    if (rows.length !== 1 || row === undefined)
      return yield* new OrdinaryCheckoutRecordError({
        recordKind: "admission",
        recordId: ref.admissionId,
        message: "The exact original checkout admission is unavailable.",
      });
    const admission = yield* readAdmission(row.command_id, row.thread_id);
    if (admission === null)
      return yield* new OrdinaryCheckoutRecordError({
        recordKind: "admission",
        recordId: ref.admissionId,
        message: "The original checkout admission is unavailable.",
      });
    return admission;
  });

  const readUse = Effect.fn("OrdinaryCheckoutStore.readUse")(function* (operationId: string) {
    const rows = yield* sql<{
      readonly canonical_path: string;
      readonly kind: string;
      readonly subject_json: string;
      readonly state: OrdinaryCheckoutUseRecordV1["state"];
      readonly started_at: string | null;
    }>`SELECT canonical_path, kind, subject_json, state, started_at
      FROM orchestration_v2_worktree_path_admissions WHERE operation_id = ${operationId}`;
    if (rows.length === 0) return null;
    const row = rows[0]!;
    const subject = yield* decodeUseSubject(row.subject_json);
    const admission = yield* resolveAdmission(subject.use.admission);
    if (
      rows.length !== 1 ||
      row.kind !== "native_operation" ||
      subject.use.operationId !== operationId ||
      row.canonical_path !== admission.capture.canonicalCheckoutPath ||
      canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(subject.use.lease)) !==
        canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(admission.capture.lease))
    )
      return yield* failure(
        admission.capture,
        "stale_admission",
        "The physical use lost its original path or lease association.",
      );
    return {
      subject,
      state: row.state,
      startedAt: row.started_at,
    } satisfies OrdinaryCheckoutUseRecordV1;
  });

  const exactUse = Effect.fn("OrdinaryCheckoutStore.exactUse")(function* (
    use: Ordinary.OrdinaryCheckoutUseV1,
  ) {
    const record = yield* readUse(use.operationId);
    if (record === null || useBytes(record.subject.use) !== useBytes(use))
      return yield* new OrdinaryCheckoutRecordError({
        recordKind: "use",
        recordId: use.operationId,
        message: "The physical use does not match its immutable original subject.",
      });
    return record;
  });

  const assertNoUnresolvedPath = Effect.fn("OrdinaryCheckoutStore.assertNoUnresolvedPath")(
    function* (admission: Ordinary.OrdinaryCheckoutAdmissionV1, operationId?: string) {
      const holds = yield* outbox.listHeldByThreadId(admission.capture.threadId);
      if (holds.length > 0)
        return yield* failure(
          admission.capture,
          "unknown_use",
          "Unresolved native effect evidence retains this thread checkout.",
        );
      const barriers = yield* sql<{
        readonly operation_id: string;
        readonly kind: string;
        readonly state: string;
      }>`
      SELECT operation_id, kind, state FROM orchestration_v2_worktree_path_admissions
      WHERE canonical_path = ${admission.capture.canonicalCheckoutPath} AND state NOT IN ('no_effect', 'released')`;
      if (
        barriers.some(
          (row) =>
            row.operation_id !== operationId ||
            row.kind !== "native_operation" ||
            (row.state !== "reserved" && row.state !== "started"),
        )
      )
        return yield* failure(
          admission.capture,
          "unknown_use",
          "Another removal or unresolved native operation retains this checkout.",
        );
    },
  );

  const validateCapture = Effect.fn("OrdinaryCheckoutStore.validateCapture")(function* (
    capture: Ordinary.OrdinaryCheckoutCaptureV1,
    source: OrdinaryCheckoutCommitCapture["source"],
    use?: { readonly operationId: string; readonly requireLiveLease: boolean },
  ) {
    const lease = yield* current({ capture, source });
    const admission = yield* readAdmission(capture.commandId, capture.threadId);
    if (admission === null)
      return yield* failure(
        capture,
        "stale_admission",
        "The captured use has no original admission.",
      );
    yield* assertNoUnresolvedPath(admission, use?.operationId);
    if (
      (use?.requireLiveLease ?? true) &&
      lease.expiresAtMs <= DateTime.toEpochMillis(yield* DateTime.now)
    )
      return yield* failure(
        capture,
        "target_changed",
        "The captured checkout lease is no longer live.",
      );
    return lease;
  });
  const readCurrentProviderRuntimeOwnerEffect = Effect.fn(
    "OrdinaryCheckoutStore.readCurrentRuntimeOwner",
  )(function* (threadId: ThreadId) {
    const local = yield* projectionStore.getThreadRecords(threadId, [
      "providerThreads",
      "providerSessions",
    ]);
    const provider = local.providerThreads.find(
      (item) => item.id === local.thread.activeProviderThreadId,
    );
    const session = local.providerSessions.find((item) => item.id === provider?.providerSessionId);
    const identity = provider?.runtimeIdentity;
    if (
      local.thread.deletedAt !== null ||
      provider === undefined ||
      provider.appThreadId !== threadId ||
      session === undefined ||
      provider.providerSessionId === null ||
      session.providerInstanceId !== provider.providerInstanceId ||
      session.driver !== provider.driver ||
      provider.nativeThreadRef?.nativeId === undefined ||
      identity?.runtimeGeneration === undefined ||
      identity.evidenceRevision === undefined
    )
      return null;
    return {
      binding: {
        threadId,
        providerThreadId: provider.id,
        providerSessionId: provider.providerSessionId,
        instanceId: provider.providerInstanceId,
        driver: provider.driver,
        nativeThreadId: provider.nativeThreadRef.nativeId,
        runtimeGeneration: identity.runtimeGeneration,
      },
      evidenceRevision: identity.evidenceRevision,
    };
  });

  const validateSource = Effect.fnUntraced(function* (
    use: Ordinary.OrdinaryCheckoutUseV1,
    admission: Ordinary.OrdinaryCheckoutAdmissionV1,
    completion = false,
  ) {
    const source = use.source;
    yield* assertNoUnresolvedPath(admission, use.operationId);
    if (source.kind === "outbox") {
      const link = yield* readOrdinaryCheckoutEffectLinkEffect(source.link.effectId);
      const effect = Option.getOrNull(yield* outbox.get(source.link.effectId));
      const now = DateTime.formatIso(yield* DateTime.now);
      if (
        link === null ||
        effect === null ||
        (source.link.effectId !== use.operationId &&
          Ordinary.ordinaryCheckoutOutboxOperationIdV1(
            source.link.effectId,
            source.expectedAttempt,
          ) !== use.operationId) ||
        canonicalJson(
          yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutEffectLinkV1)(link).pipe(
            Effect.orDie,
          ),
        ) !==
          canonicalJson(
            yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutEffectLinkV1)(source.link).pipe(
              Effect.orDie,
            ),
          ) ||
        canonicalJson(link.admission) !== canonicalJson(use.admission) ||
        ![
          "delegated-workspace.prepare",
          "provider-turn.start",
          "provider-turn.restart",
          "provider-turn.steer",
          "provider-thread.rollback",
          "provider-runtime.continue",
          "runtime-request.respond",
          "checkpoint.capture",
        ].includes(effect.request.type) ||
        effect.attemptCount !== source.expectedAttempt ||
        (completion
          ? effect.status !== "succeeded" || effect.completedAt === null
          : effect.status !== "running" ||
            effect.leaseOwner !== source.workerId ||
            effect.leaseExpiresAt === null ||
            effect.leaseExpiresAt !== DateTime.formatIso(source.leaseExpiresAt) ||
            effect.leaseExpiresAt <= now)
      )
        return yield* failure(
          admission.capture,
          "claim_mismatch",
          "Captured checkout use has no exact real effect claim or completion",
        );
      return;
    }
    if (completion)
      return yield* failure(
        admission.capture,
        "unavailable",
        "Direct preparation completion requires its actual producer observation",
      );
    if (canonicalJson(source.admission) !== canonicalJson(use.admission))
      return yield* failure(
        admission.capture,
        "stale_admission",
        "Direct preparation lost its original checkout reference",
      );
    if (source.kind === "prepared_run") {
      const local = yield* projectionStore.getThreadRecords(admission.capture.threadId, [
        "runs",
        "attempts",
        "nodes",
        "messages",
      ]);
      const run = local.runs.find((item) => item.id === source.preparation.runId);
      const attempt = local.attempts.find((item) => item.id === source.preparation.runAttemptId);
      const node = local.nodes.find((item) => item.id === source.preparation.nodeId);
      const message = local.messages.find((item) => item.id === source.preparation.messageId);
      if (
        admission.run === null ||
        canonicalJson(admission.run) !== canonicalJson(source.preparation) ||
        run?.activeAttemptId !== source.preparation.runAttemptId ||
        run.rootNodeId !== source.preparation.nodeId ||
        run.userMessageId !== source.preparation.messageId ||
        attempt?.runId !== run.id ||
        attempt.rootNodeId !== node?.id ||
        node?.runId !== run.id ||
        message?.runId !== run.id ||
        message.role !== "user"
      )
        return yield* failure(
          admission.capture,
          "stale_admission",
          "Direct prepared run no longer matches its accepted run, attempt, node and message",
        );
    } else if (
      admission.run !== null ||
      source.preparationCommandId !== admission.capture.commandId ||
      !admission.eventBasis.some(
        (event) => canonicalJson(event) === canonicalJson(source.preparationEvent),
      ) ||
      source.preparationEvent.threadId !== admission.capture.threadId ||
      source.preparationEvent.commandId !== admission.capture.commandId ||
      !["thread.created", "thread.metadata-updated"].includes(source.preparationEvent.eventType) ||
      canonicalJson(source.applicationBirth) !==
        canonicalJson(admission.capture.applicationBirth) ||
      source.projectId !== admission.capture.projectId ||
      source.canonicalProjectRoot !== admission.capture.canonicalProjectRoot ||
      source.canonicalCheckoutPath !== admission.capture.canonicalCheckoutPath ||
      source.branch !== admission.capture.branch ||
      !["thread.create", "thread.metadata.update"].includes(admission.capture.commandType)
    )
      return yield* failure(
        admission.capture,
        "stale_admission",
        "Direct runless launch does not match its actual accepted preparation",
      );
  });

  const validateOrdinaryCheckoutJoin = Effect.fnUntraced(function* (
    use: Ordinary.OrdinaryCheckoutUseV1,
    admission: Ordinary.OrdinaryCheckoutAdmissionV1,
    source: OrdinaryCheckoutCommitCapture["source"],
    requireEntered = false,
  ) {
    const record = yield* exactUse(use);
    if (
      (record.state !== "started" && (requireEntered || record.state !== "reserved")) ||
      admission.run === null ||
      canonicalJson(use.admission) !==
        canonicalJson(Ordinary.ordinaryCheckoutAdmissionRefV1(admission)) ||
      canonicalJson(record.subject.source) !== canonicalJson(source)
    )
      return yield* failure(
        admission.capture,
        "unknown_use",
        "Joined checkout work has no exact current original-run use",
      );
    yield* validateSource(use, admission);
    if (use.source.kind === "prepared_run") {
      if (use.source.preparation.runId !== admission.run.runId)
        return yield* failure(
          admission.capture,
          "stale_admission",
          "Joined preparation belongs to another run",
        );
    } else if (use.source.kind === "outbox") {
      const effect = Option.getOrNull(yield* outbox.get(use.source.link.effectId));
      if (
        effect === null ||
        !("runId" in effect.request) ||
        effect.request.runId !== admission.run.runId
      )
        return yield* failure(
          admission.capture,
          "claim_mismatch",
          "Joined effect has no exact original-run association",
        );
    } else
      return yield* failure(
        admission.capture,
        "stale_admission",
        "Runless preparation cannot supply a same-run join",
      );
    yield* validateCapture(admission.capture, source, {
      operationId: use.operationId,
      requireLiveLease: requireEntered,
    });
    return record;
  });

  const beginUse = Effect.fn("OrdinaryCheckoutStore.beginUse")(function* (input: {
    readonly operationId: string;
    readonly admission: Ordinary.OrdinaryCheckoutAdmissionRefV1;
    readonly source: Ordinary.OrdinaryCheckoutUseSourceV1;
    readonly targetSource: OrdinaryCheckoutCommitCapture["source"];
  }) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const admission = yield* resolveAdmission(input.admission);
        const subject: typeof OrdinaryCheckoutUseSubjectV1.Type = {
          schema: "t3.ordinary-checkout-use/v1",
          source: input.targetSource,
          use: {
            version: 1,
            kind: "ordinary_checkout_use",
            operationId: input.operationId,
            admission: input.admission,
            source: input.source,
            lease: admission.capture.lease,
          },
        };
        const existing = yield* readUse(input.operationId);
        if (existing !== null) {
          if (subjectBytes(existing.subject) !== subjectBytes(subject))
            return yield* failure(
              admission.capture,
              "unknown_use",
              "The existing physical operation has another original subject.",
            );
          return { status: "observe_only" as const, record: existing };
        }
        yield* current({ capture: admission.capture, source: input.targetSource });
        yield* validateSource(subject.use, admission);
        const now = DateTime.formatIso(yield* DateTime.now);
        const json = yield* encodeUseSubject(subject);
        yield* sql`INSERT INTO orchestration_v2_worktree_path_admissions
        (operation_id, canonical_path, kind, subject_json, state, started_at, outcome_json, recorded_at, updated_at)
        VALUES (${input.operationId}, ${admission.capture.canonicalCheckoutPath}, 'native_operation',
          ${json}, 'reserved', NULL, NULL, ${now}, ${now})`;
        return {
          status: "reserved" as const,
          record: { subject, state: "reserved" as const, startedAt: null },
        };
      }),
    );
  });

  const revalidateUse = Effect.fn("OrdinaryCheckoutStore.revalidateUse")(function* (
    use: Ordinary.OrdinaryCheckoutUseV1,
  ) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const record = yield* exactUse(use);
        const admission = yield* resolveAdmission(use.admission);
        if (record.state !== "reserved" && record.state !== "started")
          return yield* failure(
            admission.capture,
            "unknown_use",
            "The physical operation is no longer eligible to enter.",
          );
        const lease = yield* current({ capture: admission.capture, source: record.subject.source });
        const now = yield* DateTime.now;
        if (lease.expiresAtMs <= DateTime.toEpochMillis(now))
          return yield* failure(
            admission.capture,
            "target_changed",
            "The original checkout lease is no longer live.",
          );
        yield* validateSource(use, admission);
        if (record.state === "started") return record;
        const at = DateTime.formatIso(now);
        const rows = yield* sql`UPDATE orchestration_v2_worktree_path_admissions
        SET state = 'started', started_at = ${at}, updated_at = ${at}
        WHERE operation_id = ${use.operationId} AND state = 'reserved' AND started_at IS NULL RETURNING operation_id`;
        if (rows.length !== 1)
          return yield* failure(
            admission.capture,
            "unknown_use",
            "The physical use changed before entry.",
          );
        return { ...record, state: "started" as const, startedAt: at };
      }),
    );
  });

  const markUnknown = Effect.fn("OrdinaryCheckoutStore.markUnknown")(function* (
    use: Ordinary.OrdinaryCheckoutUseV1,
    reason: string,
  ) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const record = yield* exactUse(use);
        if (record.state === "unknown") return record;
        const admission = yield* resolveAdmission(use.admission);
        if (record.state !== "reserved" && record.state !== "started")
          return yield* failure(
            admission.capture,
            "unknown_use",
            "A settled use cannot be replaced with an unknown outcome.",
          );
        const at = DateTime.formatIso(yield* DateTime.now);
        const outcome = canonicalJson({
          schema: "t3.ordinary-checkout-outcome/v1",
          kind: "unknown",
          operationId: use.operationId,
          admission: use.admission,
          reason,
          observedAt: at,
        });
        const rows = yield* sql`UPDATE orchestration_v2_worktree_path_admissions
        SET state = 'unknown', outcome_json = ${outcome}, updated_at = ${at}
        WHERE operation_id = ${use.operationId} AND state = ${record.state} RETURNING operation_id`;
        if (rows.length !== 1)
          return yield* failure(
            admission.capture,
            "unknown_use",
            "The physical outcome changed before its unknown observation.",
          );
        return { ...record, state: "unknown" as const };
      }),
    );
  });

  const ordinaryUseBytes = useBytes;
  const ordinaryExecutionBytes = (ref: Ordinary.OrdinaryCheckoutExecutionRefV1) =>
    canonicalJson(encodeExecution(ref));
  const readOrdinaryCheckoutExecutionAssociationsEffect = Effect.fnUntraced(function* (
    originalUse: Ordinary.OrdinaryCheckoutUseV1,
  ) {
    yield* exactUse(originalUse);
    const rows = yield* sql<{
      readonly operation_id: string;
      readonly ordinal: number;
      readonly predecessor_ordinal: number | null;
      readonly association_id: string;
      readonly admission_id: string;
      readonly executor_kind: string;
      readonly effect_id: string | null;
      readonly event_kind: string;
      readonly association_json: string;
      readonly evidence_json: string;
      readonly recorded_at: string;
    }>`
      SELECT * FROM orchestration_v2_ordinary_checkout_execution_associations WHERE operation_id = ${originalUse.operationId} ORDER BY ordinal`;
    const facts = yield* Effect.forEach(
      rows,
      (row, ordinal) =>
        Effect.gen(function* () {
          const ref = yield* Schema.decodeUnknownEffect(
            Schema.fromJsonString(Ordinary.OrdinaryCheckoutExecutionRefV1),
          )(row.association_json, { onExcessProperty: "error" });
          const evidence = yield* Schema.decodeUnknownEffect(
            Schema.fromJsonString(OrdinaryCheckoutExecutionEvidenceV1),
          )(row.evidence_json, { onExcessProperty: "error" });
          const expiresAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
            evidence.expiresAt,
          );
          const recordedAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
            row.recorded_at,
          );
          if (
            row.ordinal !== ordinal ||
            row.predecessor_ordinal !== (ordinal === 0 ? null : ordinal - 1) ||
            row.association_id !== ref.associationId ||
            row.admission_id !== originalUse.admission.admissionId ||
            row.executor_kind !== ref.executor.kind ||
            row.event_kind !== evidence.kind ||
            row.operation_id !== originalUse.operationId ||
            row.effect_id !==
              (ref.executor.kind === "actual_outbox_claim"
                ? ref.executor.source.link.effectId
                : null) ||
            ordinaryUseBytes(ref.originalUse) !== ordinaryUseBytes(originalUse) ||
            DateTime.formatIso(expiresAt) !== evidence.expiresAt ||
            DateTime.formatIso(recordedAt) !== row.recorded_at
          )
            return yield* new OrdinaryCheckoutHistoryError({
              message: "Ordinary execution history lost its exact immutable succession",
            });
          if (
            evidence.schema === "t3.ordinary-checkout-execution-liveness/v1" &&
            ((evidence.kind === "renew" && evidence.previousExpiry === undefined) ||
              (evidence.kind !== "renew" && evidence.previousExpiry !== undefined))
          )
            return yield* new OrdinaryCheckoutHistoryError({
              message: "Ordinary execution liveness has no exact predecessor deadline",
            });
          if (
            evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            (evidence.kind === "unknown") !== (evidence.actualProducerOutcome.kind === "unknown")
          )
            return yield* new OrdinaryCheckoutHistoryError({
              message: "Ordinary execution outcome has no matching actual result",
            });
          if (
            evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            evidence.actualProducerOutcome.kind === "outbox_completed"
          ) {
            const outcome = evidence.actualProducerOutcome;
            if (
              ref.executor.kind !== "actual_outbox_claim" ||
              outcome.effectId !== ref.executor.source.link.effectId ||
              outcome.workerId !== ref.executor.source.workerId ||
              outcome.expectedAttempt !== ref.executor.source.expectedAttempt ||
              DateTime.formatIso(outcome.completedAt) > evidence.expiresAt
            )
              return yield* new OrdinaryCheckoutHistoryError({
                message: "Ordinary completion lost its exact original executor correlation",
              });
          }
          if (
            evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            (evidence.actualProducerOutcome.kind === "start_failed_before_open" ||
              evidence.actualProducerOutcome.kind === "start_retry_before_open")
          ) {
            const observation = evidence.actualProducerOutcome.observation;
            if (
              ref.executor.kind !== "actual_outbox_claim" ||
              ordinaryExecutionBytes(observation.execution) !== ordinaryExecutionBytes(ref) ||
              observation.completedAt > evidence.expiresAt ||
              observation.nativeEffect.outcome !== "known_no_effect"
            )
              return yield* new OrdinaryCheckoutHistoryError({
                message: "Failed start retirement lost its exact original executor correlation",
              });
          }
          if (
            evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            evidence.actualProducerOutcome.kind === "checkpoint_captured"
          ) {
            const observation = evidence.actualProducerOutcome.observation;
            if (
              observation.ordinaryCheckoutExecution?.executor.kind !== "actual_outbox_claim" ||
              (ref.executor.kind !== "actual_outbox_claim" &&
                ref.executor.kind !== "captured_managed_run") ||
              ordinaryUseBytes(observation.ordinaryCheckoutExecution.originalUse) !==
                ordinaryUseBytes(ref.originalUse) ||
              (ref.executor.kind === "actual_outbox_claim" &&
                ordinaryExecutionBytes(observation.ordinaryCheckoutExecution) !==
                  ordinaryExecutionBytes(ref)) ||
              observation.checkpoint.status !== "ready" ||
              DateTime.formatIso(observation.commit.receipt.acceptedAt) > evidence.expiresAt
            )
              return yield* new OrdinaryCheckoutHistoryError({
                message: "Checkpoint completion lost its original issued executor and deadline",
              });
          }
          if (
            evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            (evidence.actualProducerOutcome.kind === "prepared_completed" ||
              evidence.actualProducerOutcome.kind === "prepared_failed")
          ) {
            const observation = evidence.actualProducerOutcome.observation;
            const observedAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
              observation.observedAt,
            );
            if (
              ref.executor.kind !== "actual_prepared_producer" ||
              observation.producerId !== ref.executor.producerId ||
              ordinaryExecutionBytes(observation.execution) !== ordinaryExecutionBytes(ref) ||
              DateTime.formatIso(observedAt) !== observation.observedAt ||
              observation.observedAt > evidence.expiresAt ||
              observation.observedAt > row.recorded_at
            )
              return yield* new OrdinaryCheckoutHistoryError({
                message: "Prepared completion lost its exact original producer and deadline",
              });
          }
          if (
            evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            evidence.actualProducerOutcome.kind === "rollback_completed"
          ) {
            const observation = evidence.actualProducerOutcome.observation;
            const completedAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
              observation.completedAt,
            );
            if (
              ref.executor.kind !== "actual_outbox_claim" ||
              ordinaryExecutionBytes(observation.execution) !== ordinaryExecutionBytes(ref) ||
              observation.sourceEffect.effectId !== ref.executor.source.link.effectId ||
              observation.sourceEffect.commandId !== ref.executor.source.link.commandId ||
              DateTime.formatIso(completedAt) !== observation.completedAt ||
              observation.completedAt > evidence.expiresAt ||
              observation.completedAt > row.recorded_at
            )
              return yield* new OrdinaryCheckoutHistoryError({
                message: "Rollback completion lost its exact original claim and deadline",
              });
          }
          if (
            evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            evidence.actualProducerOutcome.kind === "managed_mutations_finished"
          ) {
            const closure = evidence.actualProducerOutcome.observation;
            const closedAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
              closure.closedAt,
            );
            if (
              ref.executor.kind !== "captured_managed_run" ||
              ordinaryExecutionBytes(closure.managedExecution) !== ordinaryExecutionBytes(ref) ||
              DateTime.formatIso(closedAt) !== closure.closedAt ||
              closure.closedAt > evidence.expiresAt ||
              closure.closedAt > row.recorded_at
            )
              return yield* new OrdinaryCheckoutHistoryError({
                message: "Managed closure lost its exact original executor and deadline",
              });
          }
          if (evidence.schema === "t3.ordinary-checkout-execution-activation/v1") {
            const observation = evidence.actualStartObservation;
            const observedAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
              observation.observedAt,
            );
            if (
              ref.executor.kind !== "captured_managed_run" ||
              observation.managedExecutor.kind !== "captured_managed_run" ||
              observation.startExecution.executor.kind !== "actual_outbox_claim" ||
              ordinaryUseBytes(observation.startExecution.originalUse) !==
                ordinaryUseBytes(originalUse) ||
              canonicalJson(
                yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutExecutionExecutorV1)(
                  observation.managedExecutor,
                ).pipe(Effect.orDie),
              ) !==
                canonicalJson(
                  yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutExecutionExecutorV1)(
                    ref.executor,
                  ).pipe(Effect.orDie),
                ) ||
              DateTime.formatIso(observedAt) !== observation.observedAt ||
              observation.observedAt > row.recorded_at
            )
              return yield* new OrdinaryCheckoutHistoryError({
                message: "Managed activation lost its actual original start observation",
              });
          }
          if (
            evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            evidence.actualProducerOutcome.kind === "start_activated"
          ) {
            const outcome = evidence.actualProducerOutcome;
            if (
              ref.executor.kind !== "actual_outbox_claim" ||
              ordinaryExecutionBytes(outcome.actualStartObservation.startExecution) !==
                ordinaryExecutionBytes(ref) ||
              outcome.managedExecution.executor.kind !== "captured_managed_run" ||
              ordinaryUseBytes(outcome.managedExecution.originalUse) !==
                ordinaryUseBytes(originalUse)
            )
              return yield* new OrdinaryCheckoutHistoryError({
                message: "Start settlement lost its exact managed successor",
              });
          }
          return {
            ordinal,
            predecessorOrdinal: row.predecessor_ordinal,
            ref,
            eventKind: evidence.kind,
            evidence,
            recordedAt: row.recorded_at,
          } satisfies OrdinaryCheckoutExecutionAssociationFactV1;
        }),
      { concurrency: 1 },
    );
    const participants = new Map<
      string,
      OrdinaryCheckoutExecutionAssociationsV1["participants"][number]
    >();
    for (const fact of facts) {
      const prior = participants.get(fact.ref.associationId);
      const opening =
        fact.eventKind === "bind" || fact.eventKind === "activate" || fact.eventKind === "join";
      if (
        (prior !== undefined &&
          ordinaryExecutionBytes(prior.ref) !== ordinaryExecutionBytes(fact.ref)) ||
        (fact.evidence.schema === "t3.ordinary-checkout-execution-liveness/v1" &&
          fact.eventKind === "renew" &&
          (prior?.state !== "active" ||
            fact.evidence.previousExpiry !== prior.expiresAt ||
            fact.evidence.expiresAt < prior.expiresAt)) ||
        (opening && prior !== undefined) ||
        (!opening && prior === undefined) ||
        (fact.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
          prior?.state !== "active") ||
        (!opening &&
          fact.eventKind !== "renew" &&
          prior !== undefined &&
          fact.evidence.expiresAt !== prior.expiresAt)
      )
        return yield* new OrdinaryCheckoutHistoryError({
          message:
            "Ordinary execution participant was replaced or revived without its actual predecessor",
        });
      participants.set(fact.ref.associationId, {
        ref: fact.ref,
        state:
          fact.eventKind === "retire"
            ? "retired"
            : fact.eventKind === "unknown"
              ? "unknown"
              : "active",
        latestOrdinal: fact.ordinal,
        expiresAt: fact.evidence.expiresAt,
      });
    }
    for (const fact of facts) {
      if (fact.evidence.schema !== "t3.ordinary-checkout-execution-activation/v1") continue;
      const observation = fact.evidence.actualStartObservation;
      const preceding = facts
        .slice(0, fact.ordinal)
        .findLast(
          (candidate) => candidate.ref.associationId === observation.startExecution.associationId,
        );
      const settlement = facts[fact.ordinal + 1];
      if (
        preceding === undefined ||
        preceding.eventKind === "retire" ||
        preceding.eventKind === "unknown" ||
        settlement?.evidence.schema !== "t3.ordinary-checkout-execution-outcome/v1" ||
        settlement.evidence.actualProducerOutcome.kind !== "start_activated" ||
        ordinaryExecutionBytes(settlement.ref) !==
          ordinaryExecutionBytes(fact.evidence.actualStartObservation.startExecution) ||
        ordinaryExecutionBytes(settlement.evidence.actualProducerOutcome.managedExecution) !==
          ordinaryExecutionBytes(fact.ref) ||
        canonicalJson(
          yield* Schema.encodeEffect(OrdinaryManagedStartObservationV1)(
            settlement.evidence.actualProducerOutcome.actualStartObservation,
          ).pipe(Effect.orDie),
        ) !==
          canonicalJson(
            yield* Schema.encodeEffect(OrdinaryManagedStartObservationV1)(
              fact.evidence.actualStartObservation,
            ).pipe(Effect.orDie),
          )
      )
        return yield* new OrdinaryCheckoutHistoryError({
          message: "Managed activation and exact original start retirement were not atomic",
        });
    }
    for (const fact of facts) {
      if (
        fact.evidence.schema !== "t3.ordinary-checkout-execution-liveness/v1" ||
        fact.evidence.completionBasis === undefined
      )
        continue;
      const basis = fact.evidence.completionBasis;
      if (
        fact.eventKind !== "join" ||
        basis.joinOrdinal !== fact.ordinal ||
        ordinaryExecutionBytes(basis.checkpointExecution) !== ordinaryExecutionBytes(fact.ref) ||
        fact.ref.executor.kind !== "actual_outbox_claim" ||
        fact.ref.executor.source.link.effectId !== basis.effectId ||
        basis.managedRetirements.length === 0 ||
        new Set(basis.managedRetirements.map((item) => item.managedExecution.associationId))
          .size !== basis.managedRetirements.length
      )
        return yield* new OrdinaryCheckoutHistoryError({
          message: "Final checkpoint has no exact atomic joined completion basis",
        });
      for (const [index, retirement] of basis.managedRetirements.entries()) {
        const retired = facts[retirement.retirementOrdinal];
        if (
          retirement.retirementOrdinal !== fact.ordinal + index + 1 ||
          retired?.eventKind !== "retire" ||
          ordinaryExecutionBytes(retired.ref) !==
            ordinaryExecutionBytes(retirement.managedExecution) ||
          retired.evidence.schema !== "t3.ordinary-checkout-execution-outcome/v1" ||
          retired.evidence.actualProducerOutcome.kind !== "managed_mutations_finished" ||
          retirement.closureSha256 !==
            sha256(
              canonicalJson(
                yield* Schema.encodeEffect(ProviderManagedActorClosureV1)(
                  retired.evidence.actualProducerOutcome.observation,
                ).pipe(Effect.orDie),
              ),
            )
        )
          return yield* new OrdinaryCheckoutHistoryError({
            message: "Final checkpoint join preceded no qualified exact managed retirement",
          });
      }
    }
    for (const fact of facts) {
      if (
        fact.ref.executor.kind === "captured_managed_run" &&
        fact.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
        fact.evidence.actualProducerOutcome.kind === "checkpoint_captured"
      ) {
        const outcome = fact.evidence.actualProducerOutcome;
        const checkpointExecution = outcome.observation.ordinaryCheckoutExecution;
        if (
          checkpointExecution === undefined ||
          !facts.some(
            (opening) =>
              opening.ordinal < fact.ordinal &&
              ordinaryExecutionBytes(opening.ref) === ordinaryExecutionBytes(fact.ref) &&
              opening.evidence.schema === "t3.ordinary-checkout-execution-activation/v1" &&
              opening.evidence.actualStartObservation.settlementMode ===
                "primary_terminal_checkpoint",
          ) ||
          !facts.some(
            (completion) =>
              completion.ordinal < fact.ordinal &&
              completion.eventKind === "retire" &&
              ordinaryExecutionBytes(completion.ref) ===
                ordinaryExecutionBytes(checkpointExecution) &&
              completion.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
              canonicalJson(
                Schema.encodeSync(OrdinaryCheckoutExecutorOutcomeV1)(
                  completion.evidence.actualProducerOutcome,
                ),
              ) === canonicalJson(Schema.encodeSync(OrdinaryCheckoutExecutorOutcomeV1)(outcome)),
          )
        )
          return yield* new OrdinaryCheckoutHistoryError({
            message:
              "Default managed settlement has no original mode and preceding exact checkpoint result",
          });
      }
      if (
        fact.evidence.schema !== "t3.ordinary-checkout-execution-outcome/v1" ||
        fact.evidence.actualProducerOutcome.kind !== "managed_mutations_finished"
      )
        continue;
      if (
        !facts.some(
          (joined) =>
            joined.evidence.schema === "t3.ordinary-checkout-execution-liveness/v1" &&
            joined.evidence.completionBasis?.managedRetirements.some(
              (retired) =>
                retired.retirementOrdinal === fact.ordinal &&
                ordinaryExecutionBytes(retired.managedExecution) ===
                  ordinaryExecutionBytes(fact.ref),
            ),
        )
      )
        return yield* new OrdinaryCheckoutHistoryError({
          message: "Managed retirement has no preceding real final checkpoint handoff",
        });
    }
    return {
      originalUse,
      latestOrdinal: facts.length - 1,
      facts,
      participants: [...participants.values()],
    } satisfies OrdinaryCheckoutExecutionAssociationsV1;
  });
  const validateOrdinaryCheckoutExecutionAttribution = Effect.fnUntraced(function* (
    input: Ordinary.OrdinaryCheckoutExecutionRefV1,
    admission: Ordinary.OrdinaryCheckoutAdmissionV1,
    plainUse?: Ordinary.OrdinaryCheckoutUseV1,
  ) {
    const ref = yield* Schema.decodeUnknownEffect(
      Schema.toType(Ordinary.OrdinaryCheckoutExecutionRefV1),
    )(input, { onExcessProperty: "error" });
    if (
      canonicalJson(ref.originalUse.admission) !==
        canonicalJson(Ordinary.ordinaryCheckoutAdmissionRefV1(admission)) ||
      (plainUse !== undefined && ordinaryUseBytes(plainUse) !== ordinaryUseBytes(ref.originalUse))
    )
      return yield* failure(
        admission.capture,
        "stale_admission",
        "Executor attribution differs from its immutable original use",
      );
    const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(ref.originalUse);
    if (
      !history.facts.some(
        (fact) => ordinaryExecutionBytes(fact.ref) === ordinaryExecutionBytes(ref),
      )
    )
      return yield* failure(
        admission.capture,
        "unknown_use",
        "Executor attribution has no actual immutable association",
      );
    return ref;
  });
  const appendOrdinaryCheckoutExecutionFact = Effect.fnUntraced(function* (
    ref: Ordinary.OrdinaryCheckoutExecutionRefV1,
    kind: OrdinaryCheckoutExecutionAssociationFactV1["eventKind"],
    expiresAt: string,
    previousExpiry?: string,
    actualProducerOutcome?: OrdinaryCheckoutExecutorOutcomeV1,
    actualStartObservation?: typeof OrdinaryManagedStartObservationV1.Type,
  ) {
    const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(ref.originalUse);
    const ordinal = history.latestOrdinal + 1;
    const now = DateTime.formatIso(yield* DateTime.now);
    const evidence = yield* Schema.decodeUnknownEffect(
      Schema.toType(OrdinaryCheckoutExecutionEvidenceV1),
    )(
      kind === "activate"
        ? {
            version: 1,
            schema: "t3.ordinary-checkout-execution-activation/v1",
            kind,
            expiresAt,
            actualStartObservation,
          }
        : kind === "retire" || kind === "unknown"
          ? {
              version: 1,
              schema: "t3.ordinary-checkout-execution-outcome/v1",
              kind,
              expiresAt,
              actualProducerOutcome,
            }
          : {
              version: 1,
              schema: "t3.ordinary-checkout-execution-liveness/v1",
              kind,
              expiresAt,
              ...(previousExpiry === undefined ? {} : { previousExpiry }),
            },
      { onExcessProperty: "error" },
    );
    yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations
      (operation_id, ordinal, predecessor_ordinal, association_id, admission_id, executor_kind, effect_id, event_kind, association_json, evidence_json, recorded_at)
      VALUES (${ref.originalUse.operationId}, ${ordinal}, ${history.latestOrdinal < 0 ? null : history.latestOrdinal}, ${ref.associationId},
        ${ref.originalUse.admission.admissionId}, ${ref.executor.kind}, ${ref.executor.kind === "actual_outbox_claim" ? ref.executor.source.link.effectId : null},
        ${kind}, ${ordinaryExecutionBytes(ref)}, ${canonicalJson(yield* Schema.encodeEffect(OrdinaryCheckoutExecutionEvidenceV1)(evidence).pipe(Effect.orDie))}, ${now})`;
    return ordinal;
  });

  const validateOrdinaryManagedExecutor = Effect.fnUntraced(function* (
    executor: Extract<
      Ordinary.OrdinaryCheckoutExecutionExecutorV1,
      { readonly kind: "captured_managed_run" }
    >,
    admission: Ordinary.OrdinaryCheckoutAdmissionV1,
  ) {
    if (
      admission.run === null ||
      canonicalJson(executor.run) !== canonicalJson(admission.run) ||
      executor.binding.threadId !== admission.capture.threadId
    )
      return yield* failure(
        admission.capture,
        "stale_admission",
        "Managed executor belongs to another original accepted run",
      );
    const local = yield* projectionStore.getThreadRecords(admission.capture.threadId, [
      "runs",
      "attempts",
      "nodes",
      "checkpointScopes",
      "providerThreads",
      "providerTurns",
    ]);
    const run = local.runs.find((item) => item.id === executor.run.runId);
    const provider = local.providerThreads.find(
      (item) => item.id === executor.binding.providerThreadId,
    );
    const scope = local.checkpointScopes.find((item) => item.id === executor.checkpointScopeId);
    if (
      run?.activeAttemptId !== executor.run.runAttemptId ||
      run.rootNodeId !== executor.run.nodeId ||
      run.userMessageId !== executor.run.messageId ||
      run.providerThreadId !== provider?.id ||
      provider?.appThreadId !== executor.binding.threadId ||
      provider.providerInstanceId !== executor.binding.instanceId ||
      provider.providerSessionId !== executor.binding.providerSessionId ||
      provider.driver !== executor.driver ||
      scope === undefined ||
      scope.threadId !== admission.capture.threadId ||
      scope.providerThreadId !== provider.id ||
      (executor.nativeThreadId !== undefined &&
        provider.nativeThreadRef?.nativeId !== executor.nativeThreadId)
    )
      return yield* failure(
        admission.capture,
        "stale_admission",
        "Managed executor lost its captured run, scope or provider attachment",
      );
    if (executor.runtimeGeneration !== undefined || executor.evidenceRevision !== undefined) {
      const owner = yield* readCurrentProviderRuntimeOwnerEffect(admission.capture.threadId);
      if (
        (executor.evidenceRevision !== undefined && owner === null) ||
        (owner !== null &&
          (owner.binding.providerThreadId !== executor.binding.providerThreadId ||
            owner.binding.providerSessionId !== executor.binding.providerSessionId ||
            owner.binding.instanceId !== executor.binding.instanceId ||
            owner.binding.driver !== executor.driver ||
            (executor.runtimeGeneration !== undefined &&
              owner.binding.runtimeGeneration !== executor.runtimeGeneration) ||
            (executor.evidenceRevision !== undefined &&
              owner.evidenceRevision !== executor.evidenceRevision) ||
            (executor.nativeThreadId !== undefined &&
              owner.binding.nativeThreadId !== executor.nativeThreadId)))
      )
        return yield* failure(
          admission.capture,
          "stale_admission",
          "Actual captured runtime evidence changed",
        );
    }
    if (executor.providerTurnId !== undefined) {
      const turn = local.providerTurns.find((item) => item.id === executor.providerTurnId);
      if (
        turn?.providerThreadId !== provider.id ||
        turn.runAttemptId !== executor.run.runAttemptId ||
        !local.nodes.some((node) => node.id === turn.nodeId && node.runId === run.id)
      )
        return yield* failure(
          admission.capture,
          "stale_admission",
          "Captured managed turn lost its actual run attribution",
        );
    }
  });
  const validateOrdinaryExecutionActor = Effect.fnUntraced(function* (
    ref: Ordinary.OrdinaryCheckoutExecutionRefV1,
    expiresAt: string,
    admission: Ordinary.OrdinaryCheckoutAdmissionV1,
  ) {
    const now = DateTime.formatIso(yield* DateTime.now);
    if (expiresAt <= now)
      return yield* failure(
        admission.capture,
        "claim_mismatch",
        "Expired ordinary execution cannot regain liveness",
      );
    const executor = ref.executor;
    yield* assertNoUnresolvedPath(admission, ref.originalUse.operationId);
    if (executor.kind === "actual_outbox_claim") {
      const source = executor.source;
      const link = yield* readOrdinaryCheckoutEffectLinkEffect(source.link.effectId);
      const effect = Option.getOrNull(yield* outbox.get(source.link.effectId));
      if (
        link === null ||
        effect?.status !== "running" ||
        effect.leaseOwner !== source.workerId ||
        effect.attemptCount !== source.expectedAttempt ||
        effect.leaseExpiresAt !== expiresAt ||
        effect.leaseExpiresAt <= now ||
        canonicalJson(
          yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutEffectLinkV1)(link).pipe(
            Effect.orDie,
          ),
        ) !==
          canonicalJson(
            yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutEffectLinkV1)(source.link).pipe(
              Effect.orDie,
            ),
          ) ||
        canonicalJson(link.admission) !== canonicalJson(ref.originalUse.admission) ||
        ![
          "delegated-workspace.prepare",
          "provider-turn.start",
          "provider-turn.restart",
          "provider-turn.steer",
          "provider-thread.rollback",
          "provider-runtime.continue",
          "runtime-request.respond",
          "checkpoint.capture",
        ].includes(effect.request.type)
      )
        return yield* failure(
          admission.capture,
          "claim_mismatch",
          "Ordinary executor has no exact current claimed effect",
        );
      if (admission.run !== null) {
        const local = yield* projectionStore.getThreadRecords(admission.capture.threadId, [
          "runs",
          "attempts",
          "nodes",
          "messages",
          "runtimeRequests",
          "providerThreads",
        ]);
        const run = local.runs.find((item) => item.id === admission.run!.runId);
        if (
          run === undefined ||
          run.activeAttemptId !== admission.run.runAttemptId ||
          run.rootNodeId !== admission.run.nodeId ||
          run.userMessageId !== admission.run.messageId
        )
          return yield* failure(
            admission.capture,
            "stale_admission",
            "Claimed ordinary work lost its original accepted run",
          );
        const request = effect.request;
        const matches =
          "runId" in request
            ? request.runId === run.id
            : request.type === "provider-runtime.continue"
              ? request.sourceRunId === run.id
              : request.type === "runtime-request.respond"
                ? local.runtimeRequests.some(
                    (item) =>
                      item.id === request.requestId &&
                      local.nodes.some((node) => node.id === item.nodeId && node.runId === run.id),
                  )
                : request.type === "provider-thread.rollback"
                  ? run.providerThreadId === request.providerThreadId
                  : request.type === "provider-turn.steer"
                    ? local.messages.some(
                        (item) => item.id === request.messageId && item.runId === run.id,
                      )
                    : false;
        if (!matches)
          return yield* failure(
            admission.capture,
            "stale_admission",
            "Claimed ordinary work belongs to another run or request",
          );
      } else
        return yield* failure(
          admission.capture,
          "stale_admission",
          "Runless preparation cannot acquire an arbitrary outbox executor",
        );
      return;
    }
    const callback = ordinaryExecutionCallbacks.get(ref.associationId);
    if (callback === undefined)
      return yield* failure(
        admission.capture,
        "unknown_use",
        "Original captured executor is unavailable in this process",
      );
    yield* callback;
    if (executor.kind === "actual_prepared_producer") {
      yield* validateSource({ ...ref.originalUse, source: executor.source }, admission);
      return;
    }
    yield* validateOrdinaryManagedExecutor(executor, admission);
  });
  const validateOrdinaryCheckoutExecutionEffect = Effect.fnUntraced(function* (
    input: Ordinary.OrdinaryCheckoutExecutionRefV1,
    enter = false,
    liveLease = true,
  ) {
    const ref = yield* Schema.decodeUnknownEffect(
      Schema.toType(Ordinary.OrdinaryCheckoutExecutionRefV1),
    )(input, { onExcessProperty: "error" });
    const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(ref.originalUse);
    const participant = history.participants.find(
      (item) => item.ref.associationId === ref.associationId,
    );
    const record = yield* exactUse(ref.originalUse);
    const admission = yield* resolveAdmission(ref.originalUse.admission);
    if (
      participant?.state !== "active" ||
      ordinaryExecutionBytes(participant.ref) !== ordinaryExecutionBytes(ref) ||
      (record.state !== "reserved" && record.state !== "started")
    )
      return yield* failure(
        admission.capture,
        "unknown_use",
        "Ordinary executor has no exact active original reservation",
      );
    yield* validateCapture(admission.capture, record.subject.source, {
      operationId: ref.originalUse.operationId,
      requireLiveLease: liveLease,
    });
    yield* validateOrdinaryExecutionActor(ref, participant.expiresAt, admission);
    if (enter && record.state === "reserved") {
      const now = DateTime.formatIso(yield* DateTime.now);
      const changed =
        yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'started', started_at = ${now}, updated_at = ${now}
        WHERE operation_id = ${ref.originalUse.operationId} AND state = 'reserved' AND started_at IS NULL RETURNING operation_id`;
      if (changed.length !== 1)
        return yield* failure(
          admission.capture,
          "unknown_use",
          "Ordinary reservation changed before execution entry",
        );
    }
    return { ref, history, participant, record, admission };
  });
  const bindOrdinaryCheckoutExecution = Effect.fnUntraced(function* <E>(input: {
    readonly originalUse: Ordinary.OrdinaryCheckoutUseV1;
    readonly executor: Ordinary.OrdinaryCheckoutExecutionExecutorV1;
    readonly targetSource: OrdinaryCheckoutCommitCapture["source"];
    readonly revalidateProducer?: Effect.Effect<void, E>;
  }) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const ref = yield* Schema.decodeUnknownEffect(
          Schema.toType(Ordinary.OrdinaryCheckoutExecutionRefV1),
        )(
          Ordinary.makeOrdinaryCheckoutExecutionRefV1({
            originalUse: input.originalUse,
            executor: input.executor,
          }),
          { onExcessProperty: "error" },
        );
        const record = yield* exactUse(ref.originalUse);
        const admission = yield* resolveAdmission(ref.originalUse.admission);
        const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(ref.originalUse);
        if (
          canonicalJson(record.subject.source) !== canonicalJson(input.targetSource) ||
          (record.state !== "reserved" && record.state !== "started") ||
          ref.executor.kind === "captured_managed_run" ||
          (ref.executor.kind === "actual_outbox_claim"
            ? canonicalJson(
                yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutOutboxExecutionSourceV1)(
                  ref.executor.source,
                ).pipe(Effect.orDie),
              ) !==
              canonicalJson(
                yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutUseSourceV1)(
                  ref.originalUse.source,
                ).pipe(Effect.orDie),
              )
            : canonicalJson(
                yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutPreparedExecutionSourceV1)(
                  ref.executor.source,
                ).pipe(Effect.orDie),
              ) !==
              canonicalJson(
                yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutUseSourceV1)(
                  ref.originalUse.source,
                ).pipe(Effect.orDie),
              ))
        )
          return yield* failure(
            admission.capture,
            "stale_admission",
            "Initial execution differs from its real original actor and target",
          );
        yield* validateCapture(admission.capture, record.subject.source, {
          operationId: ref.originalUse.operationId,
          requireLiveLease: false,
        });
        if (input.revalidateProducer !== undefined)
          yield* input.revalidateProducer.pipe(
            Effect.mapError(
              (cause) =>
                new OrdinaryCheckoutHistoryError({
                  message: "The captured producer no longer validates its original source.",
                  cause,
                }),
            ),
          );
        if (
          ref.executor.kind === "actual_prepared_producer" &&
          input.revalidateProducer === undefined
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Prepared execution has no actual captured producer",
          );
        const existing = history.participants.find(
          (item) => item.ref.associationId === ref.associationId,
        );
        if (existing !== undefined) {
          if (ordinaryExecutionBytes(existing.ref) !== ordinaryExecutionBytes(ref))
            return yield* failure(admission.capture, "unknown_use", "Execution identity collision");
          const now = DateTime.formatIso(yield* DateTime.now);
          if (existing.state !== "active" || existing.expiresAt <= now)
            return yield* failure(
              admission.capture,
              "claim_mismatch",
              "Expired or retired execution cannot be rebound",
            );
          if (ref.executor.kind === "actual_outbox_claim")
            yield* validateOrdinaryExecutionActor(ref, existing.expiresAt, admission);
          else yield* validateSource(ref.originalUse, admission);
          if (input.revalidateProducer !== undefined)
            yield* transactions.afterCommit(
              Effect.sync(() => {
                ordinaryExecutionCallbacks.set(
                  ref.associationId,
                  input.revalidateProducer!.pipe(
                    Effect.mapError(
                      (cause) =>
                        new OrdinaryCheckoutHistoryError({
                          message: "The original captured producer rejected revalidation.",
                          cause,
                        }),
                    ),
                  ),
                );
              }),
            );
          return ref;
        }
        if (history.facts.length !== 0)
          return yield* failure(
            admission.capture,
            "unknown_use",
            "New actor must join the actual original execution",
          );
        yield* validateSource(ref.originalUse, admission);
        const expiry =
          ref.executor.kind === "actual_outbox_claim"
            ? DateTime.formatIso(ref.executor.source.leaseExpiresAt)
            : DateTime.formatIso(DateTime.add(yield* DateTime.now, { minutes: 5 }));
        yield* appendOrdinaryCheckoutExecutionFact(ref, "bind", expiry);
        if (input.revalidateProducer !== undefined)
          yield* transactions.afterCommit(
            Effect.sync(() => {
              ordinaryExecutionCallbacks.set(
                ref.associationId,
                input.revalidateProducer!.pipe(
                  Effect.mapError(
                    (cause) =>
                      new OrdinaryCheckoutHistoryError({
                        message: "The original captured producer rejected revalidation.",
                        cause,
                      }),
                  ),
                ),
              );
            }),
          );
        return ref;
      }),
    );
  });

  const validateOrdinaryCheckpointOutcome = Effect.fnUntraced(function* (
    ref: Ordinary.OrdinaryCheckoutExecutionRefV1,
    outcome: Extract<OrdinaryCheckoutExecutorOutcomeV1, { readonly kind: "checkpoint_captured" }>,
    expiresAt: string,
  ) {
    const admission = yield* resolveAdmission(ref.originalUse.admission);
    const observation = outcome.observation;
    const checkpoint = observation.checkpoint;
    const executor = ref.executor;
    const now = yield* DateTime.now;
    if (
      executor.kind !== "actual_outbox_claim" ||
      admission.run === null ||
      observation.ordinaryCheckoutExecution === undefined ||
      ordinaryExecutionBytes(observation.ordinaryCheckoutExecution) !==
        ordinaryExecutionBytes(ref) ||
      checkpoint.status !== "ready" ||
      checkpoint.threadId !== admission.capture.threadId ||
      checkpoint.runId !== admission.run.runId ||
      checkpoint.nodeId !== admission.run.nodeId ||
      DateTime.toEpochMillis(checkpoint.capturedAt) > DateTime.toEpochMillis(now) ||
      DateTime.formatIso(checkpoint.capturedAt) > expiresAt ||
      DateTime.formatIso(observation.commit.receipt.acceptedAt) > expiresAt ||
      !observation.commit.committed ||
      observation.commit.receipt.status !== "accepted" ||
      observation.commit.receipt.error !== null ||
      observation.commit.receipt.commandType !== "checkpoint.capture" ||
      observation.commit.receipt.threadId !== admission.capture.threadId ||
      observation.commit.receipt.commandId !==
        `command:effect:checkpoint.capture:${admission.run.runId}`
    )
      return yield* failure(
        admission.capture,
        "unknown_use",
        "Final checkpoint is not an issued ready result of this exact executor",
      );
    const effect = Option.getOrNull(yield* outbox.get(executor.source.link.effectId));
    const link = yield* readOrdinaryCheckoutEffectLinkEffect(executor.source.link.effectId);
    if (
      effect?.request.type !== "checkpoint.capture" ||
      effect.threadId !== admission.capture.threadId ||
      effect.request.runId !== admission.run.runId ||
      effect.request.scopeId !== checkpoint.scopeId ||
      effect.attemptCount !== executor.source.expectedAttempt ||
      link === null ||
      canonicalJson(
        yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutEffectLinkV1)(link).pipe(Effect.orDie),
      ) !==
        canonicalJson(
          yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutEffectLinkV1)(
            executor.source.link,
          ).pipe(Effect.orDie),
        )
    )
      return yield* failure(
        admission.capture,
        "claim_mismatch",
        "Final checkpoint belongs to another real effect or original run",
      );
    const receipt = Option.getOrNull(
      yield* commandReceipts.getByCommandId(observation.commit.receipt.commandId),
    );
    const events = yield* eventStore
      .readByCommandId({ commandId: observation.commit.receipt.commandId })
      .pipe(Stream.runCollect);
    const encoded = yield* Schema.encodeEffect(OrdinaryCheckpointProducerObservationV1)(
      observation,
    ).pipe(Effect.orDie);
    const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(ref.originalUse);
    const handoff = history.facts.find(
      (fact) =>
        fact.evidence.schema === "t3.ordinary-checkout-execution-liveness/v1" &&
        fact.evidence.completionBasis !== undefined &&
        ordinaryExecutionBytes(fact.ref) === ordinaryExecutionBytes(ref),
    );
    if (
      handoff?.evidence.schema === "t3.ordinary-checkout-execution-liveness/v1" &&
      handoff.evidence.completionBasis !== undefined
    ) {
      const basis = handoff.evidence.completionBasis;
      const lastRetirement = history.facts[basis.joinOrdinal + basis.managedRetirements.length];
      if (
        observation.ordinaryFinalCheckpointBasis === undefined ||
        lastRetirement === undefined ||
        canonicalJson(
          yield* Schema.encodeEffect(OrdinaryFinalCheckpointCompletionBasisV1)(
            observation.ordinaryFinalCheckpointBasis,
          ).pipe(Effect.orDie),
        ) !==
          canonicalJson(
            yield* Schema.encodeEffect(OrdinaryFinalCheckpointCompletionBasisV1)(basis).pipe(
              Effect.orDie,
            ),
          ) ||
        DateTime.formatIso(checkpoint.capturedAt) < lastRetirement.recordedAt ||
        history.facts
          .slice(lastRetirement.ordinal + 1)
          .some(
            (fact) =>
              ordinaryExecutionBytes(fact.ref) !== ordinaryExecutionBytes(ref) ||
              (fact.eventKind !== "renew" &&
                !(
                  fact.eventKind === "retire" &&
                  fact.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
                  fact.evidence.actualProducerOutcome.kind === "checkpoint_captured"
                )),
          )
      )
        return yield* failure(
          admission.capture,
          "unknown_use",
          "Physical checkpoint did not follow the exact final managed mutation cohort",
        );
    } else if (observation.ordinaryFinalCheckpointBasis !== undefined)
      return yield* failure(
        admission.capture,
        "unknown_use",
        "Checkpoint observation supplied an uncommitted completion basis",
      );
    if (
      receipt === null ||
      canonicalJson({
        ...receipt,
        acceptedAt: DateTime.formatIso(receipt.acceptedAt),
      }) !== canonicalJson(encoded.commit.receipt) ||
      canonicalJson(
        events.map((event) => Schema.encodeSync(OrchestrationV2StoredEventJson)(event)),
      ) !== canonicalJson(encoded.commit.storedEvents)
    )
      return yield* failure(
        admission.capture,
        "unknown_use",
        "Issued checkpoint has no exact durable receipt and event batch",
      );
    const matching = events.filter(
      (stored) =>
        stored.commandId === receipt.commandId &&
        stored.sequence > 0 &&
        stored.sequence <= receipt.resultSequence &&
        stored.event.type === "checkpoint.captured" &&
        stored.event.threadId === checkpoint.threadId &&
        stored.event.runId === checkpoint.runId &&
        stored.event.nodeId === checkpoint.nodeId &&
        canonicalJson(Schema.encodeSync(OrchestrationV2CheckpointJson)(stored.event.payload)) ===
          canonicalJson(encoded.checkpoint),
    );
    const local = yield* projectionStore.getThreadRecords(admission.capture.threadId, [
      "runs",
      "checkpointScopes",
      "checkpoints",
    ]);
    const run = local.runs.find((item) => item.id === admission.run!.runId);
    const scope = local.checkpointScopes.find((item) => item.id === checkpoint.scopeId);
    const projected = local.checkpoints.find((item) => item.id === checkpoint.id);
    if (
      matching.length !== 1 ||
      run?.activeAttemptId !== admission.run.runAttemptId ||
      run.rootNodeId !== checkpoint.nodeId ||
      run.checkpointId !== checkpoint.id ||
      scope === undefined ||
      scope.threadId !== checkpoint.threadId ||
      projected === undefined ||
      canonicalJson(
        yield* Schema.encodeEffect(OrchestrationV2CheckpointJson)(projected).pipe(Effect.orDie),
      ) !== canonicalJson(encoded.checkpoint)
    )
      return yield* failure(
        admission.capture,
        "unknown_use",
        "Final physical checkpoint lost its exact committed run, scope or projection",
      );
    return { admission, effect };
  });
  const validateOrdinaryPreparedOutcome = Effect.fnUntraced(function* (
    ref: Ordinary.OrdinaryCheckoutExecutionRefV1,
    outcome: Extract<
      OrdinaryCheckoutExecutorOutcomeV1,
      { readonly kind: "prepared_completed" | "prepared_failed" }
    >,
    expiresAt: string,
  ) {
    const admission = yield* resolveAdmission(ref.originalUse.admission);
    const record = yield* exactUse(ref.originalUse);
    const observation = outcome.observation;
    const currentLease = yield* current({
      capture: admission.capture,
      source: record.subject.source,
    });
    const observedAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
      observation.observedAt,
    );
    const now = DateTime.formatIso(yield* DateTime.now);
    if (
      ref.executor.kind !== "actual_prepared_producer" ||
      observation.producerId !== ref.executor.producerId ||
      ordinaryExecutionBytes(observation.execution) !== ordinaryExecutionBytes(ref) ||
      canonicalJson(observation.targetSource) !== canonicalJson(record.subject.source) ||
      observation.checkoutPath !== admission.capture.canonicalCheckoutPath ||
      observation.branch !== currentLease.branch ||
      DateTime.formatIso(observedAt) !== observation.observedAt ||
      observation.observedAt > now ||
      observation.observedAt > expiresAt ||
      record.startedAt === null ||
      observation.observedAt < record.startedAt ||
      (observation.worktree !== null &&
        (observation.worktree.path !== observation.checkoutPath ||
          observation.worktree.refName !== admission.capture.branch)) ||
      (outcome.kind === "prepared_completed"
        ? outcome.observation.setup.status === "completed" &&
          outcome.observation.setup.cwd !== observation.checkoutPath
        : outcome.observation.readback.cwd !== observation.checkoutPath ||
          canonicalJson(outcome.observation.setup.ownerBirth) !==
            canonicalJson(admission.capture.applicationBirth))
    )
      return yield* failure(
        admission.capture,
        "unknown_use",
        "Prepared result does not belong to the original entered producer and checkout",
      );
    yield* validateSource({ ...ref.originalUse, source: ref.executor.source }, admission);
    return { admission, record };
  });

  const currentLeaseForExecution = (
    admission: Ordinary.OrdinaryCheckoutAdmissionV1,
    record: OrdinaryCheckoutUseRecordV1,
  ) => current({ capture: admission.capture, source: record.subject.source });

  const transitionOrdinaryPreparedBranch = Effect.fnUntraced(function* (input: {
    readonly commandId: CommandId;
    readonly observation: import("./ThreadLaunchService.ts").OrdinaryPreparedPhysicalResultV1;
    readonly commitMetadata: Effect.Effect<
      {
        readonly sequence: number;
        readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
      },
      Error
    >;
  }) {
    yield* transactions.requireOwned;
    const { readIssuedOrdinaryPreparedPhysicalResult } = yield* Effect.promise(
      () => import("./ThreadLaunchService.ts"),
    );
    const issued = readIssuedOrdinaryPreparedPhysicalResult(input.observation);
    if (issued === null || issued.kind !== "prepared_branch_renamed")
      return yield* new OrdinaryCheckoutEvidenceWriteError({
        eventCount: 0,
        cause: "Branch transition has no unchanged actual producer observation",
      });
    const observation = yield* Schema.decodeUnknownEffect(
      Schema.toType(OrdinaryPreparedBranchObservationV1),
    )(issued, { onExcessProperty: "error" });
    const ref = observation.execution;
    const previous = yield* sql<{ readonly transition_json: string }>`
      SELECT transition_json FROM orchestration_v2_ordinary_checkout_target_transitions WHERE operation_id = ${ref.originalUse.operationId}`;
    if (previous.length !== 0) {
      const retained = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(Ordinary.OrdinaryCheckoutTargetTransitionV1),
      )(previous[0]!.transition_json, { onExcessProperty: "error" });
      const evidence = yield* Schema.decodeUnknownEffect(OrdinaryPreparedBranchTransitionV1)(
        retained.evidence,
        { onExcessProperty: "error" },
      );
      if (
        evidence.commandId !== input.commandId ||
        canonicalJson(
          yield* Schema.encodeEffect(OrdinaryPreparedBranchObservationV1)(observation),
        ) !==
          canonicalJson(
            yield* Schema.encodeEffect(OrdinaryPreparedBranchObservationV1)(evidence.observation),
          )
      )
        return yield* new OrdinaryCheckoutEvidenceWriteError({
          eventCount: 0,
          cause: "Branch transition replay differs from its immutable original result",
        });
      return yield* resolveOrdinaryCheckoutLease(ref.originalUse.lease);
    }
    const validated = yield* validateOrdinaryCheckoutExecutionEffect(ref, true);
    const record = yield* exactUse(ref.originalUse);
    const admission = validated.admission;
    if (admission.run !== null) {
      const local = yield* projectionStore.getThreadRecords(admission.capture.threadId, ["runs"]);
      const run = local.runs.find((item) => item.id === admission.run!.runId);
      if (
        run === undefined ||
        ["completed", "failed", "cancelled", "interrupted"].includes(run.status)
      )
        return yield* failure(
          admission.capture,
          "stale_admission",
          "Stopped preparation cannot publish a late physical rename",
        );
    }
    const lease = yield* current({ capture: admission.capture, source: record.subject.source });
    const observedAt = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(
      observation.observedAt,
    );
    const now = yield* DateTime.now;
    if (
      ref.executor.kind !== "actual_prepared_producer" ||
      observation.producerId !== ref.executor.producerId ||
      canonicalJson(observation.targetSource) !== canonicalJson(record.subject.source) ||
      observation.checkoutPath !== admission.capture.canonicalCheckoutPath ||
      observation.oldBranch !== admission.capture.branch ||
      observation.oldBranch !== lease.branch ||
      observation.oldBranch === observation.renamedBranch ||
      observation.readback.cwd !== observation.checkoutPath ||
      observation.readback.refName !== observation.renamedBranch ||
      DateTime.formatIso(observedAt) !== observation.observedAt ||
      observation.observedAt > DateTime.formatIso(now) ||
      observation.observedAt >= validated.participant.expiresAt ||
      record.startedAt === null ||
      observation.observedAt < record.startedAt ||
      lease.expiresAtMs <= DateTime.toEpochMillis(now)
    )
      return yield* failure(
        admission.capture,
        "target_changed",
        "Branch rename differs from its original live producer and checkout",
      );
    const committed = yield* input.commitMetadata;
    const event = committed.storedEvents.find(
      (item) =>
        item.commandId === input.commandId &&
        item.event.type === "thread.metadata-updated" &&
        item.event.threadId === admission.capture.threadId &&
        item.event.payload.branch === observation.renamedBranch &&
        item.event.payload.worktreePath === record.subject.source.worktreePath,
    );
    if (
      event === undefined ||
      event.sequence !== committed.sequence ||
      readIssuedOrdinaryPreparedPhysicalResult(input.observation) !== issued
    )
      return yield* failure(
        admission.capture,
        "target_changed",
        "Branch transition lost its issued result or metadata receipt",
      );
    const changed =
      yield* sql`UPDATE worktree_ownership_leases SET branch = ${observation.renamedBranch}
      WHERE resource_path = ${lease.resourcePath} AND lease_id = ${lease.leaseId}
        AND owner_thread_id = ${lease.ownerThreadId} AND owner_incarnation = ${lease.ownerIncarnation}
        AND branch IS ${lease.branch} AND acquired_at_ms = ${lease.acquiredAtMs}
        AND renewed_at_ms = ${lease.renewedAtMs} AND expires_at_ms = ${lease.expiresAtMs}
      RETURNING lease_id`;
    if (changed.length !== 1)
      return yield* failure(
        admission.capture,
        "target_changed",
        "Original lease changed before branch transition",
      );
    const transition: Ordinary.OrdinaryCheckoutTargetTransitionV1 = {
      version: 1,
      operationId: ref.originalUse.operationId,
      admission: ref.originalUse.admission,
      source: ref.originalUse.source,
      canonicalCheckoutPath: admission.capture.canonicalCheckoutPath,
      leaseId: lease.leaseId,
      applicationBirth: admission.capture.applicationBirth,
      beforeLease: lease,
      afterLease: { ...lease, branch: observation.renamedBranch },
      fromBranch: observation.oldBranch,
      toBranch: observation.renamedBranch,
      evidence: yield* Schema.encodeEffect(OrdinaryPreparedBranchTransitionV1)({
        observation,
        commandId: input.commandId,
        eventId: event.event.id,
        sequence: event.sequence,
        associationOrdinal: validated.participant.latestOrdinal,
      }),
      recordedAt: now,
    };
    yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_target_transitions
      (operation_id, admission_id, transition_json, recorded_at)
      VALUES (${ref.originalUse.operationId}, ${ref.originalUse.admission.admissionId},
        ${canonicalJson(yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutTargetTransitionV1)(transition))}, ${DateTime.formatIso(now)})`;
    yield* resolveOrdinaryCheckoutLease(admission.capture.lease);
    return { ...lease, branch: observation.renamedBranch };
  });

  const renewOrdinaryCheckoutExecution = Effect.fnUntraced(function* (input: {
    readonly ref: Ordinary.OrdinaryCheckoutExecutionRefV1;
    readonly now: DateTime.Utc;
    readonly newExpiry: DateTime.Utc;
    readonly expectedClaimExpiry?: DateTime.Utc;
  }) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const current = yield* validateOrdinaryCheckoutExecutionEffect(input.ref, false, false);
        const now = DateTime.formatIso(yield* DateTime.now);
        const suppliedNow = DateTime.formatIso(input.now);
        const expiry = DateTime.formatIso(input.newExpiry);
        if (
          suppliedNow > now ||
          expiry <= now ||
          expiry < current.participant.expiresAt ||
          expiry > DateTime.formatIso(DateTime.add(yield* DateTime.now, { minutes: 5 }))
        )
          return yield* failure(
            current.admission.capture,
            "claim_mismatch",
            "Ordinary renewal cannot reverse or extend beyond its five-minute horizon",
          );
        if (current.ref.executor.kind === "actual_outbox_claim") {
          if (
            input.expectedClaimExpiry === undefined ||
            DateTime.formatIso(input.expectedClaimExpiry) !== current.participant.expiresAt ||
            !(yield* outbox.renewClaim({
              effectId: current.ref.executor.source.link.effectId,
              workerId: current.ref.executor.source.workerId,
              expectedAttempt: current.ref.executor.source.expectedAttempt,
              expectedLeaseExpiresAt: current.participant.expiresAt,
              leaseExpiresAt: expiry,
            }))
          )
            return yield* failure(
              current.admission.capture,
              "claim_mismatch",
              "Ordinary claim changed before exact deadline renewal",
            );
        } else if (input.expectedClaimExpiry !== undefined)
          return yield* failure(
            current.admission.capture,
            "claim_mismatch",
            "Captured producer renewal cannot borrow an outbox deadline",
          );
        const lease = yield* currentLeaseForExecution(current.admission, current.record);
        const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
        const expiryMs = DateTime.toEpochMillis(input.newExpiry);
        if (lease.expiresAtMs <= nowMs)
          return yield* failure(
            current.admission.capture,
            "claim_mismatch",
            "A captured actor cannot restore an expired checkout grant.",
          );
        const renewed =
          yield* sql`UPDATE worktree_ownership_leases SET renewed_at_ms = ${nowMs}, expires_at_ms = ${Math.max(lease.expiresAtMs, expiryMs)}
          WHERE resource_path = ${lease.resourcePath} AND lease_id = ${lease.leaseId} AND owner_thread_id = ${lease.ownerThreadId}
            AND owner_incarnation = ${lease.ownerIncarnation} AND branch IS ${lease.branch} AND expires_at_ms = ${lease.expiresAtMs}
            AND expires_at_ms > ${nowMs} RETURNING resource_path`;
        if (renewed.length !== 1)
          return yield* failure(
            current.admission.capture,
            "claim_mismatch",
            "The original checkout grant changed before captured-actor renewal.",
          );
        yield* appendOrdinaryCheckoutExecutionFact(
          current.ref,
          "renew",
          expiry,
          current.participant.expiresAt,
        );
        return current.ref;
      }),
    );
  });

  const retainExecutionUnknown = Effect.fn("OrdinaryCheckoutStore.retainExecutionUnknown")(
    function* (ref: Ordinary.OrdinaryCheckoutExecutionRefV1, reason: string) {
      return yield* transactions.withTransaction(
        Effect.gen(function* () {
          const admission = yield* resolveAdmission(ref.originalUse.admission);
          yield* validateOrdinaryCheckoutExecutionAttribution(ref, admission, ref.originalUse);
          const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(ref.originalUse);
          const participant = history.participants.find(
            (item) => item.ref.associationId === ref.associationId,
          );
          if (participant?.state === "unknown") return yield* exactUse(ref.originalUse);
          if (participant?.state !== "active")
            return yield* failure(
              admission.capture,
              "unknown_use",
              "A retired executor cannot be replaced with unknown evidence.",
            );
          yield* appendOrdinaryCheckoutExecutionFact(
            ref,
            "unknown",
            participant.expiresAt,
            undefined,
            {
              kind: "unknown",
              reason,
              observedAt: yield* DateTime.now,
            },
          );
          return yield* markUnknown(ref.originalUse, reason);
        }),
      );
    },
  );

  const bindOutboxExecution = Effect.fn("OrdinaryCheckoutStore.bindOutboxExecution")(function* (
    use: Ordinary.OrdinaryCheckoutUseV1,
  ) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const record = yield* exactUse(use);
        const admission = yield* resolveAdmission(use.admission);
        if (
          use.source.kind !== "outbox" ||
          (record.state !== "reserved" && record.state !== "started")
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "An initial executor must be the actual admitted outbox claim.",
          );
        yield* current({ capture: admission.capture, source: record.subject.source });
        yield* validateSource(use, admission);
        const ref = Ordinary.makeOrdinaryCheckoutExecutionRefV1({
          originalUse: use,
          executor: { kind: "actual_outbox_claim", source: use.source },
        });
        const rows = yield* sql<{ readonly association_json: string }>`
        SELECT association_json FROM orchestration_v2_ordinary_checkout_execution_associations
        WHERE operation_id = ${use.operationId} ORDER BY ordinal`;
        const encoded = canonicalJson(encodeExecution(ref));
        if (rows.length > 0) {
          if (rows.length !== 1 || rows[0]!.association_json !== encoded)
            return yield* failure(
              admission.capture,
              "unknown_use",
              "The initial executor has an existing successor or another binding.",
            );
          return ref;
        }
        const at = DateTime.formatIso(yield* DateTime.now);
        const evidence = canonicalJson({
          version: 1,
          schema: "t3.ordinary-checkout-execution-liveness/v1",
          kind: "bind",
          expiresAt: DateTime.formatIso(use.source.leaseExpiresAt),
        });
        yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_execution_associations
        (operation_id, ordinal, predecessor_ordinal, association_id, admission_id, executor_kind,
          effect_id, event_kind, association_json, evidence_json, recorded_at)
        VALUES (${use.operationId}, 0, NULL, ${ref.associationId}, ${use.admission.admissionId},
          'actual_outbox_claim', ${use.source.link.effectId}, 'bind', ${encoded}, ${evidence}, ${at})`;
        return ref;
      }),
    );
  });

  const joinOrdinaryCheckoutClaim = Effect.fnUntraced(function* (input: {
    readonly originalUse: Ordinary.OrdinaryCheckoutUseV1;
    readonly claim: typeof Ordinary.OrdinaryCheckoutOutboxExecutionSourceV1.Type;
    readonly predecessorExecution?: Ordinary.OrdinaryCheckoutExecutionRefV1;
  }) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(input.originalUse);
        const record = yield* exactUse(input.originalUse);
        const admission = yield* resolveAdmission(input.originalUse.admission);
        const now = DateTime.formatIso(yield* DateTime.now);
        const active = history.participants.filter((participant) => participant.state === "active");
        const preparedPredecessor = input.predecessorExecution;
        const retiredPreparation =
          preparedPredecessor?.executor.kind === "actual_prepared_producer"
            ? history.facts.findLast(
                (fact) =>
                  ordinaryExecutionBytes(fact.ref) ===
                    ordinaryExecutionBytes(preparedPredecessor) &&
                  fact.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
                  fact.evidence.actualProducerOutcome.kind === "prepared_completed",
              )
            : undefined;
        let completedPreparation = false;
        if (
          retiredPreparation?.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
          retiredPreparation.evidence.actualProducerOutcome.kind === "prepared_completed" &&
          preparedPredecessor !== undefined
        ) {
          const effect = Option.getOrNull(yield* outbox.get(input.claim.link.effectId));
          const binding = (yield* readOrdinaryCheckoutCurrentCommandsEffect(
            input.claim.link.commandId,
          )).find((item) => item.threadId === admission.capture.threadId);
          if (
            effect?.request.type === "provider-turn.start" &&
            admission.run !== null &&
            effect.request.runId === admission.run.runId &&
            binding?.canonicalCommand.type === "prepared-run.release" &&
            binding.canonicalCommand.runId === admission.run.runId &&
            binding.joinedUse !== null &&
            ordinaryUseBytes(binding.joinedUse) === ordinaryUseBytes(input.originalUse) &&
            binding.ordinaryCheckoutExecution !== null &&
            ordinaryExecutionBytes(binding.ordinaryCheckoutExecution) ===
              ordinaryExecutionBytes(preparedPredecessor)
          ) {
            yield* validateOrdinaryCheckoutExecutionAttribution(
              preparedPredecessor,
              admission,
              input.originalUse,
            );
            yield* validateOrdinaryPreparedOutcome(
              preparedPredecessor,
              retiredPreparation.evidence.actualProducerOutcome,
              retiredPreparation.evidence.expiresAt,
            );
            yield* validateCapture(admission.capture, record.subject.source, {
              operationId: input.originalUse.operationId,
              requireLiveLease: true,
            });
            completedPreparation = true;
          }
        }
        if (
          history.facts.length === 0 ||
          record.state !== "started" ||
          (active.length === 0 && !completedPreparation) ||
          history.participants.some((participant) => participant.state === "unknown") ||
          active.some((participant) => participant.expiresAt <= now)
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "A pending association is not entered original work",
          );
        for (const participant of active)
          yield* validateOrdinaryExecutionActor(participant.ref, participant.expiresAt, admission);
        if (input.predecessorExecution !== undefined) {
          if (
            ordinaryUseBytes(input.predecessorExecution.originalUse) !==
            ordinaryUseBytes(input.originalUse)
          )
            return yield* failure(
              admission.capture,
              "stale_admission",
              "Joined claim belongs to another original operation",
            );
          if (!completedPreparation)
            yield* validateOrdinaryCheckoutExecutionEffect(input.predecessorExecution);
        }
        const ref = Ordinary.makeOrdinaryCheckoutExecutionRefV1({
          originalUse: input.originalUse,
          executor: { kind: "actual_outbox_claim", source: input.claim },
        });
        if (
          history.facts.some(
            (fact) =>
              fact.evidence.schema === "t3.ordinary-checkout-execution-liveness/v1" &&
              fact.evidence.completionBasis !== undefined &&
              ordinaryExecutionBytes(fact.ref) !== ordinaryExecutionBytes(ref),
          )
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Final checkpoint handoff has sealed this original mutation cohort",
          );
        yield* validateCapture(admission.capture, record.subject.source, {
          operationId: input.originalUse.operationId,
          requireLiveLease: false,
        });
        const existing = history.participants.find(
          (item) => item.ref.associationId === ref.associationId,
        );
        if (existing !== undefined && existing.state !== "active")
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Retired or unknown claim cannot rejoin its original operation",
          );
        const expiry = existing?.expiresAt ?? DateTime.formatIso(input.claim.leaseExpiresAt);
        yield* validateOrdinaryExecutionActor(ref, expiry, admission);
        if (existing === undefined) yield* appendOrdinaryCheckoutExecutionFact(ref, "join", expiry);
        else if (ordinaryExecutionBytes(existing.ref) !== ordinaryExecutionBytes(ref))
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Joined actor differs from its immutable original association",
          );
        return ref;
      }),
    );
  });
  const writeSystemEffects = Effect.fnUntraced(function* (
    effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>,
    contexts: ReadonlyArray<OrdinaryCheckoutSystemEffectsV1>,
  ) {
    const subjects = new Map<
      RunId,
      {
        readonly context: OrdinaryCheckoutSystemEffectsV1;
        readonly admission: Ordinary.OrdinaryCheckoutAdmissionV1;
      }
    >();
    for (const context of contexts) {
      const admission = yield* resolveAdmission(context.admission);
      if (subjects.has(context.runId) || admission.run?.runId !== context.runId)
        return yield* failure(
          admission.capture,
          "stale_admission",
          "System effect has no unique original accepted run admission",
        );
      const original = yield* readOrdinaryCheckoutAdmissionForRunEffect({
        threadId: admission.capture.threadId,
        runId: context.runId,
      });
      if (
        original === null ||
        canonicalJson(Ordinary.ordinaryCheckoutAdmissionRefV1(original)) !==
          canonicalJson(context.admission)
      )
        return yield* failure(
          admission.capture,
          "stale_admission",
          "System effect differs from its immutable original run admission",
        );
      if (context.ordinaryCheckoutExecution !== undefined) {
        yield* validateOrdinaryCheckoutExecutionAttribution(
          context.ordinaryCheckoutExecution,
          admission,
          context.joinedUse,
        );
        const execution = yield* validateOrdinaryCheckoutExecutionEffect(
          context.ordinaryCheckoutExecution,
        );
        if (
          execution.record.state !== "started" ||
          canonicalJson(execution.record.subject.source) !== canonicalJson(context.source)
        )
          return yield* failure(
            admission.capture,
            "stale_admission",
            "System effect lost its exact entered executor source",
          );
      } else if (context.joinedUse === undefined)
        yield* validateCapture(admission.capture, context.source);
      else yield* validateOrdinaryCheckoutJoin(context.joinedUse, admission, context.source);
      if ((yield* outbox.listHeldByThreadId(admission.capture.threadId)).length > 0)
        return yield* failure(
          admission.capture,
          "unknown_use",
          "Unresolved native evidence prevents ordinary system association",
        );
      subjects.set(context.runId, { context, admission });
    }
    const matched = new Set<RunId>();
    for (const pending of effects) {
      if (
        ![
          "provider-turn.start",
          "provider-turn.restart",
          "provider-turn.steer",
          "provider-thread.rollback",
          "provider-runtime.continue",
          "runtime-request.respond",
          "checkpoint.capture",
        ].includes(pending.request.type)
      )
        continue;
      if (
        pending.request.type !== "provider-turn.start" &&
        pending.request.type !== "provider-turn.restart" &&
        pending.request.type !== "checkpoint.capture"
      )
        return yield* new OrdinaryCheckoutEvidenceWriteError({
          eventCount: 0,
          cause: "System checkout effect lacks a supported exact run association",
        });
      const request = pending.request;
      const subject = subjects.get(request.runId);
      if (subject === undefined || pending.threadId !== subject.admission.capture.threadId)
        return yield* new OrdinaryCheckoutEvidenceWriteError({
          eventCount: 0,
          cause: "System checkout effect has no original captured subject",
        });
      const { admission } = subject;
      const local = yield* projectionStore.getThreadRecords(pending.threadId, [
        "runs",
        "attempts",
        "nodes",
        "checkpointScopes",
      ]);
      const run = local.runs.find((item) => item.id === request.runId);
      const attempt = local.attempts.find(
        (item) => item.id === run?.activeAttemptId && item.runId === run?.id,
      );
      const node = local.nodes.find(
        (item) => item.id === run?.rootNodeId && item.runId === run?.id,
      );
      const scope =
        request.type === "checkpoint.capture"
          ? local.checkpointScopes.find((item) => item.id === request.scopeId)
          : null;
      if (
        run === undefined ||
        attempt === undefined ||
        node === undefined ||
        run.userMessageId !== admission.run!.messageId ||
        run.rootNodeId !== admission.run!.nodeId ||
        attempt.rootNodeId !== node.id ||
        (request.type === "checkpoint.capture" &&
          (request.scopeId !== node.checkpointScopeId ||
            scope === null ||
            scope === undefined ||
            scope.threadId !== pending.threadId ||
            scope.providerThreadId !== node.providerThreadId))
      )
        return yield* failure(
          admission.capture,
          "stale_admission",
          "System effect lost its exact current run, attempt, node or scope",
        );
      const effect = Option.getOrNull(yield* outbox.get(pending.id));
      if (
        effect === null ||
        effect.commandId !== pending.commandId ||
        effect.threadId !== pending.threadId ||
        canonicalJson(
          yield* Schema.encodeEffect(EffectOutbox.OrchestrationEffectRequestV2)(
            effect.request,
          ).pipe(Effect.orDie),
        ) !==
          canonicalJson(
            yield* Schema.encodeEffect(EffectOutbox.OrchestrationEffectRequestV2)(
              pending.request,
            ).pipe(Effect.orDie),
          )
      )
        return yield* failure(
          admission.capture,
          "stale_admission",
          "System checkout link has no exact enqueued effect",
        );
      const existing = yield* readOrdinaryCheckoutEffectLinkEffect(pending.id);
      if (existing !== null) {
        if (canonicalJson(existing.admission) !== canonicalJson(subject.context.admission))
          return yield* failure(
            admission.capture,
            "stale_admission",
            "System effect already belongs to another admission",
          );
        const rows = yield* sql<{
          readonly link_json: string;
        }>`SELECT link_json FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = ${pending.id}`;
        const raw = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
        )(rows[0]!.link_json);
        const originalExecution =
          raw.schema === "t3.ordinary-checkout-system-link/v1"
            ? (yield* Schema.decodeUnknownEffect(OrdinaryCheckoutSystemLinkPayloadV1)(raw, {
                onExcessProperty: "error",
              })).ordinaryCheckoutExecution
            : null;
        if (
          canonicalJson(originalExecution) !==
          canonicalJson(subject.context.ordinaryCheckoutExecution ?? null)
        )
          return yield* failure(
            admission.capture,
            "stale_admission",
            "System effect cannot change its original execution attribution",
          );
      } else {
        if (effect.status !== "pending")
          return yield* failure(
            admission.capture,
            "claim_mismatch",
            "Unbound running or terminal effects cannot acquire a checkout origin",
          );
        const now = yield* DateTime.now;
        const link: Ordinary.OrdinaryCheckoutEffectLinkV1 = {
          version: 1,
          effectId: pending.id,
          commandId: pending.commandId,
          threadId: pending.threadId,
          requestSha256: sha256(
            canonicalJson(
              yield* Schema.encodeEffect(EffectOutbox.OrchestrationEffectRequestV2)(
                pending.request,
              ).pipe(Effect.orDie),
            ),
          ),
          admission: subject.context.admission,
          recordedAt: now,
        };
        const payload =
          subject.context.ordinaryCheckoutExecution === undefined
            ? yield* Schema.encodeEffect(Ordinary.OrdinaryCheckoutEffectLinkV1)(link).pipe(
                Effect.orDie,
              )
            : yield* Schema.encodeEffect(OrdinaryCheckoutSystemLinkPayloadV1)({
                schema: "t3.ordinary-checkout-system-link/v1",
                version: 1,
                link,
                ordinaryCheckoutExecution: subject.context.ordinaryCheckoutExecution,
              }).pipe(Effect.orDie);
        yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_effect_links (effect_id, admission_id, link_json, recorded_at)
          VALUES (${link.effectId}, ${admission.admissionId}, ${canonicalJson(payload)}, ${DateTime.formatIso(now)})`;
      }
      matched.add(request.runId);
    }
    for (const [runId, subject] of subjects)
      if (!matched.has(runId))
        return yield* failure(
          subject.admission.capture,
          "stale_admission",
          "Ordinary system context has no real enqueued effect",
        );
  });
  const completePreparedUse = Effect.fnUntraced(function* (input: {
    readonly originalUse: Ordinary.OrdinaryCheckoutUseV1;
    readonly expectedAssociationOrdinal: number;
    readonly completionEvidence: typeof OrdinaryCheckoutCompletionEvidenceV1.Type;
  }) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const evidence = yield* Schema.decodeUnknownEffect(
          Schema.toType(OrdinaryCheckoutCompletionEvidenceV1),
        )(input.completionEvidence, { onExcessProperty: "error" });
        if (
          evidence.actualProducerOutcome.kind !== "prepared_completed" &&
          evidence.actualProducerOutcome.kind !== "prepared_failed"
        )
          return yield* new OrdinaryCheckoutHistoryError({
            message: "Prepared-use completion requires its original preparation producer outcome.",
          });
        const record = yield* exactUse(input.originalUse);
        const admission = yield* resolveAdmission(input.originalUse.admission);
        const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(input.originalUse);
        if (
          !Number.isSafeInteger(input.expectedAssociationOrdinal) ||
          input.expectedAssociationOrdinal < 0 ||
          input.expectedAssociationOrdinal !== history.latestOrdinal ||
          ordinaryUseBytes(evidence.ref.originalUse) !== ordinaryUseBytes(input.originalUse)
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Completion differs from the exact final original association",
          );
        const encodedEvidence = yield* Schema.encodeEffect(OrdinaryCheckoutCompletionEvidenceV1)(
          evidence,
        ).pipe(Effect.orDie);
        const completion = canonicalJson({
          version: 1,
          schema: "t3.ordinary-checkout-completed/v1",
          operationId: input.originalUse.operationId,
          associationOrdinal: history.latestOrdinal,
          completionEvidence: encodedEvidence,
        });
        const stored = yield* sql<{
          readonly outcome_json: string | null;
        }>`SELECT outcome_json FROM orchestration_v2_worktree_path_admissions
        WHERE operation_id = ${input.originalUse.operationId}`;
        if (record.state === "released" && stored[0]?.outcome_json === completion)
          return {
            status: "already_completed" as const,
            originalUse: input.originalUse,
            associationOrdinal: history.latestOrdinal,
          };
        const participant = history.participants.find(
          (item) => item.ref.associationId === evidence.ref.associationId,
        );
        const exactResult = history.facts.find(
          (fact) =>
            fact.eventKind === "retire" &&
            ordinaryExecutionBytes(fact.ref) === ordinaryExecutionBytes(evidence.ref) &&
            fact.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            canonicalJson(
              Schema.encodeSync(OrdinaryCheckoutExecutorOutcomeV1)(
                fact.evidence.actualProducerOutcome,
              ),
            ) ===
              canonicalJson(
                Schema.encodeSync(OrdinaryCheckoutExecutorOutcomeV1)(
                  evidence.actualProducerOutcome,
                ),
              ),
        );
        if (
          record.state !== "started" ||
          history.participants.length === 0 ||
          history.participants.some((item) => item.state !== "retired") ||
          participant?.state !== "retired" ||
          exactResult === undefined
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Original operation still has a mutating or uncertain participant",
          );
        yield* validateCapture(admission.capture, record.subject.source, {
          operationId: input.originalUse.operationId,
          requireLiveLease: false,
        });
        {
          const outcome = evidence.actualProducerOutcome;
          const executor = evidence.ref.executor;
          const finalState = outcome.observation.readback;
          const currentBranch = (yield* resolveOrdinaryCheckoutLease(admission.capture.lease))
            .branch;
          const unspecifiedRoot =
            record.subject.source.worktreePath === null && currentBranch === null;
          // A failed preparation ends a run's use only while that run still awaits
          // preparation and nothing but the original producer ever joined the use.
          const preparedRun = admission.run;
          const failedRunStillPreparing =
            outcome.kind !== "prepared_failed" || preparedRun === null
              ? true
              : (yield* projectionStore.getThreadRecords(admission.capture.threadId, [
                  "runs",
                ])).runs.some((run) => run.id === preparedRun.runId && run.status === "preparing");
          if (
            executor.kind !== "actual_prepared_producer" ||
            (outcome.kind === "prepared_failed"
              ? (executor.source.kind === "prepared_run") !== (preparedRun !== null) ||
                history.participants.length !== 1 ||
                !failedRunStillPreparing
              : preparedRun !== null || executor.source.kind !== "prepared_launch") ||
            finalState === undefined ||
            finalState.cwd !== admission.capture.canonicalCheckoutPath ||
            (!unspecifiedRoot && (!finalState.isRepo || finalState.refName !== currentBranch)) ||
            (!finalState.isRepo && finalState.refName !== null)
          )
            return yield* failure(
              admission.capture,
              "unknown_use",
              "Prepared completion requires the original producer's post-native checkout and ref readback",
            );
          yield* validateOrdinaryPreparedOutcome(evidence.ref, outcome, participant.expiresAt);
        }
        const unfinished =
          yield* sql`SELECT effect.effect_id FROM orchestration_v2_ordinary_checkout_effect_links link
        JOIN orchestration_v2_effect_outbox effect ON effect.effect_id = link.effect_id
        WHERE link.admission_id = ${input.originalUse.admission.admissionId} AND effect.status IN ('pending', 'running') LIMIT 1`;
        if (
          unfinished.length !== 0 ||
          (yield* outbox.listHeldByThreadId(admission.capture.threadId)).length !== 0
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Original run still has pending real work or unresolved effects",
          );
        const now = DateTime.formatIso(yield* DateTime.now);
        const completed =
          yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'completed', outcome_json = ${completion}, updated_at = ${now}
        WHERE operation_id = ${input.originalUse.operationId} AND state = 'started' RETURNING operation_id`;
        const released =
          yield* sql`UPDATE orchestration_v2_worktree_path_admissions SET state = 'released', updated_at = ${now}
        WHERE operation_id = ${input.originalUse.operationId} AND state = 'completed' AND outcome_json = ${completion} RETURNING operation_id`;
        if (completed.length !== 1 || released.length !== 1)
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Original path changed before exact completion and retirement",
          );
        return {
          status: "completed" as const,
          originalUse: input.originalUse,
          associationOrdinal: history.latestOrdinal,
        };
      }),
    );
  });

  const recordPreparedOutcome = Effect.fnUntraced(function* <E>(input: {
    readonly ref: Ordinary.OrdinaryCheckoutExecutionRefV1;
    readonly actualProducerOutcome: Extract<
      OrdinaryCheckoutExecutorOutcomeV1,
      { readonly kind: "prepared_completed" | "prepared_failed" }
    >;
    readonly revalidateProducer: Effect.Effect<void, E>;
    readonly completeOriginalUse?: boolean;
  }) {
    return yield* transactions.withTransaction(
      Effect.gen(function* () {
        const ref = yield* Schema.decodeUnknownEffect(
          Schema.toType(Ordinary.OrdinaryCheckoutExecutionRefV1),
        )(input.ref, { onExcessProperty: "error" });
        const outcome = yield* Schema.decodeUnknownEffect(
          Schema.toType(OrdinaryCheckoutExecutorOutcomeV1),
        )(input.actualProducerOutcome, { onExcessProperty: "error" });
        if (outcome.kind !== "prepared_completed" && outcome.kind !== "prepared_failed")
          return yield* new OrdinaryCheckoutHistoryError({
            message: "This producer can qualify only its original preparation outcome.",
          });
        const bytes = canonicalJson(
          yield* Schema.encodeEffect(OrdinaryCheckoutExecutorOutcomeV1)(outcome),
        );
        const history = yield* readOrdinaryCheckoutExecutionAssociationsEffect(ref.originalUse);
        const previous = history.facts.find(
          (fact) =>
            ordinaryExecutionBytes(fact.ref) === ordinaryExecutionBytes(ref) &&
            fact.evidence.schema === "t3.ordinary-checkout-execution-outcome/v1" &&
            canonicalJson(
              Schema.encodeSync(OrdinaryCheckoutExecutorOutcomeV1)(
                fact.evidence.actualProducerOutcome,
              ),
            ) === bytes,
        );
        if (previous !== undefined) return previous;
        const participant = history.participants.find(
          (item) => item.ref.associationId === ref.associationId,
        );
        const record = yield* exactUse(ref.originalUse);
        const admission = yield* resolveAdmission(ref.originalUse.admission);
        if (
          participant?.state !== "active" ||
          ordinaryExecutionBytes(participant.ref) !== ordinaryExecutionBytes(ref) ||
          record.state !== "started"
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Executor outcome has no original entered actor.",
          );
        const { readIssuedOrdinaryPreparedPhysicalResult } = yield* Effect.promise(
          () => import("./ThreadLaunchService.ts"),
        );
        const issued = readIssuedOrdinaryPreparedPhysicalResult(
          input.actualProducerOutcome.observation,
        );
        if (
          issued?.kind !==
          (outcome.kind === "prepared_completed"
            ? "prepared_setup_completed"
            : "prepared_failure_observed")
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Prepared completion requires its actual retained producer issuer.",
          );
        yield* validateOrdinaryExecutionActor(ref, participant.expiresAt, admission);
        yield* input.revalidateProducer;
        yield* validateCapture(admission.capture, record.subject.source, {
          operationId: ref.originalUse.operationId,
          requireLiveLease: false,
        });
        yield* validateOrdinaryPreparedOutcome(ref, outcome, participant.expiresAt);
        if (
          participant.expiresAt <= DateTime.formatIso(yield* DateTime.now) ||
          (yield* outbox.listHeldByThreadId(admission.capture.threadId)).length > 0
        )
          return yield* failure(
            admission.capture,
            "unknown_use",
            "Prepared completion lost its original actor before qualification.",
          );
        yield* input.revalidateProducer;
        yield* appendOrdinaryCheckoutExecutionFact(
          ref,
          "retire",
          participant.expiresAt,
          undefined,
          outcome,
        );
        const updated = yield* readOrdinaryCheckoutExecutionAssociationsEffect(ref.originalUse);
        const retirement = updated.facts[updated.latestOrdinal]!;
        if (input.completeOriginalUse === true)
          yield* completePreparedUse({
            originalUse: ref.originalUse,
            expectedAssociationOrdinal: retirement.ordinal,
            completionEvidence: { ref, actualProducerOutcome: input.actualProducerOutcome },
          });
        return retirement;
      }),
    );
  });
  const captureJoinedCommand = Effect.fnUntraced(function* (input: {
    readonly command: Extract<OrchestrationV2Command, { readonly type: "prepared-run.release" }>;
    readonly originalUse: Ordinary.OrdinaryCheckoutUseV1;
    readonly execution: Ordinary.OrdinaryCheckoutExecutionRefV1;
  }) {
    const current = yield* validateOrdinaryCheckoutExecutionEffect(input.execution);
    const admission = current.admission;
    if (
      input.execution.executor.kind !== "actual_prepared_producer" ||
      current.record.state !== "started" ||
      ordinaryUseBytes(input.originalUse) !== ordinaryUseBytes(input.execution.originalUse) ||
      admission.run?.runId !== input.command.runId ||
      admission.capture.threadId !== input.command.threadId
    )
      return yield* failure(
        admission.capture,
        "stale_admission",
        "Release must retain the original entered preparation and run.",
      );
    return {
      capture: admission.capture,
      source: current.record.subject.source,
      joinedUse: input.originalUse,
      ordinaryCheckoutExecution: input.execution,
      command: input.command,
    } satisfies OrdinaryCheckoutCommitCapture;
  });

  return {
    capture,
    acquireBeforeRead,
    acquireNewborn,
    current,
    recordAcceptance,
    writeSystemEffects,
    capturePreparedLaunch,
    readAdmission,
    readAdmissionForRun: readOrdinaryCheckoutAdmissionForRunEffect,
    readEffectLink,
    readCurrentCommands: readOrdinaryCheckoutCurrentCommandsEffect,
    resolveAdmission,
    readUse,
    beginUse,
    revalidateUse,
    markUnknown,
    bindOutboxExecution,
    readExecutionHistory: readOrdinaryCheckoutExecutionAssociationsEffect,
    validateExecutionAttribution: validateOrdinaryCheckoutExecutionAttribution,
    retainExecutionUnknown,
    bindExecution: bindOrdinaryCheckoutExecution,
    validateExecution: validateOrdinaryCheckoutExecutionEffect,
    renewExecution: renewOrdinaryCheckoutExecution,
    validateCheckpointOutcome: validateOrdinaryCheckpointOutcome,
    validatePreparedOutcome: validateOrdinaryPreparedOutcome,
    joinClaim: joinOrdinaryCheckoutClaim,
    recordPreparedOutcome,
    captureJoinedCommand,
    transitionPreparedBranch: (input: Parameters<typeof transitionOrdinaryPreparedBranch>[0]) =>
      transactions.withTransaction(transitionOrdinaryPreparedBranch(input)),
    revalidateExecution: (ref: Ordinary.OrdinaryCheckoutExecutionRefV1) =>
      transactions
        .withTransaction(validateOrdinaryCheckoutExecutionEffect(ref, true, true))
        .pipe(Effect.map((result) => result.ref)),
  };
});

export type OrdinaryCheckoutLifetime = Pick<
  Effect.Success<ReturnType<typeof makeOrdinaryCheckoutStore>>,
  | "readAdmission"
  | "readAdmissionForRun"
  | "capturePreparedLaunch"
  | "readCurrentCommands"
  | "readEffectLink"
  | "beginUse"
  | "bindExecution"
  | "revalidateExecution"
  | "readExecutionHistory"
  | "renewExecution"
  | "retainExecutionUnknown"
  | "recordPreparedOutcome"
  | "joinClaim"
  | "captureJoinedCommand"
  | "transitionPreparedBranch"
>;
