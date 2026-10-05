import {
  type CommandId,
  OrchestrationV2Command,
  type OrchestrationV2StoredEvent,
  WorktreeOwnershipConflictError,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { readApplicationThreadBirth } from "./ApplicationThreadBirth.ts";
import { canonicalJson, sha256 } from "./CanonicalJson.ts";
import type * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
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
}

export interface OrdinaryCheckoutUseRecordV1 {
  readonly subject: typeof OrdinaryCheckoutUseSubjectV1.Type;
  readonly state: "reserved" | "started" | "unknown" | "completed" | "no_effect" | "released";
  readonly startedAt: string | null;
}

class OrdinaryCheckoutRecordError extends Schema.TaggedError<OrdinaryCheckoutRecordError>()(
  "OrdinaryCheckoutRecordError",
  {
    recordKind: Schema.Literals(["admission", "use"]),
    recordId: Schema.String,
    message: Schema.String,
  },
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
}

/** These methods run inside the EventSink transaction that owns acceptance. */
export const makeOrdinaryCheckoutStore = Effect.fn("makeOrdinaryCheckoutStore")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
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
          input.command.type === "runtime-request.respond"
            ? { kind: "runtime_request_answer" as const, requestId: input.command.requestId }
            : { kind: "command" as const },
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
    const lease = leases.length === 1 ? yield* decodeLease(leases[0]) : null;
    if (
      !Ordinary.ordinaryCheckoutCaptureMatchesV1(capture) ||
      birth === null ||
      canonicalJson(birth) !== canonicalJson(capture.applicationBirth) ||
      target === undefined ||
      target.project_id !== capture.projectId ||
      target.workspace_root !== input.source.projectWorkspaceRoot ||
      target.worktree_path !== input.source.worktreePath ||
      target.branch !== capture.branch ||
      lease === null ||
      canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(lease)) !==
        canonicalJson(Ordinary.ordinaryCheckoutLeaseIdentityV1(capture.lease))
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
        capture,
        "target_changed",
        "The captured checkout lease cannot be acquired.",
      );
    const lease = yield* decodeLease(rows[0]);
    const captured = { ...input, capture: { ...capture, lease } };
    yield* current(captured);
    const active = yield* sql<{ readonly state: string; readonly subject_json: string }>`
      SELECT state, subject_json FROM orchestration_v2_worktree_path_admissions
      WHERE canonical_path = ${capture.canonicalCheckoutPath} AND state NOT IN ('no_effect', 'released')`;
    if (active.length > 0) {
      const row = active[0]!;
      if (input.joinedUse === undefined || row.state === "unknown" || row.state === "completed")
        return yield* failure(
          capture,
          "unknown_use",
          "The checkout still has an admitted physical use.",
        );
      const original = yield* decodeUseSubject(row.subject_json);
      if (useBytes(original.use) !== useBytes(input.joinedUse))
        return yield* failure(
          capture,
          "claim_mismatch",
          "The command does not join the original checkout use.",
        );
    }
    return captured;
  });

  const recordAcceptance = Effect.fn("OrdinaryCheckoutStore.recordAcceptance")(function* (input: {
    readonly captured: OrdinaryCheckoutCommitCapture;
    readonly receipt: CommandReceiptStore.CommandReceiptV2;
    readonly events: ReadonlyArray<OrchestrationV2StoredEvent>;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
  }) {
    const { capture } = input.captured;
    const runEvent = input.events.find(
      (stored) => stored.event.type === "run.updated" && stored.event.threadId === capture.threadId,
    );
    const run = runEvent?.event.type === "run.updated" ? runEvent.event.payload : null;
    const admission: Ordinary.OrdinaryCheckoutAdmissionV1 = {
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
    yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_admissions
      (admission_id, command_id, thread_id, admission_sha256, admission_json, recorded_at)
      VALUES (${ref.admissionId}, ${capture.commandId}, ${capture.threadId}, ${ref.admissionSha256},
        ${yield* encodeAdmission(admission)}, ${recordedAt})`;
    yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_commands
      (command_id, thread_id, admission_id, canonical_command_json, command_digest, recorded_at)
      VALUES (${capture.commandId}, ${capture.threadId}, ${ref.admissionId},
        ${canonicalJson(capture.canonicalCommand)}, ${capture.commandDigest}, ${recordedAt})`;
    for (const effect of input.effects.filter(
      (item) =>
        item.threadId === capture.threadId &&
        [
          "provider-turn.start",
          "provider-turn.restart",
          "provider-turn.steer",
          "provider-thread.rollback",
          "provider-runtime.continue",
          "runtime-request.respond",
          "checkpoint.capture",
        ].includes(item.request.type),
    )) {
      if (effect.commandId !== capture.commandId)
        return yield* failure(
          capture,
          "claim_mismatch",
          "The effect is attributed to another outer command.",
        );
      const link: Ordinary.OrdinaryCheckoutEffectLinkV1 = {
        version: 1,
        effectId: effect.id,
        commandId: capture.commandId,
        threadId: capture.threadId,
        requestSha256: sha256(canonicalJson(encodeRequest(effect.request))),
        admission: ref,
        recordedAt: admission.recordedAt,
      };
      yield* sql`INSERT INTO orchestration_v2_ordinary_checkout_effect_links
        (effect_id, admission_id, link_json, recorded_at)
        VALUES (${effect.id}, ${ref.admissionId}, ${yield* encodeLink({
          schema: "t3.ordinary-checkout-command-link/v1",
          version: 1,
          link,
          command: capture.canonicalCommand,
          commandDigest: capture.commandDigest,
          ...(input.captured.joinedUse === undefined
            ? {}
            : { joinedUse: input.captured.joinedUse }),
        })}, ${recordedAt})`;
    }
    return admission;
  });

  const readAdmission = Effect.fn("OrdinaryCheckoutStore.readAdmission")(function* (
    commandId: CommandId,
    threadId: ThreadId,
  ) {
    const rows = yield* sql<{ readonly admission_json: string; readonly admission_sha256: string }>`
      SELECT admission_json, admission_sha256 FROM orchestration_v2_ordinary_checkout_admissions
      WHERE command_id = ${commandId} AND thread_id = ${threadId}`;
    if (rows.length === 0) return null;
    const admission = yield* decodeAdmission(rows[0]!.admission_json);
    if (
      !Ordinary.ordinaryCheckoutAdmissionMatchesV1(admission) ||
      Ordinary.ordinaryCheckoutAdmissionRefV1(admission).admissionSha256 !==
        rows[0]!.admission_sha256 ||
      admission.capture.commandId !== commandId ||
      admission.capture.threadId !== threadId
    )
      return yield* failure(
        admission.capture,
        "stale_admission",
        "The permanent admission no longer matches its recorded identity.",
      );
    return admission;
  });

  const readEffectLink = Effect.fn("OrdinaryCheckoutStore.readEffectLink")(function* (
    effect: EffectOutbox.OrchestrationEffectV2,
  ) {
    const rows = yield* sql<{ readonly link_json: string }>`
      SELECT link_json FROM orchestration_v2_ordinary_checkout_effect_links WHERE effect_id = ${effect.id}`;
    if (rows.length === 0) return null;
    const payload = yield* decodeLink(rows[0]!.link_json);
    const { link } = payload;
    const admission = yield* readAdmission(link.commandId, link.threadId);
    if (
      admission === null ||
      link.effectId !== effect.id ||
      link.threadId !== effect.threadId ||
      payload.commandDigest !== Ordinary.ordinaryCheckoutCommandDigestV1(payload.command) ||
      payload.commandDigest !== admission.capture.commandDigest ||
      link.requestSha256 !== sha256(canonicalJson(encodeRequest(effect.request))) ||
      canonicalJson(link.admission) !==
        canonicalJson(Ordinary.ordinaryCheckoutAdmissionRefV1(admission))
    )
      return yield* new Ordinary.OrdinaryCheckoutOwnershipError({
        reason: "claim_mismatch",
        threadId: effect.threadId,
        path: admission?.capture.canonicalCheckoutPath ?? "",
        message: "The outbox effect does not match its accepted checkout admission.",
      });
    return { link, admission, payload };
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

  const validateSource = Effect.fn("OrdinaryCheckoutStore.validateSource")(function* (
    use: Ordinary.OrdinaryCheckoutUseV1,
    admission: Ordinary.OrdinaryCheckoutAdmissionV1,
  ) {
    if (use.source.kind !== "outbox")
      return yield* failure(
        admission.capture,
        "unavailable",
        "Direct preparation requires its captured producer binding.",
      );
    const source = use.source;
    const effect = Option.getOrNull(yield* outbox.get(source.link.effectId));
    const linked = effect === null ? null : yield* readEffectLink(effect);
    const now = DateTime.formatIso(yield* DateTime.now);
    if (
      effect === null ||
      linked === null ||
      (use.operationId !== effect.id &&
        use.operationId !==
          Ordinary.ordinaryCheckoutOutboxOperationIdV1(effect.id, source.expectedAttempt)) ||
      canonicalJson(encodeEffectLink(linked.link)) !==
        canonicalJson(encodeEffectLink(source.link)) ||
      canonicalJson(linked.link.admission) !== canonicalJson(use.admission) ||
      effect.status !== "running" ||
      effect.leaseOwner !== source.workerId ||
      effect.attemptCount !== source.expectedAttempt ||
      effect.leaseExpiresAt === null ||
      effect.leaseExpiresAt !== DateTime.formatIso(source.leaseExpiresAt) ||
      effect.leaseExpiresAt <= now
    )
      return yield* failure(
        admission.capture,
        "claim_mismatch",
        "The physical use has no exact live outbox claim.",
      );
  });

  const beginUse = Effect.fn("OrdinaryCheckoutStore.beginUse")(function* (input: {
    readonly operationId: string;
    readonly admission: Ordinary.OrdinaryCheckoutAdmissionRefV1;
    readonly source: Ordinary.OrdinaryCheckoutUseSourceV1;
    readonly targetSource: OrdinaryCheckoutCommitCapture["source"];
  }) {
    return yield* sql.withTransaction(
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
    return yield* sql.withTransaction(
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
    return yield* sql.withTransaction(
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

  const bindOutboxExecution = Effect.fn("OrdinaryCheckoutStore.bindOutboxExecution")(function* (
    use: Ordinary.OrdinaryCheckoutUseV1,
  ) {
    return yield* sql.withTransaction(
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

  return {
    capture,
    acquireBeforeRead,
    current,
    recordAcceptance,
    readAdmission,
    readEffectLink,
    resolveAdmission,
    readUse,
    beginUse,
    revalidateUse,
    markUnknown,
    bindOutboxExecution,
  };
});
