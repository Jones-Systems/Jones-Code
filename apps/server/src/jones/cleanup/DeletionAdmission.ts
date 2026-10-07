import * as EffectPath from "effect/Path";
import {
  CommandId,
  ProjectId,
  ThreadId,
  WorktreeCleanupRules,
  type ServerSettingsError,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Receipts from "../../orchestration-v2/CommandReceiptStore.ts";
import {
  nativeCreationCanonicalJson,
  nativeCreationSha256,
} from "../nativeCreation/NativeCreationPreparation.ts";
import {
  DeletionWorktreeRemovalStartV1,
  DeletionWorktreeRemovalTargetV1,
} from "./DeletionWorktreeRemovalTypes.ts";
import type { DeletionWorktreeRemovalObservationV1 } from "./DeletionWorktreeRemoval.ts";

export class DeletionAdmissionError extends Schema.TaggedError<DeletionAdmissionError>()(
  "DeletionAdmissionError",
  {
    reason: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}
export type DeletionPolicyRead = Effect.Effect<WorktreeCleanupRules, ServerSettingsError>;
export type DeletionLiveRead = Effect.Effect<boolean>;
export interface DeletionAdmissionInput {
  readonly threadId: ThreadId;
  readonly target: DeletionWorktreeRemovalTargetV1;
  readonly rules: WorktreeCleanupRules;
  readonly currentRules: DeletionPolicyRead;
  readonly currentLive: DeletionLiveRead;
}
const Binding = Schema.Struct({
  actor: Schema.Literal("storage-cleanup-policy"),
  threadId: ThreadId,
  birthEventId: Schema.NonEmptyString,
  birthSequence: Schema.Int,
  deletionEventId: Schema.NonEmptyString,
  deletionSequence: Schema.Int,
  commandId: CommandId,
  deletionPayload: Schema.String,
  target: DeletionWorktreeRemovalTargetV1,
  rules: WorktreeCleanupRules,
});
type Binding = typeof Binding.Type;
const ThreadIdentity = Schema.Struct({
  id: ThreadId,
  projectId: ProjectId,
  createdAt: Schema.String,
  deletedAt: Schema.NullOr(Schema.String),
  worktreePath: Schema.NullOr(Schema.String),
  branch: Schema.NullOr(Schema.String),
});
const Readback = Schema.Struct({
  registration: Schema.Union([
    Schema.Struct({
      status: Schema.Literal("complete"),
      projectRoot: Schema.NonEmptyString,
      gitCommonDirectory: Schema.NonEmptyString,
      entries: Schema.Array(
        Schema.Struct({
          path: Schema.NonEmptyString,
          head: Schema.NullOr(
            Schema.String.check(Schema.isPattern(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/)),
          ),
          branch: Schema.NullOr(Schema.String),
          bare: Schema.Boolean,
        }),
      ),
    }),
    Schema.Struct({ status: Schema.Literal("unavailable"), reason: Schema.NonEmptyString }),
  ]),
  filesystem: Schema.Union([
    Schema.Struct({ status: Schema.Literals(["present", "absent"]), path: Schema.NonEmptyString }),
    Schema.Struct({
      status: Schema.Literal("unavailable"),
      path: Schema.NonEmptyString,
      reason: Schema.NonEmptyString,
    }),
  ]),
});
const Observation = Schema.Struct({
  version: Schema.Literal(1),
  start: DeletionWorktreeRemovalStartV1,
  startOrdinal: Schema.Int,
  operation: Schema.Union([
    Schema.Struct({
      kind: Schema.Literal("executed"),
      exitCode: Schema.NullOr(Schema.Int),
      completion: Schema.Literals(["exited", "unknown"]),
    }),
    Schema.Struct({ kind: Schema.Literal("reconciled"), completion: Schema.Literal("unknown") }),
    Schema.Struct({
      kind: Schema.Literal("already_absent"),
      completion: Schema.Literal("not_invoked"),
    }),
  ]),
  before: Readback,
  after: Readback,
  observedAt: Schema.String,
});
const jsonBinding = Schema.fromJsonString(Binding);
const jsonStart = Schema.fromJsonString(DeletionWorktreeRemovalStartV1);
const same = (left: unknown, right: unknown) =>
  nativeCreationCanonicalJson(left) === nativeCreationCanonicalJson(right);
// These methods execute inside EventSink's owning SQL context. They never run Git.
export const makeDeletionAdmission = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const path = yield* EffectPath.Path;
  const overlaps = (left: string, right: string) =>
    left === right ||
    left.startsWith(`${right}${path.sep}`) ||
    right.startsWith(`${left}${path.sep}`);
  const newlyIssuedStarts = new Set<string>();
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const failure = (reason: string) => new DeletionAdmissionError({ reason });
  const load = (effectId: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        readonly binding_json: string;
        readonly start_json: string;
        readonly state: string;
      }>`SELECT binding_json,start_json,state FROM jones_deletion_worktree_admissions WHERE effect_id=${effectId}`;
      if (rows.length !== 1) return null;
      return {
        binding: yield* Schema.decodeUnknownEffect(jsonBinding)(rows[0]!.binding_json),
        start: yield* Schema.decodeUnknownEffect(jsonStart)(rows[0]!.start_json),
        state: rows[0]!.state,
      };
    });
  const validateBinding = (binding: Binding) =>
    Effect.gen(function* () {
      const target = binding.target;
      if (
        !binding.rules.worktreeOnDelete ||
        target.force ||
        !path.isAbsolute(target.path) ||
        !path.isAbsolute(target.projectRoot) ||
        path.resolve(target.path) !== target.path ||
        path.resolve(target.projectRoot) !== target.projectRoot ||
        overlaps(target.path, target.projectRoot)
      )
        return yield* failure("original_policy_target_invalid");
      const native = yield* sql<{
        readonly worktree_path: string;
      }>`SELECT worktree_path FROM jones_native_workspace_admissions`;
      if (native.some((row) => overlaps(row.worktree_path, target.path)))
        return yield* failure("native_workspace_original_owner_handoff_required");
      // Lease expiry is not evidence that an old owner's uncertain effect ended.
      const claims = yield* sql<{
        readonly worktree_path: string | null;
      }>`SELECT worktree_path FROM native_creation_intents`;
      if (
        claims.some((row) => row.worktree_path !== null && overlaps(row.worktree_path, target.path))
      )
        return yield* failure("native_creation_original_owner_handoff_required");
      const leases = yield* sql<{
        readonly resource_path: string;
      }>`SELECT resource_path FROM worktree_ownership_leases`;
      if (leases.some((row) => overlaps(row.resource_path, target.path)))
        return yield* failure("worktree_original_lease_owner_unavailable");
      const deletion = yield* sql<{
        readonly payload_json: string;
        readonly command_id: string | null;
      }>`SELECT payload_json,command_id FROM orchestration_events WHERE event_id=${binding.deletionEventId} AND sequence=${binding.deletionSequence} AND stream_id=${binding.threadId} AND application_event_version=2 AND aggregate_kind='thread' AND event_type='thread.deleted'`;
      if (
        deletion.length !== 1 ||
        deletion[0]!.command_id !== binding.commandId ||
        deletion[0]!.payload_json !== binding.deletionPayload
      )
        return yield* failure("original_deletion_event_changed");
      const receipt = Option.getOrNull(yield* receipts.getByCommandId(binding.commandId));
      if (
        receipt?.status !== "accepted" ||
        receipt.threadId !== binding.threadId ||
        receipt.commandType !== "thread.delete" ||
        receipt.resultSequence < binding.deletionSequence
      )
        return yield* failure("original_deletion_receipt_unavailable");
      const birth = yield* sql<{
        readonly event_id: string;
        readonly sequence: number;
        readonly payload_json: string;
      }>`SELECT event_id,sequence,payload_json FROM orchestration_events WHERE stream_id=${binding.threadId} AND application_event_version=2 AND aggregate_kind='thread' AND event_type='thread.created' ORDER BY sequence DESC LIMIT 1`;
      if (
        birth.length !== 1 ||
        birth[0]!.event_id !== binding.birthEventId ||
        birth[0]!.sequence !== binding.birthSequence ||
        binding.birthSequence >= binding.deletionSequence
      )
        return yield* failure("original_application_birth_changed");
      const identity = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ThreadIdentity))(
        binding.deletionPayload,
      );
      const born = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(
          Schema.Struct({ id: ThreadId, projectId: ProjectId, createdAt: Schema.String }),
        ),
      )(birth[0]!.payload_json);
      const projection = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${binding.threadId}`;
      if (projection.length !== 1) return yield* failure("original_deleted_projection_unavailable");
      const current = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ThreadIdentity))(
        projection[0]!.payload_json,
      );
      if (
        identity.id !== binding.threadId ||
        identity.projectId !== target.projectId ||
        identity.deletedAt === null ||
        identity.worktreePath !== target.path ||
        identity.branch !== target.branch ||
        born.id !== identity.id ||
        born.projectId !== identity.projectId ||
        born.createdAt !== identity.createdAt ||
        !same(current, identity)
      )
        return yield* failure("original_deleted_thread_target_changed");
      const project = yield* sql<{
        readonly workspace_root: string;
      }>`SELECT workspace_root FROM projection_projects WHERE project_id=${target.projectId}`;
      if (project.length !== 1 || project[0]!.workspace_root !== target.projectRoot)
        return yield* failure("original_project_root_changed");
      const active = yield* sql<{
        readonly path: string | null;
      }>`SELECT json_extract(payload_json,'$.worktreePath') AS path FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL`;
      if (active.some((row) => row.path !== null && overlaps(row.path, target.path)))
        return yield* failure("shared_worktree_retained");
      const sessions = yield* sql<{
        readonly cwd: string;
      }>`SELECT json_extract(payload_json,'$.cwd') AS cwd FROM orchestration_v2_projection_provider_sessions WHERE status != 'stopped'`;
      if (sessions.some((row) => overlaps(row.cwd, target.path)))
        return yield* failure("provider_cleanup_not_observed");
      const pending =
        yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE thread_id=${binding.threadId} AND status NOT IN ('succeeded','cancelled') LIMIT 1`;
      if (pending.length > 0) return yield* failure("original_cleanup_prerequisites_unsettled");
    });
  const checkCurrent = (
    binding: Binding,
    currentRules?: DeletionPolicyRead,
    currentLive?: DeletionLiveRead,
  ) =>
    Effect.gen(function* () {
      if (currentRules === undefined || currentLive === undefined)
        return yield* failure("current_policy_owner_unavailable");
      const rules = yield* currentRules;
      if (!rules.worktreeOnDelete || !same(rules, binding.rules))
        return yield* failure("original_worktree_policy_changed");
      if (!(yield* currentLive))
        return yield* failure("current_cleanup_owner_or_live_prerequisites_changed");
      yield* validateBinding(binding);
    });
  const start = (input: DeletionAdmissionInput) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const deletions = yield* sql<{
            readonly event_id: string;
            readonly sequence: number;
            readonly command_id: string | null;
            readonly payload_json: string;
          }>`SELECT event_id,sequence,command_id,payload_json FROM orchestration_events WHERE stream_id=${input.threadId} AND application_event_version=2 AND aggregate_kind='thread' AND event_type='thread.deleted' ORDER BY sequence DESC LIMIT 1`;
          const births = yield* sql<{
            readonly event_id: string;
            readonly sequence: number;
          }>`SELECT event_id,sequence FROM orchestration_events WHERE stream_id=${input.threadId} AND application_event_version=2 AND aggregate_kind='thread' AND event_type='thread.created' ORDER BY sequence DESC LIMIT 1`;
          if (deletions.length !== 1 || births.length !== 1 || deletions[0]!.command_id === null)
            return yield* failure("original_birth_or_deletion_command_unavailable");
          const deletion = deletions[0]!;
          const binding: Binding = {
            actor: "storage-cleanup-policy",
            threadId: input.threadId,
            birthEventId: births[0]!.event_id,
            birthSequence: births[0]!.sequence,
            deletionEventId: deletion.event_id,
            deletionSequence: deletion.sequence,
            commandId: CommandId.make(deletion.command_id!),
            deletionPayload: deletion.payload_json,
            target: input.target,
            rules: input.rules,
          };
          yield* checkCurrent(binding, input.currentRules, input.currentLive);
          const bindingSha256 = nativeCreationSha256(nativeCreationCanonicalJson(binding));
          const effectId = `deletion-worktree:${binding.deletionEventId}:${bindingSha256}`;
          const existing = yield* sql<{
            readonly effect_id: string;
          }>`SELECT effect_id FROM jones_deletion_worktree_admissions WHERE worktree_path=${input.target.path}`;
          if (existing.length > 0) {
            if (existing[0]!.effect_id !== effectId)
              return yield* failure("another_original_worktree_operation_retained");
            const original = yield* load(effectId);
            if (original === null || !same(original.binding, binding))
              return yield* failure("original_removal_binding_changed");
            return { status: "observe_only" as const, start: original.start, ordinal: 0 };
          }
          const startedAt = DateTime.formatIso(yield* DateTime.now);
          const captured: DeletionWorktreeRemovalStartV1 = {
            schema: "t3.deletion-worktree-removal-start/v1",
            effectId,
            bindingSha256,
            workerId: "storage-cleanup-policy",
            expectedAttempt: 1,
            target: input.target,
            startedAt,
          };
          yield* sql`INSERT INTO jones_deletion_worktree_admissions(effect_id,worktree_path,binding_json,start_json,state) VALUES(${effectId},${input.target.path},${nativeCreationCanonicalJson(binding)},${nativeCreationCanonicalJson(captured)},'started')`;
          return { status: "start_now" as const, start: captured, ordinal: 0 };
        }),
      )
      .pipe(
        Effect.map((result) => {
          // Lost responses and process recovery cannot recreate this fresh invocation permit.
          if (result.status === "start_now") newlyIssuedStarts.add(result.start.effectId);
          return result;
        }),
        Effect.mapError((cause) =>
          Schema.is(DeletionAdmissionError)(cause)
            ? cause
            : new DeletionAdmissionError({ reason: "deletion_admission_failed", cause }),
        ),
      );
  const read = (effectId: string) =>
    load(effectId).pipe(
      Effect.map((row) => (row === null ? null : { start: row.start, ordinal: 0 })),
      Effect.mapError(
        (cause) => new DeletionAdmissionError({ reason: "original_start_read_failed", cause }),
      ),
    );
  const readTarget = (threadId: ThreadId, target: DeletionWorktreeRemovalTargetV1) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        readonly effect_id: string;
      }>`SELECT effect_id FROM jones_deletion_worktree_admissions WHERE worktree_path=${target.path}`;
      if (rows.length === 0) return null;
      const original = yield* load(rows[0]!.effect_id);
      if (
        original === null ||
        original.binding.threadId !== threadId ||
        !same(original.binding.target, target)
      )
        return yield* failure("another_original_worktree_operation_retained");
      return { start: original.start, ordinal: 0 };
    }).pipe(
      Effect.mapError((cause) =>
        Schema.is(DeletionAdmissionError)(cause)
          ? cause
          : new DeletionAdmissionError({ reason: "original_target_read_failed", cause }),
      ),
    );
  const revalidate = (
    captured: DeletionWorktreeRemovalStartV1,
    ordinal: number,
    rules?: DeletionPolicyRead,
    live?: DeletionLiveRead,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const original = yield* load(captured.effectId);
          if (
            original === null ||
            original.state !== "started" ||
            ordinal !== 0 ||
            !same(original.start, captured) ||
            nativeCreationSha256(nativeCreationCanonicalJson(original.binding)) !==
              captured.bindingSha256
          )
            return yield* failure("original_start_no_longer_executable");
          const outcomes =
            yield* sql`SELECT 1 FROM jones_deletion_worktree_observations WHERE effect_id=${captured.effectId} LIMIT 1`;
          if (outcomes.length > 0) return yield* failure("original_start_already_observed");
          if (!newlyIssuedStarts.has(captured.effectId))
            return yield* failure("original_start_observation_only");
          yield* checkCurrent(original.binding, rules, live);
          // Consume after the final owner check, before control returns to the Git producer.
          // Interruption afterwards leaves the durable start retained, never replayable.
          if (!newlyIssuedStarts.delete(captured.effectId))
            return yield* failure("original_start_observation_only");
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          Schema.is(DeletionAdmissionError)(cause)
            ? cause
            : new DeletionAdmissionError({ reason: "original_start_revalidation_failed", cause }),
        ),
      );
  const qualify = (input: DeletionWorktreeRemovalObservationV1) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const observation = yield* Schema.decodeEffect(Observation)(input, {
            onExcessProperty: "error",
          });
          const original = yield* load(observation.start.effectId);
          const observedAt = DateTime.make(observation.observedAt);
          const startTime = DateTime.make(observation.start.startedAt);
          if (
            original === null ||
            observation.startOrdinal !== 0 ||
            !same(original.start, observation.start) ||
            nativeCreationSha256(nativeCreationCanonicalJson(original.binding)) !==
              observation.start.bindingSha256 ||
            Option.isNone(observedAt) ||
            Option.isNone(startTime) ||
            DateTime.formatIso(observedAt.value) !== observation.observedAt ||
            DateTime.toEpochMillis(observedAt.value) < DateTime.toEpochMillis(startTime.value)
          )
            return yield* failure("observation_original_operation_mismatch");
          yield* validateBinding(original.binding);
          const target = original.binding.target;
          for (const readback of [observation.before, observation.after]) {
            if (
              readback.filesystem.path !== target.path ||
              (readback.registration.status === "complete" &&
                (readback.registration.projectRoot !== target.projectRoot ||
                  !path.isAbsolute(readback.registration.gitCommonDirectory) ||
                  new Set(readback.registration.entries.map((entry) => entry.path)).size !==
                    readback.registration.entries.length ||
                  readback.registration.entries.some(
                    (entry) =>
                      !path.isAbsolute(entry.path) || path.resolve(entry.path) !== entry.path,
                  )))
            )
              return yield* failure("observation_target_readback_mismatch");
          }
          const before = observation.before.registration;
          const after = observation.after.registration;
          const entry =
            before.status === "complete"
              ? before.entries.find((row) => row.path === target.path)
              : undefined;
          const absent =
            before.status === "complete" &&
            after.status === "complete" &&
            before.gitCommonDirectory === after.gitCommonDirectory &&
            observation.after.filesystem.status === "absent" &&
            !after.entries.some((row) => row.path === target.path);
          const confirmed =
            absent &&
            observation.operation.kind === "executed" &&
            observation.operation.completion === "exited" &&
            observation.operation.exitCode === 0 &&
            observation.before.filesystem.status === "present" &&
            entry !== undefined &&
            !entry.bare &&
            (entry.branch === target.branch || entry.branch === `refs/heads/${target.branch}`);
          const absence =
            absent &&
            (observation.operation.kind === "reconciled" ||
              (observation.operation.kind === "already_absent" &&
                entry === undefined &&
                observation.before.filesystem.status === "absent"));
          const result = confirmed ? "confirmed" : absence ? "absent" : "unknown";
          const rows = yield* sql<{
            readonly ordinal: number;
          }>`SELECT ordinal FROM jones_deletion_worktree_observations WHERE effect_id=${observation.start.effectId} ORDER BY ordinal DESC LIMIT 1`;
          const ordinal = (rows[0]?.ordinal ?? 0) + 1;
          yield* sql`INSERT INTO jones_deletion_worktree_observations(effect_id,ordinal,observation_json,result) VALUES(${observation.start.effectId},${ordinal},${nativeCreationCanonicalJson(observation)},${result})`;
          yield* sql`UPDATE jones_deletion_worktree_admissions SET state=${result === "unknown" ? "unknown" : "completed"} WHERE effect_id=${observation.start.effectId} AND state != 'completed'`;
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          Schema.is(DeletionAdmissionError)(cause)
            ? cause
            : new DeletionAdmissionError({ reason: "observation_qualification_failed", cause }),
        ),
      );
  return { start, read, readTarget, revalidate, qualify };
});
