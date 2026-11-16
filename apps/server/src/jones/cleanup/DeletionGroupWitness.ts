import {
  OrchestrationV2AppThreadJson as OrchestrationV2AppThread,
  OrchestrationV2ExecutionNodeJson as OrchestrationV2ExecutionNode,
  OrchestrationV2ProviderSessionJson as OrchestrationV2ProviderSession,
  OrchestrationV2ProviderThreadJson as OrchestrationV2ProviderThread,
  OrchestrationV2ProviderTurnJson as OrchestrationV2ProviderTurn,
  OrchestrationV2RunJson as OrchestrationV2Run,
  OrchestrationV2RunAttemptJson as OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { canonical, digest, DirectoryIdentity } from "../ordinaryCheckout/CheckoutProtocol.ts";
import { CheckoutTargetObservation, overlaps } from "../ordinaryCheckout/CheckoutTarget.ts";
import { DeletionWorktreeRemovalTargetV1 } from "./DeletionWorktreeRemovalTypes.ts";

export class DeletionGroupWitnessError extends Schema.TaggedError<DeletionGroupWitnessError>()(
  "DeletionGroupWitnessError",
  {
    reason: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}
const id = Schema.NonEmptyString;
const hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const reference = Schema.Struct({ eventId: id, sequence: Schema.Int, sha256: hash });
const member = Schema.Struct({
  threadId: id,
  parentThreadId: Schema.NullOr(id),
  birth: reference,
  edge: Schema.NullOr(reference),
  provenanceSha256: hash,
  identitySha256: hash,
});
export const DeletionGroupPhysicalV1 = Schema.Struct({
  directory: Schema.Struct({ realPath: id, identity: DirectoryIdentity }),
  project: Schema.Struct({ realPath: id, identity: DirectoryIdentity }),
  gitCommonDirectory: id,
  gitDirectory: id,
  gitCommonIdentity: DirectoryIdentity,
  gitDirectoryIdentity: DirectoryIdentity,
  head: id,
  branch: Schema.NullOr(id),
});
export type DeletionGroupPhysicalV1 = typeof DeletionGroupPhysicalV1.Type;
export const DeletionGroupWitnessV1 = Schema.Struct({
  schema: Schema.Literal("jones.deletion-original-native-group/v1"),
  ownerThreadId: id,
  deletionEventId: id,
  deletionSequence: Schema.Int,
  target: DeletionWorktreeRemovalTargetV1,
  members: Schema.Array(member),
  generationsSha256: hash,
  physical: DeletionGroupPhysicalV1,
});
export type DeletionGroupWitnessV1 = typeof DeletionGroupWitnessV1.Type;

// The production owner must provide this port only while it holds the complete
// workspace exclusion. SQL reconstruction below remains independent authority.
// Historical v2 admissions retain this requirement. The v3 completed-native
// policy records projected completion and does not claim physical retirement.
export class DeletionGroupRuntime extends Context.Service<
  DeletionGroupRuntime,
  {
    readonly capture: (
      target: DeletionWorktreeRemovalTargetV1,
    ) => Effect.Effect<DeletionGroupPhysicalV1, DeletionGroupWitnessError>;
    readonly revalidate: (
      witness: DeletionGroupWitnessV1,
    ) => Effect.Effect<void, DeletionGroupWitnessError>;
  }
>()("t3/jones/cleanup/DeletionGroupWitness/DeletionGroupRuntime") {}

// Identity-only read view: persisted subagent bodies can omit their prompt/result.
// These fields are all required; this is not a more permissive wire decoder.
const nativeEdge = Schema.Struct({
  id,
  threadId: id,
  runId: id,
  parentNodeId: id,
  origin: Schema.Literal("provider_native"),
  driver: id,
  providerInstanceId: id,
  providerThreadId: Schema.NullOr(id),
  childThreadId: id,
});
const metadata = Schema.Struct({
  rawEventId: id,
  driver: id,
  providerInstanceId: id,
  runId: Schema.optional(Schema.NullOr(id)),
  nodeId: Schema.optional(Schema.NullOr(id)),
});
const eventRow = Schema.Struct({
  sequence: Schema.Int,
  event_id: id,
  stream_id: id,
  event_type: id,
  actor_kind: id,
  payload_json: Schema.String,
  metadata_json: Schema.String,
});
type EventRow = typeof eventRow.Type;
const decodeThreadJson = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2AppThread));
const decodeRunJson = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2Run));
const decodeAttemptJson = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2RunAttempt));
const decodeNodeJson = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2ExecutionNode));
const decodeTurnJson = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2ProviderTurn));
const decodeProviderJson = Schema.decodeEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThread),
);
const decodeSessionJson = Schema.decodeEffect(
  Schema.fromJsonString(OrchestrationV2ProviderSession),
);
const decodeMetadataJson = Schema.decodeEffect(Schema.fromJsonString(metadata));
const decodeSubagentStatusJson = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({ status: Schema.String, completedAt: Schema.NullOr(Schema.String) }),
  ),
);
const decodeRecordJson = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const isDeletionGroupWitnessError = Schema.is(DeletionGroupWitnessError);

const groupError = (cause: unknown) =>
  isDeletionGroupWitnessError(cause)
    ? cause
    : new DeletionGroupWitnessError({ reason: "original_group_evidence_unavailable", cause });
const fail = (reason: string) => Effect.fail(new DeletionGroupWitnessError({ reason }));
const ref = (row: EventRow) => ({
  eventId: row.event_id,
  sequence: row.sequence,
  sha256: digest(row),
});
const threadIdentity = (thread: OrchestrationV2AppThread) => ({
  id: thread.id,
  projectId: thread.projectId,
  createdAt: thread.createdAt,
  worktreePath: thread.worktreePath,
  branch: thread.branch,
  lineage: thread.lineage,
  forkedFrom: thread.forkedFrom,
  createdBy: thread.createdBy,
  creationSource: thread.creationSource,
});
const terminal = new Set([
  "completed",
  "interrupted",
  "failed",
  "cancelled",
  "rolled_back",
  "superseded",
]);
const decodeOrchestrationV2ExecutionNode = Schema.decodeUnknownEffect(OrchestrationV2ExecutionNode);
const decodeOrchestrationV2Run = Schema.decodeUnknownEffect(OrchestrationV2Run);
const decodeOrchestrationV2RunAttempt = Schema.decodeUnknownEffect(OrchestrationV2RunAttempt);
const decodeOrchestrationV2ProviderThread = Schema.decodeUnknownEffect(
  OrchestrationV2ProviderThread,
);
const decodeOrchestrationV2ProviderSession = Schema.decodeUnknownEffect(
  OrchestrationV2ProviderSession,
);
const decodeDeletionGroupWitnessV1 = Schema.decodeEffect(DeletionGroupWitnessV1);

// SQL evidence only. Each caller supplies its owning writer client, including
// transaction context; no filesystem, provider calls or wall-clock liveness here.
export const makeDeletionGroupEvidence = Effect.fn("DeletionGroupWitness.makeEvidence")(function* (
  sql: SqlClient.SqlClient,
) {
  const path = yield* Path.Path;
  const boundaryRows = Effect.fn("DeletionGroupWitness.boundaryRows")(function* (
    ownerThreadId: string,
    deletionEventId: string,
    deletionSequence: number,
    target: DeletionWorktreeRemovalTargetV1,
  ) {
    const deletion =
      yield* sql`SELECT 1 FROM orchestration_events WHERE event_id=${deletionEventId} AND sequence=${deletionSequence} AND stream_id=${ownerThreadId} AND event_type='thread.deleted' AND application_event_version=2`;
    if (deletion.length !== 1) return yield* fail("original_group_deletion_boundary_unavailable");
    const births =
      yield* sql<EventRow>`SELECT sequence,event_id,stream_id,event_type,actor_kind,payload_json,metadata_json FROM orchestration_events WHERE application_event_version=2 AND aggregate_kind='thread' AND event_type='thread.created' AND sequence<${deletionSequence} AND json_extract(payload_json,'$.projectId')=${target.projectId} ORDER BY sequence`;
    const original = new Map<
      string,
      { row: EventRow; thread: typeof OrchestrationV2AppThread.Type }
    >();
    for (const row of births) {
      const thread = yield* decodeThreadJson(row.payload_json);
      if (thread.worktreePath === null || !overlaps(thread.worktreePath, target.path, path.sep))
        continue;
      if (
        thread.worktreePath !== target.path ||
        thread.branch !== target.branch ||
        thread.id !== row.stream_id ||
        original.has(thread.id)
      )
        return yield* fail("original_group_birth_ambiguous");
      original.set(thread.id, { row, thread });
    }
    const owner = original.get(ownerThreadId);
    if (
      owner === undefined ||
      owner.thread.lineage.parentThreadId !== null ||
      owner.thread.lineage.rootThreadId !== ownerThreadId
    )
      return yield* fail("original_group_root_unavailable");
    if (original.size < 2) return yield* fail("original_native_group_unavailable");
    const ids = [...original.keys()].sort();
    const rows =
      yield* sql<EventRow>`SELECT sequence,event_id,stream_id,event_type,actor_kind,payload_json,metadata_json FROM orchestration_events WHERE application_event_version=2 AND aggregate_kind='thread' AND ${sql.in("stream_id", ids)} AND sequence<${deletionSequence} AND event_type IN ('subagent.updated','node.updated','run.created','run.updated','run-attempt.created','run-attempt.updated','provider-thread.updated','provider-session.updated') ORDER BY sequence`;
    // Decode once; retain complete raw row digests, including provider metadata.
    const decoded = yield* Effect.forEach(rows, (row) =>
      Effect.map(decodeRecordJson(row.payload_json), (payload) => ({ row, payload })),
    );
    const latest = (threadId: string, types: readonly string[], key: string) =>
      decoded
        .filter(
          (item) =>
            item.row.stream_id === threadId &&
            types.includes(item.row.event_type) &&
            item.payload.id === key,
        )
        .at(-1);
    const members: Array<typeof member.Type> = [];
    for (const threadId of ids) {
      const born = original.get(threadId)!;
      if (threadId === ownerThreadId) {
        members.push({
          threadId,
          parentThreadId: null,
          birth: ref(born.row),
          edge: null,
          identitySha256: digest(threadIdentity(born.thread)),
          provenanceSha256: digest([born.row]),
        });
        continue;
      }
      const parentId = born.thread.lineage.parentThreadId;
      if (
        born.thread.lineage.relationshipToParent !== "subagent" ||
        born.thread.lineage.sideChat === true ||
        parentId === null ||
        !original.has(parentId) ||
        born.thread.lineage.rootThreadId !== ownerThreadId ||
        born.thread.forkedFrom?.type !== "node"
      )
        return yield* fail("original_group_native_parent_unavailable");
      const seen = new Set([threadId]);
      let ancestor: string | null = parentId;
      while (ancestor !== null) {
        if (seen.has(ancestor) || !original.has(ancestor))
          return yield* fail("original_group_lineage_cycle_or_gap");
        seen.add(ancestor);
        ancestor = original.get(ancestor)!.thread.lineage.parentThreadId;
      }
      if (!seen.has(ownerThreadId)) return yield* fail("original_group_wrong_root");
      const edgeRow = latest(parentId, ["subagent.updated"], born.thread.forkedFrom.nodeId);
      if (edgeRow === undefined) return yield* fail("original_group_native_edge_unavailable");
      const edge = yield* Schema.decodeUnknownEffect(nativeEdge)(edgeRow.payload);
      const edgeMeta = yield* decodeMetadataJson(edgeRow.row.metadata_json);
      const birthMeta = yield* decodeMetadataJson(born.row.metadata_json);
      if (
        edgeRow.row.actor_kind !== "provider" ||
        born.row.actor_kind !== "provider" ||
        edge.threadId !== parentId ||
        edge.childThreadId !== threadId ||
        edge.id !== born.thread.forkedFrom.nodeId ||
        edgeMeta.nodeId !== edge.id ||
        edgeMeta.runId !== edge.runId ||
        edgeMeta.driver !== edge.driver ||
        edgeMeta.providerInstanceId !== edge.providerInstanceId ||
        birthMeta.driver !== edge.driver ||
        birthMeta.providerInstanceId !== edge.providerInstanceId
      )
        return yield* fail("original_group_native_provenance_mismatch");
      const nodeRow = latest(parentId, ["node.updated"], edge.parentNodeId);
      const runRow = latest(parentId, ["run.created", "run.updated"], edge.runId);
      if (nodeRow === undefined || runRow === undefined)
        return yield* fail("original_group_parent_execution_unavailable");
      const node = yield* decodeOrchestrationV2ExecutionNode(nodeRow.payload);
      const run = yield* decodeOrchestrationV2Run(runRow.payload);
      if (
        node.threadId !== parentId ||
        node.runId !== run.id ||
        node.rootNodeId !== run.rootNodeId ||
        run.threadId !== parentId ||
        run.activeAttemptId === null ||
        run.providerThreadId === null ||
        run.providerInstanceId !== edge.providerInstanceId
      )
        return yield* fail("original_group_parent_execution_mismatch");
      const attemptRow = latest(
        parentId,
        ["run-attempt.created", "run-attempt.updated"],
        run.activeAttemptId,
      );
      const providerRow = latest(parentId, ["provider-thread.updated"], run.providerThreadId);
      if (attemptRow === undefined || providerRow === undefined)
        return yield* fail("original_group_parent_session_unavailable");
      const attempt = yield* decodeOrchestrationV2RunAttempt(attemptRow.payload);
      const provider = yield* decodeOrchestrationV2ProviderThread(providerRow.payload);
      if (
        attempt.runId !== run.id ||
        attempt.providerThreadId !== provider.id ||
        attempt.providerInstanceId !== edge.providerInstanceId ||
        provider.providerInstanceId !== edge.providerInstanceId ||
        provider.driver !== edge.driver ||
        provider.appThreadId !== parentId ||
        provider.providerSessionId === null ||
        provider.nativeThreadRef === null
      )
        return yield* fail("original_group_parent_session_mismatch");
      const sessionRow = latest(parentId, ["provider-session.updated"], provider.providerSessionId);
      if (sessionRow === undefined) return yield* fail("original_group_parent_session_unavailable");
      const session = yield* decodeOrchestrationV2ProviderSession(sessionRow.payload);
      if (
        session.cwd !== target.path ||
        session.driver !== edge.driver ||
        session.providerInstanceId !== edge.providerInstanceId
      )
        return yield* fail("original_group_session_target_mismatch");
      members.push({
        threadId,
        parentThreadId: parentId,
        birth: ref(born.row),
        edge: ref(edgeRow.row),
        identitySha256: digest(threadIdentity(born.thread)),
        provenanceSha256: digest([
          born.row,
          edgeRow.row,
          nodeRow.row,
          runRow.row,
          attemptRow.row,
          providerRow.row,
          sessionRow.row,
        ]),
      });
    }
    return members;
  });

  const generations = Effect.fn("DeletionGroupWitness.generations")(function* (
    ownerThreadId: string,
    members: readonly (typeof member.Type)[],
    target: DeletionWorktreeRemovalTargetV1,
  ) {
    const ids = members.map((item) => item.threadId);
    const projections = yield* sql<{
      readonly thread_id: string;
      readonly payload_json: string;
    }>`SELECT thread_id,payload_json FROM orchestration_v2_projection_threads WHERE ${sql.in("thread_id", ids)} ORDER BY thread_id`;
    if (projections.length !== ids.length) return yield* fail("original_group_member_missing");
    for (const row of projections) {
      const thread = yield* decodeThreadJson(row.payload_json);
      const identity = members.find((item) => item.threadId === row.thread_id)!;
      if (
        digest(threadIdentity(thread)) !== identity.identitySha256 ||
        thread.id !== identity.threadId ||
        thread.projectId !== target.projectId ||
        thread.worktreePath !== target.path ||
        thread.branch !== target.branch ||
        thread.lineage.parentThreadId !== identity.parentThreadId ||
        thread.lineage.rootThreadId !== ownerThreadId ||
        (thread.id === ownerThreadId ? thread.deletedAt === null : thread.deletedAt !== null) ||
        (identity.edge !== null && thread.forkedFrom?.type !== "node")
      )
        return yield* fail("original_group_member_identity_changed");
    }
    // Enumerate all overlapping projections. No caller-supplied exclusion list.
    const active = yield* sql<{
      readonly thread_id: string;
      readonly path: string | null;
    }>`SELECT thread_id,json_extract(payload_json,'$.worktreePath') AS path FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL ORDER BY thread_id`;
    const overlapping = active.filter(
      (row) => row.path !== null && overlaps(row.path, target.path, path.sep),
    );
    if (
      canonical(overlapping.map((row) => row.thread_id)) !==
      canonical(ids.filter((id) => id !== ownerThreadId))
    )
      return yield* fail("original_group_membership_changed");
    const runs = yield* sql<{
      readonly payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE ${sql.in("thread_id", ids)} ORDER BY thread_id,ordinal`;
    for (const row of runs) {
      const run = yield* decodeRunJson(row.payload_json);
      if (
        !terminal.has(run.status) ||
        run.completedAt === null ||
        run.queueHeld === true ||
        run.workspacePreparation !== undefined
      )
        return yield* fail("original_group_run_unsettled");
    }
    const attempts = yield* sql<{
      readonly payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_run_attempts WHERE ${sql.in("thread_id", ids)} ORDER BY thread_id,run_id,attempt_ordinal`;
    for (const row of attempts) {
      const attempt = yield* decodeAttemptJson(row.payload_json);
      if (
        !terminal.has(attempt.status) ||
        attempt.completedAt === null ||
        attempt.providerSettlement == null ||
        attempt.providerSettlement.runAttemptId !== attempt.id ||
        attempt.providerSettlement.providerTurnId !== attempt.providerTurnId
      )
        return yield* fail("original_group_provider_settlement_unavailable");
    }
    const nodes = yield* sql<{
      readonly payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_nodes WHERE ${sql.in("thread_id", ids)} ORDER BY thread_id,node_id`;
    for (const row of nodes) {
      const node = yield* decodeNodeJson(row.payload_json);
      if (!terminal.has(node.status) || node.completedAt === null)
        return yield* fail("original_group_node_unsettled");
    }
    const turns = yield* sql<{
      readonly payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_provider_turns WHERE ${sql.in("thread_id", ids)} ORDER BY thread_id,provider_turn_id`;
    for (const row of turns) {
      const turn = yield* decodeTurnJson(row.payload_json);
      if (!terminal.has(turn.status) || turn.completedAt === null)
        return yield* fail("original_group_turn_unsettled");
    }
    const subagents = yield* sql<{
      readonly payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_subagents WHERE ${sql.in("thread_id", ids)} ORDER BY thread_id,subagent_id`;
    for (const row of subagents) {
      const status = yield* decodeSubagentStatusJson(row.payload_json);
      if (!terminal.has(status.status) || status.completedAt === null)
        return yield* fail("original_group_subagent_unsettled");
    }
    const requests = yield* sql<{
      readonly payload_json: string;
      readonly status: string;
    }>`SELECT payload_json,status FROM orchestration_v2_projection_runtime_requests WHERE ${sql.in("thread_id", ids)} ORDER BY thread_id,runtime_request_id`;
    if (requests.some((row) => !["resolved", "expired", "cancelled"].includes(row.status)))
      return yield* fail("original_group_request_unsettled");
    const providers = yield* sql<{
      readonly payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE ${sql.in("thread_id", ids)} ORDER BY provider_thread_id`;
    const sessionIds = new Set<string>();
    for (const row of providers) {
      const provider = yield* decodeProviderJson(row.payload_json);
      if (
        !["idle", "closed", "archived"].includes(provider.status) ||
        (provider.pendingBackgroundTasks?.length ?? 0) !== 0 ||
        provider.providerSessionId === null
      )
        return yield* fail("original_group_provider_unsettled");
      sessionIds.add(provider.providerSessionId);
    }
    const sessions = yield* sql<{
      readonly provider_session_id: string;
      readonly payload_json: string;
      readonly status: string;
    }>`SELECT provider_session_id,payload_json,status FROM orchestration_v2_projection_provider_sessions ORDER BY provider_session_id`;
    const relevantSessions: (typeof sessions)[number][] = [];
    for (const row of sessions) {
      const session = yield* decodeSessionJson(row.payload_json);
      if (!sessionIds.has(row.provider_session_id) && !overlaps(session.cwd, target.path, path.sep))
        continue;
      relevantSessions.push(row);
      if (session.status !== "stopped" || row.status !== "stopped")
        return yield* fail("original_group_session_not_stopped");
      sessionIds.delete(row.provider_session_id);
    }
    if (sessionIds.size !== 0) return yield* fail("original_group_session_missing");
    const pending =
      yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE ${sql.in("thread_id", ids)} AND status NOT IN ('succeeded','cancelled') LIMIT 1`;
    if (pending.length !== 0) return yield* fail("original_group_outbox_unsettled");
    const cursors =
      yield* sql`SELECT stream_id,MAX(sequence) AS sequence FROM orchestration_events WHERE application_event_version=2 AND ${sql.in("stream_id", ids)} GROUP BY stream_id ORDER BY stream_id`;
    const outbox =
      yield* sql`SELECT effect_id,status,attempt_count,updated_at FROM orchestration_v2_effect_outbox WHERE ${sql.in("thread_id", ids)} ORDER BY effect_id`;
    return digest({
      projections,
      runs,
      attempts,
      nodes,
      turns,
      subagents,
      requests,
      providers,
      sessions: relevantSessions,
      cursors,
      outbox,
    });
  });

  return {
    capture: Effect.fn("DeletionGroupWitness.captureEvidence")(function* (input: {
      readonly ownerThreadId: string;
      readonly deletionEventId: string;
      readonly deletionSequence: number;
      readonly target: DeletionWorktreeRemovalTargetV1;
      readonly physical: DeletionGroupPhysicalV1;
    }) {
      const members = yield* boundaryRows(
        input.ownerThreadId,
        input.deletionEventId,
        input.deletionSequence,
        input.target,
      );
      const generationsSha256 = yield* generations(input.ownerThreadId, members, input.target);
      return yield* decodeDeletionGroupWitnessV1(
        { schema: "jones.deletion-original-native-group/v1", ...input, members, generationsSha256 },
        { onExcessProperty: "error" },
      );
    }, Effect.mapError(groupError)),
    validate: Effect.fn("DeletionGroupWitness.validatePersisted")(function* (
      witness: DeletionGroupWitnessV1,
    ) {
      const decoded = yield* decodeDeletionGroupWitnessV1(witness, {
        onExcessProperty: "error",
      });
      const members = yield* boundaryRows(
        decoded.ownerThreadId,
        decoded.deletionEventId,
        decoded.deletionSequence,
        decoded.target,
      );
      if (
        canonical(members) !== canonical(decoded.members) ||
        (yield* generations(decoded.ownerThreadId, members, decoded.target)) !==
          decoded.generationsSha256
      )
        return yield* fail("original_group_generation_changed");
    }, Effect.mapError(groupError)),
  };
});

// Physical observation is explicitly separate from the persisted validator.
export const captureDeletionGroupPhysical = Effect.fn("DeletionGroupWitness.capturePhysical")(
  function* (target: DeletionWorktreeRemovalTargetV1) {
    const sql = yield* SqlClient.SqlClient;
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* fail("group_physical_observation_inside_transaction");
    const observation = yield* CheckoutTargetObservation;
    const directory = yield* observation.directory(target.path);
    const project = yield* observation.directory(target.projectRoot);
    const git = yield* observation.git(directory.realPath);
    const projectGit = yield* observation.git(project.realPath);
    if (
      directory.realPath !== target.path ||
      project.realPath !== target.projectRoot ||
      git === null ||
      projectGit === null ||
      git.root !== directory.realPath ||
      git.commonDirectory !== projectGit.commonDirectory ||
      !projectGit.registeredWorktrees.includes(directory.realPath) ||
      git.head === undefined ||
      git.branch !==
        (target.branch === null
          ? null
          : target.branch.startsWith("refs/")
            ? target.branch
            : `refs/heads/${target.branch}`)
    )
      return yield* fail("original_group_physical_identity_unavailable");
    return {
      directory,
      project,
      gitCommonDirectory: git.commonDirectory,
      gitDirectory: git.gitDirectory,
      gitCommonIdentity: git.commonIdentity,
      gitDirectoryIdentity: git.directoryIdentity,
      head: git.head,
      branch: git.branch,
    } satisfies DeletionGroupPhysicalV1;
  },
  Effect.mapError(groupError),
);
