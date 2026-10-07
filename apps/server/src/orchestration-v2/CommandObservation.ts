import { RecordedRunJson as OrchestrationV2RunJson } from "./RecordedTypes.ts";
import {
  type CommandId,
  type MessageId,
  type ThreadId,
  TurnId,
  type OrchestrationCommandObservation,
  type OrchestrationDispatchTarget,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { EventSinkV2 } from "./EventSink.ts";
import { legacyBootstrapBirth } from "./LegacyBootstrap.ts";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as ProjectionStore from "./ProjectionStore.ts";

export const makeCommandObservationQuery = Effect.fn("makeCommandObservationQuery")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const eventSink = yield* EventSinkV2;
  const readTarget = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const [row] = yield* sql<{ snapshotSequence: number; lastEventSequence: number }>`SELECT
      COALESCE((SELECT MAX(sequence) FROM orchestration_events), 0) AS snapshotSequence,
      COALESCE((SELECT MAX(sequence) FROM orchestration_events WHERE aggregate_kind = 'thread'
        AND stream_id = ${threadId} AND event_type NOT IN ('legacy-bootstrap.preflight-intent', 'legacy-bootstrap.preflight-outcome')), 0) AS lastEventSequence`;
      const shell = yield* projections.getThreadShell(threadId);
      if (shell === null) return { ...row!, target: null };
      const records = yield* projections.getThreadRecords(threadId, [
        "runs",
        "providerSessions",
        "runtimeRequests",
      ]);
      const session = records.providerSessions
        .filter((s) => s.providerInstanceId === shell.providerInstanceId)
        .toSorted(
          (a, b) => DateTime.toEpochMillis(b.updatedAt) - DateTime.toEpochMillis(a.updatedAt),
        )[0];
      const blockers: Array<OrchestrationDispatchTarget["blockers"][number]> = [];
      if (shell.archivedAt !== null) blockers.push("archived");
      if (shell.settledOverride === "settled") blockers.push("settled");
      if (records.runs.some((r) => ["preparing", "queued", "starting"].includes(r.status)))
        blockers.push("pending_turn");
      if (records.runs.some((r) => ["running", "waiting"].includes(r.status)))
        blockers.push("running_turn");
      if (session?.status === "starting") blockers.push("session_starting");
      if (session?.status === "running" || session?.status === "waiting")
        blockers.push("session_running");
      if (shell.activeRunId !== null) blockers.push("active_turn");
      if (
        records.runtimeRequests.some(
          (request) => request.status === "pending" && request.kind !== "user_input",
        )
      )
        blockers.push("pending_approval");
      if (
        records.runtimeRequests.some(
          (request) => request.status === "pending" && request.kind === "user_input",
        )
      )
        blockers.push("pending_user_input");
      if (shell.hasActionableProposedPlan) blockers.push("actionable_plan");
      if ((shell.pendingBackgroundTasks?.length ?? 0) > 0) blockers.push("background_work");
      const target: OrchestrationDispatchTarget = {
        modelSelection: shell.modelSelection,
        sessionStatus:
          session === undefined ? null : session.status === "waiting" ? "running" : session.status,
        activeTurnId: shell.activeRunId === null ? null : TurnId.make(shell.activeRunId),
        latestTurnId: shell.latestRunId === null ? null : TurnId.make(shell.latestRunId),
        requireIdle: true,
        idle: blockers.length === 0,
        blockers,
      };
      return { ...row!, target };
    });
  const getTarget = (threadId: ThreadId) => sql.withTransaction(readTarget(threadId));
  const observe = (input: { threadId: ThreadId; commandId: CommandId; messageId: MessageId }) =>
    sql.withTransaction(
      Effect.gen(function* () {
        const { snapshotSequence, target } = yield* readTarget(input.threadId);
        const receipts = yield* sql<{
          aggregate_kind: string;
          command_type: string;
          aggregate_id: string;
          status: "accepted" | "rejected";
          result_sequence: number;
        }>`
      SELECT aggregate_kind, command_type, aggregate_id, status, result_sequence FROM orchestration_command_receipts WHERE command_id = ${input.commandId}`;
        const events = yield* sql<{
          sequence: number;
          stream_id: string;
          aggregate_kind: string;
          event_type: string;
          payload_json: string;
        }>`
      SELECT sequence, stream_id, aggregate_kind, event_type, payload_json FROM orchestration_events WHERE command_id = ${input.commandId} ORDER BY sequence`;
        const runs = yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_runs
      WHERE thread_id = ${input.threadId} AND json_extract(payload_json, '$.userMessageId') = ${input.messageId} LIMIT 2`;
        const births = events.filter((e) => e.event_type === "run.created");
        const receipt = receipts[0];
        let correlation: OrchestrationCommandObservation["correlation"] = "missing";
        let turn: OrchestrationCommandObservation["turn"] = null;
        let created = yield* Effect.forEach(births, (e) =>
          Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2RunJson))(e.payload_json),
        );
        let releaseBinding = false;
        if (receipt?.command_type === "prepared-run.release" && receipt.status === "accepted") {
          const releases = yield* Effect.forEach(
            events.filter((e) => e.event_type === "run.updated"),
            (e) =>
              Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2RunJson))(
                e.payload_json,
              ).pipe(Effect.map((run) => ({ run, sequence: e.sequence }))),
          );
          const legacyReleases = releases.filter(({ run }) => run.legacyBootstrap !== undefined);
          if (legacyReleases.length > 1) correlation = "ambiguous";
          else if (legacyReleases.length === 1) {
            const release = legacyReleases[0]!;
            const policy = release.run.legacyBootstrap!;
            const collect = (commandId: CommandId) =>
              eventSink.readByCommandId({ commandId }).pipe(
                Stream.runCollect,
                Effect.map((stored) => Array.from(stored)),
              );
            const proof = legacyBootstrapBirth({
              policy,
              claimEvents: yield* collect(policy.createCommandId),
              birthEvents: yield* collect(policy.birthCommandId),
            });
            const bindings = yield* sql<{
              command_id: string;
              command_type: string;
              aggregate_kind: string;
              aggregate_id: string;
              status: string;
              result_sequence: number;
            }>`
              SELECT command_id, command_type, aggregate_kind, aggregate_id, status, result_sequence
              FROM orchestration_command_receipts WHERE command_id IN (${policy.createCommandId}, ${policy.birthCommandId})`;
            const accepted = (commandId: CommandId, commandType: string) =>
              bindings.some(
                (r) =>
                  r.command_id === commandId &&
                  r.command_type === commandType &&
                  r.aggregate_kind === "thread" &&
                  r.aggregate_id === policy.threadId &&
                  r.status === "accepted",
              );
            if (proof.type === "ambiguous") correlation = "ambiguous";
            else if (
              policy.releaseCommandId !== input.commandId ||
              policy.threadId !== input.threadId ||
              policy.messageId !== input.messageId ||
              policy.runId !== release.run.id ||
              release.run.threadId !== input.threadId ||
              release.run.userMessageId !== input.messageId ||
              release.run.status !== "starting" ||
              proof.type === "mismatched" ||
              !accepted(
                policy.createCommandId,
                policy.ownsNewThread ? "thread.create" : "thread.metadata.update",
              ) ||
              !accepted(policy.birthCommandId, "message.dispatch")
            )
              correlation = "mismatched";
            else if (proof.type === "valid" && proof.sequence < release.sequence) {
              created = [proof.run];
              releaseBinding = true;
            }
          }
        }
        if (
          (receipt !== undefined &&
            (receipt.aggregate_kind !== "thread" || receipt.aggregate_id !== input.threadId)) ||
          events.some((e) => e.aggregate_kind !== "thread" || e.stream_id !== input.threadId) ||
          created.some((r) => r.threadId !== input.threadId || r.userMessageId !== input.messageId)
        )
          correlation = "mismatched";
        else if (births.length > 1 || runs.length > 1) correlation = "ambiguous";
        else if (
          correlation === "missing" &&
          receipt?.status === "accepted" &&
          (births.length === 1 || releaseBinding) &&
          events.some((e) => e.sequence === receipt.result_sequence)
        ) {
          if (receipt.result_sequence <= snapshotSequence && runs[0] !== undefined) {
            const run = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(OrchestrationV2RunJson),
            )(runs[0].payload_json);
            if (run.id !== created[0]!.id) correlation = "mismatched";
            else {
              const messages = yield* sql<{
                messageId: MessageId;
              }>`SELECT json_extract(payload_json, '$.id') AS messageId
            FROM orchestration_v2_projection_messages WHERE thread_id = ${input.threadId}
              AND json_extract(payload_json, '$.runId') = ${run.id} AND json_extract(payload_json, '$.role') = 'assistant'
            ORDER BY json_extract(payload_json, '$.updatedAt') DESC LIMIT 1`;
              turn = {
                turnId: run.startedAt === null ? null : TurnId.make(run.id),
                state:
                  run.status === "completed"
                    ? "completed"
                    : run.status === "failed"
                      ? "error"
                      : ["interrupted", "cancelled", "rolled_back"].includes(run.status)
                        ? "interrupted"
                        : ["preparing", "queued", "starting"].includes(run.status)
                          ? "pending"
                          : "running",
                requestedAt: DateTime.formatIso(run.requestedAt),
                startedAt: run.startedAt === null ? null : DateTime.formatIso(run.startedAt),
                completedAt: run.completedAt === null ? null : DateTime.formatIso(run.completedAt),
                assistantMessageId: messages[0]?.messageId ?? null,
              };
              correlation = turn.turnId === null ? "pending" : "exact";
            }
          } else correlation = "pending";
        }
        return {
          ...input,
          snapshotSequence,
          commandStatus: receipt?.status ?? "not_found",
          acceptedSequence: receipt?.status === "accepted" ? receipt.result_sequence : null,
          correlation,
          turn,
          target,
        } satisfies OrchestrationCommandObservation;
      }),
    );
  return { getTarget, observe };
});
