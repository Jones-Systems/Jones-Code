import {
  OrchestrationV2ConversationMessage,
  OrchestrationV2ExecutionNode,
  OrchestrationV2Run,
  OrchestrationV2RunAttempt,
  ProviderRuntimeBinding,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { runtimeBinding } from "@t3tools/provider-core/server/ProviderAdapter";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { isActiveRun } from "../../orchestration-v2/CommandPolicy.ts";
import type { ProjectionStoreV2Shape } from "../../orchestration-v2/ProjectionStore.ts";
import type { makeRuntimeStopMethods } from "../runtime/RuntimeStopSqlite.ts";
import { validateQueuedSteerBatch } from "./QueuedSteerBatch.ts";

type StopFences = Pick<
  ReturnType<typeof makeRuntimeStopMethods>,
  "readRunFences" | "hasStopIntent"
>;
type FenceRows = Effect.Success<ReturnType<StopFences["readRunFences"]>>;
type QueueRecords = Pick<OrchestrationV2ThreadProjection, "thread" | "runs"> &
  Partial<
    Pick<
      OrchestrationV2ThreadProjection,
      "attempts" | "nodes" | "messages" | "providerTurns" | "providerThreads"
    >
  >;
export type Command = Extract<
  OrchestrationV2ServerCommand,
  {
    readonly type:
      | "queue.resume"
      | "queued-run.start"
      | "queued-run.reorder"
      | "queued-run.cancel"
      | "queued-run.edit"
      | "queued-message.promote-to-steer"
      | "queued-message.promote-batch-to-steer";
  }
>;
interface Snapshot {
  readonly command: Command;
  readonly projection: QueueRecords;
  readonly fences: FenceRows;
}

export interface Planning {
  snapshot?: Snapshot;
  automaticBatchDelivery?: boolean;
}

export class QueuedSteerTargetStopping extends Error {}

export const read = <A extends QueueRecords, E>(
  sql: SqlClient.SqlClient,
  stops: StopFences,
  command: Command,
  records: Effect.Effect<A, E>,
  planning: Planning,
) =>
  sql
    .withTransaction(
      Effect.gen(function* () {
        const projection = yield* records;
        const fences = yield* stops.readRunFences(
          projection.thread.id,
          projection.runs.filter((run) => run.status === "queued").map((run) => run.id),
        );
        return { command, projection, fences };
      }),
    )
    .pipe(
      NodeSqliteClient.readOnly,
      Effect.map((snapshot) => {
        planning.snapshot = snapshot;
        return snapshot.projection;
      }),
    );

function targetRuntime(projection: QueueRecords, target: OrchestrationV2Run | undefined) {
  const provider = projection.providerThreads?.find((row) => row.id === target?.providerThreadId);
  const generation = provider?.runtimeIdentity?.runtimeGeneration;
  return provider === undefined || generation === undefined
    ? undefined
    : runtimeBinding(provider, generation);
}

export function forCommand(
  { command, projection, fences }: Snapshot,
  events: ReadonlyArray<OrchestrationV2DomainEvent>,
  automaticBatchDelivery = false,
) {
  const requireIdle = command.type === "queued-run.start";
  const wholeQueue =
    requireIdle || command.type === "queue.resume" || command.type === "queued-run.reorder";
  const writtenRuns = new Set(
    events.flatMap((event) =>
      event.threadId === projection.thread.id && event.type === "run.updated"
        ? [event.payload.id]
        : [],
    ),
  );
  if (command.type === "queued-run.edit") writtenRuns.add(command.runId);
  const queued = projection.runs.filter(
    (run) => run.status === "queued" && (wholeQueue || writtenRuns.has(run.id)),
  );
  const runIds = new Set(queued.map((run) => run.id));
  const messageIds = new Set(queued.map((run) => run.userMessageId));
  const attemptIds = new Set(queued.map((run) => run.activeAttemptId));
  const nodeIds = new Set(queued.map((run) => run.rootNodeId));
  const targetId =
    command.type === "queued-message.promote-to-steer" ||
    command.type === "queued-message.promote-batch-to-steer"
      ? command.targetRunId
      : undefined;
  const target = projection.runs.find((run) => run.id === targetId);
  const { id: threadId, archivedAt, deletedAt } = projection.thread;
  return {
    threadId,
    archived: archivedAt !== null,
    deleted: deletedAt !== null,
    wholeQueue,
    requireIdle,
    queued,
    targetId,
    target,
    runtime: targetRuntime(projection, target),
    activeTurns: (projection.providerTurns ?? [])
      .filter((turn) => turn.runAttemptId === target?.activeAttemptId && turn.status === "running")
      .map((turn) => turn.id),
    messages: (projection.messages ?? []).filter((message) => messageIds.has(message.id)),
    attempts: (projection.attempts ?? []).filter((attempt) => attemptIds.has(attempt.id)),
    nodes: (projection.nodes ?? []).filter((node) => nodeIds.has(node.id)),
    fences: fences.filter((fence) => runIds.has(fence.run_id)),
    batch:
      command.type === "queued-message.promote-batch-to-steer"
        ? { command, automaticBatchDelivery }
        : undefined,
  };
}

export type Basis = ReturnType<typeof forCommand>;

const sameRun = Schema.toEquivalence(OrchestrationV2Run);
const sameMessage = Schema.toEquivalence(OrchestrationV2ConversationMessage);
const sameAttempt = Schema.toEquivalence(OrchestrationV2RunAttempt);
const sameNode = Schema.toEquivalence(OrchestrationV2ExecutionNode);
const sameRuntime = Schema.toEquivalence(Schema.UndefinedOr(ProviderRuntimeBinding));
const staleQueue = "Queued input changed before acceptance. Refresh the queue and retry.";

function sameRows<A extends { readonly id: string }>(
  expected: ReadonlyArray<A>,
  current: ReadonlyArray<A>,
  equivalent: (left: A, right: A) => boolean,
): boolean {
  if (expected.length !== current.length) return false;
  const byId = new Map(current.map((row) => [row.id, row]));
  return expected.every((row) => {
    const found = byId.get(row.id);
    return found !== undefined && equivalent(row, found);
  });
}

// The event sink checks this basis inside the transaction that appends events and enqueues effects.
export const rejectionReason = Effect.fnUntraced(function* (
  projectionStore: Pick<ProjectionStoreV2Shape, "getThreadRecords">,
  stops: StopFences,
  threadId: OrchestrationV2ThreadProjection["thread"]["id"],
  basis: Basis,
) {
  if (basis.threadId !== threadId) return staleQueue;
  const runIds = basis.queued.map((run) => run.id);
  const messageIds = new Set(basis.messages.map((message) => message.id));
  // An untouched run can move ahead of the selected batch. Read the current
  // queue plus the active run's input, without loading their thread history.
  const batch =
    basis.batch === undefined
      ? undefined
      : {
          ...basis.batch,
          runs: (yield* projectionStore.getThreadRecords(threadId, ["runs"], {
            runStatuses: ["queued", "running"],
          })).runs,
        };
  const current = yield* projectionStore.getThreadRecords(
    threadId,
    [
      "runs",
      "messages",
      "attempts",
      "nodes",
      "providerTurns",
      "providerThreads",
      ...(batch === undefined ? [] : (["providerSessions", "runtimeRequests"] as const)),
    ],
    {
      ...(basis.wholeQueue
        ? {
            runStatuses: basis.requireIdle
              ? ["queued", "preparing", "starting", "running", "waiting"]
              : ["queued"],
          }
        : { runIds: basis.targetId === undefined ? runIds : [...runIds, basis.targetId] }),
      ...(batch === undefined
        ? { messageIds: [...messageIds] }
        : {
            messageIds: batch.runs.map((run) => run.userMessageId),
            messageRoles: ["user"],
            runtimeRequestStatuses: ["pending"],
          }),
      attemptIds: basis.attempts.map((attempt) => attempt.id),
      nodeIds: basis.nodes.map((node) => node.id),
      providerThreadIds:
        basis.target?.providerThreadId == null ? [] : [basis.target.providerThreadId],
      providerTurnAttemptIds:
        basis.target?.activeAttemptId == null ? [] : [basis.target.activeAttemptId],
    },
  );
  const target = current.runs.find((run) => run.id === basis.targetId);
  const activeTurns = current.providerTurns.filter((turn) => turn.status === "running");
  const runtime = targetRuntime(current, target);
  if (
    (current.thread.archivedAt !== null) !== basis.archived ||
    (current.thread.deletedAt !== null) !== basis.deleted ||
    (basis.requireIdle && current.runs.some(isActiveRun)) ||
    !sameRows(
      basis.queued,
      current.runs.filter((run) => run.status === "queued"),
      sameRun,
    ) ||
    (basis.target === undefined
      ? target !== undefined
      : target === undefined || !sameRun(basis.target, target)) ||
    activeTurns.length !== basis.activeTurns.length ||
    activeTurns.some((turn) => !basis.activeTurns.includes(turn.id)) ||
    !sameRuntime(basis.runtime, runtime) ||
    !sameRows(
      basis.messages,
      current.messages.filter((message) => messageIds.has(message.id)),
      sameMessage,
    ) ||
    !sameRows(basis.attempts, current.attempts, sameAttempt) ||
    !sameRows(basis.nodes, current.nodes, sameNode)
  )
    return staleQueue;

  // A matching intent owns the runtime as soon as Stop is accepted, including
  // when its fences were already present in the planning snapshot.
  if (runtime !== undefined && (yield* stops.hasStopIntent(runtime)))
    return "The target provider runtime is stopping and cannot be steered.";

  const fences = yield* stops.readRunFences(threadId, runIds);
  if (
    fences.length !== basis.fences.length ||
    fences.some(
      ({ run_id, command_id }, index) =>
        run_id !== basis.fences[index]?.run_id || command_id !== basis.fences[index]?.command_id,
    )
  )
    return staleQueue;

  if (batch !== undefined) {
    const eligibility = validateQueuedSteerBatch(
      { ...current, runs: batch.runs },
      batch.command,
      batch.automaticBatchDelivery,
    );
    if ("error" in eligibility) return eligibility.error;
  }
});
