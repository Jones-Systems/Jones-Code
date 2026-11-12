import {
  CommandId,
  type OrchestrationV2Run,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2TurnItem,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import { RecordedEventJson } from "../../orchestration-v2/RecordedTypes.ts";

// A bounded, thread-scoped proof, requested only when there are held candidates.
// Truncated/oversized or undecodable history retains holds; no lifetime replay,
// new hold reason, timestamp ordering, or writes to upstream storage are used.
const MAX_EVENTS = 4096;
const MAX_BYTES = 8 * 1024 * 1024;
interface Receipt {
  readonly command_id: string;
  readonly command_type: string;
  readonly result_sequence: number;
}
interface Hold {
  readonly sequence: number;
  readonly ownerId?: RunId;
  readonly ownerAttempt?: OrchestrationV2Run["activeAttemptId"];
  readonly failureSequence?: number | undefined;
}
interface Trace {
  run: OrchestrationV2Run;
  readonly birth: number;
  readonly blocker?: RunId;
  readonly ordinary: boolean;
  executed?: number;
  failure?: number | undefined;
  completed?: { readonly sequence: number; readonly id: string };
  hold?: Hold | undefined;
}
const active = (run: OrchestrationV2Run) =>
  ["preparing", "starting", "running", "waiting"].includes(run.status);
const sameIdentity = (left: OrchestrationV2Run, right: OrchestrationV2Run) =>
  left.id === right.id &&
  left.threadId === right.threadId &&
  left.userMessageId === right.userMessageId &&
  left.activeAttemptId === right.activeAttemptId &&
  left.rootNodeId === right.rootNodeId &&
  left.providerThreadId === right.providerThreadId &&
  left.providerInstanceId === right.providerInstanceId;

export function classifyAutomaticQueueHolds(input: {
  readonly events: ReadonlyArray<OrchestrationV2StoredEvent>;
  readonly receipts: ReadonlyArray<Receipt>;
  readonly candidates: ReadonlyArray<OrchestrationV2Run>;
  readonly recovery:
    | {
        readonly type: "completion";
        readonly stored: OrchestrationV2StoredEvent;
        readonly current: OrchestrationV2Run;
      }
    | { readonly type: "preparation"; readonly current: OrchestrationV2Run };
}): ReadonlySet<RunId> {
  const result = new Set<RunId>();
  if (input.events.length === 0 || input.events[0]?.event.type !== "thread.created") return result;
  const receipts = new Map(input.receipts.map((receipt) => [receipt.command_id, receipt]));
  const traces = new Map<RunId, Trace>();
  const errors = new Map<
    string,
    {
      readonly sequence: number;
      readonly item: Extract<OrchestrationV2TurnItem, { type: "error" }>;
    }
  >();
  const stopped = new Set<RunId>();
  let sequence = -1;
  for (const stored of input.events) {
    if (
      !Number.isSafeInteger(stored.sequence) ||
      stored.sequence <= sequence ||
      stored.event.threadId !== input.events[0]!.event.threadId
    )
      return result;
    sequence = stored.sequence;
    const event = stored.event;
    if (event.type === "turn-item.updated") {
      if (event.payload.type === "error")
        errors.set(event.payload.id, { sequence, item: event.payload });
      if (event.payload.type === "run_interrupt_request" && event.payload.runId !== null)
        stopped.add(event.payload.runId);
      continue;
    }
    if (event.type !== "run.updated" && event.type !== "run.created") continue;
    const run = event.payload;
    if (run.threadId !== event.threadId || (event.runId !== undefined && event.runId !== run.id))
      return result;
    let trace = traces.get(run.id);
    if (trace === undefined) {
      const blockers = [...traces.values()].filter((candidate) => active(candidate.run));
      const receipt = stored.commandId === null ? undefined : receipts.get(stored.commandId);
      trace = {
        run,
        birth: sequence,
        ordinary:
          run.status === "queued" &&
          run.queueHeld !== true &&
          receipt?.command_type === "message.dispatch" &&
          receipt.result_sequence >= sequence &&
          blockers.length === 1 &&
          !stopped.has(blockers[0]!.run.id) &&
          ![...traces.values()].some(
            (candidate) =>
              candidate.run.status === "queued" &&
              candidate.run.queueHeld === true &&
              candidate.blocker === blockers[0]!.run.id,
          ),
        ...(blockers.length === 1 ? { blocker: blockers[0]!.run.id } : {}),
        ...(active(run) ? { executed: sequence } : {}),
        ...(run.queueHeld === true ? { hold: { sequence } } : {}),
      };
      traces.set(run.id, trace);
    } else {
      if (!sameIdentity(trace.run, run)) {
        // A new attempt/target cannot inherit authority from the old interval.
        trace.hold = run.queueHeld === true ? { sequence } : undefined;
      } else if (
        run.status === "queued" &&
        run.queueHeld === true &&
        trace.run.queueHeld !== true
      ) {
        const owner = trace.blocker === undefined ? undefined : traces.get(trace.blocker);
        const rootErrors = [...errors.values()].filter(
          ({ sequence: errorSequence, item }) =>
            owner !== undefined &&
            item.runId === owner.run.id &&
            item.nodeId === owner.run.rootNodeId &&
            item.status === "failed" &&
            owner.failure !== undefined &&
            errorSequence <= owner.failure,
        );
        const rootError = rootErrors.sort((left, right) => left.sequence - right.sequence).at(-1)
          ?.item.failure;
        const latestExecuted = [...traces.values()]
          .filter((candidate) => candidate.executed !== undefined)
          .sort((left, right) => right.executed! - left.executed!)[0];
        const preparation =
          owner?.run.workspacePreparation !== undefined &&
          rootError?.code === "workspace_preparation_failed";
        const automatic =
          trace.ordinary &&
          stored.commandId === null &&
          owner !== undefined &&
          owner.run.status === "failed" &&
          owner.failure !== undefined &&
          owner.run.activeAttemptId !== null &&
          owner === latestExecuted &&
          ![...traces.values()].some((candidate) => active(candidate.run)) &&
          !stopped.has(owner.run.id) &&
          rootError !== undefined &&
          (preparation ||
            (rootError.class !== "validation_error" &&
              owner.run.providerInstanceId === run.providerInstanceId));
        trace.hold = automatic
          ? {
              sequence,
              ownerId: owner!.run.id,
              ownerAttempt: owner!.run.activeAttemptId,
              failureSequence: owner!.failure,
            }
          : { sequence };
      } else if (run.status !== "queued" || run.queueHeld !== true) {
        trace.hold = undefined;
      }
      if (active(run) && !active(trace.run)) trace.executed = sequence;
      if (run.status === "failed" && trace.run.status !== "failed") trace.failure = sequence;
      if (run.status !== "failed") trace.failure = undefined;
      trace.run = run;
    }
    if (run.status === "failed" && trace.failure === undefined) trace.failure = sequence;
    if (run.status === "completed" && trace.completed === undefined)
      trace.completed = { sequence, id: event.id };
  }
  const recovery = input.recovery;
  const recovered = traces.get(recovery.current.id);
  if (recovered === undefined || !sameIdentity(recovered.run, recovery.current)) return result;
  if (
    recovery.type === "completion" &&
    (recovery.stored.event.type !== "run.updated" ||
      recovery.stored.event.payload.status !== "completed" ||
      recovery.stored.event.payload.id !== recovery.current.id ||
      !sameIdentity(recovery.stored.event.payload, recovery.current) ||
      recovered.completed?.sequence !== recovery.stored.sequence ||
      recovered.completed.id !== recovery.stored.event.id ||
      recovered.run.status !== "completed" ||
      recovery.current.status !== "completed")
  )
    return result;
  for (const current of input.candidates) {
    const trace = traces.get(current.id);
    const hold = trace?.hold;
    if (
      trace === undefined ||
      hold?.ownerId === undefined ||
      hold.failureSequence === undefined ||
      !sameIdentity(trace.run, current) ||
      trace.run.status !== "queued" ||
      trace.run.queueHeld !== true ||
      current.status !== "queued" ||
      current.queueHeld !== true
    )
      continue;
    // Accepted Stops reach the existing queued cohort even if its bit was already
    // true. Replayed old receipts stay at their original sequence. A zero-sequence
    // receipt cannot establish ordering and therefore grants no release authority.
    if (
      input.receipts.some(
        (receipt) =>
          ["thread.stop", "run.interrupt"].includes(receipt.command_type) &&
          (receipt.result_sequence === 0 || receipt.result_sequence >= trace.birth),
      )
    )
      continue;
    const owner = traces.get(hold.ownerId);
    if (
      owner === undefined ||
      owner.run.activeAttemptId !== hold.ownerAttempt ||
      owner.failure !== hold.failureSequence ||
      stopped.has(hold.ownerId)
    )
      continue;
    if (recovery.type === "preparation") {
      if (
        recovery.current.id !== hold.ownerId ||
        recovery.current.status !== "failed" ||
        recovery.current.workspacePreparation === undefined ||
        recovery.current.activeAttemptId !== hold.ownerAttempt
      )
        continue;
    } else if (
      recovered.executed === undefined ||
      recovered.executed <= hold.sequence ||
      recovery.stored.sequence <= hold.sequence
    )
      continue;
    result.add(current.id);
  }
  return result;
}

const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeEvent = Schema.decodeUnknownEffect(RecordedEventJson);
const decodeCommandId = Schema.decodeEffect(CommandId);

export const readAutomaticQueueHolds = Effect.fn("QueueHoldProvenance.read")(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly threadId: ThreadId;
    readonly candidates: ReadonlyArray<OrchestrationV2Run>;
    readonly recovery: Parameters<typeof classifyAutomaticQueueHolds>[0]["recovery"];
  },
) {
  if (input.candidates.length === 0) return new Set<RunId>();
  const rows = yield* sql<{
    sequence: number;
    event_id: string;
    command_id: string | null;
    event_type: string;
    occurred_at: string;
    payload_json: string;
    metadata_json: string;
  }>`SELECT sequence, event_id, command_id, event_type, occurred_at, payload_json, metadata_json
     FROM orchestration_events
     WHERE aggregate_kind = 'thread' AND stream_id = ${input.threadId} AND application_event_version = 2
       AND event_type IN ('thread.created', 'run.created', 'run.updated', 'turn-item.updated')
     ORDER BY sequence DESC LIMIT ${MAX_EVENTS + 1}`;
  if (
    rows.length > MAX_EVENTS ||
    rows.reduce((bytes, row) => bytes + row.payload_json.length + row.metadata_json.length, 0) >
      MAX_BYTES
  )
    return new Set<RunId>();
  const events: OrchestrationV2StoredEvent[] = [];
  for (const row of [...rows].reverse()) {
    const metadata = yield* decodeJson(row.metadata_json);
    const event = yield* decodeEvent({
      id: row.event_id,
      type: row.event_type,
      threadId: input.threadId,
      occurredAt: row.occurred_at,
      payload: yield* decodeJson(row.payload_json),
      ...(typeof metadata === "object" && metadata !== null ? metadata : {}),
    });
    if (
      event.type === "thread.created" ||
      event.type === "run.created" ||
      event.type === "run.updated" ||
      event.type === "turn-item.updated"
    )
      events.push({
        sequence: row.sequence,
        commandId: row.command_id === null ? null : yield* decodeCommandId(row.command_id),
        event,
      });
  }
  const receipts = yield* sql<Receipt>`SELECT command_id, command_type, result_sequence
    FROM orchestration_command_receipts WHERE aggregate_kind = 'thread' AND aggregate_id = ${input.threadId}
      AND status = 'accepted' AND command_type IN ('message.dispatch', 'thread.stop', 'run.interrupt')
    ORDER BY result_sequence DESC LIMIT ${MAX_EVENTS + 1}`;
  if (receipts.length > MAX_EVENTS) return new Set<RunId>();
  return classifyAutomaticQueueHolds({
    events,
    receipts,
    candidates: input.candidates,
    recovery: input.recovery,
  });
});
