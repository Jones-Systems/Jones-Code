import {
  OrchestratorMcpFailure, ThreadId, type ThreadRequestObservation,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { makeDependencyStore } from "./DependencyStore.ts";
import * as Changes from "./DependencyChanges.ts";
import { terminalRun } from "./DependencyDuties.ts";
import { readAdmissionObservation } from "./AdmissionObservation.ts";
const encodeToken = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export class RequestWatch extends Context.Service<RequestWatch, {
  readonly observe: (input: {
    readonly originEnvironmentId: string;
    readonly operationId: string;
    readonly targetThreadId?: ThreadId;
    readonly afterToken?: string;
    readonly fence?: boolean;
    readonly timeoutMs: number;
  }) => Effect.Effect<ThreadRequestObservation, OrchestratorMcpFailure>;
}>()("jones/threadResultDependencies/RequestWatch") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = makeDependencyStore(sql, makeJonesTablePresence(sql));
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const changes = yield* Changes.DependencyChanges;
  const observe: RequestWatch["Service"]["observe"] = Effect.fn("RequestWatch.observe")((input) => Effect.scoped(Effect.gen(function* () {
    if (!(yield* store.available)) return yield* new OrchestratorMcpFailure({ code: "capability_denied", message: "Correlated result observation is unavailable." });
    // The Jones subscription precedes all reads; event replay starts at a captured cursor.
    const subscription = yield* changes.subscribe;
    const triggers = yield* Queue.unbounded<void>();
    yield* Stream.fromSubscription(subscription).pipe(Stream.runForEach(() => Queue.offer(triggers, undefined)), Effect.forkScoped);
    const subscribed = new Set<ThreadId>();
    const continuations = new Set<string>();
    const read = Effect.gen(function* () {
      const { row, admission } = yield* sql.withTransaction(Effect.gen(function* () {
        return {
          row: yield* store.requestByOperation(input.originEnvironmentId, input.operationId),
          admission: yield* readAdmissionObservation(sql, input),
        };
      }));
      const target = row?.targetThreadId ?? input.targetThreadId;
      if (target !== undefined && !subscribed.has(target)) {
        const cursor = yield* orchestrator.getThreadEventSequence(target);
        subscribed.add(target);
        yield* orchestrator.streamStoredEventsFrom({ threadId: target, afterSequence: cursor }).pipe(Stream.runForEach(() => Queue.offer(triggers, undefined)), Effect.forkScoped);
        // The first request read may have preceded subscription; repeat it below.
        yield* Queue.offer(triggers, undefined);
      }
      let observation: ThreadRequestObservation["observation"] = admission?.status === "dispatching" ? "admission_dispatching" : "admission_held";
      let state: ThreadRequestObservation["state"] = "pending";
      let outcome: ThreadRequestObservation["outcome"] = null;
      let result: string | null = null;
      let observedRun: unknown = null;
      if (row) {
        if (row.state !== "pending") {
          state = "answered";
          // The durable answer survives target pruning; no run read is needed to prove it.
          observation = "delivery_unknown";
          outcome = row.state;
          result = row.result;
        }
        if (row.state === "pending") {
        const records = yield* orchestrator.getThreadRecords(row.targetThreadId, ["messages", "runs", "runtimeRequests"], { messageIds: [row.messageId] });
        const message = records.messages.find((candidate) => candidate.id === row.messageId);
        let run = records.runs.find((candidate) => candidate.id === message?.runId);
        // Follow restarts for observation only. An unrelated or completed run never answers.
        while (run !== undefined) {
          const next = records.runs.find((candidate) => candidate.restartContinuationOfRunId === run!.id);
          if (!next) break;
          run = next;
        }
        const continuationPending = run && terminalRun(run.status) ? yield* orchestrator.restartContinuationPending(run) : false;
        observedRun = run ? [run.id, run.status, continuationPending] : null;
        observation = records.runtimeRequests.some((request) => request.status === "pending") ? "receiver_needs_owner"
          : run?.status === "queued" ? (run.queueHeld === true ? "queued_held" : "queued")
          : continuationPending || run && !terminalRun(run.status) ? "receiver_working"
          : (run?.status === "cancelled" || run?.status === "interrupted") ? "receiver_stopped" : "receiver_idle_unanswered";
        if (run && terminalRun(run.status) && !continuations.has(run.id) && continuationPending) {
          continuations.add(run.id);
          yield* orchestrator.awaitRestartContinuation(run).pipe(Effect.andThen(Queue.offer(triggers, undefined)), Effect.forkScoped);
        }
        }
      } else if (admission?.status === "denied" || admission?.status === "cancelled") {
        state = "answered";
        outcome = admission.status === "denied" ? "refused" : "withdrawn";
        result = admission.status === "denied" ? "The receiving owner declined admission." : "The receiving request was withdrawn.";
      } else if (admission?.outcome !== undefined && admission.outcome !== null) {
        state = "answered";
        outcome = admission.outcome;
        result = admission.result;
        observation = "delivery_unknown";
      } else if (!admission && (yield* store.fenced(input.originEnvironmentId, input.operationId))) {
        state = "not_sent";
        outcome = "not_sent";
        result = "The operation was fenced before a request or admission was accepted.";
      } else if (!admission) observation = "delivery_unknown";
      const value = { state, request: row ?? null, admissionId: admission?.id ?? null, observation, outcome, result };
      return { ...value, token: encodeToken([row?.requestId, row?.state, row?.requesterReleased, admission, observedRun, observation]) } satisfies ThreadRequestObservation;
    });
    if (input.fence) {
      yield* sql.withTransaction(Effect.gen(function* () {
        const request = yield* store.requestByOperation(input.originEnvironmentId, input.operationId);
        const admissions = yield* sql`SELECT 1 FROM jones_remote_admissions WHERE origin_environment_id = ${input.originEnvironmentId} AND operation_id = ${input.operationId}`;
        if (!request && admissions.length === 0) yield* store.fence(input.originEnvironmentId, input.operationId);
      }));
    }
    let current = yield* read;
    if (input.afterToken !== current.token || current.state !== "pending" || input.timeoutMs <= 0) return current;
    const next = Effect.gen(function* () {
      while (true) {
        yield* Queue.take(triggers);
        current = yield* read;
        if (current.token !== input.afterToken || current.state !== "pending") return current;
      }
    });
    return yield* next.pipe(Effect.timeoutOption(Duration.millis(input.timeoutMs)), Effect.map((value) => Option.getOrElse(value, () => current)));
  })), Effect.mapError((error) => Schema.is(OrchestratorMcpFailure)(error) ? error : new OrchestratorMcpFailure({ code: "orchestration_error", message: "Unable to observe the result request." })));
  return RequestWatch.of({ observe });
});
export const layer = Layer.effect(RequestWatch, make);
