import * as P from "@t3tools/contracts/jones/workstreamsPersonal";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as PersonalWorkstreams from "./service.ts";

export type AssignmentExpectation =
  | { readonly kind: "unassigned" }
  | { readonly kind: "revision"; readonly revision: number };
export interface PersonalWorkstreamList {
  readonly environmentId: EnvironmentId;
  readonly sequence: number;
  readonly observedAt: string;
  readonly workstreams: readonly P.PersonalWorkstream[];
}
export interface ExpectingResult {
  readonly outcome:
    | "applied"
    | "already_satisfied"
    | "precondition_failed"
    | "workstream_not_found"
    | "thread_not_found";
  readonly environmentId: EnvironmentId;
  readonly assignment: P.PersonalWorkstreamAssignment | null;
  readonly workstream: P.PersonalWorkstream | null;
  readonly sequence: number;
  readonly observedAt: string;
}
// One write plus one retry, per the race rule of the bearer assign contract.
const EXPECTING_WRITE_ATTEMPTS = 2;
const compareText = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const byOrder = (a: P.PersonalWorkstream, b: P.PersonalWorkstream) =>
  compareText(a.orderKey, b.orderKey) || compareText(a.id, b.id);

export class AgentWorkstreams extends Context.Service<
  AgentWorkstreams,
  {
    readonly create: (input: {
      id: string;
      name: string;
      lifecycle?: P.PersonalWorkstreamLifecycle;
      color?: string | null;
    }) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly update: (
      input: P.PersonalWorkstreamPatch & { id: string; delete?: boolean },
    ) => Effect.Effect<P.PutResult, P.WorkstreamsPersonalError>;
    readonly set: (input: {
      threadId: ThreadId;
      workstreamId: string | null;
      expectedRevision: number;
      workstream?: P.PersonalWorkstream;
    }) => Effect.Effect<P.AssignResult, P.WorkstreamsPersonalError>;
    /** Live definitions ordered by order key, then id. */
    readonly list: () => Effect.Effect<PersonalWorkstreamList, P.WorkstreamsPersonalError>;
    /**
     * Places a thread under a caller precondition and reports every domain result as an
     * outcome. Only storage and validation failures use the error channel.
     */
    readonly setExpecting: (input: {
      threadId: ThreadId;
      workstreamId: string | null;
      expected: AssignmentExpectation;
    }) => Effect.Effect<ExpectingResult, P.WorkstreamsPersonalError, SqlClient.SqlClient>;
  }
>()("t3/jones/workstreams/personal/agentService/AgentWorkstreams") {}
const make = Effect.gen(function* () {
  const service = yield* PersonalWorkstreams.PersonalWorkstreams;
  const find = Effect.fn("AgentWorkstreams.find")(function* (id: string) {
    const w = (yield* service.readDefinitions([id]))[0];
    if (!w) return yield* new P.WorkstreamsPersonalError({ code: "workstream_not_found" });
    if (w.deletedAt !== null)
      return yield* new P.WorkstreamsPersonalError({ code: "workstream_deleted" });
    return w;
  });
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const readThread = (threadId: ThreadId) =>
    service.readThread(threadId).pipe(
      Effect.map((read) => ({
        assignment: read.assignment,
        // Deletion clears memberships, so a deleted definition here is a concurrent read.
        workstream: read.workstream?.deletedAt === null ? read.workstream : null,
      })),
      Effect.catchTags({
        WorkstreamsPersonalError: (error) =>
          error.code === "thread_not_found" ? Effect.succeed(null) : Effect.fail(error),
      }),
    );
  const setExpecting = Effect.fn("AgentWorkstreams.setExpecting")(function* (input: {
    threadId: ThreadId;
    workstreamId: string | null;
    expected: AssignmentExpectation;
  }) {
    const sql = yield* SqlClient.SqlClient;
    yield* Effect.annotateCurrentSpan({
      "workstreams.expected": input.expected.kind,
      "workstreams.unassign": input.workstreamId === null,
    });
    const evaluate: (
      writesLeft: number,
    ) => Effect.Effect<ExpectingResult, P.WorkstreamsPersonalError> = Effect.fn(
      "AgentWorkstreams.evaluateExpecting",
    )(function* (writesLeft: number) {
      // One transaction, so the reported sequence matches the assignment and definitions read.
      const [thread, snapshot] = yield* sql
        .withTransaction(Effect.all([readThread(input.threadId), service.snapshot()]))
        .pipe(
          Effect.catchTags({
            SqlError: () =>
              Effect.fail(new P.WorkstreamsPersonalError({ code: "storage_unavailable" })),
          }),
        );
      const observe = (
        outcome: ExpectingResult["outcome"],
        read: typeof thread,
        sequence = snapshot.sequence,
      ) =>
        now.pipe(
          Effect.map((observedAt): ExpectingResult => ({
            outcome,
            environmentId: snapshot.environmentId,
            assignment: read?.assignment ?? null,
            workstream: read?.workstream ?? null,
            sequence,
            observedAt,
          })),
        );
      if (thread === null) return yield* observe("thread_not_found", null);
      const target =
        input.workstreamId === null
          ? null
          : (snapshot.workstreams.find((w) => w.id === input.workstreamId) ?? null);
      if (input.workstreamId !== null && (target === null || target.deletedAt !== null))
        return yield* observe("workstream_not_found", thread);
      const current = thread.assignment;
      if (current.workstreamId === input.workstreamId)
        return yield* observe("already_satisfied", thread);
      const holds =
        input.expected.kind === "unassigned"
          ? current.workstreamId === null
          : current.revision === input.expected.revision;
      if (!holds || writesLeft === 0) return yield* observe("precondition_failed", thread);
      return yield* service
        .assign({
          threadId: input.threadId,
          workstream: target,
          expectedRevision: current.revision,
        })
        .pipe(
          Effect.flatMap((written) =>
            observe(
              written.outcome,
              {
                assignment: written.assignment,
                workstream: written.workstream?.deletedAt === null ? written.workstream : null,
              },
              written.sequence,
            ),
          ),
          Effect.catchTags({
            WorkstreamsPersonalError: (error) => {
              switch (error.code) {
                // A lost compare-and-swap or a concurrent delete re-evaluates from fresh reads.
                case "precondition_failed":
                case "thread_not_found":
                case "workstream_not_found":
                case "workstream_deleted":
                  return evaluate(error.code === "precondition_failed" ? writesLeft - 1 : 0);
                default:
                  return Effect.fail(error);
              }
            },
          }),
        );
    });
    return yield* evaluate(EXPECTING_WRITE_ATTEMPTS);
  });
  return AgentWorkstreams.of({
    list: Effect.fn("AgentWorkstreams.list")(function* () {
      const snapshot = yield* service.snapshot();
      return {
        environmentId: snapshot.environmentId,
        sequence: snapshot.sequence,
        observedAt: yield* now,
        workstreams: snapshot.workstreams.filter((w) => w.deletedAt === null).toSorted(byOrder),
      };
    }),
    setExpecting,
    create: service.create,
    update: Effect.fn("AgentWorkstreams.update")(function* (input) {
      const old = yield* find(input.id);
      const s = yield* service.snapshot();
      const now = yield* DateTime.now.pipe(Effect.map(DateTime.formatIso));
      const candidate = yield* Effect.try({
        try: () =>
          input.delete === true
            ? P.deletePersonalWorkstream(old, now, `env:${s.environmentId}`)
            : P.editPersonalWorkstream(old, input, now, `env:${s.environmentId}`),
        catch: () => new P.WorkstreamsPersonalError({ code: "invalid_request" }),
      });
      return yield* service.put(candidate);
    }),
    set: Effect.fn("AgentWorkstreams.set")(function* (input) {
      const w =
        input.workstreamId === null
          ? null
          : (input.workstream ?? (yield* find(input.workstreamId)));
      if (w !== null && w.id !== input.workstreamId)
        return yield* new P.WorkstreamsPersonalError({ code: "invalid_request" });
      return yield* service.assign({
        threadId: input.threadId,
        workstream: w,
        expectedRevision: input.expectedRevision,
      });
    }),
  });
});
export const layer = Layer.effect(AgentWorkstreams, make);
