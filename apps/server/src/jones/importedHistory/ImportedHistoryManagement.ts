import {
  EnvironmentAuthenticatedPrincipal,
  ImportedHistoryStart,
  type ImportedHistoryDelivery,
  type ImportedHistoryReview,
  type ImportedHistoryOutcome,
  type ThreadId,
  type CommandId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { EventSinkV2Shape } from "../../orchestration-v2/EventSink.ts";
import * as ThreadCommands from "../../orchestration-v2/ThreadCommandExecutor.ts";
import * as Choice from "./ImportedHistoryChoice.ts";

type CommitInput = Parameters<EventSinkV2Shape["commitCommand"]>[0];
export type StartPlan = Pick<CommitInput, "events" | "effects" | "cancelUnsettledEffects">;
export interface Ports<F> {
  readonly commit: (
    input: CommitInput & { readonly importedHistory: Choice.ImportedHistoryContext },
  ) => Effect.Effect<Effect.Success<ReturnType<EventSinkV2Shape["commitCommand"]>>, F>;
  readonly targetCapabilities: (
    threadId: ThreadId,
    delivery: ImportedHistoryDelivery,
  ) => Effect.Effect<unknown, F>;
  readonly plan: (command: ImportedHistoryStart) => Effect.Effect<StartPlan, F>;
  readonly onError: (threadId: ThreadId, cause: unknown) => F;
}
const decodeStart = Schema.decodeEffect(ImportedHistoryStart);
interface Management<F> {
  readonly reviewImportedHistory: (input: {
    readonly threadId: ThreadId;
    readonly delivery: ImportedHistoryDelivery;
  }) => Effect.Effect<ImportedHistoryReview, F, EnvironmentAuthenticatedPrincipal>;
  readonly startWithImportedHistory: (
    input: ImportedHistoryStart,
  ) => Effect.Effect<ImportedHistoryOutcome, F, EnvironmentAuthenticatedPrincipal>;
  readonly observeImportedHistoryStart: (input: {
    readonly threadId: ThreadId;
    readonly commandId: CommandId;
  }) => Effect.Effect<ImportedHistoryOutcome | null, F, EnvironmentAuthenticatedPrincipal>;
}

// Planning is outside SQL. Choice, ordinary receipt, projections and start reservation
// are accepted together by the existing sink, which rechecks the reviewed basis.
export const make = Effect.fn("ImportedHistoryManagement.make")(function* <F>(
  ports: Ports<F>,
): Effect.fn.Return<
  Management<F>,
  never,
  SqlClient.SqlClient | ThreadCommands.ThreadCommandExecutor
> {
  const sql = yield* SqlClient.SqlClient;
  const threadDispatch = yield* ThreadCommands.ThreadCommandExecutor;
  const review = Effect.fn("ImportedHistoryManagement.review")(function* (input: {
    readonly threadId: ThreadId;
    readonly delivery: ImportedHistoryDelivery;
  }) {
    const principal = yield* EnvironmentAuthenticatedPrincipal;
    const targetCapabilities = yield* ports.targetCapabilities(input.threadId, input.delivery);
    const result = yield* sql.withTransaction(
      Choice.reviewImportedHistory({ ...input, principal, targetCapabilities }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    );
    return result.review;
  });
  const observe = Effect.fn("ImportedHistoryManagement.observe")(function* (input: {
    readonly threadId: ThreadId;
    readonly commandId: CommandId;
  }) {
    const principal = yield* EnvironmentAuthenticatedPrincipal;
    return yield* sql.withTransaction(
      Choice.readImportedHistoryChoice({ ...input, principal }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    );
  });
  const start = Effect.fn("ImportedHistoryManagement.start")(function* (
    candidate: ImportedHistoryStart,
  ) {
    const principal = yield* EnvironmentAuthenticatedPrincipal;
    const command = yield* decodeStart(candidate, { onExcessProperty: "error" });
    // Validate delivery identity before the planner can consume it.
    yield* Choice.commandIdentity({ command, principal, targetCapabilities: null });
    return yield* threadDispatch.withLock(
      command.threadId,
      Effect.gen(function* () {
        const prior = yield* observe(command);
        // A durable replay does not depend on a currently available provider adapter.
        const targetCapabilities =
          prior === null
            ? yield* ports.targetCapabilities(command.threadId, command.delivery)
            : null;
        let plan: StartPlan = { events: [], effects: [] };
        if (prior === null) {
          const reviewed = yield* sql.withTransaction(
            Choice.reviewImportedHistory({
              threadId: command.threadId,
              delivery: command.delivery,
              principal,
              targetCapabilities,
            }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          );
          if (
            reviewed.review.status === "available" &&
            reviewed.review.reviewedBasis === command.reviewedBasis
          ) {
            plan = yield* ports.plan(command);
            if (plan.events.length === 0 || plan.effects.length === 0)
              return yield* new Choice.ImportedHistoryChoiceError({
                code: "reviewed_start_not_planned",
              });
          }
        }
        // Even a replay enters the real receipt/choice owner: full command and actor
        // identity must match, and an ordinary receipt alone is insufficient.
        yield* ports.commit({
          commandId: command.commandId,
          threadId: command.threadId,
          commandType: command.type,
          acceptedAt: yield* DateTime.now,
          ...plan,
          importedHistory: { command, principal, targetCapabilities },
        });
        const outcome = yield* observe(command);
        if (outcome === null)
          return yield* new Choice.ImportedHistoryChoiceError({ code: "unknown_receipt" });
        return outcome;
      }),
    );
  });
  return {
    reviewImportedHistory: Effect.fn("ImportedHistoryManagement.reviewResult")(
      (input: Parameters<typeof review>[0]) =>
        review(input).pipe(Effect.mapError((cause) => ports.onError(input.threadId, cause))),
    ),
    startWithImportedHistory: Effect.fn("ImportedHistoryManagement.startResult")(
      (input: ImportedHistoryStart) =>
        start(input).pipe(Effect.mapError((cause) => ports.onError(input.threadId, cause))),
    ),
    observeImportedHistoryStart: Effect.fn("ImportedHistoryManagement.observeResult")(
      (input: Parameters<typeof observe>[0]) =>
        observe(input).pipe(Effect.mapError((cause) => ports.onError(input.threadId, cause))),
    ),
  };
});
