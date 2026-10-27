import {
  type CommandId,
  type SettlementRestoreInspectRequest,
  type SettlementRestoreInspectResult,
  type SettlementRestoreRejectionCode,
  type SettlementRestoreResult,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { makeJonesTablePresence } from "../persistence/JonesTablePresence.ts";
import { inspectSettlementRestore, readSettlementRestoreRejection } from "./plan.ts";
import { makeSettlementRestoreStore } from "./store.ts";

export class SettlementRestoreReadError extends Schema.TaggedError<SettlementRestoreReadError>()(
  "SettlementRestoreReadError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Failed to read settlement restore records.";
  }
}

export class SettlementRestoreDispatchError extends Schema.TaggedError<SettlementRestoreDispatchError>()(
  "SettlementRestoreDispatchError",
  { commandId: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return "The settlement restore could not be completed.";
  }
}

/** Receipts keep a rejected command's ID; only a new restore command can try again. */
export class SettlementRestorePreviouslyRejectedError extends Schema.TaggedError<SettlementRestorePreviouslyRejectedError>()(
  "SettlementRestorePreviouslyRejectedError",
  { commandId: Schema.String },
) {
  override get message(): string {
    return "This restore command was already rejected. Inspect the thread and retry with a new request ID.";
  }
}

export class SettlementRestore extends Context.Service<
  SettlementRestore,
  {
    readonly inspect: (
      input: SettlementRestoreInspectRequest,
    ) => Effect.Effect<SettlementRestoreInspectResult, SettlementRestoreReadError>;
    readonly restore: (input: {
      readonly commandId: CommandId;
      readonly threadId: ThreadId;
      readonly settleCommandId: CommandId;
    }) => Effect.Effect<
      SettlementRestoreResult,
      | SettlementRestoreReadError
      | SettlementRestoreDispatchError
      | SettlementRestorePreviouslyRejectedError
    >;
  }
>()("t3/jones/settlementRestore/SettlementRestore") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = makeSettlementRestoreStore(sql, makeJonesTablePresence(sql));
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const environment = yield* ServerEnvironment.ServerEnvironment;

  const readThread = Effect.fn("SettlementRestore.readThread")(function* (threadId: ThreadId) {
    const shell = yield* projections.getThreadShell(threadId);
    return shell === null || shell.deletedAt !== null
      ? null
      : yield* projections.getThread(threadId);
  });

  const inspect: SettlementRestore["Service"]["inspect"] = Effect.fn("SettlementRestore.inspect")(
    function* (input) {
      const environmentId = yield* environment.getEnvironmentId;
      const now = yield* DateTime.now;
      const items = yield* Effect.forEach(input.items, (query) =>
        Effect.gen(function* () {
          const stored = yield* store.read(query.threadId);
          const thread = stored === null ? null : yield* readThread(query.threadId);
          return {
            threadId: query.threadId,
            ...(query.settleCommandId === undefined
              ? {}
              : { requestedSettleCommandId: query.settleCommandId }),
            ...inspectSettlementRestore({
              stored,
              requestedSettleCommandId: query.settleCommandId,
              thread,
              now,
            }),
            record: stored?.record ?? null,
          };
        }),
      );
      return { environmentId, items };
    },
    Effect.mapError((cause) => new SettlementRestoreReadError({ cause })),
  );

  const restore: SettlementRestore["Service"]["restore"] = Effect.fn("SettlementRestore.restore")(
    function* (input) {
      const environmentId = yield* environment.getEnvironmentId;
      const base = {
        environmentId,
        threadId: input.threadId,
        settleCommandId: input.settleCommandId,
        restoreCommandId: input.commandId,
      };
      const rejected = (code: SettlementRestoreRejectionCode): SettlementRestoreResult => ({
        ...base,
        outcome: code,
        ...(code === "stale_settle_command" ? { historicalOutcome: "unavailable" as const } : {}),
      });
      const dispatched = yield* orchestrator
        .dispatch({
          type: "thread.settlement.restore",
          commandId: input.commandId,
          threadId: input.threadId,
          settleCommandId: input.settleCommandId,
        })
        .pipe(
          Effect.map((result) => ({ type: "accepted" as const, result })),
          Effect.catch(
            (
              error,
            ): Effect.Effect<
              { readonly type: "rejected"; readonly code: SettlementRestoreRejectionCode },
              SettlementRestoreDispatchError | SettlementRestorePreviouslyRejectedError
            > => {
              if (error._tag === "OrchestratorCommandIdConflictError")
                return Effect.succeed({ type: "rejected", code: "command_id_reused" });
              if (error._tag === "OrchestratorCommandPreviouslyRejectedError")
                return Effect.fail(
                  new SettlementRestorePreviouslyRejectedError({ commandId: input.commandId }),
                );
              const code =
                error._tag === "OrchestratorDispatchError"
                  ? readSettlementRestoreRejection(error.cause)
                  : null;
              return code === null
                ? Effect.fail(
                    new SettlementRestoreDispatchError({
                      commandId: input.commandId,
                      cause: error,
                    }),
                  )
                : Effect.succeed({ type: "rejected", code });
            },
          ),
        );
      if (dispatched.type === "rejected") return rejected(dispatched.code);
      const { result } = dispatched;
      const event = result.storedEvents.find(
        (stored) =>
          stored.event.type === "thread.unsettled" && stored.event.threadId === input.threadId,
      );
      if (event !== undefined)
        return {
          ...base,
          outcome: "restored" as const,
          restoredAt: DateTime.formatIso(event.event.occurredAt),
          sequence: result.sequence,
        };
      // A retry after receipt compaction commits nothing; the record keeps the original result.
      const stored = yield* store
        .read(input.threadId)
        .pipe(Effect.mapError((cause) => new SettlementRestoreReadError({ cause })));
      const marker = stored?.record.restore;
      return marker?.commandId === input.commandId
        ? {
            ...base,
            outcome: "restored" as const,
            restoredAt: marker.restoredAt,
            sequence: marker.sequence,
          }
        : { ...base, outcome: "restored" as const, sequence: result.sequence };
    },
  );

  return SettlementRestore.of({ inspect, restore });
});

export const layer = Layer.effect(SettlementRestore, make);
