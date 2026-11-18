import { RunId, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { type RecordedAppThread, type RecordedRun } from "../../orchestration-v2/RecordedTypes.ts";
import * as Storage from "./Storage.ts";
import { OrchestratorProjectionError } from "../../orchestration-v2/Orchestrator.ts";

const decodeRunId = Schema.decodeEffect(RunId);
// Suspend the imported error schema to avoid the Core/reader initialization cycle.
const isProjectionError = Schema.is(Schema.suspend(() => OrchestratorProjectionError));

export interface LegacyBootstrapRecords {
  readonly thread: RecordedAppThread;
  readonly runs: ReadonlyArray<RecordedRun>;
}

// This explicit private reader composes matching sidecars in one read-only snapshot.
export const make = Effect.gen(function* () {
  const database = yield* Effect.serviceOption(SqlClient.SqlClient);
  const store = Option.isSome(database) ? Storage.make(database.value) : undefined;
  return Effect.fn("LegacyBootstrap.readRetainedRecords")(
    function* (threadId: ThreadId, filter?: { readonly runIds?: ReadonlyArray<RunId> }) {
      if (Option.isNone(database) || store === undefined)
        return yield* new OrchestratorProjectionError({ threadId });
      const sql = database.value;
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const found = yield* store.readThread(threadId);
            if (found === undefined) return yield* new OrchestratorProjectionError({ threadId });
            const rows =
              filter?.runIds?.length === 0
                ? []
                : yield* sql<{ readonly run_id: string }>`
          SELECT run_id FROM orchestration_v2_projection_runs WHERE thread_id = ${threadId}
          ${filter?.runIds === undefined ? sql`` : sql`AND run_id IN ${sql.in(filter.runIds)}`}
          ORDER BY ordinal ASC
        `;
            const runs: RecordedRun[] = [];
            for (const row of rows) {
              const runId = yield* decodeRunId(row.run_id);
              const retained = yield* store.readRun(runId);
              if (retained === undefined || retained.run.threadId !== threadId)
                return yield* new OrchestratorProjectionError({ threadId });
              runs.push(retained.run);
            }
            return { thread: found.thread, runs } satisfies LegacyBootstrapRecords;
          }),
        )
        .pipe(NodeSqliteClient.readOnly);
    },
    (effect, threadId, _filter?: { readonly runIds?: ReadonlyArray<RunId> }) =>
      effect.pipe(
        Effect.mapError((cause) =>
          isProjectionError(cause) ? cause : new OrchestratorProjectionError({ threadId, cause }),
        ),
      ),
  );
});
