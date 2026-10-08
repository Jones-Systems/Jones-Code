// @effect-diagnostics nodeBuiltinImport:off -- The opt-in benchmark emits raw machine-readable timing records without Effect logger metadata.
import { TurnItemId } from "@t3tools/contracts";
import * as NodeAssert from "node:assert/strict";
import * as NodeConsole from "node:console";
import * as NodePerfHooks from "node:perf_hooks";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as SqlClient from "effect/sql/SqlClient";
import { test, describe } from "vite-plus/test";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import {
  historyExperimentLayer,
  historySourceSha256,
  historyStatements,
  historyThreadId,
  mapHistoryRows,
  seedHistory,
} from "./historyExperiment.ts";

// Opt-in synthetic Node/SQLite evidence; no elapsed-time CI thresholds, disk,
// network, writer contention, cold-cache, or production-capacity claims.
describe("V2 history query characterization", () => {
  for (const count of [1_000, 10_000]) {
    test(`${count} messages: full, window, source OR and disjoint UNION`, async ({ bench }) => {
      await bench("controlled history query experiment", async () => {
        const runtime = ManagedRuntime.make(historyExperimentLayer);
        try {
          await runtime.runPromise(seedHistory(count));
          const store = await runtime.runPromise(ProjectionStore.ProjectionStoreV2);
          const sql = await runtime.runPromise(SqlClient.SqlClient);
          const full = await runtime.runPromise(store.getThreadSnapshot(historyThreadId));
          for (const options of [
            { rowLimit: 25 },
            { rowLimit: 25, userTurnLimit: 10 },
            { rowLimit: 25, anchorItemId: TurnItemId.make("item-000059") },
            { rowLimit: 25, userTurnLimit: 10, anchorItemId: TurnItemId.make("item-000059") },
          ]) {
            const window = await runtime.runPromise(
              store.getThreadSnapshotWindow(historyThreadId, options),
            );
            const cohort = window.projection.visibleTurnItems.flatMap(({ item }) =>
              item.type === "user_message" ? [item.messageId] : [],
            );
            const expected = full.projection.messages.filter(
              (message) => cohort.includes(message.id) || message.runId === "active-run",
            );
            const positions = new Map(cohort.map((id, index) => [id, index]));
            const windowExpected = expected.toSorted(
              (a, b) =>
                (positions.get(a.id) ?? Infinity) - (positions.get(b.id) ?? Infinity) ||
                a.id.localeCompare(b.id),
            );
            NodeAssert.deepEqual(window.projection.messages, windowExpected);
            const statements = historyStatements(cohort);
            const samples = [[], []] as [number[], number[]];
            const decodeSamples = [[], []] as [number[], number[]];
            const projectionSamples = { full: [] as number[], window: [] as number[] };
            const plans = [];
            let baselineRows: ReadonlyArray<{ payload_json: string }> | undefined;
            for (const statement of statements) {
              const rows = await runtime.runPromise(
                sql.unsafe<{ payload_json: string }>(statement.text, statement.values),
              );
              if (baselineRows === undefined) baselineRows = rows;
              else NodeAssert.deepEqual(rows, baselineRows);
              NodeAssert.deepEqual(mapHistoryRows(rows), expected);
              plans.push(
                await runtime.runPromise(
                  sql.unsafe(`EXPLAIN QUERY PLAN ${statement.text}`, statement.values),
                ),
              );
            }
            for (let round = -3; round < 15; round++) {
              for (const index of round % 2 === 0 ? ([0, 1] as const) : ([1, 0] as const)) {
                const statement = statements[index];
                const start = NodePerfHooks.performance.now();
                const rows = await runtime.runPromise(
                  sql.unsafe<{ payload_json: string }>(statement.text, statement.values),
                );
                const sqlMs = NodePerfHooks.performance.now() - start;
                const decodeStart = NodePerfHooks.performance.now();
                const mapped = mapHistoryRows(rows);
                const decodeMs = NodePerfHooks.performance.now() - decodeStart;
                NodeAssert.deepEqual(mapped, expected);
                if (round >= 0) {
                  samples[index].push(sqlMs);
                  decodeSamples[index].push(decodeMs);
                }
              }
              for (const kind of round % 2 === 0
                ? (["full", "window"] as const)
                : (["window", "full"] as const)) {
                const start = NodePerfHooks.performance.now();
                const value = await runtime.runPromise(
                  kind === "full"
                    ? store.getThreadSnapshot(historyThreadId)
                    : store.getThreadSnapshotWindow(historyThreadId, options),
                );
                const ms = NodePerfHooks.performance.now() - start;
                NodeAssert.deepEqual(
                  value.projection.messages,
                  kind === "full" ? full.projection.messages : windowExpected,
                );
                if (round >= 0) projectionSamples[kind].push(ms);
              }
            }
            // @effect-diagnostics-next-line globalConsole:off - The opt-in benchmark emits raw JSON records without logger metadata for machine-readable timing evidence.
            NodeConsole.log(
              JSON.stringify({
                workload: "v2-history",
                sourceSha256: historySourceSha256,
                node: process.version,
                count,
                options,
                returnedRows: expected.length,
                statements,
                plans,
                sqlMs: samples,
                decodeMs: decodeSamples,
                projectionMs: projectionSamples,
                limits:
                  "in-memory migrated schema; active run retains one third of messages; UNION is benchmark-only; outer benchmark includes setup",
              }),
            );
          }
        } finally {
          await runtime.dispose();
        }
      }).run({ time: 0, iterations: 1, warmupTime: 0, warmupIterations: 0 });
    });
  }
});
