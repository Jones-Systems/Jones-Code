// @effect-diagnostics nodeBuiltinImport:off -- Synthetic qualification entry; timings use a monotonic clock.
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { readerPoolOptions } from "./persistenceOptions.ts";
import { backgroundRead, interactiveRead } from "./readClassification.ts";
import type { SqliteReaderObservation } from "@t3tools/shared/jones/sqliteReaderContracts";

export const percentiles = (values: readonly number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  return { count: sorted.length, p95: sorted[Math.max(0, Math.ceil(sorted.length * .95) - 1)] ?? null,
    p99: sorted[Math.max(0, Math.ceil(sorted.length * .99) - 1)] ?? null, max: sorted.at(-1) ?? null };
};

/** Real reader scheduler and writer run while caller drives native work. */
export async function combinedReaderWriterWindow(filename: string, count: 2 | 3, acquisitions: number,
  compute: () => Promise<void>) {
  const observations: SqliteReaderObservation[] = [];
  const writerMs: number[] = [], controlMs: number[] = [];
  const database = NodeSqliteClient.layer({ filename, checkpointWorker: true, readerFailureMode: "fail",
    readerPool: readerPoolOptions(count, (event) => { if (observations.length < 100_000) observations.push(event); }) });
  await Effect.runPromise(Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`CREATE TABLE IF NOT EXISTS synthetic_load(id INTEGER PRIMARY KEY, value TEXT)`;
    yield* Effect.all([
      Effect.promise(compute),
      Effect.forEach(Array.from({ length: acquisitions }), (_, index) =>
        sql<{ value: number }>`SELECT COUNT(*) AS value FROM synthetic_load`.pipe(
          NodeSqliteClient.readOnly, index % 2 === 0 ? backgroundRead : interactiveRead,
          Effect.andThen(Effect.yieldNow)), { concurrency: count * 2, discard: true }),
      Effect.forEach(Array.from({ length: acquisitions }), (_, index) => Effect.gen(function* () {
        const start = performance.now();
        yield* sql.withTransaction(sql`INSERT INTO synthetic_load(value) VALUES (${String(index)})`);
        writerMs.push(performance.now() - start);
        yield* Effect.yieldNow;
      }), { discard: true }),
      Effect.forEach(Array.from({ length: acquisitions }), () => Effect.gen(function* () {
        const start = performance.now();
        yield* sql`SELECT 1`.pipe(NodeSqliteClient.readOnly, interactiveRead);
        controlMs.push(performance.now() - start);
        yield* Effect.yieldNow;
      }), { discard: true }),
    ], { concurrency: "unbounded", discard: true });
  }).pipe(Effect.provide(database), Effect.provide(NodeServices.layer)));
  return { readerQueue: percentiles(observations.filter((event) => event.phase === "queue").map((event) => event.durationMs)),
    checkpointAdmission: percentiles(observations.filter((event) => event.phase === "checkpoint").map((event) => event.durationMs)),
    readerExecute: percentiles(observations.filter((event) => event.phase === "execute").map((event) => event.durationMs)),
    readerReplyEstimate: percentiles(observations.filter((event) => event.phase === "reply").map((event) => event.durationMs)),
    lease: percentiles(observations.filter((event) => event.phase === "lease").map((event) => event.durationMs)),
    writer: percentiles(writerMs), control: percentiles(controlMs), observationLimitReached: observations.length === 100_000 };
}
