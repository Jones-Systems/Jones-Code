// @effect-diagnostics nodeBuiltinImport:off -- Synthetic qualification entry; timings use a monotonic clock.
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { readerPoolOptions } from "./persistenceOptions.ts";
import { backgroundRead, interactiveRead } from "./readClassification.ts";
import type { SqliteReaderObservation } from "@t3tools/shared/jones/sqliteReaderContracts";
import { observeSafely } from "./observations.ts";
import type { DiagnosticCpu, DiagnosticEvent, DiagnosticSink } from "./diagnosticObservation.ts";
import type { ComputeObservation } from "./observations.ts";

export const percentiles = (values: readonly number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    count: sorted.length,
    p95: sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)] ?? null,
    p99: sorted[Math.max(0, Math.ceil(sorted.length * 0.99) - 1)] ?? null,
    max: sorted.at(-1) ?? null,
  };
};

/** Keep absence separate from zero and retain estimate bounds in the raw observations. */
export function summarizeComputeObservations(observations: readonly ComputeObservation[]) {
  const phases = [
    "admission",
    "queue",
    "startup",
    "execute",
    "reply",
    "reply_delivery",
    "parent_continuation",
    "request_round_trip",
    "job",
  ] as const;
  return Object.fromEntries(
    phases.map((phase) => {
      const events = observations.filter((event) => event.phase === phase);
      const values = events.flatMap((event) =>
        "durationMs" in event && event.durationMs !== null ? [event.durationMs] : [],
      );
      return [
        phase,
        {
          ...percentiles(values),
          unavailable: events.length - values.length,
          availability: values.length === 0 ? "unavailable" : "observed",
        },
      ];
    }),
  );
}

/** Real reader scheduler and writer run while caller drives native work. */
export async function combinedReaderWriterWindow(
  filename: string,
  count: 2 | 3,
  acquisitions: number,
  compute: () => Promise<void>,
  computeObservations?: () => readonly ComputeObservation[],
  diagnostic?: DiagnosticSink,
) {
  const emit = (
    event: Extract<DiagnosticEvent, { kind: "combined" }>["event"],
    index?: number,
    cpu?: DiagnosticCpu,
    atMs?: number,
  ) => {
    if (diagnostic)
      observeSafely(diagnostic)({
        kind: "combined",
        event,
        atMs: atMs ?? performance.now(),
        phase: "combined",
        ...(index === undefined ? {} : { index }),
        ...(cpu ? { cpu } : {}),
      });
  };
  const cpuStart = diagnostic
    ? { process: process.cpuUsage(), thread: process.threadCpuUsage() }
    : undefined;
  let diagnosticCpu: DiagnosticCpu | undefined;
  emit("window-begin");
  const observations: SqliteReaderObservation[] = [];
  const writerMs: number[] = [],
    controlMs: number[] = [];
  emit("database-begin");
  const database = NodeSqliteClient.layer({
    filename,
    checkpointWorker: true,
    readerFailureMode: "fail",
    readerPool: readerPoolOptions(count, (event) => {
      if (observations.length < 100_000) observations.push(event);
      if (diagnostic)
        observeSafely(diagnostic)({
          kind: "combined",
          event: "reader-observation",
          atMs: performance.now(),
          phase: "combined",
          observation: { ...event },
        });
    }),
  });
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA journal_mode = WAL`;
        yield* sql`CREATE TABLE IF NOT EXISTS synthetic_load(id INTEGER PRIMARY KEY, value TEXT)`;
        emit("database-ready");
        emit("sql-load-begin");
        yield* Effect.all(
          [
            Effect.promise(compute),
            Effect.forEach(
              Array.from({ length: acquisitions }),
              (_, index) =>
                diagnostic
                  ? Effect.gen(function* () {
                      emit("reader-begin", index);
                      yield* sql<{
                        value: number;
                      }>`SELECT COUNT(*) AS value FROM synthetic_load`.pipe(
                        NodeSqliteClient.readOnly,
                        index % 2 === 0 ? backgroundRead : interactiveRead,
                        Effect.ensuring(Effect.sync(() => emit("reader-end", index))),
                      );
                      yield* Effect.yieldNow;
                    })
                  : sql<{ value: number }>`SELECT COUNT(*) AS value FROM synthetic_load`.pipe(
                      NodeSqliteClient.readOnly,
                      index % 2 === 0 ? backgroundRead : interactiveRead,
                      Effect.andThen(Effect.yieldNow),
                    ),
              { concurrency: count * 2, discard: true },
            ),
            Effect.forEach(
              Array.from({ length: acquisitions }),
              (_, index) =>
                Effect.gen(function* () {
                  const start = performance.now();
                  emit("writer-begin", index, undefined, start);
                  yield* sql.withTransaction(
                    sql`INSERT INTO synthetic_load(value) VALUES (${String(index)})`,
                  );
                  const end = performance.now();
                  writerMs.push(end - start);
                  emit("writer-end", index, undefined, end);
                  yield* Effect.yieldNow;
                }),
              { discard: true },
            ),
            Effect.forEach(
              Array.from({ length: acquisitions }),
              (_, index) =>
                Effect.gen(function* () {
                  const start = performance.now();
                  emit("control-begin", index, undefined, start);
                  yield* sql`SELECT 1`.pipe(NodeSqliteClient.readOnly, interactiveRead);
                  const end = performance.now();
                  controlMs.push(end - start);
                  emit("control-end", index, undefined, end);
                  yield* Effect.yieldNow;
                }),
              { discard: true },
            ),
          ],
          { concurrency: "unbounded", discard: true },
        );
        emit("sql-load-end");
      }).pipe(Effect.provide(database.pipe(Layer.provide(NodeServices.layer)))),
    );
  } finally {
    if (cpuStart)
      diagnosticCpu = {
        process: process.cpuUsage(cpuStart.process),
        thread: process.threadCpuUsage(cpuStart.thread),
      };
    emit("window-end", undefined, diagnosticCpu);
  }
  const computeEvents = computeObservations?.() ?? null;
  return {
    ...(diagnosticCpu ? { diagnosticCpu } : {}),
    readerQueue: percentiles(
      observations.filter((event) => event.phase === "queue").map((event) => event.durationMs),
    ),
    checkpointAdmission: percentiles(
      observations.filter((event) => event.phase === "checkpoint").map((event) => event.durationMs),
    ),
    readerExecute: percentiles(
      observations.filter((event) => event.phase === "execute").map((event) => event.durationMs),
    ),
    readerReplyEstimate: percentiles(
      observations.filter((event) => event.phase === "reply").map((event) => event.durationMs),
    ),
    lease: percentiles(
      observations.filter((event) => event.phase === "lease").map((event) => event.durationMs),
    ),
    writer: percentiles(writerMs),
    control: percentiles(controlMs),
    readerObservations: observations,
    computeObservations: computeEvents,
    computePhases: computeEvents === null ? null : summarizeComputeObservations(computeEvents),
    observationLimitReached: observations.length === 100_000,
  };
}
