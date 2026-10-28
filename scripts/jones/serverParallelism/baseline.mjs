// Real same-source negative control: default single reader and the identical
// native algorithms run in this main process, as before worker offload.
import { mkdtemp, realpath, mkdir, writeFile, rm, rename } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, isAbsolute } from "node:path";
import { monitorEventLoopDelay } from "node:perf_hooks";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as SqlClient from "effect/sql/SqlClient";
import { make } from "../../../apps/server/src/jones/workspace/nativeSearchRuntime.ts";
import { percentiles } from "../../../apps/server/src/jones/serverParallelism/combinedQualification.ts";
const [output, profile] = process.argv.slice(2);
if (!isAbsolute(output ?? "") || !["laptop", "vps"].includes(profile))
  throw new Error("baseline.mjs <absolute output> <laptop|vps>");
const count = profile === "vps" ? 3 : 2,
  acquisitions = 500,
  files = 512;
const scratch = await realpath(await mkdtemp(join(tmpdir(), "jones-sp-baseline-")));
const scope = await Effect.runPromise(Scope.make());
const runtimes = new Map(),
  keys = [],
  windows = [],
  churnWindows = [];
let nativeScopeClosed = false,
  report;
const controller = new AbortController(),
  stop = () => controller.abort();
process.once("SIGTERM", stop);
process.once("SIGINT", stop);
const execute = (work) => Effect.runPromise(work, { signal: controller.signal });
const runtime = async (key) => {
  if (!runtimes.has(key))
    runtimes.set(
      key,
      await execute(make(key.cwd, key.variant).pipe(Effect.provideService(Scope.Scope, scope))),
    );
  return runtimes.get(key);
};
const request = async (key) => {
  const native = await runtime(key);
  const result = await execute(
    key.variant === "content"
      ? native.searchContents({
          query: "needle",
          limit: 32,
          caseSensitive: true,
          wholeWord: false,
          useRegex: false,
        })
      : native.search("file", 32),
  );
  if ((result.entries ?? result.matches).length === 0)
    throw new Error("Baseline performed no useful native work");
  if (key.variant === "content") {
    const absent = await execute(
      native.searchContents({
        query: "qualification-absent-marker",
        limit: 32,
        caseSensitive: true,
        wholeWord: false,
        useRegex: false,
      }),
    );
    if (absent.matches.length !== 0) throw new Error("Baseline absent marker unexpectedly matched");
  }
};
try {
  for (let index = 0; index < 8 * count; index++) {
    const cwd = join(scratch, `workspace-${index}`);
    await mkdir(cwd);
    await writeFile(join(cwd, ".gitignore"), "ignored/\n");
    const content = `needle-${index} alpha beta\n`.repeat(256);
    for (let file = 0; file < files; file++)
      await writeFile(join(cwd, `file-${file}.txt`), content);
    keys.push({ cwd, variant: index % 2 === 0 ? "paths" : "content" });
  }
  for (const key of keys.slice(0, 4 * count)) await runtime(key);
  for (let window = 0; window < 3; window++) {
    const writer = [],
      control = [],
      loop = monitorEventLoopDelay({ resolution: 10 });
    loop.enable();
    try {
      const sqlWork = Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA journal_mode = WAL`;
        yield* sql`CREATE TABLE synthetic_load(id INTEGER PRIMARY KEY, value TEXT)`;
        yield* Effect.all(
          [
            Effect.promise(async () => {
              for (let round = 0; round < acquisitions; round++) {
                for (let lane = 0; lane < count; lane++)
                  await request(keys[lane * 4 + (round % 4)]);
              }
            }),
            Effect.forEach(
              Array.from({ length: acquisitions }),
              () =>
                sql`SELECT COUNT(*) FROM synthetic_load`.pipe(
                  NodeSqliteClient.readOnly,
                  Effect.andThen(Effect.yieldNow),
                ),
              { concurrency: count * 2, discard: true },
            ),
            Effect.forEach(
              Array.from({ length: acquisitions }),
              (_, index) =>
                Effect.gen(function* () {
                  const start = performance.now();
                  yield* sql.withTransaction(
                    sql`INSERT INTO synthetic_load(value) VALUES (${String(index)})`,
                  );
                  writer.push(performance.now() - start);
                  yield* Effect.yieldNow;
                }),
              { discard: true },
            ),
            Effect.forEach(
              Array.from({ length: acquisitions }),
              () =>
                Effect.gen(function* () {
                  const start = performance.now();
                  yield* sql`SELECT 1`.pipe(NodeSqliteClient.readOnly);
                  control.push(performance.now() - start);
                  yield* Effect.yieldNow;
                }),
              { discard: true },
            ),
          ],
          { concurrency: "unbounded", discard: true },
        );
      }).pipe(
        Effect.provide(
          NodeSqliteClient.layer({
            filename: join(scratch, `baseline-${window}.sqlite`),
            checkpointWorker: true,
            readerFailureMode: "fail",
          }),
        ),
        Effect.provide(NodeServices.layer),
      );
      await execute(sqlWork);
      windows.push({
        writer: percentiles(writer),
        control: percentiles(control),
        loop: { p99: loop.percentile(99) / 1e6, max: loop.max / 1e6 },
      });
    } finally {
      loop.disable();
    }
  }
  for (const workingSet of [4 * count + 1, 8 * count]) {
    const started = performance.now(),
      latency = [];
    for (let round = 0; round < 3; round++) {
      for (const key of keys.slice(0, workingSet)) {
        const begin = performance.now();
        await request(key);
        const changed = join(key.cwd, "churn.txt"),
          moved = join(key.cwd, "renamed.txt");
        await writeFile(changed, "needle churn\n".repeat(128));
        await rename(changed, moved);
        await mkdir(join(key.cwd, "ignored"), { recursive: true });
        await writeFile(join(key.cwd, "ignored", "hidden.txt"), "needle ignored");
        await execute((await runtime(key)).refresh());
        await rm(moved);
        latency.push(performance.now() - begin);
      }
      for (const hot of keys.slice(0, count)) await request(hot);
    }
    churnWindows.push({
      workingSet,
      durationMs: performance.now() - started,
      latency: percentiles(latency),
    });
  }
  // Medians across repeated windows avoid selecting the slowest baseline as the target.
  const median = (values) => [...values].sort((a, b) => a - b)[1];
  report = {
    schema: "jones.server-parallelism.baseline/v1",
    outcome: "passed",
    source: process.env.JONES_SP_SOURCE_SHA,
    profile,
    count,
    files,
    acquisitions,
    windows,
    churnWindows,
    readerCount: 1,
    computePlacement: "main-thread",
    writer: {
      p95: median(windows.map((w) => w.writer.p95)),
      p99: median(windows.map((w) => w.writer.p99)),
    },
    loop: { p99: median(windows.map((w) => w.loop.p99)) },
    meaning:
      "Same candidate algorithms/native patch; serial main-process execution and default single-reader baseline, not a historical Git build",
  };
} catch (cause) {
  report = {
    schema: "jones.server-parallelism.baseline/v1",
    outcome: "failed",
    source: process.env.JONES_SP_SOURCE_SHA,
    profile,
    scratch,
    windows,
    churnWindows,
    error: String(cause),
  };
  process.exitCode = 1;
} finally {
  try {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    nativeScopeClosed = true;
  } catch (cause) {
    report = { ...report, outcome: "failed", cleanupError: String(cause) };
    process.exitCode = 1;
  } finally {
    await writeFile(
      output,
      JSON.stringify(
        {
          ...report,
          scratch,
          nativeScopeClosed,
          cleanupProved: false,
          cleanupOwner: "CI parent after captured process exit",
        },
        null,
        2,
      ) + "\n",
      { flag: "wx", mode: 0o600 },
    );
    // Scope close cannot prove native-tail retirement. The captured CI parent
    // removes this exact root only after observing this process close.
    process.removeListener("SIGTERM", stop);
    process.removeListener("SIGINT", stop);
  }
}
