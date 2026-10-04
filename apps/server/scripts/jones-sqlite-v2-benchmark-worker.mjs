import * as Assert from "node:assert/strict";
import * as FS from "node:fs";
import * as Path from "node:path";
import * as URL from "node:url";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { makeSqlitePersistenceLive } from "../src/persistence/Layers/Sqlite.ts";
import * as EventStore from "../src/orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../src/orchestration-v2/ProjectionStore.ts";
import * as EventSink from "../src/orchestration-v2/EventSink.ts";
import { runV2Workload } from "./jones-sqlite-v2-workload.ts";
import { readRuntimeBinding } from "../../../scripts/performance-staging/runtime-binding.mjs";
import { assertCurrentCandidate } from "../../../scripts/performance-staging/current-qualification.mjs";

const root = FS.realpathSync(process.cwd());
let report, databaseClosed = false;
const phase = (name) => FS.appendFileSync(Path.join(root, "phases.jsonl"), `${JSON.stringify({ phase: name, pid: process.pid })}\n`);
try {
  Assert.match(Path.basename(root), /^v2-benchmark-[a-f0-9-]{36}$/);
  const requestPath = Path.join(root, "request.json");
  Assert.ok(FS.lstatSync(requestPath).size <= 16384);
  const request = JSON.parse(FS.readFileSync(requestPath, "utf8"));
  Assert.equal(request.root, root);
  const candidate = assertCurrentCandidate(request.candidate);
  Assert.equal(candidate.worktreePath, Path.resolve(Path.dirname(URL.fileURLToPath(import.meta.url)), "../../.."));
  const runtime = readRuntimeBinding();
  Assert.equal(runtime.executablePath, process.execPath);
  Assert.equal(runtime.nodeVersion, process.versions.node);
  for (const key of ["HOME", "TMPDIR", "TMP", "TEMP", "XDG_CACHE_HOME"]) Assert.ok(process.env[key]?.startsWith(`${root}${Path.sep}`));
  phase("bound");
  const dbPath = Path.join(root, "synthetic.sqlite");
  FS.closeSync(FS.openSync(dbPath, "wx", 0o600));
  const database = makeSqlitePersistenceLive(dbPath).pipe(Layer.provide(NodeServices.layer));
  const stores = Layer.mergeAll(database,
    EventStore.layer.pipe(Layer.provideMerge(database)),
    ProjectionStore.layer.pipe(Layer.provideMerge(database)));
  const services = Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores)));
  phase("workload-start");
  const measurements = await Effect.runPromise(runV2Workload(request.workload).pipe(Effect.provide(services)));
  databaseClosed = true;
  Assert.equal(measurements.journal[0]?.journal_mode, "wal");
  phase("database-closed");
  assertCurrentCandidate(candidate);
  report = { schema: "jones-sqlite-v2-benchmark/v1", outcome: "passed", candidate, runtime, measurements };
} catch (error) {
  report = { schema: "jones-sqlite-v2-benchmark/v1", outcome: "failed", error: String(error?.stack ?? error).slice(0, 8192) };
}
process.stdout.write(`${JSON.stringify({ ...report, databaseClosed, installedRuntimeQualification: "unverified" })}\n`);
process.exitCode = report.outcome === "passed" ? 0 : 1;
