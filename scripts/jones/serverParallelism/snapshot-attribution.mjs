import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as HostProcess from "../../../packages/shared/src/HostProcess.ts";

const [output] = process.argv.slice(2);
if (
  !NodePath.isAbsolute(output ?? "") ||
  HostProcess.Platform.defaultValue() !== "darwin" ||
  process.env.GITHUB_EVENT_NAME !== "workflow_dispatch"
) {
  throw new Error("Snapshot attribution requires an explicitly dispatched macOS job");
}

const rev = (ref) =>
  NodeChildProcess.execFileSync("git", ["rev-parse", ref], { encoding: "utf8" }).trim();
const source = rev("HEAD");
const sourceTree = rev("HEAD^{tree}");
const env = { ...process.env, JONES_SP_SOURCE_SHA: source, JONES_SP_SOURCE_TREE: sourceTree };
const fixture = NodePath.join(import.meta.dirname, "snapshot.mjs");
const preload = NodePath.join(import.meta.dirname, "snapshot-timing-preload.mjs");
const results = [];
const arms = [
  { name: "control", compression: "sync", instrumented: false },
  { name: "async-compression", compression: "async", instrumented: false },
  { name: "async-compression-sql-trace", compression: "async", instrumented: true },
  { name: "sql-trace", compression: "sync", instrumented: true },
];

for (const { name, compression, instrumented } of arms) {
  const reportPath = NodePath.join(output, "snapshot-" + name + ".json");
  const traceName = instrumented ? "snapshot-" + name + "-calls.json" : null;
  const log = NodeFS.openSync(NodePath.join(output, "snapshot-" + name + ".log"), "wx", 0o600);
  try {
    const run = NodeChildProcess.spawnSync(
      process.execPath,
      [...(instrumented ? ["--import", preload] : []), fixture, reportPath, "2000", compression],
      {
        env: traceName ? { ...env, JONES_SP_SNAPSHOT_SQL: NodePath.join(output, traceName) } : env,
        stdio: ["ignore", log, log],
        timeout: 120_000,
        killSignal: "SIGKILL",
      },
    );
    results.push({
      name,
      compression,
      traceName,
      status: run.status,
      signal: run.signal,
      error: run.error?.message,
      capturedExit: Number.isSafeInteger(run.pid) && (run.status !== null || run.signal !== null),
    });
    if (!results.at(-1)?.capturedExit) break;
  } finally {
    NodeFS.closeSync(log);
  }
}

const cleanupProved =
  results.length === arms.length && results.every((result) => result.capturedExit);
const read = (relative) => {
  try {
    return JSON.parse(NodeFS.readFileSync(NodePath.join(output, relative), "utf8"));
  } catch {
    return null;
  }
};
const complete =
  cleanupProved &&
  results.every((result) => {
    const report = read("snapshot-" + result.name + ".json");
    const trace = result.traceName && read(result.traceName);
    return (
      report?.source === source &&
      report.threadCount === 2000 &&
      report.compression === result.compression &&
      report.samples?.length === 3 &&
      (result.status === 0 || result.status === 1) &&
      (!result.traceName ||
        (trace?.source === source &&
          trace.overflow === false &&
          Array.isArray(trace.calls) &&
          trace.calls.length > 0))
    );
  });
NodeFS.writeFileSync(
  NodePath.join(output, "qualification.json"),
  JSON.stringify(
    {
      schema: "jones.server-parallelism.snapshot-attribution/v1",
      source,
      sourceTree,
      eligible: false,
      outcome: complete ? "recorded" : "incomplete",
      predicates: { cleanupProved },
      results,
    },
    null,
    2,
  ) + "\n",
  { flag: "wx", mode: 0o600 },
);
if (!complete) process.exitCode = 1;
