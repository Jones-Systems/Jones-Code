import * as NodeAssert from "node:assert/strict";
import * as NodePath from "node:path";
import * as NodeFS from "node:fs";
import { test } from "vite-plus/test";
import * as NodeURL from "node:url";
import {
  validateV2BenchmarkRequest,
  v2BenchmarkMetadata,
  v2BenchmarkClosureKnown,
  runV2Benchmark,
} from "./v2Benchmark.mjs";
import { assertCurrentCandidate } from "../../../../scripts/jones/performance/current-qualification.mjs";
import { readRuntimeBinding } from "../../../../scripts/jones/performance/runtime-binding.mjs";

test("V2 benchmark refuses another source checkout and oversized requests", () => {
  const worktreePath = NodePath.resolve(
    NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
    "../../../..",
  );
  const request = {
    candidate: { worktreePath },
    parentPath: "/explicit/scratch",
    workload: { commands: 1, payloadBytes: 1, intervalMs: 0 },
  };
  NodeAssert.equal(validateV2BenchmarkRequest(request), request);
  NodeAssert.throws(() =>
    validateV2BenchmarkRequest({ ...request, candidate: { worktreePath: "/other/source" } }),
  );
  NodeAssert.throws(() =>
    validateV2BenchmarkRequest({ ...request, workload: { ...request.workload, commands: 65 } }),
  );
  NodeAssert.throws(() =>
    validateV2BenchmarkRequest({ ...request, databasePath: "/existing.sqlite" }),
  );
  NodeAssert.throws(() =>
    validateV2BenchmarkRequest({ ...request, failureMode: "before-workload" }),
  );
});

test("benchmark metadata keeps execution explicitly unavailable by default", () => {
  const metadata = v2BenchmarkMetadata();
  NodeAssert.equal(metadata.outcome, "unavailable");
  NodeAssert.equal(metadata.reason, "explicit candidate/runtime binding required");
  NodeAssert.deepEqual(metadata.caps, { commands: 64, payloadBytes: 4096, intervalMs: 100 });
  NodeAssert.equal(metadata.sqlTiming, "unavailable");
});

test("benchmark disposal requires evidence, captured child closure and database scope closure", () => {
  for (const outcome of [
    "success",
    "failed",
    "cancelled",
    "timed_out",
    "output_limited",
    "spawn_refused",
  ]) {
    const child = { closed: true, reaped: true, outcome };
    NodeAssert.equal(v2BenchmarkClosureKnown(child, { databaseClosed: true }, true), true);
    NodeAssert.equal(v2BenchmarkClosureKnown(child, { databaseClosed: false }, true), false);
    NodeAssert.equal(v2BenchmarkClosureKnown(child, { databaseClosed: true }, false), false);
    NodeAssert.equal(
      v2BenchmarkClosureKnown({ ...child, closed: false }, { databaseClosed: true }, true),
      false,
    );
    NodeAssert.equal(
      v2BenchmarkClosureKnown({ ...child, reaped: false }, { databaseClosed: true }, true),
      false,
    );
  }
  NodeAssert.equal(
    v2BenchmarkClosureKnown(
      { closed: true, reaped: true, outcome: "unknown" },
      { databaseClosed: true },
      true,
    ),
    false,
  );
  NodeAssert.equal(v2BenchmarkClosureKnown(undefined, undefined, true), true);
  NodeAssert.equal(v2BenchmarkClosureKnown(undefined, undefined, false), false);
});

// Opt in with an exact clean candidate plus an explicit executable-hash runtime binding.
test.skipIf(!process.env.JONES_V2_BENCHMARK_TEST_BINDING)(
  "bound synthetic V2 workload preserves success/failure evidence and closes child/database scratch",
  async () => {
    const bindingPath = process.env.JONES_V2_BENCHMARK_TEST_BINDING;
    NodeAssert.equal(NodePath.resolve(bindingPath), bindingPath);
    const info = NodeFS.lstatSync(bindingPath);
    NodeAssert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 16384);
    const binding = JSON.parse(NodeFS.readFileSync(bindingPath, "utf8"));
    NodeAssert.deepEqual(Object.keys(binding).sort(), [
      "candidate",
      "parentPath",
      "runtimeBindingPath",
    ]);
    NodeAssert.equal(process.env.JONES_RUNTIME_BINDING, binding.runtimeBindingPath);
    const runtime = readRuntimeBinding();
    assertCurrentCandidate(binding.candidate);
    const workload = { commands: 3, payloadBytes: 32, intervalMs: 1 };
    for (const failureMode of [undefined, "after-workload"]) {
      const result = await runV2Benchmark({
        candidate: binding.candidate,
        parentPath: binding.parentPath,
        workload,
        ...(failureMode ? { failureMode } : {}),
      });
      NodeAssert.equal(result.outcome, failureMode ? "failed" : "passed", result.error);
      NodeAssert.equal(result.report.outcome, failureMode ? "failed" : "passed");
      NodeAssert.equal(result.report.databaseClosed, true);
      NodeAssert.deepEqual(result.report.candidate, binding.candidate);
      NodeAssert.deepEqual(result.report.runtime, runtime);
      NodeAssert.equal(result.report.installedRuntimeQualification, "unverified");
      const measurements = result.report.measurements;
      NodeAssert.deepEqual(measurements.options, workload);
      for (const summary of Object.values(measurements.traffic)) {
        NodeAssert.equal(summary.samples, workload.commands);
        for (const key of ["minimumMs", "medianMs", "p95Ms", "maximumMs"])
          NodeAssert.ok(Number.isFinite(summary[key]) && summary[key] >= 0);
      }
      NodeAssert.equal(measurements.protocol.eventSinkCompletion.samples, 5);
      for (const key of ["acceptedReplay", "rejectedReplay", "rollbackUnchanged", "retryCommitted"])
        NodeAssert.equal(measurements.protocol[key], true);
      NodeAssert.deepEqual(measurements.consistency, {
        finalSequence: workload.commands + 2,
        integrity: "ok",
        foreignKeyViolations: 0,
        projectedTitleMatches: true,
      });
      NodeAssert.equal(measurements.journal[0].journal_mode, "wal");
      NodeAssert.ok(measurements.engine[0].version && measurements.engine[0].sourceId);
      NodeAssert.equal(result.cleanup.absent, true);
      NodeAssert.ok(result.evidencePath);
      const evidenceInfo = NodeFS.lstatSync(result.evidencePath);
      NodeAssert.ok(evidenceInfo.isFile() && evidenceInfo.size <= 128 * 1024);
      const [evidence, cleanup] = NodeFS.readFileSync(result.evidencePath, "utf8")
        .trim()
        .split("\n")
        .map(JSON.parse);
      NodeAssert.deepEqual(cleanup.cleanup, result.cleanup);
      NodeAssert.equal(evidence.child.closed, true);
      NodeAssert.equal(evidence.child.reaped, true);
      NodeAssert.equal(evidence.child.truncated, false);
      NodeAssert.equal(evidence.child.outcome, failureMode ? "failed" : "success");
      NodeAssert.equal(evidence.child.exitCode, failureMode ? 1 : 0);
      NodeAssert.equal(evidence.child.stdout.trim().split("\n").length, 1);
      NodeAssert.deepEqual(JSON.parse(evidence.child.stdout), result.report);
      const phases = evidence.phases
        .trim()
        .split("\n")
        .map(JSON.parse)
        .map((entry) => entry.phase);
      NodeAssert.deepEqual(phases, [
        "bound",
        "workload-start",
        "workload-completed",
        ...(failureMode ? ["failure-injected"] : []),
        "database-closed",
      ]);
      NodeAssert.equal(NodeFS.existsSync(result.evidencePath.slice(0, -".jsonl".length)), false);
      if (failureMode) NodeAssert.match(result.error, /synthetic workload failed/);
    }
    assertCurrentCandidate(binding.candidate);
  },
);
