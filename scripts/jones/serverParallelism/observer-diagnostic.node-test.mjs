import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import {
  validateDiagnosticAdmission,
  diagnosticCompletion,
  diagnosticLifecycleComplete,
  summarizeDiagnostic,
  verifyDiagnosticArtifacts,
  assertNewDiagnosticPaths,
} from "./observer-diagnostic.mjs";

const source = "a".repeat(40),
  sourceTree = "b".repeat(40),
  build = "c".repeat(64);
const entry = "/fixture/apps/server/src/bin.ts",
  executable = "/fixture/node";
const baseline = {
  schema: "jones.server-parallelism.baseline/v1",
  source,
  profile: "laptop",
  count: 2,
  files: 512,
  acquisitions: 500,
  windows: [{}, {}, {}],
  churnWindows: [
    { workingSet: 9, durationMs: 1, latency: { p99: 1 } },
    { workingSet: 16, durationMs: 1, latency: { p99: 1 } },
  ],
  readerCount: 1,
  computePlacement: "main-thread",
  outcome: "passed",
  nativeScopeClosed: true,
  capturedExit: true,
  cleanupProved: true,
  writer: { p95: 1, p99: 2 },
  loop: { p99: 3 },
};
const manifest = {
  schema: "jones.server-parallelism.command/v1",
  runtimeForm: "source",
  file: executable,
  args: ["--max-old-space-size=256", entry, "__jones-server-compute"],
  build,
  identityResolver: "/fixture/apps/server/src/jones/serverCompute/workerCommand.ts",
};
const admission = {
  manifest,
  baseline,
  mode: "full",
  source,
  sourceTree,
  build,
  entry,
  executable,
  platform: "linux",
  parallelism: 2,
  baselineHash: "d".repeat(64),
};
const prior = {
  schema: "jones.server-parallelism.observer-diagnostic/v1",
  eligible: false,
  complete: true,
  outcome: "recorded",
  mode: "full",
  source,
  sourceTree,
  build,
  baselineHash: admission.baselineHash,
  cleanupProved: true,
  runtimeForm: "source",
  profile: "laptop",
  bounds: {
    count: 2,
    windows: 3,
    acquisitions: 500,
    files: 512,
    fullRuns: 1,
    controlRunsMaximum: 1,
  },
};
const retained = { attempted: 0, retained: 0, dropped: 0, overflow: false };
const report = {
  build,
  count: 2,
  eligible: false,
  shutdownProved: true,
  resources: [],
  windows: [],
  diagnostic: { mode: "full", eligible: false, cpuResources: [], cpuRetention: retained },
  retention: {
    workload: { count: 2, windows: 3, acquisitions: 500, files: 512, workloadMs: 600_000 },
    observations: retained,
    executions: retained,
    resources: retained,
  },
  predicates: { productiveOverlap: false, resourcesComplete: false },
};
const completion = {
  report,
  trace: retained,
  cleanup: { shutdownProved: true, fixtureRemoved: true },
  elapsedMs: 1,
  error: null,
  aborted: false,
};

NodeTest.test("admits only the exact Linux source workload, build and unchanged baseline", () => {
  NodeAssert.doesNotThrow(() => validateDiagnosticAdmission(admission));
  NodeAssert.throws(() => validateDiagnosticAdmission({ ...admission, platform: "darwin" }));
  NodeAssert.throws(() => validateDiagnosticAdmission({ ...admission, source: "e".repeat(40) }));
  NodeAssert.throws(() => validateDiagnosticAdmission({ ...admission, build: "e".repeat(64) }));
  NodeAssert.throws(() => validateDiagnosticAdmission({ ...admission, mode: "FULL" }));
  NodeAssert.throws(() =>
    validateDiagnosticAdmission({ ...admission, manifest: { ...manifest, runtimeForm: "bundle" } }),
  );
  NodeAssert.throws(() =>
    validateDiagnosticAdmission({ ...admission, baseline: { ...baseline, count: 3 } }),
  );
  NodeAssert.throws(() =>
    validateDiagnosticAdmission({ ...admission, baseline: { ...baseline, files: 2048 } }),
  );
  NodeAssert.throws(() =>
    validateDiagnosticAdmission({ ...admission, baseline: { ...baseline, acquisitions: 501 } }),
  );
  NodeAssert.throws(() =>
    validateDiagnosticAdmission({ ...admission, baseline: { ...baseline, cleanupProved: false } }),
  );
});

NodeTest.test("requires a separately selected full receipt for the one control and never chooses favorable latency", () => {
  NodeAssert.throws(() => validateDiagnosticAdmission({ ...admission, mode: "cpu-stat-only" }));
  NodeAssert.doesNotThrow(() =>
    validateDiagnosticAdmission({ ...admission, mode: "cpu-stat-only", prior }),
  );
  NodeAssert.throws(() =>
    validateDiagnosticAdmission({
      ...admission,
      mode: "cpu-stat-only",
      prior: { ...prior, mode: "cpu-stat-only" },
    }),
  );
  NodeAssert.throws(() =>
    validateDiagnosticAdmission({
      ...admission,
      mode: "cpu-stat-only",
      prior: { ...prior, baselineHash: "e".repeat(64) },
    }),
  );
  NodeAssert.throws(() => validateDiagnosticAdmission({ ...admission, prior }));
  const favorable = {
    ...report,
    diagnostic: { ...report.diagnostic, mode: "cpu-stat-only" },
    predicates: { productiveOverlap: true, normalTargets: true },
  };
  NodeAssert.equal(diagnosticCompletion({ ...completion, report: favorable }), true);
  NodeAssert.equal(summarizeDiagnostic(favorable, []).eligible, false);
});

NodeTest.test("makes timeout, loss, absent cleanup, failure and cancellation incomplete independently of official predicates", () => {
  NodeAssert.equal(diagnosticCompletion(completion), true);
  NodeAssert.equal(diagnosticCompletion({ ...completion, elapsedMs: 600_001 }), false);
  NodeAssert.equal(
    diagnosticCompletion({ ...completion, trace: { ...retained, dropped: 1, overflow: true } }),
    false,
  );
  NodeAssert.equal(
    diagnosticCompletion({
      ...completion,
      cleanup: { shutdownProved: true, fixtureRemoved: false },
    }),
    false,
  );
  NodeAssert.equal(diagnosticCompletion({ ...completion, cleanup: undefined }), false);
  NodeAssert.equal(diagnosticCompletion({ ...completion, error: { raw: "failed read" } }), false);
  NodeAssert.equal(diagnosticCompletion({ ...completion, aborted: true }), false);
});

NodeTest.test("requires captured exit, stream close and journal retirement separately", () => {
  const boot = { slotGeneration: 1, nonce: "n" };
  const events = ["spawn", "retire-resolved", "captured-close", "retired-end"].map((event) => ({
    kind: event === "retired-end" || event === "retire-resolved" ? "qualification" : "lifecycle",
    event,
    pid: 2,
    bootstrap: boot,
  }));
  NodeAssert.equal(diagnosticLifecycleComplete(events), false);
  events.push({ kind: "lifecycle", event: "captured-exit", pid: 2, bootstrap: boot });
  NodeAssert.equal(diagnosticLifecycleComplete(events), true);
  NodeAssert.equal(
    diagnosticLifecycleComplete(
      events.map((row) =>
        row.event === "captured-exit" ? { ...row, bootstrap: { ...boot, nonce: "reused" } } : row,
      ),
    ),
    false,
  );
});

NodeTest.test("retains missing CPU rows and sticky start-time mismatch without bridging a successful retry", () => {
  const boot = { version: 1, build, authorityId: "a", slotId: 0, slotGeneration: 1, nonce: "n" };
  const rows = [1, 2, 3, 4, 5].map((readId) => ({
    readId,
    at: readId * 10,
    finishedAt: readId * 10 + 1,
    phase: "steady-0",
    generations: [{ pid: 2, bootstrap: boot }],
    sample: { processes: readId === 2 ? [] : [{ pid: 2, cpuMs: readId }] },
  }));
  const events = [1, 3, 4, 5].map((readId) => ({
    kind: "resource",
    stage: "stat",
    edge: "end",
    readId,
    pid: 2,
    bootstrap: boot,
    atMs: readId * 10,
    phase: "steady-0",
    startTimeTicks: readId < 4 ? "10" : "11",
  }));
  events.push({
    kind: "qualification",
    event: "request-timing",
    requestId: 7,
    pid: 2,
    bootstrap: boot,
    phase: "steady-0",
    atMs: 51,
    timing: {
      worker: { executionOutcome: "ok", executionStartedMs: 0, executionCompletedMs: 100 },
      delivery: { offsetUpperMs: 0, offsetLowerMs: 0 },
    },
  });
  const summary = summarizeDiagnostic({ ...report, resources: rows }, events);
  NodeAssert.equal(summary.resourceIntervals.length, 5);
  NodeAssert.equal(summary.resourceIntervals[1].available, false);
  NodeAssert.equal(
    summary.cpuEnvelopes.some((row) => row.relation === "contained"),
    false,
  );
  NodeAssert.equal(summary.resourceIntervals[4].relation, "pid-identity-mismatch");
});

NodeTest.test("binds all separate artifacts and refuses output collisions before fixture creation", async () => {
  const directory = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "jones-observer-admission-"),
  );
  const output = NodePath.join(directory, "report.json");
  try {
    await assertNewDiagnosticPaths(output, [NodePath.join(directory, "manifest.json")]);
    await NodeAssert.rejects(assertNewDiagnosticPaths(output, [output]));
    const boot = { slotGeneration: 1, nonce: "n" };
    const events = ["spawn", "captured-exit", "captured-close", "retired-end"].map((event) => ({
      kind: event === "retired-end" ? "qualification" : "lifecycle",
      event,
      pid: 2,
      bootstrap: boot,
    }));
    const trace = {
      events,
      retention: { ...retained, retained: events.length, attempted: events.length },
    };
    const values = {
      raw: report,
      trace,
      summary: { eligible: false },
      error: null,
      cleanup: completion.cleanup,
    };
    const artifacts = {};
    for (const [kind, value] of Object.entries(values)) {
      const bytes = JSON.stringify(value);
      await NodeFSP.writeFile(`${output}.${kind}.json`, bytes, { flag: "wx" });
      artifacts[kind] = { sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex") };
    }
    const receipt = { ...prior, elapsedMs: 1, aborted: false, artifacts };
    NodeAssert.equal(await verifyDiagnosticArtifacts(receipt, output), true);
    await NodeAssert.rejects(assertNewDiagnosticPaths(output, []));
    await NodeFSP.writeFile(`${output}.cleanup.json`, JSON.stringify({ shutdownProved: false }));
    NodeAssert.equal(await verifyDiagnosticArtifacts(receipt, output), false);
  } finally {
    await NodeFSP.rm(directory, { recursive: true, force: true }).catch(() => {});
  }
});
