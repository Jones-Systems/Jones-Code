import assert from "node:assert/strict";
import { AssertionError } from "node:assert";
import { test } from "node:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { EventEmitter } from "node:events";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createManifest } from "../../../apps/desktop/scripts/ui-evidence/manifest.mjs";
import {
  allocateRun,
  superviseRun,
  removeRun,
} from "../../../apps/desktop/scripts/ui-evidence/lifecycle.mjs";
import { sha256 } from "../../../apps/desktop/scripts/ui-evidence/provenance.mjs";
import {
  classifyNativeFailure,
  validateNativeBoundary,
  readNativeEvidence,
} from "../../../apps/desktop/scripts/ui-evidence/native-workflow-boundary.mjs";
import {
  nativeVariants,
  nativeAdmission,
  attemptNativeVariant,
  finishNativeVariant,
  aggregateNative,
  runOwnedNative,
  readNativeWorkflowRows,
} from "./native-workflow-evidence.mjs";

const repo = fileURLToPath(new URL("../../../", import.meta.url));
const workflow = () =>
  fs.readFile(path.join(repo, ".github/workflows/jones-native-workflows.yml"), "utf8");
const cleanup = { outcome: "complete", remainingRoots: 0 };
const generation = {
  index: 0,
  closeAttempted: true,
  closed: true,
  exit: { code: 0, signal: null },
  processes: { electron: { pid: 41, startTicks: "100" }, server: { pid: 42, startTicks: "101" } },
};

function boundaryFixture(failed = true) {
  const parent = "/synthetic/state/jones-code-ui-evidence";
  const runId = "12345678-1234-1234-1234-123456789abc";
  const expected = {
    artifacts: "/synthetic/evidence/positive",
    scenario: "/synthetic/scenario.mjs",
    scenarioSha256: sha256("scenario"),
    head: "a".repeat(40),
    electron: "40.0.0",
    exitCode: failed ? 1 : 0,
  };
  const source = {
    head: expected.head,
    clean: true,
    statusSha256: sha256(""),
    patch: { sha256: sha256("") },
    untracked: { sha256: sha256("[]") },
  };
  const receipt = {
    schema: "jones-code-ui-evidence-build/v1",
    source: {
      head: source.head,
      statusSha256: source.statusSha256,
      patchSha256: source.patch.sha256,
      untrackedSha256: source.untracked.sha256,
    },
    build: Object.fromEntries(
      ["desktop", "server", "web", "boot"].map((name) => [name, sha256(name)]),
    ),
  };
  const receiptSha256 = sha256("exact receipt");
  const build = {
    ...Object.fromEntries(
      Object.entries(receipt.build).map(([name, hash]) => [name, { sha256: hash }]),
    ),
    sourceCorrespondence: { status: "receipt-matched", receipt: { sha256: receiptSha256 } },
  };
  const inner = {
    status: failed ? "failed" : "passed",
    error: failed ? "Scenario assertion failed: Backend target is pinned" : null,
    runtime: { electron: expected.electron },
    pendingElectronLaunch: false,
    isolation: { status: "passed" },
    boundary: {
      schema: "jones-native-boundary/v1",
      kind: failed ? "assertion-failed" : "passed",
      phase: "scenario",
      scenarioSha256: expected.scenarioSha256,
      teardown: "complete",
      xvfbClosed: true,
    },
    assertions: [{ name: "Backend target is pinned", status: failed ? "failed" : "passed" }],
    steps: [],
    fixture: {},
    processGenerations: [structuredClone(generation)],
  };
  const namespaceProcess = { pid: 93, startTicks: "202", exited: true };
  const manifest = createManifest({
    runId,
    provenance: {
      source,
      harness: { head: source.head, clean: true, statusSha256: sha256("") },
      build,
    },
    scenario: { id: "custom", path: expected.scenario, sha256: expected.scenarioSha256 },
    inner: { ...inner, namespaceProcess },
    cleanup: { outcome: "complete", rootAbsent: true },
    status: inner.status,
  });
  const record = {
    id: runId,
    parent,
    registry: path.join(parent, "registry", `${runId}.json`),
    root: path.join(parent, "scratch", `${runId}-unique`),
    cleanup: "complete",
    pid: null,
    namespaceProcess,
  };
  const result = {
    code: expected.exitCode,
    status: inner.status,
    error: inner.error,
    artifacts: expected.artifacts,
    runId,
    cleanup: { outcome: "complete", rootAbsent: true },
  };
  return {
    result,
    manifest,
    inner,
    record,
    receipt,
    receiptSha256,
    current: structuredClone(manifest),
    expected,
    parent,
  };
}

function syntheticExpected(variant) {
  return {
    ...variant,
    output: "/synthetic/evidence",
    source: "/synthetic/source",
    scenario: `/synthetic/scenarios/${variant.scenario}`,
    artifacts: `/synthetic/evidence/${variant.id}`,
  };
}

test("ordinary first assertion failure with proved partial cleanup permits later first attempts but leaves the aggregate red", async () => {
  const rows = [],
    starts = [],
    validated = [];
  for (const variant of nativeVariants) {
    const failedPositive = variant.id === "positive";
    const code = failedPositive ? 1 : variant.expectedCode;
    const fixture = boundaryFixture(code === 1);
    const attempt = await attemptNativeVariant({
      variant,
      rows,
      expected: syntheticExpected(variant),
      admit: async () => {},
      execute: async () => {
        starts.push(variant.id);
        return { started: true, code, groupAbsent: true };
      },
      validate: async () => {
        validated.push(variant.id);
        return validateNativeBoundary(fixture);
      },
    });
    rows.push(finishNativeVariant(variant, attempt, failedPositive ? 1 : 0));
  }
  assert.deepEqual(
    starts,
    nativeVariants.map((variant) => variant.id),
  );
  assert.deepEqual(validated, starts);
  assert.equal(new Set(starts).size, starts.length);
  assert.equal(rows[0].safe, true);
  assert.equal(rows[0].nativeCode, 1);
  assert.equal(rows[0].status, "failed");
  assert.equal(rows[1].status, "expected-negative");
  assert.equal(rows[2].status, "passed");
  assert.equal(aggregateNative(rows, false, cleanup).status, "failed");
  const healed = rows.map((row, i) =>
    i === 0 ? { ...row, status: "passed", nativeCode: 0, oracleCode: 0 } : row,
  );
  assert.equal(aggregateNative(healed, false, cleanup).status, "passed");
  assert.equal(aggregateNative(healed).status, "failed", "missing job cleanup must not pass");
});

test("unknown cleanup, dirty receipt or identity mismatch suppresses every later native start without losing the first code", async () => {
  for (const mutate of [
    (f) => {
      f.manifest.cleanup.outcome = "unknown";
    },
    (f) => {
      f.receipt.source.statusSha256 = sha256("dirty");
    },
    (f) => {
      f.record.id = "another-run";
    },
  ]) {
    const rows = [],
      starts = [];
    const fixture = boundaryFixture();
    mutate(fixture);
    for (const variant of nativeVariants) {
      const attempt = await attemptNativeVariant({
        variant,
        rows,
        expected: syntheticExpected(variant),
        admit: async () => {},
        execute: async () => {
          starts.push(variant.id);
          return { started: true, code: 1, groupAbsent: true };
        },
        validate: async () => validateNativeBoundary(fixture),
      });
      rows.push(attempt);
    }
    assert.deepEqual(starts, ["positive"]);
    assert.equal(rows[0].nativeCode, 1);
    assert.equal(rows[0].safe, false);
    assert.ok(rows.slice(1).every((row) => row.status === "not-executed/blocked"));
    assert.equal(aggregateNative(rows, false, cleanup).status, "failed");
  }
});

test("the actual boundary validator refuses missing, foreign, dirty, pending and incomplete cleanup evidence", () => {
  for (const mutate of [
    (f) => {
      delete f.inner.boundary;
    },
    (f) => {
      f.manifest.namespaceProcess.exited = "unknown";
    },
    (f) => {
      f.record.namespaceProcess = { pid: 94, startTicks: "202", exited: true };
    },
    (f) => {
      f.manifest.harness.clean = false;
    },
    (f) => {
      f.current.source.clean = false;
    },
    (f) => {
      f.current.build.web.sha256 = sha256("changed build");
    },
    (f) => {
      f.manifest.scenario.sha256 = sha256("wrong variant");
    },
    (f) => {
      f.result.runId = "foreign";
    },
    (f) => {
      f.record.registry = "/foreign/registry.json";
    },
    (f) => {
      f.record.root = "/foreign/scratch";
    },
    (f) => {
      f.inner.closeError = "cleanup incomplete";
    },
    (f) => {
      f.inner.startupDiagnosticsError = "missing";
    },
    (f) => {
      f.inner.pendingElectronLaunch = true;
    },
    (f) => {
      f.inner.processGenerations[0].closed = false;
    },
    (f) => {
      delete f.inner.processGenerations[0].processes.server;
    },
    (f) => {
      f.inner.boundary.xvfbClosed = false;
    },
    (f) => {
      f.inner.fixture.nodeCleanupFailed = true;
    },
    (f) => {
      f.inner.fixture.nodeClient = { removed: false };
    },
    (f) => {
      f.inner.boundary.kind = "unknown";
      f.manifest.boundary.kind = "unknown";
    },
  ]) {
    const fixture = boundaryFixture();
    mutate(fixture);
    assert.throws(() => validateNativeBoundary(fixture));
  }
  const service = boundaryFixture(false);
  service.expected.secretService = true;
  service.inner.secretService = {
    status: "closed",
    cancelled: null,
    services: [{ pid: 61, startTicks: "301", closed: true, exited: true, error: false }],
  };
  assert.equal(validateNativeBoundary(service).safe, true);
  for (const mutate of [
    (f) => {
      f.inner.secretService.status = "cleanup-unproved";
    },
    (f) => {
      f.inner.secretService.services[0].closed = false;
    },
    (f) => {
      f.inner.secretService.services[0].startTicks = null;
    },
    (f) => {
      f.inner.secretService.cancelled = "SIGTERM";
    },
  ]) {
    const fixture = structuredClone(service);
    mutate(fixture);
    assert.throws(() => validateNativeBoundary(fixture));
  }
});

test("only a recorded exact scenario assertion is ordinary; transport, setup and aggregated cleanup exceptions hold", () => {
  const assertions = [{ name: "target", status: "failed" }];
  const error = new Error("Scenario assertion failed: target");
  assert.equal(classifyNativeFailure("scenario", error, assertions), "assertion-failed");
  for (const [phase, caught, recorded] of [
    ["electron-launch", error, assertions],
    ["scenario", new Error("Transport closed"), assertions],
    ["scenario", error, []],
    ["scenario", new AggregateError([error], error.message), assertions],
  ])
    assert.equal(classifyNativeFailure(phase, caught, recorded), "unknown");
  const nodeAssertion = new AssertionError({
    actual: false,
    expected: true,
    operator: "strictEqual",
  });
  const steps = [{ name: "Saved selection", status: "failed" }];
  assert.equal(classifyNativeFailure("scenario", nodeAssertion, [], steps), "assertion-failed");
  assert.equal(classifyNativeFailure("scenario", nodeAssertion, [], []), "unknown");
  assert.equal(classifyNativeFailure("scenario-setup", nodeAssertion, [], steps), "unknown");
  const fixture = boundaryFixture();
  fixture.inner.assertions = [];
  fixture.manifest.assertions = [];
  fixture.inner.steps = steps;
  fixture.manifest.scenario.steps = steps;
  fixture.inner.boundary.failure = {
    name: "AssertionError",
    code: "ERR_ASSERTION",
    step: "Saved selection",
  };
  fixture.inner.error = fixture.manifest.error = fixture.result.error = nodeAssertion.message;
  assert.equal(validateNativeBoundary(fixture).safe, true);
  fixture.inner.boundary.failure.code = "ERR_TRANSPORT";
  assert.throws(() => validateNativeBoundary(fixture));
});

test("cancellation and a resource hold admit no starts; duplicate variants and missing evidence never become a pass", async () => {
  const variant = nativeVariants[0];
  for (const cancelled of [true, false]) {
    const attempt = await attemptNativeVariant({
      variant,
      rows: [],
      expected: syntheticExpected(variant),
      cancelled,
      admit: async () => {
        throw new Error("resource-hold");
      },
      execute: async () => assert.fail("Native start"),
      validate: async () => assert.fail("Missing evidence accepted"),
    });
    assert.equal(attempt.attempted, false);
    assert.equal(attempt.status, cancelled ? "cancelled" : "not-executed/blocked");
    assert.notEqual(aggregateNative([attempt], cancelled, cleanup).status, "passed");
  }
  assert.equal(nativeAdmission("positive", [{ id: "positive" }]), "first-attempt-already-recorded");
  const missing = await attemptNativeVariant({
    variant,
    rows: [],
    expected: syntheticExpected(variant),
    admit: async () => {},
    execute: async () => ({ started: true, code: 0, groupAbsent: true }),
    validate: async () => {
      throw Object.assign(new Error("missing manifest"), { code: "ENOENT" });
    },
  });
  assert.equal(finishNativeVariant(variant, missing, 0).status, "failed");
  assert.equal(aggregateNative([], false, cleanup).variants.length, 7);
  assert.ok(aggregateNative([], true, cleanup).variants.every((row) => row.status === "cancelled"));
  const negative = nativeVariants.find((value) => value.id === "negative");
  const safeFailure = {
    id: negative.id,
    safe: true,
    attempted: true,
    status: "failed",
    nativeCode: 1,
  };
  assert.equal(finishNativeVariant(negative, safeFailure, 0).status, "expected-negative");
  assert.equal(
    finishNativeVariant(negative, { ...safeFailure, nativeCode: 0 }, 0).status,
    "failed",
  );
  assert.equal(finishNativeVariant(negative, safeFailure, 1).status, "failed");
});

test("the consumed child controller forwards the exact cancellation signal and awaits close before boundary cleanup", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "native-workflow-signal-"));
  try {
    for (const signal of ["SIGINT", "SIGTERM"]) {
      const events = [],
        signals = new EventEmitter(),
        child = new EventEmitter();
      child.pid = 73;
      child.exitCode = null;
      child.signalCode = null;
      child.stdio = [
        null,
        null,
        null,
        {
          end: (gate) => {
            assert.equal(gate, "1");
            events.push("admitted");
            queueMicrotask(() => signals.emit(signal));
          },
        },
      ];
      let starts = 0;
      const result = await runOwnedNative({
        argv: ["synthetic-only"],
        output: root,
        id: signal,
        signals,
        launch: (_exe, _args, options) => {
          starts++;
          assert.equal(options.detached, true);
          return child;
        },
        captureIdentity: async () => ({ pid: 73, pgid: 73, startTicks: "400" }),
        journal: async () => events.push("identity-journal"),
        absent: () => true,
        signalGroup: (pgid, forwarded) => {
          assert.equal(pgid, 73);
          assert.equal(forwarded, signal);
          events.push(`signal:${forwarded}`);
          queueMicrotask(() => {
            child.signalCode = forwarded;
            events.push("close");
            child.emit("close", null, forwarded);
          });
        },
      });
      events.push("boundary-cleanup");
      assert.equal(starts, 1);
      assert.equal(result.cancelled, signal);
      assert.deepEqual(result.forwarded, [signal]);
      assert.equal(result.code, signal === "SIGINT" ? 130 : 143);
      assert.deepEqual(events, [
        "identity-journal",
        "admitted",
        `signal:${signal}`,
        "close",
        "boundary-cleanup",
      ]);
      assert.equal(signals.listenerCount("SIGINT") + signals.listenerCount("SIGTERM"), 0);
    }
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("the existing namespace owner forwards cancellation before removing its exact registered scratch", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "native-workflow-namespace-"));
  try {
    const events = [],
      signals = new EventEmitter(),
      child = new EventEmitter();
    child.pid = undefined;
    child.exitCode = null;
    child.signalCode = null;
    child.kill = (signal) => {
      events.push(`signal:${signal}`);
      queueMicrotask(() => {
        child.signalCode = signal;
        events.push("close");
        child.emit("close", null, signal);
      });
      return true;
    };
    const run = await allocateRun(root);
    const result = await superviseRun({
      run,
      signals,
      start: () => {
        setImmediate(() => signals.emit("SIGTERM"));
        return child;
      },
      remove: async (owned) => {
        events.push("remove");
        assert.equal(owned, run);
        return removeRun(owned);
      },
    });
    assert.equal(result.code, 143);
    assert.deepEqual(events, ["signal:SIGTERM", "close", "remove"]);
    const registry = await readNativeEvidence(run.registry);
    assert.equal(registry.cleanup, "complete");
    assert.deepEqual(registry.namespaceProcess, result.namespaceProcess);
    await assert.rejects(fs.lstat(run.root), { code: "ENOENT" });
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("the consumed row reader fences an orphan first attempt and rejects evidence lost after a saved safe boundary", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "native-workflow-evidence-"));
  try {
    await fs.writeFile(path.join(root, "positive-started.json"), JSON.stringify({ attempts: 1 }));
    const orphan = await readNativeWorkflowRows(root);
    assert.equal(orphan[0].safe, false);
    assert.match(orphan[0].reason, /reconciliation required/);
    assert.equal(nativeAdmission("positive", orphan), "first-attempt-already-recorded");
    const fixture = boundaryFixture(false);
    const evidence = {};
    for (const [key, file, value] of [
      ["result", "positive-result.json", fixture.result],
      ["manifest", "positive/manifest.json", fixture.manifest],
      ["inner", "positive/inner.json", fixture.inner],
      ["registry", "positive-boundary/registry.json", fixture.record],
    ]) {
      const bytes = JSON.stringify(value);
      await fs.mkdir(path.dirname(path.join(root, file)), { recursive: true });
      await fs.writeFile(path.join(root, file), bytes);
      evidence[key] = { file, sha256: sha256(bytes) };
    }
    await fs.mkdir(path.join(root, "build"));
    const receipt = JSON.stringify(fixture.receipt);
    await fs.writeFile(path.join(root, "build/receipt.json"), receipt);
    evidence.buildReceiptSha256 = sha256(receipt);
    const attempt = {
      id: "positive",
      journey: "sidebar",
      attempted: true,
      safe: true,
      status: "failed",
      oracleCode: null,
      nativeCode: 0,
      runId: fixture.manifest.runId,
      controller: { groupAbsent: true, identity: { pid: 91, pgid: 91, startTicks: "500" } },
      evidence,
    };
    await fs.writeFile(path.join(root, "positive-attempt.json"), JSON.stringify(attempt));
    await fs.writeFile(
      path.join(root, "positive-outcome.json"),
      JSON.stringify(finishNativeVariant(nativeVariants[0], attempt, 0)),
    );
    assert.equal((await readNativeWorkflowRows(root))[0].status, "passed");
    await fs.rm(path.join(root, "positive/inner.json"));
    const missing = (await readNativeWorkflowRows(root))[0];
    assert.equal(missing.safe, false);
    assert.equal(missing.nativeCode, 0);
    assert.equal(missing.status, "failed");
    assert.equal(nativeAdmission("negative", [missing]), "unproved-boundary:positive");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

function workflowStep(yaml, name) {
  const step = yaml.split(`      - name: ${name}\n`)[1];
  assert.ok(step, name);
  return step.split(/\n      - name: /)[0];
}

test("actual YAML keeps one build, exact qualification gates, serial first attempts, unchanged negative oracles and evidence retention", async () => {
  const yaml = await workflow();
  const groups = [
    ["Run JE-01R and its unchanged-oracle negative once each", "sidebar", null],
    ["Verify Electron A and ordinarily paired read-only Node B", "observer", "sidebar"],
    [
      "Verify native thread self-configuration and follow-up continuity",
      "self_configure",
      "observer",
    ],
    ["Run JE-06 and its unchanged-oracle no-edit negative once each", "queue", "self_configure"],
    ["Run JE-03 with private Secret Service and a whole-run deadline", "servers", "queue"],
  ];
  for (const [name, id, previous] of groups) {
    const step = workflowStep(yaml, name);
    assert.ok(step.includes(`id: ${id}\n`));
    const condition = step.split("        if: >-\n")[1].split("        env:")[0];
    assert.ok(condition.includes("!cancelled()"));
    assert.ok(!condition.includes("always()"));
    for (const key of [
      "allocation",
      "candidate",
      "dependencies",
      "pins",
      "prerequisites",
      "runtime",
      "namespaces",
      "harness",
      "build",
      "doctor",
      "negatives",
    ])
      assert.ok(condition.includes(`steps.${key}.outcome == 'success'`), `${id}:${key}`);
    if (previous) assert.ok(condition.includes(`steps.${previous}.outputs.safe == 'true'`));
    assert.ok(!step.includes("continue-on-error"));
    assert.ok(!step.includes("continue-on-error") && !/for\s+attempt\s+in/.test(step));
    if (id !== "servers") {
      assert.ok(step.includes("scripts/jones/ci/native-workflow-evidence.mjs run"));
      assert.ok(step.includes("scripts/jones/ci/native-workflow-evidence.mjs finish"));
      assert.ok(step.includes(`scripts/jones/ci/native-workflow-evidence.mjs gate ${id}`));
      assert.ok(!step.includes("ui-evidence.mjs run"));
    }
  }
  assert.equal((yaml.match(/spawnSync\('vp', \['run', 'build:desktop'\]/g) ?? []).length, 1);
  assert.ok(yaml.includes("for variant in positive negative; do"));
  assert.ok(yaml.includes("for queue_case in workflow no-edit-negative; do"));
  assert.ok(
    yaml.includes("const expected = 'Scenario assertion failed: Backend target is pinned'"),
  );
  assert.ok(
    yaml.includes(
      "const expected = 'Scenario assertion failed: M2 retains exactly the edited text'",
    ),
  );
  for (const oracle of [
    "manifest.assertions.length, 91",
    "manifest.assertions.length, 99",
    "manifest.assertions.length, 25",
    "generations.length, variant === 'positive' ? 3 : 1",
    "assertFullQueueEvidence(fixture)",
    "assertDeliveries(beforeCleanup.ledger, fixture, positive)",
    "managedClosed",
    "credentialFence",
    "encryptedCatalog",
    "twoGenerationsClosed",
    "nativeChecks",
  ])
    assert.ok(yaml.includes(oracle), oracle);
  const servers = workflowStep(yaml, groups[4][0]);
  assert.ok(servers.includes("'wholeRunCapSeconds': 390"));
  assert.ok(servers.includes("--secret-service', 'system'"));
  const signalHandler = servers
    .split("def cancelled(signum, _frame):")[1]
    .split("signal.signal(signal.SIGALRM")[0];
  assert.ok(
    signalHandler.indexOf("signal_owned(signum)") < signalHandler.indexOf("raise RuntimeError"),
  );
  const cleanupStep = workflowStep(yaml, "Verify cleanup of this job's registered runs");
  assert.ok(cleanupStep.includes("if: always()"));
  assert.ok(
    yaml.indexOf("Validate the JE-03 first-attempt boundary") <
      yaml.indexOf("Verify cleanup of this job's registered runs"),
  );
  assert.ok(
    yaml.indexOf("Verify cleanup of this job's registered runs") <
      yaml.indexOf("Aggregate every required first attempt"),
  );
  assert.ok(
    yaml.indexOf("Aggregate every required first attempt") <
      yaml.indexOf("Preserve bounded native evidence"),
  );
  const remove = workflowStep(yaml, "Remove only this job's scratch after evidence upload");
  assert.ok(
    remove.includes("steps.evidence.outcome == 'success' && steps.cleanup.outcome == 'success'"),
  );
  assert.ok(yaml.includes('cron: "45 4 * * *"'));
  assert.ok(yaml.includes("contains(github.event.pull_request.labels.*.name, 'native-e2e')"));
});

test("the YAML's original JE-01R negative oracle accepts its exact failing assertion and rejects changed or missing evidence", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "native-workflow-oracle-"));
  try {
    const yaml = await workflow();
    const step = workflowStep(yaml, "Run JE-01R and its unchanged-oracle negative once each");
    const body = step
      .split("node --input-type=module <<'NODE' || oracle_code=$?\n")[1]
      .split("\n          NODE")[0]
      .split("\n")
      .map((line) => line.replace(/^          /, ""))
      .join("\n");
    const fixture = boundaryFixture();
    fixture.manifest.fixture.variant = "no-action-negative";
    fixture.manifest.resources = { settledIntervalSeconds: 20 };
    await fs.mkdir(path.join(root, "negative"));
    await fs.writeFile(path.join(root, "negative-result.json"), JSON.stringify(fixture.result));
    await fs.writeFile(path.join(root, "negative/manifest.json"), JSON.stringify(fixture.manifest));
    await fs.writeFile(path.join(root, "negative/inner.json"), JSON.stringify(fixture.inner));
    await fs.writeFile(
      path.join(root, "scenario-hashes.json"),
      JSON.stringify({ negative: fixture.manifest.scenario.sha256 }),
    );
    const invoke = () =>
      execFileSync(process.execPath, ["--input-type=module", "-e", body], {
        cwd: repo,
        env: {
          ...process.env,
          JCUE_VARIANT: "negative",
          JCUE_OUTPUT: root,
          JCUE_NATIVE_CODE: "1",
          JCUE_HARNESS_OID: fixture.expected.head,
          JCUE_ELECTRON_VERSION: fixture.expected.electron,
        },
        stdio: "pipe",
      });
    invoke();
    fixture.manifest.assertions[0].name = "A different assertion";
    await fs.writeFile(path.join(root, "negative/manifest.json"), JSON.stringify(fixture.manifest));
    assert.throws(invoke);
    await fs.rm(path.join(root, "negative/inner.json"));
    assert.throws(invoke);
    await assert.rejects(readNativeEvidence(path.join(root, "negative/inner.json")), {
      code: "ENOENT",
    });
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
