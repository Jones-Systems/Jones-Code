import assert from "node:assert/strict";
import { AssertionError } from "node:assert";
import fs from "node:fs/promises";
import path from "node:path";
import { validateManifest } from "./manifest.mjs";
import { collectProvenance, sha256 } from "./provenance.mjs";
import { processIdentity } from "./lifecycle.mjs";

export function classifyNativeFailure(phase, error, assertions = [], steps = []) {
  if (phase !== "scenario") return "unknown";
  const failed = assertions.filter(item => item.status === "failed");
  if (error?.constructor === Error && failed.length === 1 &&
    error.message === `Scenario assertion failed: ${failed[0].name}`) return "assertion-failed";
  return error instanceof AssertionError && error.code === "ERR_ASSERTION" && failed.length === 0 &&
    steps.some(step => step.status === "failed") ? "assertion-failed" : "unknown";
}

const identity = value => Number.isSafeInteger(value?.pid) && value.pid > 0 &&
  /^\d+$/.test(value.startTicks ?? "");

// Safety deliberately has no successful-journey assertion or generation count.
// Those remain in each original oracle; a partial journey must close every
// generation it actually opened, including its auxiliary service lifetimes.
export function validateNativeBoundary({ result, manifest, inner, record, receipt,
  receiptSha256, current, expected, parent }) {
  validateManifest(manifest);
  const runId = manifest.runId;
  assert.match(runId, /^[a-f0-9-]{36}$/);
  assert.equal(result.runId, runId);
  assert.equal(result.artifacts, expected.artifacts);
  assert.equal(record.id, runId);
  assert.equal(record.parent, parent);
  assert.equal(record.registry, path.join(parent, "registry", `${runId}.json`));
  assert.equal(path.dirname(record.root), path.join(parent, "scratch"));
  assert.ok(path.basename(record.root).startsWith(`${runId}-`));
  assert.equal(record.cleanup, "complete");
  assert.equal(record.pid, null);
  assert.deepEqual(record.namespaceProcess, manifest.namespaceProcess);
  assert.ok(identity(manifest.namespaceProcess), "Missing namespace capture");
  assert.equal(manifest.namespaceProcess.exited, true);
  for (const value of [result, manifest]) {
    assert.equal(value.cleanup.outcome, "complete");
    assert.equal(value.cleanup.rootAbsent, true);
  }
  assert.equal(receipt.schema, "jones-code-ui-evidence-build/v1");
  assert.equal(receipt.source.head, expected.head);
  assert.equal(receipt.source.statusSha256, sha256(""));
  assert.equal(receipt.source.patchSha256, sha256(""));
  assert.equal(receipt.source.untrackedSha256, sha256("[]"));
  for (const value of [manifest, current]) {
    assert.equal(value.source.head, expected.head);
    assert.equal(value.harness.head, expected.head);
    assert.equal(value.source.clean, true);
    assert.equal(value.harness.clean, true);
    assert.equal(value.harness.statusSha256, sha256(""));
    assert.equal(value.source.statusSha256, sha256(""));
    assert.equal(value.source.patch.sha256, sha256(""));
    assert.equal(value.source.untracked.sha256, sha256("[]"));
    assert.equal(value.build.sourceCorrespondence.status, "receipt-matched");
    assert.equal(value.build.sourceCorrespondence.receipt.sha256, receiptSha256);
    for (const name of ["desktop", "server", "web", "boot"])
      assert.equal(value.build[name].sha256, receipt.build[name]);
  }
  assert.equal(manifest.scenario.path, expected.scenario);
  assert.equal(manifest.scenario.id, "custom");
  assert.equal(manifest.scenario.sha256, expected.scenarioSha256);
  assert.equal(inner.boundary.scenarioSha256, expected.scenarioSha256);
  assert.equal(manifest.scenario.queueCase, expected.queueCase);
  assert.equal(inner.runtime.electron, expected.electron);
  assert.equal(manifest.runtime.electron, expected.electron);
  assert.equal(inner.isolation.status, "passed");
  assert.deepEqual(manifest.assertions, inner.assertions);
  assert.deepEqual(manifest.scenario.steps, inner.steps);
  assert.deepEqual(manifest.fixture, inner.fixture);
  assert.deepEqual(manifest.boundary, inner.boundary);
  assert.equal(inner.boundary.schema, "jones-native-boundary/v1");
  assert.equal(inner.boundary.teardown, "complete");
  assert.equal(inner.pendingElectronLaunch, false);
  for (const key of ["closeError", "startupDiagnosticsError", "secretServiceTeardownError",
    "secretServiceCancellationCleanup", "auxiliaryCloseError"])
    assert.equal(inner[key], undefined, `Unproved cleanup: ${key}`);
  assert.ok(inner.processGenerations.length > 0);
  for (const [index, generation] of inner.processGenerations.entries()) {
    assert.equal(generation.index, index);
    assert.equal(generation.closeAttempted, true);
    assert.equal(generation.closed, true);
    assert.equal(generation.closeError, undefined);
    assert.equal(generation.exit.code, 0);
    assert.equal(generation.exit.signal, null);
    for (const role of ["electron", "server"])
      assert.ok(identity(generation.processes[role]), `Missing ${role} generation capture`);
  }
  assert.equal(inner.boundary.xvfbClosed, true);
  if (inner.windowManager) assert.equal(inner.windowManager.closed, true);
  assert.equal(inner.fixture?.nodeCleanupFailed, undefined);
  if (inner.fixture?.nodeClient)
    for (const key of ["removed", "credentialsCleared", "projectionCleared", "socketClosed"])
      assert.equal(inner.fixture.nodeClient[key], true, `Node client cleanup unproved: ${key}`);
  if (expected.queueCase) {
    assert.equal(inner.fixture.queueCase, expected.queueCase);
    assert.match(inner.fixture.runId ?? "", /^[a-f0-9-]{36}$/);
    assert.equal(inner.fixture.stubCleanup.closed, true);
  }
  if (expected.secretService) {
    assert.equal(inner.secretService.status, "closed");
    assert.equal(inner.secretService.cancelled, null);
    assert.ok(inner.secretService.services.length > 0);
    for (const service of inner.secretService.services) {
      assert.ok(identity(service));
      assert.equal(service.closed, true);
      assert.equal(service.exited, true);
      assert.equal(service.error, false);
    }
  } else assert.equal(inner.secretService, undefined);
  assert.equal(result.code, expected.exitCode);
  assert.equal(inner.status, result.status);
  assert.equal(manifest.status, result.status);
  assert.equal(manifest.error, result.error);
  assert.equal(inner.error ?? null, result.error);
  if (result.status === "passed") {
    assert.equal(result.code, 0);
    assert.equal(inner.boundary.kind, "passed");
  } else {
    assert.equal(result.status, "failed");
    assert.equal(result.code, 1);
    assert.equal(inner.boundary.kind, "assertion-failed");
    if (classifyNativeFailure(inner.boundary.phase, new Error(inner.error), inner.assertions) !== "assertion-failed") {
      assert.equal(inner.boundary.phase, "scenario");
      assert.equal(inner.boundary.failure.name, "AssertionError");
      assert.equal(inner.boundary.failure.code, "ERR_ASSERTION");
      assert.ok(inner.assertions.every(item => item.status === "passed"));
      const failed = inner.steps.filter(step => step.status === "failed");
      assert.ok(failed.length > 0);
      assert.equal(failed.at(-1).name, inner.boundary.failure.step);
    }
  }
  return { safe: true, runId, kind: inner.boundary.kind };
}

async function readNativeCapture(file) {
  const stat = await fs.lstat(file);
  assert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.size <= 2 * 1024 * 1024,
    "Native evidence must be a bounded regular file");
  const bytes = await fs.readFile(file);
  assert.ok(bytes.length <= 2 * 1024 * 1024, "Native evidence grew beyond its bound");
  return { value: JSON.parse(bytes), sha256: sha256(bytes) };
}

export async function readNativeEvidence(file) {
  return (await readNativeCapture(file)).value;
}

export async function validateNativeBoundaryFiles(expected) {
  const { output, artifacts, source, stateHome } = expected;
  const parent = path.join(stateHome, "jones-code-ui-evidence");
  assert.equal(await fs.realpath(parent), parent);
  assert.equal(await fs.realpath(path.join(parent, "registry")), path.join(parent, "registry"));
  const captured = {};
  const read = async (key, file) => {
    const capture = await readNativeCapture(file);
    captured[key] = { file: path.relative(output, file), sha256: capture.sha256 };
    return capture.value;
  };
  const result = await read("result", path.join(output, `${expected.id}-result.json`));
  assert.match(result.runId ?? "", /^[a-f0-9-]{36}$/);
  const manifest = await read("manifest", path.join(artifacts, "manifest.json"));
  const inner = await read("inner", path.join(artifacts, "inner.json"));
  const registry = await readNativeCapture(path.join(parent, "registry", `${result.runId}.json`));
  const record = registry.value;
  const receiptPath = path.join(output, "build/receipt.json");
  const receipt = await readNativeEvidence(receiptPath);
  const receiptSha256 = sha256(await fs.readFile(receiptPath));
  const boundaryArtifacts = path.join(output, `${expected.id}-boundary`);
  await fs.mkdir(boundaryArtifacts, { mode: 0o700 });
  const registryCopy = path.join(boundaryArtifacts, "registry.json");
  await fs.writeFile(registryCopy, JSON.stringify(record), { flag: "wx", mode: 0o600 });
  captured.registry = { file: path.relative(output, registryCopy), sha256: sha256(JSON.stringify(record)) };
  const current = await collectProvenance({ source, harness: source, artifacts: boundaryArtifacts,
    buildReceipt: receiptPath, buildPaths: { desktop: path.join(source, "apps/desktop/dist-electron"),
      server: path.join(source, "apps/server/dist"), web: path.join(source, "apps/server/dist/client") } });
  const proof = validateNativeBoundary({ result, manifest, inner, record, receipt,
    receiptSha256, current, expected, parent });
  if (expected.queueCase) {
    const fixture = inner.fixture;
    const ledger = await readNativeEvidence(path.join(artifacts, `je06-${fixture.runId}-ledger.json`));
    assert.equal(ledger.runId, fixture.runId);
    assert.deepEqual(ledger.cleanup, fixture.stubCleanup);
    assert.ok(ledger.ledger.every(row => row.runId === fixture.runId));
    const starts = ledger.cleanup.starts;
    assert.ok(Array.isArray(starts));
    assert.equal(ledger.ledger.filter(row => row.event === "start").length, starts.length);
    assert.equal(new Set(starts.map(row => row.processId)).size, starts.length);
    for (const start of starts) {
      assert.ok(identity(start));
      assert.equal(ledger.ledger.filter(row => row.event === "start" && row.processId === start.processId &&
        row.pid === start.pid && row.startTicks === start.startTicks).length, 1);
      assert.equal(ledger.ledger.filter(row => row.event === "close" && row.processId === start.processId).length, 1);
    }
  }
  await assert.rejects(fs.lstat(record.root), { code: "ENOENT" });
  assert.deepEqual(await fs.readdir(path.join(parent, "scratch")), [], "Unaccounted scratch roots");
  assert.notEqual(await processIdentity(manifest.namespaceProcess.pid), manifest.namespaceProcess.startTicks);
  return { ...proof, evidence: { ...captured, buildReceiptSha256: receiptSha256 } };
}
