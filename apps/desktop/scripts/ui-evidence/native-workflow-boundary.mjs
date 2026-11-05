import * as NodeAssert from "node:assert";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { validateManifest } from "./manifest.mjs";
import { collectProvenance, sha256 } from "./provenance.mjs";
import { processIdentity } from "./lifecycle.mjs";

export function classifyNativeFailure(phase, error, assertions = [], steps = []) {
  if (phase !== "scenario") return "unknown";
  const failed = assertions.filter((item) => item.status === "failed");
  if (
    error?.constructor === Error &&
    failed.length === 1 &&
    error.message === `Scenario assertion failed: ${failed[0].name}`
  )
    return "assertion-failed";
  return error instanceof NodeAssert.AssertionError &&
    error.code === "ERR_ASSERTION" &&
    failed.length === 0 &&
    steps.some((step) => step.status === "failed")
    ? "assertion-failed"
    : "unknown";
}

const identity = (value) =>
  Number.isSafeInteger(value?.pid) && value.pid > 0 && /^\d+$/.test(value.startTicks ?? "");

// Safety deliberately has no successful-journey assertion or generation count.
// Those remain in each original oracle; a partial journey must close every
// generation it actually opened, including its auxiliary service lifetimes.
export function validateNativeBoundary({
  result,
  manifest,
  inner,
  record,
  receipt,
  receiptSha256,
  current,
  expected,
  parent,
}) {
  validateManifest(manifest);
  const runId = manifest.runId;
  NodeAssert.strict.match(runId, /^[a-f0-9-]{36}$/);
  NodeAssert.strict.equal(result.runId, runId);
  NodeAssert.strict.equal(result.artifacts, expected.artifacts);
  NodeAssert.strict.equal(record.id, runId);
  NodeAssert.strict.equal(record.parent, parent);
  NodeAssert.strict.equal(record.registry, NodePath.join(parent, "registry", `${runId}.json`));
  NodeAssert.strict.equal(NodePath.dirname(record.root), NodePath.join(parent, "scratch"));
  NodeAssert.strict.ok(NodePath.basename(record.root).startsWith(`${runId}-`));
  NodeAssert.strict.equal(record.cleanup, "complete");
  NodeAssert.strict.equal(record.pid, null);
  NodeAssert.strict.deepEqual(record.namespaceProcess, manifest.namespaceProcess);
  NodeAssert.strict.ok(identity(manifest.namespaceProcess), "Missing namespace capture");
  NodeAssert.strict.equal(manifest.namespaceProcess.exited, true);
  for (const value of [result, manifest]) {
    NodeAssert.strict.equal(value.cleanup.outcome, "complete");
    NodeAssert.strict.equal(value.cleanup.rootAbsent, true);
  }
  NodeAssert.strict.equal(receipt.schema, "jones-code-ui-evidence-build/v1");
  NodeAssert.strict.equal(receipt.source.head, expected.head);
  NodeAssert.strict.equal(receipt.source.statusSha256, sha256(""));
  NodeAssert.strict.equal(receipt.source.patchSha256, sha256(""));
  NodeAssert.strict.equal(receipt.source.untrackedSha256, sha256("[]"));
  for (const value of [manifest, current]) {
    NodeAssert.strict.equal(value.source.head, expected.head);
    NodeAssert.strict.equal(value.harness.head, expected.head);
    NodeAssert.strict.equal(value.source.clean, true);
    NodeAssert.strict.equal(value.harness.clean, true);
    NodeAssert.strict.equal(value.harness.statusSha256, sha256(""));
    NodeAssert.strict.equal(value.source.statusSha256, sha256(""));
    NodeAssert.strict.equal(value.source.patch.sha256, sha256(""));
    NodeAssert.strict.equal(value.source.untracked.sha256, sha256("[]"));
    NodeAssert.strict.equal(value.build.sourceCorrespondence.status, "receipt-matched");
    NodeAssert.strict.equal(value.build.sourceCorrespondence.receipt.sha256, receiptSha256);
    for (const name of ["desktop", "server", "web", "boot"])
      NodeAssert.strict.equal(value.build[name].sha256, receipt.build[name]);
  }
  NodeAssert.strict.equal(manifest.scenario.path, expected.scenario);
  NodeAssert.strict.equal(manifest.scenario.id, "custom");
  NodeAssert.strict.equal(manifest.scenario.sha256, expected.scenarioSha256);
  NodeAssert.strict.equal(inner.boundary.scenarioSha256, expected.scenarioSha256);
  NodeAssert.strict.equal(manifest.scenario.queueCase, expected.queueCase);
  NodeAssert.strict.equal(inner.runtime.electron, expected.electron);
  NodeAssert.strict.equal(manifest.runtime.electron, expected.electron);
  NodeAssert.strict.equal(inner.isolation.status, "passed");
  NodeAssert.strict.deepEqual(manifest.assertions, inner.assertions);
  NodeAssert.strict.deepEqual(manifest.scenario.steps, inner.steps);
  NodeAssert.strict.deepEqual(manifest.fixture, inner.fixture);
  NodeAssert.strict.deepEqual(manifest.boundary, inner.boundary);
  NodeAssert.strict.equal(inner.boundary.schema, "jones-native-boundary/v1");
  NodeAssert.strict.equal(inner.boundary.teardown, "complete");
  NodeAssert.strict.equal(inner.pendingElectronLaunch, false);
  for (const key of [
    "closeError",
    "startupDiagnosticsError",
    "secretServiceTeardownError",
    "secretServiceCancellationCleanup",
    "auxiliaryCloseError",
  ])
    NodeAssert.strict.equal(inner[key], undefined, `Unproved cleanup: ${key}`);
  NodeAssert.strict.ok(inner.processGenerations.length > 0);
  for (const [index, generation] of inner.processGenerations.entries()) {
    NodeAssert.strict.equal(generation.index, index);
    NodeAssert.strict.equal(generation.closeAttempted, true);
    NodeAssert.strict.equal(generation.closed, true);
    NodeAssert.strict.equal(generation.closeError, undefined);
    NodeAssert.strict.equal(generation.exit.code, 0);
    NodeAssert.strict.equal(generation.exit.signal, null);
    for (const role of ["electron", "server"])
      NodeAssert.strict.ok(identity(generation.processes[role]), `Missing ${role} generation capture`);
  }
  NodeAssert.strict.equal(inner.boundary.xvfbClosed, true);
  if (inner.windowManager) NodeAssert.strict.equal(inner.windowManager.closed, true);
  NodeAssert.strict.equal(inner.fixture?.nodeCleanupFailed, undefined);
  if (inner.fixture?.nodeClient)
    for (const key of ["removed", "credentialsCleared", "projectionCleared", "socketClosed"])
      NodeAssert.strict.equal(inner.fixture.nodeClient[key], true, `Node client cleanup unproved: ${key}`);
  if (expected.queueCase) {
    NodeAssert.strict.equal(inner.fixture.queueCase, expected.queueCase);
    NodeAssert.strict.match(inner.fixture.runId ?? "", /^[a-f0-9-]{36}$/);
    NodeAssert.strict.equal(inner.fixture.stubCleanup.closed, true);
  }
  if (expected.secretService) {
    NodeAssert.strict.equal(inner.secretService.status, "closed");
    NodeAssert.strict.equal(inner.secretService.cancelled, null);
    NodeAssert.strict.ok(inner.secretService.services.length > 0);
    for (const service of inner.secretService.services) {
      NodeAssert.strict.ok(identity(service));
      NodeAssert.strict.equal(service.closed, true);
      NodeAssert.strict.equal(service.exited, true);
      NodeAssert.strict.equal(service.error, false);
    }
  } else NodeAssert.strict.equal(inner.secretService, undefined);
  NodeAssert.strict.equal(result.code, expected.exitCode);
  NodeAssert.strict.equal(inner.status, result.status);
  NodeAssert.strict.equal(manifest.status, result.status);
  NodeAssert.strict.equal(manifest.error, result.error);
  NodeAssert.strict.equal(inner.error ?? null, result.error);
  if (result.status === "passed") {
    NodeAssert.strict.equal(result.code, 0);
    NodeAssert.strict.equal(inner.boundary.kind, "passed");
  } else {
    NodeAssert.strict.equal(result.status, "failed");
    NodeAssert.strict.equal(result.code, 1);
    NodeAssert.strict.equal(inner.boundary.kind, "assertion-failed");
    if (
      classifyNativeFailure(inner.boundary.phase, new Error(inner.error), inner.assertions) !==
      "assertion-failed"
    ) {
      NodeAssert.strict.equal(inner.boundary.phase, "scenario");
      NodeAssert.strict.equal(inner.boundary.failure.name, "AssertionError");
      NodeAssert.strict.equal(inner.boundary.failure.code, "ERR_ASSERTION");
      NodeAssert.strict.ok(inner.assertions.every((item) => item.status === "passed"));
      const failed = inner.steps.filter((step) => step.status === "failed");
      NodeAssert.strict.ok(failed.length > 0);
      NodeAssert.strict.equal(failed.at(-1).name, inner.boundary.failure.step);
    }
  }
  return { safe: true, runId, kind: inner.boundary.kind };
}

async function readNativeCapture(file) {
  const stat = await NodeFSP.lstat(file);
  NodeAssert.strict.ok(
    stat.isFile() && !stat.isSymbolicLink() && stat.size <= 2 * 1024 * 1024,
    "Native evidence must be a bounded regular file",
  );
  const bytes = await NodeFSP.readFile(file);
  NodeAssert.strict.ok(bytes.length <= 2 * 1024 * 1024, "Native evidence grew beyond its bound");
  return { value: JSON.parse(bytes), sha256: sha256(bytes) };
}

export async function readNativeEvidence(file) {
  return (await readNativeCapture(file)).value;
}

export async function validateNativeBoundaryFiles(expected) {
  const { output, artifacts, source, stateHome } = expected;
  const parent = NodePath.join(stateHome, "jones-code-ui-evidence");
  NodeAssert.strict.equal(await NodeFSP.realpath(parent), parent);
  NodeAssert.strict.equal(await NodeFSP.realpath(NodePath.join(parent, "registry")), NodePath.join(parent, "registry"));
  const captured = {};
  const read = async (key, file) => {
    const capture = await readNativeCapture(file);
    captured[key] = { file: NodePath.relative(output, file), sha256: capture.sha256 };
    return capture.value;
  };
  const result = await read("result", NodePath.join(output, `${expected.id}-result.json`));
  NodeAssert.strict.match(result.runId ?? "", /^[a-f0-9-]{36}$/);
  const manifest = await read("manifest", NodePath.join(artifacts, "manifest.json"));
  const inner = await read("inner", NodePath.join(artifacts, "inner.json"));
  const registry = await readNativeCapture(NodePath.join(parent, "registry", `${result.runId}.json`));
  const record = registry.value;
  const receiptPath = NodePath.join(output, "build/receipt.json");
  const receipt = await readNativeEvidence(receiptPath);
  const receiptSha256 = sha256(await NodeFSP.readFile(receiptPath));
  const boundaryArtifacts = NodePath.join(output, `${expected.id}-boundary`);
  await NodeFSP.mkdir(boundaryArtifacts, { mode: 0o700 });
  const registryCopy = NodePath.join(boundaryArtifacts, "registry.json");
  await NodeFSP.writeFile(registryCopy, JSON.stringify(record), { flag: "wx", mode: 0o600 });
  captured.registry = {
    file: NodePath.relative(output, registryCopy),
    sha256: sha256(JSON.stringify(record)),
  };
  const current = await collectProvenance({
    source,
    harness: source,
    artifacts: boundaryArtifacts,
    buildReceipt: receiptPath,
    buildPaths: {
      desktop: NodePath.join(source, "apps/desktop/dist-electron"),
      server: NodePath.join(source, "apps/server/dist"),
      web: NodePath.join(source, "apps/server/dist/client"),
    },
  });
  const proof = validateNativeBoundary({
    result,
    manifest,
    inner,
    record,
    receipt,
    receiptSha256,
    current,
    expected,
    parent,
  });
  if (expected.queueCase) {
    const fixture = inner.fixture;
    const ledger = await readNativeEvidence(
      NodePath.join(artifacts, `je06-${fixture.runId}-ledger.json`),
    );
    NodeAssert.strict.equal(ledger.runId, fixture.runId);
    NodeAssert.strict.deepEqual(ledger.cleanup, fixture.stubCleanup);
    NodeAssert.strict.ok(ledger.ledger.every((row) => row.runId === fixture.runId));
    const starts = ledger.cleanup.starts;
    NodeAssert.strict.ok(Array.isArray(starts));
    NodeAssert.strict.equal(ledger.ledger.filter((row) => row.event === "start").length, starts.length);
    NodeAssert.strict.equal(new Set(starts.map((row) => row.processId)).size, starts.length);
    for (const start of starts) {
      NodeAssert.strict.ok(identity(start));
      NodeAssert.strict.equal(
        ledger.ledger.filter(
          (row) =>
            row.event === "start" &&
            row.processId === start.processId &&
            row.pid === start.pid &&
            row.startTicks === start.startTicks,
        ).length,
        1,
      );
      NodeAssert.strict.equal(
        ledger.ledger.filter((row) => row.event === "close" && row.processId === start.processId)
          .length,
        1,
      );
    }
  }
  await NodeAssert.strict.rejects(NodeFSP.lstat(record.root), { code: "ENOENT" });
  NodeAssert.strict.deepEqual(await NodeFSP.readdir(NodePath.join(parent, "scratch")), [], "Unaccounted scratch roots");
  NodeAssert.strict.notEqual(
    await processIdentity(manifest.namespaceProcess.pid),
    manifest.namespaceProcess.startTicks,
  );
  return { ...proof, evidence: { ...captured, buildReceiptSha256: receiptSha256 } };
}
