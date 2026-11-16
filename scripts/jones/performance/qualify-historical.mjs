import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  assertHistoricalPreparationReceipt,
  inspectHistoricalPreparationReceipt,
  preparationPins,
} from "./historical-preparation.mjs";
import {
  drainHistoricalImports,
  installHistoricalResolutionGuard,
} from "./historical-resolution.mjs";
import {
  migrationRestoreSchema,
  nativeBackupSchema,
  runMigrationRestoreCase,
} from "./migration-restore.mjs";
import { readRuntimeBinding } from "./runtime-binding.mjs";

export const strictHistoricalCases = Object.freeze(["foreign-87", "receiving-creation-lookup"]);
const hash = (value) => NodeCrypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");
const script = NodeURL.fileURLToPath(import.meta.url);
const checkout = NodePath.resolve(import.meta.dirname, "../../..");
const errorValue = (error) => ({
  code: error.code ?? "qualification_failed",
  message: String(error.message ?? error),
});
export function assertStrictHistoricalResult(caseId, evidence, candidate, sourceParent) {
  NodeAssert.ok(strictHistoricalCases.includes(caseId));
  NodeAssert.equal(evidence?.schema, migrationRestoreSchema);
  NodeAssert.equal(evidence.caseId, caseId);
  NodeAssert.equal(evidence.status, "complete", "skip/unavailable is not qualification");
  NodeAssert.deepEqual(evidence.candidate, candidate);
  NodeAssert.equal(evidence.runnerClosed, true);
  NodeAssert.equal(evidence.cleanup?.outcome, "complete");
  NodeAssert.equal(evidence.cleanup.absent, true);
  NodeAssert.ok(
    evidence.cleanups.length >= 2 &&
      evidence.cleanups.every((item) => item.outcome === "complete" && item.absent === true),
  );
  const selected = caseId === "foreign-87" ? "lease-current" : "lease";
  const sources = ["history", selected].map((directory) => ({
    repository: "Jones-Systems/Jones-Code",
    sourceRevision: preparationPins.find((pin) => pin.directory === directory).sourceRevision,
    worktreePath: NodePath.join(sourceParent, directory),
  }));
  NodeAssert.deepEqual(evidence.sourceIdentity, sources);
  const names = [
    "historical-seed",
    "exact-historical-foreign-loader",
    "receiving-guard-preserves-foreign-content",
    "receiving-loader-and-V1-import",
    "receiving-rerun-idempotent",
    "receiving-V2-codec-refuses-foreign-command-wire",
  ];
  if (caseId === "foreign-87")
    names.push(
      "foreign-negative-history-unknown-ID7-name",
      "receiving-badHistory-refusal-unknown-ID7-name",
      "foreign-negative-history-missing-original-prefix",
      "receiving-badHistory-refusal-missing-original-prefix",
    );
  else names.push("receiving-055-application-birth-lookup");
  NodeAssert.deepEqual(
    evidence.phases.map(({ name }) => name),
    names,
    "all original case phases required in order",
  );
  for (const phase of evidence.phases)
    NodeAssert.equal(
      phase.closeKnown,
      true,
      `${phase.name}: unknown phase/resource/listener closure`,
    );
  NodeAssert.deepEqual(evidence.phases[0].databaseSource, sources[0]);
  NodeAssert.deepEqual(evidence.phases[1].databaseSource, sources[1]);
  for (const phase of evidence.phases.slice(2))
    NodeAssert.deepEqual(phase.databaseSource, candidate);
  const imported = evidence.phases.find(({ name }) => name === "receiving-loader-and-V1-import");
  const repeated = evidence.phases.find(({ name }) => name === "receiving-rerun-idempotent");
  NodeAssert.ok(
    imported.capture?.content && repeated.capture?.content,
    "full import/readback captures required",
  );
  NodeAssert.deepEqual(
    repeated.capture.content,
    imported.capture.content,
    "idempotence capture differs",
  );
  NodeAssert.equal(evidence.backups.length, 1);
  for (const backup of evidence.backups) {
    NodeAssert.equal(backup.schema, nativeBackupSchema);
    NodeAssert.equal(backup.backupCompleted, true);
    NodeAssert.equal(backup.sourceClosed, true);
    NodeAssert.equal(
      backup.closureBasis,
      "awaited-native-backup-completion-and-observed-source-close",
    );
    NodeAssert.match(backup.sourceCaptureSha256, /^[a-f0-9]{64}$/);
    NodeAssert.equal(backup.sourceCaptureSha256, backup.restoredContentSha256);
    NodeAssert.equal(backup.sourceCaptureSha256, hash(backup.capture.content));
    NodeAssert.equal(
      backup.originalClosedOutput.healthQualification,
      "unqualified-readonly-health",
    );
  }
  NodeAssert.deepEqual(evidence.foreignWire, {
    receivingCodec: "OrchestrationV2Command",
    rejected: true,
    unchanged: true,
    dispatched: false,
  });
  NodeAssert.deepEqual(evidence.foreignGuard, {
    unchangedForeignContent: true,
    adoptedForeignFeatures: false,
  });
  NodeAssert.equal(evidence.foreignHistory.featureAdopted, false);
  if (caseId === "foreign-87") {
    NodeAssert.deepEqual(evidence.badHistory, {
      rejected: true,
      unchanged: true,
      variants: ["unknown-ID7-name", "missing-original-prefix"],
    });
  } else {
    NodeAssert.equal(evidence.badHistory, undefined);
    NodeAssert.equal(evidence.successor.owner, "055_OrchestrationV2/RecoveryIndexes");
    NodeAssert.equal(evidence.successor.index, "orchestration_events_v2_created_threads_idx");
    for (const oracle of [
      "actualLookupExecuted",
      "foreignBirthExcluded",
      "changedProjectionRejected",
      "missingIndexRejected",
    ])
      NodeAssert.equal(evidence.successor[oracle], true);
  }
  return evidence;
}
export function assertStrictHistoricalResults(results, candidate, sourceParent) {
  NodeAssert.deepEqual(
    results.map(({ caseId }) => caseId),
    strictHistoricalCases,
  );
  for (const result of results)
    assertStrictHistoricalResult(result.caseId, result, candidate, sourceParent);
  return results;
}
export function historicalQualificationClosureKnown(
  results,
  pendingCase,
  failedEvidence,
  preflight,
) {
  if (
    !preflight?.complete ||
    !preflight.guardClosed ||
    !preflight.batches?.length ||
    preflight.batches.some(
      (batch) => !batch.finished || batch.firstError || batch.launched !== batch.settled,
    ) ||
    (results.length === 0 && pendingCase === undefined)
  )
    return false;
  const closedCase = (result) =>
    result?.runnerClosed === true &&
    result.cleanup?.absent === true &&
    result.phases?.length > 0 &&
    result.phases.every((phase) => phase.closeKnown === true);
  return results.every(closedCase) && (pendingCase === undefined || closedCase(failedEvidence));
}
// Each probe owns all its import promises. Cancellation bounds the caller's wait,
// but cannot cancel module evaluation; an unfinished probe keeps guard/root custody.
export async function consumeHistoricalImportPreflight(probes, preflight, signal) {
  preflight.complete = false;
  preflight.batches = [];
  for (const probe of probes) {
    signal?.throwIfAborted();
    const batch = { launched: 0, settled: 0, finished: false };
    preflight.batches.push(batch);
    const pending = Promise.resolve()
      .then(() => probe(batch))
      .finally(() => {
        batch.finished = true;
      });
    let abort;
    try {
      await Promise.race([
        pending,
        new Promise((_, reject) => {
          abort = () => reject(signal.reason);
          signal?.addEventListener("abort", abort, { once: true });
          if (signal?.aborted) abort();
        }),
      ]);
    } catch (error) {
      // The loader can observe a rejection before its last sibling settles.
      throw batch.firstError ?? error;
    } finally {
      if (abort) signal?.removeEventListener("abort", abort);
    }
  }
  preflight.complete = true;
}
export function finalizeHistoricalImportPreflight(preflight, guard) {
  preflight.importsSettled =
    Boolean(preflight.batches?.length) &&
    preflight.batches.every((batch) => batch.finished && batch.launched === batch.settled);
  preflight.guardClosed = false;
  if (preflight.importsSettled && guard) {
    guard.close();
    preflight.guardClosed = true;
  }
}
function saveEvidence(request, value) {
  const bytes = JSON.stringify(value);
  NodeAssert.ok(
    Buffer.byteLength(bytes) <= request.maxEvidenceBytes,
    "full result exceeds declared evidence bound",
  );
  NodeAssert.equal(NodeFS.realpathSync(request.evidenceParent), request.evidenceParent);
  const path = NodePath.join(
    request.evidenceParent,
    `historical-qualification-${NodeCrypto.randomUUID()}.json`,
  );
  const fd = NodeFS.openSync(path, "wx", 0o600);
  try {
    NodeFS.writeFileSync(fd, bytes);
    NodeFS.fsyncSync(fd);
  } finally {
    NodeFS.closeSync(fd);
  }
  return path;
}
let retainedImportPreflight;
export async function qualifyHistorical(request, signal) {
  NodeAssert.equal(
    retainedImportPreflight,
    undefined,
    "prior import closure unknown; retry refused",
  );
  NodeAssert.equal(request.explicitSyntheticRequest, true);
  NodeAssert.ok(Number.isSafeInteger(request.maxEvidenceBytes) && request.maxEvidenceBytes > 0);
  const preparation = assertHistoricalPreparationReceipt(
    request.preparationReceiptPath,
    request.preparationReceiptSha256,
  );
  const runtime = readRuntimeBinding();
  NodeAssert.equal(process.execPath, runtime.executablePath);
  NodeAssert.equal(process.versions.node, runtime.nodeVersion);
  NodeAssert.equal(runtime.nodeVersion, "26.12.1");
  NodeAssert.ok(request.maxEvidenceBytes <= preparation.bounds.maxOutputBytes);
  NodeAssert.equal(
    request.evidenceParent,
    preparation.creation.evidenceRoot,
    "qualification evidence must stay in the declared output custody",
  );
  const cancellation = new AbortController();
  const deadline = AbortSignal.timeout(preparation.bounds.deadlineMs);
  signal = AbortSignal.any([...(signal ? [signal] : []), cancellation.signal, deadline]);
  let monitorError;
  const monitor = setInterval(() => {
    try {
      inspectHistoricalPreparationReceipt(preparation);
    } catch (error) {
      monitorError = errorValue(error);
      cancellation.abort();
    }
  }, 100);
  const oldParent = process.env.JONES_PERFORMANCE_SOURCE_PARENT;
  process.env.JONES_PERFORMANCE_SOURCE_PARENT = preparation.sourceParent;

  let guard;
  const preflight = { complete: false, batches: [] };
  const results = [],
    imports = [];
  let originalError,
    postconditionError,
    guardError,
    resolution,
    evidencePath,
    evidenceError,
    pendingCase,
    failedEvidence;
  try {
    guard = installHistoricalResolutionGuard({
      receivingRoot: checkout,
      sourceInputs: [
        { root: checkout, lockSha256: preparation.candidate.lockSha256 },
        ...preparation.sources.map(({ source, inputs }) => ({
          root: source.worktreePath,
          lockSha256: inputs["pnpm-lock.yaml"],
          inputs,
        })),
      ],
      historicalRoots: preparation.sources.map(({ source }) => source.worktreePath),
      maxRecords: preparation.bounds.maxEntries,
      maxFileBytes: preparation.bounds.maxFileBytes,
    });
    let probeHistoricalFixtureImports, probeMigrationRestoreImports;
    const sources = Object.fromEntries(
      preparation.sources.map(({ source }) => [NodePath.basename(source.worktreePath), source]),
    );
    await consumeHistoricalImportPreflight(
      [
        async (importState) => {
          const workers = await drainHistoricalImports(
            [
              () => import("./fixtures-historical-worker.mjs"),
              () => import("./migration-restore-worker.mjs"),
            ],
            importState,
          );
          ({ probeHistoricalFixtureImports } = workers[0]);
          ({ probeMigrationRestoreImports } = workers[1]);
        },
        async (importState) =>
          imports.push({
            source: sources.history,
            modules: await probeHistoricalFixtureImports(sources.history, {
              resolutionGuard: guard,
              importState,
            }),
          }),
        ...[sources["lease-current"], sources.lease, preparation.candidate].map(
          (source) => async (importState) =>
            imports.push({
              source,
              modules: await probeMigrationRestoreImports(
                source,
                source === preparation.candidate,
                { resolutionGuard: guard, importState },
              ),
            }),
        ),
      ],
      preflight,
      signal,
    );
    // No database exists until every consumed loader imports under the resolution hook.
    resolution = guard.receipt();
    for (const caseId of strictHistoricalCases) {
      signal?.throwIfAborted();
      assertHistoricalPreparationReceipt(
        request.preparationReceiptPath,
        request.preparationReceiptSha256,
      );
      const info = NodeFS.statfsSync(preparation.creation.root, { bigint: true });
      NodeAssert.ok(
        info.bavail * info.bsize >= BigInt(30 * 1024 ** 3 + preparation.bounds.estimatedPeakBytes),
        "fresh storage admission refused",
      );
      pendingCase = caseId;
      const result = await runMigrationRestoreCase(caseId, {
        ...request.options,
        explicitSyntheticRequest: true,
        candidate: preparation.candidate,
        parentPath: preparation.creation.root,
        childName: `qualification-${caseId}-${NodeCrypto.randomUUID()}`,
        signal,
      });
      results.push(result);
      pendingCase = undefined;
      assertStrictHistoricalResult(caseId, result, preparation.candidate, preparation.sourceParent);
    }
    assertStrictHistoricalResults(results, preparation.candidate, preparation.sourceParent);
    assertHistoricalPreparationReceipt(
      request.preparationReceiptPath,
      request.preparationReceiptSha256,
    );
    resolution = guard.receipt();
  } catch (error) {
    originalError = errorValue(error);
    failedEvidence = error.qualificationEvidence;
  } finally {
    clearInterval(monitor);
    try {
      assertHistoricalPreparationReceipt(
        request.preparationReceiptPath,
        request.preparationReceiptSha256,
      );
    } catch (error) {
      postconditionError = errorValue(error);
    }
    try {
      finalizeHistoricalImportPreflight(preflight, guard);
    } catch (error) {
      guardError = errorValue(error);
    }
    if (!preflight.guardClosed) retainedImportPreflight = { guard, preflight };
    if (preflight.guardClosed) {
      if (oldParent === undefined) delete process.env.JONES_PERFORMANCE_SOURCE_PARENT;
      else process.env.JONES_PERFORMANCE_SOURCE_PARENT = oldParent;
    }
  }
  const consumerClosed = historicalQualificationClosureKnown(
    results,
    pendingCase,
    failedEvidence,
    preflight,
  );
  const report = {
    schema: "jones-strict-historical-qualification/v1",
    outcome:
      originalError || postconditionError || monitorError || guardError ? "failed" : "complete",
    preparationReceiptSha256: request.preparationReceiptSha256,
    candidate: preparation.candidate,
    runtime,
    imports,
    importPreflight: {
      complete: preflight.complete,
      importsSettled: preflight.importsSettled,
      guardClosed: preflight.guardClosed,
      sourceParentRetained: !preflight.guardClosed,
      batches: preflight.batches.map(({ launched, settled, finished, firstError }) => ({
        launched,
        settled,
        finished,
        ...(firstError ? { failure: errorValue(firstError) } : {}),
      })),
    },
    resolution,
    results,
    failedEvidence,
    originalError,
    postconditionError,
    monitorError,
    guardError,
    consumerClosed,
    packagedBuild: "unqualified",
  };
  try {
    evidencePath = saveEvidence(request, report);
  } catch (error) {
    evidenceError = errorValue(error);
  }
  return {
    consumerClosed: consumerClosed && Boolean(evidencePath),
    value: report,
    originalError: originalError ?? postconditionError ?? monitorError ?? guardError,
    evidenceError,
    evidencePath,
  };
}
if (process.argv[1] && NodePath.resolve(process.argv[1]) === script) {
  if (process.argv.length === 2 || process.argv[2] === "--help")
    process.stdout.write(
      `${JSON.stringify({ schema: "jones-strict-historical-qualification/v1", cases: strictHistoricalCases, status: "unavailable", reason: "explicit preparation receipt, receiving runtime and synthetic request required" })}\n`,
    );
  else {
    NodeAssert.ok(process.argv.length === 4 && process.argv[2] === "--request");
    const info = NodeFS.lstatSync(process.argv[3]);
    NodeAssert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 16384);
    const abort = new AbortController(),
      cancel = () => abort.abort();
    process.once("SIGINT", cancel);
    process.once("SIGTERM", cancel);
    try {
      const result = await qualifyHistorical(
        JSON.parse(NodeFS.readFileSync(process.argv[3], "utf8")),
        abort.signal,
      );
      process.stdout.write(`${JSON.stringify(result)}\n`);
      process.exitCode =
        !result.originalError && !result.evidenceError && result.value.outcome === "complete"
          ? 0
          : 1;
    } finally {
      process.removeListener("SIGINT", cancel);
      process.removeListener("SIGTERM", cancel);
    }
  }
}
