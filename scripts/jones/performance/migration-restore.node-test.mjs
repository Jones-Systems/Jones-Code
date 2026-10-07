import * as NodeAssert from "node:assert/strict";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";

import {
  foreignV2Names, historicalQualificationRequirements, migrationRestoreCase, migrationRestoreOptIn,
  migrationRestoreSchema, nativeBackupSchema, originalJonesNames, qualificationCases, runMigrationRestoreCase,
} from "./migration-restore.mjs";
import { qualificationSourcePins } from "./sources.mjs";

NodeTest.test("migration restore V2 case table preserves receiving-loader semantics and the pending successor", () => {
  NodeAssert.equal(migrationRestoreSchema, "jones-performance-migration-restore/v2");
  NodeAssert.equal(nativeBackupSchema, "jones-performance-native-backup/v1");
  NodeAssert.deepEqual(qualificationCases.map(({ id }) => id), [
    "open-e5", "open-414", "open-six", "foreign-lease-007", "foreign-87", "foreign-77", "foreign-91",
    "rollback-injected", "restore-pre", "restore-post", "successor-138",
  ]);
  NodeAssert.equal(migrationRestoreCase("successor-138").prerequisite, "138_JonesThreadCreationLookupIndex");
  NodeAssert.equal(new Set(qualificationCases.map(({ id }) => id)).size, qualificationCases.length);
  NodeAssert.equal(originalJonesNames.length, 6);
  NodeAssert.equal(foreignV2Names.length, 7);
  NodeAssert.ok(!qualificationCases.some(({ id }) => /A007|upgrade-seven|rollback-seven/.test(id)));
});

NodeTest.test("historical cases name exact source revisions; added pins require root-qualified tree and lock", () => {
  for (const requirement of historicalQualificationRequirements) {
    NodeAssert.match(requirement.sourceRevision, /^[a-f0-9]{40}$/);
    const pin = qualificationSourcePins.find(({ directory }) => directory === requirement.directory);
    NodeAssert.ok(pin, `${requirement.directory}: historical qualification pin requires root binding`);
    NodeAssert.equal(pin.sourceRevision, requirement.sourceRevision);
    NodeAssert.match(pin.tree, /^[a-f0-9]{40}$/);
    if (pin.lockSha256 !== undefined) NodeAssert.match(pin.lockSha256, /^[a-f0-9]{64}$/);
  }
  NodeAssert.equal(historicalQualificationRequirements.find(({ directory }) => directory === "lease-current").foreign, "lookup007");
  NodeAssert.equal(historicalQualificationRequirements.find(({ directory }) => directory === "v2-aggregate-77").foreignPrefix, 7);
  NodeAssert.equal(historicalQualificationRequirements.find(({ directory }) => directory === "v2-aggregate-91").foreignPrefix, 5);
});

NodeTest.test("qualification refuses an old A007 case before importing a worker or allocating scratch", async () => {
  await NodeAssert.rejects(runMigrationRestoreCase("rollback-seven", {}), { code: "invalid_case" });
});
NodeTest.test("qualification requires explicit opt-in before binding or allocating scratch", async () => {
  await NodeAssert.rejects(runMigrationRestoreCase("open-e5", {}), { code: "unavailable" });
});
NodeTest.test("opt-in still refuses an unbound receiving candidate before allocating scratch", async () => {
  await NodeAssert.rejects(runMigrationRestoreCase("rollback-injected", { explicitSyntheticRequest: true, candidate: {} }), { code: "invalid_source" });
});
NodeTest.test("cancelled requests refuse before binding or creating scratch", async () => {
  const controller = new AbortController();
  controller.abort();
  await NodeAssert.rejects(runMigrationRestoreCase("rollback-injected", { explicitSyntheticRequest: true, signal: controller.signal }), { name: "AbortError" });
});
NodeTest.test("qualification refuses an unbounded deadline before binding or creating scratch", async () => {
  await NodeAssert.rejects(runMigrationRestoreCase("rollback-injected", { explicitSyntheticRequest: true, timeoutMs: 120001 }), { code: "invalid_options" });
});

const requestPath = process.env[migrationRestoreOptIn];
NodeTest.test("bound synthetic migration restore qualification", {
  skip: requestPath ? false : "explicit synthetic request unbound; historical source parent unbound",
}, async (test) => {
  NodeAssert.ok(NodePath.isAbsolute(requestPath) && NodePath.resolve(requestPath) === requestPath);
  const info = NodeFS.lstatSync(requestPath);
  NodeAssert.ok(info.isFile() && !info.isSymbolicLink() && info.size <= 16 * 1024);
  const request = JSON.parse(NodeFS.readFileSync(requestPath, "utf8"));
  NodeAssert.equal(request.schema, "jones-performance-migration-restore-request/v2");
  NodeAssert.ok(Array.isArray(request.cases) && request.cases.length > 0 && request.cases.length <= qualificationCases.length);
  NodeAssert.equal(new Set(request.cases).size, request.cases.length);
  for (const id of request.cases) migrationRestoreCase(id);
  const { readRuntimeBinding } = await import("./runtime-binding.mjs");
  const runtime = readRuntimeBinding();
  NodeAssert.equal(process.execPath, runtime.executablePath);
  NodeAssert.equal(process.versions.node, runtime.nodeVersion);
  const cancellation = new AbortController();
  let pending;
  try {
    for (const caseId of request.cases) {
      const caseRoot = NodePath.join(request.options.parentPath, `migration-${caseId}-${request.options.binding.runId}`);
      const options = { ...request.options, childName: NodePath.basename(caseRoot), signal: AbortSignal.any([test.signal, cancellation.signal]) };
      pending = runMigrationRestoreCase(caseId, options);
      const evidence = await pending;
      if (evidence.status === "unavailable") { test.diagnostic(JSON.stringify({ caseId, status: evidence.status, reason: evidence.reason })); continue; }
      NodeAssert.equal(evidence.schema, migrationRestoreSchema);
      NodeAssert.equal(evidence.runnerClosed, true);
      NodeAssert.equal(evidence.cleanup.outcome, "complete");
      NodeAssert.equal(evidence.cleanup.absent, true);
      NodeAssert.equal(NodeFS.existsSync(caseRoot), false);
      for (const phase of evidence.phases) NodeAssert.equal(phase.closeKnown, true, `${phase.name}: unknown resource close`);
      for (const backup of evidence.backups) {
        NodeAssert.equal(backup.schema, nativeBackupSchema);
        NodeAssert.equal(backup.backupCompleted, true);
        NodeAssert.equal(backup.sourceClosed, true);
        NodeAssert.equal(backup.sourceCaptureSha256, backup.restoredContentSha256);
        NodeAssert.equal(backup.originalClosedOutput.healthQualification, "unqualified-readonly-health");
      }
      if (caseId.startsWith("foreign-")) {
        NodeAssert.equal(evidence.badHistory.rejected, true);
        NodeAssert.equal(evidence.badHistory.unchanged, true);
        NodeAssert.deepEqual(evidence.foreignGuard, { unchangedForeignContent: true, adoptedForeignFeatures: false });
      }
      if (caseId === "rollback-injected") NodeAssert.equal(evidence.rollback.retrySucceeded, true);
      test.diagnostic(JSON.stringify({ caseId, status: evidence.status, runnerClosed: evidence.runnerClosed, cleanup: evidence.cleanup.outcome, phases: evidence.phases.map(({ name, closeKnown }) => ({ name, closeKnown })) }));
    }
  } finally {
    cancellation.abort();
    if (pending) await pending.catch(() => undefined);
  }
});
