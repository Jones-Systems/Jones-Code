import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";

import {
  qualificationCases,
  qualificationProducerIdentity,
  pinnedHead,
  runMigrationRestoreCase,
} from "./migration-restore-worker.mjs";
import { createOwnedRoot, disposeOwnedRoot } from "./guard.mjs";
import { qualificationDatabaseSource } from "./sources.mjs";

const directory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const worktree = NodePath.resolve(directory, "../..");
const candidateRevision = "da5f4aee0035beec471b38598eaa2857d1e5155c";
const expectedForkNames = [
  "WorktreeOwnershipLeases",
  "ProjectionThreadRuntimeIdentity",
  "NativeCreationIntents",
  "NativeCreationCommandIdentities",
  "WorkstreamsNativeAttempts",
  "WorkstreamsProviderEnrollments",
  "ThreadCreationLookupIndex",
];
const expectedSeeds = {
  e5: {
    revision: "e5a31aceec91484b64315c63dcce80f6e7581604",
    worktreePath: qualificationDatabaseSource("e5a31aceec91484b64315c63dcce80f6e7581604")
      .worktreePath,
    forkCount: 4,
  },
  live: {
    revision: "414bb8da204c3275cd0b76b2ec4d74dfb09a97e4",
    worktreePath: qualificationDatabaseSource("414bb8da204c3275cd0b76b2ec4d74dfb09a97e4")
      .worktreePath,
    forkCount: 2,
  },
  six: {
    revision: "c4c68bb0b33eafb72545e6e23b0b7258e49bd613",
    worktreePath: qualificationDatabaseSource("c4c68bb0b33eafb72545e6e23b0b7258e49bd613")
      .worktreePath,
    forkCount: 6,
  },
};
const sha256 = (value) =>
  NodeCrypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");

async function unknownCloseCounterexample(test) {
  const outer = await NodeFSP.mkdtemp(NodePath.join(directory, ".migration-close-test-"));
  const identity = await NodeFSP.lstat(outer, { bigint: true });
  const policy = {
    homePath: worktree,
    worktreePaths: [worktree],
    protectedPaths: [],
    maxFiles: 32,
    maxFileBytes: 32 * 1024 * 1024,
    maxTotalBytes: 64 * 1024 * 1024,
    maxReceiptBytes: 24 * 1024,
  };
  const owner = createOwnedRoot({
    parentPath: outer,
    childName: "original-test-enclosure",
    policy,
    binding: {
      repository: "Jones-Systems/Jones-Code",
      sourceRevision: qualificationProducerIdentity().sourceRevision,
      taskRef: "spec.jones-performance-portfolio#task.e-migrate.001",
      runId: NodeCrypto.randomUUID(),
    },
  });
  const prepare = NodeSqlite.DatabaseSync.prototype.prepare;
  const close = NodeSqlite.DatabaseSync.prototype.close;
  const resources = new Set();
  const nativeClosed = new Set();
  const rejection = new Error(
    "declared rejection after independently observed actual native close",
  );
  test.mock.method(NodeSqlite.DatabaseSync.prototype, "prepare", function (sql) {
    resources.add(this);
    return prepare.call(this, sql);
  });
  test.mock.method(NodeSqlite.DatabaseSync.prototype, "close", function () {
    close.call(this);
    nativeClosed.add(this);
    throw rejection;
  });
  let failure;
  let pending;
  let captured;
  try {
    pending = runMigrationRestoreCase("upgrade-e5", {
      parentPath: owner.creationReceipt.canonicalRootPath,
      childName: "case",
      runId: NodeCrypto.randomUUID(),
      policy,
      signal: test.signal,
    });
    captured = await pending.then(
      () => {
        throw new Error("unknown close was unexpectedly accepted");
      },
      (error) => error,
    );
    const evidence = captured.qualificationEvidence;
    NodeAssert.equal(evidence.cleanup.outcome, "retained");
    NodeAssert.equal(evidence.cleanup.reason, "unknown_resource_or_child_close");
    NodeAssert.equal(evidence.phases[0].closeKnown, false);
    NodeAssert.equal(NodeFS.existsSync(evidence.creationReceipt.canonicalRootPath), true);
    NodeAssert.deepEqual(evidence.backups, []);
    NodeAssert.ok(evidence.phases[0].failure);
  } catch (error) {
    failure = error;
  } finally {
    if (pending) await pending.catch(() => {});
    test.mock.restoreAll();
    try {
      // This injected rejection follows saved native close. A genuine unknown has no such witness.
      NodeAssert.ok(captured?.qualificationEvidence);
      NodeAssert.equal(resources.size, 1);
      NodeAssert.equal(nativeClosed.size, resources.size);
      NodeAssert.ok([...resources].every((resource) => nativeClosed.has(resource)));
      const cleanup = disposeOwnedRoot(owner);
      NodeAssert.equal(cleanup.outcome, "complete");
      NodeAssert.equal(cleanup.absent, true);
      NodeAssert.deepEqual(await NodeFSP.readdir(outer), []);
      const current = await NodeFSP.lstat(outer, { bigint: true });
      NodeAssert.equal(current.dev, identity.dev);
      NodeAssert.equal(current.ino, identity.ino);
      await NodeFSP.rmdir(outer);
      test.diagnostic(
        JSON.stringify({
          counterexample: "private-unknown-close-retains",
          producerCloseKnown: false,
          nativeCloseIndependentlyWitnessed: true,
          originalTestCreatorCleanup: cleanup.outcome,
          outerAbsent: !NodeFS.existsSync(outer),
        }),
      );
    } catch (error) {
      failure = failure
        ? new AggregateError([failure, error], "Counterexample and retained original enclosure", {
            cause: failure,
          })
        : error;
    }
  }
  if (failure) throw failure;
}

async function withCase(test, specification, inspect) {
  const outer = await NodeFSP.mkdtemp(NodePath.join(directory, ".migration-restore-test-"));
  const identity = await NodeFSP.lstat(outer, { bigint: true });
  const caseRoot = NodePath.join(outer, "case");
  const cancellation = new AbortController();
  let pending;
  let evidence;
  let failure;
  try {
    pending = runMigrationRestoreCase(specification.id, {
      parentPath: outer,
      childName: "case",
      runId: NodeCrypto.randomUUID(),
      signal: AbortSignal.any([test.signal, cancellation.signal]),
      policy: {
        homePath: worktree,
        worktreePaths: [worktree],
        protectedPaths: [NodePath.join(outer, "synthetic-protected.sqlite")],
        maxFiles: 32,
        maxFileBytes: 32 * 1024 * 1024,
        maxTotalBytes: 64 * 1024 * 1024,
        maxReceiptBytes: 24 * 1024,
      },
    });
    evidence = await pending;
    await inspect(evidence);
  } catch (error) {
    evidence ??= error.qualificationEvidence;
    failure = error;
  } finally {
    cancellation.abort();
    if (pending) await pending.catch(() => {});
    try {
      const children =
        evidence?.backups.flatMap((backup) => (backup.childReceipt ? [backup.childReceipt] : [])) ??
        [];
      // This outer creator removes only its empty exact directory after the case proves closure and cleanup.
      if (
        NodeFS.existsSync(caseRoot) ||
        (evidence && evidence.cleanup.outcome !== "complete") ||
        children.some((child) => !child.closed || !child.reaped || child.outcome === "unknown")
      )
        throw new Error(
          `Migration scratch retained at ${outer}; original closure or cleanup is unproved`,
        );
      NodeAssert.deepEqual(await NodeFSP.readdir(outer), []);
      const current = await NodeFSP.lstat(outer, { bigint: true });
      NodeAssert.equal(current.dev, identity.dev);
      NodeAssert.equal(current.ino, identity.ino);
      await NodeFSP.rmdir(outer);
      if (failure instanceof Error)
        failure.scratchEvidence = { outer, absent: !NodeFS.existsSync(outer), children };
    } catch (error) {
      failure = failure
        ? new AggregateError(
            [failure, error],
            "Original qualification failure and retained scratch",
            { cause: failure },
          )
        : error;
    }
  }
  if (failure) throw failure;
  return evidence;
}

function diagnostic(evidence) {
  return {
    caseId: evidence.caseId,
    producer: { sourceRevision: evidence.producer.sourceRevision, files: evidence.producer.files },
    sources: evidence.sourceIdentity.map((identity) => ({
      sourceRevision: identity.sourceRevision,
      forkCount: identity.forkCount,
      productionSha256: identity.production.sha256,
      migrationManifestSha256: identity.migrationManifest.sha256,
      configurationSha256: sha256(identity.configuration),
      dependenciesSha256: sha256(identity.dependencies),
    })),
    phases: evidence.phases.map((phase) => ({
      name: phase.name,
      mode: phase.mode,
      sourceRevision: phase.databaseSource.sourceRevision,
      closeKnown: phase.closeKnown,
      ...(phase.failure ? { failure: phase.failure } : {}),
      captureSha256: phase.capture === undefined ? null : sha256(phase.capture),
      runtime: phase.capture?.runtime,
      forkCount: (phase.capture?.content?.ledgers ?? phase.capture?.ledgers)?.jones_sql_migrations
        .length,
    })),
    backups: evidence.backups.map((backup) => ({
      snapshotRevision: backup.snapshotSource.sourceRevision,
      sourceCaptureSha256: backup.sourceCaptureSha256,
      restoredContentSha256: backup.restoredContentSha256,
      originalClosedOutput: backup.originalClosedOutput,
      child: {
        outcome: backup.childReceipt.outcome,
        closed: backup.childReceipt.closed,
        reaped: backup.childReceipt.reaped,
      },
    })),
    cleanup: { outcome: evidence.cleanup.outcome, absent: evidence.cleanup.absent },
  };
}

for (const specification of qualificationCases) {
  NodeTest.test(specification.title, async (test) => {
    await withCase(test, specification, (evidence) => {
      const expected = expectedSeeds[specification.seed];
      const before = evidence.phases.find(
        (phase) => phase.name === "closed-old-source-canonical-state",
      );
      NodeAssert.equal(before.databaseSource.sourceRevision, expected.revision);
      NodeAssert.equal(before.databaseSource.worktreePath, expected.worktreePath);
      NodeAssert.equal(
        before.capture.content.ledgers.jones_sql_migrations.length,
        expected.forkCount,
      );
      NodeAssert.deepEqual(
        before.capture.content.ledgers.jones_sql_migrations,
        expectedForkNames
          .slice(0, expected.forkCount)
          .map((name, index) => ({ id: index + 1, name })),
      );
      for (const table of ["workstreams_native_attempts", "workstreams_native_enrollments"]) {
        NodeAssert.equal(
          before.capture.content.tables[table].status,
          expected.forkCount >= 6 ? "present" : "absent",
        );
        if (expected.forkCount >= 6)
          NodeAssert.equal(before.capture.content.tables[table].count, 0);
      }
      const final = evidence.phases.at(-1);
      NodeAssert.equal(final.databaseSource.sourceRevision, candidateRevision);
      NodeAssert.equal(final.mode, "engine");
      NodeAssert.deepEqual(
        final.capture.content.ledgers.jones_sql_migrations,
        expectedForkNames.map((name, index) => ({ id: index + 1, name })),
      );
      NodeAssert.equal(final.capture.content.ledgers.effect_sql_migrations.length, 54);
      NodeAssert.equal(final.capture.content.tables.workstreams_native_attempts.count, 1);
      NodeAssert.equal(final.capture.content.tables.workstreams_native_enrollments.count, 1);
      NodeAssert.ok(final.capture.content.tables.auth_sessions.count >= 1);
      NodeAssert.equal(final.capture.application.native.status, "present");
      NodeAssert.deepEqual(final.capture.application.native.effectPhases, ["started", "completed"]);
      NodeAssert.equal(final.capture.application.readModel.equivalent, true);
      NodeAssert.equal(final.capture.application.pages.overlap, 0);
      NodeAssert.equal(final.capture.application.blobs.length, 1);
      NodeAssert.equal(final.capture.application.attachmentFiles.length, 1);
      for (const phase of evidence.phases) {
        NodeAssert.equal(phase.closeKnown, true, `${phase.name} has unknown resource closure`);
        if (phase.capture?.content) {
          NodeAssert.deepEqual(phase.capture.integrity, ["ok"]);
          NodeAssert.deepEqual(phase.capture.foreignKeys, []);
          NodeAssert.equal(phase.capture.runtime.nodeVersion, process.versions.node);
          NodeAssert.equal(phase.capture.runtime.executable, process.execPath);
          NodeAssert.match(phase.capture.runtime.sqliteSourceId, /^\d{4}-\d{2}-\d{2} /);
        }
      }
      for (const identity of evidence.sourceIdentity) {
        NodeAssert.match(identity.production.sha256, /^[a-f0-9]{64}$/);
        NodeAssert.ok(identity.production.fileCount > 0);
        NodeAssert.ok(
          identity.configuration.some((item) => item.relativePath === "pnpm-lock.yaml"),
        );
        NodeAssert.ok(identity.dependencies.every((dependency) => dependency.package.version));
      }
      if (specification.seed === "six") {
        const history = evidence.sourceIdentity.find(
          (identity) => identity.sourceRevision === expected.revision,
        );
        NodeAssert.equal(
          history.productionEquivalentAnchor,
          "8de693b103e6fb69e7a8b07775a9dbbab8d0f149",
        );
        NodeAssert.equal(history.excludedHistoryDifferences.length, 6);
      }
      if (specification.id === "rollback-seven") {
        NodeAssert.equal(evidence.api.fault.ddlExecuted, true);
        NodeAssert.equal(evidence.api.fault.indexObservedInTransaction, true);
        NodeAssert.equal(evidence.api.fault.ledgerSevenObservedInTransaction, true);
        NodeAssert.equal(evidence.api.fault.calls, 1);
        NodeAssert.equal(evidence.api.fault.failureKind, "Failed");
        NodeAssert.equal(evidence.api.fault.failureReason, "UnknownError");
        NodeAssert.equal(evidence.api.fault.removedBeforeRetry, true);
        const failedIndex = evidence.phases.findIndex(
          (phase) => phase.name === "declared-post-DDL007-fault",
        );
        const rollback = evidence.phases[failedIndex + 1];
        NodeAssert.equal(rollback.name, "rollback-client-canonical-state");
        NodeAssert.deepEqual(rollback.capture.content, before.capture.content);
        NodeAssert.deepEqual(rollback.capture.definitions, before.capture.definitions);
        NodeAssert.ok(
          evidence.phases.findIndex((phase) => phase.name === "candidate-migration-only") >
            failedIndex,
        );
      }
      if (specification.id.startsWith("restore-")) {
        NodeAssert.equal(evidence.backups.length, 1);
        const backup = evidence.backups[0];
        NodeAssert.equal(
          backup.snapshotSource.sourceRevision,
          specification.id === "restore-414" ? expected.revision : candidateRevision,
        );
        NodeAssert.equal(backup.completion.backupCompleted, true);
        NodeAssert.equal(backup.completion.sourceClosed, true);
        NodeAssert.equal(backup.childReceipt.closed, true);
        NodeAssert.equal(backup.childReceipt.reaped, true);
        NodeAssert.equal(backup.childReceipt.truncated, false);
        NodeAssert.equal(backup.sourceCaptureSha256, backup.restoredContentSha256);
        NodeAssert.equal(
          backup.originalClosedOutput.healthEligibility,
          "unqualified-backup-output",
        );
        NodeAssert.ok([1, 2].includes(backup.originalClosedOutput.header.writeVersion));
        NodeAssert.ok([1, 2].includes(backup.originalClosedOutput.header.readVersion));
        NodeAssert.equal(backup.companionFiles.length, 6);
        NodeAssert.ok(
          backup.companionFiles.every(
            (file) => file.originalRelativePath !== file.restoredRelativePath,
          ),
        );
        NodeAssert.equal(final.capture.application.files.length, 12);
        NodeAssert.ok(
          final.capture.application.references.workspaces.every((path) => path.startsWith("seed/")),
        );
        NodeAssert.ok(
          final.capture.application.references.worktrees.every((path) => path.startsWith("seed/")),
        );
      }
      if (specification.id.startsWith("restore-") || specification.id === "old-write-rollforward") {
        NodeAssert.equal(evidence.api.oldSource.replayAddedEvents, 0);
        NodeAssert.equal(evidence.api.oldSource.generationFenced, true);
        NodeAssert.equal(evidence.api.oldSource.staleIncarnationRenewed, false);
        NodeAssert.equal(evidence.api.oldSource.foreignAcquired, false);
        NodeAssert.notEqual(
          evidence.api.oldSource.createdIncarnation,
          evidence.api.oldSource.incarnation,
        );
        const oldIndex = evidence.phases.findIndex(
          (phase) => phase.name === "actual414-supported-writes",
        );
        NodeAssert.equal(
          evidence.phases[oldIndex].databaseSource.sourceRevision,
          expectedSeeds.live.revision,
        );
        NodeAssert.equal(evidence.phases[oldIndex].capture.application.native.status, "absent");
        NodeAssert.equal(
          evidence.phases[oldIndex + 1].name,
          "candidate-roll-forward-migration-only",
        );
      }
      NodeAssert.equal(evidence.cleanup.outcome, "complete");
      NodeAssert.equal(evidence.cleanup.absent, true);
      NodeAssert.equal(NodeFS.existsSync(evidence.creationReceipt.canonicalRootPath), false);
      NodeAssert.ok(
        evidence.cleanups.every((cleanup) => cleanup.outcome === "complete" && cleanup.absent),
      );
      const report = diagnostic(evidence);
      NodeAssert.ok(Buffer.byteLength(JSON.stringify(report)) <= 49 * 1024);
      test.diagnostic(JSON.stringify(report));
    });
    if (specification.id === "rollback-seven") await unknownCloseCounterexample(test);
  });
}

NodeTest.test("source HEAD reader accepts ordinary and linked Git metadata", async () => {
  const outer = await NodeFSP.mkdtemp(NodePath.join(directory, ".migration-source-metadata-test-"));
  const revision = "1111111111111111111111111111111111111111";
  const ref = "refs/heads/performance-source";
  try {
    const ordinary = NodePath.join(outer, "ordinary");
    const ordinaryGit = NodePath.join(ordinary, ".git");
    await NodeFSP.mkdir(NodePath.join(ordinaryGit, "refs/heads"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(ordinaryGit, "HEAD"), `ref: ${ref}\n`);
    await NodeFSP.writeFile(NodePath.join(ordinaryGit, ref), `${revision}\n`);
    NodeAssert.equal(pinnedHead(ordinary), revision);

    const linked = NodePath.join(outer, "linked");
    const metadata = NodePath.join(outer, "linked-metadata");
    const common = NodePath.join(outer, "common-metadata");
    await NodeFSP.mkdir(linked);
    await NodeFSP.mkdir(metadata);
    await NodeFSP.mkdir(NodePath.join(common, "refs/heads"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(linked, ".git"), "gitdir: ../linked-metadata\n");
    await NodeFSP.writeFile(NodePath.join(metadata, "commondir"), "../common-metadata\n");
    await NodeFSP.writeFile(NodePath.join(metadata, "HEAD"), `ref: ${ref}\n`);
    await NodeFSP.writeFile(NodePath.join(common, ref), `${revision}\n`);
    NodeAssert.equal(pinnedHead(linked), revision);

    await NodeFSP.writeFile(NodePath.join(ordinaryGit, "HEAD"), `${revision}\n`);
    NodeAssert.equal(pinnedHead(ordinary), revision);
    await NodeFSP.writeFile(NodePath.join(ordinaryGit, "HEAD"), `ref: ${ref}\n`);
    await NodeFSP.unlink(NodePath.join(ordinaryGit, ref));
    await NodeFSP.writeFile(NodePath.join(ordinaryGit, "packed-refs"), `${revision} ${ref}\n`);
    NodeAssert.equal(pinnedHead(ordinary), revision);

    await NodeFSP.writeFile(NodePath.join(common, ref), "not-a-commit\n");
    NodeAssert.throws(() => pinnedHead(linked));
    await NodeFSP.writeFile(NodePath.join(linked, ".git"), "../linked-metadata\n");
    NodeAssert.throws(() => pinnedHead(linked));
  } finally {
    await NodeFSP.rm(outer, { recursive: true });
    NodeAssert.equal(NodeFS.existsSync(outer), false);
  }
});
