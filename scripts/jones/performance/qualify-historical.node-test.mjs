import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  historicalQualificationClosureKnown,
  assertStrictHistoricalResult,
  assertStrictHistoricalResults,
  qualifyHistorical,
} from "./qualify-historical.mjs";
import { preparationPins } from "./historical-preparation.mjs";
import { migrationRestoreSchema, nativeBackupSchema } from "./migration-restore.mjs";

const candidate = { sourceRevision: "synthetic-candidate" };
const parent = "/synthetic/owned/sources";
function receipt(caseId) {
  const selected = caseId === "foreign-87" ? "lease-current" : "lease";
  const sources = ["history", selected].map((directory) => ({
    repository: "Jones-Systems/Jones-Code",
    sourceRevision: preparationPins.find((pin) => pin.directory === directory).sourceRevision,
    worktreePath: NodePath.join(parent, directory),
  }));
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
  const capture = { content: { rows: "preserved synthetic" } };
  const hash = NodeCrypto.createHash("sha256")
    .update(JSON.stringify(capture.content))
    .digest("hex");
  return {
    schema: migrationRestoreSchema,
    caseId,
    status: "complete",
    candidate,
    runnerClosed: true,
    sourceIdentity: sources,
    phases: names.map((name, i) => ({
      name,
      closeKnown: true,
      databaseSource: i < 2 ? sources[i] : candidate,
      capture,
    })),
    cleanups: [
      { outcome: "complete", absent: true },
      { outcome: "complete", absent: true },
    ],
    cleanup: { outcome: "complete", absent: true },
    backups: [
      {
        schema: nativeBackupSchema,
        backupCompleted: true,
        sourceClosed: true,
        closureBasis: "awaited-native-backup-completion-and-observed-source-close",
        capture,
        sourceCaptureSha256: hash,
        restoredContentSha256: hash,
        originalClosedOutput: { healthQualification: "unqualified-readonly-health" },
      },
    ],
    foreignWire: {
      receivingCodec: "OrchestrationV2Command",
      rejected: true,
      unchanged: true,
      dispatched: false,
    },
    foreignGuard: { unchangedForeignContent: true, adoptedForeignFeatures: false },
    foreignHistory: { featureAdopted: false },
    ...(caseId === "foreign-87"
      ? {
          badHistory: {
            rejected: true,
            unchanged: true,
            variants: ["unknown-ID7-name", "missing-original-prefix"],
          },
        }
      : {
          successor: {
            owner: "055_OrchestrationV2/RecoveryIndexes",
            index: "orchestration_events_v2_created_threads_idx",
            actualLookupExecuted: true,
            foreignBirthExcluded: true,
            changedProjectionRejected: true,
            missingIndexRejected: true,
          },
        }),
  };
}

NodeTest.test(
  "strict qualification consumes both full case receipts and refuses unavailable/skip/partial success",
  () => {
    const results = [receipt("foreign-87"), receipt("receiving-creation-lookup")];
    NodeAssert.equal(assertStrictHistoricalResults(results, candidate, parent), results);
    for (const status of ["unavailable", "skipped", "passed", undefined]) {
      const evidence = receipt("foreign-87");
      evidence.status = status;
      NodeAssert.throws(
        () => assertStrictHistoricalResult("foreign-87", evidence, candidate, parent),
        /not qualification/,
      );
    }
    NodeAssert.throws(() => assertStrictHistoricalResults([results[0]], candidate, parent));
    NodeAssert.throws(() =>
      assertStrictHistoricalResults([...results].reverse(), candidate, parent),
    );
  },
);
NodeTest.test(
  "full acceptance rejects missing phase, unknown closure, bad backup, source substitution and weakened oracles",
  () => {
    const variants = [
      (r) => r.phases.splice(1, 1),
      (r) => {
        r.phases[0].closeKnown = false;
      },
      (r) => {
        r.runnerClosed = false;
      },
      (r) => {
        r.cleanup.absent = false;
      },
      (r) => {
        r.backups[0].sourceClosed = false;
      },
      (r) => {
        r.backups[0].restoredContentSha256 = "0".repeat(64);
      },
      (r) => {
        r.phases[4].capture = { content: { changed: true } };
      },
      (r) => {
        r.sourceIdentity[0].sourceRevision = "borrowed";
      },
      (r) => {
        r.foreignWire.dispatched = true;
      },
      (r) => {
        r.foreignGuard.unchangedForeignContent = false;
      },
      (r) => {
        r.foreignHistory.featureAdopted = true;
      },
      (r) => {
        r.badHistory = { rejected: false };
      },
    ];
    for (const mutate of variants) {
      const r = receipt("foreign-87");
      mutate(r);
      NodeAssert.throws(() => assertStrictHistoricalResult("foreign-87", r, candidate, parent));
    }
    for (const oracle of [
      "actualLookupExecuted",
      "foreignBirthExcluded",
      "changedProjectionRejected",
      "missingIndexRejected",
    ]) {
      const r = receipt("receiving-creation-lookup");
      r.successor[oracle] = false;
      NodeAssert.throws(() =>
        assertStrictHistoricalResult("receiving-creation-lookup", r, candidate, parent),
      );
    }
  },
);
NodeTest.test(
  "strict entrypoint refuses missing preparation evidence before importing workers or allocating databases",
  async () => {
    await NodeAssert.rejects(
      qualifyHistorical({
        explicitSyntheticRequest: true,
        maxEvidenceBytes: 65536,
        preparationReceiptPath: "/absent",
        preparationReceiptSha256: "0".repeat(64),
      }),
    );
  },
);

NodeTest.test(
  "failed acceptance never releases a returned case with unknown resource closure",
  () => {
    const preflight = {
      complete: true,
      guardClosed: true,
      batches: [{ launched: 2, settled: 2, finished: true }],
    };
    const result = receipt("foreign-87");
    NodeAssert.equal(
      historicalQualificationClosureKnown([result], undefined, undefined, preflight),
      true,
    );
    result.runnerClosed = false;
    NodeAssert.equal(
      historicalQualificationClosureKnown([result], undefined, undefined, preflight),
      false,
    );
    NodeAssert.equal(
      historicalQualificationClosureKnown([], "foreign-87", undefined, preflight),
      false,
    );
    result.runnerClosed = true;
    NodeAssert.equal(
      historicalQualificationClosureKnown([], "foreign-87", result, preflight),
      true,
    );
    result.phases[0].closeKnown = false;
    NodeAssert.equal(
      historicalQualificationClosureKnown([], "foreign-87", result, preflight),
      false,
    );
  },
);

NodeTest.test("empty results never prove import preflight closure", () => {
  NodeAssert.equal(historicalQualificationClosureKnown([], undefined, undefined), false);
});
