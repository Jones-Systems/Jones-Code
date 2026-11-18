import { expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { StorageCleanupReport } from "../settings.ts";

const decodeReport = Schema.decodeUnknownSync(StorageCleanupReport);

const legacy = {
  trigger: "manual",
  startedAt: "2026-11-16T22:00:00.000Z",
  finishedAt: "2026-11-16T22:01:00.000Z",
  entries: [],
  counts: { removed: 0, kept: 0, failed: 0 },
  omittedCount: 0,
  bytesFreed: 0,
};

it("decodes retained legacy reports without inventing an operation", () => {
  expect(decodeReport(legacy).operation).toBeUndefined();
});

it("keeps reports readable when a newer server supplies an unknown operation", () => {
  expect(decodeReport({ ...legacy, operation: "future-action" }).operation).toBeUndefined();
});

const retirement = {
  state: "held",
  reasonCodes: [],
  receiptId: null,
  fingerprint: "fingerprint",
  paths: [],
  details: [],
  outputObligations: [],
};
const decodeRetirement = (extra: Record<string, unknown>) =>
  decodeReport({
    ...legacy,
    entries: [
      {
        kind: "worktree",
        outcome: "kept",
        reason: "proposed",
        path: "/w",
        threadId: null,
        threadTitle: null,
        retirement: { ...retirement, ...extra },
      },
    ],
  }).entries[0]!.retirement!;

it("decodes retirement outcomes written before recoverability and preserve branches", () => {
  const decoded = decodeRetirement({});
  expect(decoded.recoverability).toBeUndefined();
  expect(decoded.preserve).toBeUndefined();
});

it("carries the recoverability plan and the preserve receipt of a retirement", () => {
  const decoded = decodeRetirement({
    recoverability: {
      verdict: "retirable_after_preserve",
      regenerable: ["/w/dist"],
      preserve: {
        files: 3,
        deletions: 1,
        bytes: 40,
        remote: null,
        excluded: [{ path: ".env", reason: "sensitive_name" }],
        excludedBytes: 5,
      },
    },
    preserve: {
      branch: "preserve/20261117/mini/w",
      sha: "abc",
      remote: "origin",
      verification: "remote_verified",
      excluded: [],
      restore: "git worktree add /w preserve/20261117/mini/w",
      forced: true,
      readback: { local: "verified", remote: "verified" },
    },
  });
  expect(decoded.recoverability?.preserve?.remote).toBeNull();
  expect(decoded.recoverability?.preserve?.excluded[0]?.reason).toBe("sensitive_name");
  expect(decoded.preserve?.verification).toBe("remote_verified");
  expect(decoded.preserve?.readback).toEqual({ local: "verified", remote: "verified" });
});

it("drops a verdict or verification a newer server adds instead of failing the report", () => {
  const decoded = decodeRetirement({
    recoverability: { verdict: "future_verdict", regenerable: [] },
    preserve: {
      branch: "b",
      sha: "abc",
      remote: null,
      verification: "future_verification",
      excluded: [],
      restore: "r",
      forced: false,
    },
  });
  expect(decoded.recoverability).toBeUndefined();
  expect(decoded.preserve).toBeUndefined();
  expect(decoded.state).toBe("held");
});

it.each(["cleanup", "propose", "retire"] as const)(
  "preserves the %s operation across report decoding",
  (operation) => {
    expect(decodeReport({ ...legacy, operation }).operation).toBe(operation);
  },
);
