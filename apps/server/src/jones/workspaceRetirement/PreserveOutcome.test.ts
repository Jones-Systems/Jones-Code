import { expect, it } from "vite-plus/test";
import type { RetirementPreserve } from "./Receipts.ts";
import { confirmedPaths, preserveOutcome, recoverabilityOutcome } from "./PreserveOutcome.ts";
import type { PreservePlan, Recoverability } from "./Recoverability.ts";

const plan = (patch: Partial<PreservePlan> = {}): PreservePlan => ({
  schema: "workspace-preserve-plan/v1",
  selected: ["a.ts", "b.ts"],
  deletions: ["gone.ts"],
  excluded: [],
  selectedBytes: 2048,
  excludedBytes: 0,
  ...patch,
});
const recoverability = (verdict: Recoverability["verdict"]): Recoverability => ({
  schema: "workspace-recoverability/v1",
  verdict,
  reasons: { untracked: "preservable" },
  regenerable: ["/w/dist"],
});

it("summarizes the preserve plan for the owner without file lists or origins", () => {
  expect(
    recoverabilityOutcome(
      {
        recoverability: recoverability("retirable_after_preserve"),
        preservePlan: plan({
          excluded: [{ path: ".env", reason: "sensitive_name", origin: "untracked" }],
        }),
      },
      "origin",
    ),
  ).toEqual({
    verdict: "retirable_after_preserve",
    regenerable: ["/w/dist"],
    preserve: {
      files: 2,
      deletions: 1,
      bytes: 2048,
      remote: "origin",
      excluded: [{ path: ".env", reason: "sensitive_name" }],
      excludedBytes: 0,
    },
  });
});

it("states a remote only when one was resolved, and null when the repository has none", () => {
  const input = {
    recoverability: recoverability("retirable_after_preserve"),
    preservePlan: plan(),
  };
  expect(recoverabilityOutcome(input)?.preserve).not.toHaveProperty("remote");
  expect(recoverabilityOutcome(input, null)?.preserve?.remote).toBeNull();
});

it("yields nothing for a qualification observed before recoverability existed", () => {
  expect(recoverabilityOutcome(undefined)).toBeUndefined();
  expect(recoverabilityOutcome(null)).toBeUndefined();
  expect(recoverabilityOutcome({ preservePlan: plan() })).toBeUndefined();
  expect(recoverabilityOutcome({ recoverability: recoverability("recreatable") })).toEqual({
    verdict: "recreatable",
    regenerable: ["/w/dist"],
  });
});

it("carries the receipt's preserve branch, verification, restore command and readback", () => {
  const receipt: RetirementPreserve = {
    branch: "preserve/20261117/mini/w",
    sha: "0123456789abcdef0123456789abcdef01234567",
    remote: null,
    verification: "local_only",
    excluded: [{ path: "big.bin", reason: "oversized" }],
    restore: "git worktree add /w preserve/20261117/mini/w",
    forced: true,
    readback: { local: "verified", remote: "not_applicable" },
  };
  expect(preserveOutcome(receipt)).toEqual(receipt);
  const unread: RetirementPreserve = { ...receipt };
  delete unread.readback;
  expect(preserveOutcome(unread)).not.toHaveProperty("readback");
  expect(preserveOutcome(undefined)).toBeUndefined();
});

it("leaves untracked paths out of the covered paths only when a preserve branch carries them", () => {
  expect(confirmedPaths("retirable_after_preserve", ["/w/dist"], ["/w/new.ts"])).toEqual([
    "/w/dist",
  ]);
  expect(confirmedPaths("held", ["/w/dist"], ["/w/new.ts"])).toEqual(["/w/dist", "/w/new.ts"]);
  expect(confirmedPaths(undefined, ["/w/dist"], ["/w/new.ts"])).toEqual(["/w/dist", "/w/new.ts"]);
});
