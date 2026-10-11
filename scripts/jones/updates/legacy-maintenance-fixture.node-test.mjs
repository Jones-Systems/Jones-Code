import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as OS from "node:os";
import * as Path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  extractPinnedLegacyHelper,
  legacyMaintenanceSource,
  readPinnedLegacyHelper,
  runLegacyMaintenanceFixture,
  withMaintenanceScratch,
} from "./legacy-maintenance-fixture.mjs";

const python = process.env.JONES_LEGACY_FIXTURE_PYTHON ?? "/usr/bin/python3";
Assert.ok(
  Path.isAbsolute(python),
  "JONES_LEGACY_FIXTURE_PYTHON must be an absolute interpreter path.",
);

test("legacy helper is selected by immutable commit, blob and byte digests", async () => {
  const helper = await readPinnedLegacyHelper(fileURLToPath(new URL("../../../", import.meta.url)));
  Assert.ok(helper.startsWith("#!/usr/bin/env python3\n"));
  Assert.equal(legacyMaintenanceSource.commit, "bd20d2d2fc0ba89797698192b877a7f1312f8fe3");
  Assert.throws(() => extractPinnedLegacyHelper(helper));
});

test("scratch cleanup preserves the original failure and concurrent fixture ownership", async () => {
  const roots = [];
  await withMaintenanceScratch(OS.tmpdir(), async (outer) => {
    roots.push(outer);
    const sentinel = Path.join(outer, "survivor");
    await FS.writeFile(sentinel, "preserved");
    const failure = new Error("deliberate fixture failure");
    await Assert.rejects(
      withMaintenanceScratch(OS.tmpdir(), async (inner) => {
        roots.push(inner);
        Assert.notEqual(inner, outer);
        throw failure;
      }),
      (error) => error === failure,
    );
    await Assert.rejects(FS.lstat(roots[1]), { code: "ENOENT" });
    Assert.equal(await FS.readFile(sentinel, "utf8"), "preserved");
  });
  for (const root of roots) await Assert.rejects(FS.lstat(root), { code: "ENOENT" });
});

test(
  "exact legacy helper proves maintenance refusal and unsafe identical-epoch rollback",
  {
    skip: process.platform !== "darwin" && "Native macOS helper execution is required.",
    timeout: 70000,
  },
  async () => {
    const result = await runLegacyMaintenanceFixture({ python });
    Assert.equal(result.status, "qualified-fixture");
    Assert.equal(result.scratchRemoved, true);
    Assert.equal(result.childrenReaped, true);
    Assert.equal(result.stateUnchanged, true);
    Assert.equal(result.lockInodeUnchanged, true);
    Assert.equal(result.productionTransitionImplemented, false);
    Assert.equal(result.electronAdmissionProved, false);
    Assert.equal(result.externalExecution, false);
    Assert.deepEqual(result.cases, [
      { name: "fixed-lock-exclusion", outcome: "refused" },
      { name: "maintenance-after-controller-death-activate", outcome: "refused" },
      { name: "maintenance-after-controller-death-launch", outcome: "refused" },
      { name: "maintenance-after-controller-death-cli", outcome: "refused" },
      { name: "same-original-rollback-negative-control", outcome: "journal-intent-written" },
      { name: "rotated-epoch-old-intent", outcome: "refused" },
      { name: "journal-resumed", outcome: "unchanged" },
      { name: "journal-rolled-back", outcome: "unchanged" },
      { name: "journal-blocked", outcome: "unchanged" },
      { name: "journal-preparing", outcome: "blocked-written" },
      { name: "journal-trial", outcome: "blocked-written" },
      { name: "legacy-receipt-mismatch", outcome: "refused" },
      { name: "legacy-incomplete-preparation", outcome: "refused" },
    ]);
  },
);

test(
  "cancellation with two owned children reaps them before removing fixture scratch",
  {
    skip: process.platform !== "darwin" && "Native macOS helper execution is required.",
    timeout: 70000,
  },
  async () => {
    await withMaintenanceScratch(process.env.RUNNER_TEMP ?? OS.tmpdir(), async (parent) => {
      const controller = new AbortController();
      const failure = new Error("deliberate owned-child cancellation");
      let observed = false;
      await Assert.rejects(
        runLegacyMaintenanceFixture({
          python,
          scratchParent: parent,
          signal: controller.signal,
          onChildrenReady(event) {
            Assert.equal(event.childCount, 2);
            observed = true;
            controller.abort(failure);
          },
        }),
        (cause) => cause === failure,
      );
      Assert.equal(observed, true);
      Assert.deepEqual(await FS.readdir(parent), []);
    });
  },
);
