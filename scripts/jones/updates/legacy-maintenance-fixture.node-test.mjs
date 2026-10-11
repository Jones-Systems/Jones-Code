import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as OS from "node:os";
import * as Path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  extractPinnedLegacyHelper,
  legacyMaintenanceSource,
  MaintenanceCustodyError,
  readPinnedLegacyHelper,
  runLegacyMaintenanceFixture,
  runMaintenanceProcess,
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

test(
  "a closed readiness pipe reports EPIPE through the owned cleanup path",
  { timeout: 10000 },
  async () => {
    let allocated;
    await Assert.rejects(
      withMaintenanceScratch(OS.tmpdir(), async (root) => {
        allocated = root;
        // This lifecycle probe has no descendants. Close the read end before the
        // readiness bytes, making the parent's acknowledgement deterministically fail.
        const source = `
      const fs = require('node:fs');
      fs.closeSync(0);
      fs.writeSync(1, JSON.stringify({event:'children-ready',childCount:2})+'\\n'+
        JSON.stringify({event:'children-reaped',count:2})+'\\n');
    `;
        await runMaintenanceProcess(process.execPath, ["-e", source], root);
      }),
      (cause) => cause.code === "EPIPE",
    );
    await Assert.rejects(FS.lstat(allocated), { code: "ENOENT" });
  },
);

test(
  "missing descendant reaping proof preserves scratch and the original cleanup failure",
  { timeout: 10000 },
  async () => {
    const parent = await FS.mkdtemp(
      Path.join(await FS.realpath(OS.tmpdir()), "jones-maintenance-custody-test-"),
    );
    let captured;
    try {
      await FS.writeFile(Path.join(parent, "unrelated"), "preserve");
      await Assert.rejects(
        withMaintenanceScratch(parent, async (root) => {
          captured = root;
          // The captured child is actually reaped, then its controller fails to
          // publish the normal proof. The runner must retain evidence nonetheless.
          const source = `
        const fs = require('node:fs');
        const {spawn} = require('node:child_process');
        process.stdin.once('data', () => {
          const child = spawn(process.execPath, ['-e', 'process.exit(0)'], {stdio:'ignore'});
          child.once('close', (code, signal) => {
            fs.writeFileSync('test-owned-child-exit.json', JSON.stringify({code,signal}));
            fs.writeSync(2, 'injected descendant cleanup reporting failure');
            process.exitCode = 23;
          });
        });
        console.log(JSON.stringify({event:'children-ready',childCount:2}));
      `;
          await runMaintenanceProcess(process.execPath, ["-e", source], root);
        }),
        (cause) => {
          Assert.ok(cause instanceof MaintenanceCustodyError);
          Assert.match(cause.errors[0].message, /injected descendant cleanup reporting failure/);
          Assert.deepEqual(cause.preservedScratchRoots, [captured]);
          return true;
        },
      );
      Assert.deepEqual(
        JSON.parse(await FS.readFile(Path.join(captured, "test-owned-child-exit.json"), "utf8")),
        { code: 0, signal: null },
      );
      Assert.equal(await FS.readFile(Path.join(parent, "unrelated"), "utf8"), "preserve");
    } finally {
      // Only this probe's controller/child can exist; its close and exact exit
      // record above establish custody before the test retires its retained tree.
      if (captured !== undefined) {
        Assert.deepEqual(
          JSON.parse(await FS.readFile(Path.join(captured, "test-owned-child-exit.json"), "utf8")),
          { code: 0, signal: null },
        );
      }
      await FS.rm(parent, { recursive: true, force: false });
    }
  },
);

test("scratch cleanup failure retains the original failure as its primary cause", async () => {
  const original = new Error("original fixture failure");
  await Assert.rejects(
    withMaintenanceScratch(OS.tmpdir(), async (root) => {
      await FS.rm(root, { recursive: true, force: false });
      throw original;
    }),
    (cause) => {
      Assert.ok(cause instanceof AggregateError);
      Assert.equal(cause.errors[0], original);
      Assert.equal(cause.errors[1].code, "ENOENT");
      return true;
    },
  );
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
