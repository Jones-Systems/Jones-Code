import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";
import {
  extractPinnedLegacyHelper,
  legacyMaintenanceSource,
  MaintenanceCustodyError,
  readPinnedLegacyHelper,
  runLegacyMaintenanceFixture,
  runMaintenanceProcess,
  withMaintenanceScratch,
} from "./legacy-maintenance-fixture.mjs";

// oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone native qualification observes the actual host without loading the application runtime.
const nativePlatform = process.platform;

const python = process.env.JONES_LEGACY_FIXTURE_PYTHON ?? "/usr/bin/python3";
NodeAssert.ok(
  NodePath.isAbsolute(python),
  "JONES_LEGACY_FIXTURE_PYTHON must be an absolute interpreter path.",
);

NodeTest.test("legacy helper is selected by immutable commit, blob and byte digests", async () => {
  const helper = await readPinnedLegacyHelper(
    NodeURL.fileURLToPath(new URL("../../../", import.meta.url)),
  );
  NodeAssert.ok(helper.startsWith("#!/usr/bin/env python3\n"));
  NodeAssert.equal(legacyMaintenanceSource.commit, "bd20d2d2fc0ba89797698192b877a7f1312f8fe3");
  NodeAssert.throws(() => extractPinnedLegacyHelper(helper));
});

NodeTest.test(
  "a closed readiness pipe reports EPIPE through the owned cleanup path",
  { timeout: 10000 },
  async () => {
    let allocated;
    await NodeAssert.rejects(
      withMaintenanceScratch(NodeOS.tmpdir(), async (root) => {
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
    await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
  },
);

NodeTest.test(
  "missing descendant reaping proof preserves scratch and the original cleanup failure",
  { timeout: 10000 },
  async () => {
    const parent = await NodeFSP.mkdtemp(
      NodePath.join(await NodeFSP.realpath(NodeOS.tmpdir()), "jones-maintenance-custody-test-"),
    );
    let captured;
    try {
      await NodeFSP.writeFile(NodePath.join(parent, "unrelated"), "preserve");
      await NodeAssert.rejects(
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
          NodeAssert.ok(cause instanceof MaintenanceCustodyError);
          NodeAssert.match(
            cause.errors[0].message,
            /injected descendant cleanup reporting failure/,
          );
          NodeAssert.deepEqual(cause.preservedScratchRoots, [captured]);
          return true;
        },
      );
      NodeAssert.deepEqual(
        JSON.parse(
          await NodeFSP.readFile(NodePath.join(captured, "test-owned-child-exit.json"), "utf8"),
        ),
        { code: 0, signal: null },
      );
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(parent, "unrelated"), "utf8"),
        "preserve",
      );
    } finally {
      // Only this probe's controller/child can exist; its close and exact exit
      // record above establish custody before the test retires its retained tree.
      if (captured !== undefined) {
        NodeAssert.deepEqual(
          JSON.parse(
            await NodeFSP.readFile(NodePath.join(captured, "test-owned-child-exit.json"), "utf8"),
          ),
          { code: 0, signal: null },
        );
      }
      await NodeFSP.rm(parent, { recursive: true, force: false });
    }
  },
);

NodeTest.test(
  "scratch cleanup failure retains the original failure as its primary cause",
  async () => {
    const original = new Error("original fixture failure");
    await NodeAssert.rejects(
      withMaintenanceScratch(NodeOS.tmpdir(), async (root) => {
        await NodeFSP.rm(root, { recursive: true, force: false });
        throw original;
      }),
      (cause) => {
        NodeAssert.ok(cause instanceof AggregateError);
        NodeAssert.equal(cause.errors[0], original);
        NodeAssert.equal(cause.errors[1].code, "ENOENT");
        return true;
      },
    );
  },
);

NodeTest.test(
  "scratch cleanup preserves the original failure and concurrent fixture ownership",
  async () => {
    const roots = [];
    await withMaintenanceScratch(NodeOS.tmpdir(), async (outer) => {
      roots.push(outer);
      const sentinel = NodePath.join(outer, "survivor");
      await NodeFSP.writeFile(sentinel, "preserved");
      const failure = new Error("deliberate fixture failure");
      await NodeAssert.rejects(
        withMaintenanceScratch(NodeOS.tmpdir(), async (inner) => {
          roots.push(inner);
          NodeAssert.notEqual(inner, outer);
          throw failure;
        }),
        (error) => error === failure,
      );
      await NodeAssert.rejects(NodeFSP.lstat(roots[1]), { code: "ENOENT" });
      NodeAssert.equal(await NodeFSP.readFile(sentinel, "utf8"), "preserved");
    });
    for (const root of roots) await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  },
);

NodeTest.test(
  "exact legacy helper proves maintenance refusal and unsafe identical-epoch rollback",
  {
    skip: nativePlatform !== "darwin" && "Native macOS helper execution is required.",
    timeout: 70000,
  },
  async () => {
    const result = await runLegacyMaintenanceFixture({ python });
    NodeAssert.equal(result.status, "qualified-fixture");
    NodeAssert.equal(result.scratchRemoved, true);
    NodeAssert.equal(result.childrenReaped, true);
    NodeAssert.equal(result.stateUnchanged, true);
    NodeAssert.equal(result.lockInodeUnchanged, true);
    NodeAssert.equal(result.productionTransitionImplemented, false);
    NodeAssert.equal(result.electronAdmissionProved, false);
    NodeAssert.equal(result.externalExecution, false);
    NodeAssert.deepEqual(result.cases, [
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

NodeTest.test(
  "cancellation with two owned children reaps them before removing fixture scratch",
  {
    skip: nativePlatform !== "darwin" && "Native macOS helper execution is required.",
    timeout: 70000,
  },
  async () => {
    await withMaintenanceScratch(process.env.RUNNER_TEMP ?? NodeOS.tmpdir(), async (parent) => {
      const controller = new AbortController();
      const failure = new Error("deliberate owned-child cancellation");
      let observed = false;
      await NodeAssert.rejects(
        runLegacyMaintenanceFixture({
          python,
          scratchParent: parent,
          signal: controller.signal,
          onChildrenReady(event) {
            NodeAssert.equal(event.childCount, 2);
            observed = true;
            controller.abort(failure);
          },
        }),
        (cause) => cause === failure,
      );
      NodeAssert.equal(observed, true);
      NodeAssert.deepEqual(await NodeFSP.readdir(parent), []);
    });
  },
);
