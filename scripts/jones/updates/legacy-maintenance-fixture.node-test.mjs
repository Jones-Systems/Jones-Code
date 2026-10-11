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

function fixtureFailureSummary(cause) {
  let remaining = 8;
  const text = (value, limit) =>
    typeof value === "string"
      ? value
          .slice(0, 512)
          .replace(/(?:[A-Za-z]:[\\/]|\/)[^\s"'<>)]*/g, "<path>")
          .replace(/[\r\n\t]/g, " ")
          .slice(0, limit)
      : undefined;
  const visit = (error) => {
    if (remaining-- <= 0) return { name: "truncated" };
    const result = {
      name: text(error?.name, 64),
      code: text(error?.code, 64),
      message: text(error?.message, 192),
    };
    if (Array.isArray(error?.errors)) result.errors = error.errors.slice(0, 3).map(visit);
    if (error?.cause !== undefined) result.cause = visit(error.cause);
    return result;
  };
  return visit(cause);
}

NodeTest.test(
  "fixture failure diagnostics preserve nested codes with bounded redacted text",
  () => {
    const primary = Object.assign(new Error("write EPIPE"), { code: "EPIPE" });
    const cleanup = Object.assign(new Error("kill EPERM at /synthetic/fixture/private-marker"), {
      code: "EPERM",
    });
    cleanup.cause = cleanup;
    const summary = fixtureFailureSummary(new AggregateError([primary, cleanup], "cleanup failed"));
    NodeAssert.equal(summary.errors[0].code, "EPIPE");
    NodeAssert.equal(summary.errors[1].code, "EPERM");
    const encoded = JSON.stringify(summary);
    NodeAssert.ok(encoded.length < 4096);
    NodeAssert.equal(encoded.includes("private-marker"), false);
    NodeAssert.match(encoded, /<path>/);
    NodeAssert.match(encoded, /truncated/);
  },
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
        // This probe has no descendants. A referenced MessagePort keeps the
        // controller alive until SIGTERM, separating EPIPE from an exit race.
        const source = `
      const fs = require('node:fs');
      const {MessageChannel} = require('node:worker_threads');
      const {port1, port2} = new MessageChannel();
      port1.on('message', () => {});
      process.once('SIGTERM', () => {
        fs.writeSync(1, JSON.stringify({event:'children-reaped',count:2})+'\\n');
        port1.close(); port2.close();
      });
      fs.closeSync(0);
      fs.writeSync(1, JSON.stringify({event:'children-ready',childCount:2})+'\\n');
    `;
        await runMaintenanceProcess(process.execPath, ["-e", source], root);
      }),
      (cause) => {
        NodeAssert.equal(cause.code, "EPIPE", JSON.stringify(fixtureFailureSummary(cause)));
        return true;
      },
    );
    await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
  },
);

NodeTest.test(
  "a captured controller exit prevents stale process-group signaling",
  { timeout: 10000 },
  async (test) => {
    const kill = test.mock.method(process, "kill", () => {
      NodeAssert.fail("An exited controller must not authorize process-group signaling.");
    });
    const controller = new AbortController();
    const original = new Error("cancelled after captured controller exit");
    let allocated;
    let exited = false;
    await NodeAssert.rejects(
      withMaintenanceScratch(NodeOS.tmpdir(), async (root) => {
        allocated = root;
        // There are no descendants; the acknowledgement orders ordinary exit
        // before the parent's cancellation callback.
        const source = `
      const fs = require('node:fs');
      process.stdin.once('data', () => {
        fs.writeSync(1, JSON.stringify({event:'children-reaped',count:2})+'\\n');
        process.stdin.destroy();
      });
      console.log(JSON.stringify({event:'children-ready',childCount:2}));
    `;
        await runMaintenanceProcess(
          process.execPath,
          ["-e", source],
          root,
          controller.signal,
          undefined,
          (outcome) => {
            NodeAssert.deepEqual(outcome, { code: 0, stoppedBy: null });
            exited = true;
            controller.abort(original);
          },
        );
      }),
      (cause) => cause === original,
    );
    NodeAssert.equal(exited, true);
    NodeAssert.equal(kill.mock.callCount(), 0);
    await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
  },
);

NodeTest.test(
  "a process-group permission failure preserves its cause and scratch despite controller exit",
  { timeout: 10000 },
  async (test) => {
    const parent = await NodeFSP.mkdtemp(
      NodePath.join(await NodeFSP.realpath(NodeOS.tmpdir()), "jones-maintenance-denied-test-"),
    );
    const denied = Object.assign(new Error("injected kill EPERM"), { code: "EPERM" });
    const original = new Error("deliberate readiness cancellation");
    const kill = test.mock.method(process, "kill", (pid, signal) => {
      NodeAssert.ok(Number.isInteger(pid) && pid < 0);
      NodeAssert.equal(signal, "SIGTERM");
      throw denied;
    });
    const controller = new AbortController();
    let captured;
    let exited = false;
    try {
      await NodeAssert.rejects(
        withMaintenanceScratch(parent, async (root) => {
          captured = root;
          // No descendants exist. The pipe holds the controller alive until
          // after readiness triggers an injected signaling denial.
          const source = `
        const fs = require('node:fs');
        process.stdin.once('data', () => {
          fs.writeSync(1, JSON.stringify({event:'children-reaped',count:2})+'\\n');
          process.stdin.destroy();
        });
        console.log(JSON.stringify({event:'children-ready',childCount:2}));
      `;
          await runMaintenanceProcess(
            process.execPath,
            ["-e", source],
            root,
            controller.signal,
            (_event, input) => {
              controller.abort(original);
              input.end("G");
            },
            (outcome) => {
              NodeAssert.deepEqual(outcome, { code: 0, stoppedBy: null });
              exited = true;
            },
          );
        }),
        (cause) => {
          NodeAssert.ok(cause instanceof MaintenanceCustodyError);
          NodeAssert.deepEqual(cause.errors[0].errors, [original, denied]);
          NodeAssert.deepEqual(cause.preservedScratchRoots, [captured]);
          return true;
        },
      );
      NodeAssert.equal(kill.mock.callCount(), 1);
      NodeAssert.equal(exited, true);
      NodeAssert.equal((await NodeFSP.lstat(captured)).isDirectory(), true);
    } finally {
      // Only retire retained evidence after the exact no-descendant controller exited.
      NodeAssert.equal(exited, true);
      await NodeFSP.rm(parent, { recursive: true, force: false });
    }
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
