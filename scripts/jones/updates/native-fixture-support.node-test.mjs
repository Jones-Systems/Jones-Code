import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeStream from "node:stream";
import {
  nativeFixtureFailureReport,
  runNativeFixtureCommand,
  withNativeFixtureScratch,
} from "./native-fixture-support.mjs";

for (const scenario of [
  { name: "closed timeout", phase: "observer-compile", expected: "observer-compile", close: true },
  { name: "unknown close", phase: "observer-driver", expected: "observer-driver", close: false },
  { name: "unknown phase", phase: "synthetic-private-phase", expected: "fixture", close: true },
]) {
  NodeTest.test(
    `command diagnostics preserve phase without payloads: ${scenario.name}`,
    async (context) => {
      const secret = "synthetic-private-payload-not-for-diagnostics";
      const child = new NodeEvents.EventEmitter();
      child.stdout = new NodeStream.PassThrough();
      child.stderr = new NodeStream.PassThrough();
      child.exitCode = null;
      child.signalCode = null;
      const signals = [];
      let released = false;
      child.kill = (signal) => {
        signals.push(signal);
        child.signalCode = signal;
        child.emit("exit", null, signal);
        if (scenario.close) child.emit("close", null, signal);
        return true;
      };
      child.unref = () => {
        released = true;
      };
      let allocated;
      let captured;
      context.mock.timers.enable({ apis: ["setTimeout"] });
      try {
        await NodeAssert.rejects(
          withNativeFixtureScratch(async (root) => {
            allocated = root;
            let settled = false;
            const observed = runNativeFixtureCommand(
              `/not-executed/${secret}`,
              ["--synthetic-input", secret],
              root,
              { phase: scenario.phase, spawn: () => child },
            ).then(
              () => {
                throw new Error("Timed-out synthetic command unexpectedly succeeded.");
              },
              (cause) => {
                settled = true;
                return cause;
              },
            );
            child.emit("spawn");
            child.stdout.write(secret);
            child.stderr.write(secret);
            context.mock.timers.tick(29999);
            NodeAssert.deepEqual(signals, []);
            context.mock.timers.tick(1);
            NodeAssert.deepEqual(signals, ["SIGTERM"]);
            if (!scenario.close) {
              context.mock.timers.tick(5999);
              await Promise.resolve();
              NodeAssert.equal(settled, false);
              context.mock.timers.tick(1);
            }
            captured = await observed;
            throw captured;
          }),
          (cause) => cause === captured,
        );
        const primary = scenario.close ? captured : captured.cause;
        NodeAssert.equal(
          primary.message,
          `Native identity command (${scenario.expected}) exceeded 30 seconds.`,
        );
        const report = nativeFixtureFailureReport(captured);
        NodeAssert.deepEqual(report.commandPhases, [scenario.expected]);
        NodeAssert.equal(JSON.stringify(report).includes(secret), false);
        NodeAssert.equal(JSON.stringify(report).includes("synthetic-private-phase"), false);
        NodeAssert.deepEqual(signals, ["SIGTERM"]);
        if (scenario.close) {
          NodeAssert.equal(released, false);
          NodeAssert.deepEqual(report.commandCustody, []);
          await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
        } else {
          NodeAssert.ok(captured instanceof AggregateError);
          NodeAssert.equal(captured.errors[0], primary);
          NodeAssert.equal(captured.preserveScratch, true);
          NodeAssert.equal(released, true);
          NodeAssert.equal(report.retainedScratch, allocated);
          NodeAssert.equal(report.commandCustody.length, 1);
          const custody = report.commandCustody[0];
          const receipt = JSON.parse(await NodeFSP.readFile(custody.evidencePath, "utf8"));
          NodeAssert.equal(custody.persisted, true);
          NodeAssert.equal(receipt.phase, scenario.expected);
          NodeAssert.equal(receipt.spawnObserved, true);
          NodeAssert.equal(receipt.exitObserved, true);
          NodeAssert.equal(receipt.closeObserved, false);
          NodeAssert.equal(receipt.signalCode, "SIGTERM");
          NodeAssert.equal(receipt.permitsPidSignalling, false);
          NodeAssert.equal(receipt.permitsProcessAdoption, false);
          NodeAssert.equal(JSON.stringify(receipt).includes(secret), false);
          NodeAssert.equal((await NodeFSP.lstat(allocated)).isDirectory(), true);
        }
      } finally {
        context.mock.timers.reset();
        child.stdout.destroy();
        child.stderr.destroy();
        // The captured EventEmitter is entirely synthetic: no OS child or image
        // was launched. This test owns only its new root and diagnostic receipt.
        child.emit("close", child.exitCode, child.signalCode);
        if (allocated) await NodeFSP.rm(allocated, { recursive: true, force: true });
      }
    },
  );
}

NodeTest.test(
  "repeated synchronous signal errors preserve cancellation through proved closure",
  { timeout: 10000 },
  async (context) => {
    let allocated;
    await withNativeFixtureScratch(async (root) => {
      allocated = root;
      const cancellation = new AbortController();
      const original = new Error("fixture command cancellation");
      let child;
      let reaped;
      let cleanupObserved = false;
      const spawn = (...args) => {
        child = NodeChildProcess.spawn(...args);
        const ownedKill = child.kill.bind(child);
        reaped = new Promise((resolve) =>
          child.once("close", (code, signal) => {
            cleanupObserved = true;
            resolve({ code, signal });
          }),
        );
        child.kill = () => {
          try {
            child.emit(
              "error",
              Object.assign(new Error("first injected denial"), { code: "EPERM" }),
            );
            child.emit(
              "error",
              Object.assign(new Error("second injected denial"), { code: "EPERM" }),
            );
            throw Object.assign(new Error("injected thrown denial"), { code: "EPERM" });
          } finally {
            // The test owns this still-unreaped handle; the receipt is not used to select it.
            ownedKill("SIGKILL");
          }
        };
        child.once("spawn", () => cancellation.abort(original));
        return child;
      };
      let originalFailure;
      try {
        await NodeAssert.rejects(
          runNativeFixtureCommand(process.execPath, ["-e", "setInterval(() => {}, 1000)"], root, {
            signal: AbortSignal.any([context.signal, cancellation.signal]),
            spawn,
          }),
          (cause) => {
            NodeAssert.ok(cause instanceof AggregateError);
            NodeAssert.equal(cause.cause, original);
            NodeAssert.equal(cause.errors[0], original);
            NodeAssert.deepEqual(
              cause.errors.slice(1).map((item) => item.code),
              ["EPERM", "EPERM", "EPERM"],
            );
            NodeAssert.deepEqual(nativeFixtureFailureReport(cause).commandCustody, []);
            return true;
          },
        );
        NodeAssert.deepEqual(await reaped, { code: null, signal: "SIGKILL" });
      } catch (cause) {
        originalFailure = cause;
        throw cause;
      } finally {
        if (child && !cleanupObserved) {
          const retained = new AggregateError(
            originalFailure ? [originalFailure] : [],
            "Captured child closure is unproved; exact test root retained.",
            { cause: originalFailure },
          );
          retained.preserveScratch = true;
          throw retained;
        }
      }
    });
    await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
  },
);

NodeTest.test(
  "unresolved commands persist custody through aggregation and let their driver drain",
  { timeout: 20000 },
  async (context) => {
    let allocated;
    await withNativeFixtureScratch(async (root) => {
      allocated = root;
      const fixtureModule = new URL("./native-fixture-support.mjs", import.meta.url).href;
      const driver = NodePath.join(root, "custody-driver.mjs");
      await NodeFSP.writeFile(
        driver,
        `
import * as fs from 'node:fs';
import * as cp from 'node:child_process';
import {withNativeFixtureScratch,runNativeFixtureCommand,nativeFixtureFailureReport} from ${JSON.stringify(fixtureModule)};
let controller, ownedKill, controllerClosed = false, cleanupStarted = false;
const write = (name, value) => fs.writeFileSync(name, JSON.stringify(value), {mode:0o600});
const cleanup = (failure) => {
  if (failure) { process.exitCode = 1; write('driver-error.json', {message:failure.message}); }
  if (!controller || controllerClosed || cleanupStarted) return;
  cleanupStarted = true;
  controller.ref();
  try { ownedKill('SIGKILL'); }
  catch (cause) { write('controller-cleanup-error.json', {code:cause.code}); process.exitCode = 1; }
};
process.once('beforeExit', () => {
  write('driver-liveness.json', {beforeExit:true,controllerAlive:!controllerClosed && controller?.exitCode === null && controller?.signalCode === null});
  cleanup();
});
process.on('SIGTERM', () => cleanup(new Error('driver cancelled')));
process.on('uncaughtException', cleanup);
const watchdog = setTimeout(() => cleanup(new Error('driver exceeded its bound')), 10000);
watchdog.unref();
const cancellation = new AbortController();
const original = new Error('fixture command cancellation');
const spawn = (...args) => {
  controller = cp.spawn(...args);
  ownedKill = controller.kill.bind(controller);
  controller.once('close', (code, signal) => {
    controllerClosed = true;
    clearTimeout(watchdog);
    write('controller-reaped.json', {pid:controller.pid,code,signal});
  });
  controller.kill = (signal) => {
    controller.emit('error', Object.assign(new Error('injected synchronous denial'), {code:'EPERM'}));
    if (signal === 'SIGKILL') throw Object.assign(new Error('injected thrown denial'), {code:'EPERM'});
    return false;
  };
  controller.once('spawn', () => cancellation.abort(original));
  return controller;
};
try {
  await withNativeFixtureScratch(async (childRoot) => {
    try {
      await runNativeFixtureCommand(process.execPath, ['-e','setInterval(() => {}, 1000)'], childRoot, {signal:cancellation.signal,spawn});
    } catch (cause) {
      if (cause.cause !== original || cause.errors.length !== 4) throw new Error('Original failure or signal errors were lost.');
      throw new AggregateError([cause,new Error('fixture wrapper')], 'wrapped custody', {cause});
    }
  }, {temporaryDirectory:process.cwd()});
  cleanup(new Error('unresolved command unexpectedly succeeded'));
} catch (cause) {
  const report = nativeFixtureFailureReport(cause);
  write('driver-report.json', report);
  process.stdout.write(JSON.stringify(report)+'\\n');
  if (report.commandCustody.length !== 1 || !report.retainedScratch) cleanup(new Error('custody reference was lost'));
}
`,
        { flag: "wx", mode: 0o600 },
      );
      let originalFailure;
      try {
        const outcome = await runNativeFixtureCommand(process.execPath, [driver], root, {
          signal: context.signal,
        });
        NodeAssert.equal(outcome.code, 0, outcome.stderr);
        NodeAssert.equal(outcome.stoppedBy, null);
        const report = JSON.parse(outcome.stdout);
        NodeAssert.equal(report.status, "failed");
        NodeAssert.equal(report.activationAllowed, false);
        NodeAssert.equal(report.commandCustody.length, 1);
        const custody = report.commandCustody[0];
        NodeAssert.equal(custody.persisted, true);
        NodeAssert.equal(NodePath.dirname(custody.evidencePath), report.retainedScratch);
        NodeAssert.equal(NodePath.dirname(report.retainedScratch), root);
        const receipt = JSON.parse(await NodeFSP.readFile(custody.evidencePath, "utf8"));
        const { persisted, ...observations } = custody;
        NodeAssert.equal(persisted, true);
        NodeAssert.deepEqual(receipt, observations);
        NodeAssert.equal((await NodeFSP.stat(custody.evidencePath)).mode & 0o777, 0o600);
        NodeAssert.equal(receipt.spawnObserved, true);
        NodeAssert.equal(receipt.exitObserved, false);
        NodeAssert.equal(receipt.closeObserved, false);
        NodeAssert.equal(receipt.identityScope, "captured-child-handle-only");
        NodeAssert.equal(receipt.permitsPidSignalling, false);
        NodeAssert.equal(receipt.permitsProcessAdoption, false);
        NodeAssert.equal(receipt.pidMayBeReused, true);
        NodeAssert.equal(receipt.errorCount, 3);
        NodeAssert.deepEqual(
          receipt.signalAttempts.map((item) => [item.signal, item.returned, item.errorCount]),
          [
            ["SIGTERM", false, 1],
            ["SIGKILL", null, 2],
          ],
        );
        NodeAssert.deepEqual(
          JSON.parse(await NodeFSP.readFile(NodePath.join(root, "driver-liveness.json"), "utf8")),
          { beforeExit: true, controllerAlive: true },
        );
        NodeAssert.deepEqual(
          JSON.parse(await NodeFSP.readFile(NodePath.join(root, "controller-reaped.json"), "utf8")),
          { pid: receipt.controllerPid, code: null, signal: "SIGKILL" },
        );
      } catch (cause) {
        originalFailure = cause;
        throw cause;
      } finally {
        // Require the actual captured controller's close event even when an
        // assertion fails; otherwise retain this exact test root and its receipt.
        try {
          const reaped = JSON.parse(
            await NodeFSP.readFile(NodePath.join(root, "controller-reaped.json"), "utf8"),
          );
          NodeAssert.equal(reaped.signal, "SIGKILL");
        } catch (cause) {
          const retained = new AggregateError(
            originalFailure ? [originalFailure, cause] : [cause],
            "Controller cleanup is unproved; exact test root retained.",
            { cause: originalFailure ?? cause },
          );
          retained.preserveScratch = true;
          throw retained;
        }
      }
    });
    await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
  },
);

NodeTest.test(
  "identity scratch owners clean success and failure, preserving the cause",
  async () => {
    let successful;
    await withNativeFixtureScratch(async (root) => {
      successful = root;
    });
    await NodeAssert.rejects(NodeFSP.lstat(successful), { code: "ENOENT" });
    let failed;
    const original = new Error("synthetic identity failure");
    await NodeAssert.rejects(
      withNativeFixtureScratch(async (root) => {
        failed = root;
        throw original;
      }),
      (cause) => cause === original,
    );
    await NodeAssert.rejects(NodeFSP.lstat(failed), { code: "ENOENT" });
  },
);

NodeTest.test(
  "unknown image custody retains its exact root and cleanup errors preserve the cause",
  async () => {
    let retained;
    const unknown = Object.assign(new Error("synthetic unknown custody"), {
      preserveScratch: true,
    });
    try {
      await NodeAssert.rejects(
        withNativeFixtureScratch(async (root) => {
          retained = root;
          throw unknown;
        }),
        (cause) => cause === unknown && cause.scratchRoot === retained,
      );
      NodeAssert.equal((await NodeFSP.lstat(retained)).isDirectory(), true);
    } finally {
      // This case creates an empty directory only; no image or process exists.
      if (retained) await NodeFSP.rmdir(retained);
    }
    const original = new Error("operation failed");
    await NodeAssert.rejects(
      withNativeFixtureScratch(async (root) => {
        await NodeFSP.rmdir(root);
        throw original;
      }),
      (cause) =>
        cause instanceof AggregateError && cause.cause === original && cause.errors[0] === original,
    );
  },
);
