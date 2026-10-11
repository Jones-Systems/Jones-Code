import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeURL from "node:url";
import * as NodeUtil from "node:util";
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
  "a denied live controller cannot keep its failed driver alive and retains exact custody evidence",
  {
    skip: nativePlatform !== "darwin" && "Native macOS process-exit observation is required.",
    timeout: 45000,
  },
  async () => {
    const root = await NodeFSP.mkdtemp(
      NodePath.join(await NodeFSP.realpath(NodeOS.tmpdir()), "jones-maintenance-driver-test-"),
    );
    let cleanupComplete = false;
    let failure;
    const nonce = NodeCrypto.randomUUID();
    try {
      await NodeFSP.writeFile(
        NodePath.join(root, "controller.cjs"),
        `
const fs = require('node:fs');
const net = require('node:net');
const socket = net.connect(process.argv[2]);
socket.on('error', () => { process.exitCode = 1; });
socket.once('connect', () => socket.write(JSON.stringify({pid:process.pid,nonce:${JSON.stringify(nonce)}})+'\\n'));
let pending = '';
socket.on('data', bytes => {
  pending += bytes;
  while (pending.includes('\\n')) {
    const end = pending.indexOf('\\n');
    const command = pending.slice(0,end); pending = pending.slice(end+1);
    if (command === 'armed') fs.writeSync(1, JSON.stringify({event:'children-ready',childCount:2})+'\\n');
    else if (command === 'probe') socket.write('held\\n');
    else if (command === 'release') socket.end(() => socket.destroy());
  }
});
`,
      );
      await NodeFSP.writeFile(
        NodePath.join(root, "driver.mjs"),
        `
import {runMaintenanceProcess,withMaintenanceScratch,MaintenanceCustodyError} from ${JSON.stringify(new URL("./legacy-maintenance-fixture.mjs", import.meta.url).href)};
const controller = new AbortController();
const original = Object.assign(new Error('owned persistent controller cancellation'), {code:'OWNED_CANCEL'});
process.kill = (pid, signal) => {
  if (!(Number.isInteger(pid) && pid < 0 && ['SIGTERM','SIGKILL'].includes(signal))) throw new Error('Unexpected signal target');
  throw Object.assign(new Error('injected group permission denial'), {code:'EPERM'});
};
try {
  await withMaintenanceScratch(process.argv[2], root => runMaintenanceProcess(
    process.execPath, [process.argv[3],process.argv[4]], root, controller.signal,
    () => controller.abort(original)));
} catch (cause) {
  const codes = error => [...(error?.code ? [error.code] : []), ...(error?.errors ?? []).flatMap(codes)];
  console.log(JSON.stringify({custody: cause instanceof MaintenanceCustodyError,
    roots:cause.preservedScratchRoots, controller:cause.controllerCustody,codes:codes(cause)}));
  process.exitCode = 1;
}
`,
      );
      // The supervisor owns the driver handle and a control socket that holds
      // its descendant controller alive. kqueue observes that exact process's
      // exit after driver exit; an orphan is not represented as our waitpid child.
      await NodeFSP.writeFile(
        NodePath.join(root, "supervisor.py"),
        String.raw`
import json, pathlib, select, socket, subprocess, sys
root = pathlib.Path(sys.argv[1])
server = socket.socket(socket.AF_UNIX)
driver = None
connection = None
watcher = None
controller_exited = False
driver_reaped = False
report = {'passed': False, 'cleanupComplete': False, 'controllerReapedBySupervisor': False}
def line(stream):
    result = bytearray()
    while not result.endswith(b'\n'):
        piece = stream.recv(1)
        if not piece: raise RuntimeError('Controller control channel ended early')
        result.extend(piece)
        if len(result) > 4096: raise RuntimeError('Controller output exceeded bound')
    return result.decode().strip()
try:
    server.bind('control.sock'); server.listen(1); server.settimeout(10)
    driver = subprocess.Popen([sys.argv[2],str(root/'driver.mjs'),str(root),str(root/'controller.cjs'),'../control.sock'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    connection, _ = server.accept(); connection.settimeout(5)
    identity = json.loads(line(connection))
    assert identity['nonce'] == sys.argv[3]
    pid = identity['pid']
    watcher = select.kqueue()
    watcher.control([select.kevent(pid, filter=select.KQ_FILTER_PROC, flags=select.KQ_EV_ADD|select.KQ_EV_ONESHOT, fflags=select.KQ_NOTE_EXIT)], 0, 0)
    connection.sendall(b'armed\n')
    output, error = driver.communicate(timeout=12)
    driver_reaped = True
    assert driver.returncode == 1 and not error
    result = json.loads(output)
    custody = result['controller']
    assert result['custody'] and result['codes'] == ['OWNED_CANCEL','EPERM','EPERM']
    assert custody['controllerPid'] == pid and custody['processGroupId'] == pid
    assert custody['driverPid'] == driver.pid and custody['executable'] == sys.argv[2]
    assert not custody['controllerExitObserved'] and not custody['descendantReapingProved']
    assert custody['driverHandleDetached']
    assert custody['deniedSignals'] == [{'signal':'SIGTERM','code':'EPERM'},{'signal':'SIGKILL','code':'EPERM'}]
    retained = pathlib.Path(result['roots'][0])
    assert retained.parent == root and retained.is_dir()
    assert pathlib.Path(custody['scratchRoot']) == retained
    assert json.loads((retained/'maintenance-custody.json').read_text()) == custody
    assert len(custody['argumentsSha256']) == 64
    connection.sendall(b'probe\n')
    assert line(connection) == 'held' and not watcher.control([],1,0)
    report.update(passed=True, driverExitedBeforeRelease=True, controllerAliveAfterDriverExit=True, retainedEvidenceMatched=True)
except Exception as error:
    report['failure'] = type(error).__name__
finally:
    if driver is not None and not driver_reaped:
        try:
            if driver.poll() is None: driver.kill()
            driver.communicate(timeout=5); driver_reaped = True
        except Exception as error: report['driverCleanupFailure'] = type(error).__name__
    if connection is not None and watcher is not None:
        try:
            connection.sendall(b'release\n')
        except Exception as error: report['releaseFailure'] = type(error).__name__
        try:
            events = watcher.control([],1,5)
            controller_exited = len(events) == 1 and events[0].ident == pid and bool(events[0].fflags & select.KQ_NOTE_EXIT)
        except Exception as error: report['cleanupFailure'] = type(error).__name__
    if connection is not None: connection.close()
    if watcher is not None: watcher.close()
    server.close()
    report.update(cleanupComplete=(driver is None or driver_reaped) and (driver is None or controller_exited), controllerExitObserved=controller_exited, driverReaped=driver_reaped)
    print(json.dumps(report),flush=True)
`,
      );
      const { stdout, stderr } = await NodeUtil.promisify(NodeChildProcess.execFile)(
        python,
        ["-I", "-B", NodePath.join(root, "supervisor.py"), root, process.execPath, nonce],
        {
          cwd: root,
          encoding: "utf8",
          timeout: 40000,
          maxBuffer: 8192,
          env: { PATH: "/usr/bin:/bin", HOME: root, TMPDIR: root },
        },
      );
      const report = JSON.parse(stdout);
      cleanupComplete = report.cleanupComplete === true;
      NodeAssert.equal(stderr, "");
      NodeAssert.deepEqual(report, {
        passed: true,
        cleanupComplete: true,
        controllerReapedBySupervisor: false,
        driverExitedBeforeRelease: true,
        controllerAliveAfterDriverExit: true,
        retainedEvidenceMatched: true,
        controllerExitObserved: true,
        driverReaped: true,
      });
    } catch (cause) {
      failure = cause;
      throw cause;
    } finally {
      if (cleanupComplete) {
        try {
          await NodeFSP.rm(root, { recursive: true, force: false });
        } catch (cleanup) {
          throw failure === undefined ? cleanup : new AggregateError([failure, cleanup]);
        }
      } else {
        const retained = new Error(`Owned supervisor custody unresolved; retained ${root}`);
        throw failure === undefined ? retained : new AggregateError([failure, retained]);
      }
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
