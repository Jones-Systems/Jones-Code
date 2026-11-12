import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import {
  packagedDesktopFaultPoints,
  preparePackagedDesktopHelperFault,
} from "./packaged-desktop-helper-fault.mjs";
import { assertFixtureProcessesStopped, runOwnedChild } from "./native-startup-fixture.mjs";

// This executable control proves external tracing, not packaged macOS behavior.
const control = String.raw`
import json, pathlib, sys
if "--claim-preparation" in sys.argv: raise SystemExit(0)
manifest_path = pathlib.Path(sys.argv[2])
intent_file = pathlib.Path(sys.argv[4])
intent = json.loads(intent_file.read_text())
tx = intent_file.parent

def durable(path, record):
    with path.open('w') as stream:
        json.dump(record, stream)
    path.chmod(0o600)

def storage_preflight(*args): pass
def reserve_recovery_capacity(*args): pass
def kernel_process_identity(*args): return True
def alive(proof): return kernel_process_identity(proof)
def stop_exact(proofs):
    for proof in proofs: alive(proof)
    any(alive(proof) for proof in proofs)
def pair_state(*args): raise AssertionError('Control passed the fault boundary')

def activate():
    transaction_id = intent['transactionId']
    journal = {'intent': intent, 'phase': 'intent'}
    preparation_step = 'storage'
    durable(tx / 'journal.json', journal)
    try:
        storage_preflight()
        journal['phase'] = 'preparing'
        durable(tx / 'journal.json', journal)
        preparation_step = 'reserve'
        reserve_recovery_capacity()
        preparation_step = 'stop-processes'
        stop_exact([123])
        journal['phase'] = 'quiescent'
        durable(tx / 'journal.json', journal)
        preparation_step = 'pair'
        pair_state()
    except OSError as failure:
        journal.update(phase='rolled-back', preEffect={'step': preparation_step, 'errno': failure.errno})
        durable(tx / 'journal.json', journal)
    else:
        raise AssertionError('Control did not fault')

activate()
`;

async function fixture(body, source = control) {
  const root = await NodeFSP.mkdtemp(NodePath.join(await NodeFSP.realpath(NodeOS.tmpdir()), "jones-desktop-preload-"));
  await NodeFSP.chmod(root, 0o700);
  try {
    const home = NodePath.join(root, "home");
    const updates = NodePath.join(home, "runtime/jones-updates");
    const tx = NodePath.join(updates, "transactions", "e".repeat(64));
    await NodeFSP.mkdir(tx, { recursive: true, mode: 0o700 });
    const manifest = NodePath.join(home, "runtime/jones-active-install.json");
    const profile = NodePath.join(root, "profile");
    const state = NodePath.join(home, "userdata");
    await NodeFSP.mkdir(profile, { mode: 0o700 });
    await NodeFSP.mkdir(state, { mode: 0o700 });
    const databasePath = NodePath.join(state, "statev2.sqlite");
    await NodeFSP.writeFile(databasePath, "unchanged-before-relaunch", { mode: 0o600 });
    const active = { protocol: 1, owner: "desktop", home, profile, databasePath };
    const intent = NodePath.join(tx, "intent.json");
    await NodeFSP.writeFile(manifest, JSON.stringify(active), { mode: 0o600 });
    await NodeFSP.writeFile(
      intent,
      JSON.stringify({ protocol: 1, transactionId: "e".repeat(64), expected: active }),
      { mode: 0o600 },
    );
    const digest = NodeCrypto.createHash("sha256").update(source).digest("hex");
    const helper = NodePath.join(updates, "activate-v1-" + digest + ".py");
    await NodeFSP.writeFile(helper, source, { mode: 0o700 });
    await body({ root, helper, manifest, intent, tx, active });
  } finally {
    assertFixtureProcessesStopped();
    await NodeFSP.rm(root, { recursive: true, force: false });
    await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  }
}

async function execute(f, prepared, args = prepared.argv) {
  return runOwnedChild("/usr/bin/python3", args, {
    cwd: f.root,
    timeout: 5000,
    env: {
      PATH: "/usr/bin:/bin",
      HOME: f.root,
      TMPDIR: f.root,
      ...prepared.environment,
    },
  });
}

for (const point of packagedDesktopFaultPoints) {
  NodeTest.test(
    "external preload faults only the bound " + point + " boundary and preserves helper bytes",
    async () => {
      await fixture(async (f) => {
        const helperBefore = await NodeFSP.readFile(f.helper);
        const manifestBefore = await NodeFSP.readFile(f.manifest);
        const p = await preparePackagedDesktopHelperFault({ fixtureRoot: f.root, ...f, point });
        const result = await execute(f, p);
        NodeAssert.equal(result.code, 0, result.stderr);
        const receipt = await p.readReceipt();
        NodeAssert.equal(receipt.point, point);
        NodeAssert.deepEqual(await NodeFSP.readFile(f.helper), helperBefore);
        NodeAssert.deepEqual(await NodeFSP.readFile(f.manifest), manifestBefore);
        const journal = JSON.parse(await NodeFSP.readFile(NodePath.join(f.tx, "journal.json")));
        NodeAssert.equal(journal.phase, "rolled-back");
        NodeAssert.equal(journal.preEffect.errno, 5);
        NodeAssert.equal(p.packagedDesktopMatrixPassed, false);
        const before = await NodeFSP.readFile(p.receiptPath);
        const replay = await execute(f, p);
        NodeAssert.equal(replay.code, 125);
        NodeAssert.deepEqual(await NodeFSP.readFile(p.receiptPath), before);
      });
    },
  );
}

NodeTest.test("unrelated Python is inert even with an unavailable inherited plan", async () => {
  await fixture(async (f) => {
    const p = await preparePackagedDesktopHelperFault({
      fixtureRoot: f.root,
      ...f,
      point: "storage",
    });
    const result = await execute(
      f,
      {
        ...p,
        environment: { ...p.environment, JONES_PACKAGED_DESKTOP_FAULT_PLAN: "/not-readable" },
      },
      ["-c", "print('unrelated')"],
    );
    NodeAssert.equal(result.code, 0, result.stderr);
    NodeAssert.equal(result.stdout.trim(), "unrelated");
    await NodeAssert.rejects(NodeFSP.lstat(p.receiptPath), { code: "ENOENT" });
  });
});

NodeTest.test(
  "stale manifest and helper bytes stop before activation or receipt creation",
  async () => {
    for (const changed of ["manifest", "helper"]) {
      await fixture(async (f) => {
        const p = await preparePackagedDesktopHelperFault({
          fixtureRoot: f.root,
          ...f,
          point: "storage",
        });
        await NodeFSP.appendFile(f[changed], "\n");
        const result = await execute(f, p);
        NodeAssert.equal(result.code, 125, result.stderr);
        await NodeAssert.rejects(NodeFSP.lstat(p.receiptPath), { code: "ENOENT" });
        await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(f.tx, "journal.json")), {
          code: "ENOENT",
        });
      });
    }
  },
);

NodeTest.test("foreign helper path and symlinked input are refused", async () => {
  await fixture(async (f) => {
    const p = await preparePackagedDesktopHelperFault({
      fixtureRoot: f.root,
      ...f,
      point: "storage",
    });
    const other = NodePath.join(NodePath.dirname(f.helper), "other", NodePath.basename(f.helper));
    await NodeFSP.mkdir(NodePath.dirname(other), { mode: 0o700 });
    await NodeFSP.copyFile(f.helper, other);
    await NodeFSP.chmod(other, 0o700);
    const result = await execute(f, p, [other, ...p.argv.slice(1)]);
    NodeAssert.equal(result.code, 125, result.stderr);
    await NodeAssert.rejects(NodeFSP.lstat(p.receiptPath), { code: "ENOENT" });
  });
  await fixture(async (f) => {
    const copy = f.intent + ".original";
    await NodeFSP.rename(f.intent, copy);
    await NodeFSP.symlink(copy, f.intent);
    await NodeAssert.rejects(
      preparePackagedDesktopHelperFault({ fixtureRoot: f.root, ...f, point: "storage" }),
    );
  });
});

function relaunchControl(corruptBeforeLaunch) {
  const opener = [
    "def launch(active, descriptor=None):",
    "    with pathlib.Path(active['databasePath']).open('a') as stream:",
    "        stream.write('-normal-startup-write')",
    "    (pathlib.Path(active['profile']) / 'started').write_text('started')",
    "",
    "def activate():",
  ].join("\n");
  const catchBody = [
    "        evidence = {'phase': journal['phase'], 'step': preparation_step,",
    "                    'reason': 'preparation-failed', 'diagnostics': {'errno': failure.errno}, 'relaunch': 'skipped'}",
    "        journal.update(phase='rolled-back', preEffect=evidence)",
    "        durable(tx / 'journal.json', journal)",
    ...(corruptBeforeLaunch
      ? [
          "        pathlib.Path(intent['expected']['databasePath']).write_text('unexpected-pre-effect-change')",
        ]
      : []),
    "        relaunch_previous()",
    "        journal['preEffect']['relaunch'] = 'started'",
    "        durable(tx / 'journal.json', journal)",
  ].join("\n");
  return control
    .replace("def activate():", opener)
    .replace(
      "    preparation_step = 'storage'",
      "    def relaunch_previous():\n        launch(intent['expected'])\n    preparation_step = 'storage'",
    )
    .replace(
      "        journal.update(phase='rolled-back', preEffect={'step': preparation_step, 'errno': failure.errno})\n        durable(tx / 'journal.json', journal)",
      catchBody,
    );
}

NodeTest.test(
  "separate profile observes the actual relaunch entry after a throwing fault trace, before startup writes",
  async () => {
    await fixture(async (f) => {
      const original = await NodeFSP.readFile(f.active.databasePath);
      const prepared = await preparePackagedDesktopHelperFault({
        fixtureRoot: f.root,
        ...f,
        point: "exit-wait",
        observeRelaunch: true,
      });
      const result = await execute(f, prepared);
      NodeAssert.equal(result.code, 0, result.stderr);
      const fault = await prepared.readReceipt();
      const before = await prepared.readRelaunchReceipt();
      await NodeAssert.rejects(prepared.readLaunchObservation());
      NodeAssert.equal(before.pid, fault.pid);
      NodeAssert.equal(
        before.state[f.active.databasePath].sha256,
        NodeCrypto.createHash("sha256").update(original).digest("hex"),
      );
      NodeAssert.deepEqual(before.state[f.active.profile].children, {});
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(f.active.profile, "started"), "utf8"),
        "started",
      );
      NodeAssert.notDeepEqual(await NodeFSP.readFile(f.active.databasePath), original);
      const journal = JSON.parse(await NodeFSP.readFile(NodePath.join(f.tx, "journal.json")));
      NodeAssert.equal(journal.preEffect.relaunch, "started");
      await NodeAssert.rejects(prepared.assertNoRelaunch());
    }, relaunchControl(false));
  },
);

NodeTest.test(
  "pre-relaunch observation refuses protected-state changes before reopen and binds the fault PID",
  async () => {
    await fixture(async (f) => {
      const prepared = await preparePackagedDesktopHelperFault({
        fixtureRoot: f.root,
        ...f,
        point: "exit-wait",
        observeRelaunch: true,
      });
      const result = await execute(f, prepared);
      NodeAssert.equal(result.code, 0, result.stderr);
      await NodeAssert.rejects(
        prepared.readRelaunchReceipt(),
        /Protected fixture state changed before relaunch/,
      );
      const path = NodePath.join(NodePath.dirname(prepared.planPath), "before-relaunch.json");
      const receipt = JSON.parse(await NodeFSP.readFile(path));
      receipt.pid += 1;
      await NodeFSP.writeFile(path, JSON.stringify(receipt), { mode: 0o600 });
      await NodeAssert.rejects(prepared.readRelaunchReceipt());
    }, relaunchControl(true));
  },
);

async function installSelection(f) {
  const staging = NodePath.join(f.active.home, "runtime/jones-updates/staging");
  await NodeFSP.mkdir(staging, { mode: 0o700 });
  const selection = NodePath.join(staging, "selected.json");
  const app = { handle: "d".repeat(64), sourceSha: "a".repeat(40) };
  const active = { ...f.active, environmentId: "generated-control" };
  await NodeFSP.writeFile(f.manifest, JSON.stringify(active), { mode: 0o600 });
  await NodeFSP.writeFile(selection, JSON.stringify({ active, app }), { mode: 0o600 });
  await NodeFSP.unlink(f.intent);
  return {
    selection,
    active,
    app,
    async prepare() {
      const receipt = NodePath.join(f.tx, "continuation.json");
      const claim = {
        protocol: 1,
        preparationClaimProtocol: 3,
        transactionId: "e".repeat(64),
        stagedHandle: app.handle,
        selectionPath: selection,
        selectionSha256: NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(selection))
          .digest("hex"),
        home: active.home,
        profile: active.profile,
        databasePath: active.databasePath,
        environmentId: active.environmentId,
      };
      for (const name of ["prepare-intent.json", "prepare-dispatched.json"])
        await NodeFSP.writeFile(NodePath.join(f.tx, name), JSON.stringify(claim), { mode: 0o600 });
      await NodeFSP.writeFile(receipt, JSON.stringify({ ...claim, prepared: true }), {
        mode: 0o600,
      });
      await NodeFSP.writeFile(
        f.intent,
        JSON.stringify({
          protocol: 1,
          transactionId: claim.transactionId,
          expected: active,
          staged: app,
          continuationReceipt: receipt,
        }),
        { mode: 0o600 },
      );
    },
  };
}

function launchServicesControl(mode = "returned") {
  return relaunchControl(false)
    .replace(
      "def kernel_process_identity(*args): return True",
      "def kernel_process_identity(*args): return False",
    )
    .replace(
      "def pair_state",
      "def valid_process_proofs(proofs): return proofs == [123]\ndef pair_state",
    )
    .replace(
      "    preparation_step = 'storage'",
      "    captured_processes = [123]\n    preparation_step = 'storage'",
    )
    .replace(
      "def launch(active, descriptor=None):",
      `def launch_services(request):
    ${mode === "services-failed" ? "raise ValueError('private service failure')" : "pass"}
    ${mode === "write-failed" ? "durable(pathlib.Path(__import__('os').environ['JONES_PACKAGED_DESKTOP_FAULT_PLAN']).with_name('launch-services.json'), {})" : "pass"}
    ${mode === "refused" ? "return {'protocol': 1, 'status': 'failed', 'domain': 'NSCocoaErrorDomain', 'code': 256}" : "pass"}
    ${mode === "unavailable" ? "return {'protocol': 1, 'status': 'unavailable'}" : "pass"}
    return {'protocol': 1, 'status': 'launched', 'pid': 321,
            'bundlePath': intent['expected']['appPath'], 'executablePath': intent['expected']['executablePath']}

def launch(active, descriptor=None):
    reply = launch_services({'appPath': active['appPath'], 'environment': {}})
    if reply.get('status') != 'launched': raise RuntimeError('unproved control launch')
    ${mode === "duplicate" ? "reply = launch_services({'appPath': active['appPath'], 'environment': {}})" : "pass"}
    ${mode === "failed" ? "raise RuntimeError('private failure text must not escape')" : "pass"}`,
    )
    .replace(
      "    (pathlib.Path(active['profile']) / 'started').write_text('started')",
      "    (pathlib.Path(active['profile']) / 'started').write_text('started')\n    return {'pid': reply['pid'], 'startSec': 456, 'startUsec': 789, 'executable': active['executablePath']}",
    )
    .replace(
      "        relaunch_previous()",
      mode === "skipped" ? "        pass" : "        relaunch_previous()",
    );
}

async function deferredLaunchFixture(body, mode = "returned") {
  await fixture(async (f) => {
    const appPath = NodePath.join(f.root, "Previous.app");
    const executablePath = NodePath.join(appPath, "Contents/MacOS/previous");
    await NodeFSP.mkdir(NodePath.dirname(executablePath), { recursive: true, mode: 0o700 });
    await NodeFSP.writeFile(executablePath, "not an executable app", { mode: 0o600 });
    f.active = { ...f.active, appPath, executablePath };
    const selected = await installSelection(f);
    const prepared = await preparePackagedDesktopHelperFault({
      fixtureRoot: f.root,
      helper: f.helper,
      manifest: f.manifest,
      selection: selected.selection,
      point: "before-pair",
      observeRelaunch: true,
    });
    await selected.prepare();
    const result = await execute(f, prepared, [
      f.helper,
      "--manifest",
      f.manifest,
      "--activate",
      f.intent,
    ]);
    await body({ ...f, prepared, result });
  }, launchServicesControl(mode));
}

NodeTest.test(
  "deferred instrumentation retains the LaunchServices reply after launch validation throws",
  async () => {
    await deferredLaunchFixture(async ({ prepared, result, active }) => {
      NodeAssert.equal(result.code, 1);
      NodeAssert.match(result.stderr, /private failure text must not escape/);
      const replyPath = NodePath.join(NodePath.dirname(prepared.planPath), "launch-services.json");
      const reply = JSON.parse(await NodeFSP.readFile(replyPath));
      NodeAssert.equal(reply.reply.pid, 321);
      NodeAssert.equal(reply.reply.executablePath, active.executablePath);
      const observed = await prepared.readLaunchObservation();
      NodeAssert.equal(observed.launchReturn.failure, "launch-validation-unproved");
      NodeAssert.equal(observed.launchReturn.identity, null);
      NodeAssert.equal(JSON.stringify(observed).includes("private failure text"), false);
      NodeAssert.equal(observed.externalDescendantSettlementRequired, true);
      NodeAssert.equal(observed.activationAllowed, false);
    }, "failed");
  },
);

NodeTest.test(
  "deferred launch observations bind source, order, exact reply paths and birth without granting custody",
  async () => {
    await deferredLaunchFixture(async ({ prepared, result, active, tx }) => {
      NodeAssert.equal(result.code, 0, result.stderr);
      const preload = NodePath.dirname(prepared.planPath);
      const servicesPath = NodePath.join(preload, "launch-services.json");
      const returnPath = NodePath.join(preload, "launch-return.json");
      const originalServices = await NodeFSP.readFile(servicesPath);
      const originalReturn = await NodeFSP.readFile(returnPath);
      const services = JSON.parse(originalServices);
      const returned = JSON.parse(originalReturn);
      for (const path of [servicesPath, returnPath]) {
        const original = await NodeFSP.readFile(path);
        await NodeFSP.unlink(path);
        await NodeAssert.rejects(prepared.readLaunchObservation(), { code: "ENOENT" });
        await NodeFSP.writeFile(path, original, { mode: 0o600 });
      }
      for (const mutate of [
        (s) => {
          s.protocol = true;
        },
        (s) => {
          s.transactionId = "0".repeat(64);
        },
        (s) => {
          s.helperSha256 = "0".repeat(64);
        },
        (s) => {
          s.sequence = 2;
        },
        (s) => {
          s.beforeRelaunchSha256 = "0".repeat(64);
        },
        (s) => {
          s.reply.pid = "321";
        },
        (s) => {
          s.reply.executablePath += ".foreign";
        },
        (s) => {
          s.reply.bundlePath += ".foreign";
        },
        (s) => {
          s.reply.status = "available";
        },
        (s) => {
          s.unboundedData = "unexpected";
        },
        (_s, r) => {
          r.identity.pid += 1;
        },
        (_s, r) => {
          r.identity.startUsec = 1000000;
        },
        (_s, r) => {
          r.identity.executable += ".foreign";
        },
        (_s, r) => {
          r.failure = "raw exception";
        },
      ]) {
        const changedServices = structuredClone(services);
        const changedReturn = structuredClone(returned);
        mutate(changedServices, changedReturn);
        const bytes = JSON.stringify(changedServices);
        changedReturn.launchServicesSha256 = NodeCrypto.createHash("sha256")
          .update(bytes)
          .digest("hex");
        await NodeFSP.writeFile(servicesPath, bytes);
        await NodeFSP.writeFile(returnPath, JSON.stringify(changedReturn));
        await NodeAssert.rejects(prepared.readLaunchObservation());
      }
      await NodeFSP.writeFile(servicesPath, originalServices);
      await NodeFSP.writeFile(returnPath, originalReturn);
      const dispatchedPath = NodePath.join(tx, "prepare-dispatched.json");
      const dispatched = await NodeFSP.readFile(dispatchedPath);
      await NodeFSP.writeFile(
        dispatchedPath,
        JSON.stringify({ ...JSON.parse(dispatched), preparationClaimProtocol: 2 }),
      );
      await NodeAssert.rejects(prepared.readLaunchObservation());
      await NodeFSP.writeFile(dispatchedPath, dispatched);
      const observed = await prepared.readLaunchObservation();
      NodeAssert.equal(observed.launchServices.reply.pid, observed.launchReturn.identity.pid);
      NodeAssert.equal(observed.launchReturn.identity.executable, active.executablePath);
      NodeAssert.equal(observed.launchReturn.failure, null);
      NodeAssert.equal(observed.activationAllowed, false);
      NodeAssert.equal(observed.externalDescendantSettlementRequired, true);
      NodeAssert.ok(Object.isFrozen(observed.launchServices.reply));
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(active.profile, "started"), "utf8"),
        "started",
      );
      await NodeFSP.appendFile(servicesPath, " ");
      await NodeAssert.rejects(prepared.readLaunchObservation());
    });
  },
);

NodeTest.test(
  "duplicate or failed post-effect observation preserves helper behavior and rejects custody evidence",
  async () => {
    for (const mode of ["duplicate", "write-failed"]) {
      await deferredLaunchFixture(async ({ prepared, result, active }) => {
        NodeAssert.equal(result.code, 0, result.stderr);
        NodeAssert.equal(
          await NodeFSP.readFile(NodePath.join(active.profile, "started"), "utf8"),
          "started",
        );
        const marker = JSON.parse(
          await NodeFSP.readFile(
            NodePath.join(NodePath.dirname(prepared.planPath), "launch-observation-failure.json"),
          ),
        );
        NodeAssert.equal(
          marker.failure,
          mode === "duplicate" ? "duplicate-observation" : "observation-failed",
        );
        await NodeAssert.rejects(prepared.readLaunchObservation());
      }, mode);
    }
  },
);

NodeTest.test(
  "failed LaunchServices return has a finite category and preserves the original exception",
  async () => {
    await deferredLaunchFixture(async ({ prepared, result }) => {
      NodeAssert.equal(result.code, 1);
      NodeAssert.match(result.stderr, /private service failure/);
      const observed = await prepared.readLaunchObservation();
      NodeAssert.equal(observed.launchServices.reply, null);
      NodeAssert.equal(observed.launchServices.failure, "launch-services-return-unproved");
      NodeAssert.equal(observed.launchReturn.failure, "launch-services-unproved");
      NodeAssert.equal(JSON.stringify(observed).includes("private service failure"), false);
    }, "services-failed");
    for (const mode of ["refused", "unavailable"]) {
      await deferredLaunchFixture(async ({ prepared, result }) => {
        NodeAssert.equal(result.code, 1);
        NodeAssert.match(result.stderr, /unproved control launch/);
        const observed = await prepared.readLaunchObservation();
        NodeAssert.equal(
          observed.launchServices.reply.status,
          mode === "refused" ? "failed" : "unavailable",
        );
        NodeAssert.equal(observed.launchReturn.failure, "launch-validation-unproved");
        NodeAssert.equal(observed.launchReturn.identity, null);
      }, mode);
    }
  },
);

NodeTest.test("no-relaunch proof requires every launch observation absent", async () => {
  await deferredLaunchFixture(async ({ prepared, result }) => {
    NodeAssert.equal(result.code, 0, result.stderr);
    await prepared.assertNoRelaunch();
    for (const name of [
      "launch-services.json",
      "launch-return.json",
      "launch-observation-failure.json",
    ]) {
      const path = NodePath.join(NodePath.dirname(prepared.planPath), name);
      await NodeFSP.writeFile(path, "{}", { mode: 0o600 });
      await NodeAssert.rejects(prepared.assertNoRelaunch());
      await NodeFSP.unlink(path);
    }
    await prepared.assertNoRelaunch();
    await NodeAssert.rejects(prepared.readLaunchObservation(), { code: "ENOENT" });
  }, "skipped");
});

NodeTest.test(
  "deferred before-pair snapshots quiet state after live writes and rejects changes before reopen",
  async () => {
    for (const corrupt of [false, true]) {
      const source = relaunchControl(corrupt)
        .replace(
          "def pair_state",
          "def valid_process_proofs(proofs): return proofs == [123]\ndef pair_state",
        )
        .replace(
          "    for proof in proofs: alive(proof)",
          "    for proof in proofs: alive(proof)\n    pathlib.Path(intent['expected']['databasePath']).write_text('last-live-write')",
        )
        .replace(
          "def kernel_process_identity(*args): return True",
          "def kernel_process_identity(*args): return False",
        )
        .replace(
          "    preparation_step = 'storage'",
          "    captured_processes = [123]\n    preparation_step = 'storage'",
        );
      await fixture(async (f) => {
        const selected = await installSelection(f);
        const prepared = await preparePackagedDesktopHelperFault({
          fixtureRoot: f.root,
          helper: f.helper,
          manifest: f.manifest,
          selection: selected.selection,
          point: "before-pair",
          observeRelaunch: true,
        });
        await selected.prepare();
        const result = await execute(f, prepared, [
          f.helper,
          "--manifest",
          f.manifest,
          "--activate",
          f.intent,
        ]);
        NodeAssert.equal(result.code, corrupt ? 125 : 0, result.stderr);
        if (corrupt) {
          await NodeAssert.rejects(prepared.readRelaunchReceipt());
          await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(f.active.profile, "started")), {
            code: "ENOENT",
          });
        } else {
          const receipt = await prepared.readRelaunchReceipt();
          NodeAssert.equal(
            receipt.state[f.active.databasePath].sha256,
            NodeCrypto.createHash("sha256").update("last-live-write").digest("hex"),
          );
          const snapshotPath = NodePath.join(
            NodePath.dirname(prepared.planPath),
            "before-pair-state.json",
          );
          const snapshot = JSON.parse(await NodeFSP.readFile(snapshotPath));
          snapshot.pid += 1;
          await NodeFSP.writeFile(snapshotPath, JSON.stringify(snapshot), { mode: 0o600 });
          await NodeAssert.rejects(prepared.readRelaunchReceipt());
        }
      }, source);
    }
  },
);

NodeTest.test(
  "deferred relaunch observation refuses points without a quiet before-pair boundary",
  async () => {
    await fixture(async (f) => {
      const selected = await installSelection(f);
      for (const point of ["storage", "reserve", "exit-wait"])
        await NodeAssert.rejects(
          preparePackagedDesktopHelperFault({
            fixtureRoot: f.root,
            helper: f.helper,
            manifest: f.manifest,
            selection: selected.selection,
            point,
            observeRelaunch: true,
          }),
        );
    });
  },
);

NodeTest.test(
  "deferred before-pair refuses a snapshot while a recorded process is still alive",
  async () => {
    const source = relaunchControl(false)
      .replace(
        "def pair_state",
        "def valid_process_proofs(proofs): return proofs == [123]\ndef pair_state",
      )
      .replace(
        "    preparation_step = 'storage'",
        "    captured_processes = [123]\n    preparation_step = 'storage'",
      );
    await fixture(async (f) => {
      const selected = await installSelection(f);
      const prepared = await preparePackagedDesktopHelperFault({
        fixtureRoot: f.root,
        helper: f.helper,
        manifest: f.manifest,
        selection: selected.selection,
        point: "before-pair",
        observeRelaunch: true,
      });
      await selected.prepare();
      const result = await execute(f, prepared, [
        f.helper,
        "--manifest",
        f.manifest,
        "--activate",
        f.intent,
      ]);
      NodeAssert.equal(result.code, 125, result.stderr);
      for (const name of ["fault-receipt.json", "before-pair-state.json", "before-relaunch.json"])
        await NodeAssert.rejects(
          NodeFSP.lstat(NodePath.join(NodePath.dirname(prepared.planPath), name)),
          { code: "ENOENT" },
        );
      await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(f.active.profile, "started")), {
        code: "ENOENT",
      });
    }, source);
  },
);

NodeTest.test(
  "Install preload binds the later prepared intent without intercepting claim commands",
  async () => {
    await fixture(async (f) => {
      const selected = await installSelection(f);
      const p = await preparePackagedDesktopHelperFault({
        fixtureRoot: f.root,
        helper: f.helper,
        manifest: f.manifest,
        selection: selected.selection,
        point: "storage",
      });
      NodeAssert.equal(p.argv, null);
      await NodeAssert.rejects(NodeFSP.lstat(f.intent), { code: "ENOENT" });
      // The same named helper's non-activate operations must not consume an Install plan.
      const unrelated = await execute(f, p, ["-c", "print('unrelated')"]);
      NodeAssert.equal(unrelated.code, 0, unrelated.stderr);
      NodeAssert.equal(unrelated.stdout.trim(), "unrelated");
      const claim = await execute(f, p, [
        f.helper,
        "--manifest",
        f.manifest,
        "--claim-preparation",
        selected.app.handle,
      ]);
      NodeAssert.equal(claim.code, 0, claim.stderr);
      await NodeAssert.rejects(NodeFSP.lstat(p.receiptPath), { code: "ENOENT" });
      await selected.prepare();
      const result = await execute(f, p, [
        f.helper,
        "--manifest",
        f.manifest,
        "--activate",
        f.intent,
      ]);
      NodeAssert.equal(result.code, 0, result.stderr);
      const receipt = await p.readReceipt();
      NodeAssert.equal(receipt.transactionId, "e".repeat(64));
      NodeAssert.equal(
        receipt.intentSha256,
        NodeCrypto.createHash("sha256")
          .update(await NodeFSP.readFile(f.intent))
          .digest("hex"),
      );
      NodeAssert.equal(p.packagedDesktopMatrixPassed, false);
      const before = await NodeFSP.readFile(p.receiptPath);
      NodeAssert.equal(
        (await execute(f, p, [f.helper, "--manifest", f.manifest, "--activate", f.intent])).code,
        125,
      );
      NodeAssert.deepEqual(await NodeFSP.readFile(p.receiptPath), before);
    });
  },
);

NodeTest.test(
  "Install preload refuses changed selections, unprepared claims and mismatched staged inputs before effects",
  async () => {
    for (const changed of ["selection", "claim", "intent"]) {
      await fixture(async (f) => {
        const selected = await installSelection(f);
        const p = await preparePackagedDesktopHelperFault({
          fixtureRoot: f.root,
          helper: f.helper,
          manifest: f.manifest,
          selection: selected.selection,
          point: "storage",
        });
        await selected.prepare();
        if (changed === "selection") await NodeFSP.appendFile(selected.selection, "\n");
        if (changed === "claim")
          await NodeFSP.writeFile(NodePath.join(f.tx, "prepare-dispatched.json"), "{}", {
            mode: 0o600,
          });
        if (changed === "intent") {
          const actual = JSON.parse(await NodeFSP.readFile(f.intent));
          actual.staged.handle = "f".repeat(64);
          await NodeFSP.writeFile(f.intent, JSON.stringify(actual), { mode: 0o600 });
        }
        const result = await execute(f, p, [
          f.helper,
          "--manifest",
          f.manifest,
          "--activate",
          f.intent,
        ]);
        NodeAssert.equal(result.code, 125, result.stderr);
        await NodeAssert.rejects(NodeFSP.lstat(p.receiptPath), { code: "ENOENT" });
        await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(f.tx, "journal.json")), {
          code: "ENOENT",
        });
      });
    }
  },
);


// Native identity APIs below are deliberate portable doubles. These controls
// exercise ordering/containment and never attest libproc or a packaged app.
function custodyControl(mode) {
  return (mode === "released-relaunch" ? relaunchControl(false) : mode === "mutated-before-relaunch" ? relaunchControl(true) : control).replace("import json, pathlib, sys", "import json, pathlib, sys, os")
    .replace("def storage_preflight(*args): pass", `
def proof(pid): return dict(pid=pid, startSec=1, startUsec=0, executable='/control/' + str(pid))
def kernel_process_sample(pid): return proof(pid), os.getppid()
def valid_kernel_process(p): return set(p) == {'pid', 'startSec', 'startUsec', 'executable'}
def valid_kernel_processes(ps): return bool(ps) and all(valid_kernel_process(p) for p in ps)
def valid_process_proofs(ps): return valid_kernel_processes(ps)
def valid_process_roles(ps): return ps == [{'role': 'main', 'pid': os.getppid()}]
def prove_quiescence(active):
    ${mode === "unknown-writer" ? "raise RuntimeError('writer inspection unavailable')" : "pass"}
def storage_preflight(*args): pass
`)
    .replace("def alive(proof): return kernel_process_identity(proof)", `def alive(p):
    kernel_process_identity(p)
    if p['pid'] == int(os.environ['CONTROL_PID']): return ${mode === "lost-controller" ? "False" : "True"}
    return ${mode === "live-writer" ? "True" : "not pathlib.Path(os.environ['JONES_PACKAGED_DESKTOP_FAULT_PLAN']).with_name('release.json').exists()"}`)
    .replace("    preparation_step = 'storage'", `    captured_processes = [proof(os.getppid())]
    preparation_step = 'storage'`)
    .replace("stop_exact([123])", "stop_exact(captured_processes)")
    .replace("    any(alive(proof) for proof in proofs)", mode === "missed-exit-wait" ? "    pass" : "    any(alive(proof) for proof in proofs)")
    .replace("        pair_state()", mode === "missed-before-pair" ? "        swap_bundle()" : "        pair_state()")
    .replace("        storage_preflight()", mode === "missed-storage" ? "        pass" : "        storage_preflight()")
    .replace("        reserve_recovery_capacity()", mode === "missed-reserve" ? "        prepare_bundle()" : "        reserve_recovery_capacity()")
    .replace("def pair_state(*args):", "def prepare_bundle(): raise AssertionError('bundle escaped')\ndef swap_bundle(): raise AssertionError('swap escaped')\ndef pair_state(*args):");
}

async function custodyFixture(point, mode = "released") {
  await fixture(async (f) => {
    const selected = await installSelection(f);
    let prepared;
    let custody;
    let callback;
    const launcher = NodePath.join(f.root, "main.py");
    await NodeFSP.writeFile(launcher, String.raw`
import os, pathlib, subprocess, sys, time
plan = pathlib.Path(sys.argv[1])
deadline = time.monotonic() + 5
while not plan.exists():
    assert time.monotonic() < deadline
    time.sleep(0.01)
env = dict(os.environ, PYTHONPATH=str(plan.parent), PYTHONNOUSERSITE='1', PYTHONDONTWRITEBYTECODE='1',
           JONES_PACKAGED_DESKTOP_FAULT_PLAN=str(plan), JONES_PACKAGED_DESKTOP_INSTALL_FAULT='1', T3CODE_HOME=sys.argv[4])
raise SystemExit(subprocess.call(['/usr/bin/python3', sys.argv[2], '--manifest', sys.argv[3], '--activate', sys.argv[5]], env=env))
`, { mode: 0o600 });
    const proof = (pid) => ({ pid, startSec: 1, startUsec: 0, executable: '/control/' + pid });
    const result = await runOwnedChild('/usr/bin/python3', [launcher,
      NodePath.join(f.root, 'python-preload/fault-plan.json'), f.helper, f.manifest, selected.active.home, f.intent], {
      cwd: f.root, timeout: 5000, env: { PATH: '/usr/bin:/bin', HOME: f.root, CONTROL_PID: String(process.pid) },
      onSpawn: async (pid) => {
        await selected.prepare();
        const intent = JSON.parse(await NodeFSP.readFile(f.intent));
        intent.processes = [{ role: 'main', pid }];
        await NodeFSP.writeFile(f.intent, JSON.stringify(intent), { mode: 0o600 });
        prepared = await preparePackagedDesktopHelperFault({ fixtureRoot: f.root, helper: f.helper,
          manifest: f.manifest, selection: selected.selection, point, observeRelaunch: true,
          controller: { identity: proof(process.pid), main: { ...proof(mode === 'wrong-parent' ? pid + 1 : pid), startSec: mode === 'stale-parent' ? 2 : 1 }, timeoutMs: 500 } });
        if (['lost-controller', 'wrong-parent', 'stale-parent', 'missed-storage'].includes(mode)) return;
        const deadline = Date.now() + 2000;
        while (true) {
          try { custody = await prepared.readCustody(); break; }
          catch (error) { if (error.code !== 'ENOENT' || Date.now() >= deadline) throw error; }
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        if (mode === 'timeout') return;
        if (mode === 'selection-symlink') {
          await NodeFSP.rename(selected.selection, selected.selection + '.original');
          await NodeFSP.symlink(selected.selection + '.original', selected.selection);
        }
        if (mode === 'helper-drift') await NodeFSP.appendFile(f.helper, '\n');
        if (mode === 'manifest-drift') await NodeFSP.appendFile(f.manifest, '\n');
        if (mode === 'selection-drift') await NodeFSP.appendFile(selected.selection, '\n');
        if (mode === 'claim-drift') await NodeFSP.appendFile(NodePath.join(f.tx, 'prepare-intent.json'), ' ');
        if (mode === 'replayed-release') {
          await NodeFSP.writeFile(NodePath.join(f.root, 'python-preload/release.json'), '{}', { mode: 0o600 });
          return;
        }
        if (mode.endsWith('-drift') || mode === 'selection-symlink') {
          await NodeFSP.writeFile(NodePath.join(f.root, 'python-preload/release.json'), JSON.stringify({
            protocol: 1, planSha256: prepared.planSha256, custodySha256: custody.custodySha256,
          }), { mode: 0o600 });
        } else callback = await prepared.releaseCustody(custody);
      },
    });
    const passing = mode === 'released' || mode === 'released-relaunch';
    NodeAssert.equal(result.code, passing ? 0 : 125, result.stderr);
    if (passing) {
      const fault = await prepared.readReceipt();
      NodeAssert.equal(fault.pid, custody.helper.pid);
      NodeAssert.equal(fault.point, point);
      NodeAssert.equal(callback, undefined);
      if (mode === 'released-relaunch') {
        const receipt = await prepared.readRelaunchReceipt();
        NodeAssert.equal(receipt.pid, custody.helper.pid);
        NodeAssert.equal(receipt.state[f.active.databasePath].sha256,
          NodeCrypto.createHash('sha256').update('unchanged-before-relaunch').digest('hex'));
        NodeAssert.equal(await NodeFSP.readFile(NodePath.join(f.active.profile, 'started'), 'utf8'), 'started');
      }
      await NodeAssert.rejects(prepared.releaseCustody(custody), { code: 'EEXIST' });
    } else if (mode === 'mutated-before-relaunch') {
      await prepared.readReceipt();
      await NodeAssert.rejects(prepared.readRelaunchReceipt());
      await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(f.active.profile, 'started')), { code: 'ENOENT' });
    } else {
      await NodeAssert.rejects(NodeFSP.lstat(prepared.receiptPath), { code: 'ENOENT' });
    }
    for (const name of ['previous-pair', 'trial-descriptor.json', 'commit-grant.json'])
      await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(f.tx, name)), { code: 'ENOENT' });
  }, custodyControl(mode));
}

for (const point of packagedDesktopFaultPoints)
  NodeTest.test('controller releases same captured helper before ' + point, () => custodyFixture(point));
for (const mode of ['wrong-parent', 'stale-parent', 'selection-symlink', 'lost-controller', 'timeout', 'live-writer', 'unknown-writer', 'replayed-release', 'claim-drift', 'helper-drift', 'manifest-drift', 'selection-drift', 'missed-storage', 'missed-reserve', 'missed-exit-wait', 'missed-before-pair'])
  NodeTest.test('controller containment refuses ' + mode, () => custodyFixture(mode === 'missed-reserve' ? 'reserve' : mode === 'missed-exit-wait' ? 'exit-wait' : mode === 'missed-before-pair' ? 'before-pair' : 'storage', mode));

for (const point of packagedDesktopFaultPoints)
  NodeTest.test('controller quiet baseline precedes real relaunch entry for ' + point, () => custodyFixture(point, 'released-relaunch'));
NodeTest.test('controller refuses changed protected state before reopen', () => custodyFixture('storage', 'mutated-before-relaunch'));
