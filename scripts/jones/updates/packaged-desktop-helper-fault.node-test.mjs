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
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-desktop-preload-"));
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
