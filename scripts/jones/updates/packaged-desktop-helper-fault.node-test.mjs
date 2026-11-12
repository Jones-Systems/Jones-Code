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
