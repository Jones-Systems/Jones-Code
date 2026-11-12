import * as NodeCrypto from "node:crypto";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import * as NodeVM from "node:vm";
import {
  bindPackagedDesktopPair,
  observePackagedDesktopInstallRefusal,
  validatePackagedDesktopGuiSession,
  bindPackagedDesktopPreparedIntent,
  validatePackagedDesktopPreEffect,
  validatePackagedDesktopMainWindow,
  validatePackagedDesktopProcessIdentity,
  validatePackagedDesktopStartupSettlement,
  fingerprintPackagedDesktopBundle,
  readPackagedDesktopMetadata,
  validatePackagedDesktopDialog,
} from "./packaged-desktop-pair.mjs";
import { assertFixtureProcessesStopped, runOwnedChild } from "./native-startup-fixture.mjs";

// A standard packed ASAR control, never an executable or a packaged proof.
function archive(metadata, entryChange = {}) {
  const body = Buffer.from(JSON.stringify(metadata));
  const tree = Buffer.from(
    JSON.stringify({
      files: { "package.json": { size: body.length, offset: "0", ...entryChange } },
    }),
  );
  const padded = Math.ceil(tree.length / 4) * 4;
  const size = Buffer.alloc(8);
  size.writeUInt32LE(4, 0);
  size.writeUInt32LE(8 + padded, 4);
  const header = Buffer.alloc(8 + padded);
  header.writeUInt32LE(4 + padded, 0);
  header.writeUInt32LE(tree.length, 4);
  tree.copy(header, 8);
  return Buffer.concat([size, header, body]);
}

async function fixture(body) {
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-desktop-pair-"));
  await NodeFSP.chmod(root, 0o700);
  try {
    async function app(name, source) {
      const appPath = NodePath.join(root, name + ".app");
      await NodeFSP.mkdir(NodePath.join(appPath, "Contents/MacOS"), {
        recursive: true,
        mode: 0o700,
      });
      await NodeFSP.mkdir(NodePath.join(appPath, "Contents/Resources"), { mode: 0o700 });
      const executablePath = NodePath.join(appPath, "Contents/MacOS/Jones");
      await NodeFSP.writeFile(executablePath, "non-executable-control-bytes", { mode: 0o700 });
      const metadata = {
        name: "t3code",
        main: "apps/desktop/dist-electron/boot.cjs",
        version: "fixture-" + name,
        startupGateProtocol: 1,
        jonesSource: {
          repository: "Jones-Systems/Jones-Code",
          sha: source.repeat(40),
          tree: "b".repeat(40),
        },
      };
      const asar = NodePath.join(appPath, "Contents/Resources/app.asar");
      await NodeFSP.writeFile(asar, archive(metadata), { mode: 0o600 });
      await NodeFSP.symlink("Resources/app.asar", NodePath.join(appPath, "Contents/internal"));
      const fingerprint = await fingerprintPackagedDesktopBundle(appPath);
      return {
        appPath,
        executablePath,
        metadata,
        asar,
        sourceSha: metadata.jonesSource.sha,
        sourceTree: metadata.jonesSource.tree,
        version: metadata.version,
        appDigest: fingerprint.appDigest,
        bundleIdentifier: "com.t3tools.t3code",
      };
    }
    await body({
      root,
      active: await app("Previous", "a"),
      candidate: await app("Candidate", "c"),
    });
  } finally {
    assertFixtureProcessesStopped();
    await NodeFSP.rm(root, { recursive: true, force: false });
    await NodeAssert.rejects(NodeFSP.lstat(root), { code: "ENOENT" });
  }
}

NodeTest.test(
  "binds both unchanged ASAR app identities without executing their control bytes",
  async () => {
    await fixture(async ({ root, active, candidate }) => {
      const pair = await bindPackagedDesktopPair({ fixtureRoot: root, active, candidate });
      NodeAssert.equal(pair.active.metadata.version, active.version);
      NodeAssert.equal(pair.candidate.metadata.jonesSource.sha, candidate.sourceSha);
      await pair.assertUnchanged();
      await NodeFSP.chmod(candidate.asar, 0o400);
      await NodeAssert.rejects(pair.assertUnchanged());
    });
  },
);

NodeTest.test(
  "same-source payload tamper and stale source receipts are independently refused",
  async () => {
    await fixture(async ({ root, active, candidate }) => {
      await NodeAssert.rejects(
        bindPackagedDesktopPair({
          fixtureRoot: root,
          active,
          candidate: { ...candidate, sourceSha: "d".repeat(40) },
        }),
      );
      await NodeFSP.appendFile(candidate.executablePath, "tamper");
      await NodeAssert.rejects(bindPackagedDesktopPair({ fixtureRoot: root, active, candidate }));
    });
  },
);

NodeTest.test("bundle traversal refuses an escaping symlink", async () => {
  await fixture(async ({ root, candidate }) => {
    const outside = NodePath.join(root, "outside");
    await NodeFSP.writeFile(outside, "owned outside bundle", { mode: 0o600 });
    await NodeFSP.symlink(outside, NodePath.join(candidate.appPath, "escape"));
    await NodeAssert.rejects(fingerprintPackagedDesktopBundle(candidate.appPath));
  });
});

NodeTest.test(
  "ASAR metadata reader refuses truncation, unpacked/linked metadata and out-of-range offsets",
  async () => {
    await fixture(async ({ candidate }) => {
      for (const body of [
        Buffer.alloc(7),
        archive(candidate.metadata, { unpacked: true }),
        archive(candidate.metadata, { link: "elsewhere" }),
        archive(candidate.metadata, { offset: "9007199254740992" }),
        archive(candidate.metadata, { size: 300000 }),
      ]) {
        await NodeFSP.writeFile(candidate.asar, body);
        await NodeAssert.rejects(readPackagedDesktopMetadata(candidate.asar));
      }
    });
  },
);

NodeTest.test(
  "native dialog projection requires captured identity, exact refusal and successful owned dismissal",
  () => {
    const expected = {
      pid: 123,
      executable: "/fixture/Candidate.app/Contents/MacOS/Jones",
      code: "transaction-blocked",
      transactionId: "e".repeat(64),
    };
    const receipt = {
      protocol: 1,
      kind: "packaged-desktop-native-dialog",
      pid: expected.pid,
      executable: expected.executable,
      startSec: 12345,
      startUsec: 6789,
      code: expected.code,
      transactionId: expected.transactionId,
      title: "Jones Code could not start safely",
      text:
        "Jones Code could not start safely\nStartup refused (transaction-blocked).\nTransaction: " +
        expected.transactionId,
      role: "AXWindow",
      subrole: "AXDialog",
      dismissed: true,
    };
    NodeAssert.equal(validatePackagedDesktopDialog(receipt, expected), receipt);
    for (const change of [
      { pid: 124 },
      { executable: "/other/app" },
      { text: "fake dialog" },
      { code: "writer-mismatch" },
      { transactionId: null },
      { subrole: "AXStandardWindow" },
      { dismissed: false },
    ])
      NodeAssert.throws(() => validatePackagedDesktopDialog({ ...receipt, ...change }, expected));
  },
);

NodeTest.test(
  "owned startup hook gets the actual captured PID and settles its asynchronous observation",
  async () => {
    let pid;
    let observed = false;
    const result = await runOwnedChild(
      process.execPath,
      ["-e", "process.stdout.write(String(process.pid))"],
      {
        timeout: 5000,
        async onSpawn(actual) {
          pid = actual;
          await Promise.resolve();
          observed = true;
        },
      },
    );
    NodeAssert.equal(result.code, 0, result.stderr);
    NodeAssert.equal(Number(result.stdout), pid);
    NodeAssert.equal(observed, true);
    assertFixtureProcessesStopped();
  },
);

NodeTest.test(
  "startup observation failure preserves the cause and drains only the owned child",
  async () => {
    const failure = new Error("Owned observer control failed");
    await NodeAssert.rejects(
      runOwnedChild(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
        timeout: 5000,
        async onSpawn() {
          throw failure;
        },
      }),
      (cause) => cause === failure,
    );
    assertFixtureProcessesStopped();
  },
);

NodeTest.test(
  "pre-effect result refuses wrong transaction, wrong boundary and failed or unproved reopen",
  () => {
    const activation = { transactionId: "f".repeat(64), expected: { generation: "previous" } };
    for (const [point, phase, step] of [
      ["storage", "intent", "storage"],
      ["reserve", "preparing", "reserve"],
      ["exit-wait", "preparing", "stop-processes"],
      ["before-pair", "quiescent", "pair"],
    ]) {
      const fault = {
        point,
        phase,
        step,
        transactionId: activation.transactionId,
        function: {
          storage: "storage_preflight",
          reserve: "reserve_recovery_capacity",
          "exit-wait": "kernel_process_identity",
          "before-pair": "pair_state",
        }[point],
      };
      for (const expectedRelaunch of ["started", "skipped"]) {
        const journal = {
          intent: activation,
          phase: "rolled-back",
          preEffect: {
            phase,
            step,
            reason: "preparation-failed",
            diagnostics: { errno: 5 },
            relaunch: expectedRelaunch,
          },
        };
        const expected = { activation, fault, expectedRelaunch };
        NodeAssert.equal(validatePackagedDesktopPreEffect(journal, expected), journal);
        for (const change of [
          { phase: "blocked" },
          { intent: { ...activation, transactionId: "0".repeat(64) } },
          { relaunch: "failed" },
          { phaseBeforeBlock: phase },
          { preEffect: { ...journal.preEffect, step: "unproved" } },
          { preEffect: { ...journal.preEffect, relaunch: "failed" } },
          { preEffect: { ...journal.preEffect, diagnostics: {} } },
        ])
          NodeAssert.throws(() =>
            validatePackagedDesktopPreEffect({ ...journal, ...change }, expected),
          );
        NodeAssert.throws(() =>
          validatePackagedDesktopPreEffect(journal, {
            ...expected,
            fault: { ...fault, transactionId: "0".repeat(64) },
          }),
        );
      }
    }
  },
);

async function preparedFixture(body) {
  await fixture(async ({ root, active, candidate }) => {
    const home = NodePath.join(root, "home");
    const profile = NodePath.join(root, "profile");
    await NodeFSP.mkdir(home, { mode: 0o700 });
    await NodeFSP.mkdir(profile, { mode: 0o700 });
    const runtime = NodePath.join(home, "runtime");
    const updates = NodePath.join(runtime, "jones-updates");
    const transactionId = "e".repeat(64);
    const transaction = NodePath.join(updates, "transactions", transactionId);
    await NodeFSP.mkdir(transaction, { recursive: true, mode: 0o700 });
    const databasePath = NodePath.join(home, "state.sqlite");
    await NodeFSP.writeFile(databasePath, "binding-control-only", { mode: 0o600 });
    const manifest = NodePath.join(runtime, "jones-active-install.json");
    const intent = NodePath.join(transaction, "intent.json");
    const current = {
      protocol: 1,
      owner: "desktop",
      generation: "previous",
      transactionId: "bootstrap",
      home,
      profile,
      databasePath,
      environmentId: "disposable-control",
      ...Object.fromEntries(
        ["appPath", "executablePath", "sourceSha", "sourceTree", "version", "appDigest"].map(
          (key) => [key, active[key]],
        ),
      ),
    };
    const staged = Object.fromEntries(
      ["appPath", "executablePath", "sourceSha", "sourceTree", "version", "appDigest"].map(
        (key) => [key, candidate[key]],
      ),
    );
    staged.handle = "d".repeat(64);
    const continuationReceipt = NodePath.join(transaction, "continuation.json");
    await NodeFSP.mkdir(NodePath.join(updates, "staging"), { mode: 0o700 });
    const selectionPath = NodePath.join(updates, "staging/selection.json");
    const selectionBytes = Buffer.from(JSON.stringify({ active: current, app: staged }));
    const helperBytes = Buffer.from("not-executed helper provenance control");
    const helper = NodePath.join(
      updates,
      "activate-v1-" + NodeCrypto.createHash("sha256").update(helperBytes).digest("hex") + ".py",
    );
    const claim = {
      protocol: 1,
      preparationClaimProtocol: 3,
      transactionId,
      home,
      profile,
      databasePath,
      environmentId: current.environmentId,
      stagedHandle: staged.handle,
      selectionPath,
      selectionSha256: NodeCrypto.createHash("sha256").update(selectionBytes).digest("hex"),
    };
    const activation = {
      protocol: 1,
      transactionId,
      expected: current,
      staged,
      continuationReceipt,
    };
    const write = (name, value) =>
      NodeFSP.writeFile(name, Buffer.isBuffer(value) ? value : JSON.stringify(value), {
        mode: 0o600,
      });
    await write(manifest, current);
    await write(intent, activation);
    await write(helper, helperBytes);
    await write(selectionPath, selectionBytes);
    await write(continuationReceipt, { ...claim, prepared: true });
    await write(NodePath.join(transaction, "prepare-intent.json"), claim);
    await write(NodePath.join(transaction, "prepare-dispatched.json"), claim);
    const pair = await bindPackagedDesktopPair({ fixtureRoot: root, active, candidate });
    await body({
      root,
      pair,
      helper,
      manifest,
      intent,
      activation,
      current,
      staged,
      transaction,
      claim,
      selectionPath,
      selectionBytes,
      helperBytes,
      write,
      active,
    });
  });
}

NodeTest.test(
  "prepared-intent binding consumes exact existing claims and refuses a different staged app",
  async () => {
    await preparedFixture(
      async ({
        pair,
        helper,
        manifest,
        intent,
        activation,
        staged,
        active,
        transaction,
        claim,
        write,
      }) => {
        const binding = await bindPackagedDesktopPreparedIntent({ pair, helper, manifest, intent });
        await binding.assertInputsUnchanged();
        // These inert input controls never satisfy native helper provenance or execute it.
        await write(intent, { ...activation, staged: { ...staged, appPath: active.appPath } });
        await NodeAssert.rejects(
          bindPackagedDesktopPreparedIntent({ pair, helper, manifest, intent }),
        );
        await write(intent, activation);
        await write(NodePath.join(transaction, "prepare-dispatched.json"), {
          ...claim,
          transactionId: "d".repeat(64),
        });
        await NodeAssert.rejects(
          bindPackagedDesktopPreparedIntent({ pair, helper, manifest, intent }),
        );
        await write(NodePath.join(transaction, "prepare-dispatched.json"), claim);
        await write(NodePath.join(transaction, "journal.json"), {
          intent: activation,
          phase: "intent",
        });
        await NodeAssert.rejects(
          bindPackagedDesktopPreparedIntent({ pair, helper, manifest, intent }),
        );
        const interrupted = await bindPackagedDesktopPreparedIntent({
          pair,
          helper,
          manifest,
          intent,
          interruptedPhase: "intent",
        });
        NodeAssert.equal(interrupted.initialJournal.phase, "intent");
        NodeAssert.match(interrupted.initialJournalSha256, /^[a-f0-9]{64}$/);
        await write(NodePath.join(transaction, "journal.json"), {
          intent: activation,
          phase: "blocked",
        });
        await NodeAssert.rejects(
          bindPackagedDesktopPreparedIntent({
            pair,
            helper,
            manifest,
            intent,
            interruptedPhase: "intent",
          }),
        );
      },
    );
  },
);

const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

// Inert protocol controls test custody decisions and receipt relationships only.
// They do not claim a native Install, kernel watch, process exit or packaged pass.
async function refusedFixture(body) {
  await preparedFixture(async (f) => {
    const { root, current, activation, staged, transaction, claim, write } = f;
    const appIdentity = {
      pid: 123,
      uid: process.getuid(),
      startSec: 456,
      startUsec: 789,
      executable: f.pair.active.executablePath,
    };
    activation.processes = [{ pid: appIdentity.pid, role: "main" }];
    activation.listener = "synthetic-listener";
    await write(f.intent, activation);
    await write(NodePath.join(transaction, "activation-request.json"), activation);
    const { uid, ...proof } = appIdentity;
    NodeAssert.equal(uid, process.getuid());
    const prepare = {
      protocol: 1,
      transactionId: activation.transactionId,
      stagedHandle: staged.handle,
      processes: [proof],
      claimSha256: digest(
        await NodeFSP.readFile(NodePath.join(transaction, "prepare-intent.json")),
      ),
    };
    await write(NodePath.join(transaction, "prepare-request.json"), prepare);
    const attempts = NodePath.join(current.home, "runtime/jones-updates/attempt-selections");
    await NodeFSP.mkdir(attempts, { mode: 0o700 });
    const attemptPath = NodePath.join(
      attempts,
      digest(f.selectionPath + "\n" + claim.selectionSha256) + ".json",
    );
    await write(attemptPath, {
      protocol: 1,
      transactionId: activation.transactionId,
      stagedHandle: staged.handle,
      selectionPath: f.selectionPath,
      selectionSha256: claim.selectionSha256,
      expected: current,
    });
    const preload = NodePath.join(root, "python-preload");
    await NodeFSP.mkdir(preload, { mode: 0o700 });
    const rootInfo = await NodeFSP.lstat(root, { bigint: true });
    const plan = {
      protocol: 2,
      fixtureRoot: root,
      rootDevice: String(rootInfo.dev),
      rootInode: String(rootInfo.ino),
      helper: f.helper,
      manifest: f.manifest,
      helperSha256: digest(f.helperBytes),
      manifestSha256: digest(await NodeFSP.readFile(f.manifest)),
      selection: f.selectionPath,
      selectionSha256: claim.selectionSha256,
      point: "storage",
      observeRelaunch: false,
    };
    const planPath = NodePath.join(preload, "fault-plan.json");
    await write(planPath, plan);
    const receiptPath = NodePath.join(preload, "fault-receipt.json");
    const receipt = {
      protocol: 1,
      kind: "packaged-desktop-helper-fault",
      pid: 124,
      transactionId: activation.transactionId,
      point: "storage",
      phase: "intent",
      step: "storage",
      function: "storage_preflight",
      helperSha256: plan.helperSha256,
      manifestSha256: plan.manifestSha256,
      intentSha256: digest(await NodeFSP.readFile(f.intent)),
      planSha256: digest(await NodeFSP.readFile(planPath)),
      journalSha256: digest("initial-journal-control"),
    };
    await write(receiptPath, receipt);
    const journal = {
      intent: activation,
      phase: "rolled-back",
      preEffect: {
        phase: "intent",
        step: "storage",
        reason: "preparation-failed",
        diagnostics: { errno: 5 },
        relaunch: "skipped",
      },
    };
    await write(NodePath.join(transaction, "journal.json"), journal);
    await body({
      ...f,
      receipt,
      receiptPath,
      journal,
      prepare,
      attemptPath,
      appIdentity,
      fault: {
        argv: null,
        receiptPath,
        readReceipt: async () => JSON.parse(await NodeFSP.readFile(receiptPath)),
      },
    });
  });
}

NodeTest.test(
  "completed refusal binding preserves strict default and rejects stale or forged prepared relations",
  async () => {
    await refusedFixture(async (f) => {
      const args = { pair: f.pair, helper: f.helper, manifest: f.manifest, intent: f.intent };
      await NodeAssert.rejects(bindPackagedDesktopPreparedIntent(args));
      const refused = { ...args, outcome: "pre-effect-refused", fault: f.receipt };
      const binding = await bindPackagedDesktopPreparedIntent(refused);
      NodeAssert.equal(binding.initialJournal, null);
      NodeAssert.equal(binding.finalJournal.phase, "rolled-back");
      await binding.assertInputsUnchanged();
      const mutations = [
        [f.selectionPath, { active: f.current, app: { ...f.staged, handle: "f".repeat(64) } }],
        [NodePath.join(f.transaction, "prepare-dispatched.json"), { ...f.claim, prepared: true }],
        [
          NodePath.join(f.transaction, "prepare-request.json"),
          { ...f.prepare, claimSha256: "0".repeat(64) },
        ],
        [
          NodePath.join(f.transaction, "activation-request.json"),
          { ...f.activation, processes: [] },
        ],
        [f.attemptPath, { protocol: 1, transactionId: "0".repeat(64) }],
        [f.receiptPath, { ...f.receipt, intentSha256: "0".repeat(64) }],
        [NodePath.join(f.transaction, "journal.json"), { ...f.journal, phase: "blocked" }],
        [NodePath.join(f.transaction, "journal.json"), { ...f.journal, legacySettlement: {} }],
        [
          NodePath.join(f.transaction, "journal.json"),
          { ...f.journal, preEffect: { ...f.journal.preEffect, relaunch: "started" } },
        ],
      ];
      for (const [path, value] of mutations) {
        const original = await NodeFSP.readFile(path);
        await f.write(path, value);
        await NodeAssert.rejects(bindPackagedDesktopPreparedIntent(refused));
        await f.write(path, original);
      }
      await f.write(NodePath.join(f.transaction, "commit-grant.json"), {});
      await NodeAssert.rejects(bindPackagedDesktopPreparedIntent(refused));
    });
  },
);

function installDriver(f, fail = null) {
  const events = [];
  let stopCount = 0;
  const helperIdentity = {
    pid: f.receipt.pid,
    ppid: f.appIdentity.pid,
    uid: process.getuid(),
    startSec: 457,
    startUsec: 0,
    executable: "/synthetic/python",
  };
  const app = {
    pid: f.appIdentity.pid,
    output: () => ({}),
    requestStop: () => {
      events.push("app-stop");
      stopCount += 1;
      return true;
    },
    waitForClose: async () => {
      events.push("app-close");
      return { code: 0, signal: "SIGTERM" };
    },
  };
  const native = {
    captureApp: async () => {
      events.push("app-watch");
      return { identity: f.appIdentity, kernelExitWatchRegistered: fail !== "app-watch" };
    },
    connectApp: async () => ({
      close: async () => {
        events.push("bridge-close");
      },
      evaluate: async (expression) => {
        if (expression.startsWith("window.desktopBridge.installUpdate(")) {
          events.push("install");
          NodeAssert.equal(
            expression,
            "window.desktopBridge.installUpdate(" + JSON.stringify(f.staged.handle) + ")",
          );
          if (fail === "install-reply") throw new Error("unknown Install reply");
          return { accepted: true };
        }
        return events.includes("install")
          ? { status: "downloaded", jones: { phase: "blocked" } }
          : { jones: { phase: "staged", stagedHandle: f.staged.handle } };
      },
    }),
    assertAppAlive: async () => f.appIdentity,
    waitForFault: async () => {
      events.push("fault");
      if (fail === "fault") throw new Error("helper identity unavailable");
    },
    captureHelper: async () => {
      events.push("helper-watch");
      return {
        identity: { ...helperIdentity, ppid: fail === "parent" ? 999 : f.appIdentity.pid },
        kernelExitWatchRegistered: true,
      };
    },
    waitForHelperClosure: async () => {
      events.push("helper-exit");
      if (fail === "closure") throw new Error("helper closure unknown");
      return {
        app: f.appIdentity,
        helper: helperIdentity,
        kernelExitObserved: true,
        helperAbsent: fail !== "absence",
        appContinuouslyAlive: fail !== "app-exited",
      };
    },
    settleApp: async () => {
      events.push("settled");
      return {
        app: f.appIdentity,
        helper: helperIdentity,
        appKernelExitObserved: true,
        appGroupAbsent: true,
        portClosed: true,
        labelAbsent: true,
        mountsAbsent: true,
        processesAbsent: true,
      };
    },
  };
  return {
    events,
    stopCount: () => stopCount,
    args: {
      pair: f.pair,
      helper: f.helper,
      manifest: f.manifest,
      selection: f.selectionPath,
      stagedHandle: f.staged.handle,
      fault: f.fault,
      app,
      native,
    },
  };
}

NodeTest.test(
  "normal Install custody waits for helper kernel closure while the captured app stays alive, then stops and settles",
  async () => {
    await refusedFixture(async (f) => {
      const driver = installDriver(f);
      const result = await observePackagedDesktopInstallRefusal(driver.args);
      NodeAssert.equal(result.journal.preEffect.relaunch, "skipped");
      NodeAssert.equal(result.packagedDesktopMatrixPassed, false);
      NodeAssert.deepEqual(driver.events, [
        "app-watch",
        "install",
        "fault",
        "helper-watch",
        "helper-exit",
        "app-stop",
        "app-close",
        "settled",
        "bridge-close",
      ]);
      NodeAssert.equal(driver.stopCount(), 1);
    });
  },
);

NodeTest.test(
  "unknown Install effects and missing, stale or unsupported helper custody never terminate the app",
  async () => {
    for (const fail of ["install-reply", "fault", "parent", "closure", "absence", "app-exited"]) {
      await refusedFixture(async (f) => {
        const driver = installDriver(f, fail);
        await NodeAssert.rejects(observePackagedDesktopInstallRefusal(driver.args), (error) => {
          NodeAssert.equal(error.installCustody.installAttempted, true);
          NodeAssert.equal(error.installCustody.retained, true);
          return true;
        });
        NodeAssert.equal(driver.stopCount(), 0, fail);
        NodeAssert.ok(!driver.events.includes("settled"));
      });
    }
    await refusedFixture(async (f) => {
      const driver = installDriver(f, "app-watch");
      await NodeAssert.rejects(observePackagedDesktopInstallRefusal(driver.args));
      NodeAssert.ok(!driver.events.includes("install"));
      NodeAssert.equal(driver.stopCount(), 1);
      const missing = installDriver(f);
      delete missing.args.native.captureHelper;
      await NodeAssert.rejects(
        observePackagedDesktopInstallRefusal(missing.args),
        /Missing native custody hook/,
      );
      NodeAssert.ok(!missing.events.includes("install"));
    });
  },
);

NodeTest.test(
  "deadline and cancellation retain post-Install custody without a stop, including a failing bridge close",
  async () => {
    for (const mode of ["timeout", "cancel"]) {
      await refusedFixture(async (f) => {
        const driver = installDriver(f);
        const controller = new AbortController();
        driver.args.signal = controller.signal;
        driver.args.observationTimeoutMs = 20;
        if (mode === "timeout")
          driver.args.native.waitForHelperClosure = () => new Promise(() => {});
        else
          driver.args.native.waitForFault = async () => {
            controller.abort();
          };
        const connect = driver.args.native.connectApp;
        driver.args.native.connectApp = async (...args) => ({
          ...(await connect(...args)),
          close: async () => {
            throw new Error("bridge close failed");
          },
        });
        await NodeAssert.rejects(observePackagedDesktopInstallRefusal(driver.args), (error) => {
          NodeAssert.match(error.message, /timed out|abort|cancel/i);
          NodeAssert.equal(error.installCustody.retained, true);
          NodeAssert.equal(error.bridgeCloseError.message, "bridge close failed");
          return true;
        });
        NodeAssert.equal(driver.stopCount(), 0);
      });
    }
  },
);

NodeTest.test(
  "startup main-window projection rejects splash, dialog and stale process identity",
  () => {
    const expected = { pid: 456, executable: "/fixture/Previous.app/Contents/MacOS/Jones" };
    const receipt = {
      protocol: 1,
      kind: "packaged-desktop-main-window",
      ...expected,
      startSec: 12345,
      startUsec: 9876,
      role: "AXWindow",
      subrole: "AXStandardWindow",
      main: true,
      minimized: false,
      width: 1000,
      height: 800,
      title: "Jones Code",
    };
    NodeAssert.equal(validatePackagedDesktopMainWindow(receipt, expected), receipt);
    for (const change of [
      { pid: 457 },
      { executable: "/other/app" },
      { kind: "packaged-desktop-native-dialog" },
      { subrole: "AXDialog" },
      { width: 360, height: 220 },
      { width: NaN },
      { main: false },
      { minimized: true },
      { startSec: 0 },
    ])
      NodeAssert.throws(() =>
        validatePackagedDesktopMainWindow({ ...receipt, ...change }, expected),
      );
  },
);

NodeTest.test(
  "startup settlement binds each interrupted phase to its exact native terminal evidence",
  () => {
    const activation = { transactionId: "a".repeat(64), expected: { generation: "previous" } };
    for (const interruptedPhase of ["intent-only", "intent", "preparing", "quiescent"]) {
      const settlement = {
        basis:
          interruptedPhase === "intent-only" ? "unstarted-activation" : "interrupted-preparation",
        settledBy: "startup",
        evidence: {
          phase: interruptedPhase === "intent-only" ? "intent" : interruptedPhase,
          manifestUnchanged: true,
          pairAbsent: true,
          bundleUnswapped: true,
        },
      };
      const journal = { phase: "rolled-back", intent: activation, legacySettlement: settlement };
      const expected = { activation, interruptedPhase };
      NodeAssert.equal(validatePackagedDesktopStartupSettlement(journal, expected), journal);
      for (const change of [
        { phase: "blocked" },
        { phase: "intent" },
        { intent: {} },
        { legacySettlement: { ...settlement, settledBy: "caller" } },
        {
          legacySettlement: {
            ...settlement,
            evidence: { ...settlement.evidence, pairAbsent: false },
          },
        },
        {
          legacySettlement: {
            ...settlement,
            evidence: { ...settlement.evidence, phase: "swapped" },
          },
        },
      ])
        NodeAssert.throws(() =>
          validatePackagedDesktopStartupSettlement({ ...journal, ...change }, expected),
        );
    }
  },
);

NodeTest.test(
  "successful observation can stop only its captured live handle, and cannot signal after closure",
  async () => {
    let requestStop;
    const result = await runOwnedChild(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      timeout: 5000,
      onSpawn(_pid, control) {
        requestStop = control.requestStop;
        NodeAssert.equal(requestStop(), true);
      },
    });
    NodeAssert.equal(result.signal, "SIGTERM");
    NodeAssert.equal(requestStop(), false);
    assertFixtureProcessesStopped();
  },
);

NodeTest.test(
  "packaged identity projection binds live birth, native exit event, ESRCH and exact child reap",
  () => {
    const helper = "/owned/runtime/activate-v1-" + "a".repeat(64) + ".py";
    const expected = { ownerPid: 100, helper, helperSha256: "a".repeat(64) };
    const python = {
      executable: "/usr/bin/python3",
      sha256: "b".repeat(64),
      kernelExecutable: "/System/Python.app/Contents/MacOS/Python",
      kernelSha256: "d".repeat(64),
    };
    const argv = [python.executable, "-I", "-S", "-B", helper, "--process-identity", "101"];
    const live = {
      argv,
      code: 0,
      stdout: JSON.stringify({
        pid: 101,
        startSec: 12345,
        startUsec: 56,
        executable: python.kernelExecutable,
      }),
      stderr: "",
    };
    const exited = {
      argv,
      code: 1,
      stdout: "",
      stderr: JSON.stringify({ error: "process-exited", call: "proc_pidinfo", errno: 3 }),
    };
    const receipt = {
      protocol: 1,
      case: "packaged-desktop-process-identity",
      ownerPid: 100,
      capturedPid: 101,
      helperSha256: expected.helperSha256,
      python,
      ownerIdentity: {
        ...live,
        argv: [...argv.slice(0, -1), "100"],
        stdout: JSON.stringify({
          pid: 100,
          startSec: 12344,
          startUsec: 55,
          executable: python.kernelExecutable,
        }),
      },
      commands: [live, structuredClone(live), exited, structuredClone(exited)],
      exitEvent: { pid: 101, filter: -5, flags: 0, fflags: 0x80000000 },
      wait: { pid: 101, rawStatus: 0, exitCode: 0 },
    };
    NodeAssert.equal(validatePackagedDesktopProcessIdentity(receipt, expected), receipt);
    const mutations = [
      (value) => {
        value.ownerPid = 99;
      },
      (value) => {
        value.ownerIdentity.stdout = value.ownerIdentity.stdout.replace(
          python.kernelExecutable,
          "/foreign",
        );
      },
      (value) => {
        value.ownerIdentity.code = 1;
      },
      (value) => {
        value.python.kernelSha256 = "invalid";
      },
      (value) => {
        value.helperSha256 = "c".repeat(64);
      },
      (value) => {
        value.commands[0].argv.push("--manifest", "/foreign");
      },
      (value) => {
        value.commands[1].stdout = value.commands[1].stdout.replace("12345", "12346");
      },
      (value) => {
        value.commands[0].stdout = value.commands[0].stdout.replace(
          python.kernelExecutable,
          "/foreign",
        );
      },
      (value) => {
        value.commands[2].stderr = JSON.stringify({
          error: "process-exited",
          call: "proc_pidinfo",
          errno: null,
        });
      },
      (value) => {
        value.commands[2].stderr = JSON.stringify({
          error: "process-inspection-failed",
          call: "proc_pidinfo",
          errno: 1,
        });
      },
      (value) => {
        value.commands[2].code = 0;
      },
      (value) => {
        value.exitEvent.fflags = 0;
      },
      (value) => {
        value.exitEvent.flags = 0x4000;
      },
      (value) => {
        value.exitEvent.pid = 102;
      },
      (value) => {
        value.wait.rawStatus = 9;
      },
      (value) => {
        value.wait.pid = 102;
      },
    ];
    for (const mutate of mutations) {
      const changed = structuredClone(receipt);
      mutate(changed);
      NodeAssert.throws(() => validatePackagedDesktopProcessIdentity(changed, expected));
    }
  },
);

NodeTest.test(
  "GUI admission scopes lock requirements and rejects unavailable or unproved sessions",
  () => {
    const ready = {
      schema: "jones-packaged-desktop-gui-session/v1",
      sessionAvailable: true,
      onConsole: true,
      loginDone: true,
      screenLocked: false,
      sessionUid: 501,
    };
    const accepted = validatePackagedDesktopGuiSession(ready, { uid: 501 });
    NodeAssert.deepEqual(accepted, ready);
    NodeAssert.ok(Object.isFrozen(accepted));
    for (const lock of ["any", "unlocked"]) {
      for (const screenLocked of [false, true]) {
        const receipt = { ...ready, screenLocked };
        if (lock === "unlocked" && screenLocked)
          NodeAssert.throws(
            () => validatePackagedDesktopGuiSession(receipt, { uid: 501, lock }),
            /screen is locked/,
          );
        else {
          const result = validatePackagedDesktopGuiSession(receipt, { uid: 501, lock });
          NodeAssert.deepEqual(result, receipt);
          NodeAssert.ok(Object.isFrozen(result));
        }
      }
      for (const [change, message] of [
        [{ screenLocked: null }, /state is unknown/],
        [{ screenLocked: undefined }, /state is unknown/],
        [{ screenLocked: "false" }, /state is unknown/],
        [{ sessionAvailable: false }, /unavailable/],
        [{ sessionUid: 502 }, /another user/],
        [{ onConsole: false }, /not on console/],
        [{ loginDone: false }, /login is incomplete/],
      ]) {
        NodeAssert.throws(
          () => validatePackagedDesktopGuiSession({ ...ready, ...change }, { uid: 501, lock }),
          message,
        );
      }
    }
    for (const lock of [null, false, "sometimes"])
      NodeAssert.throws(
        () => validatePackagedDesktopGuiSession(ready, { uid: 501, lock }),
        /Unknown GUI lock requirement/,
      );
  },
);

NodeTest.test(
  "GUI projection admits the unlocked native dictionary and preserves refusals",
  async () => {
    const source = await NodeFSP.readFile(
      new URL("./packaged-desktop-pair.mjs", import.meta.url),
      "utf8",
    );
    const match = source.match(/const packagedDesktopSessionScript = String\.raw`([\s\S]*?)`;/u);
    NodeAssert.ok(match);
    const session = {
      kCGSSessionUserIDKey: 501,
      kCGSSessionOnConsoleKey: true,
      kCGSessionLoginDoneKey: true,
    };
    for (const [native, expected, refusal] of [
      [session, false, null],
      [{ ...session, CGSSessionScreenIsLocked: false }, false, null],
      [{ ...session, CGSSessionScreenIsLocked: true }, true, /screen is locked/],
      [{ ...session, CGSSessionScreenIsLocked: null }, null, /state is unknown/],
      [{ ...session, CGSSessionScreenIsLocked: undefined }, null, /state is unknown/],
      [{ ...session, CGSSessionScreenIsLocked: "false" }, null, /state is unknown/],
      [null, null, /unavailable/],
      [{ ...session, kCGSSessionUserIDKey: 502 }, false, /another user/],
      [{ ...session, kCGSSessionOnConsoleKey: false }, false, /not on console/],
      [{ ...session, kCGSessionLoginDoneKey: false }, false, /login is incomplete/],
    ]) {
      const receipt = JSON.parse(
        NodeVM.runInNewContext(
          match[1],
          {
            ObjC: { import() {}, deepUnwrap: (value) => value, castRefToObject: (value) => value },
            $: { CGSessionCopyCurrentDictionary: () => native },
          },
          { timeout: 1000 },
        ),
      );
      NodeAssert.equal(receipt.screenLocked, expected);
      if (refusal)
        NodeAssert.throws(() => validatePackagedDesktopGuiSession(receipt, { uid: 501 }), refusal);
      else NodeAssert.deepEqual(validatePackagedDesktopGuiSession(receipt, { uid: 501 }), receipt);
    }
  },
);
