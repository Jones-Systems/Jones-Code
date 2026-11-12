import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

export const packagedDesktopFaultPoints = Object.freeze([
  "storage",
  "reserve",
  "exit-wait",
  "before-pair",
]);

const sha = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");

function exactKeys(record, keys) {
  NodeAssert.ok(record && typeof record === "object" && !Array.isArray(record));
  NodeAssert.deepEqual(Object.keys(record).sort(), [...keys].sort());
}

function immutable(record) {
  if (record && typeof record === "object") {
    for (const value of Object.values(record)) immutable(value);
    Object.freeze(record);
  }
  return record;
}

const launchObservationNames = Object.freeze([
  "launch-services.json",
  "launch-return.json",
  "launch-observation-failure.json",
]);

async function privatePath(root, filename, directory = false) {
  NodeAssert.equal(NodePath.resolve(filename), filename);
  NodeAssert.ok(filename.startsWith(root + NodePath.sep) || filename === root);
  NodeAssert.equal(await NodeFSP.realpath(filename), filename);
  let current = filename;
  while (true) {
    const info = await NodeFSP.lstat(current, { bigint: true });
    NodeAssert.equal(info.uid, BigInt(process.getuid()));
    NodeAssert.equal(info.mode & 0o077n, 0n, "Fixture paths must be owner-private.");
    NodeAssert.ok(
      current === filename && !directory ? info.isFile() && info.nlink === 1n : info.isDirectory(),
    );
    if (current === root) break;
    current = NodePath.dirname(current);
  }
}

async function readPrivate(root, filename, maximum) {
  await privatePath(root, filename);
  const handle = await NodeFSP.open(
    filename,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const info = await handle.stat();
    NodeAssert.ok(info.size <= maximum);
    const bytes = await handle.readFile();
    NodeAssert.equal(bytes.length, info.size);
    return bytes;
  } finally {
    await handle.close();
  }
}

async function writeExclusive(filename, bytes) {
  const handle = await NodeFSP.open(filename, "wx", 0o600);
  try {
    await handle.writeFile(bytes);
    await handle.sync();
  } finally {
    await handle.close();
  }
}

function recoveryStatePaths(active) {
  for (const key of ["databasePath", "profile"]) NodeAssert.ok(typeof active[key] === "string");
  return [
    ...["", "-wal", "-shm"].map((suffix) => active.databasePath + suffix),
    ...[
      "settings.json",
      "desktop-settings.json",
      "client-settings.json",
      "saved-environments.json",
      "environment-id",
    ].map((name) => NodePath.join(NodePath.dirname(active.databasePath), name)),
    active.profile,
  ];
}

async function snapshotRecoveryState(root, active) {
  let entries = 0;
  let bytes = 0;
  async function visit(filename, depth) {
    NodeAssert.ok(depth <= 16 && ++entries <= 2048);
    NodeAssert.equal(NodePath.resolve(filename), filename);
    NodeAssert.ok(filename.startsWith(root + NodePath.sep));
    let info;
    try {
      info = await NodeFSP.lstat(filename, { bigint: true });
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      NodeAssert.equal(
        await NodeFSP.realpath(NodePath.dirname(filename)),
        NodePath.dirname(filename),
      );
      return null;
    }
    NodeAssert.ok(!info.isSymbolicLink() && info.uid === BigInt(process.getuid()));
    NodeAssert.equal(await NodeFSP.realpath(filename), filename);
    const identity = { device: String(info.dev), inode: String(info.ino), mode: Number(info.mode) };
    if (info.isDirectory()) {
      const children = {};
      for (const name of (await NodeFSP.readdir(filename)).sort())
        Object.defineProperty(children, name, {
          value: await visit(NodePath.join(filename, name), depth + 1),
          enumerable: true,
        });
      const after = await NodeFSP.lstat(filename, { bigint: true });
      for (const key of ["dev", "ino", "mode", "mtimeNs", "ctimeNs"])
        NodeAssert.equal(after[key], info[key]);
      return { ...identity, children };
    }
    NodeAssert.ok(info.isFile() && info.nlink === 1n);
    bytes += Number(info.size);
    NodeAssert.ok(bytes <= 64 * 1024 * 1024);
    const handle = await NodeFSP.open(
      filename,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
    );
    try {
      const before = await handle.stat({ bigint: true });
      for (const key of ["dev", "ino", "mode", "size", "mtimeNs", "ctimeNs"])
        NodeAssert.equal(before[key], info[key]);
      const hash = NodeCrypto.createHash("sha256");
      for await (const chunk of handle.createReadStream({ autoClose: false })) hash.update(chunk);
      const after = await handle.stat({ bigint: true });
      const current = await NodeFSP.lstat(filename, { bigint: true });
      for (const key of ["dev", "ino", "mode", "size", "mtimeNs", "ctimeNs"]) {
        NodeAssert.equal(after[key], before[key]);
        NodeAssert.equal(current[key], before[key]);
      }
      return { ...identity, bytes: Number(info.size), sha256: hash.digest("hex") };
    } finally {
      await handle.close();
    }
  }
  const state = {};
  for (const filename of recoveryStatePaths(active))
    Object.defineProperty(state, filename, { value: await visit(filename, 0), enumerable: true });
  return state;
}

// The caller owns the disposable root and retains it on unknown native effects.
// This prepares instrumentation only; it neither executes nor qualifies an app pair.
export async function preparePackagedDesktopHelperFault({
  fixtureRoot,
  helper,
  manifest,
  intent,
  selection,
  point,
  observeRelaunch = false,
  controller,
}) {
  NodeAssert.ok(packagedDesktopFaultPoints.includes(point));
  const deferred = selection !== undefined;
  NodeAssert.ok(deferred ? intent === undefined : typeof intent === "string");
  NodeAssert.ok(
    !deferred || observeRelaunch === false || point === "before-pair" || controller !== undefined,
    "Deferred relaunch observation requires the quiet before-pair boundary.",
  );
  if (controller !== undefined) {
    NodeAssert.ok(deferred, "Controller custody requires normal Install selection.");
    NodeAssert.equal(observeRelaunch, true, "Controller custody requires pre-relaunch state observation.");
    exactKeys(controller, ["identity", "main", "timeoutMs"]);
    for (const identity of [controller.identity, controller.main]) {
      exactKeys(identity, ["pid", "startSec", "startUsec", "executable"]);
      for (const key of ["pid", "startSec", "startUsec"])
        NodeAssert.ok(Number.isSafeInteger(identity[key]) && identity[key] >= (key === "pid" ? 1 : 0));
      NodeAssert.ok(identity.startUsec < 1000000 && NodePath.isAbsolute(identity.executable));
    }
    NodeAssert.equal(controller.identity.pid, process.pid);
    NodeAssert.notEqual(controller.main.pid, process.pid);
    NodeAssert.ok(Number.isSafeInteger(controller.timeoutMs) && controller.timeoutMs >= 100 && controller.timeoutMs <= 60000);
  }
  await privatePath(fixtureRoot, fixtureRoot, true);
  const rootInfo = await NodeFSP.lstat(fixtureRoot, { bigint: true });
  const helperBytes = await readPrivate(fixtureRoot, helper, 2 * 1024 * 1024);
  const helperSha256 = sha(helperBytes);
  NodeAssert.equal(NodePath.basename(helper), "activate-v1-" + helperSha256 + ".py");
  const manifestBytes = await readPrivate(fixtureRoot, manifest, 65536);
  const intentBytes = deferred ? null : await readPrivate(fixtureRoot, intent, 65536);
  const active = JSON.parse(manifestBytes);
  const activation = deferred ? null : JSON.parse(intentBytes);
  NodeAssert.equal(active.protocol, 1);
  NodeAssert.equal(active.owner, "desktop");
  if (!deferred) {
    NodeAssert.equal(activation.protocol, 1);
    NodeAssert.match(activation.transactionId, /^[a-f0-9]{64}$/);
    NodeAssert.deepEqual(activation.expected, active);
  }
  await privatePath(fixtureRoot, active.home, true);
  NodeAssert.equal(manifest, NodePath.join(active.home, "runtime/jones-active-install.json"));
  if (!deferred)
    NodeAssert.equal(
      intent,
      NodePath.join(
        active.home,
        "runtime/jones-updates/transactions",
        activation.transactionId,
        "intent.json",
      ),
    );
  let selectionBytes;
  if (deferred) {
    selectionBytes = await readPrivate(fixtureRoot, selection, 131072);
    const selected = JSON.parse(selectionBytes);
    NodeAssert.deepEqual(selected.active, active);
    NodeAssert.match(selected.app.handle, /^[a-f0-9]{64}$/);
    NodeAssert.equal(
      NodePath.dirname(selection),
      NodePath.join(active.home, "runtime/jones-updates/staging"),
    );
  }
  NodeAssert.equal(NodePath.dirname(helper), NodePath.join(active.home, "runtime/jones-updates"));
  NodeAssert.equal(typeof observeRelaunch, "boolean");
  const stateBefore =
    observeRelaunch && !deferred ? await snapshotRecoveryState(fixtureRoot, active) : null;
  const plan = {
    protocol: deferred ? 2 : 1,
    fixtureRoot,
    rootDevice: String(rootInfo.dev),
    rootInode: String(rootInfo.ino),
    helper,
    helperSha256,
    manifest,
    manifestSha256: sha(manifestBytes),
    ...(deferred
      ? { selection, selectionSha256: sha(selectionBytes) }
      : {
          intent,
          intentSha256: sha(intentBytes),
          transactionId: activation.transactionId,
        }),
    point,
    observeRelaunch,
    ...(controller === undefined ? {} : { controller }),
  };
  const preload = NodePath.join(fixtureRoot, "python-preload");
  await NodeFSP.mkdir(preload, { mode: 0o700 });
  const planBytes = Buffer.from(JSON.stringify(plan));
  const planPath = NodePath.join(preload, "fault-plan.json");
  const launchInputs = new Map();
  await writeExclusive(
    NodePath.join(preload, "sitecustomize.py"),
    await NodeFSP.readFile(new URL("./packaged-desktop-helper-preload.py", import.meta.url)),
  );
  await writeExclusive(planPath, planBytes);
  const directory = await NodeFSP.open(
    preload,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY,
  );
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
  return Object.freeze({
    // Install creates its transaction later; only its exact activate child binds that intent.
    argv: deferred ? null : Object.freeze([helper, "--manifest", manifest, "--activate", intent]),
    environment: Object.freeze({
      PYTHONPATH: preload,
      PYTHONNOUSERSITE: "1",
      PYTHONDONTWRITEBYTECODE: "1",
      JONES_PACKAGED_DESKTOP_FAULT_PLAN: planPath,
      ...(deferred ? { JONES_PACKAGED_DESKTOP_INSTALL_FAULT: "1" } : {}),
      T3CODE_HOME: active.home,
    }),
    planPath,
    receiptPath: NodePath.join(preload, "fault-receipt.json"),
    planSha256: sha(planBytes),
    helperSha256,
    // This receipt is an observation. The caller must acquire native birth/exit
    // custody of the helper and captured writers before requesting their stop.
    async readCustody() {
      NodeAssert.ok(controller);
      NodeAssert.equal(sha(await readPrivate(fixtureRoot, planPath, 16384)), sha(planBytes));
      const info = await NodeFSP.lstat(fixtureRoot, { bigint: true });
      NodeAssert.equal(String(info.dev), plan.rootDevice);
      NodeAssert.equal(String(info.ino), plan.rootInode);
      for (const [filename, digest, limit] of [[helper, helperSha256, 2 * 1024 * 1024],
        [manifest, plan.manifestSha256, 65536], [selection, plan.selectionSha256, 131072]])
        NodeAssert.equal(sha(await readPrivate(fixtureRoot, filename, limit)), digest);
      const bytes = await readPrivate(fixtureRoot, NodePath.join(preload, "custody.json"), 65536);
      const record = JSON.parse(bytes);
      exactKeys(record, ["protocol", "kind", "planSha256", "helper", "parent", "processes", "transactionId", "intentSha256"]);
      NodeAssert.equal(record.protocol, 1);
      NodeAssert.equal(record.kind, "packaged-desktop-helper-custody");
      NodeAssert.equal(record.planSha256, sha(planBytes));
      NodeAssert.deepEqual(record.parent, controller.main);
      NodeAssert.ok(record.helper.pid !== record.parent.pid && record.helper.pid !== process.pid);
      NodeAssert.ok(record.processes.some((proof) => proof.pid === record.parent.pid && proof.startSec === record.parent.startSec &&
          proof.startUsec === record.parent.startUsec && proof.executable === record.parent.executable));
      NodeAssert.match(record.transactionId, /^[a-f0-9]{64}$/);
      NodeAssert.equal(sha(await readPrivate(fixtureRoot, NodePath.join(active.home,
        "runtime/jones-updates/transactions", record.transactionId, "intent.json"), 65536)), record.intentSha256);
      return immutable({ ...record, custodySha256: sha(bytes) });
    },
    // Release is only an acknowledgment. The same helper independently proves
    // captured writers exited and runs the real quiescence oracle before proceeding.
    async releaseCustody(custody) {
      NodeAssert.deepEqual(custody, await this.readCustody());
      const release = NodePath.join(preload, "release.json");
      if (NodeFS.existsSync(release)) {
        const error = new Error("Controller acknowledgment already exists.");
        error.code = "EEXIST";
        throw error;
      }
      const pending = NodePath.join(preload, "release-pending.json");
      await writeExclusive(pending, JSON.stringify({
        protocol: 1, planSha256: sha(planBytes), custodySha256: custody.custodySha256,
      }));
      await NodeFSP.link(pending, release);
      await NodeFSP.unlink(pending);
    },
    async readReceipt() {
      const receipt = JSON.parse(
        await readPrivate(fixtureRoot, NodePath.join(preload, "fault-receipt.json"), 16384),
      );
      NodeAssert.equal(receipt.protocol, 1);
      NodeAssert.equal(receipt.kind, "packaged-desktop-helper-fault");
      NodeAssert.equal(receipt.planSha256, sha(planBytes));
      let resolved = plan;
      if (deferred) {
        NodeAssert.match(receipt.transactionId, /^[a-f0-9]{64}$/);
        const intentPath = NodePath.join(
          active.home,
          "runtime/jones-updates/transactions",
          receipt.transactionId,
          "intent.json",
        );
        const bytes = await readPrivate(fixtureRoot, intentPath, 65536);
        const actual = JSON.parse(bytes);
        NodeAssert.equal(actual.protocol, 1);
        NodeAssert.equal(actual.transactionId, receipt.transactionId);
        NodeAssert.deepEqual(actual.expected, active);
        NodeAssert.equal(sha(await readPrivate(fixtureRoot, manifest, 65536)), plan.manifestSha256);
        NodeAssert.equal(
          sha(await readPrivate(fixtureRoot, selection, 131072)),
          plan.selectionSha256,
        );
        NodeAssert.deepEqual(actual.staged, JSON.parse(selectionBytes).app);
        resolved = { ...plan, transactionId: actual.transactionId, intentSha256: sha(bytes) };
      }
      for (const key of [
        "helperSha256",
        "manifestSha256",
        "intentSha256",
        "transactionId",
        "point",
      ])
        NodeAssert.equal(receipt[key], resolved[key]);
      NodeAssert.ok(Number.isSafeInteger(receipt.pid) && receipt.pid > 0);
      NodeAssert.match(receipt.journalSha256, /^[a-f0-9]{64}$/);
      const boundaries = {
        storage: ["storage_preflight", "intent", "storage"],
        reserve: ["reserve_recovery_capacity", "preparing", "reserve"],
        "exit-wait": ["kernel_process_identity", "preparing", "stop-processes"],
        "before-pair": ["pair_state", "quiescent", "pair"],
      };
      NodeAssert.deepEqual([receipt.function, receipt.phase, receipt.step], boundaries[point]);
      NodeAssert.equal(sha(await readPrivate(fixtureRoot, helper, 2 * 1024 * 1024)), helperSha256);
      return Object.freeze(receipt);
    },
    async readRelaunchReceipt() {
      NodeAssert.equal(observeRelaunch, true);
      const fault = await this.readReceipt();
      const bytes = await readPrivate(
        fixtureRoot,
        NodePath.join(preload, "before-relaunch.json"),
        1024 * 1024,
      );
      const receipt = JSON.parse(bytes);
      NodeAssert.equal(receipt.kind, "packaged-desktop-before-relaunch");
      for (const [key, value] of Object.entries(fault))
        if (key !== "kind" && key !== "journalSha256") NodeAssert.deepEqual(receipt[key], value);
      NodeAssert.equal(
        receipt.faultReceiptSha256,
        sha(await readPrivate(fixtureRoot, NodePath.join(preload, "fault-receipt.json"), 16384)),
      );
      NodeAssert.match(receipt.journalSha256, /^[a-f0-9]{64}$/);
      const baseline = deferred ? await this.readBeforePairState() : stateBefore;
      NodeAssert.deepEqual(
        receipt.state,
        baseline,
        "Protected fixture state changed before relaunch.",
      );
      return Object.freeze(receipt);
    },
    async readBeforePairState() {
      NodeAssert.ok(deferred && observeRelaunch && (point === "before-pair" || controller !== undefined));
      const fault = await this.readReceipt();
      const bytes = await readPrivate(
        fixtureRoot,
        NodePath.join(preload, "before-pair-state.json"),
        1024 * 1024,
      );
      NodeAssert.equal(sha(bytes), fault.beforePairStateSha256);
      const receipt = JSON.parse(bytes);
      NodeAssert.equal(receipt.kind, "packaged-desktop-before-pair-state");
      for (const key of ["pid", "planSha256", "transactionId", "intentSha256", "journalSha256"])
        NodeAssert.equal(receipt[key], fault[key]);
      NodeAssert.equal(receipt.processesStopped, true);
      NodeAssert.ok(receipt.state && typeof receipt.state === "object");
      return receipt.state;
    },
    // These are helper observations, not live handles or authority to clean a
    // launchd-born tree. Native custody must be acquired before launch effects.
    async readLaunchObservation() {
      NodeAssert.ok(deferred && observeRelaunch && (point === "before-pair" || controller !== undefined));
      await NodeAssert.rejects(
        NodeFSP.lstat(NodePath.join(preload, "launch-observation-failure.json")),
        { code: "ENOENT" },
      );
      const inputs = new Map();
      const read = async (filename, maximum = 16384) => {
        const bytes = await readPrivate(fixtureRoot, filename, maximum);
        const hash = sha(bytes);
        if (launchInputs.has(filename)) NodeAssert.equal(hash, launchInputs.get(filename));
        inputs.set(filename, hash);
        return bytes;
      };
      NodeAssert.equal(sha(await read(planPath)), sha(planBytes));
      const fault = await this.readReceipt();
      exactKeys(fault, [
        "protocol",
        "kind",
        "pid",
        "point",
        "function",
        "phase",
        "step",
        "transactionId",
        "planSha256",
        "helperSha256",
        "manifestSha256",
        "intentSha256",
        "journalSha256",
        "beforePairStateSha256",
      ]);
      await read(NodePath.join(preload, "fault-receipt.json"));
      const beforeRelaunch = await this.readRelaunchReceipt();
      exactKeys(beforeRelaunch, [...Object.keys(fault), "faultReceiptSha256", "state"]);
      const beforeBytes = await read(NodePath.join(preload, "before-relaunch.json"), 1024 * 1024);
      NodeAssert.deepEqual(JSON.parse(beforeBytes), beforeRelaunch);
      const quiet = JSON.parse(
        await read(NodePath.join(preload, "before-pair-state.json"), 1024 * 1024),
      );
      exactKeys(quiet, [
        "protocol",
        "kind",
        "processesStopped",
        "state",
        "pid",
        "planSha256",
        "transactionId",
        "intentSha256",
        "journalSha256",
      ]);
      NodeAssert.equal(quiet.protocol, 1);
      const intentPath = NodePath.join(
        active.home,
        "runtime/jones-updates/transactions",
        fault.transactionId,
        "intent.json",
      );
      const actual = JSON.parse(await read(intentPath, 65536));
      NodeAssert.equal(sha(await read(manifest, 65536)), plan.manifestSha256);
      NodeAssert.equal(sha(await read(helper, 2 * 1024 * 1024)), helperSha256);
      NodeAssert.equal(sha(await read(selection, 131072)), plan.selectionSha256);
      NodeAssert.equal(sha(await read(intentPath, 65536)), fault.intentSha256);
      const transaction = NodePath.dirname(intentPath);
      NodeAssert.equal(actual.continuationReceipt, NodePath.join(transaction, "continuation.json"));
      const claim = {
        protocol: 1,
        preparationClaimProtocol: 3,
        transactionId: fault.transactionId,
        stagedHandle: actual.staged.handle,
        selectionPath: selection,
        selectionSha256: plan.selectionSha256,
        ...Object.fromEntries(
          ["home", "profile", "databasePath", "environmentId"].map((key) => [key, active[key]]),
        ),
      };
      NodeAssert.deepEqual(JSON.parse(await read(actual.continuationReceipt, 65536)), {
        ...claim,
        prepared: true,
      });
      for (const name of ["prepare-intent.json", "prepare-dispatched.json"])
        NodeAssert.deepEqual(
          JSON.parse(await read(NodePath.join(transaction, name), 65536)),
          claim,
        );
      const servicesBytes = await read(NodePath.join(preload, "launch-services.json"));
      const services = JSON.parse(servicesBytes);
      const returned = JSON.parse(await read(NodePath.join(preload, "launch-return.json")));
      for (const [record, kind, sequence, fields] of [
        [services, "packaged-desktop-launch-services", 1, ["reply", "failure"]],
        [
          returned,
          "packaged-desktop-launch-return",
          2,
          ["launchServicesSha256", "identity", "failure"],
        ],
      ]) {
        exactKeys(record, [...Object.keys(fault), "sequence", "beforeRelaunchSha256", ...fields]);
        for (const [key, value] of Object.entries(fault))
          NodeAssert.deepEqual(record[key], key === "kind" ? kind : value);
        NodeAssert.equal(record.sequence, sequence);
        NodeAssert.equal(record.beforeRelaunchSha256, sha(beforeBytes));
      }
      NodeAssert.equal(returned.launchServicesSha256, sha(servicesBytes));
      const reply = services.reply;
      if (reply === null) {
        NodeAssert.equal(services.failure, "launch-services-return-unproved");
      } else {
        NodeAssert.equal(services.failure, null);
        NodeAssert.equal(reply.protocol, 1);
        if (reply.status === "launched") {
          exactKeys(reply, ["protocol", "status", "pid", "bundlePath", "executablePath"]);
          NodeAssert.ok(
            Number.isSafeInteger(reply.pid) && reply.pid > 0 && reply.pid <= 2147483647,
          );
          NodeAssert.notEqual(reply.pid, fault.pid);
          for (const [key, expected] of [
            ["bundlePath", active.appPath],
            ["executablePath", active.executablePath],
          ]) {
            NodeAssert.equal(reply[key], expected);
            NodeAssert.equal(NodePath.resolve(expected), expected);
            NodeAssert.ok(expected.startsWith(fixtureRoot + NodePath.sep));
            NodeAssert.equal(await NodeFSP.realpath(expected), expected);
          }
        } else if (reply.status === "failed") {
          exactKeys(reply, ["protocol", "status", "domain", "code"]);
          NodeAssert.match(reply.domain, /^[A-Za-z0-9_.-]{0,128}$/);
          NodeAssert.ok(Number.isSafeInteger(reply.code));
        } else {
          exactKeys(reply, ["protocol", "status"]);
          NodeAssert.equal(reply.status, "unavailable");
        }
      }
      if (returned.identity === null) {
        NodeAssert.equal(
          returned.failure,
          reply === null ? "launch-services-unproved" : "launch-validation-unproved",
        );
      } else {
        NodeAssert.equal(returned.failure, null);
        NodeAssert.equal(reply?.status, "launched");
        exactKeys(returned.identity, ["pid", "startSec", "startUsec", "executable"]);
        const identity = returned.identity;
        NodeAssert.equal(identity.pid, reply.pid);
        NodeAssert.equal(identity.executable, active.executablePath);
        NodeAssert.ok(Number.isSafeInteger(identity.startSec) && identity.startSec > 0);
        NodeAssert.ok(
          Number.isSafeInteger(identity.startUsec) &&
            identity.startUsec >= 0 &&
            identity.startUsec < 1000000,
        );
      }
      for (const [filename, hash] of inputs) {
        NodeAssert.equal(sha(await readPrivate(fixtureRoot, filename, 2 * 1024 * 1024)), hash);
        launchInputs.set(filename, hash);
      }
      await NodeAssert.rejects(
        NodeFSP.lstat(NodePath.join(preload, "launch-observation-failure.json")),
        { code: "ENOENT" },
      );
      return immutable({
        beforeRelaunch,
        launchServices: services,
        launchReturn: returned,
        externalDescendantSettlementRequired: true,
        activationAllowed: false,
        packagedDesktopMatrixPassed: false,
      });
    },
    async assertNoRelaunch() {
      NodeAssert.equal(observeRelaunch, true);
      for (const name of launchObservationNames)
        await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(preload, name)), { code: "ENOENT" });
      await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(preload, "before-relaunch.json")), {
        code: "ENOENT",
      });
      const baseline = deferred ? await this.readBeforePairState() : stateBefore;
      NodeAssert.deepEqual(await snapshotRecoveryState(fixtureRoot, active), baseline);
    },
    packagedDesktopMatrixPassed: false,
  });
}
