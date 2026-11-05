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
  point,
  observeRelaunch = false,
}) {
  NodeAssert.ok(packagedDesktopFaultPoints.includes(point));
  await privatePath(fixtureRoot, fixtureRoot, true);
  const rootInfo = await NodeFSP.lstat(fixtureRoot, { bigint: true });
  const helperBytes = await readPrivate(fixtureRoot, helper, 2 * 1024 * 1024);
  const helperSha256 = sha(helperBytes);
  NodeAssert.equal(NodePath.basename(helper), "activate-v1-" + helperSha256 + ".py");
  const manifestBytes = await readPrivate(fixtureRoot, manifest, 65536);
  const intentBytes = await readPrivate(fixtureRoot, intent, 65536);
  const active = JSON.parse(manifestBytes);
  const activation = JSON.parse(intentBytes);
  NodeAssert.equal(active.protocol, 1);
  NodeAssert.equal(active.owner, "desktop");
  NodeAssert.equal(activation.protocol, 1);
  NodeAssert.match(activation.transactionId, /^[a-f0-9]{64}$/);
  NodeAssert.deepEqual(activation.expected, active);
  await privatePath(fixtureRoot, active.home, true);
  NodeAssert.equal(manifest, NodePath.join(active.home, "runtime/jones-active-install.json"));
  NodeAssert.equal(
    intent,
    NodePath.join(
      active.home,
      "runtime/jones-updates/transactions",
      activation.transactionId,
      "intent.json",
    ),
  );
  NodeAssert.equal(NodePath.dirname(helper), NodePath.join(active.home, "runtime/jones-updates"));
  NodeAssert.equal(typeof observeRelaunch, "boolean");
  const stateBefore = observeRelaunch ? await snapshotRecoveryState(fixtureRoot, active) : null;
  const plan = {
    protocol: 1,
    fixtureRoot,
    rootDevice: String(rootInfo.dev),
    rootInode: String(rootInfo.ino),
    helper,
    helperSha256,
    manifest,
    manifestSha256: sha(manifestBytes),
    intent,
    intentSha256: sha(intentBytes),
    transactionId: activation.transactionId,
    point,
    observeRelaunch,
  };
  const preload = NodePath.join(fixtureRoot, "python-preload");
  await NodeFSP.mkdir(preload, { mode: 0o700 });
  const planBytes = Buffer.from(JSON.stringify(plan));
  const planPath = NodePath.join(preload, "fault-plan.json");
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
    argv: Object.freeze([helper, "--manifest", manifest, "--activate", intent]),
    environment: Object.freeze({
      PYTHONPATH: preload,
      PYTHONNOUSERSITE: "1",
      PYTHONDONTWRITEBYTECODE: "1",
      JONES_PACKAGED_DESKTOP_FAULT_PLAN: planPath,
      T3CODE_HOME: active.home,
    }),
    planPath,
    receiptPath: NodePath.join(preload, "fault-receipt.json"),
    planSha256: sha(planBytes),
    helperSha256,
    async readReceipt() {
      const receipt = JSON.parse(
        await readPrivate(fixtureRoot, NodePath.join(preload, "fault-receipt.json"), 16384),
      );
      NodeAssert.equal(receipt.protocol, 1);
      NodeAssert.equal(receipt.kind, "packaged-desktop-helper-fault");
      NodeAssert.equal(receipt.planSha256, sha(planBytes));
      for (const key of [
        "helperSha256",
        "manifestSha256",
        "intentSha256",
        "transactionId",
        "point",
      ])
        NodeAssert.equal(receipt[key], plan[key]);
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
      NodeAssert.deepEqual(
        receipt.state,
        stateBefore,
        "Protected fixture state changed before relaunch.",
      );
      return Object.freeze(receipt);
    },
    async assertNoRelaunch() {
      NodeAssert.equal(observeRelaunch, true);
      await NodeAssert.rejects(NodeFSP.lstat(NodePath.join(preload, "before-relaunch.json")), {
        code: "ENOENT",
      });
      NodeAssert.deepEqual(await snapshotRecoveryState(fixtureRoot, active), stateBefore);
    },
    packagedDesktopMatrixPassed: false,
  });
}
