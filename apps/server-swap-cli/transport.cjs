"use strict";

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const { spawn, execFile } = require("node:child_process");
const { promisify } = require("node:util");
const exec = promisify(execFile);

function absolute(value, label) {
  if (typeof value !== "string" || !path.isAbsolute(value) || /[\x00-\x1f\x7f]/.test(value)) {
    throw new Error(`${label} must be an absolute path without control characters`);
  }
  return value;
}

function quote(value) {
  return "'" + value.replaceAll("'", "'\\''") + "'";
}

function remoteArgs(target, command) {
  if (
    !target ||
    typeof target.ssh !== "string" ||
    !/^[A-Za-z0-9][A-Za-z0-9_.@-]*$/.test(target.ssh)
  ) {
    throw new Error("ssh must be an existing SSH host alias or user@host");
  }
  absolute(target.worker, "worker");
  absolute(target.plan, "plan");
  const commands = {
    plan: ["plan"],
    launch: ["worker", "launch"],
    status: ["status"],
    reconcile: ["reconcile"],
  };
  if (!Object.hasOwn(commands, command)) throw new Error("unsupported remote operation");
  if (command === "launch" && !/^[a-f0-9]{64}$/.test(target.planDigest || ""))
    throw new Error("Pinned launch planDigest required");
  const remote = [
    target.worker,
    ...commands[command],
    target.plan,
    ...(command === "launch" ? [target.planDigest] : []),
  ]
    .map(quote)
    .join(" ");
  return [
    "-T",
    "-o",
    "BatchMode=yes",
    "-o",
    "ForwardAgent=no",
    "-o",
    "ClearAllForwardings=yes",
    "-o",
    "StrictHostKeyChecking=yes",
    "-o",
    "ConnectTimeout=10",
    "--",
    target.ssh,
    remote,
  ];
}

async function remote(target, command, runner = exec, options = {}) {
  const args = remoteArgs(target, command);
  try {
    const result = await runner("ssh", args, {
      timeout: Math.max(1, Math.min(20000, options.timeoutMs ?? 20000)),
      maxBuffer: 1024 * 1024,
      encoding: "utf8",
    });
    const value = JSON.parse(result.stdout);
    if (!value || typeof value !== "object" || Array.isArray(value))
      throw new Error("invalid worker response");
    return value;
  } catch (error) {
    // The remote process can outlive SSH. A failed transport never authorizes replay.
    return {
      status: "unknown",
      reason: "SSH result unavailable; inspect the host journal before any new launch",
      transportCode: typeof error.code === "string" ? error.code : null,
    };
  }
}

function durableWrite(file, value) {
  const fd = fs.openSync(file, "wx", 0o600);
  try {
    fs.writeFileSync(fd, JSON.stringify(value) + "\n");
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  const dir = fs.openSync(path.dirname(file), "r");
  try {
    fs.fsyncSync(dir);
  } finally {
    fs.closeSync(dir);
  }
}

function launchPaths(planPath, core = require("./core.cjs")) {
  absolute(planPath, "plan");
  const plan = core.loadPlan(planPath);
  return { root: path.join(plan.transactionDir, "worker-launch"), digest: plan.digest };
}

async function workerMain(argv, dependencies = {}) {
  if (![2, 3].includes(argv.length) || !["launch", "status"].includes(argv[0])) {
    throw new Error("usage: worker launch|status /absolute/plan.json [expected-plan-sha256]");
  }
  const [operation, input] = argv;
  const planPath = fs.realpathSync(absolute(input, "plan"));
  const core = dependencies.core || require("./core.cjs");
  // Validation precedes any launch marker or child process.
  core.loadPlan(planPath);
  const { root, digest } = launchPaths(planPath, core);
  if (argv[2] !== undefined && (!/^[a-f0-9]{64}$/.test(argv[2]) || argv[2] !== digest)) {
    throw new Error("Plan digest changed; refusing launch before any claim or spawn");
  }
  const receiptPath = path.join(root, "spawn.json");
  if (fs.existsSync(root)) {
    if (fs.lstatSync(root).isSymbolicLink())
      throw new Error("launch directory cannot be a symlink");
    const receipt = fs.existsSync(receiptPath)
      ? JSON.parse(fs.readFileSync(receiptPath, "utf8"))
      : null;
    if (receipt && receipt.planDigest !== digest)
      throw new Error("Launch belongs to a different plan; transaction IDs cannot be reused");
    return {
      status: receipt ? "launched" : "unknown",
      planDigest: digest,
      receipt,
      reason: "Launch already recorded; observe or reconcile the transaction, do not launch again",
    };
  }
  if (operation === "status") return { status: "not-launched", planDigest: digest };
  fs.mkdirSync(path.dirname(root), { recursive: true, mode: 0o700 });
  if (fs.realpathSync(path.dirname(root)) !== path.dirname(root))
    throw new Error("Worker state path contains a symlink");
  const parent = fs.statSync(path.dirname(root));
  if (parent.uid !== process.getuid() || (parent.mode & 0o077) !== 0)
    throw new Error("Worker transaction directory must be private and owned by this user");
  try {
    fs.mkdirSync(root, { mode: 0o700 });
  } catch (error) {
    if (error.code === "EEXIST")
      return { status: "unknown", reason: "Concurrent launch claimed this plan" };
    throw error;
  }
  const snapshot = fs.readFileSync(planPath);
  if (crypto.createHash("sha256").update(snapshot).digest("hex") !== digest)
    throw new Error("Plan changed before launch");
  const snapshotPath = path.join(root, "plan.json");
  const snapshotFd = fs.openSync(snapshotPath, "wx", 0o600);
  try {
    fs.writeFileSync(snapshotFd, snapshot);
    fs.fsyncSync(snapshotFd);
  } finally {
    fs.closeSync(snapshotFd);
  }
  durableWrite(path.join(root, "intent.json"), { planDigest: digest, planPath, action: "apply" });
  await core.plan(snapshotPath);
  let isSea = false;
  try {
    isSea = require("node:sea").isSea();
  } catch {}
  const childArgs = isSea
    ? ["apply", snapshotPath]
    : [path.join(__dirname, "cli.cjs"), "apply", snapshotPath];
  const output = fs.openSync(path.join(root, "worker.log"), "ax", 0o600);
  try {
    const child = (dependencies.spawn || spawn)(process.execPath, childArgs, {
      detached: true,
      stdio: ["ignore", output, output],
      cwd: root,
    });
    await new Promise((resolve, reject) => {
      child.once("spawn", resolve);
      child.once("error", reject);
    });
    durableWrite(receiptPath, { pid: child.pid, planDigest: digest });
    child.unref();
    return { status: "launched", planDigest: digest, pid: child.pid };
  } finally {
    fs.closeSync(output);
  }
}

module.exports = { absolute, quote, remoteArgs, remote, durableWrite, workerMain, launchPaths };
