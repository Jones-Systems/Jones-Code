import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import {
  qualificationSourcePins,
  assertQualificationDatabaseSource,
  assertCurrentDatabaseSource,
} from "./sources.mjs";

const helper = NodeURL.fileURLToPath(new URL("./historical-process.py", import.meta.url));
const checkout = NodePath.resolve(import.meta.dirname, "../../..");
const owners = new WeakMap();
const GiB = 1024 ** 3;
export const historicalPreparationSchema = "jones-historical-preparation/v1";
export const preparationPins = Object.freeze(
  ["history", "lease-current", "lease"].map((directory) =>
    qualificationSourcePins.find((pin) => pin.directory === directory),
  ),
);
const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const failure = (error) => ({
  code: error?.code ?? "preparation_failed",
  message: String(error?.message ?? error),
});
const inside = (parent, path) => path === parent || path.startsWith(`${parent}${NodePath.sep}`);
const identity = (path) => {
  const s = NodeFS.lstatSync(path);
  return { dev: s.dev, ino: s.ino, uid: s.uid };
};
function refuse(code, message) {
  throw Object.assign(new Error(message), { code });
}
function canonicalDirectory(path) {
  NodeAssert.equal(NodePath.resolve(path), path);
  NodeAssert.equal(NodeFS.realpathSync(path), path);
  const s = NodeFS.lstatSync(path);
  NodeAssert.ok(s.isDirectory() && s.uid === process.getuid());
  return path;
}
function state(owner) {
  const result = owners.get(owner);
  if (!result) refuse("invalid_owner", "original invocation owner required");
  return result;
}
export function validatePreparationBounds(bounds) {
  for (const key of [
    "maxBytes",
    "maxEntries",
    "maxFileBytes",
    "maxCacheBytes",
    "maxOutputBytes",
    "deadlineMs",
    "terminateGraceMs",
    "reapTimeoutMs",
    "maxProcesses",
    "estimatedPeakBytes",
  ])
    if (!Number.isSafeInteger(bounds?.[key]) || bounds[key] < 1)
      refuse("missing_bounds", `finite positive ${key} required`);
  if (bounds.estimatedPeakBytes < 10 * GiB)
    refuse(
      "missing_bounds",
      "additional peak must be at least 10 GiB and cover the complete workload",
    );
  NodeAssert.ok(bounds.maxFileBytes <= bounds.maxBytes && bounds.maxCacheBytes <= bounds.maxBytes);
  return Object.freeze({ ...bounds });
}
export function admitPreparation(owner) {
  const s = state(owner);
  assertPreparationIdentity(owner);
  const scan = inspectPreparation(owner);
  const filesystems = new Map();
  for (const path of [s.root, s.evidenceRoot]) {
    const info = NodeFS.statfsSync(path, { bigint: true });
    if ([0x01021994n, 0x858458f6n].includes(info.type))
      refuse("storage_refused", "disk-backed storage required");
    const available = info.bavail * info.bsize;
    const required = BigInt(30 * GiB) + BigInt(s.bounds.estimatedPeakBytes);
    if (available < required)
      refuse(
        "storage_refused",
        "30 GiB reserve plus declared aggregate additional peak unavailable",
      );
    filesystems.set(NodeFS.statSync(path).dev, {
      path,
      availableBytes: String(available),
      reserveBytes: 30 * GiB,
      estimatedPeakBytes: s.bounds.estimatedPeakBytes,
    });
  }
  const receipt = {
    filesystems: [...filesystems.values()],
    observedBytes: scan.bytes,
    reservation: false,
  };
  s.admissions.push(receipt);
  return receipt;
}
export function createHistoricalPreparation(options) {
  const bounds = validatePreparationBounds(options.bounds);
  NodeAssert.match(options.taskId, /^[a-zA-Z0-9._-]{1,100}$/);
  NodeAssert.match(options.runId, /^[a-zA-Z0-9._-]{1,100}$/);
  NodeAssert.ok(["release-after-closed-evidence", "retain"].includes(options.retention));
  const parent = canonicalDirectory(options.parentPath);
  const evidenceParent = canonicalDirectory(options.evidenceParent);
  for (const path of [parent, evidenceParent]) {
    NodeAssert.ok(!inside(checkout, path), "invocation storage must be outside the checkout");
    const info = NodeFS.statfsSync(path);
    NodeAssert.ok(![0x01021994, 0x858458f6].includes(info.type), "disk-backed storage required");
  }
  const root = NodePath.join(parent, `historical-${options.runId}`);
  NodeAssert.ok(
    !inside(root, evidenceParent) && !inside(evidenceParent, root),
    "separate evidence custody required",
  );
  const parentIdentity = identity(parent);
  // Creation itself is small; material producers additionally recheck admission.
  NodeFS.mkdirSync(root, { mode: 0o700 });
  const marker = NodeCrypto.randomUUID();
  NodeFS.writeFileSync(NodePath.join(root, ".invocation"), marker, { mode: 0o600, flag: "wx" });
  const rootIdentity = identity(root);
  const evidenceRoot = NodeFS.mkdtempSync(NodePath.join(evidenceParent, `${options.runId}-`));
  NodeFS.chmodSync(evidenceRoot, 0o700);
  const creation = Object.freeze({
    evidenceRoot,
    evidenceIdentity: identity(evidenceRoot),
    schema: historicalPreparationSchema,
    parent,
    root,
    parentIdentity,
    rootIdentity,
    marker,
    taskId: options.taskId,
    runId: options.runId,
    helperRevision: NodeChildProcess.execFileSync("git", ["-C", checkout, "rev-parse", "HEAD"], {
      encoding: "utf8",
    }).trim(),
    helperSha256: digest(NodeFS.readFileSync(NodeURL.fileURLToPath(import.meta.url))),
    supervisorSha256: digest(NodeFS.readFileSync(helper)),
  });
  const owner = Object.freeze({ creation });
  owners.set(owner, {
    ...options,
    bounds,
    parent,
    root,
    evidenceRoot,
    creation,
    children: [],
    active: false,
    consumerActive: false,
    closureUnknown: false,
    admissions: [],
    sources: [],
    deadlineAt: Date.now() + bounds.deadlineMs,
    outputBytes: 0,
  });
  try {
    for (const path of [
      "sources/history",
      "sources/lease-current",
      "sources/lease",
      "store",
      "home",
      "config",
      "cache",
      "tmp",
    ])
      NodeFS.mkdirSync(NodePath.join(root, path), { mode: 0o700, recursive: true });
    NodeFS.writeFileSync(NodePath.join(evidenceRoot, "creation.json"), JSON.stringify(creation), {
      flag: "wx",
      mode: 0o600,
    });
  } catch (error) {
    // No producer has run. Preserve exact custody if setup cannot be reconciled.
    error.preparationOwner = owner;
    throw error;
  }
  return owner;
}
export function assertPreparationIdentity(owner) {
  const s = state(owner);
  NodeAssert.deepEqual(identity(s.parent), s.creation.parentIdentity, "parent identity changed");
  NodeAssert.deepEqual(identity(s.root), s.creation.rootIdentity, "root identity changed");
  NodeAssert.equal(NodeFS.realpathSync(s.root), s.root);
  NodeAssert.equal(
    NodeFS.readFileSync(NodePath.join(s.root, ".invocation"), "utf8"),
    s.creation.marker,
  );
  NodeAssert.equal(NodeFS.lstatSync(s.root).mode & 0o777, 0o700);
  return s.creation;
}
export function inspectPreparation(owner, { consumption = false } = {}) {
  assertPreparationIdentity(owner);
  return inspectState(state(owner), consumption);
}
function inspectState(s, consumption) {
  const pending = [s.root],
    links = new Map();
  let entries = 0,
    bytes = 0,
    cacheBytes = 0;
  while (pending.length) {
    const path = pending.pop();
    const info = NodeFS.lstatSync(path);
    if (++entries > s.bounds.maxEntries) refuse("entry_limit", "inspection entry bound exceeded");
    NodeAssert.equal(info.dev, s.creation.rootIdentity.dev, "cross-device entry");
    NodeAssert.equal(info.uid, s.creation.rootIdentity.uid, "foreign entry owner");
    if (info.isSymbolicLink()) {
      if (consumption) {
        const target = NodeFS.realpathSync(path);
        const source = preparationPins
          .map(({ directory }) => NodePath.join(s.root, "sources", directory))
          .find((root) => inside(root, path));
        NodeAssert.ok(inside(source ?? s.root, target), "symbolic dependency escapes its checkout");
      }
    } else if (info.isDirectory()) {
      for (const name of NodeFS.readdirSync(path)) {
        pending.push(NodePath.join(path, name));
        if (pending.length + entries > s.bounds.maxEntries)
          refuse("entry_limit", "inspection entry bound exceeded");
      }
    } else if (info.isFile()) {
      if (info.size > s.bounds.maxFileBytes) refuse("file_limit", "file bound exceeded");
      const key = `${info.dev}:${info.ino}`;
      const link = links.get(key) ?? { count: 0, nlink: info.nlink };
      if (link.count === 0) bytes += info.size;
      link.count++;
      links.set(key, link);
      if (inside(NodePath.join(s.root, "cache"), path)) cacheBytes += info.size;
      if (bytes > s.bounds.maxBytes || cacheBytes > s.bounds.maxCacheBytes)
        refuse("storage_limit", "invocation storage bound exceeded");
    } else refuse("special_file", "special filesystem entry refused");
  }
  for (const link of links.values())
    if (link.count !== link.nlink)
      refuse("external_hardlink", "unaccounted hardlink outside the complete private root");
  const evidenceRoot = s.evidenceRoot ?? s.creation.evidenceRoot;
  if (evidenceRoot) {
    NodeAssert.deepEqual(
      identity(evidenceRoot),
      s.creation.evidenceIdentity,
      "evidence identity changed",
    );
    for (const name of NodeFS.readdirSync(evidenceRoot)) {
      const info = NodeFS.lstatSync(NodePath.join(evidenceRoot, name));
      NodeAssert.ok(
        info.isFile() && info.nlink === 1 && info.uid === process.getuid(),
        "private evidence files required",
      );
      if (++entries > s.bounds.maxEntries)
        refuse("entry_limit", "combined evidence entry bound exceeded");
      bytes += info.size;
      if (info.size > s.bounds.maxFileBytes || bytes > s.bounds.maxBytes)
        refuse("storage_limit", "combined evidence storage bound exceeded");
    }
  }
  return { entries, bytes, cacheBytes };
}
function privateEnvironment(s) {
  return {
    PATH: "/usr/bin:/bin",
    HOME: NodePath.join(s.root, "home"),
    TMPDIR: NodePath.join(s.root, "tmp"),
    TMP: NodePath.join(s.root, "tmp"),
    TEMP: NodePath.join(s.root, "tmp"),
    XDG_CONFIG_HOME: NodePath.join(s.root, "config"),
    XDG_CACHE_HOME: NodePath.join(s.root, "cache"),
    XDG_DATA_HOME: NodePath.join(s.root, "home"),
    CI: "1",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_TERMINAL_PROMPT: "0",
    GIT_OPTIONAL_LOCKS: "0",
    COREPACK_ENABLE_NETWORK: "0",
    COREPACK_ENABLE_AUTO_PIN: "0",
    npm_config_userconfig: "/dev/null",
    npm_config_globalconfig: "/dev/null",
    npm_config_manage_package_manager_versions: "false",
    npm_config_cache: NodePath.join(s.root, "cache"),
    PNPM_HOME: NodePath.join(s.root, "config"),
  };
}
export async function runPreparationProcess(owner, { executable, args, cwd, signal, env = {} }) {
  const s = state(owner);
  NodeAssert.ok(
    !s.active && !s.closureUnknown,
    "serial producers only; unknown prior effect fences launch",
  );
  signal?.throwIfAborted();
  NodeAssert.equal(NodeFS.realpathSync(executable), executable, "canonical executable required");
  NodeAssert.ok(NodeFS.statSync(executable).isFile());
  NodeAssert.ok(inside(s.root, canonicalDirectory(cwd)), "producer cwd must be private");
  const permitted = [
    "JONES_RUNTIME_BINDING",
    "JONES_PERFORMANCE_SOURCE_PARENT",
    "NODE_ENV",
    "JONES_HISTORICAL_RESOLUTION_REQUEST",
  ];
  NodeAssert.ok(
    Object.keys(env).every((key) => permitted.includes(key)),
    "inherited configuration refused",
  );
  admitPreparation(owner);
  const remainingMs = s.deadlineAt - Date.now(),
    remainingOutput = s.bounds.maxOutputBytes - s.outputBytes;
  if (remainingMs < 1 || remainingOutput < 1)
    refuse("invocation_limit", "invocation deadline or total output bound exhausted");
  const token = NodeCrypto.randomUUID();
  const requestPath = NodePath.join(s.evidenceRoot, `${token}.request.json`);
  const receiptPath = NodePath.join(s.evidenceRoot, `${token}.process.json`);
  const request = {
    token,
    root: s.root,
    rootIdentity: Object.values(s.creation.rootIdentity),
    bounds: { ...s.bounds, deadlineMs: remainingMs, maxOutputBytes: remainingOutput },
    evidenceRoot: s.evidenceRoot,
    evidenceIdentity: Object.values(s.creation.evidenceIdentity),
    executable,
    args,
    cwd,
    env: { ...privateEnvironment(s), ...env },
    receiptPath,
  };
  NodeFS.writeFileSync(requestPath, JSON.stringify(request), { mode: 0o600, flag: "wx" });
  s.active = true;
  let report;
  try {
    const supervisor = NodeChildProcess.spawn("/usr/bin/python3", ["-B", helper, requestPath], {
      cwd: s.root,
      env: privateEnvironment(s),
      stdio: ["ignore", "pipe", "pipe", "pipe"],
    });
    let diagnostics = "",
      exceeded = false;
    for (const stream of [supervisor.stdout, supervisor.stderr])
      stream.on("data", (chunk) => {
        if (diagnostics.length + chunk.length <= s.bounds.maxOutputBytes) diagnostics += chunk;
        else {
          exceeded = true;
          supervisor.stdio[3].end();
        }
      });
    const cancel = () => {
      if (!supervisor.stdio[3].destroyed) supervisor.stdio[3].end("cancel");
    };
    signal?.addEventListener("abort", cancel, { once: true });
    if (signal?.aborted) cancel();
    let spawnError;
    supervisor.once("error", (error) => {
      spawnError = error;
    });
    const watchdog = setTimeout(
      () => {
        s.closureUnknown = true;
        supervisor.kill("SIGKILL");
        supervisor.stdout.destroy();
        supervisor.stderr.destroy();
      },
      s.bounds.deadlineMs + s.bounds.terminateGraceMs + s.bounds.reapTimeoutMs + 1000,
    );
    const closed = await new Promise((resolve) =>
      supervisor.once("close", (code, termSignal) => resolve({ code, termSignal })),
    );
    clearTimeout(watchdog);
    signal?.removeEventListener("abort", cancel);
    supervisor.stdio[3].destroy();
    if (spawnError || closed.code !== 0 || exceeded)
      refuse(
        "supervisor_lost",
        `supervisor receipt unavailable: ${spawnError?.message ?? diagnostics}`,
      );
    const info = NodeFS.lstatSync(receiptPath);
    NodeAssert.ok(
      info.isFile() &&
        info.nlink === 1 &&
        info.size <= s.bounds.maxOutputBytes * 6 + s.bounds.maxProcesses * 256 + 16384,
    );
    report = JSON.parse(NodeFS.readFileSync(receiptPath, "utf8"));
    NodeAssert.equal(report.schema, "jones-historical-process/v1");
    NodeAssert.equal(report.token, token);
    s.children.push(report);
    s.outputBytes += report.outputBytes;
    if (!report.familyClosed || !report.streamsClosed || !report.reaped) s.closureUnknown = true;
    return report;
  } catch (error) {
    s.closureUnknown = true;
    throw error;
  } finally {
    s.active = false;
  }
}
export function preparationEvidencePath(owner) {
  return state(owner).evidenceRoot;
}
export function cleanupHistoricalPreparation(owner, { evidencePreserved, consumerClosed }) {
  const s = state(owner);
  const retained = (reason, error) => {
    let size;
    try {
      size = { observedBytes: inspectPreparation(owner).bytes };
    } catch (failure) {
      size = { unavailable: failure.message };
    }
    return {
      size,
      outcome: "retained",
      absent: false,
      root: s.root,
      owner: s.creation.taskId,
      reason,
      error,
      next: "original owner reconcile exact root and closure before retry",
    };
  };
  if (
    !evidencePreserved ||
    !consumerClosed ||
    s.consumerActive ||
    s.active ||
    s.closureUnknown ||
    s.children.some((child) => !child.familyClosed)
  )
    return retained("unknown_closure_or_evidence");
  if (s.retention === "retain") return retained("explicit_retention");
  try {
    const summary = inspectPreparation(owner);
    const requestPath = NodePath.join(
      s.evidenceRoot,
      `${NodeCrypto.randomUUID()}.cleanup-request.json`,
    );
    NodeFS.writeFileSync(requestPath, JSON.stringify({ ...s.creation, bounds: s.bounds }), {
      flag: "wx",
      mode: 0o600,
    });
    const result = NodeChildProcess.spawnSync(
      "/usr/bin/python3",
      ["-B", helper, "--cleanup", requestPath],
      {
        env: privateEnvironment(s),
        encoding: "utf8",
        timeout: s.bounds.reapTimeoutMs,
        maxBuffer: 8192,
      },
    );
    if (result.error || result.status !== 0)
      return retained("cleanup_failed", result.error?.message ?? result.stderr);
    const cleanup = JSON.parse(result.stdout);
    return { ...cleanup, ...summary, root: s.root };
  } catch (error) {
    return retained("inspection_or_identity_failed", failure(error));
  }
}
export function preservePreparationEvidence(owner, value) {
  const s = state(owner);
  const path = NodePath.join(s.evidenceRoot, `${NodeCrypto.randomUUID()}.evidence.json`);
  const bytes = JSON.stringify(value);
  NodeAssert.ok(
    Buffer.byteLength(bytes) <= s.bounds.maxOutputBytes,
    "evidence output bound exceeded",
  );
  const fd = NodeFS.openSync(path, "wx", 0o600);
  try {
    NodeFS.writeFileSync(fd, bytes);
    NodeFS.fsyncSync(fd);
  } finally {
    NodeFS.closeSync(fd);
  }
  return { path, sha256: digest(bytes) };
}

export function verifyHistoricalInputs(owner, directory) {
  return verifySourceInputs(state(owner), directory);
}
function verifySourceInputs(s, directory) {
  const pin = preparationPins.find((entry) => entry.directory === directory);
  NodeAssert.ok(pin);
  const worktreePath = NodePath.join(s.root, "sources", directory);
  const source = assertQualificationDatabaseSource(
    { repository: "Jones-Systems/Jones-Code", sourceRevision: pin.sourceRevision, worktreePath },
    { JONES_PERFORMANCE_SOURCE_PARENT: NodePath.join(s.root, "sources") },
  );
  const gitPath = NodePath.join(worktreePath, ".git");
  NodeAssert.ok(NodeFS.lstatSync(gitPath).isDirectory(), "private Git object custody required");
  NodeAssert.ok(
    !NodeFS.existsSync(NodePath.join(gitPath, "objects/info/alternates")),
    "Git alternates refused",
  );
  const files = ["package.json", "pnpm-workspace.yaml", "pnpm-lock.yaml"];
  const pending = [worktreePath];
  let count = 0;
  while (pending.length) {
    const path = pending.pop();
    if (++count > s.bounds.maxEntries) refuse("entry_limit", "input inspection bound exceeded");
    for (const entry of NodeFS.readdirSync(path, { withFileTypes: true })) {
      if ([".git", "node_modules"].includes(entry.name)) continue;
      const next = NodePath.join(path, entry.name),
        relative = NodePath.relative(worktreePath, next);
      if (entry.isDirectory()) pending.push(next);
      else if (entry.name === "package.json" || relative.endsWith(".patch")) files.push(relative);
    }
  }
  const inputs = Object.fromEntries(
    [...new Set(files)].sort().map((relative) => {
      const path = NodePath.join(worktreePath, relative),
        info = NodeFS.lstatSync(path);
      NodeAssert.ok(info.isFile() && info.nlink === 1 && info.size <= s.bounds.maxFileBytes);
      return [relative, digest(NodeFS.readFileSync(path))];
    }),
  );
  const manifest = JSON.parse(
    NodeFS.readFileSync(NodePath.join(worktreePath, "package.json"), "utf8"),
  );
  NodeAssert.equal(manifest.engines.node, "^24.13.1");
  NodeAssert.equal(manifest.packageManager, "pnpm@11.10.0");
  NodeAssert.equal(manifest.scripts.prepare, "effect-tsgo patch && vp config --no-agent");
  return {
    source,
    tree: pin.tree,
    inputs,
    prepare: "unexecuted-source-import-route",
    packagedBuild: "unqualified",
  };
}

async function successful(owner, request) {
  const result = await runPreparationProcess(owner, request);
  if (result.outcome !== "success" || !result.familyClosed)
    refuse("producer_failed", `producer ${result.outcome}`);
  return result;
}
export async function inspectHistoricalToolchain(owner, toolchain, signal) {
  const s = state(owner);
  const tools = {};
  for (const key of ["node", "pnpmCli", "git"]) {
    const supplied = toolchain?.[key];
    NodeAssert.ok(
      supplied && /^[a-f0-9]{64}$/.test(supplied.sha256),
      `verified ${key} path and digest required`,
    );
    NodeAssert.equal(NodeFS.realpathSync(supplied.path), supplied.path);
    const info = NodeFS.lstatSync(supplied.path);
    NodeAssert.ok(info.isFile() && info.size <= s.bounds.maxFileBytes);
    NodeAssert.equal(digest(NodeFS.readFileSync(supplied.path)), supplied.sha256);
    tools[key] = { ...supplied };
  }
  const run = (args) =>
    successful(owner, { executable: tools.node.path, args, cwd: s.root, signal });
  const nodeVersion = (await run(["--version"])).stdout.trim();
  NodeAssert.match(nodeVersion, /^v24\.(?:1[3-9]|[2-9]\d|\d{3,})\.\d+$/);
  NodeAssert.ok(nodeVersion !== "v24.13.0", "historical Node must satisfy ^24.13.1");
  NodeAssert.equal((await run([tools.pnpmCli.path, "--version"])).stdout.trim(), "11.10.0");
  const help = await run([tools.pnpmCli.path, "install", "--help"]);
  const argumentsToInspect = [
    "--frozen-lockfile",
    "--ignore-scripts",
    "--store-dir",
    "--cache-dir",
    "--config-dir",
  ];
  // A missing control is a capability gap, never permission to guess a flag.
  for (const argument of argumentsToInspect)
    NodeAssert.ok(
      help.stdout.includes(argument),
      `pnpm 11.10.0 does not advertise ${argument}; inspect exact supported control before execution`,
    );
  const audit = toolchain.configurationAudit;
  NodeAssert.ok(
    audit && audit.pnpmCliSha256 === tools.pnpmCli.sha256,
    "exact pnpm configuration audit required",
  );
  for (const key of [
    "manage-package-manager-versions",
    "userconfig",
    "globalconfig",
    "cache-dir",
    "store-dir",
    "config-dir",
  ]) {
    const item = audit.controls?.find((control) => control.name === key);
    NodeAssert.ok(
      item && item.inspected === true && /^[a-f0-9]{64}$/.test(item.evidenceSha256),
      `individually inspected ${key} control required`,
    );
    NodeAssert.equal(NodeFS.realpathSync(item.evidencePath), item.evidencePath);
    NodeAssert.ok(
      inside(s.evidenceParent, item.evidencePath),
      "configuration inspection evidence must be in declared evidence custody",
    );
    const info = NodeFS.lstatSync(item.evidencePath);
    NodeAssert.ok(info.isFile() && info.size <= s.bounds.maxOutputBytes);
    NodeAssert.equal(digest(NodeFS.readFileSync(item.evidencePath)), item.evidenceSha256);
  }
  return {
    ...tools,
    nodeVersion,
    pnpmVersion: "11.10.0",
    arguments: argumentsToInspect,
    helpSha256: digest(help.stdout),
    configurationAudit: audit,
  };
}

export async function withHistoricalPreparation(options, use, signal) {
  NodeAssert.equal(
    options.explicitHistoricalPreparation,
    true,
    "explicit frozen historical assignment required",
  );
  const candidate = assertCurrentDatabaseSource(options.candidate);
  NodeAssert.equal(candidate.worktreePath, checkout);
  NodeAssert.equal(typeof use, "function");
  const owner = createHistoricalPreparation(options),
    s = state(owner);
  let value,
    originalError,
    consumerClosed = true;
  try {
    const tools = await inspectHistoricalToolchain(owner, options.toolchain, signal);
    s.toolchain = tools;
    for (const pin of preparationPins) {
      const cwd = NodePath.join(s.root, "sources", pin.directory);
      const git = (args) =>
        successful(owner, {
          executable: tools.git.path,
          args: ["-c", "core.hooksPath=/dev/null", ...args],
          cwd,
          signal,
        });
      await git(["init", "--template=", "."]);
      await git([
        "-c",
        "protocol.file.allow=never",
        "fetch",
        "--no-tags",
        "--depth=1",
        "https://github.com/Jones-Systems/Jones-Code.git",
        pin.sourceRevision,
      ]);
      await git(["checkout", "--detach", pin.sourceRevision]);
      const before = verifyHistoricalInputs(owner, pin.directory);
      s.sources.push(before);
      await successful(owner, {
        executable: tools.node.path,
        args: [
          tools.pnpmCli.path,
          "install",
          "--frozen-lockfile",
          "--ignore-scripts",
          "--store-dir",
          NodePath.join(s.root, "store"),
          "--cache-dir",
          NodePath.join(s.root, "cache"),
          "--config-dir",
          NodePath.join(s.root, "config"),
        ],
        cwd,
        signal,
      });
      NodeAssert.deepEqual(verifyHistoricalInputs(owner, pin.directory), before);
    }
    inspectPreparation(owner, { consumption: true });
    const preparation = {
      schema: historicalPreparationSchema,
      creation: s.creation,
      candidate,
      sources: s.sources,
      toolchain: s.toolchain,
      bounds: s.bounds,
      sourceParent: NodePath.join(s.root, "sources"),
      environment: { JONES_PERFORMANCE_SOURCE_PARENT: NodePath.join(s.root, "sources") },
      children: s.children,
      dependencies: "frozen-installed-imports-unproved",
      packagedBuild: "unqualified",
    };
    const receipt = preservePreparationEvidence(owner, preparation);
    s.consumerActive = true;
    consumerClosed = false;
    const consumed = await use({ owner, preparation, receipt, signal });
    value = consumed?.value;
    originalError = consumed?.originalError;
    NodeAssert.equal(
      consumed?.consumerClosed,
      true,
      "consumer must prove phase/process/listener/root closure",
    );
    consumerClosed = true;
    s.consumerActive = false;
    for (const source of s.sources)
      NodeAssert.deepEqual(
        verifyHistoricalInputs(owner, NodePath.basename(source.source.worktreePath)),
        source,
      );
    assertCurrentDatabaseSource(candidate);
  } catch (error) {
    originalError ??= failure(error);
  } finally {
    try {
      for (const source of s.sources)
        NodeAssert.deepEqual(
          verifyHistoricalInputs(owner, NodePath.basename(source.source.worktreePath)),
          source,
        );
    } catch (error) {
      s.postconditionError = failure(error);
    }
  }
  return finishHistoricalPreparation(owner, { value, originalError, consumerClosed });
}
export function finishHistoricalPreparation(owner, { value, originalError, consumerClosed }) {
  const s = state(owner);
  let evidence, evidenceError;
  try {
    evidence = preservePreparationEvidence(owner, {
      creation: s.creation,
      children: s.children,
      sources: s.sources,
      toolchain: s.toolchain,
      admissions: s.admissions,
      value,
      originalError,
      postconditionError: s.postconditionError,
    });
  } catch (error) {
    evidenceError = failure(error);
  }
  const cleanup = cleanupHistoricalPreparation(owner, {
    evidencePreserved: Boolean(evidence),
    consumerClosed,
  });
  const result = {
    value,
    originalError,
    postconditionError: s.postconditionError,
    evidenceError,
    cleanup,
    evidence,
    root: s.root,
  };
  try {
    result.finalEvidence = preservePreparationEvidence(owner, result);
  } catch (error) {
    result.finalEvidenceError = failure(error);
  }
  return result;
}

export function assertHistoricalPreparationReceipt(path, trustedSha256) {
  NodeAssert.match(trustedSha256, /^[a-f0-9]{64}$/);
  NodeAssert.equal(NodeFS.realpathSync(path), path);
  const info = NodeFS.lstatSync(path);
  NodeAssert.ok(info.isFile() && info.nlink === 1 && info.size <= 16 * 1024 * 1024);
  const bytes = NodeFS.readFileSync(path);
  NodeAssert.equal(digest(bytes), trustedSha256, "trusted preparation receipt digest differs");
  const receipt = JSON.parse(bytes);
  NodeAssert.equal(receipt.schema, historicalPreparationSchema);
  NodeAssert.equal(receipt.dependencies, "frozen-installed-imports-unproved");
  NodeAssert.equal(receipt.packagedBuild, "unqualified");
  const bounds = validatePreparationBounds(receipt.bounds);
  NodeAssert.ok(info.size <= bounds.maxOutputBytes);
  const creation = receipt.creation;
  canonicalDirectory(creation.parent);
  canonicalDirectory(creation.root);
  NodeAssert.deepEqual(identity(creation.parent), creation.parentIdentity);
  NodeAssert.deepEqual(identity(creation.root), creation.rootIdentity);
  NodeAssert.equal(
    NodeFS.readFileSync(NodePath.join(creation.root, ".invocation"), "utf8"),
    creation.marker,
  );
  NodeAssert.equal(receipt.sourceParent, NodePath.join(creation.root, "sources"));
  NodeAssert.deepEqual(
    receipt.sources.map(({ source }) => NodePath.basename(source.worktreePath)),
    preparationPins.map(({ directory }) => directory),
  );
  const candidate = assertCurrentDatabaseSource(receipt.candidate);
  NodeAssert.equal(candidate.worktreePath, checkout);
  NodeAssert.equal(creation.helperRevision, candidate.sourceRevision);
  NodeAssert.equal(
    creation.helperSha256,
    digest(NodeFS.readFileSync(NodeURL.fileURLToPath(import.meta.url))),
  );
  NodeAssert.equal(creation.supervisorSha256, digest(NodeFS.readFileSync(helper)));
  NodeAssert.ok(
    receipt.children.length > 0 &&
      receipt.children.every(
        (child) =>
          child.familyClosed === true &&
          child.streamsClosed === true &&
          child.reaped === true &&
          child.outcome === "success",
      ),
  );
  NodeAssert.equal(receipt.toolchain.pnpmVersion, "11.10.0");
  for (const entry of receipt.sources)
    NodeAssert.deepEqual(
      verifySourceInputs(
        { root: creation.root, bounds },
        NodePath.basename(entry.source.worktreePath),
      ),
      entry,
    );
  inspectState({ root: creation.root, bounds, creation }, true);
  return receipt;
}

export function inspectHistoricalPreparationReceipt(receipt) {
  return inspectState(
    { root: receipt.creation.root, bounds: receipt.bounds, creation: receipt.creation },
    true,
  );
}
