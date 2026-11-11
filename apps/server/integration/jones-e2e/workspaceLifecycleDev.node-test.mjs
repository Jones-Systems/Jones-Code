/**
 * SOURCE candidate: prepared Node 26.12.1; explicit --live --operator /absolute/private/operator-bindings.json.
 * Operator bindings require exported source, fresh admission, native consent and a unique 0700 root containing only provider-home.
 * Preparation owns account access; this driver reads no credentials or native profile content.
 * An explicit fleet nativeBinding retains external history/cache custody; only public launcher/release/executable bytes are verified.
 * Mini approval: V3 403a9344c2f86b3a822505b115778b9e311536ec. Actual host is recorded; laptop evidence cannot qualify Mini.
 */
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeProcess from "node:process";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import * as NodeNet from "node:net";
import * as NodeTimersPromises from "node:timers/promises";

const schema = "jones.workspace-lifecycle-dev.proof/v1";
const directory = "apps/server/integration/jones-e2e";
const entryRelative = `${directory}/workspaceLifecycleDev.entrypoint.ts`;
const driverRelative = `${directory}/workspaceLifecycleDev.node-test.mjs`;
const MiB = 1024 ** 2,
  GiB = 1024 ** 3;
const limits = Object.freeze({
  seconds: 900,
  stopSeconds: 30,
  diagnostics: MiB,
  scratchBytes: 16 * MiB,
  entries: 10000,
});
const obligations = [
  "message",
  "empty",
  "resume",
  "source-read",
  "handoff-replay-restart",
  "setup-unknown",
  "terminal-first",
  "conversation-disposal",
  "settlement-disabled",
  "settlement-restart",
  "retirement-recreation",
];

class Unavailable extends Error {
  constructor(reason) {
    super(reason);
    this.name = "WorkspaceLifecycleDevUnavailable";
  }
}
const object = (value) => {
  NodeAssert.ok(value && typeof value === "object" && !Array.isArray(value));
  return value;
};
const exact = (value, keys) => {
  object(value);
  NodeAssert.deepEqual(Object.keys(value).sort(), [...keys].sort());
};
const text = (value) => {
  NodeAssert.ok(
    typeof value === "string" &&
      value.length > 0 &&
      value.length <= 4096 &&
      !/[\0\r\n]/u.test(value),
  );
  return value;
};
const absolute = (value) => {
  text(value);
  NodeAssert.ok(NodePath.isAbsolute(value) && NodePath.normalize(value) === value && value !== "/");
  return value;
};
const inside = (parent, child) => child === parent || child.startsWith(parent + NodePath.sep);
const sha = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const quote = (value) => `'${value.replaceAll("'", "'\\''")}'`;
const exists = async (file) =>
  NodeFSP.lstat(file).then(
    () => true,
    (e) => {
      if (e.code === "ENOENT") return false;
      throw e;
    },
  );
const readJson = async (file, limit = 65536) => {
  const handle = await NodeFSP.open(file, "r");
  try {
    const stat = await handle.stat();
    NodeAssert.ok(stat.isFile() && stat.size <= limit);
    const bytes = Buffer.alloc(limit + 1);
    const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
    NodeAssert.ok(bytesRead <= limit);
    return JSON.parse(bytes.subarray(0, bytesRead).toString("utf8"));
  } finally {
    await handle.close();
  }
};

const miniFleet = Object.freeze({
  home: "/Users/mjones",
  root: "/Users/mjones/.local/share/host-tooling/auth-fleet",
  release: "ff3bae196519c74abbe416ec99b6e7aba2a17c2f",
  nativeSha256: "a9d237949db816a0b7afd88a9a8e1df36b3a597a30f5a41d1c0539eb180a3450",
  default: {
    account: "malcolm",
    configDir: null,
    homePath: "",
    launcherSha256: "d3041b4f5c8d010ac9c080ba04346f01638c96bc1c383180b67d60dd7aa6ca8f",
    observation: "controller-bound-native-observation-unavailable",
  },
  jace: {
    account: "jace",
    configDir: "/Users/mjones/.claude-t3/jace",
    homePath: "/Users/mjones/.claude-t3/jace",
    launcherSha256: "fb631562eff373232e137eeb14e5e36db25397153e16d31d71f8c933277a041c",
    observation: "owner-attested-native-observation-unavailable",
  },
});
const fresh = (date) => {
  const age = Date.now() - Date.parse(date);
  NodeAssert.ok(
    Number.isFinite(age) && age >= 0 && age <= 120000,
    "fresh external admission/account binding required",
  );
};

function validateNativeBinding(provider, host) {
  if (!Object.hasOwn(provider, "nativeBinding")) {
    NodeAssert.ok(
      !/^claude-fleet-(default|jace)$/u.test(NodePath.basename(provider.launcher)),
      "fleet launchers require an explicit native binding",
    );
    return;
  }
  const binding = provider.nativeBinding;
  exact(binding, [
    "schema",
    "alias",
    "home",
    "homePath",
    "configDir",
    "release",
    "nativeExecutable",
    "accountRoute",
  ]);
  NodeAssert.equal(binding.schema, "jones.workspace-lifecycle-dev.claude-fleet-native-binding/v1");
  NodeAssert.equal(provider.driver, "claudeAgent");
  NodeAssert.equal(host.kind, "Apple Silicon Mini");
  NodeAssert.ok(["default", "jace"].includes(binding.alias));
  const tuple = miniFleet[binding.alias];
  NodeAssert.equal(binding.home, miniFleet.home);
  NodeAssert.equal(binding.homePath, tuple.homePath);
  NodeAssert.equal(binding.configDir, tuple.configDir);
  NodeAssert.equal(provider.launcher, `${miniFleet.root}/bin/claude-fleet-${binding.alias}`);
  NodeAssert.equal(provider.launcherSha256, tuple.launcherSha256);
  exact(binding.release, ["id", "manifestSha256", "qualificationReference"]);
  NodeAssert.equal(binding.release.id, miniFleet.release);
  NodeAssert.match(binding.release.manifestSha256, /^[a-f0-9]{64}$/u);
  text(binding.release.qualificationReference);
  exact(binding.nativeExecutable, ["path", "realPath", "sha256"]);
  NodeAssert.equal(binding.nativeExecutable.path, `${miniFleet.home}/.local/bin/claude`);
  absolute(binding.nativeExecutable.realPath);
  NodeAssert.ok(inside(`${miniFleet.home}/.local`, binding.nativeExecutable.realPath));
  NodeAssert.equal(
    NodePath.basename(NodePath.dirname(binding.nativeExecutable.realPath)),
    "versions",
  );
  NodeAssert.equal(NodePath.basename(binding.nativeExecutable.realPath), "2.1.322");
  NodeAssert.equal(binding.nativeExecutable.sha256, miniFleet.nativeSha256);
  const route = binding.accountRoute;
  exact(route, [
    "reference",
    "verifiedAt",
    "preparationReference",
    "account",
    "provider",
    "model",
    "binding",
    "action",
    "allowance",
    "observation",
  ]);
  text(route.reference);
  NodeAssert.equal(route.preparationReference, provider.preparationReference);
  NodeAssert.equal(route.account, tuple.account);
  NodeAssert.equal(route.provider, provider.driver);
  NodeAssert.equal(route.model, provider.model);
  NodeAssert.equal(route.binding, provider.accountBinding);
  NodeAssert.equal(route.action, "start");
  NodeAssert.ok(["unknown", "available"].includes(route.allowance));
  NodeAssert.equal(route.observation, tuple.observation);
  fresh(route.verifiedAt);
  fresh(provider.verifiedAt);
  NodeAssert.equal(provider.consent, "operator-validated-native-consent");
}

function providerInstanceBinding(provider, providerHome) {
  const binding = provider.nativeBinding;
  return {
    ...(binding === undefined
      ? {}
      : { environment: [{ name: "HOME", value: binding.home, sensitive: false }] }),
    config: { binaryPath: provider.launcher, homePath: binding?.homePath ?? providerHome },
  };
}

// Only public launcher/release source and the selected executable are read.
// Account/consent evidence is operator attestation, never a native auth probe.
async function verifyNativeBinding(provider, host, fileSystem = NodeFSP, digest = sha) {
  validateNativeBinding(provider, host);
  if (provider.nativeBinding === undefined) return { state: "private-provider-home" };
  const binding = provider.nativeBinding;
  const releaseRoot = `${miniFleet.root}/releases/${binding.release.id}`;
  NodeAssert.equal(await fileSystem.realpath(`${miniFleet.root}/current`), releaseRoot);
  const manifestPath = `${releaseRoot}/manifest.json`;
  const readPublic = async (file, cap, executable = false) => {
    const stat = await fileSystem.lstat(file);
    NodeAssert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.size <= cap && stat.uid === 501);
    NodeAssert.ok(!executable || (stat.mode & 0o111) !== 0);
    NodeAssert.equal(await fileSystem.realpath(file), file);
    return fileSystem.readFile(file);
  };
  const launcher = await readPublic(provider.launcher, MiB, true);
  NodeAssert.equal(digest(launcher), provider.launcherSha256);
  const manifestBytes = await readPublic(manifestPath, 65536);
  NodeAssert.equal(digest(manifestBytes), binding.release.manifestSha256);
  const manifest = object(JSON.parse(manifestBytes.toString("utf8")));
  const files = Object.entries(manifest);
  NodeAssert.ok(files.length > 0 && files.length <= 64);
  for (const required of ["claude_launcher.py", "core.py", "node.py"])
    NodeAssert.ok(Object.hasOwn(manifest, `src/host_tooling/auth_fleet/${required}`));
  let bytes = 0;
  for (const [relative, expectedDigest] of files) {
    NodeAssert.match(relative, /^src\/host_tooling\/(?:[a-z_]+\/)*[a-z_]+\.py$/u);
    NodeAssert.match(expectedDigest, /^[a-f0-9]{64}$/u);
    const content = await readPublic(`${releaseRoot}/${relative}`, MiB);
    bytes += content.length;
    NodeAssert.ok(bytes <= 4 * MiB);
    NodeAssert.equal(digest(content), expectedDigest);
  }
  NodeAssert.equal(
    await fileSystem.realpath(binding.nativeExecutable.path),
    binding.nativeExecutable.realPath,
  );
  const native = await readPublic(binding.nativeExecutable.realPath, 256 * MiB, true);
  NodeAssert.equal(digest(native), binding.nativeExecutable.sha256);
  // Recheck the mutable pointer after verifying the qualified closure.
  NodeAssert.equal(await fileSystem.realpath(`${miniFleet.root}/current`), releaseRoot);
  return {
    state: "qualified-launcher-release-executable-chain-observed",
    release: binding.release,
    nativeExecutable: binding.nativeExecutable,
    files: files.length,
    account: { state: "not-observed", operatorAttestation: binding.accountRoute },
  };
}

function validateOperator(v) {
  exact(v, [
    "schema",
    "source",
    "tools",
    "host",
    "root",
    "receipt",
    "environmentId",
    "projectId",
    "provider",
    "admission",
  ]);
  NodeAssert.equal(v.schema, "jones.workspace-lifecycle-dev.operator/v1");
  exact(v.source, [
    "root",
    "commit",
    "tree",
    "manifest",
    "manifestSha256",
    "v3Root",
    "v3Commit",
    "v3SrcTree",
    "v3Manifest",
    "v3ManifestSha256",
  ]);
  for (const name of ["root", "v3Root", "manifest", "v3Manifest"]) absolute(v.source[name]);
  for (const name of ["commit", "tree", "v3Commit", "v3SrcTree"])
    NodeAssert.match(v.source[name], /^[a-f0-9]{40}$/u);
  for (const name of ["manifestSha256", "v3ManifestSha256"])
    NodeAssert.match(v.source[name], /^[a-f0-9]{64}$/u);
  exact(v.tools, [
    "node",
    "nodeSha256",
    "pnpm",
    "pnpmSha256",
    "git",
    "python",
    "pythonSha256",
    "path",
  ]);
  for (const name of ["node", "pnpm", "git", "python"]) absolute(v.tools[name]);
  for (const name of ["nodeSha256", "pnpmSha256", "pythonSha256"])
    NodeAssert.match(v.tools[name], /^[a-f0-9]{64}$/u);
  text(v.tools.path);
  exact(v.host, ["hostname", "kind", "executionEnvironmentId", "approvalReference"]);
  text(v.host.hostname);
  text(v.host.executionEnvironmentId);
  text(v.host.approvalReference);
  NodeAssert.ok(["Apple Silicon Mini", "Apple Silicon laptop"].includes(v.host.kind));
  exact(v.root, ["path", "dev", "ino", "uid", "gid", "cleanupAuthorized"]);
  absolute(v.root.path);
  NodeAssert.match(
    NodePath.basename(v.root.path),
    /^jones-workspace-lifecycle-dev-[a-f0-9-]{36}$/u,
  );
  for (const name of ["dev", "ino", "uid", "gid"])
    NodeAssert.ok(Number.isSafeInteger(v.root[name]) && v.root[name] >= 0);
  NodeAssert.equal(v.root.cleanupAuthorized, true);
  absolute(v.receipt);
  text(v.environmentId);
  text(v.projectId);
  exact(v.provider, [
    "driver",
    "launcher",
    "launcherSha256",
    "model",
    "accountBinding",
    "verifiedAt",
    "consent",
    "preparationReference",
    ...(Object.hasOwn(v.provider, "nativeBinding") ? ["nativeBinding"] : []),
  ]);
  // Each invocation selects one of the two shipped conversation adapters.
  // The other adapter remains unqualified until its own real invocation passes.
  NodeAssert.ok(["codex", "claudeAgent"].includes(v.provider.driver));
  absolute(v.provider.launcher);
  NodeAssert.match(v.provider.launcherSha256, /^[a-f0-9]{64}$/u);
  for (const name of ["model", "accountBinding", "verifiedAt", "preparationReference"])
    text(v.provider[name]);
  NodeAssert.equal(v.provider.consent, "operator-validated-native-consent");
  exact(v.admission, [
    "cpuUsedPercent",
    "availableMemoryBytes",
    "observedAt",
    "preparationReference",
  ]);
  NodeAssert.ok(
    Number.isFinite(v.admission.cpuUsedPercent) &&
      v.admission.cpuUsedPercent >= 0 &&
      v.admission.cpuUsedPercent < 85,
  );
  NodeAssert.ok(
    Number.isSafeInteger(v.admission.availableMemoryBytes) && v.admission.availableMemoryBytes > 0,
  );
  text(v.admission.preparationReference);
  for (const date of [v.admission.observedAt, v.provider.verifiedAt]) fresh(date);
  validateNativeBinding(v.provider, v.host);
  const binding = v.provider.nativeBinding;
  if (binding !== undefined) {
    NodeAssert.ok(!inside(v.root.path, binding.home), "scratch must never contain native HOME");
    const protectedPaths = [
      `${binding.home}/.claude`,
      `${binding.home}/.claude.json`,
      ...(binding.configDir === null ? [] : [binding.configDir]),
      miniFleet.root,
      binding.nativeExecutable.path,
      binding.nativeExecutable.realPath,
    ];
    for (const target of protectedPaths) {
      NodeAssert.ok(!inside(target, v.root.path) && !inside(v.root.path, target));
      NodeAssert.ok(!inside(target, v.receipt) && !inside(v.receipt, target));
    }
  }
  return v;
}

// Preparation exports committed source without borrowing a checkout's history or
// dependencies. A bounded explicit file manifest binds those bytes AND the exact
// Git tree, including modes and symlink blobs, without writing Git objects.
// Manifest: {schema,commit,tree,files:[{path,mode,sha256}]} with relative paths.
async function verifyExport(root, manifestPath, manifestSha256, commit, tree) {
  const manifest = await readJson(manifestPath, 8 * MiB);
  NodeAssert.equal(sha(await NodeFSP.readFile(manifestPath)), manifestSha256);
  exact(manifest, ["schema", "commit", "tree", "files"]);
  NodeAssert.equal(manifest.schema, "jones.workspace-lifecycle-dev.source-files/v1");
  NodeAssert.equal(manifest.commit, commit);
  NodeAssert.equal(manifest.tree, tree);
  NodeAssert.ok(
    Array.isArray(manifest.files) && manifest.files.length > 0 && manifest.files.length <= 40000,
  );
  const nodes = new Map(),
    paths = new Set();
  let total = 0;
  for (const file of manifest.files) {
    exact(file, ["path", "mode", "sha256"]);
    text(file.path);
    NodeAssert.ok(
      !NodePath.isAbsolute(file.path) &&
        !file.path.split("/").some((part) => !part || part === "." || part === ".."),
    );
    NodeAssert.ok(!paths.has(file.path));
    paths.add(file.path);
    NodeAssert.ok(["100644", "100755", "120000"].includes(file.mode));
    NodeAssert.match(file.sha256, /^[a-f0-9]{64}$/u);
    const target = NodePath.join(root, file.path),
      stat = await NodeFSP.lstat(target);
    NodeAssert.ok(stat.size <= 64 * MiB);
    NodeAssert.ok(inside(root, await NodeFSP.realpath(NodePath.dirname(target))));
    const bytes =
      file.mode === "120000"
        ? Buffer.from((NodeAssert.ok(stat.isSymbolicLink()), await NodeFSP.readlink(target)))
        : (NodeAssert.ok(stat.isFile() && !stat.isSymbolicLink()), await NodeFSP.readFile(target));
    if (file.mode !== "120000")
      NodeAssert.equal(Boolean(stat.mode & 0o111), file.mode === "100755");
    total += bytes.length;
    NodeAssert.ok(total <= 512 * MiB);
    NodeAssert.equal(sha(bytes), file.sha256);
    const oid = NodeCrypto.createHash("sha1")
      .update(`blob ${bytes.length}\0`)
      .update(bytes)
      .digest();
    let parent = nodes;
    const segments = file.path.split("/"),
      name = segments.pop();
    for (const segment of segments) {
      if (!parent.has(segment)) parent.set(segment, new Map());
      NodeAssert.ok(parent.get(segment) instanceof Map);
      parent = parent.get(segment);
    }
    NodeAssert.ok(!parent.has(name));
    parent.set(name, { mode: file.mode, oid });
  }
  const treeOid = (children) => {
    const ordered = [...children.entries()].sort(([a, left], [b, right]) =>
      Buffer.compare(
        Buffer.from(a + (left instanceof Map ? "/" : "")),
        Buffer.from(b + (right instanceof Map ? "/" : "")),
      ),
    );
    const bytes = Buffer.concat(
      ordered.flatMap(([name, child]) => [
        Buffer.from(`${child instanceof Map ? "40000" : child.mode} ${name}\0`),
        child instanceof Map ? treeOid(child) : child.oid,
      ]),
    );
    return NodeCrypto.createHash("sha1").update(`tree ${bytes.length}\0`).update(bytes).digest();
  };
  NodeAssert.equal(treeOid(nodes).toString("hex"), tree);
  return { commit, tree, files: paths.size, bytes: total, manifestSha256 };
}

// Native process identity uses PID + start time + UID + PGID. No argv/name/path
// selects a process for signalling. Full-table metadata is transient, not logged.
async function processTable(command) {
  const raw = await command("/bin/ps", ["-axo", "pid=,ppid=,pgid=,uid=,lstart="], {
    control: true,
  });
  const rows = raw
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      const m = line.trim().match(/^(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(.+)$/u);
      NodeAssert.ok(m, "native process identity unavailable");
      return { pid: +m[1], ppid: +m[2], pgid: +m[3], uid: +m[4], start: m[5] };
    });
  NodeAssert.ok(rows.length <= 32768);
  return rows;
}
const sameProcess = (a, b) =>
  a.pid === b.pid && a.pgid === b.pgid && a.uid === b.uid && a.start === b.start;

async function qualify(v) {
  const started = performance.now(),
    workEnd = started + 870000,
    end = started + 900000;
  const abort = new AbortController();
  const receipt = {
    schema,
    outcome: "failed",
    acceptance: false,
    source: v.source,
    host: {
      ...v.host,
      actualHostname: NodeOS.hostname(),
      platform: NodeProcess.platform,
      arch: NodeProcess.arch,
    },
    environmentId: v.environmentId,
    projectId: v.projectId,
    provider: {
      ...v.provider,
      accountObservation: "external-operator-attestation; native account identity not observed",
      storageCustody:
        v.provider.nativeBinding === undefined
          ? { state: "private-provider-home" }
          : {
              state: "external-native-history-and-cache",
              home: v.provider.nativeBinding.home,
              configDir: v.provider.nativeBinding.configDir,
              cleanup: "excluded; native HOME, profile and launcher are never cleanup targets",
              terminalScope:
                "explicit provider terminals can inherit this environment; setup and terminal-first omit the provider",
            },
    },
    adapterCoverage: {
      selected: v.provider.driver,
      otherAdapter: "unavailable; separate native invocation required",
    },
    limits,
    assertions: [],
    processes: [],
    diagnostics: { bytes: 0, retainedBytes: 0 },
    oracles: Object.fromEntries(obligations.map((key) => [key, "unavailable"])),
    cleanup: { state: "not_acquired" },
  };
  const mark = (name, evidence) => {
    receipt.oracles[name] = "passed";
    receipt.assertions.push({ name, evidence });
  };
  const remaining = (cleanup = false) =>
    Math.max(1, Math.floor((cleanup ? end : workEnd) - performance.now()));
  const failIfExpired = () => {
    if (performance.now() >= workEnd || abort.signal.aborted)
      throw new Unavailable("whole_run_deadline_or_cancellation");
  };
  const diagnostic = (chunk) => {
    receipt.diagnostics.bytes += chunk.length;
    if (receipt.diagnostics.bytes > limits.diagnostics)
      abort.abort(new Unavailable("combined_diagnostics_cap"));
    // Raw server/provider output can contain native auth material. Retain counts
    // only; assertion evidence below is an explicit bounded synthetic projection.
  };
  let cleanupMode = false,
    acquired = false,
    receiptWritten = false,
    retirementUnreconciled = false,
    live,
    rpc,
    database,
    phase = "preflight",
    captureGitTrace = false;
  const persistReceipt = async () => {
    const encoded = JSON.stringify(receipt, null, 2) + "\n";
    NodeAssert.ok(Buffer.byteLength(encoded) <= MiB, "bounded assertion receipt");
    await NodeFSP.writeFile(v.receipt, encoded, {
      flag: receiptWritten ? "w" : "wx",
      mode: 0o600,
    });
    receiptWritten = true;
  };
  const children = new Set();
  const owned = new Map();
  const groups = new Set();
  const listeners = new Set();
  let monitor,
    monitoring,
    monitorFailure,
    stopInProgress,
    lastInventoryAt = 0;
  const signals = ["SIGINT", "SIGTERM", "SIGHUP"];
  const cancel = () => abort.abort(new Unavailable("operator_cancelled"));
  for (const signal of signals) process.on(signal, cancel);
  const deadline = setTimeout(cancel, 870000);

  const command = async (file, args, options = {}) => {
    if (!cleanupMode) failIfExpired();
    let output = Buffer.alloc(0),
      overflow = false,
      expired = false;
    const child = NodeChildProcess.spawn(file, args, {
      cwd: options.cwd ?? v.source.root,
      env: options.env ?? environment(),
      stdio: [options.stdin === undefined ? "ignore" : "pipe", "pipe", "pipe"],
    });
    children.add(child);
    const timeout = setTimeout(
      () => {
        expired = true;
        child.kill("SIGTERM");
      },
      Math.min(options.timeout ?? 10000, remaining(cleanupMode)),
    );
    const force = setTimeout(
      () => {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      },
      Math.min(options.timeout ?? 10000, remaining(cleanupMode)) + 1000,
    );
    const onAbort = () => child.kill("SIGTERM");
    if (!cleanupMode) abort.signal.addEventListener("abort", onAbort, { once: true });
    child.stdout.on("data", (chunk) => {
      if (!options.control) diagnostic(chunk);
      if (output.length + chunk.length > MiB) {
        overflow = true;
        child.kill("SIGTERM");
      } else output = Buffer.concat([output, chunk]);
    });
    child.stderr.on("data", diagnostic);
    const completion = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => resolve({ code, signal }));
    });
    if (options.stdin !== undefined) {
      child.stdin.on("error", () => {});
      child.stdin.end(options.stdin);
    }
    try {
      const result = await completion;
      if (overflow || expired) throw new Unavailable("owned_command_bound_exceeded");
      if (result.code !== 0 && !options.allowFailure) throw new Unavailable("owned_command_failed");
      return options.allowFailure
        ? { ...result, stdout: output.toString("utf8") }
        : output.toString("utf8").trim();
    } finally {
      clearTimeout(timeout);
      clearTimeout(force);
      abort.signal.removeEventListener("abort", onAbort);
      children.delete(child);
    }
  };
  const root = v.root.path,
    repo = NodePath.join(root, "repo"),
    home = NodePath.join(root, "home");
  const state = NodePath.join(root, "jones-home", "userdata");
  const providerHome = NodePath.join(root, "provider-home");
  const environment = () => ({
    PATH: v.tools.path,
    HOME: home,
    TMPDIR: NodePath.join(root, "tmp"),
    XDG_CACHE_HOME: NodePath.join(root, "cache"),
    XDG_STATE_HOME: NodePath.join(root, "state"),
    LANG: "en_US.UTF-8",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: "/dev/null",
    SHELL: "/bin/zsh",
    PYTHONDONTWRITEBYTECODE: "1",
    ...(captureGitTrace
      ? { GIT_TRACE2_EVENT: NodePath.join(root, "native-git.jsonl"), GIT_TRACE2_EVENT_BRIEF: "1" }
      : {}),
  });
  const git = (cwd, ...args) =>
    command(
      v.tools.git,
      [
        "-c",
        "user.name=Lifecycle Development Proof",
        "-c",
        "user.email=lifecycle@example.invalid",
        "-c",
        "core.hooksPath=/dev/null",
        ...args,
      ],
      { cwd, control: true },
    );
  const checkRoot = async () => {
    const stat = await NodeFSP.lstat(root);
    NodeAssert.ok(stat.isDirectory() && !stat.isSymbolicLink() && (stat.mode & 0o777) === 0o700);
    for (const key of ["dev", "ino", "uid", "gid"]) NodeAssert.equal(stat[key], v.root[key]);
    NodeAssert.equal(stat.uid, process.getuid());
    NodeAssert.equal(stat.gid, process.getgid());
    NodeAssert.equal(await NodeFSP.realpath(root), root);
  };
  // Explicit proof inventory only, bounded to the captured synthetic root. This
  // is never installed in a production completion/observation hot path.
  const scratchInventory = async () => {
    let bytes = 0,
      entries = 0;
    const walk = async (dir) => {
      for (const item of await NodeFSP.readdir(dir, { withFileTypes: true })) {
        if (++entries > limits.entries) throw new Unavailable("scratch_entry_cap");
        const target = NodePath.join(dir, item.name),
          stat = await NodeFSP.lstat(target);
        NodeAssert.equal(stat.uid, v.root.uid, "foreign scratch owner");
        NodeAssert.equal(stat.gid, v.root.gid, "foreign scratch group");
        NodeAssert.equal(stat.dev, v.root.dev, "scratch mount boundary changed");
        if (stat.isSymbolicLink()) throw new Unavailable("scratch_symlink_cleanup_hold");
        if (stat.isDirectory()) await walk(target);
        else if (stat.isFile()) bytes += stat.size;
        else throw new Unavailable("scratch_special_file_cleanup_hold");
        if (bytes > limits.scratchBytes) throw new Unavailable("scratch_byte_cap");
      }
    };
    await walk(root);
    return { bytes, entries };
  };
  const observe = async () => {
    const rows = await processTable(command);
    const current = new Map(rows.map((p) => [p.pid, p]));
    let changed = true;
    while (changed) {
      changed = false;
      for (const p of rows) {
        const parent = owned.get(p.ppid);
        if (
          groups.has(p.pgid) ||
          (parent && current.has(parent.pid) && sameProcess(parent, current.get(parent.pid)))
        ) {
          NodeAssert.equal(p.uid, v.root.uid, "foreign process in owned containment");
          const prior = owned.get(p.pid);
          if (prior && !sameProcess(prior, p)) throw new Unavailable("owned_pid_reused");
          if (!prior) {
            owned.set(p.pid, p);
            changed = true;
          }
        }
      }
    }
    return current;
  };
  const wait = async (_label, read) => {
    for (;;) {
      failIfExpired();
      if (monitorFailure) throw monitorFailure;
      const result = await read();
      if (result) return result;
      await NodeTimersPromises.setTimeout(100, undefined, { signal: abort.signal });
    }
  };
  const rows = (sql, ...params) => {
    failIfExpired();
    const db = new database.DatabaseSync(NodePath.join(state, "statev2.sqlite"), {
      readOnly: true,
    });
    try {
      db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=1000");
      const values = db.prepare(sql).all(...params);
      NodeAssert.ok(
        values.length <= 1000 && Buffer.byteLength(JSON.stringify(values)) <= MiB,
        "bounded assertion projection",
      );
      return values;
    } finally {
      db.close();
    }
  };
  const payloads = (table, thread) => {
    NodeAssert.ok(
      [
        "threads",
        "runs",
        "run_attempts",
        "provider_threads",
        "provider_turns",
        "turn_items",
        "checkpoints",
      ].includes(table),
    );
    return rows(
      `SELECT payload_json FROM orchestration_v2_projection_${table} WHERE thread_id=? LIMIT 1001`,
      thread,
    ).map((r) => JSON.parse(r.payload_json));
  };
  const binding = (thread) => {
    const r = rows("SELECT binding FROM jones_conversation_bindings WHERE thread_id=?", thread)[0];
    return r ? JSON.parse(r.binding) : undefined;
  };
  const handoff = (thread) => {
    const r = rows("SELECT record FROM jones_conversation_handoffs WHERE thread_id=?", thread)[0];
    return r ? JSON.parse(r.record) : undefined;
  };
  const scripts = async () =>
    (
      await NodeFSP.readFile(NodePath.join(root, "script-events.jsonl"), "utf8").catch((e) => {
        if (e.code === "ENOENT") return "";
        throw e;
      })
    )
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  const api = async (urlPath, body) => {
    failIfExpired();
    const response = await fetch(new URL(urlPath, live.origin), {
      method: body === undefined ? "GET" : "POST",
      headers: { authorization: `Bearer ${credential}`, "content-type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.any([abort.signal, AbortSignal.timeout(Math.min(15000, remaining()))]),
    });
    const reader = response.body.getReader();
    let size = 0,
      chunks = [];
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.length;
        NodeAssert.ok(size <= MiB);
        chunks.push(value);
      }
    } finally {
      await reader.cancel();
      reader.releaseLock();
    }
    NodeAssert.ok(response.ok, `development_http_${response.status}`);
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  };
  const credential = NodeCrypto.randomBytes(32).toString("hex");
  const call = async (op, input) => {
    failIfExpired();
    return rpc(live.origin, credential, op, input);
  };
  const dispatch = async (threadId, command) =>
    call("dispatch", { commandId: NodeCrypto.randomUUID(), threadId, ...command });
  const message = async (threadId, value) =>
    dispatch(threadId, {
      type: "message.dispatch",
      messageId: NodeCrypto.randomUUID(),
      text: value,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "start_immediately" },
    });
  const launch = async (label, initialMessage) => {
    const result = await call("launch", {
      commandId: NodeCrypto.randomUUID(),
      projectId: v.projectId,
      title: `Lifecycle ${label}`,
      generateTitle: false,
      modelSelection: { instanceId: v.provider.driver, model: v.provider.model },
      runtimeMode: "full-access",
      interactionMode: "default",
      workspaceStrategy: { type: "auto", baseRef: "main" },
      ...(initialMessage === undefined
        ? {}
        : { initialMessage: { text: initialMessage, attachments: [] } }),
    });
    return result.threadId;
  };
  const tool = (thread, name) =>
    payloads("turn_items", thread).filter(
      (r) =>
        r.type === "dynamic_tool" &&
        (r.toolName === name ||
          r.toolName?.endsWith(`.${name}`) ||
          r.toolName?.endsWith(`__${name}`)),
    );
  const completed = async (thread, after = 0) =>
    wait("completed native turn", () => {
      const runs = payloads("runs", thread);
      if (runs.some((r) => ["failed", "cancelled"].includes(r.status)))
        throw new Unavailable("provider_run_failed_or_cancelled");
      const turns = payloads("provider_turns", thread).filter((r) => r.status === "completed");
      return turns.length > after &&
        runs.every((r) => ["completed", "interrupted", "rolled_back"].includes(r.status))
        ? turns
        : undefined;
    });
  const history = (thread) => ({
    runIds: payloads("runs", thread).map((r) => r.id),
    turnIds: payloads("provider_turns", thread).map((r) => r.id),
  });
  const completedScope = (thread, previous) => {
    const runs = payloads("runs", thread).filter((r) => !previous.runIds.includes(r.id));
    const turns = payloads("provider_turns", thread).filter(
      (r) => !previous.turnIds.includes(r.id),
    );
    NodeAssert.equal(runs.length, 1, "one newly admitted run required");
    NodeAssert.equal(turns.length, 1, "one new native turn required");
    const run = runs[0],
      turn = turns[0];
    NodeAssert.equal(run.status, "completed");
    NodeAssert.equal(turn.status, "completed");
    const attempt = payloads("run_attempts", thread).find((r) => r.id === turn.runAttemptId);
    NodeAssert.ok(attempt);
    NodeAssert.equal(attempt.runId, run.id);
    NodeAssert.equal(attempt.providerTurnId, turn.id);
    NodeAssert.equal(turn.providerThreadId, run.providerThreadId);
    NodeAssert.equal(turn.nativeTurnRef?.driver, v.provider.driver);
    text(turn.nativeTurnRef?.nativeId);
    return {
      runId: run.id,
      providerTurnId: turn.id,
      nativeTurnId: turn.nativeTurnRef.nativeId,
      runAttemptId: attempt.id,
    };
  };
  const native = (thread) => {
    const identities = payloads("provider_threads", thread);
    const ids = identities.map((r) => {
      NodeAssert.equal(r.driver, v.provider.driver);
      NodeAssert.equal(r.providerInstanceId, v.provider.driver);
      NodeAssert.equal(r.nativeThreadRef?.driver, v.provider.driver);
      text(r.nativeThreadRef?.nativeId);
      NodeAssert.equal(r.nativeThreadRef?.strength, "strong");
      return r.nativeThreadRef.nativeId;
    });
    NodeAssert.ok(ids.length > 0);
    NodeAssert.equal(new Set(ids).size, 1, "native conversation continuity required");
    const turns = payloads("provider_turns", thread).filter((r) => r.status === "completed");
    NodeAssert.ok(turns.length > 0);
    for (const turn of turns) {
      NodeAssert.equal(turn.nativeTurnRef?.driver, v.provider.driver);
      text(turn.nativeTurnRef?.nativeId);
    }
    return {
      nativeThreadId: ids[0],
      turnIds: turns.map((r) => r.nativeTurnRef.nativeId),
      runtimeIdentity: identities.at(-1).runtimeIdentity ?? { observed: "unavailable" },
    };
  };
  const contains = (value, expected) => {
    if (value && typeof value === "object") {
      if (Object.entries(expected).every(([key, val]) => value[key] === val)) return true;
      return Object.values(value).some((child) => contains(child, expected));
    }
    if (typeof value === "string" && value.length <= 65536) {
      try {
        return contains(JSON.parse(value), expected);
      } catch {
        return false;
      }
    }
    return false;
  };
  const exactResult = (value, expected) => {
    if (typeof value === "string" && value.length <= 65536) {
      try {
        return exactResult(JSON.parse(value), expected);
      } catch {
        return false;
      }
    }
    if (!value || typeof value !== "object" || value.isError === true) return false;
    try {
      NodeAssert.deepEqual(value, expected);
      return true;
    } catch {
      return Object.values(value).some((child) => exactResult(child, expected));
    }
  };
  const assertHandoffReplay = (item, input, result, scope) => {
    NodeAssert.ok(item);
    NodeAssert.equal(item.runId, scope.runId);
    NodeAssert.equal(item.providerTurnId, scope.providerTurnId);
    NodeAssert.ok(item.nativeItemRef?.nativeId);
    NodeAssert.equal(item.status, "completed");
    NodeAssert.deepEqual(item.input, input);
    NodeAssert.ok(exactResult(item.output, result), "original successful handoff result required");
    return {
      id: item.id,
      runId: item.runId,
      providerTurnId: item.providerTurnId,
      nativeItemRef: item.nativeItemRef,
      input: item.input,
      output: item.output,
    };
  };
  const cwdEvidence = (thread, cwd, after = 0, scope) => {
    const calls = payloads("turn_items", thread)
      .filter(
        (r) =>
          r.type === "command_execution" &&
          (scope === undefined ||
            (r.runId === scope.runId && r.providerTurnId === scope.providerTurnId)),
      )
      .slice(after);
    NodeAssert.ok(
      calls.length > 0,
      "native pwd tool execution required; assistant text is insufficient",
    );
    for (const r of calls) {
      NodeAssert.match(r.input, /(?:^|\s|["'])\/?(?:bin\/)?pwd(?:["']|\s|$)/u);
      NodeAssert.equal(r.status, "completed");
      if (v.provider.driver === "codex") NodeAssert.equal(r.exitCode, 0);
      // Claude's production command projection has completed/error status and
      // native item provenance, but does not expose an exitCode field.
      NodeAssert.equal(r.output?.trim(), cwd);
      NodeAssert.ok(r.providerTurnId && r.nativeItemRef?.nativeId);
    }
    return calls.map((r) => ({
      id: r.id,
      runId: r.runId,
      providerTurnId: r.providerTurnId,
      nativeItemRef: r.nativeItemRef,
      cwd: r.output.trim(),
    }));
  };
  const sourceInventory = async () => {
    // This explicit source-tool oracle alone hashes Git refs/index/object bytes.
    const entries = [],
      common = NodePath.join(repo, ".git");
    const visit = async (relative) => {
      const full = NodePath.join(common, relative);
      if (!(await exists(full))) return;
      const stat = await NodeFSP.lstat(full);
      NodeAssert.ok(!stat.isSymbolicLink());
      if (stat.isDirectory())
        for (const name of (await NodeFSP.readdir(full)).sort())
          await visit(NodePath.join(relative, name));
      else {
        NodeAssert.ok(stat.isFile() && stat.size <= MiB);
        entries.push([relative, stat.size, sha(await NodeFSP.readFile(full))]);
      }
      NodeAssert.ok(entries.length <= 1000);
    };
    for (const rel of ["refs", "objects", "index", "packed-refs", "HEAD"]) await visit(rel);
    return entries;
  };
  const readPrompt =
    "This is an authorized synthetic lifecycle qualification. No children. Run only /bin/pwd once through your native command tool, call t3_source_read with path AGENTS.md, then end the turn. Do not read any other files, write, attach, hand off, log in, install, or start another process. Do not replace either tool result with assistant text.";
  let sourceHead;
  const conversationOracle = async (
    thread,
    before,
    label,
    previous = { runIds: [], turnIds: [] },
  ) => {
    await completed(thread);
    const scope = completedScope(thread, previous);
    const recorded = rows(
      "SELECT snapshot FROM jones_conversation_turns WHERE run_id=? AND thread_id=?",
      scope.runId,
      thread,
    );
    NodeAssert.equal(recorded.length, 1, "this run's recorded source snapshot required");
    const snapshot = JSON.parse(recorded[0].snapshot);
    NodeAssert.equal(snapshot.commit, sourceHead);
    const b = binding(thread);
    NodeAssert.ok(b);
    NodeAssert.equal(b.state, "conversation");
    NodeAssert.equal(b.commit, sourceHead);
    NodeAssert.equal(b.projectId, v.projectId);
    NodeAssert.ok(inside(NodePath.join(state, "conversation-workspaces"), b.path));
    NodeAssert.equal(payloads("threads", thread)[0].worktreePath, null);
    NodeAssert.equal(
      rows("SELECT thread_id FROM jones_idle_worktree_admission WHERE thread_id=?", thread).length,
      0,
    );
    NodeAssert.equal(
      rows(
        "SELECT claim_id FROM jones_workspace_duty_claims WHERE owner=? AND released_at IS NULL",
        thread,
      ).length,
      0,
    );
    NodeAssert.equal((await scripts()).filter((r) => r.kind === "setup").length, 0);
    NodeAssert.equal(
      (await git(repo, "worktree", "list", "--porcelain")).match(/^worktree /gmu)?.length,
      1,
    );
    NodeAssert.deepEqual(await NodeFSP.readdir(b.path), []);
    const reads = tool(thread, "t3_source_read").filter(
      (r) => r.runId === scope.runId && r.providerTurnId === scope.providerTurnId,
    );
    NodeAssert.ok(
      reads.some(
        (r) =>
          r.status === "completed" &&
          r.nativeItemRef?.nativeId &&
          r.input?.path === "AGENTS.md" &&
          contains(r.output, { commit: snapshot.commit, content: instructions }),
      ),
      "exact MCP source content and commit required",
    );
    NodeAssert.deepEqual(await sourceInventory(), before);
    mark(label, {
      binding: b,
      scope,
      cwd: cwdEvidence(thread, b.path, 0, scope),
      native: native(thread),
    });
    mark("source-read", {
      scope,
      snapshot,
      commit: snapshot.commit,
      content: instructions,
      inventorySha256: sha(JSON.stringify(before)),
      unchanged: true,
      toolItemIds: reads.map((r) => r.id),
    });
    return b;
  };
  const instructions =
    "Synthetic lifecycle development proof. Read this exact committed content through t3_source_read. No installations, no children, no access to other projects or live application state.\n";

  const stop = async () => {
    if (stopInProgress) return stopInProgress;
    stopInProgress = (async () => {
      cleanupMode = true;
      if (monitor) clearInterval(monitor);
      if (monitoring) await monitoring;
      const stopEnd = Math.min(end, performance.now() + limits.stopSeconds * 1000);
      const current = await observe();
      const targets = [...owned.values()].filter((p) => current.has(p.pid));
      for (const p of targets) {
        if (!sameProcess(p, current.get(p.pid))) throw new Unavailable("stop_identity_changed");
        try {
          process.kill(p.pid, "SIGTERM");
        } catch (error) {
          if (error.code !== "ESRCH") throw error;
        }
      }
      for (const child of children)
        if (child.pid && child.exitCode === null && child.signalCode === null)
          child.kill("SIGTERM");
      const gracefulEnd = Math.min(stopEnd - 5000, performance.now() + 15000);
      for (;;) {
        const now = await observe();
        const present = [...owned.values()].filter((p) => now.has(p.pid));
        if (present.length === 0 && children.size === 0) break;
        if (performance.now() >= gracefulEnd) {
          for (const p of present) {
            if (!sameProcess(p, now.get(p.pid)))
              throw new Unavailable("force_stop_identity_changed");
            try {
              process.kill(p.pid, "SIGKILL");
            } catch (error) {
              if (error.code !== "ESRCH") throw error;
            }
          }
        }
        if (performance.now() >= stopEnd - 2000)
          throw new Unavailable("native_owned_absence_unproved");
        await NodeTimersPromises.setTimeout(100);
      }
      for (const origin of listeners)
        await new Promise((resolve, reject) => {
          const url = new URL(origin),
            socket = NodeNet.connect({ host: "127.0.0.1", port: +url.port });
          socket.once("connect", () => {
            socket.destroy();
            reject(new Unavailable("listener_still_open"));
          });
          socket.once("error", (error) =>
            error.code === "ECONNREFUSED"
              ? resolve()
              : reject(new Unavailable("listener_closure_unknown")),
          );
          socket.setTimeout(Math.min(1000, remaining(true)), () => {
            socket.destroy();
            reject(new Unavailable("listener_closure_unknown"));
          });
        });
      // Read-only closure check catches an uncaptured/reparented scratch user.
      // Such a PID is NEVER adopted or killed by its pathname.
      const handles = await command("/usr/sbin/lsof", ["-nP", "-t", "+D", root], {
        control: true,
        allowFailure: true,
        timeout: 3000,
      });
      if (handles.code !== 1 || handles.stdout.trim() !== "")
        throw new Unavailable("scratch_native_consumer_absence_unproved");
      receipt.processes = [...owned.values()];
      live = undefined;
    })();
    try {
      await stopInProgress;
    } finally {
      stopInProgress = undefined;
    }
  };
  const start = async () => {
    cleanupMode = false;
    failIfExpired();
    const runtimePath = NodePath.join(state, "server-runtime.json");
    NodeAssert.equal(
      await exists(runtimePath),
      false,
      "prior runtime record must close normally before restart",
    );
    const child = NodeChildProcess.spawn(
      v.tools.node,
      [NodePath.join(v.source.root, entryRelative), NodePath.join(root, "instance.json")],
      {
        cwd: v.source.root,
        env: environment(),
        detached: true,
        stdio: ["ignore", "pipe", "pipe", "ipc"],
      },
    );
    children.add(child);
    child.stdout.on("data", diagnostic);
    child.stderr.on("data", diagnostic);
    child.once("error", (error) => {
      monitorFailure = error;
      children.delete(child);
    });
    child.once("close", () => children.delete(child));
    NodeAssert.ok(child.pid);
    groups.add(child.pid);
    const first = (await processTable(command)).find((p) => p.pid === child.pid);
    NodeAssert.ok(
      first && first.pgid === child.pid && first.uid === v.root.uid,
      "native detached process-group identity required",
    );
    owned.set(first.pid, first);
    monitorFailure = undefined;
    monitor = setInterval(() => {
      if (monitoring) return;
      monitoring = (async () => {
        await observe();
        const trace = NodePath.join(root, "native-git.jsonl");
        const traceBytes = await NodeFSP.stat(trace).then(
          (stat) => stat.size,
          (error) => {
            if (error.code === "ENOENT") return 0;
            throw error;
          },
        );
        receipt.diagnostics.nativeTraceBytes = traceBytes;
        if (receipt.diagnostics.bytes + traceBytes > limits.diagnostics)
          throw new Unavailable("combined_diagnostics_cap");
        // Explicit qualification inventory, never a production observer hook.
        if (performance.now() - lastInventoryAt >= 5000) {
          await scratchInventory();
          lastInventoryAt = performance.now();
        }
      })()
        .catch((error) => {
          monitorFailure = error;
          abort.abort(error);
        })
        .finally(() => {
          monitoring = undefined;
        });
    }, 500);
    const runtime = await wait("production listener", async () =>
      (await exists(runtimePath)) ? readJson(runtimePath) : undefined,
    );
    NodeAssert.equal(runtime.pid, child.pid);
    NodeAssert.match(runtime.origin, /^http:\/\/127\.0\.0\.1:\d+$/u);
    live = { ...runtime, child, identity: first };
    listeners.add(runtime.origin);
    const descriptor = await api("/.well-known/t3/environment");
    NodeAssert.equal(descriptor.environmentId, v.environmentId);
    NodeAssert.equal(
      (await NodeFSP.readFile(NodePath.join(state, "environment-id"), "utf8")).trim(),
      v.environmentId,
    );
    // Two separate finder roots exercise the native compute pool. These are
    // support workers, not parallel test workers or provider turns.
    const searches = await Promise.all(
      [repo, NodePath.join(root, "compute-probe")].map((cwd) =>
        call("search", { cwd, query: "", limit: 1, kind: "file" }),
      ),
    );
    NodeAssert.equal(searches.length, 2);
    const current = await observe();
    const computeChildren = [...current.values()].filter((p) => p.ppid === child.pid);
    NodeAssert.ok(computeChildren.length >= 2, "native compute children required");
    const workerEvidence = await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => {
          child.removeListener("message", receive);
          reject(new Unavailable("native_reader_observation_unavailable"));
        },
        Math.min(5000, remaining()),
      );
      const receive = (value) => {
        clearTimeout(timer);
        resolve(value);
      };
      child.once("message", receive);
      child.send("inspect-native-workers");
    });
    NodeAssert.equal(workerEvidence.schema, "jones.workspace-lifecycle-dev.workers/v1");
    NodeAssert.equal(workerEvidence.pid, child.pid);
    NodeAssert.ok(
      workerEvidence.workerThreads >= 3,
      "two real reader workers plus the checkpoint worker required",
    );
    receipt.assertions.push({
      name: "production-support-workers",
      evidence: {
        server: first,
        configuredReaders: 2,
        configuredCompute: 2,
        workerThreads: workerEvidence.workerThreads,
        computeChildren,
      },
    });
    return live;
  };

  try {
    if (
      process.versions.node !== "26.12.1" ||
      NodeProcess.platform !== "darwin" ||
      NodeProcess.arch !== "arm64"
    )
      throw new Unavailable("qualified_apple_silicon_node_required");
    NodeAssert.equal(NodeOS.hostname(), v.host.hostname);
    NodeAssert.equal(
      process.env.JONES_LIFECYCLE_PROOF_ENVIRONMENT_ID,
      v.host.executionEnvironmentId,
      "externally bound execution environment required",
    );
    NodeAssert.equal(
      await NodeFSP.realpath(process.execPath),
      await NodeFSP.realpath(v.tools.node),
    );
    for (const item of [v.source.root, v.source.v3Root, NodePath.dirname(v.receipt)])
      NodeAssert.equal(await NodeFSP.realpath(item), item);
    NodeAssert.equal(await exists(v.receipt), false);
    NodeAssert.ok(
      !inside(root, v.receipt) &&
        !inside(v.source.root, v.receipt) &&
        !inside(v.source.v3Root, v.receipt),
    );
    await checkRoot();
    for (const excluded of [
      v.source.root,
      v.source.v3Root,
      await NodeFSP.realpath(NodeOS.tmpdir()),
      "/tmp",
      "/private/tmp",
      "/var/tmp",
      "/private/var/tmp",
    ])
      NodeAssert.ok(!inside(excluded, root) && !inside(root, excluded));
    for (let ancestor = root; ; ancestor = NodePath.dirname(ancestor)) {
      NodeAssert.equal(
        await exists(NodePath.join(ancestor, ".git")),
        false,
        "scratch outside Git ancestry required",
      );
      if (NodePath.dirname(ancestor) === ancestor) break;
    }
    NodeAssert.deepEqual(await NodeFSP.readdir(root), ["provider-home"]);
    NodeAssert.equal(await NodeFSP.realpath(providerHome), providerHome);
    const providerStat = await NodeFSP.lstat(providerHome);
    NodeAssert.ok(
      providerStat.isDirectory() &&
        providerStat.uid === v.root.uid &&
        (providerStat.mode & 0o077) === 0,
    );
    // Custody is acquired only after the exact root, initial contents, cleanup
    // authority and UID/GID are established. From here every path runs finally.
    acquired = true;
    receipt.cleanup = { state: "acquired", root: v.root };
    const volume = await NodeFSP.statfs(root),
      available = Number(volume.bavail) * Number(volume.bsize);
    receipt.admission = {
      ...v.admission,
      availableBytes: available,
      additionalPeakBytes: 16 * GiB,
      reserveBytes: 30 * GiB,
    };
    if (!Number.isSafeInteger(available) || available < 46 * GiB)
      throw new Unavailable("live_storage_admission_requires_16GiB_plus_30GiB");
    await scratchInventory();
    for (const dir of [
      home,
      repo,
      state,
      NodePath.join(root, "tmp"),
      NodePath.join(root, "cache"),
      NodePath.join(root, "state"),
      NodePath.join(root, "registries"),
      NodePath.join(root, "compute-probe"),
    ])
      await NodeFSP.mkdir(dir, { recursive: true, mode: 0o700 });
    NodeAssert.equal(await command(v.tools.pnpm, ["--version"], { control: true }), "12.14.0");
    receipt.sourceProof = [
      await verifyExport(
        v.source.root,
        v.source.manifest,
        v.source.manifestSha256,
        v.source.commit,
        v.source.tree,
      ),
      await verifyExport(
        NodePath.join(v.source.v3Root, "src"),
        v.source.v3Manifest,
        v.source.v3ManifestSha256,
        v.source.v3Commit,
        v.source.v3SrcTree,
      ),
    ];
    for (const name of ["node", "pnpm", "python"]) {
      const stat = await NodeFSP.stat(v.tools[name]);
      NodeAssert.ok(stat.isFile() && stat.size <= 256 * MiB);
      NodeAssert.equal(sha(await NodeFSP.readFile(v.tools[name])), v.tools[`${name}Sha256`]);
    }
    if (v.provider.nativeBinding !== undefined) {
      NodeAssert.equal(process.getuid(), 501);
      NodeAssert.equal(NodeOS.userInfo().username, "mjones");
      NodeAssert.deepEqual(
        await NodeFSP.readdir(providerHome),
        [],
        "native binding uses an empty placeholder only",
      );
    }
    receipt.provider.nativeBindingObservation = await verifyNativeBinding(v.provider, v.host);
    const launcher = await NodeFSP.lstat(v.provider.launcher);
    NodeAssert.ok(launcher.isFile() && launcher.size <= 64 * MiB);
    NodeAssert.equal(sha(await NodeFSP.readFile(v.provider.launcher)), v.provider.launcherSha256);
    // Package exports point to workspace .ts files, as used by the existing
    // source bin.ts dev command. No tsx/loader, build, install or borrowed tree.
    ({ developmentRpc: rpc } = await import("./workspaceLifecycleDev.entrypoint.ts"));
    database = await import("node:sqlite");
    await git(repo, "init", "--template=", "-b", "main");
    await NodeFSP.writeFile(NodePath.join(repo, "AGENTS.md"), instructions, {
      flag: "wx",
      mode: 0o600,
    });
    await git(repo, "add", "AGENTS.md");
    await git(repo, "commit", "-qm", "Synthetic lifecycle source");
    sourceHead = await git(repo, "rev-parse", "HEAD");
    const pythonPrefix = (module) => [
      "-I",
      "-S",
      "-B",
      "-c",
      `import runpy,sys; sys.path.insert(0,sys.argv.pop(1)); runpy.run_module('${module}',run_name='__main__')`,
      NodePath.join(v.source.v3Root, "src"),
    ];
    const registry = {
      executable: v.tools.python,
      arguments: pythonPrefix("codex_v3.worktree_readiness_cli"),
      database: NodePath.join(root, "registries", "lifecycle.sqlite"),
      cwd: v.source.root,
    };
    const outputs = {
      executable: v.tools.python,
      arguments: [
        ...pythonPrefix("codex_v3.build_outputs.cli"),
        "--build-root",
        NodePath.join(root, "output-allocations"),
      ],
      database: NodePath.join(root, "registries", "outputs.sqlite"),
      cwd: v.source.root,
    };
    const registryCall = async (config, args) =>
      JSON.parse(
        await command(
          config.executable,
          [...config.arguments, "--database", config.database, ...args],
          { cwd: config.cwd, control: true },
        ),
      );
    const initialized = await registryCall(registry, [
      "unmark",
      "--worktree",
      repo,
      "--reason",
      "Initialize the isolated development proof store",
    ]);
    NodeAssert.equal(initialized.schema, "worktree-readiness-cli/v1");
    const migrated = await registryCall(registry, ["migrate", "--expected-version", "1"]);
    NodeAssert.equal(migrated.schema, "workspace-lifecycle-cli/v1");
    NodeAssert.equal(migrated.result.version, 2);
    // allocate is the owning output CLI's explicit initializing write. Merely
    // constructing Registry or listing an absent store is not initialization.
    const allocated = await registryCall(outputs, [
      "allocate",
      "--producer",
      "workspace-lifecycle-dev-proof",
      "--owner",
      v.projectId,
      "--purpose",
      "isolated development qualification",
      "--class",
      "scratch",
      "--label",
      "store-initialization",
    ]);
    NodeAssert.equal(allocated.schema, "build-outputs-cli/v1");
    NodeAssert.equal(allocated.registry.present, true);
    NodeAssert.equal(await exists(registry.database), true);
    NodeAssert.equal(await exists(outputs.database), true);
    await NodeFSP.writeFile(NodePath.join(state, "environment-id"), `${v.environmentId}\n`, {
      flag: "wx",
      mode: 0o600,
    });
    await NodeFSP.writeFile(
      NodePath.join(root, "instance.json"),
      JSON.stringify({
        schema: "jones.workspace-lifecycle-dev.instance/v1",
        sourceRoot: v.source.root,
        root,
        home,
        projectRoot: repo,
        environmentId: v.environmentId,
        credential,
        workspaceLifecycle: {
          environmentId: v.environmentId,
          projects: [
            {
              projectId: v.projectId,
              repository: "Jones-Systems/Synthetic-Lifecycle-Proof",
              projectRoot: repo,
            },
          ],
          registry,
          outputs,
        },
      }),
      { flag: "wx", mode: 0o600 },
    );
    const disabled = {
      worktreeSettledAfterDays: null,
      worktreeAfterDays: null,
      worktreeOnMerge: false,
      worktreeOnDelete: false,
      worktreeUnchanged: false,
      worktreeKeepWhen: "uncommitted-changes",
      worktreeCoveredDiscard: [],
      worktreeSettledScope: "branch-retained",
    };
    await NodeFSP.writeFile(
      NodePath.join(state, "settings.json"),
      JSON.stringify({
        worktreeAllocation: "on-write",
        newWorktreesStartFromOrigin: false,
        defaultAutoPull: false,
        enableAgentBrowserAccess: false,
        enableProviderUpdateChecks: false,
        continueThreadsAfterServerUpdate: false,
        sidebarAutoSettleAfterDays: null,
        sidebarAutoSettleOnMerge: false,
        defaultModelSelection: { instanceId: v.provider.driver, model: v.provider.model },
        storageCleanup: disabled,
        providerInstances: {
          codex:
            v.provider.driver === "codex"
              ? {
                  driver: "codex",
                  enabled: true,
                  config: {
                    setupMode: "existing",
                    binaryPath: v.provider.launcher,
                    homePath: providerHome,
                  },
                }
              : { driver: "codex", enabled: false },
          claudeAgent:
            v.provider.driver === "claudeAgent"
              ? {
                  driver: "claudeAgent",
                  enabled: true,
                  ...providerInstanceBinding(v.provider, providerHome),
                }
              : { driver: "claudeAgent", enabled: false },
        },
      }),
      { flag: "wx", mode: 0o600 },
    );
    // Real project scripts run through the production setup and settlement
    // owners. Logs live outside the checkout, so no cleanup exemption is needed.
    const script = `import * as NodeFSP from 'node:fs';\nimport * as NodeTimersPromises from 'node:timers/promises';\nconst [root,kind]=process.argv.slice(2);\nNodeFSP.appendFileSync(root+'/script-events.jsonl',JSON.stringify({kind,cwd:process.cwd(),home:process.env.HOME,pid:process.pid,ppid:process.ppid,at:new Date().toISOString()})+'\\n',{mode:0o600});\nif(kind==='settle' && NodeFSP.existsSync(root+'/block-settle')) { while(NodeFSP.existsSync(root+'/block-settle')) await NodeTimersPromises.setTimeout(50); }\nNodeFSP.appendFileSync(root+'/script-events.jsonl',JSON.stringify({kind:kind+'-completed',cwd:process.cwd(),home:process.env.HOME,pid:process.pid,at:new Date().toISOString()})+'\\n');\n`;
    await NodeFSP.writeFile(NodePath.join(root, "project-script.mjs"), script, {
      flag: "wx",
      mode: 0o600,
    });
    // Handoff waits for setup writeCompletion, not script exit. This owned zsh
    // startup gate blocks that write; tiny output prevents quiet-shell readiness.
    // It loads no protected shell/profile content.
    await NodeFSP.writeFile(
      NodePath.join(home, ".zshrc"),
      `if [[ -f ${quote(NodePath.join(root, "block-setup-write"))} ]]; then\n  printf '%s\\n' "$PWD" "$$" >> ${quote(NodePath.join(root, "setup-shell-started"))}\n  while [[ -f ${quote(NodePath.join(root, "block-setup-write"))} ]]; do\n    printf 'qualification startup pending\\r'\n    /bin/sleep 0.05\n  done\nfi\n`,
      { flag: "wx", mode: 0o600 },
    );
    const projectScripts = ["setup", "settle"].map((kind) => ({
      id: `lifecycle-${kind}`,
      name: `Lifecycle ${kind}`,
      command: `${quote(v.tools.node)} ${quote(NodePath.join(root, "project-script.mjs"))} ${quote(root)} ${kind}`,
      icon: "test",
      runOnWorktreeCreate: kind === "setup",
      runOnSettle: kind === "settle",
    }));
    validateOperator(v);
    await start();
    await api("/api/projects/mutate", {
      type: "project.create",
      commandId: NodeCrypto.randomUUID(),
      projectId: v.projectId,
      title: "Synthetic lifecycle proof",
      workspaceRoot: repo,
      scripts: projectScripts,
    });
    const before = await sourceInventory();
    phase = "message";
    const main = await launch("message", readPrompt);
    const original = await conversationOracle(main, before, "message");
    const nativeBefore = native(main);
    phase = "resume";
    await stop();
    await start();
    const beforeResume = await sourceInventory(),
      resumeHistory = history(main),
      completedBefore = payloads("provider_turns", main).filter(
        (r) => r.status === "completed",
      ).length;
    await message(main, readPrompt);
    await completed(main, completedBefore);
    const resumed = await conversationOracle(main, beforeResume, "resume", resumeHistory);
    NodeAssert.equal(resumed.path, original.path);
    NodeAssert.equal(native(main).nativeThreadId, nativeBefore.nativeThreadId);
    phase = "empty";
    const empty = await launch("empty");
    const emptyBinding = binding(empty);
    NodeAssert.ok(emptyBinding);
    NodeAssert.equal(emptyBinding.state, "conversation");
    NodeAssert.equal(payloads("runs", empty).length, 0);
    NodeAssert.equal(
      rows("SELECT thread_id FROM jones_idle_worktree_admission WHERE thread_id=?", empty).length,
      0,
    );
    NodeAssert.equal((await scripts()).filter((r) => r.kind === "setup").length, 0);
    const beforeEmpty = await sourceInventory();
    await message(empty, readPrompt);
    await conversationOracle(empty, beforeEmpty, "empty");

    phase = "handoff-replay-restart";
    const key = `dev-handoff:${NodeCrypto.randomUUID()}`,
      branch = "work/lifecycle-main";
    const replayRequest = { clientRequestId: key, branch, continuationPrompt: "placeholder" };
    // The exact same request is replayed from the real continued provider turn.
    // Its prompt omits a recursive self-reference: the original continuation is
    // obtained from the already printed request in this native conversation.
    replayRequest.continuationPrompt =
      "You are the single continued synthetic lifecycle turn. Run /bin/pwd once. Replay t3_worktree_handoff with EXACTLY the clientRequestId, branch and continuationPrompt of your previous handoff (read them from this conversation, do not invent a new key or prompt). Then end. No other tools, writes or children.";
    const beforeHandoffCommands = payloads("turn_items", main).filter(
      (r) => r.type === "command_execution",
    ).length;
    await message(
      main,
      `Call t3_worktree_handoff with exactly ${JSON.stringify(replayRequest)} as your last action. This is the authorized synthetic checkout only. No children or other tools.`,
    );
    const attached = await wait("durable handoff", () => {
      const r = handoff(main);
      return r?.phase === "done" ? r : undefined;
    });
    await wait("actual continued turn and replay", () => {
      const replayed = tool(main, "t3_worktree_handoff").filter(
        (r) => r.input?.clientRequestId === key,
      );
      return (
        replayed.length >= 2 &&
        replayed.some((r) => r.status === "completed") &&
        payloads("turn_items", main).filter((r) => r.type === "command_execution").length >
          beforeHandoffCommands
      );
    });
    await completed(main);
    NodeAssert.equal(attached.commit, sourceHead);
    NodeAssert.equal(attached.requestId, key);
    NodeAssert.equal(attached.result?.worktreePath, attached.path);
    NodeAssert.equal(attached.result?.branch, branch);
    NodeAssert.equal(attached.result?.setupScript.status, "started");
    NodeAssert.equal(attached.result?.continuation.status, "scheduled");
    NodeAssert.equal(await git(attached.path, "rev-parse", "HEAD"), sourceHead);
    NodeAssert.equal(
      (await scripts()).filter((r) => r.kind === "setup" && r.cwd === attached.path).length,
      1,
    );
    const continued = cwdEvidence(main, attached.path, beforeHandoffCommands);
    NodeAssert.equal(continued.length, 1);
    NodeAssert.equal(new Set(continued.map((r) => r.providerTurnId)).size, 1);
    NodeAssert.equal(native(main).nativeThreadId, nativeBefore.nativeThreadId);
    const replayItems = tool(main, "t3_worktree_handoff").filter(
      (r) => r.input?.clientRequestId === key,
    );
    NodeAssert.equal(replayItems.length, 2);
    NodeAssert.deepEqual(replayItems[0].input, replayItems[1].input);
    const continuedReplay = assertHandoffReplay(
      replayItems.find((r) => r.providerTurnId === continued[0].providerTurnId),
      replayRequest,
      attached.result,
      continued[0],
    );
    NodeAssert.equal(await exists(original.path), false);
    await stop();
    await start();
    NodeAssert.deepEqual(handoff(main), attached);
    const restartHistory = history(main),
      replayTurnCount = payloads("provider_turns", main).filter(
        (r) => r.status === "completed",
      ).length;
    await message(
      main,
      `Replay t3_worktree_handoff with exactly ${JSON.stringify(replayRequest)} once and end. No pwd, writes, setup or children. This is a replay after isolated restart; do not schedule another continuation.`,
    );
    await completed(main, replayTurnCount);
    const restartScope = completedScope(main, restartHistory);
    const restartReplays = tool(main, "t3_worktree_handoff").filter(
      (r) => !replayItems.some((previous) => previous.id === r.id),
    );
    receipt.handoffRestartReplay = { scope: restartScope, items: restartReplays };
    NodeAssert.equal(restartReplays.length, 1, "one new restart replay item required");
    const restartReplay = assertHandoffReplay(
      restartReplays[0],
      replayRequest,
      attached.result,
      restartScope,
    );
    NodeAssert.deepEqual(handoff(main), attached);
    NodeAssert.equal(
      tool(main, "t3_worktree_handoff").filter((r) => r.input?.clientRequestId === key).length,
      3,
    );
    NodeAssert.equal(
      payloads("turn_items", main).filter((r) => r.type === "command_execution").length,
      beforeHandoffCommands + 1,
    );
    NodeAssert.equal(
      (await scripts()).filter((r) => r.kind === "setup" && r.cwd === attached.path).length,
      1,
    );
    NodeAssert.equal(
      (await git(repo, "worktree", "list", "--porcelain")).match(/^worktree /gmu)?.length,
      2,
    );
    mark("handoff-replay-restart", {
      journal: attached,
      continued,
      native: native(main),
      replayItemIds: replayItems.map((r) => r.id),
      continuedReplay,
      restartReplay,
      setupCount: 1,
      checkoutCount: 1,
    });

    // A separate first terminal attachment must precede its first command.
    // Deliberately nonempty conversation content exercises the exact obligation.
    phase = "terminal-first";
    await NodeFSP.writeFile(
      NodePath.join(emptyBinding.path, "synthetic-note"),
      "retain this exact synthetic note\n",
      { flag: "wx", mode: 0o600 },
    );
    const terminal = await call("terminal-attach", {
      threadId: empty,
      terminalId: "dev-first",
      cwd: emptyBinding.path,
      cols: 80,
      rows: 24,
    });
    const terminalThread = payloads("threads", empty)[0];
    NodeAssert.ok(terminalThread.worktreePath && terminal.cwd === terminalThread.worktreePath);
    NodeAssert.equal(
      (await scripts()).filter((r) => r.kind === "setup" && r.cwd === terminal.cwd).length,
      1,
    );
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(emptyBinding.path, "synthetic-note"), "utf8"),
      "retain this exact synthetic note\n",
    );
    const obligation = rows(
      "SELECT path,thread_id,reason FROM jones_conversation_obligations WHERE path=?",
      emptyBinding.path,
    );
    NodeAssert.equal(obligation.length, 1);
    NodeAssert.equal(obligation[0].thread_id, empty);
    NodeAssert.equal(obligation[0].reason, "conversation_dir_nonempty");
    const terminalMarker = NodePath.join(root, "terminal-cwd.txt");
    const terminalHomeMarker = NodePath.join(root, "terminal-home.txt");
    await call("terminal-write", {
      threadId: empty,
      terminalId: "dev-first",
      data: `/bin/pwd > ${quote(terminalMarker)}; printf '%s\\n' "$HOME" > ${quote(terminalHomeMarker)}\n`,
    });
    await wait(
      "native terminal cwd and HOME",
      async () => (await exists(terminalMarker)) && (await exists(terminalHomeMarker)),
    );
    NodeAssert.equal((await NodeFSP.readFile(terminalMarker, "utf8")).trim(), terminal.cwd);
    NodeAssert.equal((await NodeFSP.readFile(terminalHomeMarker, "utf8")).trim(), home);
    for (const event of (await scripts()).filter((r) => r.kind === "setup"))
      NodeAssert.equal(event.home, home, "setup must retain private server HOME");
    await call("terminal-close", { threadId: empty, terminalId: "dev-first" });
    mark("terminal-first", {
      attachedCwd: terminal.cwd,
      terminalPid: terminal.pid,
      setupBeforeUse: true,
      home,
      providerInstanceId: "omitted",
    });
    mark("conversation-disposal", {
      emptyPath: original.path,
      emptyDisposed: true,
      retainedPath: emptyBinding.path,
      obligation,
    });

    // Durable settlement intent and native script completion, with retirement disabled.
    phase = "settlement-disabled";
    const blockSettle = NodePath.join(root, "block-settle");
    await NodeFSP.writeFile(blockSettle, "synthetic script gate\n", { flag: "wx", mode: 0o600 });
    await dispatch(main, { type: "thread.settle" });
    const intent = await wait("durable settle-script intent", () => {
      const value = rows(
        "SELECT * FROM jones_workspace_retirement_intents WHERE thread_id=? ORDER BY rowid DESC LIMIT 1",
        main,
      )[0];
      return value?.script_state === "running" ? value : undefined;
    });
    NodeAssert.equal(
      rows(
        "SELECT receipt_id FROM jones_workspace_retirement_receipts WHERE worktree=? AND state='retired'",
        attached.path,
      ).length,
      0,
    );
    NodeAssert.equal(await exists(attached.path), true);
    NodeAssert.ok(
      rows(
        "SELECT claim_id FROM jones_workspace_duty_claims WHERE worktree=? AND kind='settlement-intent' AND released_at IS NULL",
        attached.path,
      ).length > 0,
    );
    await NodeFSP.unlink(blockSettle);
    const disabledIntent = await wait("completed disabled settlement", () => {
      const value = rows(
        "SELECT * FROM jones_workspace_retirement_intents WHERE event_id=?",
        intent.event_id,
      )[0];
      return value?.script_state === "completed" && value.outcome_json ? value : undefined;
    });
    NodeAssert.equal(disabledIntent.script_exit_code, 0);
    const disabledOutcome = JSON.parse(disabledIntent.outcome_json);
    NodeAssert.ok(["proposed", "held"].includes(disabledOutcome.state));
    NodeAssert.ok(disabledOutcome.reasonCodes.includes("settlement_retirement_disabled"));
    NodeAssert.ok(
      (await scripts()).some((r) => r.kind === "settle-completed" && r.cwd === attached.path),
    );
    const checkpoints = payloads("checkpoints", main).filter((r) => r.status === "ready");
    NodeAssert.ok(checkpoints.length > 0);
    NodeAssert.equal(await exists(attached.path), true);
    mark("settlement-disabled", {
      intent: disabledIntent,
      outcome: disabledOutcome,
      checkpoints: checkpoints.map((r) => ({ id: r.id, ref: r.ref, capturedAt: r.capturedAt })),
    });
    phase = "settlement-restart";
    await stop();
    await start();
    NodeAssert.deepEqual(
      rows("SELECT * FROM jones_workspace_retirement_intents WHERE event_id=?", intent.event_id)[0],
      disabledIntent,
    );
    await dispatch(main, { type: "thread.unsettle", reason: "user" });
    NodeAssert.equal(payloads("threads", main)[0].settledOverride, "active");
    mark("settlement-restart", {
      eventId: intent.event_id,
      durableCompletionUnchanged: true,
      unsettled: true,
    });

    // Enable only this isolated instance. Production native consumers and the real
    // registry gate removal. Incomplete native coverage remains unavailable.
    phase = "retirement-recreation";
    captureGitTrace = true;
    await stop();
    await start();
    await call("settings", { storageCleanup: { ...disabled, worktreeSettledAfterDays: 0 } });
    const physicalBefore = await NodeFSP.stat(attached.path);
    const headBefore = await git(attached.path, "rev-parse", "HEAD");
    retirementUnreconciled = true;
    receipt.retirement = {
      state: "unreconciled",
      threadId: main,
      worktree: attached.path,
      expectedHead: headBefore,
      observation: { intent: null, receipt: null },
    };
    await persistReceipt();
    await dispatch(main, { type: "thread.settle" });
    const readRetirement = async () => {
      const r = rows(
        "SELECT receipt_id,receipt_json FROM jones_workspace_retirement_receipts WHERE worktree=? ORDER BY rowid DESC LIMIT 1",
        attached.path,
      )[0];
      const outcome = rows(
        "SELECT event_id,outcome_json FROM jones_workspace_retirement_intents WHERE thread_id=? ORDER BY rowid DESC LIMIT 1",
        main,
      )[0];
      const observation = { intent: outcome ?? null, receipt: r ?? null };
      if (JSON.stringify(observation) !== JSON.stringify(receipt.retirement.observation)) {
        receipt.retirement.observation = observation;
        await persistReceipt();
      }
      if (!r) {
        if (outcome?.outcome_json) {
          const parsed = JSON.parse(outcome.outcome_json);
          if (
            ["held", "unknown", "not_retired", "partial"].includes(parsed.state) &&
            !parsed.reasonCodes.every((r) =>
              [
                "settle_script_pending",
                "final_checkpoint_unfinished",
                "settle_script_recovery_pending",
              ].includes(r),
            )
          )
            throw new Unavailable("native_retirement_held; inspect bounded outcome receipt");
        }
        return undefined;
      }
      const value = JSON.parse(r.receipt_json);
      if (["unknown", "partial", "not_retired"].includes(value.state))
        throw new Unavailable("native_retirement_not_successful");
      return value.state === "retired" ? value : undefined;
    };
    const retirement = await wait("real retirement receipt", readRetirement);
    NodeAssert.equal(retirement.binding.path, attached.path);
    NodeAssert.equal(retirement.qualification.head, headBefore);
    for (const ref of retirement.preservationRefs)
      NodeAssert.equal(await git(repo, "rev-parse", ref.ref), ref.head);
    const coverage = retirement.qualification.consumers?.coverage;
    NodeAssert.ok(coverage);
    for (const name of ["native", "services", "registry", "workloads", "obligations", "process"]) {
      NodeAssert.equal(coverage[name]?.status, "complete");
      NodeAssert.equal(coverage[name]?.reason, null);
    }
    NodeAssert.equal(await exists(attached.path), false);
    const finalIntent = rows(
      "SELECT * FROM jones_workspace_retirement_intents WHERE thread_id=? ORDER BY rowid DESC LIMIT 1",
      main,
    )[0];
    NodeAssert.equal(finalIntent.script_state, "completed");
    NodeAssert.equal(finalIntent.script_exit_code, 0);
    const tracePath = NodePath.join(root, "native-git.jsonl");
    const traceStat = await NodeFSP.stat(tracePath);
    NodeAssert.ok(
      traceStat.size + receipt.diagnostics.bytes <= limits.diagnostics,
      "combined diagnostics and native trace cap",
    );
    const trace = (await NodeFSP.readFile(tracePath, "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    const removals = trace.filter(
      (r) =>
        r.event === "start" &&
        Array.isArray(r.argv) &&
        r.argv.includes("worktree") &&
        r.argv.includes("remove") &&
        r.argv.includes(attached.path),
    );
    NodeAssert.equal(removals.length, 1, "one exact native removal required");
    NodeAssert.ok(
      !removals[0].argv.some((arg) => arg === "--force" || /^-f+$/u.test(arg)),
      "native removal must be unforced",
    );
    NodeAssert.ok(
      trace.some((r) => r.event === "exit" && r.sid === removals[0].sid && r.code === 0),
    );
    NodeAssert.ok(Date.parse(finalIntent.script_completed_at) <= Date.parse(removals[0].time));
    const lastRun = payloads("runs", main).sort((a, b) => b.ordinal - a.ordinal)[0];
    NodeAssert.ok(
      payloads("checkpoints", main).some(
        (r) =>
          r.runId === lastRun.id &&
          r.status === "ready" &&
          Date.parse(r.capturedAt) <= Date.parse(removals[0].time),
      ),
    );
    const confirmed = rows(
      "SELECT state,binding_json FROM jones_workspace_lifecycle_transitions WHERE worktree=?",
      attached.path,
    );
    NodeAssert.ok(
      confirmed.some((r) => r.state === "confirmed"),
      "real registry transition required",
    );
    NodeAssert.ok(retirement.coverage && retirement.qualification);
    // No manual checkout deletion, force flag, fake registry or fabricated
    // consumer evidence can reach this point.
    await stop();
    captureGitTrace = false;
    await start();
    await dispatch(main, { type: "thread.unsettle", reason: "user" });
    NodeAssert.equal(
      await exists(attached.path),
      false,
      "unsettle defers recreation until next turn",
    );
    const priorTurns = payloads("provider_turns", main).filter(
      (r) => r.status === "completed",
    ).length;
    await message(
      main,
      "Run only /bin/pwd once and end. This is the authorized synthetic recreation check. No files, children or other tools.",
    );
    await completed(main, priorTurns);
    const recreated = payloads("threads", main)[0].worktreePath;
    NodeAssert.equal(recreated, attached.path);
    NodeAssert.equal(await git(recreated, "rev-parse", "HEAD"), headBefore);
    const physicalAfter = await NodeFSP.stat(recreated);
    const proof = await readJson(
      NodePath.join(
        await git(recreated, "rev-parse", "--absolute-git-dir"),
        "workspace-recreation.json",
      ),
    );
    NodeAssert.equal(proof.predecessor, retirement.incarnation);
    NodeAssert.equal(proof.head, headBefore);
    NodeAssert.ok(proof.breadcrumb && proof.breadcrumb !== retirement.binding.breadcrumb_id);
    mark("retirement-recreation", {
      retirement,
      unforcedRemoval: { argv: removals[0].argv, startedAt: removals[0].time, exitCode: 0 },
      completedIntent: finalIntent,
      registryTransitions: confirmed,
      receiptHead: headBefore,
      physicalBefore: { dev: physicalBefore.dev, ino: physicalBefore.ino },
      physicalAfter: { dev: physicalAfter.dev, ino: physicalAfter.ino },
      recreation: proof,
      native: native(main),
    });
    receipt.retirement.state = "reconciled";
    await persistReceipt();
    retirementUnreconciled = false;

    // Unknown setup issue/restart is a separate real provider conversation. It
    // runs last so its deliberate hold cannot contaminate successful flows.
    phase = "setup-unknown";
    const unknownThread = await launch("setup-unknown", readPrompt);
    await completed(unknownThread);
    await NodeFSP.writeFile(NodePath.join(root, "block-setup-write"), "synthetic issue gate\n", {
      flag: "wx",
      mode: 0o600,
    });
    const unknownRequest = {
      clientRequestId: `unknown:${NodeCrypto.randomUUID()}`,
      branch: "work/lifecycle-unknown",
      continuationPrompt: "End immediately. No tools or children.",
    };
    await message(
      unknownThread,
      `Call t3_worktree_handoff with exactly ${JSON.stringify(unknownRequest)} as your last action. No other tools or children.`,
    );
    const issued = await wait("setup issued", () => {
      const r = handoff(unknownThread);
      return r?.phase === "setup_issued" ? r : undefined;
    });
    const setupShell = NodePath.join(root, "setup-shell-started");
    await wait("actual setup terminal before command write", async () => await exists(setupShell));
    const startup = await NodeFSP.readFile(setupShell, "utf8");
    const [startupCwd, startupPid] = startup.trim().split("\n");
    NodeAssert.equal(startupCwd, issued.path);
    const nativeSetup = (await observe()).get(Number(startupPid));
    NodeAssert.ok(
      nativeSetup && owned.has(nativeSetup.pid),
      "native owned setup terminal required",
    );
    await stop();
    await start();
    NodeAssert.equal(handoff(unknownThread).phase, "setup_issued");
    await NodeFSP.unlink(NodePath.join(root, "block-setup-write"));
    const beforeUnknownReplay = payloads("provider_turns", unknownThread).filter(
      (r) => r.status === "completed",
    ).length;
    await message(
      unknownThread,
      `Replay t3_worktree_handoff with exactly ${JSON.stringify(unknownRequest)}. Expect handoff_unknown and end. Do not repair or rerun setup, choose a new key, or create another checkout.`,
    );
    await wait("unknown setup replay holds", () =>
      tool(unknownThread, "t3_worktree_handoff").some((r) =>
        JSON.stringify(r.output).includes("handoff_unknown"),
      ),
    );
    await completed(unknownThread, beforeUnknownReplay);
    const unknownNative = native(unknownThread);
    NodeAssert.equal(handoff(unknownThread).phase, "setup_issued");
    NodeAssert.equal(await NodeFSP.readFile(setupShell, "utf8"), startup);
    NodeAssert.equal(
      (await scripts()).filter((r) => r.kind === "setup" && r.cwd === issued.path).length,
      0,
    );
    NodeAssert.equal(
      (await scripts()).filter((r) => r.kind === "setup-completed" && r.cwd === issued.path).length,
      0,
    );
    mark("setup-unknown", {
      threadId: unknownThread,
      journal: handoff(unknownThread),
      nativeSetupTerminal: nativeSetup,
      native: unknownNative,
      issuedOnce: true,
      writeCompletionUnproved: true,
      commandNotReissued: true,
    });
    receipt.acceptance = obligations.every((key) => receipt.oracles[key] === "passed");
    NodeAssert.equal(receipt.acceptance, true);
    for (const event of await scripts())
      NodeAssert.equal(event.home, home, "project scripts must retain private server HOME");
    receipt.outcome = "passed";
  } catch (error) {
    receipt.outcome = error instanceof Unavailable ? "unavailable" : "failed";
    receipt.acceptance = false;
    if (Object.hasOwn(receipt.oracles, phase)) receipt.oracles[phase] = receipt.outcome;
    // Do not serialize causes, command output, settings or provider transcripts.
    receipt.failure = {
      oracle: phase,
      kind: error instanceof Unavailable ? "unavailable" : "assertion",
      reason: error instanceof Unavailable ? error.message : "qualification_assertion_failed",
    };
  } finally {
    cleanupMode = true;
    clearTimeout(deadline);
    for (const signal of signals) process.removeListener(signal, cancel);
    if (acquired) {
      try {
        await stop();
        await checkRoot();
        receipt.scratchInventory = await scratchInventory();
        receipt.cleanup = retirementUnreconciled
          ? {
              state: "held",
              root: v.root,
              reason: "retirement_effects_unreconciled",
              nativeClosure: "native_absence_and_listener_closure_verified",
            }
          : { state: "native_absence_and_listener_closure_verified", root: v.root };
      } catch {
        receipt.cleanup = {
          state: "held",
          root: v.root,
          reason: "exact_owned_stop_or_identity_or_consumer_absence_unproved",
        };
        receipt.acceptance = false;
        if (receipt.outcome === "passed") receipt.outcome = "failed";
      }
    }
    receipt.elapsedMs = Math.round(performance.now() - started);
    // Preserve assertion evidence separately BEFORE any recursive scratch removal.
    await persistReceipt();
    if (receipt.cleanup.state === "native_absence_and_listener_closure_verified") {
      try {
        await NodeFSP.rm(root, { recursive: true });
        NodeAssert.equal(await exists(root), false);
        receipt.cleanup.state = "removed_and_absence_verified";
      } catch {
        receipt.cleanup.state = "removal_failed_or_partial";
        receipt.acceptance = false;
        if (receipt.outcome === "passed") receipt.outcome = "failed";
      }
      await persistReceipt();
    }
  }
  return receipt;
}

// Synthetic contract fixtures never open a native path or execute a provider.
async function testNativeBindings() {
  const host = { kind: "Apple Silicon Mini" };
  const providerFor = (alias = "default") => ({
    driver: "claudeAgent",
    launcher: `${miniFleet.root}/bin/claude-fleet-${alias}`,
    launcherSha256: miniFleet[alias].launcherSha256,
    model: "synthetic-model",
    accountBinding: "synthetic-controller-binding",
    verifiedAt: new Date().toISOString(),
    consent: "operator-validated-native-consent",
    preparationReference: "synthetic-preparation",
    nativeBinding: {
      schema: "jones.workspace-lifecycle-dev.claude-fleet-native-binding/v1",
      alias,
      home: miniFleet.home,
      homePath: miniFleet[alias].homePath,
      configDir: miniFleet[alias].configDir,
      release: {
        id: miniFleet.release,
        manifestSha256: "0".repeat(64),
        qualificationReference: "synthetic-release-qualification",
      },
      nativeExecutable: {
        path: `${miniFleet.home}/.local/bin/claude`,
        realPath: `${miniFleet.home}/.local/share/claude/versions/2.1.322`,
        sha256: miniFleet.nativeSha256,
      },
      accountRoute: {
        reference: "synthetic-normal-controller-route",
        verifiedAt: new Date().toISOString(),
        preparationReference: "synthetic-preparation",
        account: miniFleet[alias].account,
        provider: "claudeAgent",
        model: "synthetic-model",
        binding: "synthetic-controller-binding",
        action: "start",
        allowance: "unknown",
        observation: miniFleet[alias].observation,
      },
    },
  });
  const operatorFor = (alias = "default") => ({
    schema: "jones.workspace-lifecycle-dev.operator/v1",
    source: {
      root: "/synthetic/source",
      commit: "a".repeat(40),
      tree: "b".repeat(40),
      manifest: "/synthetic/manifest.json",
      manifestSha256: "c".repeat(64),
      v3Root: "/synthetic/v3",
      v3Commit: "d".repeat(40),
      v3SrcTree: "e".repeat(40),
      v3Manifest: "/synthetic/v3-manifest.json",
      v3ManifestSha256: "f".repeat(64),
    },
    tools: {
      node: "/synthetic/node",
      nodeSha256: "a".repeat(64),
      pnpm: "/synthetic/pnpm",
      pnpmSha256: "b".repeat(64),
      git: "/synthetic/git",
      python: "/synthetic/python",
      pythonSha256: "c".repeat(64),
      path: "/synthetic/bin",
    },
    host: {
      ...host,
      hostname: "synthetic-mini",
      executionEnvironmentId: "synthetic-host",
      approvalReference: "synthetic-approval",
    },
    root: {
      path: `${miniFleet.home}/jones-workspace-lifecycle-dev-00000000-0000-4000-8000-000000000001`,
      dev: 1,
      ino: 2,
      uid: 501,
      gid: 20,
      cleanupAuthorized: true,
    },
    receipt: "/synthetic/receipt.json",
    environmentId: "synthetic-environment",
    projectId: "synthetic-project",
    provider: providerFor(alias),
    admission: {
      cpuUsedPercent: 10,
      availableMemoryBytes: GiB,
      observedAt: new Date().toISOString(),
      preparationReference: "synthetic-preparation",
    },
  });
  for (const alias of ["default", "jace"]) {
    const provider = providerFor(alias);
    validateOperator(operatorFor(alias));
    const translated = providerInstanceBinding(provider, "/synthetic/private/provider-home");
    NodeAssert.deepEqual(translated.environment, [
      { name: "HOME", value: miniFleet.home, sensitive: false },
    ]);
    NodeAssert.equal(translated.config.homePath, miniFleet[alias].homePath);
    NodeAssert.equal(translated.config.binaryPath, provider.launcher);
    NodeAssert.equal(
      translated.environment.some((variable) => variable.name === "CLAUDE_CONFIG_DIR"),
      false,
    );
  }
  const rejected = [
    (p) => {
      p.driver = "codex";
    },
    (p) => {
      p.nativeBinding.alias = "other";
    },
    (p) => {
      p.nativeBinding.home = "/synthetic/private/home";
    },
    (p) => {
      p.nativeBinding.homePath = `${miniFleet.home}/.claude`;
    },
    (p) => {
      p.nativeBinding.configDir = "";
    },
    (p) => {
      p.nativeBinding.environment = { HOME: miniFleet.home };
    },
    (p) => {
      p.nativeBinding.release.id = "0".repeat(40);
    },
    (p) => {
      p.launcherSha256 = "0".repeat(64);
    },
    (p) => {
      p.launcher = "/synthetic/other-launcher";
    },
    (p) => {
      p.nativeBinding.nativeExecutable.path = "/synthetic/other-native";
    },
    (p) => {
      p.nativeBinding.nativeExecutable.realPath = "/outside/versions/2.1.322";
    },
    (p) => {
      p.nativeBinding.nativeExecutable.sha256 = "0".repeat(64);
    },
    (p) => {
      p.nativeBinding.accountRoute.account = "jace";
    },
    (p) => {
      p.nativeBinding.accountRoute.provider = "other";
    },
    (p) => {
      p.nativeBinding.accountRoute.model = "other";
    },
    (p) => {
      p.nativeBinding.accountRoute.binding = "other";
    },
    (p) => {
      p.nativeBinding.accountRoute.preparationReference = "other";
    },
    (p) => {
      p.nativeBinding.accountRoute.allowance = "exhausted";
    },
    (p) => {
      p.nativeBinding.accountRoute.observation = "native-authenticated";
    },
    (p) => {
      p.nativeBinding.accountRoute.verifiedAt = new Date(Date.now() - 120001).toISOString();
    },
    (p) => {
      p.verifiedAt = new Date(Date.now() - 120001).toISOString();
    },
    (p) => {
      p.nativeBinding.accountRoute.verifiedAt = new Date(Date.now() + 60000).toISOString();
    },
    (p) => {
      p.consent = "not-validated";
    },
    (p) => {
      delete p.nativeBinding;
    },
  ];
  for (const mutate of rejected) {
    const provider = providerFor();
    mutate(provider);
    NodeAssert.throws(() => validateNativeBinding(provider, host));
  }
  NodeAssert.throws(() => validateNativeBinding(providerFor(), { kind: "Apple Silicon laptop" }));
  NodeAssert.deepEqual(
    providerInstanceBinding(
      { launcher: "/synthetic/private-cli" },
      "/synthetic/private/provider-home",
    ),
    {
      config: {
        binaryPath: "/synthetic/private-cli",
        homePath: "/synthetic/private/provider-home",
      },
    },
  );
  const badOperators = [
    (v) => {
      v.provider.environment = { HOME: miniFleet.home };
    },
    (v) => {
      v.provider.preparationReference = "";
    },
    (v) => {
      v.admission.observedAt = new Date(Date.now() - 120001).toISOString();
    },
    (v) => {
      v.root.path = `${miniFleet.home}/.claude/jones-workspace-lifecycle-dev-00000000-0000-4000-8000-000000000001`;
    },
    (v) => {
      v.receipt = `${miniFleet.home}/.claude/receipt.json`;
    },
    (v) => {
      v.receipt = `${miniFleet.root}/receipt.json`;
    },
  ];
  for (const mutate of badOperators) {
    const operator = operatorFor();
    mutate(operator);
    NodeAssert.throws(() => validateOperator(operator));
  }
  const chainFixture = () => {
    const provider = providerFor();
    const binding = provider.nativeBinding;
    const release = `${miniFleet.root}/releases/${binding.release.id}`;
    const files = new Map();
    const manifest = {};
    for (const name of ["claude_launcher.py", "core.py", "node.py"]) {
      const relative = `src/host_tooling/auth_fleet/${name}`;
      const content = Buffer.from(`synthetic:${name}`);
      manifest[relative] = sha(content);
      files.set(`${release}/${relative}`, content);
    }
    const bytes = Buffer.from(JSON.stringify(manifest));
    binding.release.manifestSha256 = sha(bytes);
    files.set(`${release}/manifest.json`, bytes);
    files.set(provider.launcher, Buffer.from("synthetic-launcher"));
    files.set(binding.nativeExecutable.realPath, Buffer.from("synthetic-native"));
    const links = new Map([
      [`${miniFleet.root}/current`, release],
      [binding.nativeExecutable.path, binding.nativeExecutable.realPath],
    ]);
    const fileSystem = {
      realpath: async (file) => links.get(file) ?? file,
      lstat: async (file) => {
        NodeAssert.ok(files.has(file), "unexpected fixture read");
        return {
          uid: 501,
          mode: 0o755,
          size: files.get(file).length,
          isFile: () => true,
          isSymbolicLink: () => false,
        };
      },
      readFile: async (file) => {
        NodeAssert.ok(files.has(file));
        return files.get(file);
      },
    };
    // Fixed shipped hashes cannot have synthetic preimages. Substitute only
    // those two byte identities; manifest and source digests use real SHA-256.
    const digest = (bytes) =>
      bytes.toString() === "synthetic-launcher"
        ? provider.launcherSha256
        : bytes.toString() === "synthetic-native"
          ? miniFleet.nativeSha256
          : sha(bytes);
    const verify = () => verifyNativeBinding(provider, host, fileSystem, digest);
    return { provider, binding, release, files, links, fileSystem, verify };
  };
  const valid = chainFixture();
  const observation = await valid.verify();
  NodeAssert.equal(observation.state, "qualified-launcher-release-executable-chain-observed");
  NodeAssert.equal(observation.account.state, "not-observed");
  const badChains = [
    (f) => f.links.set(`${miniFleet.root}/current`, "/synthetic/unqualified-release"),
    (f) => f.links.set(f.binding.nativeExecutable.path, "/synthetic/wrong-native"),
    (f) => f.files.set(f.provider.launcher, Buffer.from("tampered-launcher")),
    (f) => f.files.set(f.binding.nativeExecutable.realPath, Buffer.from("tampered-native")),
    (f) => f.files.set(`${f.release}/manifest.json`, Buffer.from("{}")),
    (f) =>
      f.files.set(`${f.release}/src/host_tooling/auth_fleet/core.py`, Buffer.from("tampered-core")),
    (f) => {
      f.fileSystem.lstat = async () => ({
        uid: 501,
        size: 1,
        mode: 0o755,
        isFile: () => true,
        isSymbolicLink: () => true,
      });
    },
    (f) => {
      f.fileSystem.lstat = async () => ({
        uid: 501,
        size: 1,
        mode: 0o600,
        isFile: () => true,
        isSymbolicLink: () => false,
      });
    },
    (f) => {
      const realpath = f.fileSystem.realpath;
      let currentReads = 0;
      f.fileSystem.realpath = async (file) =>
        file === `${miniFleet.root}/current` && ++currentReads > 1
          ? "/synthetic/changed-release"
          : realpath(file);
    },
  ];
  for (const mutate of badChains) {
    const fixture = chainFixture();
    mutate(fixture);
    await NodeAssert.rejects(fixture.verify());
  }
  console.log(
    JSON.stringify({
      schema: "jones.workspace-lifecycle-dev.native-binding-fixtures/v1",
      outcome: "passed",
      acceptedAliases: 2,
      rejectedTuples: rejected.length + 1,
      rejectedOperators: badOperators.length,
      rejectedChains: badChains.length,
      nativeAccountObservation: "not-observed",
      execution: "synthetic-only",
    }),
  );
}

async function main() {
  const args = process.argv.slice(2);
  if (args.length === 1 && args[0] === "--test-native-bindings") {
    await testNativeBindings();
    return;
  }
  if (args.length === 0) {
    console.log(
      JSON.stringify({
        schema,
        outcome: "unavailable",
        reason: "explicit_live_opt_in_and_operator_bindings_required",
        acceptance: false,
      }),
    );
    return;
  }
  if (args.length !== 3 || args[0] !== "--live" || args[1] !== "--operator")
    throw new Unavailable("use_--live_--operator_absolute_bindings_json");
  const file = absolute(args[2]),
    stat = await NodeFSP.lstat(file);
  NodeAssert.ok(
    stat.isFile() &&
      !stat.isSymbolicLink() &&
      stat.uid === process.getuid() &&
      (stat.mode & 0o077) === 0,
  );
  const operator = validateOperator(await readJson(file, 32768));
  NodeAssert.equal(
    NodePath.resolve(NodeURL.fileURLToPath(import.meta.url)),
    NodePath.join(operator.source.root, driverRelative),
  );
  const result = await qualify(operator);
  console.log(
    JSON.stringify({
      schema,
      outcome: result.outcome,
      acceptance: result.acceptance,
      receipt: operator.receipt,
      cleanup: result.cleanup.state,
    }),
  );
  if (!result.acceptance) process.exitCode = result.outcome === "unavailable" ? 3 : 1;
}
if (process.argv[1] === NodeURL.fileURLToPath(import.meta.url))
  await main().catch((error) => {
    if (process.argv.length === 3 && process.argv[2] === "--test-native-bindings") {
      console.error(error);
      process.exitCode = 1;
      return;
    }
    console.log(
      JSON.stringify({
        schema,
        outcome: "unavailable",
        reason: "operator_binding_or_preflight_unavailable",
        acceptance: false,
      }),
    );
    process.exitCode = 3;
  });
