// @effect-diagnostics nodeBuiltinImport:off -- Test-only owned Git artifacts and native consumer children.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeTimers from "node:timers";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type { WorkstreamsNativeBuild } from "@t3tools/contracts";
import type { NativeEnrollments } from "../enrollment/service.ts";
import type { NativeEnrollmentRequest } from "../enrollment/request.ts";
import type { NativeCredentialWriter } from "../enrollment/credentialFile.ts";
import type { NativeEnrollmentOperationPorts } from "../enrollment/operations.ts";
import type { NativeProviderEnrollmentBinding } from "../nativeProvider/enrollment.ts";
import type { WorkstreamsNativeProvider } from "../nativeProvider/service.ts";
import type * as AuthSessions from "../../../persistence/AuthSessions.ts";
import type { startLoopbackControlPlane } from "./loopbackControlPlane.ts";

const receiving = NodePath.resolve(import.meta.dirname, "../../../../../..");
const adapter = "apps/server/src/jones/workstreams/testing/loopbackControlPlane.ts";
const worker = NodePath.join(import.meta.dirname, "sourceRenewalWorker.mjs");
const workerOverlay = "apps/server/.source-renewal-worker.mjs";
const manifestPaths = ["apps/server/package.json", "apps/desktop/package.json"];
const producer = "scripts/stamp-jones-build.ts";
const producerHash = "43d08364f2756386778c62174acc5deab4dd2cb8f4eddaa64783ca137a731aba";
const lockHash = "9581e385d495be3e0e9ef0a132f60e93d14abc0a2d20d5c1900a29f4ffcb2f55";
export const artifactBuilds = {
  old: {
    repository: "Jones-Systems/Jones-Code",
    sha: "fe38779d2d6267e27d3f00e5de3578d0f92d43b0",
    tree: "e3d1d8e4bbefcf3e34189c1ee061396919d00f87",
  },
  next: {
    repository: "Jones-Systems/Jones-Code",
    sha: "a74cb0ce9f730363997011228695ac095c344a31",
    tree: "248ecd4b31ba878dfc2602b009b62d46cde23b15",
  },
} as const;
const sha256 = (bytes: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const fileHash = async (path: string) => sha256(await NodeFSP.readFile(path));
const receipt = (event: string, data: unknown) =>
  process.stdout.write(`${JSON.stringify({ event, data })}\n`);
const cleanEnvironment = (): NodeJS.ProcessEnv => ({
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_CONFIG_GLOBAL: "/dev/null",
  ...Object.fromEntries(
    Object.entries(process.env).filter(
      ([key]) => !/^(GIT_|GITHUB_|JONES_|CI$|NODE_OPTIONS$|VITE_|T3CODE_)/.test(key),
    ),
  ),
});

function command(executable: string, args: string[], cwd: string, env = cleanEnvironment()) {
  return new Promise<string>((resolve, reject) => {
    NodeChildProcess.execFile(
      executable,
      args,
      { cwd, env, timeout: 300_000, maxBuffer: 1024 * 1024 },
      (error, stdout, stderr) => {
        if (error)
          reject(
            new Error(`Artifact producer failed: ${executable}: ${String(stderr).slice(-4096)}`, {
              cause: error,
            }),
          );
        else resolve(stdout.trim());
      },
    );
  });
}
const git = (root: string, ...args: string[]) => command("git", args, root);
async function admission(root: string) {
  const disk = await NodeFSP.statfs(root, { bigint: true });
  NodeAssert.ok(
    disk.bavail * disk.bsize >= 50n * 1024n ** 3n,
    "artifact preparation needs 40 GiB reserve plus 10 GiB combined peak",
  );
  const sample = () =>
    NodeOS.cpus().reduce(
      (all, cpu) => ({
        idle: all.idle + cpu.times.idle,
        total: all.total + Object.values(cpu.times).reduce((a, b) => a + b, 0),
      }),
      { idle: 0, total: 0 },
    );
  const before = sample();
  await Effect.runPromise(Effect.sleep(100));
  const after = sample();
  const used = 100 * (1 - (after.idle - before.idle) / (after.total - before.total));
  const memory = /MemAvailable:\s+(\d+) kB/.exec(await NodeFSP.readFile("/proc/meminfo", "utf8"));
  NodeAssert.ok(
    memory && Number(memory[1]) * 1024 > 3 * 1024 ** 3 && used < 85,
    "fresh artifact CPU/RAM admission refused",
  );
  receipt("source-renewal producer admission", {
    used,
    availableBytes: Number(memory[1]) * 1024,
    diskAvailableBytes: String(disk.bavail * disk.bsize),
    serialTestCeiling: 1,
  });
}
type Artifact = {
  root: string;
  build: WorkstreamsNativeBuild;
  digest: string;
  inputs: Record<string, string>;
  production: Record<string, string>;
};
async function inputHashes(root: string) {
  const paths = (await git(root, "ls-files", "-z", "--", ".", ":(exclude).repos"))
    .split("\0")
    .filter((path) => path && !path.startsWith(".repos/"));
  const inputs: Record<string, string> = {};
  let bytes = 0;
  for (const path of paths) {
    const full = NodePath.join(root, path);
    const data = (await NodeFSP.lstat(full)).isSymbolicLink()
      ? Buffer.from(await NodeFSP.readlink(full))
      : await NodeFSP.readFile(full);
    bytes += data.length;
    NodeAssert.ok(bytes <= 1024 ** 3, "artifact source input cap");
    inputs[path] = sha256(data);
  }
  return inputs;
}
async function verify(artifact: Artifact) {
  NodeAssert.equal(await NodeFSP.realpath(artifact.root), artifact.root);
  NodeAssert.equal(await git(artifact.root, "rev-parse", "HEAD"), artifact.build.sha);
  NodeAssert.equal(await git(artifact.root, "rev-parse", "HEAD^{tree}"), artifact.build.tree);
  NodeAssert.equal(
    sha256(JSON.stringify(await inputHashes(artifact.root))),
    artifact.digest,
    "artifact bytes changed before consumption",
  );
  NodeAssert.equal(await fileHash(worker), artifact.inputs["$worker"]);
  NodeAssert.equal(
    await fileHash(NodePath.join(artifact.root, workerOverlay)),
    artifact.inputs["$worker"],
  );
  NodeAssert.equal(await fileHash(process.execPath), artifact.inputs["$node"]);
  NodeAssert.equal(await fileHash(familyHelper), artifact.inputs["$supervisor"]);
  NodeAssert.equal(await fileHash("/usr/bin/python3"), artifact.inputs["$python"]);
}
export async function prepareSourceRenewalArtifacts() {
  NodeAssert.equal(process.version, "v26.12.1");
  const parent = process.env.JONES_SOURCE_RENEWAL_ARTIFACT_PARENT;
  NodeAssert.ok(
    parent && NodePath.isAbsolute(parent) && !parent.split(NodePath.sep).includes("node_modules"),
    "artifact mode requires an absolute disk-backed parent outside node_modules",
  );
  NodeAssert.equal(await NodeFSP.realpath(parent), parent);
  NodeAssert.notEqual(
    (await NodeFSP.statfs(parent)).type,
    0x01021994,
    "artifact parent must not be tmpfs",
  );
  const parentStat = await NodeFSP.lstat(parent);
  NodeAssert.ok(
    parentStat.isDirectory() &&
      parentStat.uid === process.getuid!() &&
      (parentStat.mode & 0o777) === 0o700,
    "Artifact parent must be owned mode 0700",
  );
  NodeAssert.deepEqual(await NodeFSP.readdir(parent), [], "Artifact parent must be empty");
  await probeConsumerFamily();
  await admission(parent);
  const root = await NodeFSP.mkdtemp(NodePath.join(parent, ".source-renewal-artifacts-"));
  const rootStat = await NodeFSP.lstat(root);
  const custody = makeArtifactCustody({
    custodyRoot: root,
    custodyRootIdentity: [rootStat.dev, rootStat.ino, rootStat.uid],
  });
  const close = async () => {
    await custody.remove(() => NodeFSP.rm(root, { recursive: true }));
    await NodeAssert.rejects(NodeFSP.stat(root), { code: "ENOENT" });
    receipt("source-renewal artifacts absent", root);
  };
  try {
    const artifacts: Artifact[] = [];
    for (const [name, build] of Object.entries(artifactBuilds)) {
      await admission(root);
      const destination = NodePath.join(root, name);
      await NodeFSP.mkdir(destination, { mode: 0o700 });
      await git(destination, "init", "--initial-branch", `fixture-${name}`);
      await git(destination, "fetch", "--no-tags", "--depth=1", `file://${receiving}`, build.sha);
      await git(destination, "sparse-checkout", "set", "--no-cone", "/*", "!/.repos/");
      await git(destination, "checkout", "-b", `source-${name}`, "FETCH_HEAD");
      NodeAssert.equal(await git(destination, "status", "--porcelain"), "");
      NodeAssert.equal(await git(destination, "rev-parse", "HEAD"), build.sha);
      NodeAssert.equal(await git(destination, "rev-parse", "HEAD^{tree}"), build.tree);
      await NodeAssert.rejects(
        NodeFSP.stat(NodePath.join(destination, ".git/objects/info/alternates")),
        { code: "ENOENT" },
      );
      const original = await inputHashes(destination);
      NodeAssert.equal(original[producer], producerHash);
      NodeAssert.equal(original["pnpm-lock.yaml"], lockHash);
      await command(process.execPath, [NodePath.join(destination, producer)], destination, {
        ...cleanEnvironment(),
        JONES_EXPECTED_SOURCE_SHA: build.sha,
      });
      const stamped = await inputHashes(destination);
      NodeAssert.deepEqual(
        Object.keys(original)
          .filter((key) => original[key] !== stamped[key])
          .sort(),
        [...manifestPaths].sort(),
      );
      for (const path of manifestPaths) {
        const json = JSON.parse(await NodeFSP.readFile(NodePath.join(destination, path), "utf8"));
        NodeAssert.deepEqual(json.jonesSource, build);
        NodeAssert.equal(json.startupGateProtocol, 1);
      }
      await admission(root);
      NodeAssert.equal(await command("pnpm", ["--version"], destination), "12.14.0");
      await command(
        "pnpm",
        ["--filter", "t3...", "install", "--frozen-lockfile", "--offline", "--ignore-scripts"],
        destination,
      );
      NodeAssert.deepEqual(
        await inputHashes(destination),
        stamped,
        "dependency preparation changed source inputs",
      );
      // Disclosed test-only overlay; production source identities below retain their original bytes.
      await NodeFSP.copyFile(
        NodePath.join(receiving, adapter),
        NodePath.join(destination, adapter),
      );
      const production = Object.fromEntries(
        Object.entries(stamped).filter(
          ([key]) =>
            key.startsWith("apps/server/src/") ||
            key.startsWith("packages/") ||
            /package\.json$|pnpm|patches\//.test(key),
        ),
      );
      await NodeFSP.copyFile(worker, NodePath.join(destination, workerOverlay));
      const actual = await inputHashes(destination);
      const inputs = {
        ...actual,
        $worker: await fileHash(worker),
        $node: await fileHash(process.execPath),
        $supervisor: await fileHash(familyHelper),
        $python: await fileHash("/usr/bin/python3"),
      };
      const artifact = {
        root: destination,
        build,
        digest: sha256(JSON.stringify(actual)),
        inputs,
        production,
      };
      artifacts.push(artifact);
      receipt("source-renewal artifact produced", {
        root: destination,
        build,
        digest: artifact.digest,
        producerSha256: original[producer],
        originalManifests: manifestPaths.map((key) => [key, original[key]]),
        stampedManifests: manifestPaths.map((key) => [key, stamped[key]]),
        overlay: { path: adapter, original: original[adapter], sha256: actual[adapter] },
        dependencyLock: original["pnpm-lock.yaml"],
        productionDigest: sha256(JSON.stringify(production)),
        workerOverlay: { path: workerOverlay, sha256: inputs.$worker },
        workerSha256: inputs.$worker,
        nodeSha256: inputs.$node,
        supervisorSha256: inputs.$supervisor,
        pythonSha256: inputs.$python,
        node: process.version,
        pnpm: "12.14.0",
      });
      await verify(artifact);
      const allocation = Number(
        (
          await command("du", ["-s", "--block-size=1", "--exclude=node_modules", root], receiving)
        ).split(/\s/)[0],
      );
      NodeAssert.ok(allocation <= 2 * 1024 ** 3, "combined source-production cap exceeded");
    }
    return {
      old: artifacts[0]!,
      next: artifacts[1]!,
      close,
      ...custody,
      record: (event: string, data: unknown) => {
        NodeFS.appendFileSync(
          NodePath.join(root, "consumer-receipts.jsonl"),
          `${JSON.stringify({ event, data })}\n`,
        );
        receipt(event, data);
      },
    };
  } catch (error) {
    try {
      await close();
    } catch (cleanup) {
      receipt("source-renewal preparation cleanup failed", {
        primary: describe(error),
        cleanup: describe(cleanup),
        retainedRoot: root,
      });
    }
    throw error;
  }
}
export type SourceRenewalArtifacts = Awaited<ReturnType<typeof prepareSourceRenewalArtifacts>>;
type WorkerIdentity = {
  pid: number;
  root: string;
  build: WorkstreamsNativeBuild;
  providerSha256: string;
  registrationSha256: string;
  manifestSha256: string;
  effect: string;
  contracts: string;
};

export const lifecycleBudgets = { startup: 5_000, operation: 5_000, shutdown: 5_000 } as const;
const describe = (error: unknown) =>
  String(error && typeof error === "object" && "message" in error ? error.message : error).slice(
    -1024,
  );

type RootBinding = { custodyRoot: string; custodyRootIdentity: number[] };
type Lease = { attempt: number; nonce: string };
type FamilyReceipt = RootBinding &
  Lease & {
    schema: string;
    requestSha256: string;
    supervisorIdentity: number[];
    consumerIdentity: number[];
    consumerExit: { code: number | null; signal: string | null };
    controlIdentity: number[];
    outcome: string;
    signalsSent: unknown[];
    familyClosed: boolean;
    reaped: boolean;
    allIdentitiesExited: boolean;
    ipcFdReleased: boolean;
    processOverflow: boolean;
    ownershipUnresolved: boolean;
  };
type ObservedClosure = {
  supervisor: {
    pid: number | null;
    exit: { code: number | null; signal: string | null } | null;
    handlesClosed: boolean;
  };
  ipc: { disconnected: boolean };
  worker: { returnedPid: number | null };
  control: string;
  outputBytes: number;
  streams: FamilyStreamObservation;
};
const familyHelper = NodePath.join(import.meta.dirname, "sourceRenewalFamily.py");
const familyBounds = {
  executionDeadlineMs: 600_000,
  terminateGraceMs: 2_000,
  reapTimeoutMs: 3_000,
  maxTracked: 8,
  maxReceiptBytes: 65_536,
};

export async function probeConsumerFamily(platform = HostProcess.Platform.defaultValue()) {
  NodeAssert.equal(platform, "linux", "Source renewal family supervision requires Linux");
  const result = JSON.parse(
    await command("/usr/bin/python3", ["-I", "-B", familyHelper, "--probe"], receiving),
  );
  NodeAssert.equal(result.supported, true);
  const pythonVersion = await command("/usr/bin/python3", ["--version"], receiving);
  const version = /^Python (\d+)\.(\d+)/.exec(pythonVersion);
  NodeAssert.ok(
    version && (Number(version[1]) > 3 || (Number(version[1]) === 3 && Number(version[2]) >= 9)),
    "Python 3.9 or newer required",
  );
  receipt("source-renewal family capability", {
    ...result,
    helperSha256: await fileHash(familyHelper),
    pythonPath: "/usr/bin/python3",
    pythonSha256: await fileHash("/usr/bin/python3"),
    pythonVersion,
    kernel: NodeOS.release(),
  });
}

export function makeArtifactCustody(rootBinding: RootBinding) {
  const binding = structuredClone(rootBinding);
  let fenced: string | undefined;
  let removed = false;
  const ledger = new Map<
    number,
    Lease & { requestSha256?: string; receiptPath?: string; state: "open" | "settled" }
  >();
  const assertActive = () => {
    if (fenced || removed) throw new Error(`Artifact lifecycle fenced: ${fenced ?? "removed"}`);
  };
  const fence = (reason = "consumer family closure unknown") => {
    fenced ??= reason;
  };
  const requireRemoval = () => {
    assertActive();
    NodeAssert.ok(
      [...ledger.values()].every((entry) => entry.state === "settled"),
      "Artifact attempts still open",
    );
  };
  return {
    fence,
    assertActive,
    acquire: (): Lease => {
      assertActive();
      if (ledger.size >= 8) {
        fence("fork attempt limit");
        assertActive();
      }
      const lease = { attempt: ledger.size + 1, nonce: NodeCrypto.randomUUID() };
      ledger.set(lease.attempt, { ...lease, state: "open" });
      return lease;
    },
    bind: (lease: Lease, requestSha256: string, receiptPath: string) => {
      try {
        assertActive();
        const entry = ledger.get(lease.attempt);
        NodeAssert.ok(
          entry && entry.nonce === lease.nonce && !entry.requestSha256 && entry.state === "open",
          "Foreign or duplicate request binding",
        );
        entry.requestSha256 = requestSha256;
        entry.receiptPath = receiptPath;
      } catch (error) {
        fence(describe(error));
        throw error;
      }
    },
    rootBinding: () => structuredClone(binding),
    // Diagnostic binding only: this projection cannot reconstruct release authority.
    requestBinding: (attempt: number) => {
      const entry = ledger.get(attempt);
      NodeAssert.ok(entry?.requestSha256 && entry.receiptPath, "Missing original request binding");
      return structuredClone({
        ...binding,
        attempt,
        nonce: entry.nonce,
        requestSha256: entry.requestSha256,
        receiptPath: entry.receiptPath,
      });
    },
    readReceipt: (attempt: number): FamilyReceipt => {
      const path = ledger.get(attempt)?.receiptPath;
      NodeAssert.ok(path, "Missing original receipt path");
      return readFamilyReceipt(binding, path);
    },
    settle: (attempt: number, proof: FamilyReceipt, observed: ObservedClosure) => {
      try {
        assertActive();
        const entry = ledger.get(attempt);
        NodeAssert.ok(
          entry && entry.state === "open" && entry.requestSha256 && entry.receiptPath,
          "Unknown or duplicate settlement",
        );
        NodeAssert.equal(proof.schema, "jones-source-renewal-family-receipt/v1");
        NodeAssert.equal(proof.attempt, attempt);
        NodeAssert.equal(proof.nonce, entry.nonce);
        NodeAssert.equal(proof.requestSha256, entry.requestSha256);
        NodeAssert.equal(proof.custodyRoot, binding.custodyRoot);
        NodeAssert.deepEqual(proof.custodyRootIdentity, binding.custodyRootIdentity);
        NodeAssert.equal(proof.outcome, "success");
        NodeAssert.deepEqual(proof.consumerExit, { code: 0, signal: null });
        NodeAssert.deepEqual(proof.signalsSent, []);
        for (const key of [
          "familyClosed",
          "reaped",
          "allIdentitiesExited",
          "ipcFdReleased",
        ] as const)
          NodeAssert.equal(proof[key], true);
        NodeAssert.equal(proof.processOverflow, false);
        NodeAssert.equal(proof.ownershipUnresolved, false);
        NodeAssert.deepEqual(observed.supervisor.exit, { code: 0, signal: null });
        NodeAssert.equal(observed.supervisor.handlesClosed, true);
        assertNaturalFamilyStreams(observed.streams);
        NodeAssert.equal(observed.ipc.disconnected, true);
        NodeAssert.equal(observed.control, "none");
        NodeAssert.ok(observed.outputBytes <= 256 * 1024);
        NodeAssert.ok(Number.isInteger(observed.supervisor.pid) && observed.supervisor.pid! > 0);
        NodeAssert.ok(
          Number.isInteger(observed.worker.returnedPid) && observed.worker.returnedPid! > 0,
        );
        NodeAssert.equal(proof.supervisorIdentity[0], observed.supervisor.pid);
        NodeAssert.equal(proof.consumerIdentity[0], observed.worker.returnedPid);
        NodeAssert.notEqual(observed.supervisor.pid, observed.worker.returnedPid);
        NodeAssert.ok(proof.supervisorIdentity[1]! > 0 && proof.consumerIdentity[1]! > 0);
        entry.state = "settled";
      } catch (error) {
        fence(describe(error));
        throw error;
      }
    },
    transition: async (
      outgoingAttempt: number,
      stop: () => Promise<void>,
      start: () => Promise<void>,
    ) => {
      assertActive();
      try {
        await stop();
      } catch (error) {
        fence(describe(error));
        throw error;
      }
      assertActive();
      if (ledger.get(outgoingAttempt)?.state !== "settled") {
        fence("Outgoing attempt not settled");
        assertActive();
      }
      await start();
    },
    assertRemoval: requireRemoval,
    remove: async (remove: () => Promise<void>) => {
      requireRemoval();
      removed = true;
      await remove();
      receipt("source-renewal custody released", { attempts: ledger.size });
    },
  };
}

export type FamilyStreamObservation = {
  stdoutEnded: boolean;
  stderrEnded: boolean;
  prematureClose: boolean;
  parentDisposal: boolean;
  disposedBeforeEof: boolean;
};
const streamObservations = new WeakMap<object, FamilyStreamObservation>();
export function observeFamilyStreams(
  child: Pick<NodeChildProcess.ChildProcess, "stdout" | "stderr">,
) {
  const existing = streamObservations.get(child);
  if (existing) return existing;
  const observation: FamilyStreamObservation = {
    stdoutEnded: false,
    stderrEnded: false,
    prematureClose: false,
    parentDisposal: false,
    disposedBeforeEof: false,
  };
  streamObservations.set(child, observation);
  for (const [stream, key] of [
    [child.stdout, "stdoutEnded"],
    [child.stderr, "stderrEnded"],
  ] as const) {
    stream?.on("end", () => {
      observation[key] = true;
    });
    stream?.on("close", () => {
      if (!observation[key]) observation.prematureClose = true;
    });
  }
  return observation;
}
export function assertNaturalFamilyStreams(observed: FamilyStreamObservation) {
  NodeAssert.ok(
    observed.stdoutEnded &&
      observed.stderrEnded &&
      !observed.prematureClose &&
      !observed.parentDisposal &&
      !observed.disposedBeforeEof,
    "Family streams lack natural EOF or were prematurely disposed",
  );
}
export function disposeFamilyObserver(
  child: Pick<
    NodeChildProcess.ChildProcess,
    "stdout" | "stderr" | "connected" | "disconnect" | "unref"
  > & { control?: import("node:stream").Duplex | null },
  additionalStreams: ReadonlyArray<{ destroy: () => unknown } | null | undefined> = [],
) {
  const observed = observeFamilyStreams(child);
  // Record provenance before destruction can synchronously emit any observer event.
  observed.parentDisposal = true;
  observed.disposedBeforeEof ||=
    !observed.stdoutEnded || !observed.stderrEnded || observed.prematureClose;
  const failures: unknown[] = [];
  const attempt = (action: () => void) => {
    try {
      action();
    } catch (error) {
      failures.push(error);
    }
  };
  try {
    for (const stream of new Set([child.stdout, child.stderr, child.control, ...additionalStreams]))
      attempt(() => {
        stream?.destroy();
      });
    attempt(() => {
      if (child.connected) child.disconnect();
    });
  } finally {
    attempt(() => child.unref());
  }
  if (failures.length) throw new AggregateError(failures, "Fixture observer disposal failed");
}
export function readFamilyReceipt(binding: RootBinding, path: string): FamilyReceipt {
  const root = NodeFS.lstatSync(binding.custodyRoot);
  NodeAssert.ok(root.isDirectory(), "Custody root changed");
  NodeAssert.deepEqual([root.dev, root.ino, root.uid], binding.custodyRootIdentity);
  const fd = NodeFS.openSync(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const stat = NodeFS.fstatSync(fd);
    NodeAssert.ok(
      stat.isFile() && stat.nlink === 1 && stat.uid === process.getuid!() && stat.size <= 65_536,
      "Invalid receipt file",
    );
    const bytes = Buffer.alloc(65_537);
    const length = NodeFS.readSync(fd, bytes, 0, bytes.length, 0);
    NodeAssert.ok(length <= 65_536, "Receipt byte limit");
    return JSON.parse(bytes.subarray(0, length).toString("utf8")) as FamilyReceipt;
  } finally {
    NodeFS.closeSync(fd);
  }
}
export type FamilyFixtureManifest = RootBinding &
  Lease & {
    fixtureId: string;
    requestSha256: string;
    receiptPath: string;
    supervisorIdentity: number[];
    consumerIdentity: number[];
    controllerIdentity: number[];
  };
function validateFixtureReceipt(manifest: FamilyFixtureManifest, value: unknown, outcome?: string) {
  NodeAssert.ok(
    value !== null && typeof value === "object" && !Array.isArray(value),
    "Invalid family receipt",
  );
  const proof = value as FamilyReceipt;
  NodeAssert.equal(proof.schema, "jones-source-renewal-family-receipt/v1");
  for (const key of ["custodyRoot", "attempt", "nonce", "requestSha256"] as const)
    NodeAssert.equal(proof[key], manifest[key]);
  for (const key of ["custodyRootIdentity", "supervisorIdentity", "consumerIdentity"] as const) {
    NodeAssert.deepEqual(proof[key], manifest[key]);
    NodeAssert.ok(manifest[key].every((value) => Number.isSafeInteger(value) && value >= 0));
  }
  for (const key of ["supervisorIdentity", "consumerIdentity"] as const) {
    NodeAssert.equal(manifest[key].length, 2);
    NodeAssert.ok(manifest[key].every((value) => value > 0));
  }
  NodeAssert.equal(manifest.custodyRootIdentity.length, 3);
  NodeAssert.notEqual(manifest.supervisorIdentity[0], manifest.consumerIdentity[0]);
  if (outcome) NodeAssert.equal(proof.outcome, outcome);
  for (const key of ["familyClosed", "reaped", "allIdentitiesExited", "ipcFdReleased"] as const)
    NodeAssert.equal(proof[key], true);
  NodeAssert.equal(proof.ownershipUnresolved, false);
}

// Diagnostic cleanup of a fresh synthetic fixture never settles an artifact ledger.
export async function completeFamilyFixture(options: {
  manifest: FamilyFixtureManifest;
  controllerExit?: Promise<void>;
  familyCompletion: Promise<void>;
  streams: FamilyStreamObservation;
  deadlineMs: number;
  readReceipt: () => unknown;
  expectedOutcome?: string;
  allowInheritedStreamClosure: boolean;
  persist: (result: unknown) => void;
  remove: () => void;
  disposeObserver: () => void;
  primaryFailure?: unknown;
}) {
  let timer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
  let completion: unknown;
  let controllerExited = options.controllerExit === undefined;
  let familyCompleted = false;
  const observations = () => ({
    controllerExited,
    familyCompleted,
    streams: structuredClone(options.streams),
  });
  const deadline = performance.now() + options.deadlineMs;
  try {
    NodeAssert.ok(options.deadlineMs > 0, "Fixture overall deadline exceeded");
    await Promise.race([
      Promise.all([
        (options.controllerExit ?? Promise.resolve()).then(() => {
          controllerExited = true;
        }),
        options.familyCompletion.then(() => {
          familyCompleted = true;
        }),
      ]),
      new Promise<never>((_, reject) => {
        // @effect-diagnostics-next-line globalTimers:off -- Native observer deadline is cleared in finally, independently of fiber cancellation.
        timer = NodeTimers.setTimeout(
          () => reject(new Error("Fixture overall deadline exceeded")),
          Math.max(0, options.deadlineMs),
        );
      }),
    ]);
    NodeAssert.ok(performance.now() < deadline, "Fixture overall deadline exceeded");
    assertNaturalFamilyStreams(options.streams);
    const proof = options.readReceipt();
    if (proof !== undefined)
      validateFixtureReceipt(options.manifest, proof, options.expectedOutcome);
    else NodeAssert.ok(options.allowInheritedStreamClosure, "Bound family receipt missing");
    completion = {
      ...observations(),
      proof,
      inheritedStreamClosure: proof === undefined,
    };
    options.persist({
      manifest: options.manifest,
      completion,
      primaryFailure: options.primaryFailure,
      cleanupFailure: null,
    });
    NodeAssert.ok(performance.now() < deadline, "Fixture overall deadline exceeded");
    options.remove();
  } catch (error) {
    const observedCompletion = observations();
    let cleanupFailure: unknown = error;
    let disposalFailure: string | undefined;
    try {
      options.disposeObserver();
    } catch (disposeError) {
      disposalFailure = describe(disposeError);
      cleanupFailure = new AggregateError(
        [cleanupFailure, disposeError],
        "Fixture observer disposal failed",
      );
    }
    try {
      options.persist({
        manifest: options.manifest,
        completion,
        observedCompletion,
        primaryFailure: options.primaryFailure,
        cleanupFailure: describe(error),
        disposalFailure,
        observerDisposal: structuredClone(options.streams),
        retained: true,
      });
    } catch (persistError) {
      cleanupFailure = new AggregateError(
        [cleanupFailure, persistError],
        "Fixture evidence persistence failed",
      );
    }
    // oxlint-disable-next-line preserve-caught-error -- cleanupFailure retains the caught error and any disposal or persistence errors.
    throw new Error(`Fixture closure unknown; retained ${JSON.stringify(options.manifest)}`, {
      cause: cleanupFailure,
    });
  } finally {
    if (timer !== undefined) NodeTimers.clearTimeout(timer);
  }
}

type Child = Pick<
  NodeChildProcess.ChildProcess,
  | "pid"
  | "connected"
  | "exitCode"
  | "signalCode"
  | "stdout"
  | "stderr"
  | "on"
  | "send"
  | "disconnect"
  | "unref"
> & { control: import("node:stream").Duplex | null };

export function superviseConsumer(
  custody: ReturnType<typeof makeArtifactCustody>,
  lease: Lease,
  options: {
    executable: string;
    args: string[];
    cwd: string;
    env: NodeJS.ProcessEnv;
    fixtureInheritedOutput?: { stdout: 1; stderr: 2 };
  },
  platform = HostProcess.Platform.defaultValue(),
): Child {
  NodeAssert.equal(platform, "linux", "Source renewal family supervision requires Linux");
  const binding = custody.rootBinding();
  const directory = NodePath.join(binding.custodyRoot, "closure");
  NodeFS.mkdirSync(directory, { mode: 0o700, recursive: true });
  const stem = NodePath.join(directory, `${lease.attempt}-${lease.nonce}`);
  const receiptPath = `${stem}.receipt.json`;
  const request = JSON.stringify({
    schema: "jones-source-renewal-family-request/v1",
    ...binding,
    ...lease,
    receiptPath,
    executable: options.executable,
    args: options.args,
    cwd: options.cwd,
    ipcFd: 3,
    controlFd: 4,
    bounds: familyBounds,
  });
  NodeFS.writeFileSync(`${stem}.request.json`, request, { flag: "wx", mode: 0o600 });
  custody.bind(lease, sha256(request), receiptPath);
  const child = NodeChildProcess.spawn(
    "/usr/bin/python3",
    ["-I", "-B", familyHelper, `${stem}.request.json`],
    {
      cwd: options.cwd,
      env: options.env,
      serialization: "json",
      stdio: [
        "ignore",
        options.fixtureInheritedOutput?.stdout ?? "pipe",
        options.fixtureInheritedOutput?.stderr ?? "pipe",
        "ipc",
        "pipe",
      ],
    },
  );
  return Object.assign(child, { control: child.stdio[4] as import("node:stream").Duplex | null });
}
type LifecycleRequest = {
  root: string;
  artifactDigest: string;
  build: WorkstreamsNativeBuild;
  worker: string;
  workerSha256: string;
  runtime: { executable: string; version: string; sha256: string };
};
type LifecycleOptions = {
  requested: LifecycleRequest;
  custody: ReturnType<typeof makeArtifactCustody>;
  fork: (lease: Lease) => Child;
  acquired?: (child: Child, lease: Lease) => void;
  readReceipt?: (attempt: number) => FamilyReceipt;
  timers?: Pick<typeof NodeTimers, "setTimeout" | "clearTimeout">;
  record: (event: string, data: unknown) => void;
};

// Partial diagnostics carry no release authority and are retained outside removable scratch.
export function acquireFamilyFixture(options: {
  fixtureId: string;
  lifecycle: LifecycleOptions;
  birthIdentity: (pid: number) => number[];
  controllerIdentity: number[];
  persist: (value: unknown) => void;
}) {
  const diagnostics = {
    fixtureId: options.fixtureId,
    ...options.lifecycle.custody.rootBinding(),
    attempt: null as number | null,
    nonce: null as string | null,
    requestSha256: null as string | null,
    receiptPath: null as string | null,
    supervisorIdentity: null as number[] | null,
    consumerIdentity: null as number[] | null,
    controllerIdentity: options.controllerIdentity,
  };
  let child: Child | undefined;
  let acquisitionFailure: unknown;
  let manifest: FamilyFixtureManifest | undefined;
  const save = () =>
    options.persist({ trust: "diagnostic-only", acquisition: structuredClone(diagnostics) });
  const lifecycle = acquireConsumerLifecycle({
    ...options.lifecycle,
    acquired: (owned, lease) => {
      child = owned;
      try {
        Object.assign(diagnostics, options.lifecycle.custody.requestBinding(lease.attempt));
        save();
        diagnostics.supervisorIdentity = options.birthIdentity(owned.pid!);
        save();
        options.lifecycle.acquired?.(owned, lease);
      } catch (error) {
        acquisitionFailure = error;
        throw error;
      }
    },
  });
  return {
    lifecycle,
    child: () => child,
    diagnostics: () => structuredClone(diagnostics),
    manifest: () => manifest,
    ready: async <A extends { pid: number }>(validate: (value: A) => void) => {
      if (acquisitionFailure !== undefined) throw acquisitionFailure;
      return lifecycle.ready<A>((value) => {
        diagnostics.consumerIdentity = options.birthIdentity(value.pid);
        save();
        validate(value);
        const candidate = structuredClone(diagnostics) as FamilyFixtureManifest;
        options.persist({ trust: "captured", manifest: candidate });
        manifest = candidate;
      });
    },
  };
}

export function retainFamilyFixtureFailure(options: {
  diagnostics: unknown;
  primaryFailure: unknown;
  retained?: boolean;
  persist: (value: unknown) => void;
  disposeObserver: () => void;
}): never {
  const failures = [options.primaryFailure];
  try {
    options.persist({
      diagnostics: options.diagnostics,
      primaryFailure: describe(options.primaryFailure),
      retained: options.retained ?? true,
    });
  } catch (error) {
    failures.push(error);
  } finally {
    try {
      options.disposeObserver();
    } catch (error) {
      failures.push(error);
    }
  }
  const failureDetails = failures.map((error) =>
    error instanceof AggregateError
      ? { message: describe(error), errors: error.errors.slice(0, 8).map(describe) }
      : describe(error),
  );
  throw new Error(
    `Fixture retained ${JSON.stringify(options.diagnostics)}; failures ${JSON.stringify(failureDetails)}`,
    {
      cause: new AggregateError(failures, "Fixture primary, persistence and disposal failures"),
    },
  );
}

export function makeFamilyFixtureReceiver(options: {
  persist: (value: unknown) => void;
  validate: (candidate: FamilyFixtureManifest) => void;
}) {
  let received: unknown = null;
  const capture = (data: string) => {
    NodeAssert.ok(Buffer.byteLength(data) <= 8192, "Manifest channel bound exceeded");
    received = { trust: "UNTRUSTED", raw: data };
    options.persist(received);
  };
  return {
    diagnostics: () => structuredClone(received),
    capture,
    receive: (data: string) => {
      capture(data);
      const message = JSON.parse(data) as { manifest?: FamilyFixtureManifest; failure?: string };
      if (!message?.manifest || message.failure)
        throw new Error(message?.failure ?? "Missing manifest");
      options.validate(message.manifest);
      options.persist({ trust: "validated", manifest: message.manifest });
      return message.manifest;
    },
  };
}

export async function runFamilyFixtureController(options: {
  fixture: ReturnType<typeof acquireFamilyFixture>;
  deadlineMs: number;
  sendManifest: (manifest: FamilyFixtureManifest) => void;
  acknowledgement: () => Promise<string>;
  persist: (value: unknown) => void;
}) {
  let timer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
  let expired = false;
  const deadline = performance.now() + options.deadlineMs;
  try {
    NodeAssert.ok(options.deadlineMs > 0, "Controller deadline exceeded before acknowledgement");
    await Promise.race([
      (async () => {
        await options.fixture.ready((value) => {
          NodeAssert.ok(Number.isSafeInteger(value.pid) && value.pid > 0, "Worker PID unavailable");
        });
        if (expired) return;
        NodeAssert.ok(
          performance.now() < deadline,
          "Controller deadline exceeded before acknowledgement",
        );
        const manifest = options.fixture.manifest()!;
        for (const id of [
          manifest.supervisorIdentity,
          manifest.consumerIdentity,
          manifest.controllerIdentity,
        ])
          NodeAssert.ok(
            id.length === 2 && id.every((value) => Number.isSafeInteger(value) && value > 0),
            "Birth identity unavailable",
          );
        options.sendManifest(manifest);
        NodeAssert.equal(
          await options.acknowledgement(),
          manifest.nonce + "\n",
          "Ack binding mismatch",
        );
        NodeAssert.ok(
          performance.now() < deadline,
          "Controller deadline exceeded before acknowledgement",
        );
      })(),
      new Promise<never>((_, reject) => {
        // @effect-diagnostics-next-line globalTimers:off -- Native observer deadline is cleared in finally, independently of fiber cancellation.
        timer = NodeTimers.setTimeout(() => {
          expired = true;
          reject(new Error("Controller deadline exceeded before acknowledgement"));
        }, options.deadlineMs);
      }),
    ]);
  } catch (error) {
    expired = true;
    retainFamilyFixtureFailure({
      diagnostics: options.fixture.diagnostics(),
      primaryFailure: error,
      persist: options.persist,
      disposeObserver: () => {
        const child = options.fixture.child();
        if (child) disposeFamilyObserver(child);
      },
    });
  } finally {
    if (timer !== undefined) NodeTimers.clearTimeout(timer);
  }
}

// Synchronous acquisition installs all child handlers before any caller awaits identity.
export function acquireConsumerLifecycle(options: LifecycleOptions) {
  const timers = options.timers ?? NodeTimers;
  const lease = options.custody.acquire();
  const { attempt } = lease;
  const state = {
    attempt,
    nonce: lease.nonce,
    supervisor: {
      pid: null as number | null,
      exit: null as { code: number | null; signal: string | null } | null,
      handlesClosed: false,
    },
    ipc: { disconnected: false },
    worker: { returnedPid: null as number | null },
    control: "none",
    outputBytes: 0,
    streams: {
      stdoutEnded: false,
      stderrEnded: false,
      prematureClose: false,
      parentDisposal: false,
      disposedBeforeEof: false,
    },
    requested: options.requested,
    pid: null as number | null,
    returnedIdentity: undefined as unknown,
    spawned: false,
    errors: [] as string[],
    disconnected: false,
    exit: null as { code: number | null; signal: string | null } | null,
    stdioClosed: false,
    stdout: "",
    stderr: "",
    familyClosure: "unknown" as "unknown" | "closed",
    outcome: "acquired",
  };
  let sealed = false;
  let records = 0;
  const record = () => {
    if (++records <= 64)
      try {
        options.record("source-renewal consumer lifecycle", structuredClone(state));
      } catch (error) {
        fail(error);
      }
  };
  let primary: unknown;
  const cleanupErrors: unknown[] = [];
  let sequence = 0;
  let closing: Promise<void> | undefined;
  let child: Child | undefined;
  let finishStreams!: () => void;
  const streamsClosed = new Promise<void>((resolve) => {
    finishStreams = resolve;
  });
  const pending = new Map<number, { reject: (error: unknown) => void }>();
  const fail = (error: unknown) => {
    primary ??= error;
    options.custody.fence(describe(primary));
    for (const item of pending.values()) item.reject(error);
    pending.clear();
    return primary;
  };
  try {
    record();
    options.custody.assertActive();
    child = options.fork(lease);
    state.streams = observeFamilyStreams(child);
    state.supervisor.pid = child.pid ?? null;
    state.pid = child.pid ?? null;
    for (const stream of [child.stdout, child.stderr, child.control])
      stream?.on("error", (error) => {
        if (!sealed) fail(error);
      });
    child.stdout?.on("data", (data) => {
      if (sealed) return;
      state.outputBytes += Buffer.byteLength(data);
      if (state.outputBytes > 256 * 1024)
        fail(new Error("Artifact consumer output limit exceeded"));
      state.stdout = (state.stdout + String(data)).slice(-4096);
    });
    child.stderr?.on("data", (data) => {
      if (sealed) return;
      state.outputBytes += Buffer.byteLength(data);
      if (state.outputBytes > 256 * 1024)
        fail(new Error("Artifact consumer output limit exceeded"));
      state.stderr = (state.stderr + String(data)).slice(-4096);
    });
    child.on("spawn", () => {
      if (sealed) return;
      state.spawned = true;
      state.pid = child?.pid ?? null;
      record();
    });
    child.on("error", (error) => {
      if (sealed) return;
      state.pid = child?.pid ?? null;
      if (state.errors.length < 8) state.errors.push(describe(error));
      fail(error);
      record();
    });
    child.on("disconnect", () => {
      if (sealed) return;
      state.disconnected = true;
      state.ipc.disconnected = true;
      if (!closing || pending.size) fail(new Error("Artifact consumer disconnected"));
      record();
    });
    child.on("exit", (code, signal) => {
      if (sealed) return;
      state.exit = { code, signal };
      state.supervisor.exit = { code, signal };
      if (!closing || pending.size || code !== 0 || signal !== null)
        fail(new Error(`Artifact consumer exited ${code}/${signal}`));
      record();
    });
    child.on("close", () => {
      if (sealed) return;
      state.stdioClosed = true;
      state.supervisor.handlesClosed = true;
      if (!closing) fail(new Error("Artifact consumer stdio closed before shutdown"));
      record();
      finishStreams();
    });
    child.on("message", (message) => {
      if (sealed) return;
      if (!message || typeof message !== "object" || !("id" in message)) return;
      const response = message as { id: number; value?: unknown; error?: unknown };
      responses.get(response.id)?.(response);
    });
    options.acquired?.(child, lease);
    record();
  } catch (error) {
    if (state.errors.length < 8) state.errors.push(describe(error));
    fail(error);
    record();
  }
  const responses = new Map<number, (message: { value?: unknown; error?: unknown }) => void>();
  const call = <A>(
    command: string,
    args: unknown,
    millis: number,
    signal?: AbortSignal,
  ): Promise<A> =>
    new Promise<A>((resolve, reject) => {
      try {
        if (command !== "close") options.custody.assertActive();
        if (primary !== undefined && command !== "close") throw primary;
        if (closing && command !== "close") throw new Error("Artifact consumer shutting down");
        if (
          !child?.connected ||
          state.disconnected ||
          state.exit ||
          child.exitCode !== null ||
          child.signalCode !== null
        )
          throw new Error("Artifact consumer IPC unavailable");
        if (signal?.aborted) throw signal.reason ?? new Error("Artifact consumer cancelled");
      } catch (error) {
        reject(fail(error));
        return;
      }
      const id = ++sequence;
      const settle = (error: unknown, value?: unknown) => {
        if (!pending.delete(id)) return;
        responses.delete(id);
        timers.clearTimeout(timer);
        signal?.removeEventListener("abort", cancel);
        if (error !== undefined) reject(error);
        else resolve(value as A);
      };
      const cancel = () => {
        fail(signal?.reason ?? new Error("Artifact consumer cancelled"));
      };
      const timer = timers.setTimeout(
        () => {
          fail(new Error(`Artifact consumer ${command} deadline exceeded`));
        },
        command === "identity" ? lifecycleBudgets.startup : lifecycleBudgets.operation,
      );
      pending.set(id, { reject: (error) => settle(error) });
      responses.set(id, (message) => {
        // Command refusals are application results, not transport failures.
        if (message.error !== undefined && (command === "close" || command === "identity"))
          fail(message.error);
        else settle(message.error, message.value);
      });
      signal?.addEventListener("abort", cancel, { once: true });
      try {
        child!.send({ id, command, args, millis }, (error) => {
          if (error) fail(error);
        });
      } catch (error) {
        fail(error);
      }
    });
  const close = (): Promise<void> => {
    closing ??= Promise.resolve().then(async () => {
      if (child?.connected && !state.disconnected && !state.exit) {
        try {
          await call("close", null, 0);
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
      const waitHandles = (millis: number) =>
        new Promise<void>((resolve, reject) => {
          const timer = timers.setTimeout(
            () => reject(new Error("Artifact consumer handles-close deadline exceeded")),
            millis,
          );
          void streamsClosed.then(() => {
            timers.clearTimeout(timer);
            resolve();
          });
        });
      try {
        await waitHandles(lifecycleBudgets.shutdown);
      } catch (error) {
        cleanupErrors.push(error);
        if (child?.control?.writable && !child.control.destroyed) {
          state.control = "cancel-sent";
          try {
            child.control.write("cancel\n");
          } catch (error) {
            cleanupErrors.push(error);
          }
        } else state.control = "unavailable";
        try {
          await waitHandles(6_000);
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
      try {
        if (primary !== undefined) throw primary;
        if (cleanupErrors.length) throw cleanupErrors[0];
        NodeAssert.ok(state.supervisor.handlesClosed, "Supervisor handles still open");
        assertNaturalFamilyStreams(state.streams);
        const proof = (options.readReceipt ?? options.custody.readReceipt)(attempt);
        // Persist closure before granting the original ledger permission to release.
        options.record("source-renewal family closed", proof);
        options.custody.settle(attempt, proof, state);
        state.familyClosure = "closed";
        state.outcome = "settled";
        record();
        if (primary !== undefined) throw primary;
        sealed = true;
        return;
      } catch (error) {
        fail(error);
      }
      const unknown = new Error("Consumer family closure unknown; retained state and artifacts");
      options.custody.fence(describe(primary ?? unknown));
      // A late close/receipt cannot restore authority. Dispose every parent handle without signals.
      try {
        if (child) {
          if (
            state.supervisor.handlesClosed &&
            state.streams.stdoutEnded &&
            state.streams.stderrEnded &&
            !state.streams.prematureClose &&
            !child.connected
          ) {
            // Natural handle completion needs no destructive observer disposal.
            child.unref();
          } else disposeFamilyObserver(child);
        }
      } catch (error) {
        cleanupErrors.push(error);
      }
      state.outcome = state.stdioClosed ? "closed-with-failure" : "retained-unknown";
      record();
      try {
        options.record("source-renewal consumer failure", {
          attempt,
          primary: describe(primary ?? cleanupErrors[0] ?? unknown),
          cleanupErrors: cleanupErrors.map(describe),
          familyClosure: "unknown",
          requested: options.requested,
          pid: state.pid,
          workerCleanupErrors: cleanupErrors.flatMap((error) =>
            error &&
            typeof error === "object" &&
            "cleanupErrors" in error &&
            Array.isArray(error.cleanupErrors)
              ? error.cleanupErrors.slice(0, 8).map(describe)
              : [],
          ),
        });
      } catch (error) {
        cleanupErrors.push(error);
      }
      sealed = true;
      throw primary ?? cleanupErrors[0] ?? unknown;
    });
    return closing;
  };
  const ready = async <A>(validate: (identity: A) => void, signal?: AbortSignal) => {
    try {
      const identity = await call<A>("identity", null, 0, signal);
      state.returnedIdentity = identity;
      state.worker.returnedPid = (identity as { pid?: number } | null)?.pid ?? null;
      record();
      if (primary !== undefined) throw primary;
      validate(identity);
      options.custody.assertActive();
      return identity;
    } catch (error) {
      fail(error);
      try {
        await close();
      } catch {
        /* Primary error and separate cleanup receipts are retained. */
      }
      throw error;
    }
  };
  return { attempt, call, close, ready, failOperation: fail };
}

function consumer(artifact: Artifact, artifacts: SourceRenewalArtifacts) {
  return acquireConsumerLifecycle({
    requested: {
      root: artifact.root,
      artifactDigest: artifact.digest,
      build: artifact.build,
      worker: NodePath.join(artifact.root, workerOverlay),
      workerSha256: artifact.inputs.$worker!,
      runtime: {
        executable: process.execPath,
        version: process.version,
        sha256: artifact.inputs.$node!,
      },
    },
    custody: artifacts,
    record: artifacts.record,
    fork: (lease) =>
      superviseConsumer(artifacts, lease, {
        executable: NodeFS.realpathSync(process.execPath),
        args: [NodePath.join(artifact.root, workerOverlay), artifact.root],
        cwd: artifact.root,
        env: { ...cleanEnvironment(), NODE_ENV: "test" },
      }),
  });
}
function validateIdentity(artifact: Artifact, identity: WorkerIdentity) {
  NodeAssert.equal(identity.root, artifact.root);
  NodeAssert.deepEqual(identity.build, artifact.build);
  NodeAssert.equal(identity.manifestSha256, artifact.inputs[manifestPaths[0]!]);
  NodeAssert.equal(
    identity.providerSha256,
    artifact.inputs["apps/server/src/jones/workstreams/nativeProvider/service.ts"],
  );
  NodeAssert.equal(
    identity.registrationSha256,
    artifact.inputs["apps/server/src/jones/workstreams/registrationContext/service.ts"],
  );
  NodeAssert.ok(identity.effect.startsWith(`${artifact.root}/node_modules/`));
  NodeAssert.ok(identity.contracts.startsWith(`${artifact.root}/packages/contracts/`));
}

export const openArtifactStore = Effect.fn("SourceRenewalArtifacts.openStore")(function* (
  artifacts: SourceRenewalArtifacts,
  stateRoot: string,
  sourceId: string,
  initial: WorkstreamsNativeBuild,
) {
  let current: ReturnType<typeof consumer>;
  let identity: WorkerIdentity;
  let closed = true;
  const stop = async () => {
    if (!closed) {
      await current.close();
      closed = true;
    }
  };
  yield* Effect.addFinalizer(() => Effect.promise(stop));
  const start = (artifact: Artifact) =>
    Effect.promise(async (signal) => {
      signal.throwIfAborted();
      artifacts.assertActive();
      await verify(artifact);
      signal.throwIfAborted();
      artifacts.assertActive();
      current = consumer(artifact, artifacts);
      closed = false;
      identity = await current.ready<WorkerIdentity>(
        (value) => validateIdentity(artifact, value),
        signal,
      );
      try {
        await current.call(
          "open",
          { ...identity, stateRoot, sourceId },
          Date.parse("2026-01-01T00:00:00.000Z"),
          signal,
        );
      } catch (error) {
        current.failOperation(error);
        throw error;
      }
    });
  yield* start(initial.sha === artifacts.old.build.sha ? artifacts.old : artifacts.next);
  const remote = <A, E = never>(command: string, args: unknown): Effect.Effect<A, E> =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((millis) =>
        Effect.tryPromise({
          try: (signal) => current.call<A>(command, args, millis, signal),
          catch: (error) => error as E,
        }),
      ),
    );
  const enrollments: NativeEnrollments["Service"] = {
    inspect: (request) => remote("inspect", request),
    inspectExisting: (request) => remote("inspectExisting", request),
    reserve: (request) => remote("reserve", request),
    getBySessionId: (id) =>
      remote<
        NativeProviderEnrollmentBinding | null,
        import("../enrollment/service.ts").NativeEnrollmentError
      >("getBySessionId", id).pipe(Effect.map(Option.fromNullishOr)),
  };
  const materialize: NativeEnrollmentOperationPorts["materialize"] = (expected) =>
    remote("materialize", expected);
  const credential = (request: NativeEnrollmentRequest): NativeCredentialWriter => ({
    observe: () => remote("observe", request),
    publish: (token, preimage) => remote("publish", { request, token, preimage }),
  });
  const provider: Pick<WorkstreamsNativeProvider, "context"> = {
    context: (binding) => remote("context", binding),
  };
  const providerV2 = {
    context: (binding: Parameters<typeof provider.context>[0]) =>
      remote<Effect.Success<ReturnType<WorkstreamsNativeProvider<"v2">["context"]>>>(
        "contextV2",
        binding,
      ),
  };
  const sessions: Pick<AuthSessions.AuthSessionRepository["Service"], "revoke"> = {
    revoke: (args) => remote("revoke", { ...args, revokedAt: DateTime.formatIso(args.revokedAt) }),
  };
  return {
    enrollments,
    materialize,
    credential,
    provider,
    providerV2,
    sessions,
    rows: remote<{ sessions: readonly unknown[]; enrollments: readonly unknown[] }>("rows", null),
    request: (suffix: string) => remote<NativeEnrollmentRequest>("request", { suffix }),
    binding: (request: NativeEnrollmentRequest) =>
      enrollments.getBySessionId(request.session.session_id).pipe(Effect.map(Option.getOrThrow)),
    changeBuild: (build: WorkstreamsNativeBuild) =>
      Effect.gen(function* () {
        NodeAssert.deepEqual(build, artifacts.next.build);
        const context = yield* Effect.context<never>();
        yield* Effect.promise(() =>
          artifacts.transition(current.attempt, stop, () =>
            Effect.runPromiseWith(context)(start(artifacts.next)),
          ),
        );
      }),
    environment: (control: Awaited<ReturnType<typeof startLoopbackControlPlane>>) =>
      remote<{ origin: string }>("environment", { activation: control.activation }),
    identity: () => identity,
  };
});

export const rejectMismatchedConsumer = Effect.fn("SourceRenewalArtifacts.rejectMismatch")(
  function* (artifacts: SourceRenewalArtifacts, root: string) {
    const child = yield* Effect.acquireRelease(
      Effect.promise(async (signal) => {
        signal.throwIfAborted();
        artifacts.assertActive();
        await verify(artifacts.old);
        signal.throwIfAborted();
        return consumer(artifacts.old, artifacts);
      }),
      (child) => Effect.promise(() => child.close()),
    );
    yield* Effect.promise(async (signal) => {
      const identity = await child.ready<WorkerIdentity>(
        (value) => validateIdentity(artifacts.old, value),
        signal,
      );
      await NodeAssert.rejects(
        child.call(
          "open",
          { ...identity, build: artifacts.next.build, stateRoot: root, sourceId: "guard" },
          0,
        ),
      );
      await NodeAssert.rejects(NodeFSP.stat(NodePath.join(root, "guard.sqlite")), {
        code: "ENOENT",
      });
    });
  },
);
