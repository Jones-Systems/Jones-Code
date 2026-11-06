// @effect-diagnostics nodeBuiltinImport:off -- Test-only owned Git artifacts and native consumer children.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeTimers from "node:timers";
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
  await admission(parent);
  const root = await NodeFSP.mkdtemp(NodePath.join(parent, ".source-renewal-artifacts-"));
  const custody = makeArtifactCustody();
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

// No family-closure attestation exists here. A direct child close must never release custody.
export function makeArtifactCustody() {
  let fenced: string | undefined;
  let attempts = 0;
  let removed = false;
  const assertActive = () => {
    if (fenced || removed) throw new Error(`Artifact lifecycle fenced: ${fenced ?? "removed"}`);
  };
  const fence = (reason = "consumer family closure unknown") => {
    fenced ??= reason;
  };
  return {
    fence,
    assertActive,
    acquire: () => {
      assertActive();
      if (attempts >= 8) {
        fence("fork attempt limit");
        assertActive();
      }
      attempts += 1;
      return attempts;
    },
    transition: async (stop: () => Promise<void>, start: () => Promise<void>) => {
      assertActive();
      await stop();
      assertActive();
      await start();
    },
    assertRemoval: () => {
      if (attempts) fence();
      assertActive();
    },
    remove: async (remove: () => Promise<void>) => {
      if (attempts) fence();
      assertActive();
      // Seal before awaiting I/O so no fork races removal.
      removed = true;
      await remove();
    },
  };
}

type Child = Pick<
  NodeChildProcess.ChildProcess,
  "pid" | "connected" | "exitCode" | "signalCode" | "stdout" | "stderr" | "on" | "send"
>;
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
  fork: () => Child;
  timers?: Pick<typeof NodeTimers, "setTimeout" | "clearTimeout">;
  record: (event: string, data: unknown) => void;
};

// Synchronous acquisition installs all child handlers before any caller awaits identity.
export function acquireConsumerLifecycle(options: LifecycleOptions) {
  const timers = options.timers ?? NodeTimers;
  const attempt = options.custody.acquire();
  const state = {
    attempt,
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
    familyClosure: "unknown" as const,
    outcome: "acquired",
  };
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
    child = options.fork();
    state.pid = child.pid ?? null;
    child.stdout?.on("data", (data) => {
      state.stdout = (state.stdout + String(data)).slice(-4096);
    });
    child.stderr?.on("data", (data) => {
      state.stderr = (state.stderr + String(data)).slice(-4096);
    });
    child.on("spawn", () => {
      state.spawned = true;
      state.pid = child?.pid ?? null;
      record();
    });
    child.on("error", (error) => {
      state.pid = child?.pid ?? null;
      if (state.errors.length < 8) state.errors.push(describe(error));
      fail(error);
      record();
    });
    child.on("disconnect", () => {
      state.disconnected = true;
      if (!closing || pending.size) fail(new Error("Artifact consumer disconnected"));
      record();
    });
    child.on("exit", (code, signal) => {
      state.exit = { code, signal };
      if (!closing || pending.size || code !== 0 || signal !== null)
        fail(new Error(`Artifact consumer exited ${code}/${signal}`));
      record();
    });
    child.on("close", () => {
      state.stdioClosed = true;
      if (!closing) fail(new Error("Artifact consumer stdio closed before shutdown"));
      record();
      finishStreams();
    });
    child.on("message", (message) => {
      if (!message || typeof message !== "object" || !("id" in message)) return;
      const response = message as { id: number; value?: unknown; error?: unknown };
      responses.get(response.id)?.(response);
    });
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
      try {
        await new Promise<void>((resolve, reject) => {
          const timer = timers.setTimeout(
            () => reject(new Error("Artifact consumer stdio-close deadline exceeded")),
            lifecycleBudgets.shutdown,
          );
          void streamsClosed.then(() => {
            timers.clearTimeout(timer);
            resolve();
          });
        });
      } catch (error) {
        cleanupErrors.push(error);
      }
      // Even exit 0 plus stdio close says nothing about descendants.
      const unknown = new Error("Consumer family closure unknown; retained state and artifacts");
      options.custody.fence(describe(primary ?? unknown));
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
      throw primary ?? cleanupErrors[0] ?? unknown;
    });
    return closing;
  };
  const ready = async <A>(validate: (identity: A) => void, signal?: AbortSignal) => {
    try {
      const identity = await call<A>("identity", null, 0, signal);
      state.returnedIdentity = identity;
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
  return { call, close, ready, failOperation: fail };
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
    fork: () =>
      NodeChildProcess.fork(NodePath.join(artifact.root, workerOverlay), [artifact.root], {
        cwd: artifact.root,
        execArgv: [],
        env: { ...cleanEnvironment(), NODE_ENV: "test" },
        stdio: ["ignore", "pipe", "pipe", "ipc"],
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
        yield* Effect.promise(() =>
          artifacts.transition(stop, () => Effect.runPromise(start(artifacts.next))),
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
