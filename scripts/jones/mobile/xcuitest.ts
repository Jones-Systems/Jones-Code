// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalTimers:off - Standalone native test adapter; no server Effect runtime.
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";

export const TEST_ID = "JonesE2eUITests/PinWorkflowTests/testPinPersistence";
export const digest = (bytes: string | Buffer) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
export const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : String(error);
export const object = (value: unknown): Record<string, unknown> => {
  NodeAssert.ok(
    value !== null && typeof value === "object" && !Array.isArray(value),
    "Expected object",
  );
  return value as Record<string, unknown>;
};

export type CommandStage = "owned-command" | "simulator-boot-readiness";
export interface CommandDiagnostic {
  stage: CommandStage;
  timeoutMs: number;
  deadlineAt: string;
  timedOutAt?: string;
  closedAt?: string;
  cleanupError?: {
    phase: "settlement" | "term" | "term-wait" | "kill" | "kill-wait";
    syscall: "kill";
    signal: "0" | "SIGTERM" | "SIGKILL";
    errno: "EPERM" | "EACCES" | "EINVAL" | "UNKNOWN";
  };
}

const commandLifecycle = {
  spawn: (executable: string, argv: string[], options: NodeChildProcess.SpawnOptionsWithoutStdio) =>
    NodeChildProcess.spawn(executable, argv, { ...options, stdio: ["pipe", "pipe", "pipe"] }),
  kill: (pid: number, signal: 0 | NodeJS.Signals) => process.kill(pid, signal),
  now: () => Date.now(),
  wait: (ms: number) => NodeTimersPromises.setTimeout(ms),
  setTimer: (callback: () => void, ms: number) => setTimeout(callback, ms),
  clearTimer: (timer: ReturnType<typeof setTimeout>) => clearTimeout(timer),
};

export interface ChildReceipt {
  executable: string;
  argv: string[];
  pid?: number;
  pgid?: number;
  spawnedAt: string;
  code?: number | null;
  signal?: string | null;
  groupGone?: boolean;
  diagnostic?: CommandDiagnostic;
}

// A detached child creates its own process group. The captured handle is the
// authority; neither discovery by executable name nor a host process sweep is used.
export function ownedCommand(
  executable: string,
  argv: readonly string[],
  options: {
    cwd: string;
    env: NodeJS.ProcessEnv;
    signal?: AbortSignal;
    timeoutMs?: number;
    input?: string;
    receipts?: ChildReceipt[];
    stage?: CommandStage;
  },
  lifecycle = commandLifecycle,
) {
  options.signal?.throwIfAborted();
  const startedAt = lifecycle.now();
  const diagnostic: CommandDiagnostic = {
    stage: options.stage ?? "owned-command",
    timeoutMs: options.timeoutMs ?? 120_000,
    deadlineAt: new Date(startedAt + (options.timeoutMs ?? 120_000)).toISOString(),
  };
  const receipt: ChildReceipt = {
    executable,
    argv: [...argv],
    spawnedAt: new Date(startedAt).toISOString(),
    diagnostic,
  };
  options.receipts?.push(receipt);
  const child = lifecycle.spawn(executable, [...argv], {
    cwd: options.cwd,
    env: options.env,
    detached: true,
  });
  if (child.pid !== undefined) {
    receipt.pid = child.pid;
    receipt.pgid = child.pid;
  }
  let stdout = "",
    stderr = "",
    failure: Error | undefined;
  let closed = false;
  let stopPromise: Promise<void> | undefined;
  const completion = new Promise<void>((resolve) => {
    child.once("error", (error) => {
      failure ??= error;
    });
    child.once("close", (code, signal) => {
      diagnostic.closedAt = new Date(lifecycle.now()).toISOString();
      receipt.code = code;
      receipt.signal = signal;
      closed = true;
      resolve();
    });
  });
  let phase: NonNullable<CommandDiagnostic["cleanupError"]>["phase"] = "settlement";
  const recordSyscallFailure = (error: unknown, signal: "0" | "SIGTERM" | "SIGKILL") => {
    const code = (error as NodeJS.ErrnoException).code;
    diagnostic.cleanupError = {
      phase,
      syscall: "kill",
      signal,
      errno: code === "EPERM" || code === "EACCES" || code === "EINVAL" ? code : "UNKNOWN",
    };
  };
  const groupExists = () => {
    if (!receipt.pid) {
      receipt.groupGone = true;
      return false;
    }
    if (receipt.groupGone) return false;
    try {
      lifecycle.kill(-receipt.pid, 0);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
        recordSyscallFailure(error, "0");
        throw error;
      }
      receipt.groupGone = true;
      return false;
    }
  };
  const signalGroup = (signal: "SIGTERM" | "SIGKILL") => {
    if (!groupExists()) return;
    try {
      lifecycle.kill(-receipt.pid!, signal);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
        recordSyscallFailure(error, signal);
        throw error;
      }
      receipt.groupGone = true;
    }
  };
  let notifyStop: () => void = () => {};
  const stopStarted = new Promise<void>((resolve) => {
    notifyStop = resolve;
  });
  const stop = () => {
    if (!stopPromise) {
      stopPromise = (async () => {
        phase = "term";
        signalGroup("SIGTERM");
        phase = "term-wait";
        const deadline = lifecycle.now() + 2_000;
        while (groupExists() && lifecycle.now() < deadline) await lifecycle.wait(25);
        if (groupExists()) {
          phase = "kill";
          signalGroup("SIGKILL");
        }
        phase = "kill-wait";
        const killedDeadline = lifecycle.now() + 5_000;
        while (lifecycle.now() < killedDeadline) {
          if (!groupExists() && closed) break;
          await lifecycle.wait(25);
        }
        NodeAssert.ok(!groupExists() && closed, `Owned child/group ${receipt.pid} exit unresolved`);
      })();
      notifyStop();
    }
    return stopPromise;
  };
  const abort = () => {
    failure ??= new Error("Owned command cancelled");
    // The result path awaits this promise and preserves cleanup errors.
    void stop().catch(() => {});
  };
  options.signal?.addEventListener("abort", abort, { once: true });
  if (options.signal?.aborted) abort();
  const timer = lifecycle.setTimer(() => {
    diagnostic.timedOutAt = new Date(lifecycle.now()).toISOString();
    failure ??= new Error("Owned command timed out; operation effect requires readback");
    void stop().catch(() => {});
  }, options.timeoutMs ?? 120_000);
  const collect = (chunk: Buffer, stream: "stdout" | "stderr") => {
    if (stdout.length + stderr.length + chunk.length > 8 * 1024 * 1024) {
      failure ??= new Error("Owned command output limit exceeded");
      void stop().catch(() => {});
    } else if (stream === "stdout") stdout += chunk.toString();
    else stderr += chunk.toString();
  };
  child.stdout.on("data", (chunk: Buffer) => collect(chunk, "stdout"));
  child.stderr.on("data", (chunk: Buffer) => collect(chunk, "stderr"));
  child.stdin.on("error", () => {});
  child.stdin.end(options.input);
  const result = (async () => {
    let cleanupError: unknown;
    try {
      await Promise.race([completion, stopStarted]);
      if (stopPromise) await stopPromise;
      // Even a successful leader exit must not leave its owned descendants alive.
      if (groupExists()) await stop();
      NodeAssert.ok(closed, "Owned child close unavailable");
    } catch (error) {
      cleanupError = error;
    } finally {
      lifecycle.clearTimer(timer);
      options.signal?.removeEventListener("abort", abort);
    }
    if (failure || cleanupError) {
      const primary =
        failure ??
        (receipt.code !== null && receipt.code !== undefined && receipt.code !== 0
          ? new Error(`Owned command exited (${receipt.code})`)
          : undefined);
      throw new AggregateError(
        [primary, cleanupError].filter(Boolean),
        primary?.message ?? errorMessage(cleanupError),
      );
    }
    return { stdout, stderr, code: receipt.code ?? 1, receipt };
  })();
  return { child, receipt, result, stop };
}

export function nativeEnvironment(
  root: string,
  developerDir: string,
  path: string,
): NodeJS.ProcessEnv {
  return {
    PATH: path,
    HOME: NodePath.join(root, "home"),
    CFFIXED_USER_HOME: NodePath.join(root, "home"),
    TMPDIR: NodePath.join(root, "tmp"),
    XDG_CACHE_HOME: NodePath.join(root, "cache"),
    DEVELOPER_DIR: developerDir,
    CI: "1",
    NO_COLOR: "1",
    T3CODE_DISABLE_TELEMETRY: "1",
    T3CODE_MOBILE_UPDATES_ENABLED: "0",
  };
}

export function testArgv(xctestrun: string, udid: string, root: string): string[] {
  NodeAssert.match(udid, /^[0-9a-f-]{36}$/i);
  NodeAssert.ok(NodePath.isAbsolute(xctestrun) && NodePath.isAbsolute(root));
  return [
    "test-without-building",
    "-xctestrun",
    xctestrun,
    "-destination",
    `platform=iOS Simulator,id=${udid}`,
    "-destination-timeout",
    "60",
    "-derivedDataPath",
    NodePath.join(root, "DerivedData"),
    "-resultBundlePath",
    NodePath.join(root, "result.xcresult"),
    "-parallel-testing-enabled",
    "NO",
    "-maximum-concurrent-test-simulator-destinations",
    "1",
    "-test-iterations",
    "1",
    "-collect-test-diagnostics",
    "never",
    `-only-testing:${TEST_ID}`,
  ];
}

export async function fileDigest(path: string) {
  return digest(await NodeFSP.readFile(path));
}
export async function treeDigest(root: string): Promise<string> {
  const entries: string[] = [];
  async function visit(directory: string) {
    for (const name of (await NodeFSP.readdir(directory)).sort()) {
      const path = NodePath.join(directory, name),
        stat = await NodeFSP.lstat(path);
      NodeAssert.ok(!stat.isSymbolicLink(), "Build artifacts must not contain symlinks");
      if (stat.isDirectory()) await visit(path);
      else if (stat.isFile())
        entries.push(`${NodePath.relative(root, path)}\0${await fileDigest(path)}`);
      else throw new Error("Unexpected build artifact type");
    }
  }
  await visit(root);
  NodeAssert.ok(entries.length, "Empty build artifact");
  return digest(entries.join("\n"));
}

// Rebase only known build paths; preserve Xcode's SDK/platform placeholders.
// Reject extra targets, inherited runner arguments and test-plan repetitions.
export function configureTestRun(
  value: unknown,
  testRoot: string,
  environment: Record<string, string>,
) {
  const run = structuredClone(object(value));
  let target: Record<string, unknown>;
  if (Array.isArray(run.TestConfigurations)) {
    NodeAssert.equal(run.TestConfigurations.length, 1, "Expected one test configuration");
    const config = object(run.TestConfigurations[0]);
    NodeAssert.ok(
      Array.isArray(config.TestTargets) && config.TestTargets.length === 1,
      "Expected one test target",
    );
    target = object(config.TestTargets[0]);
    NodeAssert.equal(target.BlueprintName, "JonesE2eUITests");
    config.IsEnabled = true;
  } else {
    NodeAssert.deepEqual(
      Object.keys(run).filter((key) => key !== "__xctestrun_metadata__"),
      ["JonesE2eUITests"],
    );
    target = object(run.JonesE2eUITests);
  }
  NodeAssert.equal(target.IsUITestBundle, true);
  for (const key of ["TestHostPath", "UITargetAppPath"]) {
    NodeAssert.ok(
      typeof target[key] === "string" && target[key].startsWith("__TESTROOT__/"),
      `Unbound ${key}`,
    );
    const resolved = NodePath.resolve(testRoot, target[key].slice("__TESTROOT__/".length));
    NodeAssert.ok(resolved.startsWith(`${testRoot}${NodePath.sep}`), `Escaping ${key}`);
  }
  const testHost = String(target.TestHostPath).replaceAll("__TESTROOT__", testRoot);
  NodeAssert.ok(typeof target.TestBundlePath === "string", "Unbound TestBundlePath");
  NodeAssert.ok(
    target.TestBundlePath.startsWith("__TESTROOT__/") ||
      target.TestBundlePath.startsWith("__TESTHOST__/"),
    "Unbound TestBundlePath",
  );
  const testBundle = NodePath.resolve(
    target.TestBundlePath.replaceAll("__TESTHOST__", testHost).replaceAll("__TESTROOT__", testRoot),
  );
  NodeAssert.ok(testBundle.startsWith(`${testHost}${NodePath.sep}`), "Escaping TestBundlePath");
  target.TestBundlePath = testBundle;
  NodeAssert.notEqual(
    target.UseDestinationArtifacts,
    true,
    "preinstalled artifacts are not owned by this run",
  );
  const rebase = (item: unknown): unknown => {
    if (typeof item === "string") return item.replaceAll("__TESTROOT__", testRoot);
    if (Array.isArray(item)) return item.map(rebase);
    if (item && typeof item === "object")
      return Object.fromEntries(Object.entries(item).map(([key, val]) => [key, rebase(val)]));
    return item;
  };
  target.EnvironmentVariables = { ...environment };
  target.CommandLineArguments = [];
  target.UITargetAppEnvironmentVariables = {};
  target.UITargetAppCommandLineArguments = [];
  target.OnlyTestIdentifiers = ["PinWorkflowTests/testPinPersistence"];
  delete target.SkipTestIdentifiers;
  target.ParallelizationEnabled = false;
  delete target.TestExecutionOrdering;
  target.SystemAttachmentLifetime = "keepNever";
  target.UserAttachmentLifetime = "keepNever";
  return rebase(run) as Record<string, unknown>;
}
