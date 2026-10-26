// @effect-diagnostics nodeBuiltinImport:off - runs before `vp i`, so only Node built-ins exist.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

const GIB = 1024n ** 3n;
type Options = { install: boolean; warm: boolean; reserve: bigint; peak: bigint };
type Runner = (
  command: string,
  args: string[],
  options: NodeChildProcess.SpawnOptions,
) => number | null | Promise<number | null>;
type Dependencies = {
  available: (destination: string) => bigint;
  run: Runner;
  report: (message: string) => void;
  afterInstall?: () => void;
};

export function parseOptions(args: string[]): Options {
  const options: Options = { install: false, warm: false, reserve: 30n * GIB, peak: 10n * GIB };
  const seen = new Set<string>();
  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    if (seen.has(arg)) throw new Error(`Duplicate option: ${arg}`);
    seen.add(arg);
    if (arg === "--install") options.install = true;
    else if (arg === "--warm") options.warm = true;
    else if (arg === "--reserve-gib" || arg === "--peak-gib") {
      const value = args[++index];
      const minimum = arg === "--reserve-gib" ? 30n : 10n;
      if (!value || !/^[1-9][0-9]*$/.test(value) || BigInt(value) < minimum) {
        throw new Error(`${arg} requires an integer of at least ${minimum} GiB`);
      }
      if (arg === "--reserve-gib") options.reserve = BigInt(value) * GIB;
      else options.peak = BigInt(value) * GIB;
    } else throw new Error(`Unknown option: ${arg}`);
  }
  if (options.warm && !options.install)
    throw new Error("--warm requires --install in the same invocation");
  return options;
}

export function availableBytes(destination: string): bigint {
  let ancestor = NodePath.resolve(destination);
  for (;;) {
    try {
      // stat follows symlinks so the measured volume is the actual destination.
      NodeFS.statSync(ancestor);
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      const parent = NodePath.dirname(ancestor);
      if (parent === ancestor) throw error;
      ancestor = parent;
    }
  }
  const stats = NodeFS.statfsSync(ancestor, { bigint: true });
  if (stats.bavail < 0n || stats.bsize <= 0n) throw new Error("Invalid filesystem capacity");
  return stats.bavail * stats.bsize;
}

function linkEnvironment(projectRoot: string, worktree: string, report: Dependencies["report"]) {
  // Env files live as real files in the main checkout; worktrees only get
  // symlinks to them. Only a symlink is ever replaced, so a real env file is
  // never deleted, including when this runs in the main checkout itself.
  for (const file of [".env", NodePath.join("infra", "relay", ".env")]) {
    const source = NodePath.join(projectRoot, file);
    const sourceStat = NodeFS.lstatSync(source, { throwIfNoEntry: false });
    if (!sourceStat) continue;
    if (!sourceStat.isFile()) {
      report(`Skipping ${file}: ${source} is not a regular file.`);
      continue;
    }
    const target = NodePath.join(worktree, file);
    const existing = NodeFS.lstatSync(target, { throwIfNoEntry: false });
    if (existing && !existing.isSymbolicLink()) continue;
    if (existing) NodeFS.rmSync(target);
    NodeFS.symlinkSync(source, target);
  }
}

export async function setupWorktree(
  args: string[],
  projectRoot: string | undefined,
  worktree: string,
  dependencies: Dependencies,
): Promise<number> {
  const options = parseOptions(args);
  if (!projectRoot) {
    throw new Error("T3CODE_PROJECT_ROOT is not set. Run this through the t3.json setup action.");
  }
  function admit(producer: string) {
    let available: bigint;
    try {
      available = dependencies.available(worktree);
      if (typeof available !== "bigint" || available < 0n) throw new Error("Invalid capacity");
    } catch {
      throw new Error(
        `${producer} refused: capacity unknown for ${worktree}; reserve=${options.reserve} peak=${options.peak} bytes. No cleanup performed.`,
      );
    }
    if (available < options.reserve + options.peak) {
      throw new Error(
        `${producer} refused: ${worktree} available=${available} reserve=${options.reserve} peak=${options.peak} bytes. No cleanup performed.`,
      );
    }
  }
  if (options.install) {
    dependencies.report(
      "Storage admission covers the worktree filesystem; separately configured shared package stores are not measured.",
    );
    admit("Install");
    // Resolve vp through shell PATH, including Windows command shims.
    // Pass pnpm's confirmation flag through vp; closed stdin makes other prompts fail instead of hang.
    // Require the locked dependency graph: a stale lockfile must fail, never be rewritten.
    const status = await dependencies.run("vp i --frozen-lockfile -- --yes", [], {
      cwd: worktree,
      shell: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    if (status !== 0) return status ?? 1;
    dependencies.afterInstall?.();
  }
  linkEnvironment(projectRoot, worktree, dependencies.report);
  if (options.warm) {
    admit("Cache warm");
    return (
      (await dependencies.run(
        process.execPath,
        [NodePath.join(worktree, "apps", "web", "scripts", "warm-dep-cache.ts")],
        {
          cwd: worktree,
          stdio: ["ignore", "pipe", "pipe"],
        },
      )) ?? 1
    );
  }
  return 0;
}

function reportStoreFilesystem(worktree: string, report: Dependencies["report"]) {
  // pnpm cannot clone or hardlink across filesystems, so a store on another
  // device silently turns every worktree install into a full copy.
  const storeDir: unknown = JSON.parse(
    NodeFS.readFileSync(NodePath.join(worktree, "node_modules", ".modules.yaml"), "utf8"),
  ).storeDir;
  if (
    typeof storeDir === "string" &&
    NodeFS.statSync(storeDir).dev !== NodeFS.statSync(NodePath.join(worktree, "node_modules")).dev
  ) {
    report(
      [
        "",
        "WARNING: the pnpm store is on a different filesystem than this worktree.",
        `  store:    ${storeDir}`,
        `  worktree: ${worktree}`,
        "pnpm cannot hardlink or clone across filesystems, so it copied every package into node_modules.",
        "Fix: put the store on the worktree's filesystem, for example:",
        `  pnpm config set store-dir ${JSON.stringify(NodePath.join(NodePath.dirname(worktree), ".pnpm-store"))} --global`,
        "",
      ].join("\n"),
    );
  }
}

type LifecycleDependencies = {
  available: Dependencies["available"];
  spawn: typeof NodeChildProcess.spawn;
  report: Dependencies["report"];
  stdout: Pick<NodeJS.WriteStream, "write">;
  stderr: Pick<NodeJS.WriteStream, "write">;
};

export async function runSetupWorktree(
  args: string[],
  projectRoot: string | undefined,
  worktree: string,
  dependencies: LifecycleDependencies = {
    available: availableBytes,
    spawn: NodeChildProcess.spawn,
    report: (message) => process.stderr.write(`${message}\n`),
    stdout: process.stdout,
    stderr: process.stderr,
  },
): Promise<number> {
  // T3 runs setup in the background, so agents can start before it ends.
  // `setup-running` holds this PID until setup ends with `setup-done` or
  // `setup-failed`, so agents wait instead of racing it with their own `vp i`.
  const markerDir = NodePath.join(worktree, ".t3");
  const runningMarker = NodePath.join(markerDir, "setup-running");
  const doneMarker = NodePath.join(markerDir, "setup-done");
  const failedMarker = NodePath.join(markerDir, "setup-failed");
  NodeFS.rmSync(doneMarker, { force: true });
  NodeFS.rmSync(failedMarker, { force: true });
  NodeFS.mkdirSync(markerDir, { recursive: true });
  NodeFS.writeFileSync(runningMarker, `${process.pid}\n`);
  let failureDetail = "";
  const recordException = (error: Error) => {
    failureDetail = error.stack ?? String(error);
  };
  const finish = (code: number) => {
    if (code === 0) NodeFS.writeFileSync(doneMarker, "");
    else NodeFS.writeFileSync(failedMarker, `Setup exited with code ${code}.\n${failureDetail}\n`);
    NodeFS.rmSync(runningMarker, { force: true });
  };
  process.on("uncaughtExceptionMonitor", recordException);
  process.on("exit", finish);
  // A cancelled setup must still leave a marker, or agents would wait forever.
  // Forward cancellation to the active producer and wait for output closure.
  // A child left by the shell can hold output open after the shell exits, so
  // no completion marker appears while that child can still write outputs.
  let cancelSignal: NodeJS.Signals | undefined;
  let activeProducer: NodeChildProcess.ChildProcess | undefined;
  const signals = ["SIGINT", "SIGTERM", "SIGHUP"] as const;
  const cancel = (signal: NodeJS.Signals) => {
    cancelSignal = signal;
    failureDetail = `Cancelled by ${signal}.`;
    if (activeProducer) activeProducer.kill(signal);
    else process.exit(1);
  };
  const handlers = signals.map((signal) => () => cancel(signal));
  signals.forEach((signal, index) => process.on(signal, handlers[index]!));
  let status = 1;
  try {
    status = await setupWorktree(args, projectRoot, worktree, {
      available: dependencies.available,
      report: dependencies.report,
      afterInstall: () => reportStoreFilesystem(worktree, dependencies.report),
      run: async (command, commandArgs, options) => {
        const child = dependencies.spawn(command, commandArgs, options);
        activeProducer = child;
        let output = "";
        let spawnError: Error | undefined;
        child.on("error", (error) => {
          spawnError = error;
        });
        // Piped output keeps a bounded failure tail and makes `close`, rather
        // than the shell's `exit`, the completion boundary.
        for (const [source, target] of [
          [child.stdout, dependencies.stdout],
          [child.stderr, dependencies.stderr],
        ] as const) {
          source?.setEncoding("utf8");
          source?.on("data", (chunk: string) => {
            target.write(chunk);
            output = (output + chunk).slice(-8_000);
          });
        }
        const code = await new Promise<number | null>((resolve) => child.once("close", resolve));
        activeProducer = undefined;
        if (code !== 0 || cancelSignal || spawnError) {
          failureDetail = cancelSignal
            ? `Cancelled by ${cancelSignal}.`
            : spawnError
              ? (spawnError.stack ?? String(spawnError))
              : output.trimEnd().split("\n").slice(-30).join("\n");
          return code || 1;
        }
        return 0;
      },
    });
    return status;
  } catch (error) {
    failureDetail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    throw error;
  } finally {
    finish(status);
    process.removeListener("uncaughtExceptionMonitor", recordException);
    process.removeListener("exit", finish);
    signals.forEach((signal, index) => process.removeListener(signal, handlers[index]!));
  }
}
