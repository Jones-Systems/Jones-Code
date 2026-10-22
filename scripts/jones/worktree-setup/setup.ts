// @effect-diagnostics nodeBuiltinImport:off - runs before `vp i`, so only Node built-ins exist.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

const GIB = 1024n ** 3n;
type Options = { install: boolean; warm: boolean; reserve: bigint; peak: bigint };
type Runner = (command: string, args: string[], options: NodeChildProcess.SpawnSyncOptions) => number | null;
type Dependencies = {
  available: (destination: string) => bigint;
  run: Runner;
  report: (message: string) => void;
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
  if (options.warm && !options.install) throw new Error("--warm requires --install in the same invocation");
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

const defaultDependencies: Dependencies = {
  available: availableBytes,
  run: (command, args, options) => NodeChildProcess.spawnSync(command, args, options).status,
  report: (message) => process.stderr.write(`${message}\n`),
};

export function setupWorktree(
  args: string[],
  projectRoot: string | undefined,
  worktree: string,
  dependencies: Dependencies = defaultDependencies,
): number {
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
      throw new Error(`${producer} refused: capacity unknown for ${worktree}; reserve=${options.reserve} peak=${options.peak} bytes. No cleanup performed.`);
    }
    if (available < options.reserve + options.peak) {
      throw new Error(`${producer} refused: ${worktree} available=${available} reserve=${options.reserve} peak=${options.peak} bytes. No cleanup performed.`);
    }
  }
  if (options.install) {
    dependencies.report("Storage admission covers the worktree filesystem; separately configured shared package stores are not measured.");
    admit("Install");
    // `shell` resolves `vp` through PATH, including Windows command shims.
    // Setup has no one answering install prompts. Pass pnpm's confirmation flag
    // through vp and leave stdin closed so other prompts fail instead of hanging.
    // Install exactly the locked dependencies so setup never rewrites the lockfile.
    // A stale lockfile must fail setup rather than silently resolving a new one.
    const status = dependencies.run("vp i --frozen-lockfile -- --yes", [], {
      cwd: worktree, shell: true, stdio: ["ignore", "inherit", "inherit"],
    });
    if (status !== 0) return status ?? 1;
  }
  linkEnvironment(projectRoot, worktree, dependencies.report);
  if (options.warm) {
    admit("Cache warm");
    return dependencies.run(process.execPath, [NodePath.join(worktree, "apps", "web", "scripts", "warm-dep-cache.ts")], {
      cwd: worktree, stdio: "inherit",
    }) ?? 1;
  }
  return 0;
}
