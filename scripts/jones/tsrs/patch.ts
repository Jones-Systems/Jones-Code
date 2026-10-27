import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as NodeURL from "node:url";
import * as Identity from "./identity.ts";

export interface ProcessIdentity {
  pid: number;
  start?: string;
}
export interface LockRecord {
  schema: "jones-tsrs-patch/v1";
  wrapper: ProcessIdentity;
  child?: ProcessIdentity;
  phase: "allocated" | "spawning" | "running" | "exited";
}

export async function processIdentity(pid: number): Promise<ProcessIdentity> {
  if (HostProcess.Platform.defaultValue() !== "linux") return { pid };
  const stat = await Identity.files((fs) => fs.readFileString(`/proc/${pid}/stat`));
  const start = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
  if (!start || !/^\d+$/.test(start)) throw new Error(`Invalid process start identity: ${pid}`);
  return { pid, start };
}

async function alive(record: ProcessIdentity): Promise<boolean> {
  try {
    process.kill(record.pid, 0);
  } catch (error) {
    if (Identity.errorCode(error) === "ESRCH") return false;
    throw error;
  }
  if (HostProcess.Platform.defaultValue() !== "linux") return true;
  if (!record.start)
    throw new Error("Linux patch lock lacks process start identity; refusing recovery.");
  try {
    return (await processIdentity(record.pid)).start === record.start;
  } catch (error) {
    if (Identity.errorCode(error) === "ENOENT") return false;
    throw error;
  }
}

async function writeExclusive(filename: string, text: string): Promise<void> {
  await Identity.files((fs) =>
    Effect.gen(function* () {
      const file = yield* fs.open(filename, { flag: "wx", mode: 0o600 });
      yield* file.writeAll(Buffer.from(text));
    }),
  );
}

function parseLock(text: string): LockRecord {
  const value: unknown = JSON.parse(text);
  if (
    typeof value !== "object" ||
    value === null ||
    !("schema" in value) ||
    value.schema !== "jones-tsrs-patch/v1" ||
    !("wrapper" in value) ||
    !validProcess(value.wrapper) ||
    !("phase" in value) ||
    !["allocated", "spawning", "running", "exited"].includes(String(value.phase)) ||
    ("child" in value && !validProcess(value.child))
  )
    throw new Error("Invalid patch lock; preserve it for manual inspection.");
  return value as LockRecord;
}

function validProcess(value: unknown): value is ProcessIdentity {
  return (
    typeof value === "object" &&
    value !== null &&
    "pid" in value &&
    typeof value.pid === "number" &&
    Number.isSafeInteger(value.pid) &&
    value.pid > 0 &&
    (!("start" in value) || typeof value.start === "string")
  );
}

async function privateLockDirectory(root: string): Promise<string> {
  let directory = Identity.path.join(root, "node_modules");
  for (const component of [".cache", "jones", "tsrs"]) {
    directory = Identity.path.join(directory, component);
    try {
      await Identity.files((fs) => fs.makeDirectory(directory, { mode: 0o700 }));
    } catch (error) {
      if (Identity.errorCode(error) !== "EEXIST") throw error;
    }
    if ((await Identity.regularInfo(directory))?.type !== "Directory")
      throw new Error(`Unsafe patch lock directory: ${directory}`);
  }
  return directory;
}

export async function acquireLock(installed: Identity.Installed) {
  await Identity.provePrivate(installed);
  const directory = await privateLockDirectory(installed.root);
  const filename = Identity.path.join(directory, "patch.lock");
  let record: LockRecord = {
    schema: "jones-tsrs-patch/v1",
    wrapper: await processIdentity(process.pid),
    phase: "allocated",
  };
  const text = () => `${JSON.stringify(record)}\n`;
  try {
    await writeExclusive(filename, text());
  } catch (error) {
    if (Identity.errorCode(error) !== "EEXIST") throw error;
    const previous = await Identity.image(filename);
    if (!previous || previous.links !== 1n || BigInt(previous.size) > 8192n)
      throw new Error("Unsafe patch lock; preserve for manual inspection.", { cause: error });
    const oldText = await Identity.files((fs) => fs.readFileString(filename));
    const old = parseLock(oldText);
    if ((await alive(old.wrapper)) || (old.child && (await alive(old.child))))
      throw new Error("TSRS patch in progress; recorded wrapper/child is alive.", { cause: error });
    if (old.phase === "spawning")
      throw new Error(
        "Patch child creation was interrupted before PID recording; exit is unconfirmed. Preserve lock and installation; do not retry automatically.",
        { cause: error },
      );
    // One exclusive claim per stale generation prevents two reclaimers from
    // unlinking a newly acquired lock. An interrupted reclaimer fails closed.
    const claim = Identity.path.join(directory, `patch.reclaim-${previous.inode}`);
    await writeExclusive(claim, text());
    try {
      const current = await Identity.image(filename);
      if (current?.inode !== previous.inode || current.hash !== previous.hash)
        throw new Error("Patch lock changed during stale recovery.", { cause: error });
      await Identity.files((fs) => fs.remove(filename));
      await writeExclusive(filename, text());
    } finally {
      await Identity.files((fs) => fs.remove(claim));
    }
  }
  const owned = await Identity.image(filename);
  if (!owned) throw new Error("Patch lock disappeared.");
  const update = async (change: Partial<LockRecord>) => {
    if ((await Identity.image(filename))?.inode !== owned.inode)
      throw new Error("Patch lock ownership changed.");
    record = { ...record, ...change };
    await Identity.files((fs) => fs.writeFileString(filename, text(), { mode: 0o600 }));
  };
  const release = async () => {
    if ((await Identity.image(filename))?.inode !== owned.inode)
      throw new Error("Patch lock ownership changed; preserving it.");
    await Identity.files((fs) => fs.remove(filename));
  };
  return { update, release };
}

export async function restorePristine(installed: Identity.Installed): Promise<void> {
  const state = await Identity.classify(installed);
  if (state.name === "S4") throw new Error(`Unknown patch state: ${state.reason}`);
  for (const filename of state.quarantines) {
    if ((await Identity.image(filename))?.links !== 1n)
      throw new Error(`Quarantine changed: ${filename}`);
    await Identity.files((fs) => fs.remove(filename));
  }
  if (state.name !== "S0") {
    const backup = `${installed.executable}.original`;
    if ((await Identity.image(backup))?.hash !== Identity.TARGETS[installed.target].original)
      throw new Error("Original backup identity changed; refusing recovery.");
    const binary = await Identity.image(installed.executable);
    if (binary) {
      if (
        binary.links !== 1n ||
        binary.hash !== state.binary?.hash ||
        binary.inode !== state.binary.inode
      )
        throw new Error("Patch target changed; refusing recovery.");
      await Identity.files((fs) => fs.remove(installed.executable));
    }
    if ((await Identity.image(backup))?.hash !== Identity.TARGETS[installed.target].original)
      throw new Error("Original backup identity changed; refusing rename.");
    await Identity.files((fs) => fs.rename(backup, installed.executable));
  }
  const restored = await Identity.classify(installed);
  if (restored.name !== "S0" || restored.quarantines.length)
    throw new Error("Recovery did not restore the authenticated pristine compiler.");
}

export interface ChildOptions {
  executable: string;
  args: string[];
  cwd: string;
  signal: AbortSignal;
  spawned?: (pid: number) => Promise<void>;
}

// Scope closure happens only after exit observation; lock callers can preserve
// unknown spawn/exit states instead of racing a still-running patcher.
export async function runChild(options: ChildOptions): Promise<number> {
  const scope = Scope.makeUnsafe();
  let escalation: Fiber.Fiber<void> | undefined;
  let stop: (() => void) | undefined;
  let observed = false;
  let child: ChildProcessSpawner.ChildProcessHandle | undefined;
  try {
    if (options.signal.aborted) throw new Error("Command cancelled before spawn.");
    child = await Effect.runPromise(
      Effect.flatMap(ChildProcessSpawner.ChildProcessSpawner, (spawner) =>
        spawner.spawn(
          ChildProcess.make(options.executable, options.args, {
            cwd: options.cwd,
            env: HostProcess.Environment.defaultValue(),
            stdin: "inherit",
            stdout: "inherit",
            stderr: "inherit",
            forceKillAfter: 10000,
          }),
        ),
      ).pipe(Effect.provide(NodeServices.layer), Scope.provide(scope)),
    );
    const captured = child;
    const kill = (signal: NodeJS.Signals) => {
      try {
        process.kill(captured.pid, signal);
      } catch (error) {
        if (Identity.errorCode(error) !== "ESRCH") throw error;
      }
    };
    stop = () => {
      kill("SIGTERM");
      escalation ??= Effect.runFork(
        Effect.sleep(10000).pipe(Effect.tap(() => Effect.sync(() => kill("SIGKILL")))),
      );
    };
    options.signal.addEventListener("abort", stop, { once: true });
    if (options.signal.aborted) stop();
    try {
      await options.spawned?.(child.pid);
    } catch (error) {
      stop();
      await Effect.runPromiseExit(child.exitCode);
      observed = true;
      throw error;
    }
    const outcome = await Effect.runPromiseExit(child.exitCode);
    observed = true;
    if (options.signal.aborted) return options.signal.reason === "SIGINT" ? 130 : 143;
    if (Exit.isFailure(outcome))
      throw new Error(`Child ${child.pid} exited by signal or failed: ${String(outcome.cause)}`);
    return outcome.value;
  } finally {
    if (stop) options.signal.removeEventListener("abort", stop);
    if (escalation) await Effect.runPromise(Fiber.interrupt(escalation));
    if (observed || !child) await Effect.runPromise(Scope.close(scope, Exit.void));
  }
}

async function rootCompilerPresent(root: string): Promise<boolean> {
  const filename = Identity.path.join(root, "node_modules", "tsrs");
  try {
    await Identity.files((fs) => fs.readLink(filename));
    return true;
  } catch (error) {
    if (Identity.errorCode(error) === "ENOENT") return false;
    if (Identity.errorCode(error) !== "EINVAL") throw error;
    return Identity.files((fs) => fs.exists(filename));
  }
}

export async function patch(
  root: string,
  mode: "patch" | "verify" | "restore",
  signal: AbortSignal,
): Promise<number> {
  const target = Identity.hostTarget();
  if (!target) {
    process.stderr.write(
      "[tsrs] No official Effect TSRS artifact for this host; typecheck uses the pinned Effect-capable fallback locally.\n",
    );
    return mode === "verify" ? 1 : 0;
  }
  let installed: Identity.Installed;
  try {
    installed = await Identity.resolveInstalled(root, target);
  } catch (error) {
    if (
      mode === "patch" &&
      Identity.errorCode(error) === "MODULE_NOT_FOUND" &&
      !(await rootCompilerPresent(root))
    ) {
      process.stderr.write(
        "[tsrs] Root compiler absent in filtered installation; no patch attempted.\n",
      );
      return 0;
    }
    throw error;
  }
  await Identity.provePrivate(installed);
  if (mode === "verify") {
    await Identity.verified(installed);
    process.stdout.write(`[tsrs] verified ${target} ${Identity.TARGETS[target].patched}\n`);
    return 0;
  }
  const bundled = mode === "patch" ? await Identity.resolveBundled(installed) : undefined;
  const lock = await acquireLock(installed);
  let release = true;
  try {
    const state = await Identity.classify(installed);
    if (state.name === "S4")
      throw new Error(
        `TSRS ${state.name}: ${state.reason}; reinstall the exact private @tsrs platform entry. No file was changed.`,
      );
    if (signal.aborted) return 143;
    if (mode === "restore") {
      await restorePristine(installed);
      return 0;
    }
    if (state.name !== "S1" || state.quarantines.length) {
      if (state.name !== "S0" || state.quarantines.length) await restorePristine(installed);
      if (!bundled) throw new Error("Official patcher was not resolved.");
      await lock.update({ phase: "spawning" });
      release = false;
      const code = await runChild({
        executable: HostProcess.ExecutablePath.defaultValue(),
        args: [bundled.cli, "patch", "--no-typescript", "--tsrs"],
        cwd: installed.root,
        signal,
        spawned: async (pid) => {
          await lock.update({ phase: "running", child: await processIdentity(pid) });
        },
      });
      await lock.update({ phase: "exited" });
      release = true;
      const after = await Identity.classify(installed);
      if (code !== 0 || signal.aborted) {
        process.stderr.write(
          `[tsrs] patch stopped (${code}), state ${after.name}; next invocation may recover after confirmed exit.\n`,
        );
        return code || 1;
      }
    }
    await Identity.verified(installed);
    process.stdout.write(
      `[tsrs] official Effect build verified: ${target} ${Identity.EFFECT_VERSION}\n`,
    );
    return 0;
  } finally {
    if (release) await lock.release();
  }
}

export async function main(argv: string[]): Promise<number> {
  const mode =
    argv.length === 0
      ? "patch"
      : argv.length === 1 && argv[0] === "--verify"
        ? "verify"
        : argv.length === 1 && argv[0] === "--restore"
          ? "restore"
          : undefined;
  if (!mode) throw new Error("Usage: node scripts/jones/tsrs/patch.ts [--verify|--restore]");
  const controller = new AbortController();
  const interrupt = () => controller.abort("SIGINT");
  const terminate = () => controller.abort("SIGTERM");
  process.on("SIGINT", interrupt);
  process.on("SIGTERM", terminate);
  try {
    return await patch(
      Identity.path.resolve(import.meta.dirname, "../../.."),
      mode,
      controller.signal,
    );
  } finally {
    process.removeListener("SIGINT", interrupt);
    process.removeListener("SIGTERM", terminate);
  }
}

if (
  HostProcess.Arguments.defaultValue()[1] &&
  NodeURL.pathToFileURL(Identity.path.resolve(HostProcess.Arguments.defaultValue()[1]!)).href ===
    import.meta.url
) {
  main(HostProcess.Arguments.defaultValue().slice(2)).then(
    (code) => {
      process.exitCode = code;
    },
    (error: unknown) => {
      process.stderr.write(`[tsrs] ${String(error)}\n`);
      process.exitCode = 1;
    },
  );
}
