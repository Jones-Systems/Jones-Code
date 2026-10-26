import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeURL from "node:url";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Identity from "./identity.ts";
import { classifyArguments, runWithBuildInfo } from "./build-info.ts";
import { acquireLock, runChild } from "./patch.ts";

const FALLBACK_LIMITATION =
  "The pinned fallback misses configured deterministicKeys ERROR TS377049; local or cached success requires authoritative supported-platform CI.";

export async function selectCompiler(root: string, host: Identity.Host = Identity.currentHost()) {
  const target = Identity.hostTarget(host);
  if (target)
    return {
      ...(await Identity.verified(await Identity.resolveInstalled(root, target))),
      fallback: false,
    };
  return { ...(await Identity.resolveFallback(root, host)), fallback: true };
}

export async function admit(
  root: string,
  ci: string | undefined,
  host: Identity.Host = Identity.currentHost(),
): Promise<void> {
  if (ci && ci !== "0" && ci !== "false" && !Identity.hostTarget(host))
    throw new Error(
      "CI requires a supported, verified official Effect TSRS build; local fallback cannot qualify CI.",
    );
  await selectCompiler(root, host);
  const target = Identity.hostTarget(host);
  process.stdout.write(
    target
      ? `[tsrs] admission: official Effect ${Identity.EFFECT_VERSION}, ${target}, ${Identity.TARGETS[target].patched}\n`
      : `[tsrs] admission: local non-authoritative fallback ${Identity.FALLBACK.name}@${Identity.FALLBACK.version}. ${FALLBACK_LIMITATION}\n`,
  );
}

export async function launchVerified(
  installed: Identity.Installed,
  signal: AbortSignal,
  launch: (compiler: Identity.Installed, spawned: () => Promise<void>) => Promise<number>,
): Promise<number> {
  const deadline = (await Effect.runPromise(Clock.currentTimeMillis)) + 10000;
  let lock: Awaited<ReturnType<typeof acquireLock>>;
  for (;;) {
    if (signal.aborted) throw new Error("Command cancelled before verification.");
    try {
      lock = await acquireLock(installed);
      break;
    } catch (error) {
      if ((await Effect.runPromise(Clock.currentTimeMillis)) >= deadline) throw error;
      // Parallel launchers may observe an in-progress lock write or reclamation.
      // Retrying never admits a compiler without acquiring and verifying again.
      await Effect.runPromise(Effect.sleep(25));
    }
  }
  let held = true;
  const release = async () => {
    if (!held) return;
    held = false;
    await lock.release();
  };
  try {
    if (signal.aborted) throw new Error("Command cancelled before verification.");
    const compiler = await Identity.verified(installed);
    // Native spawn completion pins the executable inode. Release before waiting
    // for compiler exit so independent typechecks can run in parallel.
    return await launch(compiler, release);
  } finally {
    await release();
  }
}

export async function typecheck(
  root: string,
  cwd: string,
  args: string[],
  signal: AbortSignal,
): Promise<number> {
  classifyArguments(args);
  const target = Identity.hostTarget();
  if (target) {
    const installed = await Identity.resolveInstalled(root, target);
    return runWithBuildInfo({
      root: installed.root,
      cwd,
      args,
      compiler: installed,
      signal,
      run: (effectiveArgs, seen) =>
        launchVerified(installed, signal, (compiler, spawned) =>
          runChild({
            executable: compiler.executable,
            args: [...effectiveArgs, ...Identity.CHECKER_OPTIONS],
            cwd,
            signal,
            spawned: async () => {
              seen();
              await spawned();
            },
          }),
        ),
    });
  }
  return runFallback(root, cwd, args, signal);
}

export async function runFallback(
  root: string,
  cwd: string,
  args: string[],
  signal: AbortSignal,
): Promise<number> {
  classifyArguments(args);
  const compiler = await Identity.resolveFallback(root);
  process.stderr.write(
    `[tsrs] Unsupported host; using pinned ${Identity.FALLBACK.name}@${Identity.FALLBACK.version} with built-in Effect diagnostics (local, non-authoritative). ${FALLBACK_LIMITATION}\n`,
  );
  return runChild({ executable: compiler.executable, args, cwd, signal });
}

export async function main(args: string[]): Promise<number> {
  const root = Identity.path.resolve(import.meta.dirname, "../../..");
  if (args.includes("--admit")) {
    if (args.length !== 1) throw new Error("--admit cannot be combined with compiler options.");
    await admit(root, HostProcess.Environment.defaultValue().CI);
    return 0;
  }
  const controller = new AbortController();
  const interrupt = () => controller.abort("SIGINT");
  const terminate = () => controller.abort("SIGTERM");
  process.on("SIGINT", interrupt);
  process.on("SIGTERM", terminate);
  try {
    return await typecheck(
      root,
      HostProcess.WorkingDirectory.defaultValue(),
      args,
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
