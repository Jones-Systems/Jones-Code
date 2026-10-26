import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeURL from "node:url";
import * as Identity from "./identity.ts";
import { runChild } from "./patch.ts";

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
}

export async function typecheck(
  root: string,
  cwd: string,
  args: string[],
  signal: AbortSignal,
): Promise<number> {
  if (args.some((arg) => /^(?:--checkerAssignment|--checkers)(?:=|$)/.test(arg)))
    throw new Error("Checker assignment/count are pinned by the repository launcher.");
  const compiler = await selectCompiler(root);
  if (compiler.fallback)
    process.stderr.write(
      `[tsrs] Unsupported host; using pinned ${Identity.FALLBACK.name}@${Identity.FALLBACK.version} with built-in Effect diagnostics.\n`,
    );
  return runChild({
    executable: compiler.executable,
    args: [...args, ...(compiler.fallback ? [] : Identity.CHECKER_OPTIONS)],
    cwd,
    signal,
  });
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
