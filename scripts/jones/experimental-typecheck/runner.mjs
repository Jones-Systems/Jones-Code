import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export const VERSION = "0.18.0-ts7.1.0-dev.20260929";
export const WARNING =
  "EXPERIMENTAL: tsrs omits configured Effect diagnostics. Run the normal authoritative typecheck before accepting changes; this command does not establish workspace parity.";

export function parseArguments(args) {
  const projects = [];
  let checkers = 2;
  let seenCheckers = false;
  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    if (argument === "--checkers") {
      const value = args[++index];
      if (seenCheckers || !/^[1-9]\d*$/.test(value ?? "") || !Number.isSafeInteger(Number(value))) {
        throw new Error("--checkers requires one positive safe integer.");
      }
      checkers = Number(value);
      seenCheckers = true;
    } else if (argument.startsWith("-") || !argument.trim()) {
      throw new Error(
        `Unsupported argument: ${JSON.stringify(argument)}. Compiler flags cannot be forwarded.`,
      );
    } else {
      projects.push(argument);
    }
  }
  if (projects.length === 0)
    throw new Error("Select at least one workspace package name or repository-relative NodePath.");
  return { projects, checkers };
}

export function selectProjects(root, inventory, selectors) {
  const selected = [];
  for (const selector of selectors) {
    const matches = inventory.filter(
      (entry) =>
        entry.name === selector ||
        NodePath.relative(root, entry.path) === selector.replace(/^\.\//, "").replace(/\/$/, ""),
    );
    if (matches.length !== 1 || matches[0].path === root) {
      throw new Error(`Unknown or ambiguous workspace: ${selector}`);
    }
    const project = matches[0];
    const relative = NodePath.relative(root, project.path);
    if (relative.startsWith("..") || NodePath.isAbsolute(relative))
      throw new Error(`Workspace is outside the repository: ${selector}`);
    const manifest = JSON.parse(
      NodeFS.readFileSync(NodePath.join(project.path, "package.json"), "utf8"),
    );
    if (
      typeof manifest.scripts?.typecheck !== "string" ||
      !/^\s*tsc-rs\s+--noEmit\s*$/.test(manifest.scripts.typecheck)
    ) {
      throw new Error(
        `Unsupported typecheck script for ${selector}: ${JSON.stringify(manifest.scripts?.typecheck)}. Only tsc-rs --noEmit is supported; Astro is outside coverage.`,
      );
    }
    NodeFS.accessSync(NodePath.join(project.path, "tsconfig.json"), NodeFS.constants.R_OK);
    if (!selected.some((entry) => entry.path === project.path)) selected.push(project);
  }
  return selected;
}

// oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone CLI resolves the installed native compiler without an Effect runtime.
export async function resolveCompiler(root, platform = process.platform, arch = process.arch) {
  if (process.env.TSRS_BINARY)
    throw new Error("TSRS_BINARY overrides are unsupported; use the pinned local package.");
  const require = NodeModule.createRequire(NodePath.join(root, "package.json"));
  let manifestPath;
  try {
    NodeFS.accessSync(NodePath.join(root, "node_modules/tsrs/package.json"), NodeFS.constants.R_OK);
    manifestPath = require.resolve("tsrs/package.json");
  } catch {
    throw new Error(
      `Missing local tsrs ${VERSION}; install the repository dependencies first. No download or fallback is performed.`,
    );
  }
  const manifest = JSON.parse(NodeFS.readFileSync(manifestPath, "utf8"));
  if (manifest.version !== VERSION)
    throw new Error(`Expected tsrs ${VERSION}, found ${manifest.version}.`);
  const { getPlatformPackageName } = await import(
    NodeURL.pathToFileURL(NodePath.join(NodePath.dirname(manifestPath), "lib/platform.js")).href
  );
  const packageName = getPlatformPackageName(platform, arch);
  if (manifest.optionalDependencies?.[packageName] !== VERSION)
    throw new Error(`Unsupported tsrs platform: ${platform}/${arch}.`);
  let nativeManifestPath;
  try {
    nativeManifestPath = NodeModule.createRequire(manifestPath).resolve(
      `${packageName}/package.json`,
    );
  } catch {
    throw new Error(
      `Missing local native package ${packageName}; reinstall dependencies with optional packages enabled.`,
    );
  }
  const nativeManifest = JSON.parse(NodeFS.readFileSync(nativeManifestPath, "utf8"));
  if (nativeManifest.version !== VERSION)
    throw new Error(`Expected ${packageName} ${VERSION}, found ${nativeManifest.version}.`);
  const binary = NodePath.join(NodePath.dirname(nativeManifestPath), "tsrs");
  NodeFS.accessSync(binary, NodeFS.constants.X_OK);
  return {
    binary,
    packageName,
    version: manifest.version,
    typescriptVersion: manifest.tsrs.typescriptVersion,
  };
}

export async function runExperimental(root, args) {
  console.error(WARNING);
  let child;
  let cancelled;
  let escalation;
  let scratch;
  const handlers = new Map();
  const stop = (signal) => {
    cancelled ??= signal;
    if (child) {
      child.kill(signal);
      // The captured native child must exit before its build-info directory is removed.
      escalation ??= setTimeout(() => child?.kill("SIGKILL"), 5_000);
    }
  };
  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"]) {
    const handler = () => stop(signal);
    process.on(signal, handler);
    handlers.set(signal, handler);
  }
  const execute = (command, commandArgs, cwd, capture = false) =>
    new Promise((resolve, reject) => {
      if (cancelled) return resolve({ code: 1, signal: cancelled, stdout: "" });
      let stdout = "";
      child = NodeChildProcess.spawn(command, commandArgs, {
        cwd,
        stdio: capture ? ["ignore", "pipe", "inherit"] : "inherit",
      });
      const current = child;
      current.stdout?.setEncoding("utf8").on("data", (chunk) => {
        stdout += chunk;
      });
      current.once("error", reject);
      current.once("close", (code, signal) => {
        clearTimeout(escalation);
        escalation = undefined;
        child = undefined;
        resolve({ code: code ?? 1, signal, stdout });
      });
    });
  let result = { code: 0, signal: null };
  try {
    if (args.length === 1 && args[0] === "--help") {
      console.log(
        "Usage: pnpm typecheck:tsrs:experimental <workspace-name|relative-path>... [--checkers N]\nOnly existing tsc-rs --noEmit projects are supported. No implicit all-project selection.",
      );
      return result;
    }
    const options = parseArguments(args);
    const discovery = await execute(
      "pnpm",
      ["list", "--recursive", "--depth", "-1", "--json"],
      root,
      true,
    );
    if (discovery.code !== 0 || discovery.signal) return discovery;
    const projects = selectProjects(root, JSON.parse(discovery.stdout), options.projects);
    const compiler = await resolveCompiler(root);
    if (cancelled) return { code: 1, signal: cancelled };
    console.log(
      `tsrs ${compiler.version}; TypeScript ${compiler.typescriptVersion}; ${compiler.packageName}; binary ${compiler.binary}`,
    );
    console.log(
      `Selected projects: ${projects.map((project) => `${project.name} (${NodePath.relative(root, project.path)})`).join(", ")}; checkers=${options.checkers}`,
    );
    scratch = NodeFS.mkdtempSync(NodePath.join(root, "node_modules", ".tsrs-experimental-"));
    for (const [index, project] of projects.entries()) {
      const flags = [
        "--project",
        "tsconfig.json",
        "--noEmit",
        "--checkers",
        String(options.checkers),
        "--incremental",
        "--tsBuildInfoFile",
        NodePath.join(scratch, `${index}.tsbuildinfo`),
      ];
      console.log(`cwd=${project.path}; flags=${JSON.stringify(flags)}`);
      const outcome = await execute(compiler.binary, flags, project.path);
      if (outcome.code !== 0 && result.code === 0)
        result = { code: outcome.code, signal: outcome.signal };
      if (outcome.signal || cancelled) {
        result = { code: outcome.code, signal: cancelled ?? outcome.signal };
        break;
      }
    }
  } catch (error) {
    console.error(
      `Experimental typecheck: ${error instanceof Error ? error.message : String(error)}`,
    );
    result.code ||= 1;
  } finally {
    if (scratch) {
      try {
        NodeFS.rmSync(scratch, { recursive: true });
      } catch (error) {
        console.error(`Could not clean invocation scratch ${scratch}: ${String(error)}`);
        result.code ||= 1;
      }
    }
    clearTimeout(escalation);
    for (const [signal, handler] of handlers) process.off(signal, handler);
  }
  if (cancelled) result.signal = cancelled;
  return result;
}

export function finish(result) {
  if (result.signal) {
    process.exitCode = 128 + (NodeOS.constants.signals[result.signal] ?? 1);
    process.kill(process.pid, result.signal);
  } else {
    process.exitCode = result.code;
  }
}
