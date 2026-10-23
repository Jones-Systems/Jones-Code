import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ByteSize from "effect/ByteSize";
import * as Crypto from "effect/Crypto";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";

import * as NodeModule from "node:module";
import * as NodeOS from "node:os";

// These adapters are the standalone CLI's Effect-to-Promise boundary. Services
// come from their installed Node layers, rather than hidden native imports.
function hostPath(): Path.Path {
  return Effect.runSync(Effect.provide(Path.Path, NodePath.layer));
}

function hostFileSystem<A, E>(
  operation: (fs: FileSystem.FileSystem) => Effect.Effect<A, E, Scope.Scope>,
): Promise<A> {
  return Effect.runPromise(
    Effect.scoped(Effect.flatMap(FileSystem.FileSystem, operation)).pipe(
      Effect.provide(NodeFileSystem.layer),
    ),
  );
}

async function fileInfo(filename: string): Promise<FileSystem.File.Info> {
  // stat follows links. Reject them explicitly before inspecting their targets.
  const linked = await hostFileSystem((fs) => fs.readLink(filename)).then(
    () => true,
    (error: unknown) => {
      if (errorCode(error) === "EINVAL") return false;
      throw error;
    },
  );
  if (linked) throw new Error(`Unsafe cache entry symlink: ${filename}`);
  return hostFileSystem((fs) => fs.stat(filename));
}

export const RAW_LIMIT = 16 * 1024 * 1024;
export const ENVELOPE_LIMIT = 24 * 1024 * 1024;
export const NAMESPACE_LIMIT = 1024 * 1024 * 1024;
const SCHEMA = "jones-incremental-typecheck/v1";
const OPTIONS = ["--noEmit", "--incremental", "--checkers", "2"];
const NATIVE_VERSION = "Version 7.1.0-dev";
const BUILD_VERSION = "7.1.0-dev+effect-tsgo.0.51.1";
const UPSTREAM_VERSION = "7.1.0-dev.20261008.1";
const EXPO_MANIFEST_HASH = "b64e612beee02fbbf0aa883e9d3d767a2213abb5093d5bd04a2aef87868fd2ba";
const EXPO_STORE_PATH =
  "node_modules/.pnpm/expo@58.0.14_1106b1d11fc0fad6d0556b6a93329d99/node_modules/expo";

export const HELP = `Usage: pnpm typecheck:incremental <workspace-name-or-relative-path>... | --all
Options: --fresh (no retained reads or publication), --help
Only workspace scripts exactly equal to "tsc-rs --noEmit" are supported.
Projects run serially; every selected project runs the native Effect checker.
Cache: node_modules/.cache/jones/incremental-typecheck/v1; retained for the
worktree dependency-cache lifetime. Admission: 1 GiB including crash residue;
payload: 16 MiB; envelope: 24 MiB. No eviction or automatic cold fallback.
These are retention/admission bounds, not concurrent native-write quotas.
Retained config support: no extends, the terminal worktree tsconfig.base.json,
or mobile's qualified Expo base/binding. Unknown topology fails closed; --fresh
bypasses only retained identity/reuse/publication. Configs are bounded to 1 MiB.
`;

type JsonObject = Record<string, unknown>;
export interface Workspace {
  name: string;
  directory: string;
  script: string | undefined;
}
export interface Command {
  executable: string;
  args: string[];
  cwd: string;
  capture: boolean;
  signal: AbortSignal;
}
export interface CommandResult {
  code: number;
  stdout: string;
}
export type Runner = (command: Command) => Promise<CommandResult>;

function object(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorCode(error: unknown): string | undefined {
  if (!object(error)) return undefined;
  if (typeof error.code === "string") return error.code;
  if (object(error.reason)) return errorCode(error.reason.cause);
  return undefined;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function digest(bytes: string | Buffer): string {
  return Buffer.from(
    Effect.runSync(
      Effect.flatMap(Crypto.Crypto, (crypto) =>
        crypto.digest("SHA-256", typeof bytes === "string" ? Buffer.from(bytes) : bytes),
      ).pipe(Effect.provide(NodeCrypto.layer)),
    ),
  ).toString("hex");
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (object(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}

function contained(root: string, target: string): boolean {
  const relative = hostPath().relative(root, target);
  return (
    relative === "" ||
    (!hostPath().isAbsolute(relative) &&
      relative !== ".." &&
      !relative.startsWith(`..${hostPath().sep}`))
  );
}

export function parseArguments(args: string[]) {
  const selectors: string[] = [];
  const flags = new Set<string>();
  for (const arg of args) {
    if (arg.startsWith("-")) {
      if (!["--all", "--fresh", "--help"].includes(arg) || flags.has(arg)) {
        throw new Error(`Unknown or repeated option: ${arg}`);
      }
      flags.add(arg);
    } else {
      if (!arg.trim() || hostPath().isAbsolute(arg))
        throw new Error(`Invalid workspace selector: ${arg}`);
      selectors.push(arg);
    }
  }
  if (flags.has("--all") && selectors.length)
    throw new Error("Use --all or explicit workspaces, not both.");
  if (!flags.has("--help") && !flags.has("--all") && !selectors.length) {
    throw new Error("Explicit workspace selection is required (or use --all).");
  }
  return {
    selectors,
    all: flags.has("--all"),
    fresh: flags.has("--fresh"),
    help: flags.has("--help"),
  };
}

export function selectWorkspaces(
  root: string,
  workspaces: Workspace[],
  args: ReturnType<typeof parseArguments>,
) {
  const eligible = workspaces.filter(
    (workspace) => workspace.directory !== root && workspace.script === "tsc-rs --noEmit",
  );
  const selected = args.all
    ? eligible
    : args.selectors.map((selector) => {
        const matches = workspaces.filter(
          (workspace) =>
            workspace.name === selector ||
            workspace.directory === hostPath().resolve(root, selector),
        );
        if (matches.length !== 1)
          throw new Error(`${matches.length ? "Ambiguous" : "Unknown"} workspace: ${selector}`);
        const workspace = matches[0]!;
        if (!eligible.includes(workspace))
          throw new Error(
            `Unsupported typecheck script for ${selector}: ${workspace.script ?? "<none>"}`,
          );
        return workspace;
      });
  if (!selected.length) throw new Error("No eligible workspaces selected.");
  return {
    selected: [...new Set(selected)].sort((a, b) =>
      a.directory < b.directory ? -1 : a.directory > b.directory ? 1 : 0,
    ),
    excluded: workspaces.filter((workspace) => !eligible.includes(workspace)),
  };
}

class Interrupted extends Error {
  readonly exitCode: number;
  constructor(signal: string) {
    super(`Interrupted by ${signal}`);
    const number = NodeOS.constants.signals[signal as NodeJS.Signals];
    this.exitCode = number === undefined ? 1 : 128 + number;
  }
}

// Cleanup must not race a process whose exit could not be established.
class UnconfirmedExit extends Error {}

class NativeConfigurationFailure extends Error {
  readonly exitCode: number;
  constructor(project: string, exitCode: number) {
    super(`Native configuration resolution failed for ${project} (${exitCode}).`);
    this.exitCode = exitCode;
  }
}

function checkCancellation(signal: AbortSignal): void {
  if (signal.aborted) throw new Interrupted(String(signal.reason));
}

function groupExists(pid: number): boolean {
  try {
    process.kill(-pid, 0);
    return true;
  } catch (error) {
    if (errorCode(error) === "ESRCH") return false;
    throw error;
  }
}

export const runProcess: Runner = async (command) => {
  checkCancellation(command.signal);
  const scope = Scope.makeUnsafe();
  let exitConfirmed = true;
  let escalation: Fiber.Fiber<void> | undefined;
  let capture: Fiber.Fiber<void, PlatformError.PlatformError> | undefined;
  let stop: (() => void) | undefined;
  try {
    const child = await Effect.runPromise(
      Effect.flatMap(ChildProcessSpawner.ChildProcessSpawner, (spawner) =>
        spawner.spawn(
          ChildProcess.make(command.executable, command.args, {
            cwd: command.cwd,
            // The native checker stays in this process on all platforms.
            env: { ...process.env, GOPORT_LAUNCH: "0", RAYON_NUM_THREADS: "2" },
            detached: HostProcess.Platform.defaultValue() !== "win32",
            stdin: "ignore",
            stdout: command.capture ? "pipe" : "inherit",
            stderr: "inherit",
            windowsHide: true,
            forceKillAfter: 1500,
          }),
        ),
      ).pipe(Effect.provide(NodeServices.layer), Scope.provide(scope)),
    );
    let output = Buffer.alloc(0);
    let overflow = false;
    let rejectExit: ((error: Error) => void) | undefined;
    const kill = (signal: NodeJS.Signals) => {
      try {
        process.kill(
          HostProcess.Platform.defaultValue() === "win32" ? child.pid : -child.pid,
          signal,
        );
      } catch (error) {
        if (errorCode(error) !== "ESRCH") throw error;
      }
    };
    stop = () => {
      kill(command.signal.reason === "SIGINT" ? "SIGINT" : "SIGTERM");
      escalation ??= Effect.runFork(
        Effect.gen(function* () {
          yield* Effect.sleep(1500);
          kill("SIGKILL");
          yield* Effect.sleep(5000);
          rejectExit?.(
            new UnconfirmedExit(
              `Process ${child.pid} exit unconfirmed after escalation; invocation files retained.`,
            ),
          );
        }),
      );
    };
    if (command.capture) {
      capture = Effect.runFork(
        Stream.runForEach(child.stdout, (chunk) =>
          Effect.sync(() => {
            if (output.length + chunk.length > ENVELOPE_LIMIT) {
              overflow = true;
              stop?.();
            } else if (!overflow) output = Buffer.concat([output, chunk]);
          }),
        ),
      );
    }
    command.signal.addEventListener("abort", stop, { once: true });
    if (command.signal.aborted) stop();
    const outcome = await new Promise<{ code?: number; error?: unknown }>((resolve, reject) => {
      rejectExit = reject;
      void Effect.runPromise(child.exitCode).then(
        (code) => resolve({ code }),
        (error: unknown) => resolve({ error }),
      );
    });
    if (HostProcess.Platform.defaultValue() !== "win32" && groupExists(child.pid)) {
      kill("SIGTERM");
      for (let attempt = 0; attempt < 100 && groupExists(child.pid); attempt++) {
        if (attempt === 30) kill("SIGKILL");
        await Effect.runPromise(Effect.sleep(25));
      }
      if (groupExists(child.pid))
        throw new UnconfirmedExit(
          `Native process group ${child.pid} did not exit; invocation files retained.`,
        );
    }
    checkCancellation(command.signal);
    if (outcome.error !== undefined) {
      // The Node spawner preserves a signal exit in its PlatformError cause.
      const cause =
        object(outcome.error) && object(outcome.error.reason)
          ? outcome.error.reason.cause
          : outcome.error;
      const signal = /receipt of signal: '(SIG[A-Z]+)'/.exec(message(cause))?.[1];
      if (signal) throw new Interrupted(signal);
      throw outcome.error;
    }
    if (capture) await Effect.runPromise(Fiber.join(capture));
    if (overflow) throw new Error("Captured command output exceeded 24 MiB.");
    return { code: outcome.code ?? 1, stdout: output.toString("utf8") };
  } catch (error) {
    if (error instanceof UnconfirmedExit) exitConfirmed = false;
    throw error;
  } finally {
    if (stop) command.signal.removeEventListener("abort", stop);
    if (escalation) await Effect.runPromise(Fiber.interrupt(escalation));
    if (capture) await Effect.runPromise(Fiber.interrupt(capture));
    // An unconfirmed process retains its scope and exact invocation files;
    // scope release must not conceal an unbounded native exit wait.
    if (exitConfirmed) await Effect.runPromise(Scope.close(scope, Exit.void));
  }
};

export async function discoverWorkspaces(
  root: string,
  signal: AbortSignal,
  run: Runner = runProcess,
): Promise<Workspace[]> {
  const manager = process.env.npm_execpath;
  const viaNode = manager !== undefined && /pnpm\.(?:c?js)$/.test(manager);
  const result = await run({
    executable: viaNode ? process.execPath : "pnpm",
    args: [...(viaNode ? [manager!] : []), "list", "--recursive", "--depth", "-1", "--json"],
    cwd: root,
    capture: true,
    signal,
  });
  if (result.code !== 0)
    throw new Error(`Workspace discovery failed (${result.code}): ${result.stdout}`);
  const records: unknown = JSON.parse(result.stdout);
  if (!Array.isArray(records)) throw new Error("Invalid pnpm workspace inventory.");
  const directories = new Set<string>([root]);
  for (const record of records) {
    if (!object(record) || typeof record.path !== "string")
      throw new Error("Invalid pnpm workspace path.");
    const workspacePath = record.path;
    const directory = await hostFileSystem((fs) => fs.realPath(workspacePath));
    if (!contained(root, directory))
      throw new Error(`Workspace escapes canonical worktree: ${record.path}`);
    directories.add(directory);
  }
  return Promise.all(
    [...directories].sort().map(async (directory) => {
      const manifest: unknown = JSON.parse(
        await hostFileSystem((fs) => fs.readFileString(hostPath().join(directory, "package.json"))),
      );
      if (!object(manifest) || typeof manifest.name !== "string")
        throw new Error(`Workspace name missing: ${directory}`);
      return {
        name: manifest.name,
        directory,
        script:
          object(manifest.scripts) && typeof manifest.scripts.typecheck === "string"
            ? manifest.scripts.typecheck
            : undefined,
      };
    }),
  );
}

export async function resolveCompiler(root: string) {
  const require = NodeModule.createRequire(hostPath().join(root, "package.json"));
  const wrapper = require.resolve("tsc-rs/package.json");
  const nativeManifest = NodeModule.createRequire(wrapper).resolve(
    `@tsc-rs/${HostProcess.Platform.defaultValue()}-${HostProcess.Architecture.defaultValue()}/package.json`,
  );
  for (const file of [wrapper, nativeManifest]) {
    if (JSON.parse(await hostFileSystem((fs) => fs.readFileString(file))).version !== "0.7.0")
      throw new Error(`Expected installed tsc-rs 0.7.0: ${file}`);
  }
  const executable = await hostFileSystem((fs) =>
    fs.realPath(
      hostPath().join(
        hostPath().dirname(nativeManifest),
        "lib",
        HostProcess.Platform.defaultValue() === "win32" ? "tsc.exe" : "tsc",
      ),
    ),
  );
  await hostFileSystem((fs) => fs.access(executable));
  return { executable, wrapper, nativeManifest };
}

type Compiler = Awaited<ReturnType<typeof resolveCompiler>>;

// Validate JSONC shape and duplicate decoded keys, never resolve or merge it.
// Unsupported inheritance must not silently enable retained cache reuse.
function configObject(bytes: Buffer, filename: string): JsonObject {
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes).replace(/^\uFEFF/, "");
  let cursor = 0;
  const invalid = (): never => {
    throw new Error(`Unsupported or ambiguous JSONC config at offset ${cursor}: ${filename}`);
  };
  const whitespace = () => {
    for (;;) {
      while (/[ \t\r\n]/.test(text[cursor] ?? "x")) cursor++;
      if (text.startsWith("//", cursor)) {
        const end = text.indexOf("\n", cursor + 2);
        cursor = end < 0 ? text.length : end + 1;
      } else if (text.startsWith("/*", cursor)) {
        const end = text.indexOf("*/", cursor + 2);
        if (end < 0) invalid();
        cursor = end + 2;
      } else return;
    }
  };
  const string = (): string => {
    if (text[cursor] !== '"') invalid();
    const start = cursor++;
    while (cursor < text.length) {
      const char = text[cursor++];
      if (char === "\\") cursor++;
      else if (char === '"') {
        try {
          return JSON.parse(text.slice(start, cursor)) as string;
        } catch {
          invalid();
        }
      }
    }
    return invalid();
  };
  const value = (depth: number): unknown => {
    if (depth > 64) invalid();
    whitespace();
    const char = text[cursor];
    if (char === '"') return string();
    if (char === "{" || char === "[") {
      cursor++;
      const end = char === "{" ? "}" : "]";
      const record: JsonObject = Object.create(null);
      const list: unknown[] = [];
      const keys = new Set<string>();
      whitespace();
      while (text[cursor] !== end) {
        if (char === "{") {
          const key = string();
          if (depth === 0 && key === "extends" && keys.has(key)) invalid();
          keys.add(key);
          whitespace();
          if (text[cursor++] !== ":") invalid();
          record[key] = value(depth + 1);
        } else list.push(value(depth + 1));
        whitespace();
        if (text[cursor] === end) break;
        if (text[cursor++] !== ",") invalid();
        whitespace();
      }
      cursor++;
      return char === "{" ? record : list;
    }
    const token = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(
      text.slice(cursor),
    );
    if (!token) return invalid();
    cursor += token[0].length;
    return JSON.parse(token[0]) as unknown;
  };
  const parsed = value(0);
  whitespace();
  if (cursor !== text.length || !object(parsed)) return invalid();
  return parsed;
}

export async function configurationInputs(
  root: string,
  projects: Workspace[],
): Promise<Map<string, string>> {
  const files = new Map<string, { config: JsonObject; hash: string }>();
  const read = async (filename: string) => {
    const cached = files.get(filename);
    if (cached) return cached;
    if (
      !contained(root, filename) ||
      (await hostFileSystem((fs) => fs.realPath(filename))) !== filename
    ) {
      throw new Error(`Unsupported config path or symlink escape: ${filename}`);
    }
    const bytes = await readBounded(filename, 1024 * 1024);
    if (!bytes) throw new Error(`Missing or oversized config input (1 MiB limit): ${filename}`);
    const input = { config: configObject(bytes, filename), hash: digest(bytes) };
    files.set(filename, input);
    return input;
  };
  const identities = new Map<string, string>();
  for (const project of projects) {
    const filename = hostPath().join(project.directory, "tsconfig.json");
    const input = await read(filename);
    const extendsValue = input.config.extends;
    if (!Object.hasOwn(input.config, "extends")) {
      identities.set(project.directory, digest(canonical({ filename, projectHash: input.hash })));
      continue;
    }
    if (typeof extendsValue !== "string")
      throw new Error(`Unsupported nonliteral or missing extends: ${filename}`);
    let base: string;
    let binding: unknown = null;
    if (
      extendsValue === "expo/tsconfig.base.json" &&
      project.directory === hostPath().join(root, "apps", "mobile")
    ) {
      const packageBinding = hostPath().join(project.directory, "node_modules", "expo");
      const packageDirectory = await hostFileSystem((fs) => fs.realPath(packageBinding));
      if (packageDirectory !== hostPath().join(root, EXPO_STORE_PATH)) {
        throw new Error(`Unsupported external Expo config binding: ${packageBinding}`);
      }
      const require = NodeModule.createRequire(hostPath().join(project.directory, "package.json"));
      const manifest = hostPath().join(packageDirectory, "package.json");
      base = hostPath().join(packageDirectory, "tsconfig.base.json");
      if (
        (await hostFileSystem((fs) => fs.realPath(require.resolve("expo/package.json")))) !==
          manifest ||
        (await hostFileSystem((fs) => fs.realPath(require.resolve(extendsValue)))) !== base
      ) {
        throw new Error(`Ambiguous Expo config binding: ${packageBinding}`);
      }
      const packageInput = await read(manifest);
      if (packageInput.config.name !== "expo" || packageInput.hash !== EXPO_MANIFEST_HASH)
        throw new Error(
          `Unqualified Expo package identity: ${manifest}; use --fresh until requalified.`,
        );
      binding = {
        path: packageBinding,
        resolved: packageDirectory,
        manifest,
        manifestHash: packageInput.hash,
      };
    } else {
      base = hostPath().join(root, "tsconfig.base.json");
      if (
        !extendsValue.startsWith(".") ||
        hostPath().isAbsolute(extendsValue) ||
        hostPath().resolve(project.directory, extendsValue) !== base
      ) {
        throw new Error(
          `Unsupported config inheritance in ${filename}: only the worktree tsconfig.base.json or mobile Expo base is recognized.`,
        );
      }
    }
    const baseInput = await read(base);
    if (Object.hasOwn(baseInput.config, "extends"))
      throw new Error(`Unsupported nonterminal config base: ${base}`);
    identities.set(
      project.directory,
      digest(
        canonical({ filename, projectHash: input.hash, base, baseHash: baseInput.hash, binding }),
      ),
    );
  }
  return identities;
}

// Fingerprint metadata, not source bytes or Git HEAD. Native incremental state
// remains responsible for source, declaration, and module-resolution changes.
export async function sharedIdentity(
  root: string,
  workspaces: Workspace[],
  compiler: Compiler,
): Promise<string> {
  // Acquire the CLI's stateless adapters once per sweep. Every publication
  // revalidation calls this afresh; no metadata or bindings survive a sweep.
  const path = hostPath();
  const fs = Effect.runSync(Effect.provide(FileSystem.FileSystem, NodeFileSystem.layer));
  const crypto = Effect.runSync(Effect.provide(Crypto.Crypto, NodeCrypto.layer));
  const hash = (bytes: string | Uint8Array) =>
    Buffer.from(
      Effect.runSync(
        crypto.digest("SHA-256", typeof bytes === "string" ? Buffer.from(bytes) : bytes),
      ),
    ).toString("hex");
  const row = (label: string, valueHash: string) => `${JSON.stringify(label)}\0${valueHash}\n`;
  const chunks: string[] = [];
  // Many pnpm bindings name the same physical manifest. Read those bytes once
  // within this snapshot, while still resolving and recording EVERY binding.
  const manifests = new Map<string, Promise<string>>();
  const fileHash = (filename: string, optional = false): Promise<string> => {
    const key = `${optional ? "optional" : "required"}:${filename}`;
    let pending = manifests.get(key);
    if (!pending) {
      pending = Effect.runPromise(fs.readFile(filename)).then(hash, (error: unknown) => {
        if (optional && errorCode(error) === "ENOENT") return hash("<absent>");
        throw error;
      });
      manifests.set(key, pending);
    }
    return pending;
  };
  const file = async (filename: string, optional = false) => {
    chunks.push(row(filename, await fileHash(filename, optional)));
  };
  for (const filename of ["pnpm-lock.yaml", "pnpm-workspace.yaml", "package.json"])
    await file(path.join(root, filename));
  for (const workspace of workspaces) await file(path.join(workspace.directory, "package.json"));
  for (const filename of [".modules.yaml", ".pnpm/lock.yaml", ".pnpm-workspace-state-v1.json"])
    await file(path.join(root, "node_modules", filename), true);
  await file(compiler.wrapper);
  await file(compiler.nativeManifest);
  await file(compiler.executable);
  const libraries = path.dirname(compiler.executable);
  for (const name of (await Effect.runPromise(fs.readDirectory(libraries))).sort())
    if (/^lib.*\.d\.ts$/.test(name)) await file(path.join(libraries, name));
  if (!(await Effect.runPromise(fs.exists(path.join(libraries, "lib.d.ts")))))
    throw new Error("Native adjacent standard library is missing.");

  const seen = new Set<string>();
  const entries: Array<{ path: string; absent: boolean }> = [];
  const modules = async (directory: string): Promise<void> => {
    if (seen.has(directory)) return;
    seen.add(directory);
    let names: string[];
    try {
      names = (await Effect.runPromise(fs.readDirectory(directory))).sort();
    } catch (error) {
      if (errorCode(error) === "ENOENT") {
        entries.push({ path: directory, absent: true });
        return;
      }
      throw error;
    }
    for (const name of names) {
      if (name.startsWith(".")) continue;
      const target = path.join(directory, name);
      if (name.startsWith("@")) await modules(target);
      else entries.push({ path: target, absent: false });
    }
  };
  for (const workspace of workspaces) await modules(path.join(workspace.directory, "node_modules"));
  const store = path.join(root, "node_modules", ".pnpm");
  for (const name of (await Effect.runPromise(fs.readDirectory(store))).sort()) {
    if (name === "node_modules") await modules(path.join(store, name));
    else if ((await fileInfo(path.join(store, name))).type === "Directory")
      await modules(path.join(store, name, "node_modules"));
  }
  // Eight metadata operations at most; append only in the original traversal
  // order. Drain each batch even on failure, before returning to the caller.
  for (let offset = 0; offset < entries.length; offset += 8) {
    const batch = await Promise.allSettled(
      entries.slice(offset, offset + 8).map(async (entry) => {
        if (entry.absent) return row(entry.path, hash("<absent>"));
        const resolved = await Effect.runPromise(fs.realPath(entry.path));
        const manifest = path.join(resolved, "package.json");
        return row(entry.path, hash(resolved)) + row(manifest, await fileHash(manifest, true));
      }),
    );
    for (const result of batch) {
      if (result.status === "rejected") throw result.reason;
      chunks.push(result.value);
    }
  }
  return hash(chunks.join(""));
}

export async function effectiveConfig(
  compiler: Compiler,
  project: Workspace,
  signal: AbortSignal,
  run: Runner,
): Promise<unknown> {
  const result = await run({
    executable: compiler.executable,
    args: [...OPTIONS, "--showConfig"],
    cwd: project.directory,
    capture: true,
    signal,
  });
  if (result.code !== 0) {
    process.stdout.write(result.stdout);
    throw new NativeConfigurationFailure(project.name, result.code);
  }
  const config: unknown = JSON.parse(result.stdout);
  if (!object(config) || !object(config.compilerOptions))
    throw new Error(`Invalid native configuration for ${project.name}.`);
  return config;
}

export function projectIdentity(
  root: string,
  project: Workspace,
  shared: string,
  config: unknown,
): string {
  return digest(
    canonical({
      schema: SCHEMA,
      root,
      project: project.directory,
      shared,
      config,
      options: OPTIONS,
      nativeVersion: NATIVE_VERSION,
      upstreamVersion: UPSTREAM_VERSION,
      launch: "GOPORT_LAUNCH=0",
      rayon: 2,
    }),
  );
}

export function cachePaths(root: string, project: Workspace) {
  const namespace = hostPath().join(
    root,
    "node_modules",
    ".cache",
    "jones",
    "incremental-typecheck",
    "v1",
  );
  const directory = hostPath().join(namespace, digest(canonical([root, project.directory])));
  return { namespace, directory, entry: hostPath().join(directory, "entry.json") };
}

async function safeDirectory(root: string, directory: string, create: boolean): Promise<void> {
  if (!contained(root, directory)) throw new Error(`Cache escapes worktree: ${directory}`);
  let current = root;
  for (const component of hostPath()
    .relative(root, directory)
    .split(hostPath().sep)
    .filter(Boolean)) {
    current = hostPath().join(current, component);
    if (create) {
      try {
        await hostFileSystem((fs) => fs.makeDirectory(current));
      } catch (error) {
        if (errorCode(error) !== "EEXIST") throw error;
      }
    }
    const stat = await fileInfo(current);
    if (stat.type !== "Directory") throw new Error(`Unsafe cache directory: ${current}`);
  }
}

export async function admitNamespace(
  root: string,
  namespace: string,
  additional: number,
): Promise<void> {
  await safeDirectory(root, namespace, true);
  let bytes = 0;
  const walk = async (directory: string): Promise<void> => {
    for (const name of await hostFileSystem((fs) => fs.readDirectory(directory))) {
      const filename = hostPath().join(directory, name);
      let stat: FileSystem.File.Info;
      try {
        stat = await fileInfo(filename);
      } catch (error) {
        if (errorCode(error) === "ENOENT") continue;
        throw error;
      }
      if (stat.type !== "Directory" && stat.type !== "File")
        throw new Error(`Unsafe cache entry: ${filename}`);
      bytes += ByteSize.toNumberUnsafe(stat.size);
      if (bytes + additional > NAMESPACE_LIMIT)
        throw new Error(
          `Cache capacity admission failed at ${namespace}: requires ${additional} additional bytes within 1 GiB; no automatic fallback or eviction.`,
        );
      if (stat.type === "Directory") {
        try {
          await walk(filename);
        } catch (error) {
          if (errorCode(error) !== "ENOENT") throw error;
        }
      }
    }
  };
  await walk(namespace);
}

async function readBounded(filename: string, limit: number): Promise<Buffer | undefined> {
  try {
    await fileInfo(filename);
    return await hostFileSystem((fs) =>
      Effect.gen(function* () {
        const file = yield* fs.open(filename, { flag: "r", noFollow: true });
        const stat = yield* file.stat;
        if (stat.type !== "File") throw new Error(`Cache path is not a regular file: ${filename}`);
        const size = ByteSize.toNumberUnsafe(stat.size);
        if (size > limit) return undefined;
        const bytes = Buffer.alloc(size + 1);
        let offset = 0;
        while (offset < bytes.length) {
          const count = yield* file.read(bytes.subarray(offset));
          if (count === 0) break;
          offset += count;
        }
        return offset === size ? bytes.subarray(0, offset) : undefined;
      }),
    );
  } catch (error) {
    if (errorCode(error) === "ENOENT") return undefined;
    throw error;
  }
}

export function validNativePayload(bytes: Buffer): boolean {
  if (bytes.length > RAW_LIMIT) return false;
  try {
    const value: unknown = JSON.parse(bytes.toString("utf8"));
    if (
      !object(value) ||
      !["7.1.0-dev", BUILD_VERSION].includes(String(value.version)) ||
      !object(value.options)
    )
      return false;
    if (value.effect !== undefined && !object(value.effect)) return false;
    if (
      !Array.isArray(value.fileNames) ||
      value.fileNames.length === 0 ||
      !value.fileNames.every((name) => typeof name === "string" && name.length > 0)
    )
      return false;
    if (
      !Array.isArray(value.fileInfos) ||
      value.fileInfos.length !== value.fileNames.length ||
      !value.fileInfos.every(
        (info) =>
          (typeof info === "string" && info.length > 0) ||
          (object(info) && typeof info.version === "string" && info.version.length > 0),
      )
    )
      return false;
    const count = value.fileNames.length;
    const id = (index: unknown) =>
      Number.isInteger(index) && Number(index) >= 1 && Number(index) <= count;
    for (const key of ["packageJsons", "missingPackageJsons"]) {
      if (
        value[key] !== undefined &&
        (!Array.isArray(value[key]) || !value[key].every((item) => typeof item === "string"))
      )
        return false;
    }
    if (
      value.fileIdsList !== undefined &&
      (!Array.isArray(value.fileIdsList) ||
        !value.fileIdsList.every((list) => Array.isArray(list) && list.every(id)))
    )
      return false;
    for (const key of [
      "referencedMap",
      "semanticDiagnosticsPerFile",
      "affectedFilesPendingEmit",
      "emitDiagnosticsPerFile",
    ]) {
      if (value[key] !== undefined && !Array.isArray(value[key])) return false;
    }
    for (const key of ["target", "module", "moduleResolution", "jsx"]) {
      if (value.options[key] !== undefined && typeof value.options[key] !== "number") return false;
    }
    if (
      value.options.tsBuildInfoFile !== undefined &&
      typeof value.options.tsBuildInfoFile !== "string"
    )
      return false;
    return (
      Array.isArray(value.root) &&
      value.root.every(
        (item) =>
          id(item) ||
          (Array.isArray(item) &&
            item.length === 2 &&
            id(item[0]) &&
            id(item[1]) &&
            item[0] <= item[1]),
      )
    );
  } catch {
    return false;
  }
}

export function encodeEnvelope(identity: string, payload: Buffer): Buffer {
  if (!validNativePayload(payload))
    throw new Error("Native build info is malformed, incompatible, or exceeds 16 MiB.");
  const bytes = Buffer.from(
    JSON.stringify({
      schema: SCHEMA,
      identity,
      checksum: digest(payload),
      payload: payload.toString("base64"),
    }),
  );
  if (bytes.length > ENVELOPE_LIMIT) throw new Error("Cache envelope exceeds 24 MiB.");
  return bytes;
}

export async function readSeed(filename: string, identity: string): Promise<Buffer | undefined> {
  const bytes = await readBounded(filename, ENVELOPE_LIMIT);
  if (!bytes) return undefined;
  try {
    const envelope: unknown = JSON.parse(bytes.toString("utf8"));
    if (
      !object(envelope) ||
      envelope.schema !== SCHEMA ||
      envelope.identity !== identity ||
      typeof envelope.checksum !== "string" ||
      typeof envelope.payload !== "string" ||
      envelope.payload.length > Math.ceil(RAW_LIMIT / 3) * 4
    )
      return undefined;
    const payload = Buffer.from(envelope.payload, "base64");
    if (
      payload.toString("base64") !== envelope.payload ||
      digest(payload) !== envelope.checksum ||
      !validNativePayload(payload)
    )
      return undefined;
    return payload;
  } catch {
    return undefined;
  }
}

async function writePrivateFile(
  filename: string,
  bytes: Buffer,
  created: () => void,
): Promise<void> {
  await hostFileSystem((fs) =>
    Effect.gen(function* () {
      const file = yield* fs.open(filename, { flag: "wx", mode: 0o600 });
      created();
      yield* file.writeAll(bytes);
    }),
  );
}

export interface CheckProjectOptions {
  root: string;
  project: Workspace;
  compiler: Compiler;
  identity: string;
  fresh: boolean;
  signal: AbortSignal;
  inputsUnchanged: () => Promise<boolean>;
  run?: Runner;
  writePrivate?: typeof writePrivateFile;
  log?: (line: string) => void;
}

export async function checkProject(options: CheckProjectOptions): Promise<number> {
  const { root, project, compiler, identity, fresh, signal } = options;
  const run = options.run ?? runProcess;
  const writePrivate = options.writePrivate ?? writePrivateFile;
  const log = options.log ?? ((line: string) => process.stderr.write(`${line}\n`));
  const paths = cachePaths(root, project);
  checkCancellation(signal);
  await admitNamespace(root, paths.namespace, RAW_LIMIT + (fresh ? 0 : ENVELOPE_LIMIT));
  await safeDirectory(root, paths.directory, true);
  const nonce = Effect.runSync(
    Effect.flatMap(Crypto.Crypto, (crypto) => crypto.randomUUIDv4()).pipe(
      Effect.provide(NodeCrypto.layer),
    ),
  );
  const native = hostPath().join(paths.directory, `run-${nonce}.tsbuildinfo`);
  const staging = hostPath().join(paths.directory, `entry-${nonce}.tmp`);
  const owned = new Set<string>();
  let exitConfirmed = true;
  let result = 1;
  let failure: unknown;
  try {
    const seed = fresh ? undefined : await readSeed(paths.entry, identity);
    // Empty private files are a native cache miss; exclusive creation establishes
    // cleanup ownership before the first write, including partial setup failure.
    await writePrivate(native, seed ?? Buffer.alloc(0), () => owned.add(native));
    log(
      `[incremental] ${project.name}: ${fresh ? "fresh" : seed ? "seed validated" : "cache miss"}; ${native}`,
    );
    const outcome = await run({
      executable: compiler.executable,
      args: [...OPTIONS, "--tsBuildInfoFile", native],
      cwd: project.directory,
      capture: false,
      signal,
    });
    result = outcome.code;
    checkCancellation(signal);
    if (result === 0 && !fresh) {
      try {
        if (!(await options.inputsUnchanged())) {
          throw new Error("identity inputs changed during checking.");
        }
        checkCancellation(signal);
        await safeDirectory(root, paths.directory, false);
        const payload = await readBounded(native, RAW_LIMIT);
        if (!payload) throw new Error("Native build info missing or exceeds 16 MiB.");
        const envelope = encodeEnvelope(identity, payload);
        await admitNamespace(root, paths.namespace, envelope.length);
        await writePrivate(staging, envelope, () => owned.add(staging));
        checkCancellation(signal);
        // One rename publishes identity and payload together. Concurrent writers
        // may lose reuse, never produce a mixed generation or skip native checking.
        await hostFileSystem((fs) => fs.rename(staging, paths.entry));
        owned.delete(staging);
      } catch (error) {
        if (error instanceof Interrupted || error instanceof UnconfirmedExit) throw error;
        log(`[incremental] ${project.name}: cache not retained: ${message(error)}`);
      }
    }
  } catch (error) {
    if (error instanceof UnconfirmedExit) exitConfirmed = false;
    failure = error;
  }
  if (exitConfirmed) {
    const failures: string[] = [];
    for (const filename of owned) {
      try {
        await safeDirectory(root, paths.directory, false);
        if ((await fileInfo(filename)).type !== "File")
          throw new Error(`Owned cleanup path is not a regular file: ${filename}`);
        await hostFileSystem((fs) => fs.remove(filename));
      } catch (error) {
        if (errorCode(error) !== "ENOENT") failures.push(`${filename}: ${message(error)}`);
      }
    }
    if (failures.length)
      throw new Error(
        `Invocation cleanup failed (native status ${result}${failure ? `; ${message(failure)}` : ""}); residue: ${failures.join("; ")}`,
        { cause: failure },
      );
  } else
    log(
      `[incremental] Exit unconfirmed; preserved exact invocation files: ${[...owned].join(", ")}`,
    );
  if (failure !== undefined) throw failure;
  return result;
}

export async function runCli(
  rootDirectory: string,
  argv: string[],
  run: Runner = runProcess,
): Promise<number> {
  const cancellation = new AbortController();
  const interrupt = () => cancellation.abort("SIGINT");
  const terminate = () => cancellation.abort("SIGTERM");
  process.on("SIGINT", interrupt);
  process.on("SIGTERM", terminate);
  let firstFailure = 0;
  try {
    const args = parseArguments(argv);
    if (args.help) {
      process.stdout.write(HELP);
      return 0;
    }
    const root = await hostFileSystem((fs) => fs.realPath(rootDirectory));
    const signal = cancellation.signal;
    const workspaces = await discoverWorkspaces(root, signal, run);
    const { selected, excluded } = selectWorkspaces(root, workspaces, args);
    process.stderr.write(
      `[incremental] Selected (${selected.length}): ${selected.map((workspace) => `${workspace.name} (${hostPath().relative(root, workspace.directory)})`).join(", ")}\n`,
    );
    process.stderr.write(
      `[incremental] Excluded: ${excluded.map((workspace) => `${workspace.name}: ${workspace.script ?? "no typecheck script"}`).join("; ")}\n`,
    );
    const compiler = await resolveCompiler(root);
    const version = await run({
      executable: compiler.executable,
      args: ["--version"],
      cwd: root,
      capture: true,
      signal,
    });
    if (version.code !== 0 || version.stdout.trim() !== NATIVE_VERSION)
      throw new Error(`Unexpected native compiler version: ${version.stdout.trim()}`);
    process.stderr.write(
      `[incremental] tsc-rs 0.7.0; ${NATIVE_VERSION}; upstream TypeScript ${UPSTREAM_VERSION} (distinct from root TypeScript API).\n[incremental] ${compiler.executable} ${OPTIONS.join(" ")} --tsBuildInfoFile <private>; GOPORT_LAUNCH=0 RAYON_NUM_THREADS=2\n`,
    );
    const shared = args.fresh ? "fresh" : await sharedIdentity(root, workspaces, compiler);
    // --showConfig drops Effect settings in Rust 0.7. Raw, guarded inputs supply
    // identity for them without interpreting or merging plugin options.
    const configInputs = args.fresh
      ? new Map<string, string>()
      : await configurationInputs(root, selected);
    for (const project of selected) {
      checkCancellation(signal);
      let config: unknown;
      try {
        config = await effectiveConfig(compiler, project, signal, run);
      } catch (error) {
        if (!(error instanceof NativeConfigurationFailure)) throw error;
        process.stderr.write(`[incremental] ${message(error)}\n`);
        if (firstFailure === 0) firstFailure = error.exitCode;
        continue;
      }
      if (
        !args.fresh &&
        (await configurationInputs(root, [project])).get(project.directory) !==
          configInputs.get(project.directory)
      ) {
        throw new Error(
          `Config inputs changed before reuse for ${project.name}; rerun or use --fresh.`,
        );
      }
      const identity = projectIdentity(root, project, shared, {
        native: config,
        inputs: configInputs.get(project.directory),
      });
      const result = await checkProject({
        root,
        project,
        compiler,
        identity,
        fresh: args.fresh,
        signal,
        run,
        inputsUnchanged: async () => {
          const currentConfig = await effectiveConfig(compiler, project, signal, run);
          const currentInputs = (await configurationInputs(root, [project])).get(project.directory);
          return (
            projectIdentity(root, project, await sharedIdentity(root, workspaces, compiler), {
              native: currentConfig,
              inputs: currentInputs,
            }) === identity
          );
        },
      });
      if (result !== 0 && firstFailure === 0) firstFailure = result;
    }
    checkCancellation(signal);
    return firstFailure;
  } catch (error) {
    process.stderr.write(`[incremental] ${message(error)}\n`);
    return error instanceof Interrupted ? error.exitCode : firstFailure || 1;
  } finally {
    process.removeListener("SIGINT", interrupt);
    process.removeListener("SIGTERM", terminate);
  }
}
