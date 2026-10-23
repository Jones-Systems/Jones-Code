import * as HostProcess from "@t3tools/shared/HostProcess";

import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";

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
  return object(error) && typeof error.code === "string" ? error.code : undefined;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function digest(bytes: string | Buffer): string {
  return NodeCrypto.createHash("sha256").update(bytes).digest("hex");
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
  const relative = NodePath.relative(root, target);
  return (
    relative === "" ||
    (!NodePath.isAbsolute(relative) &&
      relative !== ".." &&
      !relative.startsWith(`..${NodePath.sep}`))
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
      if (!arg.trim() || NodePath.isAbsolute(arg))
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
            workspace.name === selector || workspace.directory === NodePath.resolve(root, selector),
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
  const child = NodeChildProcess.spawn(command.executable, command.args, {
    cwd: command.cwd,
    // ts-rust 0.7's cross-platform launcher honors GOPORT_LAUNCH=0. Its checker
    // threads then belong to this process instead of an early-returning launcher.
    env: { ...process.env, GOPORT_LAUNCH: "0", RAYON_NUM_THREADS: "2" },
    detached: HostProcess.Platform.defaultValue() !== "win32",
    stdio: ["ignore", command.capture ? "pipe" : "inherit", "inherit"],
    windowsHide: true,
  });
  let output = Buffer.alloc(0);
  let overflow = false;
  let escalation: ReturnType<typeof setTimeout> | undefined;
  let exitDeadline: ReturnType<typeof setTimeout> | undefined;
  let rejectExit: ((error: Error) => void) | undefined;
  const kill = (signal: NodeJS.Signals) => {
    if (!child.pid) return;
    try {
      if (HostProcess.Platform.defaultValue() === "win32") child.kill(signal);
      else process.kill(-child.pid, signal);
    } catch (error) {
      if (errorCode(error) !== "ESRCH") throw error;
    }
  };
  const stop = () => {
    kill(command.signal.reason === "SIGINT" ? "SIGINT" : "SIGTERM");
    escalation ??= setTimeout(() => {
      kill("SIGKILL");
      exitDeadline = setTimeout(
        () =>
          rejectExit?.(
            new UnconfirmedExit(
              `Process ${child.pid} exit unconfirmed after escalation; invocation files retained.`,
            ),
          ),
        5000,
      );
    }, 1500);
  };
  child.stdout?.on("data", (chunk: Buffer) => {
    if (output.length + chunk.length > ENVELOPE_LIMIT) {
      overflow = true;
      stop();
    } else if (!overflow) output = Buffer.concat([output, chunk]);
  });
  command.signal.addEventListener("abort", stop, { once: true });
  if (command.signal.aborted) stop();
  try {
    const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
      (resolve, reject) => {
        rejectExit = reject;
        child.once("error", reject);
        child.once("close", (code, signal) => resolve({ code, signal }));
      },
    );
    if (HostProcess.Platform.defaultValue() !== "win32" && child.pid && groupExists(child.pid)) {
      kill("SIGTERM");
      for (let attempt = 0; attempt < 100 && groupExists(child.pid); attempt++) {
        if (attempt === 30) kill("SIGKILL");
        await NodeTimersPromises.setTimeout(25);
      }
      if (groupExists(child.pid))
        throw new UnconfirmedExit(
          `Native process group ${child.pid} did not exit; invocation files retained.`,
        );
    }
    checkCancellation(command.signal);
    if (result.signal) throw new Interrupted(result.signal);
    if (overflow) throw new Error("Captured command output exceeded 24 MiB.");
    return { code: result.code ?? 1, stdout: output.toString("utf8") };
  } finally {
    command.signal.removeEventListener("abort", stop);
    clearTimeout(escalation);
    clearTimeout(exitDeadline);
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
      throw new Error("Invalid pnpm workspace NodePath.");
    const directory = NodeFS.realpathSync(record.path);
    if (!contained(root, directory))
      throw new Error(`Workspace escapes canonical worktree: ${record.path}`);
    directories.add(directory);
  }
  return [...directories].sort().map((directory) => {
    const manifest: unknown = JSON.parse(
      NodeFS.readFileSync(NodePath.join(directory, "package.json"), "utf8"),
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
  });
}

export function resolveCompiler(root: string) {
  const require = NodeModule.createRequire(NodePath.join(root, "package.json"));
  const wrapper = require.resolve("tsc-rs/package.json");
  const nativeManifest = NodeModule.createRequire(wrapper).resolve(
    `@tsc-rs/${HostProcess.Platform.defaultValue()}-${HostProcess.Architecture.defaultValue()}/package.json`,
  );
  for (const file of [wrapper, nativeManifest]) {
    if (JSON.parse(NodeFS.readFileSync(file, "utf8")).version !== "0.7.0")
      throw new Error(`Expected installed tsc-rs 0.7.0: ${file}`);
  }
  const executable = NodeFS.realpathSync(
    NodePath.join(
      NodePath.dirname(nativeManifest),
      "lib",
      HostProcess.Platform.defaultValue() === "win32" ? "tsc.exe" : "tsc",
    ),
  );
  NodeFS.accessSync(executable, NodeFS.constants.X_OK);
  return { executable, wrapper, nativeManifest };
}

type Compiler = ReturnType<typeof resolveCompiler>;

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

export function configurationInputs(root: string, projects: Workspace[]): Map<string, string> {
  const files = new Map<string, { config: JsonObject; hash: string }>();
  const read = (filename: string) => {
    const cached = files.get(filename);
    if (cached) return cached;
    if (!contained(root, filename) || NodeFS.realpathSync(filename) !== filename) {
      throw new Error(`Unsupported config path or symlink escape: ${filename}`);
    }
    const bytes = readBounded(filename, 1024 * 1024);
    if (!bytes) throw new Error(`Missing or oversized config input (1 MiB limit): ${filename}`);
    const input = { config: configObject(bytes, filename), hash: digest(bytes) };
    files.set(filename, input);
    return input;
  };
  const identities = new Map<string, string>();
  for (const project of projects) {
    const filename = NodePath.join(project.directory, "tsconfig.json");
    const input = read(filename);
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
      project.directory === NodePath.join(root, "apps", "mobile")
    ) {
      const packageBinding = NodePath.join(project.directory, "node_modules", "expo");
      const packageDirectory = NodeFS.realpathSync(packageBinding);
      if (packageDirectory !== NodePath.join(root, EXPO_STORE_PATH)) {
        throw new Error(`Unsupported external Expo config binding: ${packageBinding}`);
      }
      const require = NodeModule.createRequire(NodePath.join(project.directory, "package.json"));
      const manifest = NodePath.join(packageDirectory, "package.json");
      base = NodePath.join(packageDirectory, "tsconfig.base.json");
      if (
        NodeFS.realpathSync(require.resolve("expo/package.json")) !== manifest ||
        NodeFS.realpathSync(require.resolve(extendsValue)) !== base
      ) {
        throw new Error(`Ambiguous Expo config binding: ${packageBinding}`);
      }
      const packageInput = read(manifest);
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
      base = NodePath.join(root, "tsconfig.base.json");
      if (
        !extendsValue.startsWith(".") ||
        NodePath.isAbsolute(extendsValue) ||
        NodePath.resolve(project.directory, extendsValue) !== base
      ) {
        throw new Error(
          `Unsupported config inheritance in ${filename}: only the worktree tsconfig.base.json or mobile Expo base is recognized.`,
        );
      }
    }
    const baseInput = read(base);
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
export function sharedIdentity(root: string, workspaces: Workspace[], compiler: Compiler): string {
  const hash = NodeCrypto.createHash("sha256");
  const add = (label: string, value: string | Buffer) => {
    hash.update(JSON.stringify(label)).update("\0").update(digest(value)).update("\n");
  };
  const file = (filename: string, optional = false) => {
    try {
      add(filename, NodeFS.readFileSync(filename));
    } catch (error) {
      if (optional && errorCode(error) === "ENOENT") add(filename, "<absent>");
      else throw error;
    }
  };
  for (const filename of ["pnpm-lock.yaml", "pnpm-workspace.yaml", "package.json"])
    file(NodePath.join(root, filename));
  for (const workspace of workspaces) file(NodePath.join(workspace.directory, "package.json"));
  for (const filename of [".modules.yaml", ".pnpm/lock.yaml", ".pnpm-workspace-state-v1.json"])
    file(NodePath.join(root, "node_modules", filename), true);
  file(compiler.wrapper);
  file(compiler.nativeManifest);
  file(compiler.executable);
  const libraries = NodePath.dirname(compiler.executable);
  for (const name of NodeFS.readdirSync(libraries).sort())
    if (/^lib.*\.d\.ts$/.test(name)) file(NodePath.join(libraries, name));
  if (!NodeFS.existsSync(NodePath.join(libraries, "lib.d.ts")))
    throw new Error("Native adjacent standard library is missing.");

  const seen = new Set<string>();
  const modules = (directory: string) => {
    if (seen.has(directory)) return;
    seen.add(directory);
    let names: string[];
    try {
      names = NodeFS.readdirSync(directory).sort();
    } catch (error) {
      if (errorCode(error) === "ENOENT") {
        add(directory, "<absent>");
        return;
      }
      throw error;
    }
    for (const name of names) {
      if (name.startsWith(".")) continue;
      const target = NodePath.join(directory, name);
      if (name.startsWith("@")) {
        modules(target);
        continue;
      }
      const resolved = NodeFS.realpathSync(target);
      add(target, resolved);
      file(NodePath.join(resolved, "package.json"), true);
    }
  };
  for (const workspace of workspaces) modules(NodePath.join(workspace.directory, "node_modules"));
  const store = NodePath.join(root, "node_modules", ".pnpm");
  for (const name of NodeFS.readdirSync(store).sort()) {
    if (name === "node_modules") modules(NodePath.join(store, name));
    else if (NodeFS.lstatSync(NodePath.join(store, name)).isDirectory())
      modules(NodePath.join(store, name, "node_modules"));
  }
  return hash.digest("hex");
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
  const namespace = NodePath.join(
    root,
    "node_modules",
    ".cache",
    "jones",
    "incremental-typecheck",
    "v1",
  );
  const directory = NodePath.join(namespace, digest(canonical([root, project.directory])));
  return { namespace, directory, entry: NodePath.join(directory, "entry.json") };
}

function safeDirectory(root: string, directory: string, create: boolean): void {
  if (!contained(root, directory)) throw new Error(`Cache escapes worktree: ${directory}`);
  let current = root;
  for (const component of NodePath.relative(root, directory).split(NodePath.sep).filter(Boolean)) {
    current = NodePath.join(current, component);
    if (create) {
      try {
        NodeFS.mkdirSync(current);
      } catch (error) {
        if (errorCode(error) !== "EEXIST") throw error;
      }
    }
    const stat = NodeFS.lstatSync(current);
    if (stat.isSymbolicLink() || !stat.isDirectory())
      throw new Error(`Unsafe cache directory: ${current}`);
  }
}

export function admitNamespace(root: string, namespace: string, additional: number): void {
  safeDirectory(root, namespace, true);
  let bytes = 0;
  const walk = (directory: string) => {
    for (const name of NodeFS.readdirSync(directory)) {
      const filename = NodePath.join(directory, name);
      let stat: NodeFS.Stats;
      try {
        stat = NodeFS.lstatSync(filename);
      } catch (error) {
        if (errorCode(error) === "ENOENT") continue;
        throw error;
      }
      if (stat.isSymbolicLink() || (!stat.isDirectory() && !stat.isFile()))
        throw new Error(`Unsafe cache entry: ${filename}`);
      bytes += stat.size;
      if (bytes + additional > NAMESPACE_LIMIT)
        throw new Error(
          `Cache capacity admission failed at ${namespace}: requires ${additional} additional bytes within 1 GiB; no automatic fallback or eviction.`,
        );
      if (stat.isDirectory()) {
        try {
          walk(filename);
        } catch (error) {
          if (errorCode(error) !== "ENOENT") throw error;
        }
      }
    }
  };
  walk(namespace);
  const disk = NodeFS.statfsSync(namespace);
  if (disk.bavail * disk.bsize < additional)
    throw new Error(`Insufficient filesystem capacity for cache at ${namespace}.`);
}

function readBounded(filename: string, limit: number): Buffer | undefined {
  let fd: number;
  try {
    if (NodeFS.lstatSync(filename).isSymbolicLink())
      throw new Error(`Unsafe cache symlink: ${filename}`);
    fd = NodeFS.openSync(filename, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  } catch (error) {
    if (errorCode(error) === "ENOENT") return undefined;
    throw error;
  }
  try {
    const stat = NodeFS.fstatSync(fd);
    if (!stat.isFile()) throw new Error(`Cache path is not a regular file: ${filename}`);
    if (stat.size > limit) return undefined;
    const bytes = Buffer.alloc(stat.size + 1);
    let offset = 0;
    while (offset < bytes.length) {
      const count = NodeFS.readSync(fd, bytes, offset, bytes.length - offset, null);
      if (count === 0) break;
      offset += count;
    }
    return offset === stat.size ? bytes.subarray(0, offset) : undefined;
  } finally {
    NodeFS.closeSync(fd);
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

export function readSeed(filename: string, identity: string): Buffer | undefined {
  const bytes = readBounded(filename, ENVELOPE_LIMIT);
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

export interface CheckProjectOptions {
  root: string;
  project: Workspace;
  compiler: Compiler;
  identity: string;
  fresh: boolean;
  signal: AbortSignal;
  inputsUnchanged: () => Promise<boolean>;
  run?: Runner;
  log?: (line: string) => void;
}

export async function checkProject(options: CheckProjectOptions): Promise<number> {
  const { root, project, compiler, identity, fresh, signal } = options;
  const run = options.run ?? runProcess;
  const log = options.log ?? ((line: string) => process.stderr.write(`${line}\n`));
  const paths = cachePaths(root, project);
  checkCancellation(signal);
  admitNamespace(root, paths.namespace, RAW_LIMIT + (fresh ? 0 : ENVELOPE_LIMIT));
  safeDirectory(root, paths.directory, true);
  const nonce = NodeCrypto.randomUUID();
  const native = NodePath.join(paths.directory, `run-${nonce}.tsbuildinfo`);
  const staging = NodePath.join(paths.directory, `entry-${nonce}.tmp`);
  const owned = new Set<string>();
  let exitConfirmed = true;
  let result = 1;
  let failure: unknown;
  try {
    const seed = fresh ? undefined : readSeed(paths.entry, identity);
    // Empty private files are a native cache miss; exclusive creation establishes
    // cleanup ownership before the first write, including partial setup failure.
    const fd = NodeFS.openSync(native, "wx", 0o600);
    owned.add(native);
    try {
      if (seed) NodeFS.writeFileSync(fd, seed);
    } finally {
      NodeFS.closeSync(fd);
    }
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
        safeDirectory(root, paths.directory, false);
        const payload = readBounded(native, RAW_LIMIT);
        if (!payload) throw new Error("Native build info missing or exceeds 16 MiB.");
        const envelope = encodeEnvelope(identity, payload);
        admitNamespace(root, paths.namespace, envelope.length);
        const stagingFd = NodeFS.openSync(staging, "wx", 0o600);
        owned.add(staging);
        try {
          NodeFS.writeFileSync(stagingFd, envelope);
        } finally {
          NodeFS.closeSync(stagingFd);
        }
        checkCancellation(signal);
        // One rename publishes identity and payload together. Concurrent writers
        // may lose reuse, never produce a mixed generation or skip native checking.
        NodeFS.renameSync(staging, paths.entry);
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
        safeDirectory(root, paths.directory, false);
        NodeFS.unlinkSync(filename);
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
    const root = NodeFS.realpathSync(rootDirectory);
    const signal = cancellation.signal;
    const workspaces = await discoverWorkspaces(root, signal, run);
    const { selected, excluded } = selectWorkspaces(root, workspaces, args);
    process.stderr.write(
      `[incremental] Selected (${selected.length}): ${selected.map((workspace) => `${workspace.name} (${NodePath.relative(root, workspace.directory)})`).join(", ")}\n`,
    );
    process.stderr.write(
      `[incremental] Excluded: ${excluded.map((workspace) => `${workspace.name}: ${workspace.script ?? "no typecheck script"}`).join("; ")}\n`,
    );
    const compiler = resolveCompiler(root);
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
    const shared = args.fresh ? "fresh" : sharedIdentity(root, workspaces, compiler);
    // --showConfig drops Effect settings in Rust 0.7. Raw, guarded inputs supply
    // identity for them without interpreting or merging plugin options.
    const configInputs = args.fresh
      ? new Map<string, string>()
      : configurationInputs(root, selected);
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
        configurationInputs(root, [project]).get(project.directory) !==
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
          const currentInputs = configurationInputs(root, [project]).get(project.directory);
          return (
            projectIdentity(root, project, sharedIdentity(root, workspaces, compiler), {
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
