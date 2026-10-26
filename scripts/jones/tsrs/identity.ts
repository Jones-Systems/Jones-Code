import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as NodePath from "@effect/platform-node/NodePath";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as NodeModule from "node:module";

export const TSRS_VERSION = "0.18.0-ts7.1.0-dev.20260929";
export const EFFECT_VERSION = "0.57.0";
export const BUILD_VERSION = "7.1.0-dev+effect-tsgo.0.57.0";
export const CHECKER_OPTIONS = ["--checkerAssignment", "go", "--checkers", "2"];
export const FALLBACK = {
  name: "@tsc-rs/typescript",
  version: "0.8.0",
  catalog: "npm:@tsc-rs/typescript@0.8.0",
  tscVersion: "7.1.0-dev",
};
export const TARGETS = {
  "linux-x64-gnu": {
    effectPackage: "@effect/tsgo-linux-x64",
    original: "d0bd720c20e9900764811bb0f36e46d2d56916d3048e768053919d40db02a194",
    patched: "cc3aebc216d4631fe8a3963001be2bd2debbe445cdca44a9aab882b274e0b721",
  },
  "linux-arm64-gnu": {
    effectPackage: "@effect/tsgo-linux-arm64",
    original: "0999c116eb963bd7f4a452f811b9e3487334839972d6c1b24cb2f35881440fa4",
    patched: "3948af9a7150092f63b3b11b9c7e797dd46ac0c169a70cd22b7a9d8a4f994e99",
  },
  "darwin-arm64": {
    effectPackage: "@effect/tsgo-darwin-arm64",
    original: "b3f1c65d555aa0dbd665922353c867a5332b00894f5c890cbf8044801fa13260",
    patched: "49e4db9dcda6538d4bf7b8cd832cfcb97ec3750d4972ed7c85f88d3fafcb596b",
  },
};
export type Target = keyof typeof TARGETS;
export interface Host {
  platform: string;
  architecture: string;
  glibc?: string | undefined;
}

export function currentHost(): Host {
  const report = process.report.getReport();
  const header =
    typeof report === "object" && report !== null && "header" in report ? report.header : undefined;
  const glibc =
    typeof header === "object" &&
    header !== null &&
    "glibcVersionRuntime" in header &&
    typeof header.glibcVersionRuntime === "string"
      ? header.glibcVersionRuntime
      : undefined;
  return {
    platform: HostProcess.Platform.defaultValue(),
    architecture: HostProcess.Architecture.defaultValue(),
    glibc,
  };
}

export function hostTarget(host: Host = currentHost()): Target | undefined {
  if (host.platform === "darwin" && host.architecture === "arm64") return "darwin-arm64";
  if (host.platform !== "linux" || !["x64", "arm64"].includes(host.architecture)) return;
  const match = /^(\d+)\.(\d+)(?:\.|$)/.exec(host.glibc ?? "");
  if (!match || Number(match[1]) < 2 || (Number(match[1]) === 2 && Number(match[2]) < 35)) return;
  return host.architecture === "x64" ? "linux-x64-gnu" : "linux-arm64-gnu";
}

export const path = Effect.runSync(Effect.provide(Path.Path, NodePath.layer));

// Standalone Node entry points acquire Effect services at this Promise boundary.
export function files<A, E>(
  operation: (fs: FileSystem.FileSystem) => Effect.Effect<A, E, Scope.Scope>,
): Promise<A> {
  return Effect.runPromise(
    Effect.scoped(Effect.flatMap(FileSystem.FileSystem, operation)).pipe(
      Effect.provide(NodeFileSystem.layer),
    ),
  );
}

export function errorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return;
  if ("code" in error && typeof error.code === "string") return error.code;
  if (
    "reason" in error &&
    typeof error.reason === "object" &&
    error.reason !== null &&
    "cause" in error.reason
  ) {
    return errorCode(error.reason.cause);
  }
}

export function hash(bytes: Uint8Array): string {
  return Buffer.from(
    Effect.runSync(
      Effect.flatMap(Crypto.Crypto, (crypto) => crypto.digest("SHA-256", bytes)).pipe(
        Effect.provide(NodeCrypto.layer),
      ),
    ),
  ).toString("hex");
}

export async function regularInfo(filename: string): Promise<FileSystem.File.Info | undefined> {
  try {
    const linked = await files((fs) => fs.readLink(filename)).then(
      () => true,
      (error: unknown) => {
        if (errorCode(error) === "EINVAL") return false;
        throw error;
      },
    );
    if (linked) throw new Error(`Refusing symlink: ${filename}`);
    return await files((fs) => fs.stat(filename));
  } catch (error) {
    if (errorCode(error) === "ENOENT") return;
    throw error;
  }
}

export function contains(directory: string, filename: string): boolean {
  const relative = path.relative(directory, filename);
  return (
    relative !== "" &&
    !path.isAbsolute(relative) &&
    relative !== ".." &&
    !relative.startsWith(`..${path.sep}`)
  );
}

export interface Installed {
  root: string;
  target: Target;
  wrapper: string;
  nativeManifest: string;
  executable: string;
}

async function manifest(filename: string): Promise<Record<string, unknown>> {
  const value: unknown = JSON.parse(await files((fs) => fs.readFileString(filename)));
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error(`Invalid manifest: ${filename}`);
  return value as Record<string, unknown>;
}

export async function resolveInstalled(rootDirectory: string, target: Target): Promise<Installed> {
  const root = await files((fs) => fs.realPath(rootDirectory));
  const wrapper = NodeModule.createRequire(path.join(root, "noop.js")).resolve("tsrs/package.json");
  const metadata = await manifest(wrapper);
  const nativeName = `@tsrs/${target}`;
  const nativeManifest = NodeModule.createRequire(wrapper).resolve(`${nativeName}/package.json`);
  const native = await manifest(nativeManifest);
  if (
    metadata.name !== "tsrs" ||
    metadata.version !== TSRS_VERSION ||
    native.name !== nativeName ||
    native.version !== TSRS_VERSION
  ) {
    throw new Error(`Expected tsrs and ${nativeName} exactly ${TSRS_VERSION}`);
  }
  return {
    root,
    target,
    wrapper,
    nativeManifest,
    executable: path.join(path.dirname(nativeManifest), "tsrs"),
  };
}

export async function provePrivate(
  installed: Installed,
  inspect: typeof regularInfo = regularInfo,
): Promise<void> {
  const modules = path.join(installed.root, "node_modules");
  const store = path.join(modules, ".pnpm");
  const moduleInfo = await inspect(modules);
  const storeInfo = await inspect(store);
  const directory = await files((fs) => fs.realPath(path.dirname(installed.executable)));
  const directoryInfo = await inspect(directory);
  if (
    moduleInfo?.type !== "Directory" ||
    storeInfo?.type !== "Directory" ||
    directoryInfo?.type !== "Directory" ||
    !contains(store, directory) ||
    moduleInfo.dev !== storeInfo.dev ||
    storeInfo.dev !== directoryInfo.dev
  ) {
    throw new Error(
      "TSRS requires a private isolated node_modules/.pnpm on this worktree filesystem; disable enableGlobalVirtualStore/shared virtualStoreDir and reinstall with the isolated linker.",
    );
  }
  for (const filename of [installed.executable, `${installed.executable}.original`]) {
    const info = await inspect(filename);
    if (info && info.type !== "File") throw new Error(`Expected regular file: ${filename}`);
  }
}

export interface Image {
  hash: string;
  size: string;
  mode: number;
  links: bigint;
  inode: bigint;
}

export async function image(filename: string): Promise<Image | undefined> {
  const info = await regularInfo(filename);
  if (!info) return;
  if (info.type !== "File") throw new Error(`Expected regular file: ${filename}`);
  if (info.size > 64n * 1024n * 1024n)
    throw new Error(`Compiler/lock file exceeds the bounded identity read: ${filename}`);
  const links = Option.getOrUndefined(info.nlink);
  const inode = Option.getOrUndefined(info.ino);
  if (links === undefined || inode === undefined)
    throw new Error(`Missing inode/link metadata: ${filename}`);
  return {
    hash: hash(await files((fs) => fs.readFile(filename))),
    size: String(info.size),
    mode: info.mode,
    links,
    inode,
  };
}

export type StateName = "S0" | "S1" | "S2" | "S3" | "S4";
export interface State {
  name: StateName;
  binary: Image | undefined;
  original: Image | undefined;
  quarantines: string[];
  reason?: string;
}

export async function classify(installed: Installed): Promise<State> {
  await provePrivate(installed);
  const binary = await image(installed.executable);
  const original = await image(`${installed.executable}.original`);
  const entries = await files((fs) => fs.readDirectory(path.dirname(installed.executable)));
  const quarantines: string[] = [];
  let reason: string | undefined;
  for (const name of entries.filter(
    (entry) => entry.startsWith("tsrs") && !["tsrs", "tsrs.original"].includes(entry),
  )) {
    if (!/^tsrs\.[A-Za-z0-9-]+\.patched$/.test(name)) {
      reason = `Unexpected entry: ${name}`;
      break;
    }
    const filename = path.join(path.dirname(installed.executable), name);
    if ((await image(filename))?.links !== 1n) {
      reason = `Quarantine is not private: ${filename}`;
      break;
    }
    quarantines.push(filename);
  }
  const pins = TARGETS[installed.target];
  let name: StateName = "S4";
  if (!reason) {
    if (!original && binary?.hash === pins.original) name = "S0";
    else if (original?.hash === pins.original) {
      if (!binary) name = "S2";
      else if (binary.links === 1n && binary.inode !== original.inode) {
        if (binary.hash === pins.patched && (binary.mode & 0o111) !== 0) name = "S1";
        else if (binary.hash !== pins.original) name = "S3";
      }
    }
  }
  return {
    name,
    binary,
    original,
    quarantines,
    ...(name === "S4"
      ? {
          reason:
            reason ?? "Unrecognized binary/backup identity; no automatic mutation is allowed.",
        }
      : {}),
  };
}

export async function verified(installed: Installed): Promise<Installed> {
  const state = await classify(installed);
  if (state.name !== "S1" || state.quarantines.length)
    throw new Error(
      `Effect TSRS is not verified (${state.name}); run the repository prepare/repair command. ${state.reason ?? ""}`,
    );
  return installed;
}

export async function resolveBundled(
  installed: Installed,
): Promise<{ cli: string; artifact: string }> {
  const require = NodeModule.createRequire(path.join(installed.root, "noop.js"));
  const effectManifest = require.resolve("@effect/tsgo/package.json");
  const metadata = await manifest(effectManifest);
  const pins = TARGETS[installed.target];
  const platformManifest = NodeModule.createRequire(effectManifest).resolve(
    `${pins.effectPackage}/package.json`,
  );
  const platform = await manifest(platformManifest);
  if (
    metadata.version !== EFFECT_VERSION ||
    metadata.name !== "@effect/tsgo" ||
    platform.version !== EFFECT_VERSION ||
    platform.name !== pins.effectPackage
  )
    throw new Error(
      `Expected official @effect/tsgo ${EFFECT_VERSION} and matching platform package`,
    );
  const artifact = path.join(
    path.dirname(platformManifest),
    "artifacts",
    "tsrs",
    TSRS_VERSION,
    "tsrs",
  );
  if ((await image(artifact))?.hash !== pins.patched)
    throw new Error(`Official Effect TSRS artifact hash mismatch: ${artifact}`);
  return { cli: path.join(path.dirname(effectManifest), "dist", "effect-tsgo.cjs"), artifact };
}

export async function resolveFallback(root: string, host: Host = currentHost()) {
  const wrapper = NodeModule.createRequire(path.join(root, "package.json")).resolve(
    "typescript/package.json",
  );
  const metadata = await manifest(wrapper);
  const nativeAlias = `@typescript/typescript-${host.platform}-${host.architecture}`;
  const nativeName = `@tsc-rs/${host.platform}-${host.architecture}`;
  const optional = metadata.optionalDependencies;
  if (
    metadata.name !== FALLBACK.name ||
    metadata.version !== FALLBACK.version ||
    metadata.tscVersion !== FALLBACK.tscVersion ||
    typeof optional !== "object" ||
    optional === null ||
    !(nativeAlias in optional) ||
    optional[nativeAlias as keyof typeof optional] !== `npm:${nativeName}@${FALLBACK.version}`
  )
    throw new Error(
      `Unsupported-host fallback requires exactly ${FALLBACK.name}@${FALLBACK.version}`,
    );
  const nativeManifest = NodeModule.createRequire(wrapper).resolve(`${nativeAlias}/package.json`);
  const native = await manifest(nativeManifest);
  if (native.name !== nativeName || native.version !== FALLBACK.version)
    throw new Error(`Fallback native package mismatch: ${nativeManifest}`);
  const executable = path.join(
    path.dirname(nativeManifest),
    "lib",
    host.platform === "win32" ? "tsc.exe" : "tsc",
  );
  if ((await regularInfo(executable))?.type !== "File")
    throw new Error(`Fallback binary missing: ${executable}`);
  return { wrapper, nativeManifest, executable };
}
