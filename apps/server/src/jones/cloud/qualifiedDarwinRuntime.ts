// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Native staging is a Promise boundary with per-command cleanup timers.
// Headless Darwin uses the same native service pointer and state pair as Linux.
// Staging inspects the DMG/ASAR without launching Electron on the live home.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeTimers from "node:timers";
import * as NodePerfHooks from "node:perf_hooks";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import {
  bundleFileSystem,
  qualifiedPayloadDigest,
  QualifiedRuntimeBlockedError,
} from "./qualifiedRuntime.ts";

export interface DarwinStageCommand {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly signal?: AbortSignal;
  readonly stdin?: string;
}
/** An explicit signal stops only this command; the runner settles after its child has closed. */
export type DarwinStageRunner = (input: DarwinStageCommand) => Promise<DarwinStageResult>;
const DETACH_TIMEOUT_MS = 5_000;

export interface DarwinStageResult {
  readonly code: number;
  readonly stdout: string;
  readonly stderr?: string;
  readonly timedOut?: boolean;
  readonly truncated?: boolean;
  readonly invalidUtf8?: boolean;
  readonly inputFailed?: boolean;
}
const CLEANUP_TIMEOUT_MS = 30_000;
const INVENTORY_LIMIT = 2 * 1024 * 1024;
interface AttachmentImage {
  readonly path: string;
  readonly entities: ReadonlyArray<{ readonly device: string; readonly mount?: string }>;
}
function attachmentInventory(stdout: string): ReadonlyArray<AttachmentImage> {
  const unavailable = (): never =>
    invalid("The DMG attachment inventory is incomplete or invalid.");
  if (Buffer.byteLength(stdout) > INVENTORY_LIMIT) return unavailable();
  let value: unknown;
  try {
    value = JSON.parse(stdout);
  } catch {
    return unavailable();
  }
  const record = (item: unknown): Record<string, unknown> => {
    if (item === null || typeof item !== "object" || Array.isArray(item)) return unavailable();
    return item as Record<string, unknown>;
  };
  const images = record(value).images;
  if (!Array.isArray(images) || images.length > 1024) return unavailable();
  let entityCount = 0;
  return images.map((item) => {
    const image = record(item);
    const path = image["image-path"];
    const entities = image["system-entities"];
    if (
      typeof path !== "string" ||
      !NodePath.isAbsolute(path) ||
      path.length > 4096 ||
      !Array.isArray(entities) ||
      (entityCount += entities.length) > 8192
    )
      return unavailable();
    return {
      path,
      entities: entities.map((item) => {
        const entity = record(item);
        const device = entity["dev-entry"];
        const mount = entity["mount-point"];
        if (
          typeof device !== "string" ||
          !/^\/dev\/disk[0-9]+(?:s[0-9]+)*$/.test(device) ||
          (mount !== undefined &&
            (typeof mount !== "string" || !NodePath.isAbsolute(mount) || mount.length > 4096))
        )
          return unavailable();
        return { device, ...(typeof mount === "string" ? { mount } : {}) };
      }),
    };
  });
}

const invalid = (message: string): never => {
  throw new QualifiedRuntimeBlockedError("invalid-artifact", message);
};
const object = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    return invalid("ASAR metadata is invalid.");
  return value as Record<string, unknown>;
};

async function validateAppTree(app: string): Promise<void> {
  const fs = bundleFileSystem().promises;
  const root = await fs.realpath(app);
  let count = 0;
  let bytes = 0;
  const visit = async (directory: string): Promise<void> => {
    for (const name of await fs.readdir(directory)) {
      if (++count > 100_000) return invalid("App layout exceeds its entry bound.");
      const file = NodePath.join(directory, name);
      const stat = await fs.lstat(file);
      if (stat.isSymbolicLink()) {
        const target = await fs.readlink(file);
        const resolved = await fs.realpath(file);
        if (NodePath.isAbsolute(target) || !resolved.startsWith(`${root}${NodePath.sep}`))
          return invalid("App symlink escapes its immutable bundle.");
      } else if (stat.isDirectory()) await visit(file);
      else if (stat.isFile()) {
        bytes += stat.size;
        if (bytes > 2 * 1024 * 1024 * 1024) return invalid("App layout exceeds its byte bound.");
      } else return invalid("App layout contains a special file.");
    }
  };
  await visit(root);
}

/** Reads bounded ordinary ASAR entries without Electron's virtual filesystem. */
export async function readQualifiedAsarMetadata(
  asar: string,
  expected: { readonly version: string; readonly source: string; readonly tree: string },
): Promise<void> {
  const fd = await bundleFileSystem().promises.open(
    asar,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const stat = await fd.stat();
    if (!stat.isFile() || stat.size > 2 * 1024 * 1024 * 1024)
      return invalid("The app ASAR is not a bounded regular file.");
    const prefix = Buffer.alloc(16);
    if ((await fd.read(prefix, 0, 16, 0)).bytesRead !== 16 || prefix.readUInt32LE(0) !== 4)
      return invalid("ASAR prefix is invalid.");
    const headerSize = prefix.readUInt32LE(4);
    const jsonSize = prefix.readUInt32LE(12);
    if (
      headerSize < 8 ||
      headerSize > 16 * 1024 * 1024 ||
      jsonSize > headerSize - 8 ||
      8 + headerSize > stat.size
    )
      return invalid("ASAR header exceeds its bound.");
    const json = Buffer.alloc(jsonSize);
    if ((await fd.read(json, 0, jsonSize, 16)).bytesRead !== jsonSize)
      return invalid("ASAR header is truncated.");
    const header = object(JSON.parse(json.toString("utf8")) as unknown);
    const ordinaryEntry = (parts: ReadonlyArray<string>) => {
      let directory = header;
      for (const name of parts.slice(0, -1)) directory = object(object(directory.files)[name]);
      const entry = object(object(directory.files)[parts.at(-1) ?? ""]);
      if (
        entry.unpacked === true ||
        entry.link !== undefined ||
        typeof entry.offset !== "string" ||
        !/^\d+$/.test(entry.offset) ||
        typeof entry.size !== "number" ||
        !Number.isSafeInteger(entry.size) ||
        entry.size < 1
      )
        return invalid("Required ASAR entry is not a packed ordinary file.");
      const position = 8 + headerSize + Number(entry.offset);
      if (
        !Number.isSafeInteger(position) ||
        position < 8 + headerSize ||
        position + entry.size > stat.size
      )
        return invalid("ASAR entry exceeds its archive.");
      return { position, size: entry.size };
    };
    ordinaryEntry(["apps", "server", "dist", "bin.mjs"]);
    const packageEntry = ordinaryEntry(["package.json"]);
    if (packageEntry.size > 1024 * 1024) return invalid("App package metadata exceeds its bound.");
    const bytes = Buffer.alloc(packageEntry.size);
    if ((await fd.read(bytes, 0, bytes.length, packageEntry.position)).bytesRead !== bytes.length)
      return invalid("App package metadata is truncated.");
    const metadata = object(JSON.parse(bytes.toString("utf8")) as unknown);
    const source = object(metadata.jonesSource);
    if (
      metadata.version !== expected.version ||
      source.repository !== "Jones-Systems/Jones-Code" ||
      source.sha !== expected.source ||
      source.tree !== expected.tree
    )
      return invalid("App ASAR source or version differs from its qualified Actions receipt.");
  } finally {
    await fd.close();
  }
}

/** Copies one verified app from a read-only mounted DMG and creates its headless wrapper. */
export async function extractQualifiedDarwinRuntime(input: {
  readonly artifact: JonesStagedArtifact;
  readonly destination: string;
  /** The preparation worker transfers this owned tree to staging, which performs the full digest. */
  readonly payloadVerification?: "extraction" | "staging";
  readonly baseDir: string;
  readonly run: DarwinStageRunner;
  readonly signal?: AbortSignal;
}): Promise<void> {
  if (
    input.artifact.candidate.platform !== "darwin" ||
    input.artifact.candidate.architecture !== "arm64"
  )
    return invalid("The headless native runtime requires a Darwin arm64 artifact.");
  const mountOwner = await NodeFSP.mkdtemp(
    NodePath.join(input.baseDir, "runtime", ".jones-dmg-mount-"),
  );
  const mount = NodePath.join(mountOwner, "volume");
  await NodeFSP.mkdir(mount, { mode: 0o700 });
  let mountingAttempted = false;
  let stagingFailed = false;
  let stagingFailure: unknown;
  let before: ReadonlyArray<AttachmentImage> = [];
  let imagePath = NodePath.resolve(input.artifact.payloadPath);
  const run = async (
    command: string,
    args: ReadonlyArray<string>,
    signal = input.signal,
    stdin?: string,
  ) => {
    signal?.throwIfAborted();
    const result = await input.run({
      command,
      args,
      ...(signal === undefined ? {} : { signal }),
      ...(stdin === undefined ? {} : { stdin }),
    });
    if (
      result.code !== 0 ||
      result.timedOut ||
      result.truncated ||
      result.invalidUtf8 ||
      result.inputFailed
    ) {
      const diagnostic = (result.stderr ?? "").replace(/\p{Cc}/gu, " ").slice(0, 2048);
      return invalid(
        "Native " +
          NodePath.basename(command) +
          " " +
          (args[0] ?? "") +
          " failed (exit " +
          result.code +
          (result.timedOut ? ", deadline" : "") +
          (result.truncated ? ", output limit" : "") +
          (result.invalidUtf8 ? ", invalid UTF-8" : "") +
          (result.inputFailed ? ", input pipe" : "") +
          ")." +
          (diagnostic ? " " + diagnostic : "") +
          " The active service was preserved.",
      );
    }
    signal?.throwIfAborted();
    return result.stdout.trim();
  };
  const inventory = async (signal?: AbortSignal) => {
    const plist = await run("/usr/bin/hdiutil", ["info", "-plist"], signal);
    if (Buffer.byteLength(plist) > INVENTORY_LIMIT)
      return invalid("The DMG attachment inventory exceeds its bound.");
    return attachmentInventory(
      await run("/usr/bin/plutil", ["-convert", "json", "-o", "-", "-"], signal, plist),
    );
  };
  const bounded = async <A>(
    action: (signal: AbortSignal) => Promise<A>,
    deadline: number,
    parent?: AbortSignal,
  ): Promise<A> => {
    parent?.throwIfAborted();
    const remaining = Math.min(DETACH_TIMEOUT_MS, deadline - NodePerfHooks.performance.now());
    if (remaining <= 0) throw new Error("The owned DMG cleanup deadline expired.");
    const controller = new AbortController();
    const abort = () => controller.abort(parent?.reason);
    parent?.addEventListener("abort", abort, { once: true });
    const timer = NodeTimers.setTimeout(
      () => controller.abort(new Error("The owned DMG command deadline expired.")),
      remaining,
    );
    try {
      return await action(controller.signal);
    } finally {
      NodeTimers.clearTimeout(timer);
      parent?.removeEventListener("abort", abort);
    }
  };
  try {
    imagePath = await NodeFSP.realpath(input.artifact.payloadPath);
    before = await bounded(
      inventory,
      NodePerfHooks.performance.now() + DETACH_TIMEOUT_MS,
      input.signal,
    );
    if (
      before.some(
        (image) =>
          image.path === imagePath ||
          image.entities.some(
            (entity) =>
              entity.mount === mount || entity.mount?.startsWith(mountOwner + NodePath.sep),
          ),
      )
    )
      throw new QualifiedRuntimeBlockedError(
        "busy",
        "The candidate DMG already has an attachment; it was preserved.",
      );
    mountingAttempted = true;

    await run("/usr/bin/hdiutil", [
      "attach",
      "-readonly",
      "-nobrowse",
      "-noautoopen",
      "-mountpoint",
      mount,
      imagePath,
    ]);
    const apps = (await NodeFSP.readdir(mount)).filter((name) => name.endsWith(".app"));
    if (apps.length !== 1) return invalid("The qualified DMG must contain exactly one app.");
    const source = NodePath.join(mount, apps[0]!);
    const sourceStat = await NodeFSP.lstat(source);
    if (!sourceStat.isDirectory() || sourceStat.isSymbolicLink())
      return invalid("The DMG app is not an ordinary directory.");
    await validateAppTree(source);
    const destination = NodePath.join(input.destination, "Jones Code.app");
    await run("/usr/bin/ditto", ["--rsrc", "--extattr", source, destination]);
    await validateAppTree(destination);
    const plist = NodePath.join(destination, "Contents", "Info.plist");
    const executable = await run("/usr/bin/plutil", [
      "-extract",
      "CFBundleExecutable",
      "raw",
      "-o",
      "-",
      plist,
    ]);
    const version = await run("/usr/bin/plutil", [
      "-extract",
      "CFBundleShortVersionString",
      "raw",
      "-o",
      "-",
      plist,
    ]);
    if (
      !/^[A-Za-z0-9 _().-]+$/.test(executable) ||
      executable === "." ||
      executable === ".." ||
      version !== input.artifact.receipt.version
    )
      return invalid("App executable or plist version differs from the qualified receipt.");
    const nativeExecutable = NodePath.join(destination, "Contents", "MacOS", executable);
    const nativeStat = await NodeFSP.lstat(nativeExecutable);
    if (!nativeStat.isFile() || nativeStat.isSymbolicLink() || (nativeStat.mode & 0o111) === 0)
      return invalid("The app executable is not an ordinary executable.");
    const architectures = (await run("/usr/bin/lipo", ["-archs", nativeExecutable])).split(/\s+/);
    if (!architectures.includes("arm64"))
      return invalid("The app executable does not support arm64.");
    await readQualifiedAsarMetadata(
      NodePath.join(destination, "Contents", "Resources", "app.asar"),
      { version, source: input.artifact.candidate.source, tree: input.artifact.candidate.tree },
    );
    const wrapper = `#!/bin/sh\nset -eu\nroot=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)\nexport ELECTRON_RUN_AS_NODE=1\nexec "$root/Jones Code.app/Contents/MacOS/${executable}" "$root/Jones Code.app/Contents/Resources/app.asar/apps/server/dist/bin.mjs" "$@"\n`;
    await NodeFSP.writeFile(NodePath.join(input.destination, "t3"), wrapper, {
      flag: "wx",
      mode: 0o700,
    });
    if (input.payloadVerification !== "staging")
      await qualifiedPayloadDigest(input.destination, "darwin");
  } catch (cause) {
    stagingFailed = true;
    stagingFailure = cause;
  }
  if (mountingAttempted) {
    const deadline = NodePerfHooks.performance.now() + CLEANUP_TIMEOUT_MS;
    const preexisting = new Set(
      before.flatMap((image) => image.entities.map((entity) => entity.device)),
    );
    let ownedDevices: ReadonlyArray<string> | undefined;
    const target = (images: ReadonlyArray<AttachmentImage>): string | undefined => {
      const matches = images.filter((image) => image.path === imagePath);
      const mounts = images.filter((image) =>
        image.entities.some(
          (entity) => entity.mount === mount || entity.mount?.startsWith(mountOwner + NodePath.sep),
        ),
      );
      if (matches.length === 0 && mounts.length === 0) {
        if (
          ownedDevices === undefined &&
          images.some(
            (image) =>
              !before.some((previous) => previous.path === image.path) ||
              image.entities.some((entity) => !preexisting.has(entity.device)),
          )
        )
          throw new Error("An unattributed new DMG attachment prevents proving absence.");

        if (
          ownedDevices?.some((device) =>
            images.some((image) => image.entities.some((entity) => entity.device === device)),
          )
        )
          throw new Error("The owned DMG device identity changed before absence could be proved.");
        return undefined;
      }
      if (matches.length !== 1 || mounts.some((image) => image !== matches[0]))
        throw new Error("DMG attachment ownership is ambiguous.");
      const image = matches[0]!;
      const devices = image.entities.map((entity) => entity.device).sort();
      const whole = devices.filter((device) => /^\/dev\/disk[0-9]+$/.test(device));
      if (
        whole.length !== 1 ||
        new Set(devices).size !== devices.length ||
        devices.some(
          (device) =>
            preexisting.has(device) || (device !== whole[0] && !device.startsWith(whole[0] + "s")),
        ) ||
        image.entities.some((entity) => entity.mount !== undefined && entity.mount !== mount) ||
        images.some(
          (other) =>
            other !== image && other.entities.some((entity) => devices.includes(entity.device)),
        ) ||
        (ownedDevices !== undefined &&
          (ownedDevices.length !== devices.length ||
            ownedDevices.some((device, index) => device !== devices[index])))
      )
        throw new Error("The candidate DMG does not have one uniquely owned device.");
      ownedDevices = devices;
      return image.entities.some((entity) => entity.mount === mount) ? mount : whole[0]!;
    };
    const observe = () => bounded(inventory, deadline);
    const errors: unknown[] = [];
    try {
      let device = target(await observe());
      for (const force of [false, true]) {
        if (device === undefined) break;
        try {
          await bounded(
            (signal) =>
              run("/usr/bin/hdiutil", ["detach", device!, ...(force ? ["-force"] : [])], signal),
            deadline,
          );
        } catch (cause) {
          errors.push(cause);
        }
        // An exit code never proves that the device or its mount disappeared.
        device = target(await observe());
      }
      if (device !== undefined) {
        if (errors.length > 0)
          throw new AggregateError(errors, "Ordinary and forced DMG detach failed.", {
            cause: errors.at(-1),
          });
        throw new Error("The owned DMG attachment remains after detach.");
      }
    } catch (cause) {
      const cleanup =
        errors.length > 0 && !(cause instanceof AggregateError)
          ? new AggregateError(
              [...errors, cause],
              "DMG detach and inventory reconciliation failed.",
            )
          : cause;
      const blocked = new QualifiedRuntimeBlockedError(
        "busy",
        "DMG mount ownership needs reconciliation at " + mount + "; its directory was retained.",
      );
      blocked.cause = stagingFailed
        ? new AggregateError([stagingFailure, cleanup], "DMG staging and detach failed.")
        : cleanup;
      throw blocked;
    }
  }
  try {
    await NodeFSP.rm(mountOwner, { recursive: true, force: true });
  } catch (cause) {
    if (stagingFailed)
      throw new AggregateError([stagingFailure, cause], "DMG staging and scratch cleanup failed.");
    throw cause;
  }
  if (stagingFailed) throw stagingFailure;
}
