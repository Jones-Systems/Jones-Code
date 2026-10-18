// @effect-diagnostics nodeBuiltinImport:off
// Headless Darwin uses the same native service pointer and state pair as Linux.
// Staging inspects the DMG/ASAR without launching Electron on the live home.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import {
  bundleFileSystem,
  qualifiedPayloadDigest,
  QualifiedRuntimeBlockedError,
} from "./qualifiedRuntime.ts";

export interface DarwinStageCommand {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
}
export type DarwinStageRunner = (
  input: DarwinStageCommand,
) => Promise<{ readonly code: number; readonly stdout: string }>;
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
  readonly baseDir: string;
  readonly run: DarwinStageRunner;
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
  let mounted = false;
  let mountingAttempted = false;
  let stagingFailed = false;
  let stagingFailure: unknown;
  const run = async (command: string, args: ReadonlyArray<string>) => {
    const result = await input.run({ command, args });
    if (result.code !== 0)
      return invalid(
        "The native DMG staging command could not complete; the active service was preserved.",
      );
    return result.stdout.trim();
  };
  try {
    mountingAttempted = true;
    await run("/usr/bin/hdiutil", [
      "attach",
      "-readonly",
      "-nobrowse",
      "-noautoopen",
      "-mountpoint",
      mount,
      input.artifact.payloadPath,
    ]);
    mounted = true;
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
    await qualifiedPayloadDigest(input.destination, "darwin");
  } catch (cause) {
    stagingFailed = true;
    stagingFailure = cause;
  }
  if (mounted || mountingAttempted) {
    // A failed attach can still have mounted the volume. Detach the exact
    // owned mount before considering recursive cleanup safe.
    try {
      await run("/usr/bin/hdiutil", ["detach", mount]);
    } catch (cause) {
      const blocked = new QualifiedRuntimeBlockedError(
        "busy",
        `DMG mount ownership needs reconciliation at ${mountOwner}; it was retained.`,
      );
      // Retain the staging failure as well as the unknown detach effect.
      blocked.cause = stagingFailed
        ? new AggregateError([stagingFailure, cause], "DMG staging and detach failed.")
        : cause;
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
