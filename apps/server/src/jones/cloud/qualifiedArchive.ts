// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { QualifiedRuntimeBlockedError } from "./qualifiedRuntime.ts";

const fail = (message: string): never => {
  throw new QualifiedRuntimeBlockedError("invalid-artifact", message);
};
const text = (bytes: Buffer) => bytes.toString("utf8").replace(/\0.*$/s, "");
const octal = (bytes: Buffer): number => {
  const value = text(bytes).trim();
  if (!/^[0-7]*$/.test(value)) return fail("Archive contains an unsupported numeric header.");
  const number = value === "" ? 0 : Number.parseInt(value, 8);
  if (!Number.isSafeInteger(number) || number < 0)
    return fail("Archive numeric header exceeds its bound.");
  return number;
};

/** Extracts only bounded ordinary files/directories and contained symlinks. No shell or tar parser ambiguity. */
export async function extractQualifiedLinuxArchive(
  archive: string,
  destination: string,
): Promise<void> {
  if ((await NodeFSP.lstat(archive)).size > 2 * 1024 * 1024 * 1024)
    return fail("Compressed runtime exceeds its byte bound.");
  const source = NodeFS.createReadStream(archive);
  const gzip = source.pipe(NodeZlib.createGunzip());
  source.on("error", (error) => gzip.destroy(error));
  let buffered: Buffer = Buffer.alloc(0);
  let remaining = 0;
  let padding = 0;
  let count = 0;
  let expanded = 0;
  let ended = false;
  let entry: NodeFSP.FileHandle | undefined;
  let metadata: { kind: string; chunks: Buffer[]; bytes: number } | undefined;
  let extended: Record<string, string> = {};
  let longName: string | undefined;
  let longLink: string | undefined;
  let root: string | undefined;
  const finish = async () => {
    await entry?.sync();
    await entry?.close();
    entry = undefined;
    if (metadata !== undefined) {
      const value = Buffer.concat(metadata.chunks);
      if (metadata.kind === "L") longName = text(value).replace(/\n$/, "");
      else if (metadata.kind === "K") longLink = text(value).replace(/\n$/, "");
      else {
        let cursor = 0;
        while (cursor < value.length) {
          const space = value.indexOf(32, cursor);
          if (space < 0) return fail("Malformed archive extended header.");
          const length = Number(value.subarray(cursor, space).toString());
          if (
            !Number.isSafeInteger(length) ||
            length <= space - cursor + 1 ||
            cursor + length > value.length ||
            value[cursor + length - 1] !== 10
          )
            return fail("Malformed archive extended header length.");
          const field = value.subarray(space + 1, cursor + length - 1).toString("utf8");
          const equals = field.indexOf("=");
          if (equals < 1) return fail("Malformed archive extended field.");
          const key = field.slice(0, equals);
          if (key === "path" || key === "linkpath") extended[key] = field.slice(equals + 1);
          else if (!["mtime", "atime", "ctime", "uid", "gid", "uname", "gname"].includes(key))
            return fail("Unsupported archive extended field.");
          cursor += length;
        }
      }
      metadata = undefined;
    }
  };
  try {
    for await (const chunk of gzip) {
      expanded += chunk.length;
      if (expanded > 2 * 1024 * 1024 * 1024)
        return fail("Runtime archive exceeds its expanded byte bound.");
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length > 0) {
        if (ended) {
          if (buffered.some((byte) => byte !== 0))
            return fail("Archive has unexpected trailing payload.");
          buffered = Buffer.alloc(0);
          break;
        }
        if (remaining > 0) {
          const bytes = buffered.subarray(0, Math.min(remaining, buffered.length));
          if (entry !== undefined) await entry.writeFile(bytes);
          if (metadata !== undefined) {
            metadata.chunks.push(Buffer.from(bytes));
            metadata.bytes += bytes.length;
          }
          remaining -= bytes.length;
          buffered = buffered.subarray(bytes.length);
          if (remaining === 0) await finish();
          continue;
        }
        if (padding > 0) {
          const length = Math.min(padding, buffered.length);
          padding -= length;
          buffered = buffered.subarray(length);
          continue;
        }
        if (buffered.length < 512) break;
        const header = buffered.subarray(0, 512);
        buffered = buffered.subarray(512);
        if (header.every((byte) => byte === 0)) {
          ended = true;
          continue;
        }
        let checksum = 0;
        for (let index = 0; index < 512; index++)
          checksum += index >= 148 && index < 156 ? 32 : (header[index] ?? 0);
        if (checksum !== octal(header.subarray(148, 156)))
          return fail("Archive header checksum is invalid.");
        if (++count > 100_000) return fail("Runtime archive exceeds its entry bound.");
        const size = octal(header.subarray(124, 136));
        remaining = size;
        padding = (512 - (size % 512)) % 512;
        const kind = String.fromCharCode(header[156] ?? 0);
        if (["x", "L", "K"].includes(kind)) {
          if (size > 64 * 1024) return fail("Archive metadata exceeds its bound.");
          metadata = { kind, chunks: [], bytes: 0 };
          if (size === 0) await finish();
          continue;
        }
        const prefix = text(header.subarray(345, 500));
        const name =
          extended.path ??
          longName ??
          `${prefix === "" ? "" : `${prefix}/`}${text(header.subarray(0, 100))}`;
        const link = extended.linkpath ?? longLink ?? text(header.subarray(157, 257));
        extended = {};
        longName = undefined;
        longLink = undefined;
        if (name.includes("\\") || /[\0\r\n]/.test(name) || NodePath.posix.isAbsolute(name))
          return fail("Archive path is unsafe.");
        const parts = name.replace(/\/$/, "").split("/");
        if (parts.some((part) => part === "" || part === "." || part === ".."))
          return fail("Archive path contains traversal.");
        root ??= parts[0];
        if (parts[0] !== root) return fail("Runtime archive has multiple roots.");
        if (parts.length === 1) {
          if (kind !== "5" || size !== 0) return fail("Runtime archive root is not a directory.");
          continue;
        }
        const relative = parts.slice(1).join("/");
        if (!["t3", "client", "node_modules", "resource-monitor"].includes(parts[1] ?? ""))
          return fail("Runtime archive contains an unexpected payload.");
        const target = NodePath.join(destination, relative);
        let parent = destination;
        for (const part of parts.slice(1, -1)) {
          parent = NodePath.join(parent, part);
          await NodeFSP.mkdir(parent).catch((error: NodeJS.ErrnoException) => {
            if (error.code !== "EEXIST") throw error;
          });
          const stat = await NodeFSP.lstat(parent);
          if (!stat.isDirectory() || stat.isSymbolicLink())
            return fail("Archive writes through a symlink.");
        }
        if (kind === "5") {
          if (size !== 0) return fail("Archive directory contains data.");
          await NodeFSP.mkdir(target).catch((error: NodeJS.ErrnoException) => {
            if (error.code !== "EEXIST") throw error;
          });
          const stat = await NodeFSP.lstat(target);
          if (!stat.isDirectory() || stat.isSymbolicLink())
            return fail("Archive directory conflicts with a file.");
        } else if (kind === "2") {
          if (
            size !== 0 ||
            NodePath.posix.isAbsolute(link) ||
            link.includes("\\") ||
            /[\0\r\n]/.test(link)
          )
            return fail("Archive symlink is unsafe.");
          const resolved = NodePath.resolve(NodePath.dirname(target), link);
          if (!resolved.startsWith(`${NodePath.resolve(destination)}${NodePath.sep}`))
            return fail("Archive symlink escapes its payload.");
          await NodeFSP.symlink(link, target);
        } else if (kind === "0" || kind === "\0") {
          const executable = octal(header.subarray(100, 108)) & 0o111;
          entry = await NodeFSP.open(target, "wx", 0o600 | executable);
          if (size === 0) await finish();
        } else return fail("Runtime archive contains an unsupported entry type.");
      }
    }
    if (!ended || remaining !== 0 || padding !== 0) return fail("Runtime archive is truncated.");
  } finally {
    source.destroy();
    gzip.destroy();
    await entry?.close();
  }
}
