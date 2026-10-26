import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as ByteSize from "effect/ByteSize";
import * as Crypto from "effect/Crypto";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as Effect from "effect/Effect";
import * as CompilerIdentity from "./identity.ts";
// These adapters are the standalone CLI's Effect-to-Promise boundary. Services
// come from their installed Node layers, rather than hidden native imports.
export function hostPath(): Path.Path {
  return Effect.runSync(Effect.provide(Path.Path, NodePath.layer));
}

export function hostFileSystem<A, E>(
  operation: (fs: FileSystem.FileSystem) => Effect.Effect<A, E, Scope.Scope>,
): Promise<A> {
  return Effect.runPromise(
    Effect.scoped(Effect.flatMap(FileSystem.FileSystem, operation)).pipe(
      Effect.provide(NodeFileSystem.layer),
    ),
  );
}

export async function fileInfo(filename: string): Promise<FileSystem.File.Info> {
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

const BUILD_VERSION = CompilerIdentity.BUILD_VERSION;
type JsonObject = Record<string, unknown>;
export function object(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function errorCode(error: unknown): string | undefined {
  if (!object(error)) return undefined;
  if (typeof error.code === "string") return error.code;
  if (object(error.reason)) return errorCode(error.reason.cause);
  return undefined;
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

export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (object(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}
export function contained(root: string, target: string): boolean {
  const relative = hostPath().relative(root, target);
  return (
    relative === "" ||
    (!hostPath().isAbsolute(relative) &&
      relative !== ".." &&
      !relative.startsWith(`..${hostPath().sep}`))
  );
}
export async function safeDirectory(
  root: string,
  directory: string,
  create: boolean,
): Promise<void> {
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

export async function readBounded(filename: string, limit: number): Promise<Buffer | undefined> {
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
    if (!object(value) || value.version !== BUILD_VERSION || !object(value.options)) return false;
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

export function encodeEnvelope(
  identity: string,
  payload: Buffer,
  schema = "jones-incremental-typecheck/v2",
): Buffer {
  if (!validNativePayload(payload))
    throw new Error("Native build info is malformed, incompatible, or exceeds 16 MiB.");
  const bytes = Buffer.from(
    JSON.stringify({
      schema,
      identity,
      checksum: digest(payload),
      payload: payload.toString("base64"),
    }),
  );
  if (bytes.length > ENVELOPE_LIMIT) throw new Error("Cache envelope exceeds 24 MiB.");
  return bytes;
}

export async function readSeed(
  filename: string,
  identity: string,
  schema = "jones-incremental-typecheck/v2",
): Promise<Buffer | undefined> {
  const bytes = await readBounded(filename, ENVELOPE_LIMIT);
  if (!bytes) return undefined;
  try {
    const envelope: unknown = JSON.parse(bytes.toString("utf8"));
    if (
      !object(envelope) ||
      envelope.schema !== schema ||
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

export async function writePrivateFile(
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
