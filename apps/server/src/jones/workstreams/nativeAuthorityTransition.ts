// @effect-diagnostics nodeBuiltinImport:off -- Bounded pre-server command; no server, provider or database service is started.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import packageJson from "../../../package.json" with { type: "json" };
import {
  advanceNativeAuthorityRestore,
  fenceNativeAuthorityRestore,
  type NativeAuthorityRestoreGuard,
} from "../../environment/nativeStoreAuthorityPersistence.ts";

import { bundleFileSystem } from "../cloud/qualifiedRuntime.ts";

const MAX_BYTES = 16 * 1024;
const fail = (): never => {
  throw new Error("Native authority transition refused; reconciliation required.");
};
const object = (value: unknown): Record<string, unknown> => {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return fail();
  return value as Record<string, unknown>;
};
const keys = (value: Record<string, unknown>, names: readonly string[]): void => {
  if (
    Object.keys(value).length !== names.length ||
    names.some((name) => !Object.hasOwn(value, name))
  )
    fail();
};
const text = (value: unknown): string => {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > 4096 ||
    /[\0\r\n]/.test(value)
  )
    return fail();
  return value;
};
const uid = () => (typeof process.getuid === "function" ? process.getuid() : fail());
const owned = (path: string, directory: boolean, fileSystem = NodeFS): void => {
  const stat = fileSystem.lstatSync(path);
  if (
    stat.isSymbolicLink() ||
    (directory ? !stat.isDirectory() : !stat.isFile()) ||
    stat.uid !== uid() ||
    (stat.mode & 0o022) !== 0 ||
    fileSystem.realpathSync(path) !== path
  )
    fail();
};
const readIdentity = (path: string): string => {
  owned(path, false);
  const fd = NodeFS.openSync(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const stat = NodeFS.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== uid() || (stat.mode & 0o022) !== 0 || stat.size > 1024)
      return fail();
    const bytes = Buffer.alloc(1025);
    const count = NodeFS.readSync(fd, bytes, 0, bytes.length, 0);
    if (count > 1024) return fail();
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, count)).trim();
  } finally {
    NodeFS.closeSync(fd);
  }
};
const decodeGuard = (value: unknown): NativeAuthorityRestoreGuard => {
  const guard = object(value);
  if (guard.kind === "absent") {
    keys(guard, ["kind"]);
    return { kind: "absent" };
  }
  keys(guard, [
    "kind",
    "environmentId",
    "authorityNamespace",
    "storeGeneration",
    "transitionId",
    "recordSha256",
  ]);
  if (
    guard.kind !== "fenced" ||
    typeof guard.storeGeneration !== "number" ||
    !Number.isSafeInteger(guard.storeGeneration) ||
    guard.storeGeneration < 1 ||
    !/^t3-native:[0-9a-f-]{36}$/.test(text(guard.authorityNamespace)) ||
    !/^[0-9a-f-]{36}$/.test(text(guard.transitionId)) ||
    !/^[0-9a-f]{64}$/.test(text(guard.recordSha256))
  )
    return fail();
  return {
    kind: "fenced",
    environmentId: text(guard.environmentId),
    authorityNamespace: text(guard.authorityNamespace),
    storeGeneration: guard.storeGeneration,
    transitionId: text(guard.transitionId),
    recordSha256: text(guard.recordSha256),
  };
};

// Runtime paths come from the executing process, never request fields or an environment proof.
const verifyRuntime = (candidate: Record<string, unknown>): void => {
  keys(candidate, ["version", "sourceSha", "sourceTree"]);
  const source = object((packageJson as { readonly jonesSource?: unknown }).jonesSource);
  if (
    candidate.version !== packageJson.version ||
    source.repository !== "Jones-Systems/Jones-Code" ||
    candidate.sourceSha !== source.sha ||
    candidate.sourceTree !== source.tree ||
    !/^[0-9a-f]{40}$/.test(text(candidate.sourceSha)) ||
    !/^[0-9a-f]{40}$/.test(text(candidate.sourceTree))
  )
    fail();
  const executable = process.execPath;
  const macOS = NodePath.dirname(executable);
  const contents = NodePath.dirname(macOS);
  if (NodePath.basename(macOS) !== "MacOS" || NodePath.basename(contents) !== "Contents") fail();
  owned(NodePath.dirname(contents), true);
  owned(contents, true);
  owned(macOS, true);
  owned(executable, false);
  NodeFS.accessSync(executable, NodeFS.constants.X_OK);
  const resources = NodePath.join(contents, "Resources");
  owned(resources, true);
  // Electron presents the archive as a virtual directory; custody checks need the physical file.
  owned(NodePath.join(resources, "app.asar"), false, bundleFileSystem());
  if (process.argv[1] !== NodePath.join(resources, "app.asar", "apps/server/dist/bin.mjs")) fail();
};

export const executeNativeAuthorityTransition = (bytes: Uint8Array): unknown => {
  if (
    bytes.byteLength > MAX_BYTES ||
    process.argv.length !== 3 ||
    process.argv[2] !== "__jones-native-authority-transition"
  )
    return fail();
  const request = object(
    JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown,
  );
  const operation = request.operation;
  if (request.protocol !== 1 || (operation !== "fence" && operation !== "advance")) return fail();
  keys(request, [
    "protocol",
    "operation",
    "baseDir",
    "databasePath",
    "environmentId",
    "candidate",
    ...(operation === "advance" ? ["guard"] : []),
  ]);
  const baseDir = text(request.baseDir);
  const databasePath = text(request.databasePath);
  const environmentId = text(request.environmentId);
  const directory = NodePath.join(baseDir, "native-store-authority");
  if (
    !NodePath.isAbsolute(baseDir) ||
    NodePath.resolve(baseDir) !== baseDir ||
    databasePath !== NodePath.join(baseDir, "userdata/statev2.sqlite") ||
    (process.env.T3CODE_NATIVE_AUTHORITY_STATE_DIR !== undefined &&
      process.env.T3CODE_NATIVE_AUTHORITY_STATE_DIR !== directory)
  )
    return fail();
  owned(baseDir, true);
  owned(NodePath.join(baseDir, "userdata"), true);
  owned(databasePath, false);
  if (readIdentity(NodePath.join(baseDir, "userdata/environment-id")) !== environmentId)
    return fail();
  verifyRuntime(object(request.candidate));
  if (operation === "fence")
    return { protocol: 1, operation, guard: fenceNativeAuthorityRestore(directory, environmentId) };
  const state = advanceNativeAuthorityRestore(
    directory,
    environmentId,
    databasePath,
    decodeGuard(request.guard),
  );
  return state === null
    ? { protocol: 1, operation, state: "absent" }
    : {
        protocol: 1,
        operation,
        state: "advanced",
        authorityNamespace: state.authority_namespace,
        storeGeneration: state.store_generation,
      };
};

export const runNativeAuthorityTransition = async (): Promise<void> => {
  try {
    const chunks: Buffer[] = [];
    let size = 0;
    for await (const chunk of process.stdin) {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string);
      size += bytes.byteLength;
      if (size > MAX_BYTES) fail();
      chunks.push(bytes);
    }
    const result = executeNativeAuthorityTransition(Buffer.concat(chunks));
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } catch {
    process.stderr.write("Native authority transition refused; reconciliation required.\n");
    process.exitCode = 1;
  }
};
