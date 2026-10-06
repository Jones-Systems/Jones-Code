// The enrolled subprocess boundary needs descriptor-level no-follow opens and byte hashing.
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

import * as Schema from "effect/Schema";

// Inspect input keys before Struct decoding removes undeclared properties.
const closed = <Fields extends Schema.Struct.Fields>(schema: Schema.Struct<Fields>) =>
  Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter(
        (value) =>
          typeof value === "object" &&
          value !== null &&
          !Array.isArray(value) &&
          Reflect.ownKeys(value).every((key) => Object.hasOwn(schema.fields, key)),
      ),
    ),
  );
const sha256 = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const absolutePath = Schema.String.check(
  Schema.isMaxLength(4096),
  Schema.makeFilter(
    (value) =>
      !value.endsWith("/") &&
      !value.includes("\0") &&
      NodePath.posix.isAbsolute(value) &&
      NodePath.posix.normalize(value) === value,
  ),
);

export const TOKEN_ACCOUNTING_SOURCE_PATHS = [
  "codex_v3/__init__.py",
  "codex_v3/cache_keepalive/__init__.py",
  "codex_v3/cache_keepalive/contracts.py",
  "codex_v3/cache_keepalive/costing.py",
  "codex_v3/token_info/__init__.py",
  "codex_v3/token_info/accounting_contracts.py",
  "codex_v3/token_info/accounting_report.py",
  "codex_v3/token_info/attribution_contracts.py",
  "codex_v3/token_info/attribution_report.py",
  "codex_v3/token_info/contracts.py",
  "codex_v3/token_info/query.py",
  "codex_v3/token_info/report.py",
  "codex_v3/token_info/saved_reader.py",
  "codex_v3/token_info/saved_reader_contracts.py",
  "codex_v3/token_info/saved_reader_projection.py",
] as const;
const sourceClosure = closed(
  Schema.Struct(
    Object.fromEntries(TOKEN_ACCOUNTING_SOURCE_PATHS.map((path) => [path, sha256])) as Record<
      (typeof TOKEN_ACCOUNTING_SOURCE_PATHS)[number],
      typeof sha256
    >,
  ),
);
const filePin = closed(Schema.Struct({ path: absolutePath, sha256 }));

export const ProcessReaderConfiguration = closed(
  Schema.Struct({
    bindingPath: absolutePath,
    bindingSha256: sha256,
    reportId: sha256,
  }),
);
export type ProcessReaderConfiguration = typeof ProcessReaderConfiguration.Type;

export const ProcessReaderBinding = closed(
  Schema.Struct({
    schema: Schema.Literal("programmatic-token-info.saved-accounting-binding/v1"),
    machine_id_sha256: sha256,
    uid: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER })),
    python: filePin,
    helper: filePin,
    source_root: absolutePath,
    source_closure: sourceClosure,
    source_closure_sha256: sha256,
    archive_root: absolutePath,
    report_id: sha256,
    authority_effect: Schema.Literal("none"),
  }),
);
export type ProcessReaderBinding = typeof ProcessReaderBinding.Type;

export type ProcessReaderStat = Pick<
  NodeFS.Stats,
  "size" | "uid" | "mode" | "nlink" | "dev" | "ino" | "isFile" | "isDirectory" | "isSymbolicLink"
>;
export interface ProcessReaderFile {
  readonly stat: () => Promise<ProcessReaderStat>;
  readonly read: (buffer: Uint8Array, position: number) => Promise<number>;
  readonly close: () => Promise<void>;
}
export interface ProcessReaderFileSystem {
  readonly lstat: (path: string) => Promise<ProcessReaderStat>;
  readonly open: (path: string) => Promise<ProcessReaderFile>;
}
export interface ProcessReaderIdentity {
  readonly realUid: number;
  readonly effectiveUid: number;
  readonly savedUid: number;
}

export const processReaderFileSystem: ProcessReaderFileSystem = {
  lstat: (path) => NodeFSP.lstat(path),
  open: async (path) => {
    const handle = await NodeFSP.open(
      path,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
    );
    return {
      stat: () => handle.stat(),
      read: async (buffer, position) =>
        (await handle.read(buffer, 0, buffer.byteLength, position)).bytesRead,
      close: () => handle.close(),
    };
  },
};

const assertActive = (signal: AbortSignal) => {
  if (signal.aborted) throw new Error("reader_cancelled");
};
const hashBytes = (bytes: Uint8Array) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const utf8 = new TextDecoder("utf-8", { fatal: true });
const MAX_BINDING_BYTES = 64 * 1024;
const MAX_PIN_BYTES = 128 * 1024 * 1024;
const CHUNK_BYTES = 64 * 1024;

async function readFileBytes(
  fileSystem: ProcessReaderFileSystem,
  path: string,
  maximum: number,
  signal: AbortSignal,
  expected?: ProcessReaderStat,
): Promise<Uint8Array> {
  assertActive(signal);
  const file = await fileSystem.open(path);
  try {
    assertActive(signal);
    const stat = await file.stat();
    if (!stat.isFile() || stat.size > maximum) throw new Error("binding_unverified");
    if (
      expected !== undefined &&
      (stat.dev !== expected.dev ||
        stat.ino !== expected.ino ||
        stat.uid !== expected.uid ||
        stat.nlink !== expected.nlink ||
        stat.mode !== expected.mode)
    ) {
      throw new Error("binding_unverified");
    }
    const buffer = new Uint8Array(Math.min(CHUNK_BYTES, maximum + 1));
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    while (true) {
      assertActive(signal);
      const count = await file.read(buffer, bytes);
      assertActive(signal);
      if (count === 0) break;
      bytes += count;
      if (bytes > maximum) throw new Error("binding_unverified");
      chunks.push(buffer.slice(0, count));
    }
    const result = new Uint8Array(bytes);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return result;
  } finally {
    await file.close();
  }
}

export async function processReaderIdentity(
  signal: AbortSignal,
  platform: NodeJS.Platform,
): Promise<ProcessReaderIdentity> {
  if (platform !== "linux" || process.getuid === undefined || process.geteuid === undefined) {
    throw new Error("binding_unverified");
  }
  const status = utf8.decode(
    await readFileBytes(processReaderFileSystem, "/proc/self/status", MAX_BINDING_BYTES, signal),
  );
  const uids = /^Uid:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$/m.exec(status);
  if (uids === null) throw new Error("binding_unverified");
  const realUid = process.getuid();
  const effectiveUid = process.geteuid();
  if (Number(uids[1]) !== realUid || Number(uids[2]) !== effectiveUid)
    throw new Error("binding_unverified");
  return { realUid, effectiveUid, savedUid: Number(uids[3]) };
}

function assertCustody(stat: ProcessReaderStat, uid: number, directory: boolean): void {
  if (
    stat.isSymbolicLink() ||
    (directory ? !stat.isDirectory() : !stat.isFile()) ||
    (stat.uid !== 0 && stat.uid !== uid)
  )
    throw new Error("binding_unverified");
  const stickyRootDirectory = directory && stat.uid === 0 && (stat.mode & 0o1000) !== 0;
  if ((stat.mode & 0o022) !== 0 && !stickyRootDirectory) throw new Error("binding_unverified");
}

async function verifyPath(
  fileSystem: ProcessReaderFileSystem,
  path: string,
  uid: number,
  directory: boolean,
  signal: AbortSignal,
): Promise<ProcessReaderStat> {
  const components = path.split("/").filter(Boolean);
  const parents = [
    "/",
    ...components
      .slice(0, -1)
      .map((_part, index) => `/${components.slice(0, index + 1).join("/")}`),
  ];
  for (const parent of parents) {
    assertActive(signal);
    assertCustody(await fileSystem.lstat(parent), uid, true);
  }
  assertActive(signal);
  const stat = await fileSystem.lstat(path);
  assertActive(signal);
  assertCustody(stat, uid, directory);
  return stat;
}

async function verifyFilePin(
  fileSystem: ProcessReaderFileSystem,
  path: string,
  digest: string,
  uid: number,
  signal: AbortSignal,
): Promise<void> {
  const expected = await verifyPath(fileSystem, path, uid, false, signal);
  const file = await fileSystem.open(path);
  try {
    assertActive(signal);
    const stat = await file.stat();
    assertCustody(stat, uid, false);
    if (stat.dev !== expected.dev || stat.ino !== expected.ino || stat.size > MAX_PIN_BYTES) {
      throw new Error("binding_unverified");
    }
    const hash = NodeCrypto.createHash("sha256");
    const buffer = new Uint8Array(CHUNK_BYTES);
    let bytes = 0;
    while (true) {
      assertActive(signal);
      const count = await file.read(buffer, bytes);
      assertActive(signal);
      if (count === 0) break;
      bytes += count;
      if (bytes > MAX_PIN_BYTES) throw new Error("binding_unverified");
      hash.update(buffer.subarray(0, count));
    }
    if (hash.digest("hex") !== digest) throw new Error("binding_unverified");
  } finally {
    await file.close();
  }
}

const decodeBinding = Schema.decodeUnknownSync(Schema.fromJsonString(ProcessReaderBinding));

export async function verifyProcessReaderConfiguration(
  config: ProcessReaderConfiguration,
  fileSystem: ProcessReaderFileSystem,
  identity: (signal: AbortSignal) => Promise<ProcessReaderIdentity>,
  signal: AbortSignal,
): Promise<ProcessReaderBinding> {
  const uids = await identity(signal);
  assertActive(signal);
  if (
    !Number.isSafeInteger(uids.realUid) ||
    uids.realUid <= 0 ||
    uids.realUid !== uids.effectiveUid ||
    uids.realUid !== uids.savedUid
  ) {
    throw new Error("binding_unverified");
  }
  const bindingStat = await verifyPath(fileSystem, config.bindingPath, uids.realUid, false, signal);
  if (
    bindingStat.uid !== uids.realUid ||
    (bindingStat.mode & 0o077) !== 0 ||
    bindingStat.nlink !== 1
  ) {
    throw new Error("binding_unverified");
  }
  const bytes = await readFileBytes(
    fileSystem,
    config.bindingPath,
    MAX_BINDING_BYTES,
    signal,
    bindingStat,
  );
  if (hashBytes(bytes) !== config.bindingSha256) throw new Error("binding_unverified");
  const binding = decodeBinding(utf8.decode(bytes));
  if (
    binding.uid !== uids.realUid ||
    binding.report_id !== config.reportId ||
    binding.helper.path !== `${binding.source_root}/codex_v3/token_info/saved_reader.py` ||
    binding.helper.sha256 !== binding.source_closure["codex_v3/token_info/saved_reader.py"]
  ) {
    throw new Error("binding_unverified");
  }
  const closureJson = JSON.stringify(
    Object.fromEntries(
      Object.entries(binding.source_closure).sort(([left], [right]) =>
        left < right ? -1 : left > right ? 1 : 0,
      ),
    ),
  );
  if (hashBytes(new TextEncoder().encode(closureJson)) !== binding.source_closure_sha256) {
    throw new Error("binding_unverified");
  }
  const machineBytes = await readFileBytes(fileSystem, "/etc/machine-id", 1024, signal);
  if (machineBytes.some((byte) => byte > 127)) throw new Error("binding_unverified");
  const machineId = utf8.decode(machineBytes).replace(/^[\t\n\v\f\r ]+|[\t\n\v\f\r ]+$/g, "");
  if (
    !/^[0-9a-f]{32}$/.test(machineId) ||
    hashBytes(new TextEncoder().encode(machineId)) !== binding.machine_id_sha256
  ) {
    throw new Error("binding_unverified");
  }
  await verifyPath(fileSystem, binding.source_root, binding.uid, true, signal);
  await verifyPath(fileSystem, binding.archive_root, binding.uid, true, signal);
  await verifyFilePin(fileSystem, binding.python.path, binding.python.sha256, binding.uid, signal);
  for (const relativePath of TOKEN_ACCOUNTING_SOURCE_PATHS) {
    await verifyFilePin(
      fileSystem,
      `${binding.source_root}/${relativePath}`,
      binding.source_closure[relativePath],
      binding.uid,
      signal,
    );
  }
  assertActive(signal);
  return binding;
}
