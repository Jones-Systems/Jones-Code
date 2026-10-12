// @effect-diagnostics-next-line nodeBuiltinImport:off -- The closed SEA worker owns inherited descriptors and verifies fixed native namespace bytes.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- File access is confined to the inherited maintenance generation.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Digests bind exact owner records and emitted plan bytes.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Paths are derived from the retained executable, never supplied by a caller.
import * as NodePath from "node:path";
import * as NodeSea from "node:sea";
import * as NodeURL from "node:url";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import {
  applyContinuationPlan,
  classifyPreparation,
  createContinuationPlan,
  readPreparationBasis,
} from "./offlinePreparationCore.ts";
import {
  decodeOfflinePreparationPlan,
  encodeOfflinePreparationPlan,
  type OfflinePreparationPlan,
} from "./offlinePreparationPlan.ts";

const limits = {
  header: 16384,
  payload: 8 * 1024 * 1024,
  metadata: 1024 * 1024,
  database: 8 * 1024 * 1024 * 1024,
} as const;
const hashPattern = /^[a-f0-9]{64}$/;
const uuidPattern = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const sha = (bytes: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
class WorkerRefused extends Schema.TaggedError<WorkerRefused>()("WorkerRefused", {}) {
  override get message() {
    return "Closed preparation worker refused.";
  }
}
function requireValue(condition: unknown): asserts condition {
  if (!condition) throw new WorkerRefused();
}
function record(value: unknown): Record<string, unknown> {
  requireValue(value !== null && typeof value === "object" && !Array.isArray(value));
  return value as Record<string, unknown>;
}
function exact(value: unknown, keys: readonly string[]) {
  const item = record(value);
  requireValue(Object.keys(item).sort().join("\0") === [...keys].sort().join("\0"));
  return item;
}
function text(value: unknown): string {
  requireValue(typeof value === "string");
  return value;
}
function hash(value: unknown): string {
  const result = text(value);
  requireValue(hashPattern.test(result));
  return result;
}
function integer(value: unknown, maximum = Number.MAX_SAFE_INTEGER): number {
  requireValue(
    typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value <= maximum,
  );
  return value;
}
function canonical(value: unknown): string {
  // Protocol canonicalization recursively orders closed validated JSON records.
  const encoded = JSON.stringify(value, (_key, item: unknown) =>
    item !== null && typeof item === "object" && !Array.isArray(item)
      ? Object.fromEntries(
          Object.entries(item).sort(([left], [right]) =>
            left < right ? -1 : left > right ? 1 : 0,
          ),
        )
      : item,
  );
  requireValue(typeof encoded === "string");
  return encoded;
}
function json(bytes: Uint8Array): unknown {
  const decoded = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  // Every decoded native record is narrowed at its closed contract below.
  return JSON.parse(decoded) as unknown;
}
function canonicalRecord(bytes: Uint8Array) {
  const value = record(json(bytes));
  requireValue(Buffer.from(canonical(value) + "\n").equals(bytes));
  return value;
}

const fields: Record<string, readonly string[]> = {
  READY: ["runtimeSha256", "entrySha256"],
  PLAN: ["pairSha256", "snapshotSha256", "settingsSha256", "planId", "timestamp"],
  PLAN_READY: ["planSha256"],
  APPLY: ["planSha256", "envelopeSha256"],
  RECONCILE: ["planSha256", "envelopeSha256"],
  SQL_SCOPE_OPENED: ["scopeId", "purpose", "access"],
  CONTINUE: ["scopeId"],
  CLOSED_RESULT: ["scopeId", "planSha256", "outcome", "errorCode"],
  EXIT: [],
  ABORT: [],
};
export interface PreparationFrame {
  readonly header: Readonly<Record<string, unknown>>;
  readonly payload: Uint8Array;
}
function validateHeader(value: unknown) {
  const item = record(value),
    type = text(item.type);
  requireValue(Object.hasOwn(fields, type));
  exact(item, ["schema", "type", "sessionId", "sequence", "payloadBytes", ...fields[type]!]);
  requireValue(item.schema === 1);
  hash(item.sessionId);
  integer(item.sequence, 8);
  integer(item.payloadBytes, limits.payload);
  requireValue(type === "PLAN_READY" ? item.payloadBytes !== 0 : item.payloadBytes === 0);
  requireValue(type === "READY" ? item.sequence === 0 : item.sequence !== 0);
  for (const key of [
    "runtimeSha256",
    "entrySha256",
    "pairSha256",
    "snapshotSha256",
    "settingsSha256",
    "envelopeSha256",
    "scopeId",
  ])
    if (key in item) hash(item[key]);
  if ("planSha256" in item && !(type === "CLOSED_RESULT" && item.planSha256 === null))
    hash(item.planSha256);
  if (type === "PLAN") {
    requireValue(
      /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(
        text(item.planId),
      ),
    );
    const timestamp = text(item.timestamp);
    const parsed = DateTime.make(timestamp);
    requireValue(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(timestamp) &&
        Option.isSome(parsed) &&
        DateTime.formatIso(parsed.value) === timestamp,
    );
  }
  if (type === "SQL_SCOPE_OPENED")
    requireValue(
      ["plan", "apply", "reconcile"].includes(text(item.purpose)) &&
        item.access === (item.purpose === "reconcile" ? "read-only" : "read-write"),
    );
  if (type === "CLOSED_RESULT")
    requireValue(
      ["applied", "not-applied", "held", "query-failed"].includes(text(item.outcome)) &&
        item.errorCode === (item.outcome === "query-failed" ? "query-rejected" : null) &&
        (item.planSha256 !== null || item.outcome === "query-failed"),
    );
  return item;
}
export function encodePreparationFrame(
  header: Readonly<Record<string, unknown>>,
  payload: Uint8Array = new Uint8Array(),
): Uint8Array {
  const item = validateHeader(header);
  requireValue(item.payloadBytes === payload.byteLength);
  const encoded = Buffer.from(canonical(item));
  requireValue(encoded.byteLength > 0 && encoded.byteLength <= limits.header);
  const prefix = Buffer.alloc(4);
  prefix.writeUInt32BE(encoded.byteLength);
  return Buffer.concat([prefix, encoded, payload]);
}
export function decodePreparationFrame(bytes: Uint8Array): PreparationFrame {
  const source = Buffer.from(bytes);
  requireValue(source.length >= 4);
  const length = source.readUInt32BE(0);
  requireValue(length > 0 && length <= limits.header && source.length >= 4 + length);
  const header = validateHeader(json(source.subarray(4, 4 + length)));
  requireValue(
    Buffer.from(canonical(header)).equals(source.subarray(4, 4 + length)) &&
      source.length === 4 + length + integer(header.payloadBytes, limits.payload),
  );
  return { header, payload: source.subarray(4 + length) };
}
/** Descriptor role syntax only; successful parsing grants no maintenance custody. */
export function preparationWorkerArguments(args: readonly string[]) {
  if (
    args.length !== 5 ||
    args[0] !== "--jones-preparation-worker-v1" ||
    args[1] !== "--lock-fd" ||
    args[3] !== "--owner-directory-fd"
  )
    return null;
  if (![args[2]!, args[4]!].every((value) => /^[1-9][0-9]{0,6}$/.test(value))) return null;
  const lock = Number(args[2]),
    directory = Number(args[4]);
  return lock > 2 && directory > 2 && lock !== directory ? { lock, directory } : null;
}
export function requirePreparationCorrelation(
  frame: PreparationFrame,
  sessionId: string,
  sequence: number,
  type: string,
  scopeId?: string,
): void {
  requireValue(
    frame.header.sessionId === sessionId &&
      frame.header.sequence === sequence &&
      frame.header.type === type &&
      frame.payload.length === 0,
  );
  if (scopeId !== undefined) requireValue(frame.header.scopeId === scopeId);
}

/** Path-free SQL operations. Only the private inherited worker below supplies a database scope. */
export function preparationWorkerSql(
  request:
    | { readonly kind: "plan"; readonly settings: string | undefined }
    | {
        readonly kind: "apply" | "reconcile";
        readonly settings: string | undefined;
        readonly plan: OfflinePreparationPlan;
      },
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const mode = yield* sql.unsafe<{ journal_mode: string }>("PRAGMA journal_mode");
    if (mode.length !== 1 || mode[0]!.journal_mode !== "wal")
      return yield* Effect.fail(new WorkerRefused());
    const before = yield* readPreparationBasis(request.settings);
    if (request.kind === "plan") return { kind: "plan", basis: before } as const;
    const current = classifyPreparation(request.plan, before);
    if (request.kind === "reconcile" || current !== "not-applied")
      return { kind: "result", outcome: current } as const;
    yield* applyContinuationPlan(request.plan, request.settings);
    const after = classifyPreparation(request.plan, yield* readPreparationBasis(request.settings));
    if (after !== "applied") return yield* Effect.fail(new WorkerRefused());
    return { kind: "result", outcome: after } as const;
  });
}

type NativeStat = NodeFS.BigIntStats;
const sameObject = (left: NativeStat, right: NativeStat) =>
  left.dev === right.dev &&
  left.ino === right.ino &&
  left.uid === right.uid &&
  left.mode === right.mode &&
  left.nlink === right.nlink;
const sameFile = (left: NativeStat, right: NativeStat) =>
  sameObject(left, right) &&
  left.size === right.size &&
  left.mtimeNs === right.mtimeNs &&
  left.ctimeNs === right.ctimeNs;
function ownedFile(info: NativeStat, maximum: number) {
  requireValue(
    info.isFile() &&
      info.uid === BigInt(process.getuid!()) &&
      info.nlink === 1n &&
      (info.mode & 0o022n) === 0n &&
      info.size <= BigInt(maximum),
  );
}
/** Bounded descriptor reads only; the caller retains ownership, identity checks and closure. */
export async function readBoundedPreparationMetadata(
  handle: {
    read(
      buffer: Buffer,
      offset: number,
      length: number,
      position: number,
    ): Promise<{ bytesRead: number }>;
  },
  maximum: number,
): Promise<Buffer> {
  integer(maximum, 64 * 1024 * 1024);
  const value = Buffer.allocUnsafe(maximum + 1);
  let offset = 0;
  while (offset < value.length) {
    const length = Math.min(65536, value.length - offset);
    const { bytesRead } = await handle.read(value, offset, length, offset);
    integer(bytesRead, length);
    if (bytesRead === 0) break;
    offset += bytesRead;
  }
  requireValue(offset <= maximum);
  return value.subarray(0, offset);
}
async function bytes(path: string, maximum = limits.metadata): Promise<Buffer> {
  const handle = await NodeFSP.open(
    path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  try {
    const before = await handle.stat({ bigint: true });
    ownedFile(before, maximum);
    const value = await readBoundedPreparationMetadata(handle, maximum);
    requireValue(
      BigInt(value.length) === before.size &&
        sameFile(before, await handle.stat({ bigint: true })) &&
        sameFile(before, await NodeFSP.lstat(path, { bigint: true })),
    );
    return value;
  } finally {
    await handle.close();
  }
}
async function fileDigest(path: string, maximum: number) {
  const handle = await NodeFSP.open(
    path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  try {
    const before = await handle.stat({ bigint: true });
    ownedFile(before, maximum);
    const digest = NodeCrypto.createHash("sha256"),
      buffer = Buffer.alloc(1024 * 1024);
    let position = 0;
    while (position < Number(before.size)) {
      const read = await handle.read(
        buffer,
        0,
        Math.min(buffer.length, Number(before.size) - position),
        position,
      );
      requireValue(read.bytesRead > 0);
      position += read.bytesRead;
      digest.update(buffer.subarray(0, read.bytesRead));
    }
    requireValue(
      sameFile(before, await handle.stat({ bigint: true })) &&
        sameFile(before, await NodeFSP.lstat(path, { bigint: true })),
    );
    return { sha256: digest.digest("hex"), size: position, info: before };
  } finally {
    await handle.close();
  }
}
async function directory(path: string) {
  requireValue(NodePath.isAbsolute(path) && NodePath.normalize(path) === path);
  const parts = path.split(NodePath.sep).filter(Boolean);
  let current = NodePath.parse(path).root;
  for (const part of parts) {
    current = NodePath.join(current, part);
    requireValue((await NodeFSP.lstat(current)).isDirectory());
  }
  const info = await NodeFSP.lstat(path, { bigint: true });
  requireValue(info.uid === BigInt(process.getuid!()) && (info.mode & 0o022n) === 0n);
  return info;
}
async function absent(path: string) {
  try {
    await NodeFSP.lstat(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  throw new WorkerRefused();
}
const sourceInputs = [
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "package.json",
  "apps/server/package.json",
  "apps/desktop/package.json",
  "apps/web/package.json",
  "packages/contracts/package.json",
  "apps/server/vite.config.ts",
  "vite.config.ts",
  "scripts/jones/updates/offline-preparation-build.ts",
  "apps/server/src/jones/updates/offlinePreparationEntry.ts",
  "apps/server/src/jones/updates/offlinePreparation.ts",
  "apps/server/src/jones/updates/offlinePreparationCore.ts",
  "apps/server/src/jones/updates/offlinePreparationPlan.ts",
  "apps/server/src/jones/updates/offlinePreparationWorker.ts",
];
const seaConfiguration =
  '{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n';
function preparationBundle(input: unknown) {
  const bundle = exact(input, [
    "schema",
    "repository",
    "sourceSha",
    "sourceTree",
    "version",
    "bootstrapSha256",
    "executorSha256",
    "maintenanceSha256",
    "identityNativeSha256",
    "identityNativeSourceSha256",
    "nativeArchitecture",
    "preparation",
  ]);
  requireValue(
    bundle.schema === 3 &&
      bundle.repository === "Jones-Systems/Jones-Code" &&
      bundle.nativeArchitecture === "arm64",
  );
  for (const key of ["sourceSha", "sourceTree"])
    requireValue(/^[a-f0-9]{40}$/.test(text(bundle[key])));
  for (const key of [
    "bootstrapSha256",
    "executorSha256",
    "maintenanceSha256",
    "identityNativeSha256",
    "identityNativeSourceSha256",
  ])
    hash(bundle[key]);
  requireValue(
    text(bundle.version).length <= 128 &&
      /^[0-9]+\.[0-9]+\.[0-9]+-preview\.[0-9]{8}\.[0-9]+(?:\.[0-9]+)?$/.test(text(bundle.version)),
  );
  const preparation = exact(bundle.preparation, [
    "schema",
    "protocol",
    "provenanceSha256",
    "sourceInputHashes",
    "entry",
    "seaConfig",
    "executable",
    "node",
    "sqlite",
  ]);
  requireValue(preparation.schema === 1 && preparation.protocol === 1);
  hash(preparation.provenanceSha256);
  for (const value of Object.values(exact(preparation.sourceInputHashes, sourceInputs)))
    hash(value);
  for (const [key, bound] of [
    ["entry", 32 * 1024 * 1024],
    ["seaConfig", 16384],
    ["executable", 256 * 1024 * 1024],
  ] as const) {
    const item = exact(
      preparation[key],
      key === "executable" ? ["name", "sha256", "size"] : ["sha256", "size"],
    );
    hash(item.sha256);
    requireValue(integer(item.size, bound) > 0);
  }
  const executable = record(preparation.executable),
    entry = record(preparation.entry),
    config = record(preparation.seaConfig);
  requireValue(
    executable.name === "jones-offline-preparation" &&
      config.sha256 === sha(seaConfiguration) &&
      config.size === Buffer.byteLength(seaConfiguration),
  );
  const node = exact(preparation.node, [
    "version",
    "platform",
    "architecture",
    "archiveSha256",
    "archiveBytes",
    "checksumsSha256",
    "runtimeSha256",
    "runtimeBytes",
  ]);
  requireValue(
    node.version === "26.8.2" && node.platform === "darwin" && node.architecture === "arm64",
  );
  for (const key of ["archiveSha256", "checksumsSha256", "runtimeSha256"]) hash(node[key]);
  requireValue(
    integer(node.archiveBytes, 128 * 1024 * 1024) > 0 &&
      integer(node.runtimeBytes, 256 * 1024 * 1024) > 0,
  );
  const sqlite = exact(preparation.sqlite, ["version", "sourceId"]);
  requireValue(
    /^(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})\.(?:0|[1-9][0-9]{0,2})$/.test(
      text(sqlite.version),
    ),
  );
  const sourceId = text(sqlite.sourceId);
  requireValue(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [a-f0-9]{64}$/.test(sourceId));
  const stamp = sourceId.slice(0, 19).replace(" ", "T") + ".000Z";
  // @effect-diagnostics-next-line globalDate:off -- Parse only the producer's bounded SQLite source timestamp.
  requireValue(new Date(stamp).toISOString() === stamp);
  return {
    runtimeSha256: hash(executable.sha256),
    runtimeBytes: integer(executable.size),
    entrySha256: hash(entry.sha256),
    sqliteVersion: text(sqlite.version),
    sqliteSourceId: sourceId,
  };
}

async function ownerNamespace(roles: NonNullable<ReturnType<typeof preparationWorkerArguments>>) {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- The retained SEA must verify its actual native fixture host before inherited-descriptor dispatch.
  const nativePlatform = process.platform;
  // oxlint-disable-next-line t3code/no-global-process-runtime -- The retained SEA verifies the architecture of its actual executing binary.
  const nativeArchitecture = process.arch;
  requireValue(
    NodeSea.isSea() &&
      nativePlatform === "darwin" &&
      nativeArchitecture === "arm64" &&
      process.versions.node === "26.8.2",
  );
  const executable = process.execPath,
    tools = NodePath.dirname(executable),
    generationPath = NodePath.dirname(tools),
    maintenance = NodePath.dirname(generationPath),
    runtimeRoot = NodePath.dirname(maintenance);
  requireValue(
    NodePath.basename(executable) === "jones-offline-preparation" &&
      NodePath.basename(tools) === "tools" &&
      NodePath.basename(maintenance) === "jones-maintenance",
  );
  const generation = hash(NodePath.basename(generationPath)),
    generationInfo = await directory(generationPath),
    state = NodePath.join(generationPath, "state"),
    stateInfo = await directory(state);
  requireValue(sameObject(generationInfo, NodeFS.fstatSync(roles.directory, { bigint: true })));
  const lockPath = NodePath.join(runtimeRoot, "jones-activation.lock"),
    lockInfo = NodeFS.fstatSync(roles.lock, { bigint: true });
  ownedFile(lockInfo, 25);
  const lockBytes = Buffer.alloc(25);
  const lockLength = NodeFS.readSync(roles.lock, lockBytes, 0, lockBytes.length, 0);
  requireValue(
    lockBytes.subarray(0, lockLength).equals(Buffer.from("jones-activation-lock-v1\n")) &&
      sameObject(lockInfo, await NodeFSP.lstat(lockPath, { bigint: true })),
  );
  const planRaw = await bytes(NodePath.join(generationPath, "plan.json"), 64 * 1024 * 1024),
    plan = record(json(planRaw));
  requireValue(plan.schema === 1 && plan.maintenanceGeneration === generation);
  const planSha256 = sha(planRaw),
    preparation = preparationBundle(plan.bundle),
    originalText = text(plan.originalManifest);
  requireValue(sha(originalText) === plan.originalManifestSha256);
  const original = record(json(Buffer.from(originalText))),
    database = text(original.databasePath),
    manifestPath = text(plan.manifestPath);
  requireValue(
    NodePath.isAbsolute(database) &&
      NodePath.normalize(database) === database &&
      NodePath.dirname(manifestPath) === runtimeRoot &&
      original.owner === "desktop" &&
      original.sourceSha === "bd20d2d2fc0ba89797698192b877a7f1312f8fe3",
  );
  const databaseParent = await directory(NodePath.dirname(database));
  const runtime = await fileDigest(executable, 256 * 1024 * 1024);
  requireValue(
    runtime.sha256 === preparation.runtimeSha256 &&
      runtime.size === preparation.runtimeBytes &&
      (runtime.info.mode & 0o777n) === 0o500n,
  );
  const journalPath = NodePath.join(generationPath, "journal.json");
  const journal = record(json(await bytes(journalPath))),
    cold = exact(journal.coldOwner, ["schema", "sessionId", "bootSessionUuid", "phase"]);
  const sessionId = hash(cold.sessionId);
  requireValue(
    cold.schema === 1 &&
      cold.phase === "entered" &&
      uuidPattern.test(text(cold.bootSessionUuid)) &&
      uuidPattern.test(text(journal.armedBootSessionUuid)) &&
      text(cold.bootSessionUuid).toLowerCase() !== text(journal.armedBootSessionUuid).toLowerCase(),
  );
  const dispatchPath = NodePath.join(state, "worker-" + sessionId + ".json"),
    dispatchRaw = await bytes(dispatchPath, 16384);
  const dispatch = exact(canonicalRecord(dispatchRaw), [
    "schema",
    "sessionId",
    "bootSessionUuid",
    "parentPid",
    "maintenanceGeneration",
    "maintenancePlanSha256",
    "transactionId",
    "intentSha256",
    "pairSha256",
    "runtimeSha256",
    "entrySha256",
    "purpose",
  ]);
  requireValue(
    dispatch.schema === 1 &&
      dispatch.sessionId === sessionId &&
      dispatch.bootSessionUuid === cold.bootSessionUuid &&
      dispatch.parentPid === process.ppid &&
      dispatch.maintenanceGeneration === generation &&
      dispatch.maintenancePlanSha256 === planSha256 &&
      dispatch.runtimeSha256 === preparation.runtimeSha256 &&
      dispatch.entrySha256 === preparation.entrySha256 &&
      (dispatch.purpose === "prepare" || dispatch.purpose === "recover"),
  );
  hash(dispatch.transactionId);
  hash(dispatch.intentSha256);
  hash(dispatch.pairSha256);
  const intentRaw = await bytes(NodePath.join(state, "intent.json")),
    intent = record(json(intentRaw));
  requireValue(
    sha(intentRaw) === dispatch.intentSha256 &&
      intent.transactionId === dispatch.transactionId &&
      intent.maintenanceGeneration === generation &&
      intent.maintenancePlanSha256 === planSha256 &&
      intent.originalManifestSha256 === plan.originalManifestSha256,
  );
  const pairRaw = await bytes(NodePath.join(state, "pair-descriptor.json")),
    pair = exact(json(pairRaw), ["schema", "intentSha256", "source", "copied", "reserve"]);
  requireValue(
    pair.schema === 1 &&
      pair.intentSha256 === dispatch.intentSha256 &&
      sha(pairRaw) === dispatch.pairSha256,
  );
  const current = async () => {
    requireValue(
      process.ppid === dispatch.parentPid &&
        sameObject(generationInfo, await directory(generationPath)) &&
        sameObject(stateInfo, await directory(state)) &&
        sameObject(databaseParent, await directory(NodePath.dirname(database))) &&
        sameObject(generationInfo, NodeFS.fstatSync(roles.directory, { bigint: true })),
    );
    requireValue(
      sameFile(lockInfo, NodeFS.fstatSync(roles.lock, { bigint: true })) &&
        sameFile(lockInfo, await NodeFSP.lstat(lockPath, { bigint: true })) &&
        sameFile(runtime.info, await NodeFSP.lstat(executable, { bigint: true })),
    );
    requireValue(
      (await bytes(NodePath.join(generationPath, "plan.json"), 64 * 1024 * 1024)).equals(planRaw) &&
        (await bytes(dispatchPath, 16384)).equals(dispatchRaw) &&
        (await bytes(NodePath.join(state, "intent.json"))).equals(intentRaw) &&
        (await bytes(NodePath.join(state, "pair-descriptor.json"))).equals(pairRaw),
    );
    const cursor = exact(json(await bytes(NodePath.join(maintenance, "current.json"), 8192)), [
      "schema",
      "generation",
      "planSha256",
    ]);
    requireValue(
      cursor.schema === 1 && cursor.generation === generation && cursor.planSha256 === planSha256,
    );
    const activeJournal = record(json(await bytes(journalPath))),
      fence = record(activeJournal.stateTransaction);
    requireValue(
      activeJournal.schema === 1 &&
        activeJournal.phase === "armed" &&
        activeJournal.maintenanceGeneration === generation &&
        activeJournal.planSha256 === planSha256 &&
        canonical(activeJournal.coldOwner) === canonical(cold) &&
        fence.transactionId === dispatch.transactionId &&
        fence.intentSha256 === dispatch.intentSha256,
    );
    const active = record(json(await bytes(manifestPath)));
    requireValue(
      canonical(active) ===
        canonical({
          ...original,
          owner: "maintenance",
          generation,
          maintenance: { schema: 1, planSha256 },
        }),
    );
    const paired = record(json(await bytes(NodePath.join(state, "journal.json"))));
    requireValue(
      paired.schema === 1 &&
        paired.intentSha256 === dispatch.intentSha256 &&
        paired.pairSha256 === dispatch.pairSha256 &&
        ["paired", "preparation-intent", "prepared"].includes(text(paired.phase)),
    );
    return paired;
  };
  await current();
  return { state, database, dispatch, pair, preparation, current, sessionId };
}

async function verifyMember(path: string, input: unknown, maximum: number) {
  if (input === null) {
    await absent(path);
    return;
  }
  const member = record(input);
  requireValue(Array.isArray(member.content) && member.content.length === 1);
  const row = member.content[0];
  requireValue(Array.isArray(row) && row.length === 5 && row[0] === "." && row[1] === "file");
  const observed = await fileDigest(path, maximum);
  requireValue(
    observed.sha256 === hash(row[4]) &&
      observed.size === integer(row[3], maximum) &&
      Number(observed.info.mode & 0o777n) === integer(row[2], 0o777),
  );
}
async function verifyFamily(database: string, members: Record<string, unknown>) {
  requireValue(members.database !== null);
  for (const [key, suffix] of [
    ["database", ""],
    ["wal", "-wal"],
    ["shm", "-shm"],
  ] as const)
    await verifyMember(database + suffix, members[key], limits.database);
  await absent(database + "-journal");
}
async function retainedBasis(owner: Awaited<ReturnType<typeof ownerNamespace>>) {
  await directory(NodePath.join(owner.state, "previous-pair"));
  const copied = record(owner.pair.copied),
    source = record(owner.pair.source);
  for (const name of ["database", "wal", "shm", "settings.json"]) {
    const old = source[name],
      saved = copied[name];
    requireValue(
      old === null
        ? saved === null
        : saved !== null && canonical(record(old).content) === canonical(record(saved).content),
    );
    await verifyMember(
      NodePath.join(owner.state, "previous-pair", name),
      saved,
      name === "settings.json" ? 262144 : limits.database,
    );
  }
  requireValue(copied.database !== null);
  const snapshotSha256 = hash((record(copied.database).content as unknown[][])[0]![4]);
  const settings =
    copied["settings.json"] === null
      ? undefined
      : (
          await bytes(NodePath.join(owner.state, "previous-pair", "settings.json"), 262144)
        ).toString("utf8");
  const settingsSha256 = sha(settings ?? "");
  return { copied, source, settings, snapshotSha256, settingsSha256 };
}
async function durablePlan(
  owner: Awaited<ReturnType<typeof ownerNamespace>>,
  command: PreparationFrame,
  basis: Awaited<ReturnType<typeof retainedBasis>>,
) {
  const paired = await owner.current();
  requireValue(["preparation-intent", "prepared"].includes(text(paired.phase)));
  const planRaw = await bytes(NodePath.join(owner.state, "preparation-plan.json"), limits.payload),
    decoded = decodeOfflinePreparationPlan(planRaw);
  const envelopeRaw = await bytes(NodePath.join(owner.state, "preparation-envelope.json"), 16384);
  const envelope = exact(canonicalRecord(envelopeRaw), [
    "schema",
    "maintenanceGeneration",
    "maintenancePlanSha256",
    "transactionId",
    "intentSha256",
    "pairSha256",
    "runtimeSha256",
    "entrySha256",
    "originSessionId",
    "originBootSessionUuid",
    "planSha256",
    "planBytes",
    "snapshotSha256",
    "settingsSha256",
  ]);
  requireValue(
    envelope.schema === 1 &&
      envelope.planBytes === planRaw.length &&
      envelope.planSha256 === decoded.sha256 &&
      paired.planSha256 === decoded.sha256 &&
      command.header.planSha256 === decoded.sha256 &&
      command.header.envelopeSha256 === sha(envelopeRaw),
  );
  for (const key of [
    "maintenanceGeneration",
    "maintenancePlanSha256",
    "transactionId",
    "intentSha256",
    "pairSha256",
    "runtimeSha256",
    "entrySha256",
  ])
    requireValue(envelope[key] === owner.dispatch[key]);
  hash(envelope.originSessionId);
  requireValue(uuidPattern.test(text(envelope.originBootSessionUuid)));
  if (owner.dispatch.purpose === "prepare")
    requireValue(
      envelope.originSessionId === owner.sessionId &&
        envelope.originBootSessionUuid === owner.dispatch.bootSessionUuid,
    );
  requireValue(
    envelope.snapshotSha256 === basis.snapshotSha256 &&
      envelope.settingsSha256 === basis.settingsSha256 &&
      decoded.plan.snapshotSha256 === basis.snapshotSha256 &&
      decoded.plan.settingsSha256 === basis.settingsSha256,
  );
  return decoded;
}

class WorkerChannel {
  readonly abort = new AbortController();
  private pending = Buffer.alloc(0);
  private wake: (() => void) | undefined;
  private failure: unknown;
  private waiting = false;
  private readonly onData = (chunk: Buffer) => {
    if (this.pending.length + chunk.length > limits.header + limits.payload + 4) {
      this.fail(new WorkerRefused());
      return;
    }
    this.pending = Buffer.concat([this.pending, chunk]);
    this.wake?.();
  };
  private readonly onEnd = () => this.fail(new WorkerRefused());
  private readonly onError = () => this.fail(new WorkerRefused());
  private readonly onSignal = () => this.fail(new WorkerRefused());
  // @effect-diagnostics-next-line globalTimers:off -- The child bounds its inherited pipe lifetime independently of the scoped SQL runtime.
  private readonly deadline = setTimeout(() => this.fail(new WorkerRefused()), 120000);
  constructor() {
    process.stdin.on("data", this.onData);
    process.stdin.on("end", this.onEnd);
    process.stdin.on("error", this.onError);
    process.stdout.on("error", this.onError);
    process.on("SIGTERM", this.onSignal);
    process.on("SIGINT", this.onSignal);
  }
  private fail(error: unknown) {
    this.failure ??= error;
    this.abort.abort();
    this.wake?.();
  }
  async receive(): Promise<PreparationFrame> {
    requireValue(!this.waiting);
    this.waiting = true;
    try {
      while (true) {
        if (this.failure !== undefined) throw this.failure;
        if (this.pending.length >= 4) {
          const length = this.pending.readUInt32BE(0);
          requireValue(length > 0 && length <= limits.header);
          if (this.pending.length >= 4 + length) {
            const header = validateHeader(json(this.pending.subarray(4, 4 + length)));
            const total = 4 + length + integer(header.payloadBytes, limits.payload);
            if (this.pending.length >= total) {
              requireValue(this.pending.length === total);
              const frame = decodePreparationFrame(this.pending);
              this.pending = Buffer.alloc(0);
              return frame;
            }
          }
        }
        await new Promise<void>((resolve) => {
          this.wake = resolve;
        });
        this.wake = undefined;
      }
    } finally {
      this.waiting = false;
      this.wake = undefined;
    }
  }
  async send(header: Readonly<Record<string, unknown>>, payload: Uint8Array = new Uint8Array()) {
    if (this.failure !== undefined) throw this.failure;
    const encoded = encodePreparationFrame(header, payload);
    await new Promise<void>((resolve, reject) => {
      const cancelled = () => reject(new WorkerRefused());
      this.abort.signal.addEventListener("abort", cancelled, { once: true });
      if (this.abort.signal.aborted) {
        this.abort.signal.removeEventListener("abort", cancelled);
        cancelled();
        return;
      }
      try {
        process.stdout.write(encoded, (error) => {
          this.abort.signal.removeEventListener("abort", cancelled);
          if (error) reject(error);
          else resolve();
        });
      } catch (error) {
        this.abort.signal.removeEventListener("abort", cancelled);
        reject(error);
      }
    });
    if (this.failure !== undefined) throw this.failure;
  }
  close() {
    // Release the exact timer acquired by this inherited pipe channel.
    clearTimeout(this.deadline);
    process.stdin.pause();
    process.stdin.off("data", this.onData);
    process.stdin.off("end", this.onEnd);
    process.stdin.off("error", this.onError);
    process.stdout.off("error", this.onError);
    process.off("SIGTERM", this.onSignal);
    process.off("SIGINT", this.onSignal);
    this.wake = undefined;
    this.pending = Buffer.alloc(0);
  }
}

/** The only stateful production entry derives all paths and authority relationships internally. */
export async function runInheritedPreparationWorker(): Promise<void> {
  const roles = preparationWorkerArguments(process.argv.slice(2));
  requireValue(roles !== null);
  const channel = new WorkerChannel();
  let primary: unknown;
  try {
    const owner = await ownerNamespace(roles);
    let sequence = 0,
      planned = false,
      applied = false,
      queryFailed = false;
    const header = (type: string, additional: Record<string, unknown> = {}, payloadBytes = 0) => ({
      schema: 1,
      type,
      sessionId: owner.sessionId,
      sequence,
      payloadBytes,
      ...additional,
    });
    await channel.send(
      header("READY", {
        runtimeSha256: owner.preparation.runtimeSha256,
        entrySha256: owner.preparation.entrySha256,
      }),
    );
    while (true) {
      const command = await channel.receive();
      sequence += 1;
      const type = text(command.header.type);
      requirePreparationCorrelation(command, owner.sessionId, sequence, type);
      requireValue(["PLAN", "APPLY", "RECONCILE", "EXIT", "ABORT"].includes(type));
      await owner.current();
      if (type === "EXIT" || type === "ABORT") {
        await channel.send(header(type));
        return;
      }
      requireValue(!queryFailed);
      const basis = await retainedBasis(owner);
      let database = owner.database,
        settings = basis.settings,
        plan: OfflinePreparationPlan | undefined,
        planSha256: string | null = null;
      if (type === "PLAN") {
        requireValue(
          owner.dispatch.purpose === "prepare" && !planned && !applied && sequence === 1,
        );
        planned = true;
        const paired = await owner.current();
        requireValue(paired.phase === "paired" && paired.planSha256 === null);
        requireValue(
          command.header.pairSha256 === owner.dispatch.pairSha256 &&
            command.header.snapshotSha256 === basis.snapshotSha256 &&
            command.header.settingsSha256 === basis.settingsSha256,
        );
        const work = NodePath.join(owner.state, "preparation-work", owner.sessionId);
        await directory(work);
        database = NodePath.join(work, "state.sqlite");
        await verifyFamily(database, basis.copied);
      } else {
        requireValue(
          type === "APPLY"
            ? owner.dispatch.purpose === "prepare" && planned && !applied && sequence === 2
            : owner.dispatch.purpose === "recover" && !planned && !applied && sequence === 1,
        );
        applied = true;
        const decoded = await durablePlan(owner, command, basis);
        plan = decoded.plan;
        planSha256 = decoded.sha256;
        if (type === "APPLY") await verifyFamily(database, basis.source);
        else {
          const info = await NodeFSP.lstat(database, { bigint: true });
          ownedFile(info, limits.database);
          await absent(database + "-journal");
        }
        await verifyMember(
          NodePath.join(NodePath.dirname(database), "settings.json"),
          basis.source["settings.json"],
          262144,
        );
        settings =
          basis.source["settings.json"] === null
            ? undefined
            : (
                await bytes(NodePath.join(NodePath.dirname(database), "settings.json"), 262144)
              ).toString("utf8");
        requireValue(sha(settings ?? "") === basis.settingsSha256);
      }
      await owner.current();
      const sqlDirectory = NodePath.dirname(database),
        sqlDirectoryInfo = await directory(sqlDirectory);
      const scopeId = NodeCrypto.randomBytes(32).toString("hex"),
        readonly = type === "RECONCILE";
      const uri = NodeURL.pathToFileURL(database);
      uri.searchParams.set("mode", readonly ? "ro" : "rw");
      const sqlLayer = ProjectionStore.layer.pipe(
        Layer.provideMerge(NodeSqliteClient.layer({ filename: uri.href, readonly })),
      );
      let abortSequence: number | undefined;
      const settled = await Effect.runPromise(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* Effect.promise(async () => {
            await channel.send(
              header("SQL_SCOPE_OPENED", {
                scopeId,
                purpose: type === "PLAN" ? "plan" : readonly ? "reconcile" : "apply",
                access: readonly ? "read-only" : "read-write",
              }),
            );
            const response = await channel.receive();
            if (response.header.type === "ABORT") {
              requirePreparationCorrelation(response, owner.sessionId, sequence + 1, "ABORT");
              abortSequence = sequence + 1;
              return;
            }
            requirePreparationCorrelation(response, owner.sessionId, sequence, "CONTINUE", scopeId);
            await owner.current();
            requireValue(sameObject(sqlDirectoryInfo, await directory(sqlDirectory)));
          });
          if (abortSequence !== undefined) return null;
          const sqlite = yield* sql.unsafe<{ version: string; source_id: string }>(
            "SELECT sqlite_version() AS version, sqlite_source_id() AS source_id",
          );
          if (
            sqlite.length !== 1 ||
            sqlite[0]!.version !== owner.preparation.sqliteVersion ||
            sqlite[0]!.source_id !== owner.preparation.sqliteSourceId
          )
            return yield* Effect.die(new WorkerRefused());
          // Query failure is captured inside the SQL scope. Close failures remain outside this Exit.
          return yield* preparationWorkerSql(
            plan === undefined
              ? { kind: "plan", settings }
              : { kind: readonly ? "reconcile" : "apply", settings, plan },
          ).pipe(Effect.exit);
        }).pipe(Effect.provide(sqlLayer)),
        { signal: channel.abort.signal },
      );
      await owner.current();
      requireValue(sameObject(sqlDirectoryInfo, await directory(sqlDirectory)));
      if (abortSequence !== undefined) {
        sequence = abortSequence;
        await channel.send(header("ABORT"));
        return;
      }
      requireValue(settled !== null);
      if (Exit.isFailure(settled)) {
        queryFailed = true;
        await channel.send(
          header("CLOSED_RESULT", {
            scopeId,
            planSha256,
            outcome: "query-failed",
            errorCode: "query-rejected",
          }),
        );
        continue;
      }
      if (settled.value.kind === "plan") {
        const result = createContinuationPlan(settled.value.basis, {
          id: text(command.header.planId),
          timestamp: text(command.header.timestamp),
          snapshotSha256: basis.snapshotSha256,
          settingsSha256: basis.settingsSha256,
          preparationIds: settled.value.basis.selections
            .filter((item) => item.existingId === null)
            .map(() => NodeCrypto.randomUUID()),
        });
        const payload = encodeOfflinePreparationPlan(result);
        await channel.send(
          header("PLAN_READY", { planSha256: sha(payload) }, payload.length),
          payload,
        );
      } else
        await channel.send(
          header("CLOSED_RESULT", {
            scopeId,
            planSha256,
            outcome: settled.value.outcome,
            errorCode: null,
          }),
        );
    }
  } catch (error) {
    primary = error;
    throw error;
  } finally {
    const failures: unknown[] = [];
    try {
      channel.close();
    } catch (error) {
      failures.push(error);
    }
    for (const fd of [roles.directory, roles.lock]) {
      try {
        NodeFS.closeSync(fd);
      } catch (error) {
        failures.push(error);
      }
    }
    // The Python creator retains/removes its exact disposable family only after actual child reaping.
    if (primary === undefined && failures.length > 0) throw new WorkerRefused();
  }
}
