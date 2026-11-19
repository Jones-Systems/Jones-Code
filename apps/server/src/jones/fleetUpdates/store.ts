// @effect-diagnostics nodeBuiltinImport:off
// Durable receipts live outside userdata so a native rollback cannot erase dispatch history.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as Schema from "effect/Schema";
import {
  FleetEnrollment,
  FleetHostOperation,
  FleetOperationId,
  FleetSourceSha,
} from "@t3tools/contracts/jones/fleet-updates";
import { withQualifiedRuntimeLock } from "../cloud/qualifiedRuntime.ts";

const StoredDirective = Schema.Struct({
  directiveId: FleetOperationId,
  enrollmentId: FleetOperationId,
  targetSource: FleetSourceSha,
  targetTree: FleetSourceSha,
  mode: Schema.Literal("manual-review"),
  issuedAt: Schema.String,
  leaderAtIssue: Schema.NullOr(
    Schema.Struct({ committedSource: FleetSourceSha, publishedAt: Schema.String }),
  ),
});

export const DIRECTIVE_HISTORY_LIMIT = 128;

/**
 * What this host knows about following its laptop. The leader is bound to the enrollment that
 * published it, so re-enrolling discards it. The automatic-activation time is host-wide and
 * survives the restart that the activation itself causes.
 */
export const FleetFollowerRecord = Schema.Struct({
  schema: Schema.Literal(1),
  leader: Schema.NullOr(
    Schema.Struct({
      enrollmentId: FleetOperationId,
      committedSource: FleetSourceSha,
      /** Absent on a record written before the laptop sent provenance; it waits for a new one. */
      committedTree: Schema.optionalKey(FleetSourceSha),
      publishedAt: Schema.String,
    }),
  ),
  /** The follower's own operation, reused until it settles so a retry never mints another install. */
  operation: Schema.NullOr(
    Schema.Struct({
      operationId: FleetOperationId,
      targetSource: FleetSourceSha,
      expectedInstalledSource: FleetSourceSha,
    }),
  ),
  lastAutomaticActivationAt: Schema.NullOr(Schema.String),
  directive: Schema.optionalKey(StoredDirective),
  directiveHistory: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({ directiveId: FleetOperationId, prior: Schema.NullOr(StoredDirective) }),
    ).check(Schema.isMaxLength(DIRECTIVE_HISTORY_LIMIT)),
  ),
  releasedDirectives: Schema.optionalKey(
    Schema.Array(FleetOperationId).check(Schema.isMaxLength(DIRECTIVE_HISTORY_LIMIT)),
  ),
  fencedDirectives: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        directiveId: FleetOperationId,
        bindingHash: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
        /** Written with the denial in releasedDirectives. Older fences lack this proof. */
        rollbackDenied: Schema.optionalKey(Schema.Literal(true)),
      }),
    ).check(Schema.isMaxLength(DIRECTIVE_HISTORY_LIMIT)),
  ),
});
export type FleetFollowerRecord = typeof FleetFollowerRecord.Type;

export interface FleetHostStore {
  readonly readEnrollment: () => Promise<FleetEnrollment | null>;
  readonly editEnrollment: (
    change: (old: FleetEnrollment | null) => FleetEnrollment,
  ) => Promise<FleetEnrollment>;
  readonly readOperation: (id: string) => Promise<FleetHostOperation | null>;
  readonly editOperation: (
    id: string,
    change: (old: FleetHostOperation | null) => FleetHostOperation,
  ) => Promise<FleetHostOperation>;
  readonly readFollower: () => Promise<FleetFollowerRecord | null>;
  readonly editFollower: (
    change: (old: FleetFollowerRecord | null) => FleetFollowerRecord,
  ) => Promise<FleetFollowerRecord>;
  readonly transactDirective: <A>(
    change: (
      old: FleetFollowerRecord | null,
      enrollment: FleetEnrollment | null,
    ) => { readonly record: FleetFollowerRecord | null; readonly result: A },
  ) => Promise<A>;
}

const FOLLOWER_BYTES = 64 * 1024;
const isEnrollment = Schema.is(FleetEnrollment);
const isFollower = Schema.is(FleetFollowerRecord);
const isOperation = Schema.is(FleetHostOperation);
const isOperationId = Schema.is(FleetOperationId);

async function syncDirectory(path: string) {
  const directory = await NodeFSP.open(path, "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

export async function ownedDirectory(path: string, privateMode: boolean) {
  await NodeFSP.mkdir(path, { recursive: true, mode: 0o700 });
  const stat = await NodeFSP.lstat(path);
  if (
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    (stat.mode & (privateMode ? 0o077 : 0o022)) !== 0 ||
    (typeof process.getuid === "function" && stat.uid !== process.getuid())
  ) {
    throw new Error("Fleet receipt directory ownership is unknown.");
  }
  await syncDirectory(NodePath.dirname(path));
}

export async function read<T>(
  path: string,
  valid: (value: unknown) => value is T,
  limit = 64 * 1024,
): Promise<T | null> {
  const file = await NodeFSP.open(
    path,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  ).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return null;
    throw error;
  });
  if (file === null) return null;
  try {
    const stat = await file.stat();
    if (
      !stat.isFile() ||
      stat.nlink !== 1 ||
      stat.size > limit ||
      (stat.mode & 0o077) !== 0 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())
    ) {
      throw new Error("Fleet receipt ownership or size is invalid.");
    }
    const bytes = Buffer.alloc(stat.size);
    const { bytesRead } = await file.read(bytes, 0, bytes.length, 0);
    if (bytesRead !== stat.size || (await file.stat()).size !== stat.size)
      throw new Error("Fleet receipt changed during its bounded read.");
    const value: unknown = JSON.parse(bytes.toString("utf8"));
    if (!valid(value))
      throw new Error("Fleet receipt is invalid; it cannot be replaced automatically.");
    return value;
  } finally {
    await file.close();
  }
}

export async function replace(path: string, value: unknown) {
  const temporary = `${path}.${NodeCrypto.randomUUID()}.tmp`;
  const file = await NodeFSP.open(temporary, "wx", 0o600);
  try {
    try {
      await file.writeFile(`${JSON.stringify(value)}\n`);
      await file.sync();
    } finally {
      await file.close();
    }
    await NodeFSP.rename(temporary, path);
    await syncDirectory(NodePath.dirname(path));
  } finally {
    await NodeFSP.unlink(temporary).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
    });
  }
}

/**
 * The runtime lock refuses a second holder in this process instead of waiting for it, so every
 * store for one home queues its own requests before taking the lock.
 */
const queues = new Map<string, Promise<unknown>>();
export function serialized<T>(key: string, operation: () => Promise<T>): Promise<T> {
  const result = (queues.get(key) ?? Promise.resolve()).then(operation, operation);
  const settled = result.then(
    () => undefined,
    () => undefined,
  );
  queues.set(key, settled);
  void settled.then(() => {
    if (queues.get(key) === settled) queues.delete(key);
  });
  return result;
}

export function createFleetHostStore(baseDir: string): FleetHostStore {
  const runtime = NodePath.join(baseDir, "runtime");
  const directory = NodePath.join(runtime, "jones-fleet");
  const enrollment = NodePath.join(directory, "enrollment.json");
  const follower = NodePath.join(directory, "follower.json");
  const operationPath = (id: string) => {
    if (!isOperationId(id)) throw new Error("Invalid fleet operation identifier.");
    return NodePath.join(directory, `${id}.json`);
  };
  const locked = <T>(operation: () => Promise<T>) =>
    serialized(directory, async () => {
      await ownedDirectory(runtime, false);
      await ownedDirectory(directory, true);
      return withQualifiedRuntimeLock(baseDir, "jones-fleet", operation);
    });
  const readFollower = () => read(follower, isFollower, FOLLOWER_BYTES);
  const writeFollower = async (record: FleetFollowerRecord) => {
    if (!isFollower(record) || Buffer.byteLength(JSON.stringify(record)) + 1 > FOLLOWER_BYTES)
      throw new Error("Invalid or oversized fleet follower record.");
    await replace(follower, record);
  };
  return {
    readEnrollment: () => locked(() => read(enrollment, isEnrollment)),
    editEnrollment: (change) =>
      locked(async () => {
        const next = change(await read(enrollment, isEnrollment));
        if (!isEnrollment(next)) throw new Error("Invalid fleet enrollment.");
        await replace(enrollment, next);
        return next;
      }),
    readOperation: (id) => locked(() => read(operationPath(id), isOperation)),
    editOperation: (id, change) =>
      locked(async () => {
        const path = operationPath(id);
        const next = change(await read(path, isOperation));
        if (!isOperation(next) || next.input.operationId !== id)
          throw new Error("Invalid fleet operation.");
        await replace(path, next);
        return next;
      }),
    readFollower: () => locked(readFollower),
    editFollower: (change) =>
      locked(async () => {
        const next = change(await readFollower());
        await writeFollower(next);
        return next;
      }),
    transactDirective: (change) =>
      locked(async () => {
        const current = await readFollower();
        const { record, result } = change(current, await read(enrollment, isEnrollment));
        if (record !== null && record !== current) await writeFollower(record);
        // A previous rename may be visible even if its directory sync failed.
        else await syncDirectory(directory);
        return result;
      }),
  };
}
