// @effect-diagnostics nodeBuiltinImport:off
// The preparation worker runs outside any Effect runtime. Parent and worker share these
// shapes and bounds; every value crossing the process boundary is decoded, never trusted.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  JonesActionsError,
  type JonesActionsBlockedReason,
  type JonesActionsCandidate,
} from "@t3tools/shared/jones/jonesActions";
import {
  decodeStagedQualifiedRuntime,
  QualifiedRuntimeBlockedError,
  type StagedQualifiedRuntime,
} from "../../cloud/qualifiedRuntime.ts";
import { decodeMigrationPlan, type MigrationPlan } from "../migrationPlan.ts";

export const PREPARATION_PROTOCOL = 1 as const;
/** Hidden first argument of the server's own executable; `bin.ts` dispatches it before the CLI loads. */
export const PREPARATION_COMMAND = "__jones-preparation-worker";
/** One worker per home. SQLite releases it when the holder dies. */
export const PREPARATION_LOCK = "jones-preparation-lock";
export const REQUEST_FILE = "request.json";
export const RESULT_FILE = "result.json";

export const PREPARATION_LIMITS = {
  requestBytes: 64 * 1024,
  resultBytes: 256 * 1024,
  eventLineBytes: 8 * 1024,
  eventStreamBytes: 1024 * 1024,
  stderrBytes: 16 * 1024,
  messageChars: 512,
} as const;

const JOB_ID = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const STAGED_HANDLE = JOB_ID;

/** Job storage lives in the service home, on the same disk as the runtime it prepares. */
export function preparationJobsDirectory(baseDir: string): string {
  return NodePath.join(baseDir, "runtime", "jones-preparation", "jobs");
}

export function isPreparationJobId(value: string): boolean {
  return JOB_ID.test(value);
}

/**
 * Runtime lock name, inside the job directory, that the job's owner holds for the job's whole
 * life. A sweep removes a job directory only after taking this lock, so never a live job's.
 */
export function preparationJobLock(jobId: string): string {
  return NodePath.join("jones-preparation", "jobs", jobId, "owner");
}

export interface PreparationHost {
  readonly platform: "linux" | "darwin";
  readonly architecture: "x64" | "arm64";
}

export type PreparationJob =
  | {
      readonly kind: "stage";
      readonly candidate: JonesActionsCandidate;
      readonly cacheRoot: string;
      readonly dbPath: string;
      readonly activeVersion: string;
      readonly host: PreparationHost;
    }
  | {
      readonly kind: "verify";
      readonly stagedHandle: string;
      readonly activeVersion: string;
      readonly host: PreparationHost;
    }
  | { readonly kind: "sweep"; readonly cacheRoot: string };

export interface PreparationRequest {
  readonly protocol: typeof PREPARATION_PROTOCOL;
  readonly jobId: string;
  readonly baseDir: string;
  /** The worker stops itself after this; the parent enforces its own, later deadline. */
  readonly deadlineMs: number;
  /** Set only by the supervisor, which always starts the worker as a process-group leader. */
  readonly processGroup: boolean;
  readonly job: PreparationJob;
}

export type PreparationPhase = "waiting" | "downloading" | "verifying" | "sweeping";

export type PreparationEvent =
  | {
      readonly type: "ready";
      readonly protocol: number;
      readonly pid: number;
      readonly priority: number | null;
    }
  | { readonly type: "phase" | "heartbeat"; readonly phase: PreparationPhase };

export interface PreparationSweep {
  readonly removed: number;
  readonly retained: number;
  readonly failed: number;
}

export type PreparationOutcome =
  | {
      readonly kind: "stage";
      readonly staged: StagedQualifiedRuntime;
      readonly migrationPlan?: MigrationPlan;
      /** Extraction scratch the worker could not remove; the job directory removal owns it. */
      readonly leftover?: string;
    }
  | { readonly kind: "verify"; readonly staged: StagedQualifiedRuntime }
  | ({ readonly kind: "sweep" } & PreparationSweep);

export type PreparationOutcomeFor<K extends PreparationJob["kind"]> = Extract<
  PreparationOutcome,
  { readonly kind: K }
>;

export interface PreparationFailure {
  readonly name: string;
  readonly message: string;
  readonly reason?: string;
}

export type PreparationResult =
  | {
      readonly protocol: typeof PREPARATION_PROTOCOL;
      readonly jobId: string;
      readonly ok: true;
      readonly outcome: PreparationOutcome;
    }
  | {
      readonly protocol: typeof PREPARATION_PROTOCOL;
      readonly jobId: string;
      readonly ok: false;
      readonly error: PreparationFailure;
    };

const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
const absolute = (value: unknown): value is string =>
  typeof value === "string" && value.length <= 4096 && NodePath.isAbsolute(value);
const count = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

function decodeHost(value: unknown): PreparationHost | undefined {
  const r = record(value);
  return r !== undefined &&
    (r.platform === "linux" || r.platform === "darwin") &&
    (r.architecture === "x64" || r.architecture === "arm64")
    ? { platform: r.platform, architecture: r.architecture }
    : undefined;
}

function decodeJob(value: unknown): PreparationJob | undefined {
  const r = record(value);
  if (r?.kind === "sweep")
    return absolute(r.cacheRoot) ? { kind: "sweep", cacheRoot: r.cacheRoot } : undefined;
  const host = decodeHost(r?.host);
  if (r === undefined || host === undefined || typeof r.activeVersion !== "string")
    return undefined;
  if (r.kind === "verify")
    return typeof r.stagedHandle === "string" && STAGED_HANDLE.test(r.stagedHandle)
      ? { kind: "verify", stagedHandle: r.stagedHandle, activeVersion: r.activeVersion, host }
      : undefined;
  // The worker's Actions client revalidates every candidate field before any network use.
  const candidate = record(r.candidate);
  return r.kind === "stage" &&
    candidate !== undefined &&
    absolute(r.cacheRoot) &&
    absolute(r.dbPath)
    ? {
        kind: "stage",
        candidate: candidate as unknown as JonesActionsCandidate,
        cacheRoot: r.cacheRoot,
        dbPath: r.dbPath,
        activeVersion: r.activeVersion,
        host,
      }
    : undefined;
}

/** A request binds its own job directory, so a worker never writes outside the home it was given. */
export function decodePreparationRequest(
  value: unknown,
  jobDir: string,
): PreparationRequest | undefined {
  const r = record(value);
  const job = decodeJob(r?.job);
  if (
    r === undefined ||
    job === undefined ||
    r.protocol !== PREPARATION_PROTOCOL ||
    typeof r.jobId !== "string" ||
    !JOB_ID.test(r.jobId) ||
    !absolute(r.baseDir) ||
    typeof r.processGroup !== "boolean" ||
    !count(r.deadlineMs) ||
    r.deadlineMs < 1 ||
    r.deadlineMs > 24 * 60 * 60 * 1000 ||
    NodePath.join(preparationJobsDirectory(r.baseDir), r.jobId) !== jobDir
  )
    return undefined;
  return {
    protocol: PREPARATION_PROTOCOL,
    jobId: r.jobId,
    baseDir: r.baseDir,
    deadlineMs: r.deadlineMs,
    processGroup: r.processGroup,
    job,
  };
}

const PHASES: ReadonlySet<string> = new Set(["waiting", "downloading", "verifying", "sweeping"]);

export function decodePreparationEvent(line: string): PreparationEvent | undefined {
  let value: unknown;
  try {
    value = JSON.parse(line);
  } catch {
    return undefined;
  }
  const r = record(value);
  if (r?.type === "ready")
    return count(r.protocol) &&
      count(r.pid) &&
      (r.priority === null || (typeof r.priority === "number" && Number.isSafeInteger(r.priority)))
      ? { type: "ready", protocol: r.protocol, pid: r.pid, priority: r.priority }
      : undefined;
  return (r?.type === "phase" || r?.type === "heartbeat") &&
    typeof r.phase === "string" &&
    PHASES.has(r.phase)
    ? { type: r.type, phase: r.phase as PreparationPhase }
    : undefined;
}

function decodeFailure(value: unknown): PreparationFailure | undefined {
  const r = record(value);
  if (
    r === undefined ||
    typeof r.name !== "string" ||
    r.name.length > 64 ||
    typeof r.message !== "string" ||
    r.message.length > PREPARATION_LIMITS.messageChars ||
    (r.reason !== undefined && (typeof r.reason !== "string" || !/^[a-z-]{1,64}$/.test(r.reason)))
  )
    return undefined;
  return {
    name: r.name,
    message: r.message,
    ...(typeof r.reason === "string" ? { reason: r.reason } : {}),
  };
}

function decodeOutcome(
  value: unknown,
  kind: PreparationJob["kind"],
): PreparationOutcome | undefined {
  const r = record(value);
  if (r === undefined || r.kind !== kind) return undefined;
  if (r.kind === "sweep")
    return count(r.removed) && count(r.retained) && count(r.failed)
      ? { kind: "sweep", removed: r.removed, retained: r.retained, failed: r.failed }
      : undefined;
  const staged = decodeStagedQualifiedRuntime(r.staged);
  if (staged === undefined) return undefined;
  if (r.kind === "verify") return { kind: "verify", staged };
  const migrationPlan =
    r.migrationPlan === undefined ? undefined : decodeMigrationPlan(r.migrationPlan);
  if (
    (r.migrationPlan !== undefined && migrationPlan === undefined) ||
    (r.leftover !== undefined && !absolute(r.leftover))
  )
    return undefined;
  return {
    kind: "stage",
    staged,
    ...(migrationPlan === undefined ? {} : { migrationPlan }),
    ...(typeof r.leftover === "string" ? { leftover: r.leftover } : {}),
  };
}

export function decodePreparationResult(
  value: unknown,
  jobId: string,
  kind: PreparationJob["kind"],
): PreparationResult | undefined {
  const r = record(value);
  if (r?.protocol !== PREPARATION_PROTOCOL || r.jobId !== jobId) return undefined;
  if (r.ok === true) {
    const outcome = decodeOutcome(r.outcome, kind);
    return outcome === undefined
      ? undefined
      : { protocol: PREPARATION_PROTOCOL, jobId, ok: true, outcome };
  }
  const error = r.ok === false ? decodeFailure(r.error) : undefined;
  return error === undefined
    ? undefined
    : { protocol: PREPARATION_PROTOCOL, jobId, ok: false, error };
}

/** Name, bounded message and blocked reason only; stacks and causes stay in the worker. */
export function describePreparationFailure(error: unknown): PreparationFailure {
  const name = error instanceof Error ? error.name.slice(0, 64) : "Error";
  const message = (
    error instanceof Error ? error.message : "Jones update preparation failed."
  ).slice(0, PREPARATION_LIMITS.messageChars);
  const reason =
    error instanceof QualifiedRuntimeBlockedError || error instanceof JonesActionsError
      ? error.reason
      : undefined;
  return { name, message, ...(reason === undefined ? {} : { reason }) };
}

const QUALIFIED_REASONS: ReadonlySet<string> = new Set([
  "bootstrap-required",
  "binding-mismatch",
  "invalid-artifact",
  "integrity-mismatch",
  "occupied-cache",
  "busy",
]);
const ACTIONS_REASONS: ReadonlySet<string> = new Set([
  "gh-missing",
  "authentication-required",
  "rate-limited",
  "unavailable",
  "unqualified",
  "integrity",
  "occupied-cache",
  "unsupported-platform",
]);

/** Rebuilds the worker's failure as the same error class the in-process path raised. */
export function preparationFailureError(failure: PreparationFailure): Error {
  if (
    failure.name === "QualifiedRuntimeBlockedError" &&
    failure.reason !== undefined &&
    QUALIFIED_REASONS.has(failure.reason)
  )
    return new QualifiedRuntimeBlockedError(
      failure.reason as QualifiedRuntimeBlockedError["reason"],
      failure.message,
    );
  if (
    failure.name === "JonesActionsError" &&
    failure.reason !== undefined &&
    ACTIONS_REASONS.has(failure.reason)
  )
    return new JonesActionsError(failure.reason as JonesActionsBlockedReason);
  const error = new Error(failure.message);
  error.name = failure.name;
  return error;
}

/** Reads an owner-controlled regular file of at most `limit` bytes without following a link. */
export async function readBoundedJson(file: string, limit: number): Promise<unknown> {
  const handle = await NodeFSP.open(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const stat = await handle.stat();
    if (
      !stat.isFile() ||
      stat.size > limit ||
      (stat.mode & 0o022) !== 0 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())
    )
      throw new Error("A preparation file is not a bounded owner-controlled regular file.");
    const bytes = Buffer.alloc(stat.size);
    const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
    if (bytesRead !== bytes.length) throw new Error("A preparation file changed while read.");
    return JSON.parse(bytes.toString("utf8")) as unknown;
  } finally {
    await handle.close();
  }
}

/** Publishes `value` as a complete file: partial write, fsync, rename, then directory fsync. */
export async function writeDurableJson(file: string, value: unknown, limit: number): Promise<void> {
  const bytes = Buffer.from(`${JSON.stringify(value)}\n`, "utf8");
  if (bytes.length > limit) throw new Error("A preparation file exceeds its bound.");
  const partial = `${file}.partial`;
  const handle = await NodeFSP.open(partial, "wx", 0o600);
  try {
    await handle.writeFile(bytes);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await NodeFSP.rename(partial, file);
  const directory = await NodeFSP.open(NodePath.dirname(file), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}
