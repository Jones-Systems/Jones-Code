// @effect-diagnostics nodeBuiltinImport:off globalDate:off
// The updater runs on the Promise boundary; this record is written beside the capability receipt.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeCrypto from "node:crypto";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  JonesUpdateProgress,
  type JonesUpdateProgressStep,
  type JonesUpdateState,
} from "@t3tools/contracts/jones/jonesUpdates";

type ProgressPhase = JonesUpdateProgress["phase"];
type CatalogStep = { readonly key: string; readonly label: string; readonly phase: ProgressPhase };

/**
 * The server-declared step catalogs. Each install step maps to one step the qualified install
 * already reports (`cloud/selfUpdate.ts`); `restart` starts when the launcher accepts the handoff
 * and `confirm-restart` is settled by the next server from the launcher's startup outcome.
 */
export const DOWNLOAD_PROGRESS_STEPS: ReadonlyArray<CatalogStep> = [
  { key: "download", label: "Downloading the selected build", phase: "download" },
  { key: "verify-stage", label: "Verifying and staging the download", phase: "verify" },
];
export const INSTALL_PROGRESS_STEPS: ReadonlyArray<CatalogStep> = [
  { key: "mark-threads", label: "Marking running threads for continuation", phase: "prep" },
  { key: "verify-runtime", label: "Verifying the selected runtime", phase: "prep" },
  { key: "startup-readiness", label: "Checking startup readiness", phase: "prep" },
  { key: "capture-continuity", label: "Capturing update continuity", phase: "prep" },
  { key: "launcher-handoff", label: "Handing off to the native launcher", phase: "install" },
  { key: "restart", label: "Restarting into the new runtime", phase: "install" },
  { key: "confirm-restart", label: "Confirming the runtime after restart", phase: "verify" },
];

/** The step names the qualified install reports, mapped to catalog keys. */
export const INSTALL_REPORTED_STEP = {
  marking: "mark-threads",
  verifying: "verify-runtime",
  preflight: "startup-readiness",
  capturing: "capture-continuity",
  handoff: "launcher-handoff",
} as const;
export type InstallReportedStep = keyof typeof INSTALL_REPORTED_STEP;

const MAX_MESSAGE = 240;
const MAX_RECORD_BYTES = 4096;

function catalog(kind: JonesUpdateProgress["kind"], continueRunningThreads: boolean) {
  if (kind === "download") return DOWNLOAD_PROGRESS_STEPS;
  return continueRunningThreads
    ? INSTALL_PROGRESS_STEPS
    : INSTALL_PROGRESS_STEPS.filter((step) => step.key !== "mark-threads");
}

function phaseOf(kind: JonesUpdateProgress["kind"], key: string): ProgressPhase {
  return (
    (kind === "download" ? DOWNLOAD_PROGRESS_STEPS : INSTALL_PROGRESS_STEPS).find(
      (step) => step.key === key,
    )?.phase ?? "prep"
  );
}

export function startProgress(
  kind: JonesUpdateProgress["kind"],
  now: string,
  options: { readonly operationId?: string; readonly continueRunningThreads?: boolean } = {},
): JonesUpdateProgress {
  const steps = catalog(kind, options.continueRunningThreads === true);
  const first = steps[0]!;
  return {
    schema: 1,
    kind,
    ...(options.operationId === undefined ? {} : { operationId: options.operationId }),
    phase: first.phase,
    stepIndex: 1,
    stepCount: steps.length,
    stepKey: first.key,
    stepLabel: first.label,
    stepStartedAt: now,
    startedAt: now,
    updatedAt: now,
    reportedAt: now,
    steps: steps.map((step, index) => ({
      key: step.key,
      label: step.label,
      ...(index === 0 ? { startedAt: now } : {}),
    })),
  };
}

function terminal(progress: JonesUpdateProgress | undefined): boolean {
  return progress === undefined || progress.phase === "done" || progress.phase === "failed";
}

/** Ends the current step and starts `key`. Unknown, repeated or backward steps change nothing. */
export function advanceProgress(
  progress: JonesUpdateProgress | undefined,
  key: string,
  now: string,
): JonesUpdateProgress | undefined {
  if (progress === undefined || terminal(progress)) return progress;
  const steps = progress.steps ?? [];
  const index = steps.findIndex((step) => step.key === key);
  if (index < 0 || index + 1 <= progress.stepIndex) return progress;
  return {
    ...progress,
    phase: phaseOf(progress.kind, key),
    stepIndex: index + 1,
    stepKey: key,
    stepLabel: steps[index]!.label,
    stepStartedAt: now,
    updatedAt: now,
    reportedAt: now,
    steps: steps.map((step, position): JonesUpdateProgressStep => {
      if (position === progress.stepIndex - 1 && step.endedAt === undefined)
        return { ...step, endedAt: now };
      if (position === index) return { key: step.key, label: step.label, startedAt: now };
      return step;
    }),
  };
}

export function withOperationId(
  progress: JonesUpdateProgress | undefined,
  operationId: string | undefined,
): JonesUpdateProgress | undefined {
  if (progress === undefined || operationId === undefined || progress.operationId !== undefined)
    return progress;
  return { ...progress, operationId: operationId.slice(0, 128) };
}

/** Finishes the record at its last step. */
export function completeProgress(
  progress: JonesUpdateProgress | undefined,
  now: string,
): JonesUpdateProgress | undefined {
  if (progress === undefined || terminal(progress)) return progress;
  const steps = progress.steps ?? [];
  const last = steps.at(-1);
  const advanced =
    last === undefined || last.key === progress.stepKey
      ? progress
      : advanceProgress(progress, last.key, now)!;
  return {
    ...advanced,
    phase: "done",
    updatedAt: now,
    reportedAt: now,
    steps: (advanced.steps ?? []).map((step) =>
      step.startedAt !== undefined && step.endedAt === undefined ? { ...step, endedAt: now } : step,
    ),
  };
}

export function progressErrorCode(message: string, fallback: string): string {
  const prefix = /^([a-z][a-z0-9-]{0,63}):\s/.exec(message)?.[1];
  return prefix ?? fallback;
}

/** Marks the current step failed. A finished record is never reopened. */
export function failProgress(
  progress: JonesUpdateProgress | undefined,
  now: string,
  message: string,
  code: string = progressErrorCode(message, `${progress?.kind ?? "update"}-failed`),
): JonesUpdateProgress | undefined {
  if (progress === undefined || terminal(progress)) return progress;
  return {
    ...progress,
    phase: "failed",
    updatedAt: now,
    reportedAt: now,
    error: { code, message: message.slice(0, MAX_MESSAGE) },
    steps: (progress.steps ?? []).map((step, position) =>
      position === progress.stepIndex - 1 && step.endedAt === undefined
        ? { ...step, endedAt: now }
        : step,
    ),
  };
}

/** Stamps a served state's record with the host clock at response time. */
export function stampProgressReportedAt<S extends JonesUpdateState | null>(
  state: S,
  nowMs: number,
): S {
  if (state === null || state.progress === undefined) return state;
  return { ...state, progress: { ...state.progress, reportedAt: new Date(nowMs).toISOString() } };
}

export interface ProgressStartupOutcome {
  readonly status: "committed" | "rolled-back" | "failed";
  readonly id: string;
  readonly reason?: string;
}

/**
 * Settles a record left by the previous server. Any unfinished record belongs to a process that
 * has exited; an install record is finished from the launcher's outcome for the same operation.
 */
export function reconcileProgressAtStartup(
  record: JonesUpdateProgress | undefined,
  outcome: ProgressStartupOutcome | undefined,
  nowMs: number,
): JonesUpdateProgress | undefined {
  if (record === undefined || terminal(record)) return record;
  const now = new Date(nowMs).toISOString();
  const matched =
    record.kind === "install" &&
    outcome !== undefined &&
    (record.operationId === undefined || record.operationId === outcome.id);
  if (!matched) {
    const restarting = record.stepKey === "restart";
    return failProgress(
      record,
      now,
      restarting
        ? "The server restarted, but the launcher reported no outcome for this install."
        : "The server restarted before this step finished.",
      restarting ? "outcome-unavailable" : "interrupted",
    );
  }
  const confirming = advanceProgress(record, "confirm-restart", now) ?? record;
  if (outcome.status === "committed") return completeProgress(confirming, now);
  return failProgress(
    confirming,
    now,
    outcome.reason ??
      (outcome.status === "rolled-back"
        ? "The launcher rolled back to the previous runtime."
        : "The launcher could not start the new runtime."),
    outcome.status === "rolled-back" ? "rolled-back" : "launcher-failed",
  );
}

const decodeProgress = Schema.decodeUnknownOption(JonesUpdateProgress);

function progressPath(baseDir: string) {
  return NodePath.join(baseDir, "runtime", "jones-update-progress.json");
}

function encodeBounded(progress: JonesUpdateProgress): string | undefined {
  const full = `${JSON.stringify(progress)}\n`;
  if (new TextEncoder().encode(full).byteLength <= MAX_RECORD_BYTES) return full;
  const { steps: _steps, ...compact } = progress;
  const smaller = `${JSON.stringify(compact)}\n`;
  return new TextEncoder().encode(smaller).byteLength <= MAX_RECORD_BYTES ? smaller : undefined;
}

/** Atomically replaces the record: owner-only scratch, fsync, rename. Oversized records are skipped. */
export async function publishJonesUpdateProgress(
  baseDir: string,
  progress: JonesUpdateProgress,
): Promise<void> {
  const body = encodeBounded(progress);
  if (body === undefined) return;
  const directory = NodePath.join(baseDir, "runtime");
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  const scratch = NodePath.join(directory, `.jones-progress-${NodeCrypto.randomUUID()}`);
  const handle = await NodeFSP.open(scratch, "wx", 0o600);
  try {
    await handle.writeFile(body);
    await handle.sync();
    await handle.close();
    await NodeFSP.rename(scratch, progressPath(baseDir));
  } finally {
    await handle.close();
    await NodeFSP.rm(scratch, { force: true });
  }
}

/** Reads an owner-only, bounded, valid record; anything else reads as absent. */
export async function readJonesUpdateProgress(
  baseDir: string,
): Promise<JonesUpdateProgress | undefined> {
  const handle = await NodeFSP.open(
    progressPath(baseDir),
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  ).catch(() => undefined);
  if (handle === undefined) return undefined;
  try {
    const stat = await handle.stat();
    if (
      !stat.isFile() ||
      stat.uid !== NodeOS.userInfo().uid ||
      (stat.mode & 0o077) !== 0 ||
      stat.size > MAX_RECORD_BYTES
    )
      return undefined;
    const parsed: unknown = JSON.parse(await handle.readFile("utf8"));
    const decoded = decodeProgress(parsed);
    return Option.getOrUndefined(decoded);
  } catch {
    return undefined;
  } finally {
    await handle.close();
  }
}

function sameRecord(left: JonesUpdateProgress | undefined, right: JonesUpdateProgress): boolean {
  if (left === undefined) return false;
  const { reportedAt: _left, ...a } = left;
  const { reportedAt: _right, ...b } = right;
  return JSON.stringify(a) === JSON.stringify(b);
}

/**
 * Persists each changed record in order. Writes are best effort: a failed write never fails or
 * delays the update that produced it.
 */
export function createProgressRecorder(
  baseDir: string,
  write: typeof publishJonesUpdateProgress = publishJonesUpdateProgress,
) {
  let last: JonesUpdateProgress | undefined;
  let writes = Promise.resolve();
  return {
    persist(state: Pick<JonesUpdateState, "progress">): void {
      const progress = state.progress;
      if (progress === undefined || sameRecord(last, progress)) return;
      last = progress;
      writes = writes.catch(() => {}).then(() => write(baseDir, progress));
      void writes.catch(() => {});
    },
    /** Settles pending writes; for tests and orderly shutdown. */
    flushed: (): Promise<void> => writes.catch(() => {}),
  };
}
