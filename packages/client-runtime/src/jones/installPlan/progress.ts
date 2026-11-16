import type { JonesUpdateProgress } from "@t3tools/contracts/jones/jonesUpdates";

export type InstallProgressStepStatus = "done" | "current" | "pending" | "failed";

export interface InstallProgressStepView {
  readonly key: string;
  readonly label: string;
  readonly status: InstallProgressStepStatus;
  /** Host-clock duration of a finished step. */
  readonly durationMs?: number;
}

export interface InstallProgressView {
  readonly kind: JonesUpdateProgress["kind"];
  readonly phase: JonesUpdateProgress["phase"];
  readonly operationId?: string;
  /** A step is running, so the row shows a live timer. */
  readonly active: boolean;
  /** The current step restarts the host, so silence is expected for a while. */
  readonly restarting: boolean;
  /** One-line row text without the timer; absent when the ledger text says more. */
  readonly text?: string;
  readonly stepIndex: number;
  readonly stepCount: number;
  readonly stepKey: string;
  readonly stepLabel: string;
  /** Host-clock epoch milliseconds. */
  readonly stepStartedAt: number;
  readonly updatedAt: number;
  /** Host clock minus client clock, in milliseconds; zero when the receive time is unknown. */
  readonly skewMs: number;
  readonly steps: readonly InstallProgressStepView[];
  readonly error?: { readonly code: string; readonly message: string };
}

/**
 * The host's progress record, only when it describes the ledger's latest operation for this
 * device. Without a plan (a preview), any record the host reports is shown.
 */
export function progressForOperation(
  progress: JonesUpdateProgress | undefined,
  latestOperationId: string | undefined,
  planless: boolean,
): JonesUpdateProgress | undefined {
  if (progress === undefined) return undefined;
  if (planless) return progress;
  return latestOperationId !== undefined && progress.operationId === latestOperationId
    ? progress
    : undefined;
}

/**
 * Host clock minus client clock. `reportedAt` is the host time the status read was served and
 * `receivedAt` the client time it arrived, so their difference cancels clock skew.
 */
export function progressClockSkewMs(progress: JonesUpdateProgress, receivedAt: number | undefined) {
  const reported = Date.parse(progress.reportedAt);
  return receivedAt === undefined || !Number.isFinite(reported) ? 0 : reported - receivedAt;
}

/** Host time elapsed since a host timestamp, read from the client clock corrected by skew. */
export function hostElapsedMs(sinceHostMs: number, nowClientMs: number, skewMs: number) {
  return Number.isFinite(sinceHostMs) ? Math.max(0, nowClientMs + skewMs - sinceHostMs) : 0;
}

const ACTIVE_PHASES = new Set(["prep", "download", "install", "verify"]);

function progressText(progress: JonesUpdateProgress): string | undefined {
  const of = `${progress.stepIndex} of ${progress.stepCount}`;
  switch (progress.phase) {
    case "prep":
      return `Preparing ${of}`;
    case "download":
      return "Downloading";
    case "install":
      return progress.stepKey === "restart" ? "Restarting" : "Installing";
    case "verify":
      return progress.kind === "download" ? "Checking download" : "Confirming";
    case "done":
      // A finished download keeps the ledger's staged text, which says what to do next.
      return progress.kind === "install" ? "Updated · verified" : undefined;
    case "failed":
      return `${progress.kind === "install" ? "Install" : "Download"} failed at step ${of}`;
  }
}

function stepStatus(progress: JonesUpdateProgress, index: number): InstallProgressStepStatus {
  if (progress.phase === "done" || index < progress.stepIndex) return "done";
  if (index > progress.stepIndex) return "pending";
  return progress.phase === "failed" ? "failed" : "current";
}

export function installProgressView(
  progress: JonesUpdateProgress,
  receivedAt: number | undefined,
): InstallProgressView {
  const active = ACTIVE_PHASES.has(progress.phase);
  const text = progressText(progress);
  return {
    kind: progress.kind,
    phase: progress.phase,
    ...(progress.operationId === undefined ? {} : { operationId: progress.operationId }),
    active,
    restarting: active && progress.stepKey === "restart",
    ...(text === undefined ? {} : { text }),
    stepIndex: progress.stepIndex,
    stepCount: progress.stepCount,
    stepKey: progress.stepKey,
    stepLabel: progress.stepLabel,
    stepStartedAt: Date.parse(progress.stepStartedAt),
    updatedAt: Date.parse(progress.updatedAt),
    skewMs: progressClockSkewMs(progress, receivedAt),
    steps: (progress.steps ?? []).map((step, offset) => {
      const status = stepStatus(progress, offset + 1);
      const started = step.startedAt === undefined ? Number.NaN : Date.parse(step.startedAt);
      const ended = step.endedAt === undefined ? Number.NaN : Date.parse(step.endedAt);
      return {
        key: step.key,
        label: step.label,
        status,
        ...(status === "done" && Number.isFinite(started) && Number.isFinite(ended)
          ? { durationMs: Math.max(0, ended - started) }
          : {}),
      };
    }),
    ...(progress.error === undefined ? {} : { error: progress.error }),
  };
}
