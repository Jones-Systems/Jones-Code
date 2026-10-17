// Imported by the detached launcher: keep this boundary free of runtime dependencies.

type QualifiedCommitStep = "pointer-write" | "post-write-liveness" | "committed-message";

export interface QualifiedCommitAttempt {
  readonly updateId: string;
  readonly step: QualifiedCommitStep;
}

const MAX_CAUSE_DEPTH = 4;
const ERROR_CODES = new Set([
  "EACCES",
  "EBADF",
  "EBUSY",
  "EDQUOT",
  "EEXIST",
  "EINTR",
  "EINVAL",
  "EIO",
  "EISDIR",
  "EMFILE",
  "ENFILE",
  "ENOENT",
  "ENOMEM",
  "ENOSPC",
  "ENOTDIR",
  "ENOTEMPTY",
  "EPERM",
  "EPIPE",
  "EROFS",
  "ESRCH",
  "EXDEV",
  "ERR_IPC_CHANNEL_CLOSED",
  "ERR_IPC_DISCONNECTED",
  "ERR_STREAM_DESTROYED",
  "ERR_STREAM_WRITE_AFTER_END",
]);

/** Nested defects can contain credentials or paths; expose only fixed categories and known codes. */
export function describeErrorChain(error: unknown): string {
  const parts: string[] = [];
  let current = error;
  while (current !== undefined && current !== null && parts.length < MAX_CAUSE_DEPTH) {
    if (!(current instanceof Error)) {
      parts.push("non-Error failure (details withheld)");
      break;
    }
    const code = "code" in current ? current.code : undefined;
    parts.push(
      typeof code === "string" && ERROR_CODES.has(code) ? code : "Error (details withheld)",
    );
    current = current.cause;
  }
  return parts.length === 0 ? "unknown failure" : parts.join(" <- caused by: ");
}

/** A previous update's commit cannot forbid a later update's pre-commit rollback. */
export function qualifiedRollbackRefusal(
  attempt: QualifiedCommitAttempt | undefined,
  updateId: string,
  rollbackReason: string,
): string | undefined {
  if (attempt === undefined || attempt.updateId !== updateId) return undefined;
  return (
    `Qualified pointer commit may have occurred (update ${updateId}, step ${attempt.step}); ` +
    `refused rollback for ${rollbackReason}; paired state is retained for reconciliation.`
  );
}

export class QualifiedCommitFailure extends Error {
  constructor(attempt: QualifiedCommitAttempt, cause: unknown) {
    super(
      `Qualified pointer commit failed at step ${attempt.step} (update ${attempt.updateId}): ` +
        `${describeErrorChain(cause)}; paired state is retained for reconciliation.`,
      { cause },
    );
  }
}

/** Preserve the launcher's existing top-level diagnostic without exposing new raw cause text. */
export function launcherFailureMessage(cause: unknown): string {
  if (cause instanceof QualifiedCommitFailure) return cause.message;
  if (!(cause instanceof Error)) return String(cause);
  return cause.cause === undefined
    ? cause.message
    : `${cause.message} <- caused by: ${
        cause.cause instanceof QualifiedCommitFailure
          ? cause.cause.message
          : describeErrorChain(cause.cause)
      }`;
}

/** Child updates leave the service manager's launcher executable unchanged. */
export function launcherDriftNotice(
  launcherVersion: string,
  childVersion: string,
): string | undefined {
  if (launcherVersion === childVersion) return undefined;
  return (
    `[service-launcher] Launcher ${launcherVersion} is supervising server ${childVersion}. ` +
    "Server updates do not replace the launcher; refresh it to the server's version with " +
    "`t3 jones host adopt` (or the host's equivalent unit update) at the next planned restart.\n"
  );
}
