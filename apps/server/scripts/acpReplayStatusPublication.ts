// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";

let publicationSequence = 0;

/** Publish a whole checkpoint without exposing an in-place truncation to readers. */
export function publishAcpReplayStatus(statusPath: string, status: unknown): void {
  const temporaryPath = `${statusPath}.${process.pid}.${publicationSequence++}.tmp`;
  let ownsTemporary = false;
  let failed = false;
  let publicationFailure: unknown;
  try {
    const descriptor = NodeFS.openSync(temporaryPath, "wx", 0o600);
    ownsTemporary = true;
    try {
      NodeFS.writeFileSync(descriptor, JSON.stringify(status), "utf8");
    } finally {
      NodeFS.closeSync(descriptor);
    }
    NodeFS.renameSync(temporaryPath, statusPath);
  } catch (cause) {
    failed = true;
    publicationFailure = cause;
    throw cause;
  } finally {
    if (ownsTemporary) {
      try {
        NodeFS.rmSync(temporaryPath, { force: true });
      } catch (cleanupFailure) {
        if (failed) {
          throw new AggregateError(
            [publicationFailure, cleanupFailure],
            "ACP replay status publication and cleanup failed.",
            { cause: publicationFailure },
          );
        }
        throw cleanupFailure;
      }
    }
  }
}
