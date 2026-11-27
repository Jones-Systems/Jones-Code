import { CommandId, type RunId } from "@t3tools/contracts";
import { findSqliteReaderAdmissionFailure } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import * as Effect from "effect/Effect";

// A run's original task-folder progress remains identifiable even if its tracked list changes.
export const taskFolderProgressId = (runId: RunId) =>
  CommandId.make(`${runId}:task-folder:worktree`);

export const TASK_FOLDER_RECONCILIATION_MESSAGE =
  "Task folder preparation needs reconciliation before it can be retried.";

export const retryWorkspacePreparationRead =
  (commandType: string) =>
  <A, E, R>(read: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
    switch (commandType) {
      case "prepared-run.progress":
      case "prepared-run.release":
      case "prepared-run.fail":
      case "prepared-run.retry":
        // Only repeat the snapshot read, before events or effects are decided.
        return read.pipe(
          Effect.retry({
            while: (cause) => findSqliteReaderAdmissionFailure(cause) !== undefined,
            times: 2,
          }),
        );
      default:
        return read;
    }
  };
