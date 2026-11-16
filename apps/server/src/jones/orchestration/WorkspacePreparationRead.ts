import { findSqliteReaderAdmissionFailure } from "@t3tools/shared/jones/sqliteReaders/admissionFailure";
import * as Effect from "effect/Effect";

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
