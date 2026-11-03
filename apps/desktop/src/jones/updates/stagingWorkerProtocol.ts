import type { JonesActionsBlockedReason } from "@t3tools/shared/jones/jonesActions";

export const JONES_STAGING_WORKER_PROTOCOL = 2;
export interface JonesStagingWorkerFailure {
  readonly name: string;
  readonly message: string;
  readonly code?: string;
  readonly reason?: JonesActionsBlockedReason;
}
export type JonesStagingWorkerMessage =
  | { readonly type: "admit-request"; readonly jobId: string }
  | { readonly type: "phase"; readonly jobId: string; readonly phase: "verifying" }
  | { readonly type: "result"; readonly jobId: string; readonly value: unknown }
  | { readonly type: "error"; readonly jobId: string; readonly error: JonesStagingWorkerFailure };

export class JonesStagingStopped extends Error {
  constructor() {
    super("Download stopped. Nothing was installed.");
    this.name = "JonesStagingStopped";
  }
}
export class JonesStagingExitUnproved extends Error {
  readonly worker: { readonly pid: number; readonly processGroup: number };
  readonly copyAdmitted: boolean;
  constructor(
    worker: { readonly pid: number; readonly processGroup: number },
    copyAdmitted = false,
  ) {
    super(
      copyAdmitted
        ? "The admitted copy did not produce a validated stage."
        : "The preparation worker exit was not confirmed.",
    );
    this.name = "JonesStagingExitUnproved";
    this.worker = worker;
    this.copyAdmitted = copyAdmitted;
  }
}
