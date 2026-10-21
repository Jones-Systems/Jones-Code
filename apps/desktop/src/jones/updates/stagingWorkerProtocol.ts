import type { JonesActionsBlockedReason } from "@t3tools/shared/jones/jonesActions";

export const JONES_STAGING_WORKER_PROTOCOL = 1;

/** Only a name, bounded message, system code and updater reason cross the process boundary. */
export interface JonesStagingWorkerFailure {
  readonly name: string;
  readonly message: string;
  readonly code?: string;
  readonly reason?: JonesActionsBlockedReason;
}

export type JonesStagingWorkerMessage =
  | { readonly type: "phase"; readonly phase: "verifying" }
  | { readonly type: "result"; readonly value: unknown }
  | { readonly type: "error"; readonly error: JonesStagingWorkerFailure };
