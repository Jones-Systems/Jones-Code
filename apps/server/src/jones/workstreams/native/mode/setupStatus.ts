import type { WorkstreamSetupStatus } from "@t3tools/contracts/jones/workstreamOperations";
/** Native sources use local admission; the CP enrollment action is never a native next step. */
export function nativeSetupStatus(status: WorkstreamSetupStatus): WorkstreamSetupStatus {
  return {
    ...status,
    next_steps: status.next_steps.filter(
      (step) => step.code !== "enroll_native_source" && step.code !== "configure_activation",
    ),
  };
}
