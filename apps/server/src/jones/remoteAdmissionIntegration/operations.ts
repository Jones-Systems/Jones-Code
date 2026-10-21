// Cancellation always remains available. Mutation metadata covers new intake
// surfaces automatically; these observations do not start or change work.
const observations = new Set([
  "task_status",
  "t3_thread_read",
  "t3_thread_wait",
  "t3_thread_launch_lookup",
  "t3_thread_configuration",
  "t3_pending_request_list",
  "t3_pending_request_read",
]);
const cancellations = new Set([
  "task_cancel",
  "t3_workflow_stop",
  "t3_queue_cancel",
  "t3_thread_interrupt",
  "t3_remote_admission_cancel",
  "t3_thread_launch_fence",
  "delete_scheduled_task",
]);
export const screensRemoteOperation = (
  operation: string,
  params: Record<string, unknown>,
  readOnly = false,
) =>
  !readOnly &&
  !observations.has(operation) &&
  !cancellations.has(operation) &&
  !(operation === "t3_thread_goal" && ["stop", "pause", "cancel"].includes(String(params.action)));
