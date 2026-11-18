import type { PersonalWorkstreamsFailure } from "@t3tools/client-runtime/jones/workstreams/personal";
const actions = {
  create: "create the Workstream",
  rename: "rename the Workstream",
  recolor: "change the color",
  snooze: "snooze the Workstream",
  unsnooze: "unsnooze the Workstream",
  lifecycle: "change the status",
  reorder: "reorder Workstreams",
  delete: "delete the Workstream",
  assign: "move this thread",
  unassign: "remove this thread",
};
export function personalWorkstreamFailureText(
  failure: PersonalWorkstreamsFailure,
  environment: string,
) {
  const action = actions[failure.action];
  switch (failure.reason) {
    case "offline":
      return `Couldn't ${action}. ${environment} is offline.`;
    case "read_only":
      return `Couldn't ${action}. You have read-only access to ${environment}.`;
    case "update_required":
      return `Couldn't ${action}. Update ${environment} to use Workstreams.`;
    case "thread_not_found":
      return "Couldn't move this thread. It no longer exists.";
    case "workstream_deleted":
      return "That Workstream was deleted.";
    case "workstream_closed":
      return "That Workstream is closed. Reopen it before moving threads there.";
    case "unsettled_threads":
      return "Settle or move every assigned thread before closing this Workstream.";
    case "closure_unavailable":
      return "Couldn't verify every environment. Connect and update each environment, then try again.";
    case "changed_elsewhere":
      return failure.action === "lifecycle" ||
        failure.action === "snooze" ||
        failure.action === "unsnooze"
        ? "This Workstream changed elsewhere. Try again."
        : "This thread was moved somewhere else. Try again.";
    case "unconfirmed":
      return "Couldn't confirm that change. Check the Workstream and try again.";
    case "failed":
      return `Couldn't ${action}. Try again.`;
  }
}
export const personalEnvironmentStatusText = {
  ready: "Ready",
  offline: "Offline",
  update_required: "Update required",
  read_only: "Read only",
  sync_failed: "Couldn't sync",
} as const;
