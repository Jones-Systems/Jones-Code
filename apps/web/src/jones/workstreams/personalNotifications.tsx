import { useEffect } from "react";
import type {
  PersonalWorkstreamsFailure,
  PersonalWorkstreamsState,
} from "@t3tools/client-runtime/jones/workstreams/personal";
import { toastManager } from "../../components/ui/toast";
import { stackedThreadToast } from "../../components/ui/toastHelpers";
import { useEnvironments } from "../../state/environments";
import { usePersonalWorkstreams } from "../../state/workstreamsPersonal";

const actions = {
  create: "create the Workstream",
  rename: "rename the Workstream",
  recolor: "change the color",
  lifecycle: "change the status",
  reorder: "reorder Workstreams",
  delete: "delete the Workstream",
  assign: "move this thread",
  unassign: "remove this thread",
};
export function personalFailureMessage(failure: PersonalWorkstreamsFailure, environment: string) {
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
    case "changed_elsewhere":
      return "This thread was moved somewhere else. Try again.";
    case "unconfirmed":
      return "Couldn't confirm that change. Check the Workstream and try again.";
    case "failed":
      return `Couldn't ${action}. Try again.`;
  }
}
const toasts = new Map<string, string>();
const dismissed = new Set<string>();
const labels = new Map<string, string>();
function close(key: string) {
  const id = toasts.get(key);
  toasts.delete(key);
  if (id) toastManager.close(id);
  dismissed.delete(key);
}
export function showPersonalFailure(failure: PersonalWorkstreamsFailure) {
  const persistent = ["offline", "read_only", "update_required"].includes(failure.reason);
  const key = persistent
    ? `condition:${failure.environmentId}:${failure.reason}`
    : failure.dedupeKey;
  if (dismissed.has(key)) return;
  const options = stackedThreadToast({
    type: "error",
    title: personalFailureMessage(
      failure,
      labels.get(failure.environmentId ?? "") ?? "This environment",
    ),
    ...(persistent
      ? {
          timeout: 0,
          actionProps: {
            children:
              failure.reason === "update_required"
                ? "Open Workstreams settings"
                : "Open Connections",
            onClick: () => {
              window.location.href =
                failure.reason === "update_required"
                  ? "/settings/jones-code"
                  : "/settings/connections";
            },
          },
        }
      : {}),
    data: {
      explicitDismissOnly: persistent,
      onClose: () => {
        toasts.delete(key);
        if (persistent) dismissed.add(key);
      },
    },
  });
  const id = toasts.get(key);
  if (id) toastManager.update(id, options);
  else toasts.set(key, toastManager.add(options));
}
export function reconcilePersonalConditions(state: PersonalWorkstreamsState) {
  for (const entry of state.environments) {
    if (entry.status !== "sync_failed") continue;
    const key = `condition:${entry.environmentId}:sync_failed`;
    if (dismissed.has(key)) continue;
    const options = stackedThreadToast({
      type: "error",
      title: `Couldn't sync Workstreams with ${labels.get(entry.environmentId) ?? "this environment"}. Check the connection and try again.`,
      timeout: 0,
      actionProps: {
        children: "Open Workstreams settings",
        onClick: () => {
          window.location.href = "/settings/jones-code";
        },
      },
      data: {
        explicitDismissOnly: true,
        onClose: () => {
          toasts.delete(key);
          dismissed.add(key);
        },
      },
    });
    const id = toasts.get(key);
    if (id) toastManager.update(id, options);
    else toasts.set(key, toastManager.add(options));
  }

  for (const key of new Set([...toasts.keys(), ...dismissed])) {
    if (!key.startsWith("condition:")) continue;
    const active = state.environments.some(
      (entry) => key === `condition:${entry.environmentId}:${entry.status}`,
    );
    if (!active) close(key);
  }
}
export function PersonalWorkstreamsNotifications() {
  const { state } = usePersonalWorkstreams();
  const { environments } = useEnvironments();
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Reconcile owned notification conditions after authoritative connection changes.
  useEffect(() => {
    for (const environment of environments)
      labels.set(environment.environmentId, environment.label);
    reconcilePersonalConditions(state);
  }, [state, environments]);
  return null;
}
export function showPinnedUnpinFailure(target: string) {
  const key = `unpin:${target}`;
  const options = stackedThreadToast({
    type: "error",
    title: "Workstream assigned, but the thread could not be unpinned. Try unpinning it again.",
  });
  const id = toasts.get(key);
  if (id) toastManager.update(id, options);
  else toasts.set(key, toastManager.add(options));
}
