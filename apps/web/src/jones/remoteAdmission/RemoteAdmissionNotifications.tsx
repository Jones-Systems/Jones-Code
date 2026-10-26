import { useEffect, useRef } from "react";
import { useNavigate } from "@tanstack/react-router";
import type { EnvironmentId } from "@t3tools/contracts";
import { toastManager } from "../../components/ui/toast";
import { useClientSettings } from "../../hooks/useSettings";
import { useEnvironments } from "../../state/environments";
import { hasDesktopNotifications } from "../../threadNotifications";
import { admissionBindingKey, actionableAdmissions, requestBlockedReason } from "./store";
import { groupAdmissions } from "./presentation";
import { RequestReviewRow } from "./RequestReviewRow";
import { useAdmission } from "./useAdmission";

export function RemoteAdmissionNotifications() {
  const { environments } = useEnvironments();
  return environments.map((environment) => (
    <EnvironmentNotifications
      key={environment.environmentId}
      environmentId={environment.environmentId}
      label={environment.label}
    />
  ));
}

function EnvironmentNotifications({
  environmentId,
  label,
}: {
  readonly environmentId: EnvironmentId;
  readonly label: string;
}) {
  const state = useAdmission(environmentId);
  const { snapshot, loadFailed, error, store } = state;
  const inApp = useClientSettings((settings) => settings.inAppNotificationsEnabled);
  const mode = useClientSettings((settings) => settings.notificationMode);
  const navigate = useNavigate();
  const toasts = useRef(new Map<string, string>());
  const dismissed = useRef(new Map<string, string>());
  const osSeen = useRef(new Map<string, string>());
  const native = useRef(new Map<string, Notification>());

  useEffect(() => {
    const currentToasts = toasts.current;
    const currentNative = native.current;
    return () => {
      for (const id of currentToasts.values()) toastManager.close(id);
      for (const notification of currentNative.values()) notification.close();
      currentToasts.clear();
      currentNative.clear();
    };
  }, []);

  useEffect(() => {
    if (!snapshot) return;
    const groups = groupAdmissions(loadFailed ? [] : actionableAdmissions(state));
    const active = new Set<string>();
    for (const category of ["Request review", "Evaluator service failure"]) {
      const matching = groups.filter((group) => group.category === category);
      if (matching.length === 0) continue;
      active.add(category);
      const records = matching.flatMap((group) => group.records);
      const fingerprint = JSON.stringify(records.map(admissionBindingKey).sort());
      const title = `${category} · ${label}`;
      const description =
        category === "Evaluator service failure"
          ? `${records.length} held · Evaluation could not complete. Review or check evaluator settings.`
          : `${records.length} held · Review the evaluator findings before deciding.`;
      const openInbox = () => {
        void navigate({ to: "/requests" });
      };
      if (inApp && !loadFailed && dismissed.current.get(category) !== fingerprint) {
        const options = {
          type: category === "Evaluator service failure" ? "error" : "warning",
          title,
          description,
          timeout: 0,
          actionProps: { children: "Open Requests", onClick: openInbox },
          data: {
            explicitDismissOnly: true,
            hideCopyButton: true,
            onClose: () => {
              dismissed.current.set(category, fingerprint);
              toasts.current.delete(category);
            },
            expandableDescriptionTrigger: true,
            expandableLabels: { expand: "Review requests", collapse: "Collapse requests" },
            expandableContent: (
              <div className="space-y-2">
                {category === "Evaluator service failure" ? (
                  <p className="text-xs">
                    This is an evaluator failure, not a finding of unsafe content.
                  </p>
                ) : null}
                {error ? (
                  <p role="alert" className="text-xs">
                    {error}
                  </p>
                ) : null}
                {records.slice(0, 3).map((record) => (
                  <div key={admissionBindingKey(record)} className="space-y-1">
                    <p className="text-xs text-muted-foreground">
                      {groupAdmissions([record])[0]?.source}
                    </p>
                    <RequestReviewRow
                      environmentId={environmentId}
                      record={record}
                      policyRevision={snapshot.policy.revision}
                      blocked={requestBlockedReason(state, admissionBindingKey(record))}
                      onDecide={(input) => {
                        void store.decide(input);
                      }}
                    />
                  </div>
                ))}
                {records.length > 3 ? (
                  <p className="text-xs">{records.length - 3} more in Requests.</p>
                ) : null}
              </div>
            ),
          },
        };
        const id = toasts.current.get(category);
        if (id) toastManager.update(id, options);
        else toasts.current.set(category, toastManager.add(options));
      } else {
        const id = toasts.current.get(category);
        if (id) toastManager.close(id);
        toasts.current.delete(category);
      }
      if (
        !loadFailed &&
        !state.loading &&
        hasDesktopNotifications(mode) &&
        osSeen.current.get(category) !== fingerprint &&
        (document.visibilityState !== "visible" || !document.hasFocus()) &&
        typeof Notification !== "undefined" &&
        Notification.permission === "granted"
      ) {
        try {
          const notification = new Notification(title, {
            body: description,
            tag: `remote-requests:${environmentId}:${category}`,
            silent: true,
          });
          native.current.get(category)?.close();
          native.current.set(category, notification);
          osSeen.current.set(category, fingerprint);
          notification.addEventListener("click", () => {
            notification.close();
            window.focus();
            void window.desktopBridge?.revealWindow?.().catch(() => undefined);
            openInbox();
          });
        } catch {
          osSeen.current.set(category, fingerprint);
        }
      }
    }
    for (const [category, id] of toasts.current) {
      if (!active.has(category)) {
        toastManager.close(id);
        toasts.current.delete(category);
      }
    }
    for (const [category, notification] of native.current) {
      if (!active.has(category) || !hasDesktopNotifications(mode)) {
        notification.close();
        native.current.delete(category);
      }
    }
  }, [state, snapshot, loadFailed, error, store, environmentId, label, inApp, mode, navigate]);
  return null;
}
