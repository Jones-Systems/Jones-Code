import { useNavigate } from "@tanstack/react-router";
import { providerUpdateNotificationKey } from "@t3tools/client-runtime/jones/provider-updates";
import { PROVIDER_DISPLAY_NAMES } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { useEffect, useRef, useState } from "react";
import { Button } from "~/components/ui/button";
import { Spinner } from "~/components/ui/spinner";
import { Tooltip, TooltipPopup, TooltipTrigger } from "~/components/ui/tooltip";
import { stackedThreadToast, toastManager } from "~/components/ui/toast";
import { getProviderUpdateRunOutcome } from "~/components/ProviderUpdateLaunchNotification.logic";
import { useLocalStorage } from "~/hooks/useLocalStorage";
import { providerUpdatePresentationAttempt } from "./providerUpdateController";
import { useJonesProviderUpdates } from "./useProviderUpdates";

const providerUpdateDismissalsSchema = Schema.Struct({ keys: Schema.Array(Schema.String) });

export function ProviderUpdateEnvironmentRows() {
  const updates = useJonesProviderUpdates();
  const navigate = useNavigate();
  return (
    <div className="flex flex-col gap-3">
      {updates.groups.map((group) => {
        const attempt = providerUpdatePresentationAttempt(group.attempt, group.allCandidates);
        const pending = attempt?.phase === "running";
        const reason = !group.connected
          ? "Connect this environment before updating providers."
          : !group.canManage
            ? "This connection cannot manage provider accounts."
            : group.candidates.length === 0
              ? "Review provider settings for manual update instructions."
              : null;
        const retry = attempt && attempt.phase !== "running" && attempt.phase !== "succeeded";
        const targets = attempt?.machine.candidates ?? group.allCandidates;
        return (
          <section
            key={group.environmentId}
            aria-label={`Provider updates on ${group.label}`}
            className="space-y-1"
          >
            <div className="flex items-center justify-between gap-3">
              <span className="font-medium">{group.label}</span>
              <Tooltip>
                <TooltipTrigger
                  render={
                    <span tabIndex={reason ? 0 : undefined} aria-label={reason ?? undefined} />
                  }
                >
                  <Button
                    size="xs"
                    variant="outline"
                    disabled={pending || reason !== null}
                    onClick={() => {
                      void updates.updateEnvironment(group.environmentId);
                    }}
                  >
                    {pending ? <Spinner size="sm" /> : retry ? "Retry" : "Update"}
                  </Button>
                </TooltipTrigger>
                <TooltipPopup>
                  {pending
                    ? "Provider updates are still running."
                    : (reason ?? "Update providers on this environment.")}
                </TooltipPopup>
              </Tooltip>
            </div>
            {attempt ? (
              <p
                role="status"
                className={
                  attempt.phase === "failed"
                    ? "text-destructive"
                    : attempt.phase === "unchanged" || attempt.phase === "interrupted"
                      ? "text-warning"
                      : "text-muted-foreground"
                }
              >
                {pending
                  ? `${attempt.runs.length}/${targets.length} provider updates finished`
                  : attempt.message}
              </p>
            ) : null}
            {reason ? <p className="text-xs text-muted-foreground">{reason}</p> : null}
            <ul className="space-y-1 text-xs">
              {targets.map((target) => {
                const run = attempt?.runs.find((entry) => entry.instanceId === target.instanceId);
                const outcome = run ? getProviderUpdateRunOutcome(run) : null;
                return (
                  <li key={target.instanceId}>
                    {PROVIDER_DISPLAY_NAMES[target.driver] ?? target.driver} · {target.instanceId}
                    {outcome
                      ? ` · ${outcome.status === "unchanged" ? "update could not be verified" : outcome.status}`
                      : pending
                        ? " · updating"
                        : ""}
                  </li>
                );
              })}
            </ul>
            {group.allCandidates.some(
              (candidate) =>
                !candidate.versionAdvisory.canUpdate ||
                candidate.versionAdvisory.updateCommand === null,
            ) ? (
              <p className="text-xs text-muted-foreground">
                Update manually with the app or tool that installed this provider. Review provider
                settings for details.
              </p>
            ) : null}
            <Button
              size="xs"
              variant="ghost-muted"
              onClick={() => {
                void navigate({
                  to: "/settings/providers",
                  search: { machine: group.environmentId },
                });
              }}
            >
              Settings
            </Button>
          </section>
        );
      })}
    </div>
  );
}

export function ProviderUpdateNotice() {
  const { noticeKey, groups, isAnySettling } = useJonesProviderUpdates();
  const [dismissed, setDismissed] = useLocalStorage(
    "t3code:provider-update-dismissals:v1",
    { keys: [] as string[] },
    providerUpdateDismissalsSchema,
  );
  const [settleGrace, setSettleGrace] = useState({ settling: isAnySettling, elapsed: false });
  if (settleGrace.settling !== isAnySettling) {
    setSettleGrace({ settling: isAnySettling, elapsed: false });
  }
  const settleGraceElapsed = settleGrace.settling === isAnySettling && settleGrace.elapsed;
  // oxlint-disable-next-line t3code/react-effect-budget -- Own the settling grace timer and cancel it when the update group stops settling.
  useEffect(() => {
    if (!isAnySettling) return;
    const timer = setTimeout(() => setSettleGrace({ settling: true, elapsed: true }), 30_000);
    return () => clearTimeout(timer);
  }, [isAnySettling]);
  const primaryOnly = groups.filter((group) => group.allCandidates.length > 0);
  const legacyPrimaryKey =
    primaryOnly.length === 1 && primaryOnly[0]!.isPrimary
      ? providerUpdateNotificationKey(primaryOnly[0]!.allCandidates)
      : null;
  const active = useRef<{ key: string; id: string } | null>(null);
  const navigate = useNavigate();
  // oxlint-disable-next-line t3code/react-effect-budget -- Close the imperative update toast when its mounted notice owner retires.
  useEffect(
    () => () => {
      if (active.current) toastManager.close(active.current.id);
      active.current = null;
    },
    [],
  );
  // oxlint-disable-next-line t3code/react-effect-budget -- Synchronize the imperative toast manager with committed update groups and dismissal preferences.
  useEffect(() => {
    if (
      (isAnySettling && !settleGraceElapsed) ||
      !noticeKey ||
      dismissed.keys.includes(noticeKey) ||
      (legacyPrimaryKey !== null && dismissed.keys.includes(legacyPrimaryKey)) ||
      active.current?.key === noticeKey
    )
      return;
    if (active.current && groups.some((group) => group.attempt?.phase === "running")) return;
    if (active.current) toastManager.close(active.current.id);
    const key = noticeKey;
    const id = toastManager.add(
      stackedThreadToast({
        type: "warning",
        title: "Provider updates available",
        timeout: 0,
        description: <ProviderUpdateEnvironmentRows />,
        actionProps: {
          children: "Settings",
          onClick: () => {
            void navigate({ to: "/settings/providers" });
          },
        },
        data: {
          hideCopyButton: true,
          onClose: () => {
            setDismissed((previous) => ({
              keys: [...previous.keys.filter((entry) => entry !== key), key],
            }));
            if (active.current?.id === id) active.current = null;
          },
        },
      }),
    );
    active.current = { key, id };
  }, [
    noticeKey,
    dismissed,
    groups,
    navigate,
    setDismissed,
    isAnySettling,
    settleGraceElapsed,
    legacyPrimaryKey,
  ]);
  return null;
}
