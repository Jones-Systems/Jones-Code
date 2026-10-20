import { useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { CheckIcon, CircleAlertIcon, MinusIcon } from "lucide-react";
import { useRef, useState } from "react";

import type { EnvironmentId } from "@t3tools/contracts";
import { AuthSettingsWriteScope } from "@t3tools/contracts";

import { useEnvironments } from "../../state/environments";
import { serverEnvironment } from "../../state/server";
import { environmentSession, readEnvironmentScopeAccess } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { cn } from "../../lib/utils";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";

const initialConfigsAtom = Atom.family((ids: string) =>
  Atom.make(
    (get) =>
      new Map(
        ids === ""
          ? []
          : ids
              .split("\n")
              .map((id) => [
                id,
                get(environmentSession.initialConfigValueAtom(id as EnvironmentId)),
              ]),
      ),
  ),
);

const DESCRIPTION =
  "Experimental: refreshes idle top-level threads after 55 minutes. Can consume usage; cache savings are not guaranteed.";

export function SidebarWorkModePill() {
  const { isReady, environments } = useEnvironments();
  const initialConfigs = useAtomValue(
    initialConfigsAtom(environments.map(({ environmentId }) => environmentId).join("\n")),
  );
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings, {
    label: "change Work mode",
    reportFailure: false,
  });
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const [retry, setRetry] = useState<{
    enabled: boolean;
    targets: Array<{ environmentId: EnvironmentId; label: string }>;
  } | null>(null);
  const targets = environments.map((environment) => {
    const config = environment.serverConfig;
    const unavailable = !environment.entry.enabled
      ? "connection disabled"
      : environment.connection.phase !== "connected"
        ? "not connected"
        : !initialConfigs.get(environment.environmentId) || !config
          ? "settings loading"
          : config.environment.capabilities.workMode !== true ||
              typeof config.settings.workModeEnabled !== "boolean"
            ? "Work unavailable on this server"
            : null;
    return { ...environment, unavailable, enabled: config?.settings.workModeEnabled === true };
  });
  const eligible = targets.filter(({ unavailable }) => unavailable === null);
  const enabled = eligible.length > 0 && eligible.every((target) => target.enabled);
  const mixed = eligible.some((target) => target.enabled) && !enabled;
  const nextEnabled = retry?.enabled ?? !enabled;
  const unavailable = !isReady
    ? "Wait for connected environments to load."
    : eligible.length === 0
      ? "Connect an environment that supports Work mode."
      : null;
  const disabled = unavailable !== null || pending;
  const skipped = targets.filter((target) => target.unavailable !== null);
  const limitations = skipped.map((target) => `${target.label}: ${target.unavailable}.`).join(" ");
  const status = pending
    ? "Saving…"
    : [
        error ?? unavailable,
        mixed ? "Work is on for some connected environments." : null,
        limitations,
      ]
        .filter(Boolean)
        .join(" ");

  async function toggle() {
    if (disabled || pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    setError(null);
    const requested =
      retry?.targets ?? eligible.map(({ environmentId, label }) => ({ environmentId, label }));
    const results = await Promise.allSettled(
      requested.map(async ({ environmentId }) => {
        const target = targets.find((candidate) => candidate.environmentId === environmentId);
        if (!target || target.unavailable !== null) return false;
        if (target.enabled === nextEnabled) return true;
        const access = readEnvironmentScopeAccess(environmentId, AuthSettingsWriteScope);
        if (access.status === "scope_missing" || access.status === "signed_out") return false;
        const result = await updateSettings({
          environmentId,
          input: { patch: { workModeEnabled: nextEnabled } },
        });
        return result._tag === "Success" && result.value.workModeEnabled === nextEnabled;
      }),
    );
    const failed = requested.filter(
      (_, index) => results[index]?.status !== "fulfilled" || results[index].value !== true,
    );
    if (failed.length > 0) {
      const saved = requested.filter((target) => !failed.includes(target));
      setError(
        `Could not save Work mode on ${failed.map(({ label }) => label).join(", ")}.${saved.length > 0 ? ` Saved on ${saved.map(({ label }) => label).join(", ")}.` : ""} Reconnect to confirm the saved state before retrying.`,
      );
      // A failed disable must retry off, even though the remaining state is mixed.
      setRetry({ enabled: nextEnabled, targets: failed });
    } else {
      setRetry(null);
    }
    pendingRef.current = false;
    setPending(false);
  }

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span className="inline-flex shrink-0">
            <button
              type="button"
              role="switch"
              aria-label="Work mode for all connected environments"
              aria-checked={enabled}
              aria-busy={pending}
              disabled={disabled}
              onClick={() => void toggle()}
              className={cn(
                "relative z-10 inline-flex h-6 shrink-0 items-center gap-1 rounded-full px-2 text-xs font-medium outline-hidden ring-ring transition-colors focus-visible:ring-2 focus-visible:ring-inset disabled:opacity-50 [-webkit-app-region:no-drag]",
                enabled && skipped.length === 0
                  ? "bg-success/20 text-success-foreground ring-1 ring-success/30"
                  : "bg-muted/30 text-muted-foreground hover:bg-muted/60",
                error && "text-destructive",
              )}
            >
              {error || skipped.length > 0 ? (
                <CircleAlertIcon aria-hidden="true" className="size-3" />
              ) : enabled ? (
                <CheckIcon aria-hidden="true" className="size-3" />
              ) : mixed ? (
                <MinusIcon aria-hidden="true" className="size-3" />
              ) : null}
              <span>{pending ? "Work…" : mixed ? "Work · Some" : "Work"}</span>
            </button>
          </span>
        }
      />
      <TooltipPopup side="bottom">
        <div className="max-w-64 space-y-1">
          <p>All connected environments · Work mode {mixed ? "mixed" : enabled ? "on" : "off"}</p>
          {targets.map((target) => (
            <p key={target.environmentId}>
              {target.label}: {target.unavailable ?? (target.enabled ? "on" : "off")}
            </p>
          ))}
          <p>
            {retry ? "Retry turning" : "Turn"} Work {nextEnabled ? "on" : "off"} for{" "}
            {retry?.targets.length ?? eligible.length} environments.
          </p>
          <p>{DESCRIPTION}</p>
          {status ? <p>{status}</p> : null}
        </div>
      </TooltipPopup>
      <span className="sr-only" role="status">
        {status}
      </span>
    </Tooltip>
  );
}
