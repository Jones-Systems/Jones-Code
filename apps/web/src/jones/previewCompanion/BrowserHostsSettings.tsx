import { useAtomValue } from "@effect/atom-react";
import {
  type DesktopCompanionConfigureInput,
  type DesktopCompanionState,
  type EnvironmentId,
} from "@t3tools/contracts";
import { InfoIcon } from "lucide-react";
import { useState } from "react";
import { useEnvironments } from "../../state/environments";
import { useSettingsScope } from "../../components/settings/SettingsScopeContext";
import { SettingsRow } from "../../components/settings/settingsLayout";
import { Button } from "../../components/ui/button";
import { DraftInput } from "../../components/ui/draft-input";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectPopup,
  SelectItem,
} from "../../components/ui/select";
import { Switch } from "../../components/ui/switch";
import { toastManager } from "../../components/ui/toast";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { companionStateAtom } from "./state.ts";
import { BrowserHostSelect } from "./HostControls.tsx";

const HOSTS_DESCRIPTION = "Where new preview tabs run. Open tabs keep their host.";

const STATUS_LABELS: Record<DesktopCompanionState["status"], string> = {
  disabled: "Off",
  awaiting_ticket: "Waiting for pairing",
  connecting: "Connecting…",
  online: "Online",
  reconnecting: "Reconnecting…",
  auth_required: "Sign-in required",
  unavailable: "Unavailable",
  unsupported: "Not supported by this environment",
  superseded: "Replaced by another connection",
  restart_required: "Restart required",
};

export function BrowserHostsSettings() {
  const { environments } = useSettingsScope();
  return (
    <>
      {environments.length === 1 ? (
        <SettingsRow
          data-browser-host-settings
          title="Browser host"
          description={HOSTS_DESCRIPTION}
          control={<BrowserHostSelect environmentId={environments[0]!.environmentId} />}
        />
      ) : environments.length > 1 ? (
        <SettingsRow
          data-browser-host-settings
          title="Browser hosts"
          description={HOSTS_DESCRIPTION}
        >
          <div className="mt-2 mb-2 overflow-hidden rounded-lg border border-border/60">
            {environments.map((environment, index) => (
              <div
                key={environment.environmentId}
                className={
                  index > 0
                    ? "flex items-center gap-3 border-t border-border/60 px-3 py-2"
                    : "flex items-center gap-3 px-3 py-2"
                }
              >
                <span className="min-w-0 flex-1 truncate text-sm text-foreground">
                  {environment.label}
                </span>
                <BrowserHostSelect environmentId={environment.environmentId} />
              </div>
            ))}
          </div>
        </SettingsRow>
      ) : null}
      <DesktopBrowserHostSettings />
    </>
  );
}

// Configuration rows appear only while hosting is on; browser-only stays reachable while set.
function DesktopBrowserHostSettings() {
  const state = useAtomValue(companionStateAtom);
  const { environments } = useEnvironments();
  const [pending, setPending] = useState(false);
  if (!state) return null;
  const configure = async (change: Partial<DesktopCompanionConfigureInput>) => {
    const bridge = window.desktopBridge?.previewCompanion;
    if (!bridge) return;
    setPending(true);
    await (async () => {
      await bridge.configure({
        enabled: state.config.enabled,
        environmentId: state.config.environmentId,
        label: state.config.label,
        browserOnly: state.config.browserOnly,
        ...change,
      });
    })()
      .catch(() => {
        toastManager.add({ type: "error", title: "Could not update browser host" });
      })
      .finally(() => {
        setPending(false);
      });
  };
  const { enabled, environmentId, browserOnly } = state.config;
  const needsEnvironment = !enabled && !environmentId;
  const enableSwitch = (
    <Switch
      aria-label="Use this desktop as a browser host"
      checked={enabled}
      disabled={pending || needsEnvironment}
      onCheckedChange={(checked) => void configure({ enabled: Boolean(checked) })}
    />
  );

  return (
    <>
      <SettingsRow
        title="Use this desktop as a browser host"
        description="Let the chosen environment open preview tabs on this desktop."
        status={
          enabled || state.status !== "disabled" ? (
            <span className="inline-flex items-center gap-2">
              <span>{STATUS_LABELS[state.status]}</span>
              {enabled && state.status !== "online" ? (
                <Button
                  size="xs"
                  variant="outline"
                  disabled={pending}
                  onClick={() =>
                    void window.desktopBridge?.previewCompanion?.retry().catch(() => undefined)
                  }
                >
                  Retry connection
                </Button>
              ) : null}
            </span>
          ) : undefined
        }
        control={
          <>
            <Select
              disabled={pending}
              value={environmentId ?? "unpaired"}
              onValueChange={(value) => {
                if (value)
                  void configure({
                    environmentId: value === "unpaired" ? null : (value as EnvironmentId),
                    ...(value === "unpaired" ? { enabled: false } : {}),
                  });
              }}
            >
              <SelectTrigger
                size="sm"
                className="w-full sm:w-44"
                aria-label="Browser host environment"
              >
                <SelectValue>
                  {environments.find((environment) => environment.environmentId === environmentId)
                    ?.label ?? "Choose environment"}
                </SelectValue>
              </SelectTrigger>
              <SelectPopup align="end">
                <SelectItem value="unpaired">Unpaired</SelectItem>
                {environments
                  .filter((environment) => environment.entry.enabled)
                  .map((environment) => (
                    <SelectItem value={environment.environmentId} key={environment.environmentId}>
                      {environment.label}
                    </SelectItem>
                  ))}
              </SelectPopup>
            </Select>
            {needsEnvironment ? (
              <Tooltip>
                <TooltipTrigger render={<span tabIndex={0} className="inline-flex rounded-full" />}>
                  {enableSwitch}
                </TooltipTrigger>
                <TooltipPopup side="top">Choose an environment first.</TooltipPopup>
              </Tooltip>
            ) : (
              enableSwitch
            )}
          </>
        }
      />
      {enabled ? (
        <SettingsRow
          title="Host name"
          description="How this desktop appears in browser host pickers."
          control={
            <DraftInput
              nativeInput
              size="sm"
              className="w-full sm:w-44"
              aria-label="Browser host label"
              maxLength={64}
              disabled={pending}
              value={state.config.label}
              onCommit={(next) => {
                const label = next.trim();
                if (label && label !== state.config.label) void configure({ label });
              }}
            />
          }
        />
      ) : null}
      {enabled || browserOnly ? (
        <SettingsRow
          title={
            <span className="inline-flex items-center gap-1">
              Browser-only desktop
              <Tooltip>
                <TooltipTrigger
                  delay={200}
                  render={
                    <Button
                      size="icon-micro"
                      variant="ghost-muted"
                      aria-label="Browser-only desktop details"
                    >
                      <InfoIcon className="size-3.5" />
                    </Button>
                  }
                />
                <TooltipPopup side="top">
                  Keeps the local backend off, even while disconnected or unpaired. Downloads,
                  uploads, recording, profile clearing and pop-up sign-in are unsupported.
                </TooltipPopup>
              </Tooltip>
            </span>
          }
          description={
            state.browserOnlyLocked
              ? "Fixed for this app."
              : "Run only the browser host here. Takes effect after restart."
          }
          control={
            <Switch
              aria-label="Browser-only desktop"
              checked={browserOnly}
              disabled={pending || state.browserOnlyLocked === true}
              onCheckedChange={(checked) => void configure({ browserOnly: Boolean(checked) })}
            />
          }
        />
      ) : null}
    </>
  );
}
