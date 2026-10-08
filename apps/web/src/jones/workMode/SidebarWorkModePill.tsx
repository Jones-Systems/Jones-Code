import { useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { CheckIcon, CircleAlertIcon } from "lucide-react";
import { useRef, useState } from "react";

import { usePrimaryEnvironment } from "../../state/environments";
import { serverEnvironment } from "../../state/server";
import { environmentSession } from "../../state/session";
import { useEnvironmentPresentation } from "../../state/presentation";
import { useAtomCommand } from "../../state/use-atom-command";
import { cn } from "../../lib/utils";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";

const EMPTY_INITIAL_CONFIG = Atom.make(null);

const DESCRIPTION =
  "Experimental: refreshes idle top-level threads after 55 minutes. Can consume usage; cache savings are not guaranteed.";

export function SidebarWorkModePill() {
  const environment = usePrimaryEnvironment();
  return (
    <EnvironmentWorkModePill
      key={environment?.environmentId ?? "unavailable"}
      environment={environment}
    />
  );
}

function EnvironmentWorkModePill({
  environment,
}: {
  readonly environment: ReturnType<typeof usePrimaryEnvironment>;
}) {
  const { isReady } = useEnvironmentPresentation(environment?.environmentId ?? null);
  const initialConfig = useAtomValue(
    environment
      ? environmentSession.initialConfigValueAtom(environment.environmentId)
      : EMPTY_INITIAL_CONFIG,
  );
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings, {
    label: "change Work mode",
  });
  const [pending, setPending] = useState(false);
  const pendingRef = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const config = environment?.serverConfig;
  const capabilities = config?.environment.capabilities;
  const supported =
    capabilities?.workMode === true && typeof config?.settings.workModeEnabled === "boolean";
  const enabled = config?.settings.workModeEnabled === true;
  const unavailable =
    !isReady ||
    !environment ||
    !environment.entry.enabled ||
    environment.connection.phase !== "connected"
      ? "Connect the primary environment to change Work mode."
      : !initialConfig || !config
        ? "Wait for this environment's settings to load."
        : !supported
          ? "Work mode is unavailable on this server."
          : null;
  const disabled = unavailable !== null || pending;
  const targetLabel = environment?.label ?? "Primary environment";
  const status = pending ? "Saving…" : (error ?? unavailable);

  async function toggle() {
    if (disabled || !environment || pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    setError(null);
    try {
      const result = await updateSettings({
        environmentId: environment.environmentId,
        input: { patch: { workModeEnabled: !enabled } },
      });
      if (result._tag !== "Success" || result.value.workModeEnabled !== !enabled) {
        setError("Could not save Work mode. Reconnect to confirm the saved state before retrying.");
      }
    } catch {
      setError("Could not save Work mode. Reconnect to confirm the saved state before retrying.");
    } finally {
      pendingRef.current = false;
      setPending(false);
    }
  }

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span className="inline-flex shrink-0">
            <button
              type="button"
              role="switch"
              aria-label={`Work mode for ${targetLabel}`}
              aria-checked={enabled}
              aria-busy={pending}
              disabled={disabled}
              onClick={() => void toggle()}
              className={cn(
                "relative z-10 inline-flex h-6 shrink-0 items-center gap-1 rounded-full px-2 text-xs font-medium outline-hidden ring-ring transition-colors focus-visible:ring-2 disabled:opacity-50 [-webkit-app-region:no-drag]",
                enabled
                  ? "bg-success/20 text-success-foreground ring-1 ring-success/30"
                  : "bg-muted/30 text-muted-foreground hover:bg-muted/60",
                error && "text-destructive",
              )}
            >
              {error ? (
                <CircleAlertIcon aria-hidden="true" className="size-3" />
              ) : enabled ? (
                <CheckIcon aria-hidden="true" className="size-3" />
              ) : null}
              <span>{pending ? "Work…" : "Work"}</span>
            </button>
          </span>
        }
      />
      <TooltipPopup side="bottom">
        <div className="max-w-64 space-y-1">
          <p>
            {targetLabel} · Work mode {enabled ? "on" : "off"}
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
