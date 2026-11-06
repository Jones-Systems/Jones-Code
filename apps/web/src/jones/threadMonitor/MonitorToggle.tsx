import { useEffect, useId, useSyncExternalStore, useState } from "react";
import { PanelRightDashedIcon } from "lucide-react";
import { Toggle } from "../../components/ui/toggle";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import {
  readMonitorNativeState,
  setMonitorNativeState,
  subscribeMonitorNativeState,
} from "./nativeState";
const candidates = new Map<string, number>();
const ownershipListeners = new Set<() => void>();
const notifyOwnership = () => {
  for (const listener of ownershipListeners) listener();
};
const readOwner = () =>
  [...candidates].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0]?.[0] ?? null;
const subscribeOwnership = (listener: () => void) => {
  ownershipListeners.add(listener);
  return () => {
    ownershipListeners.delete(listener);
  };
};
export function MonitorToggle({ priority = 20 }: { readonly priority?: number }) {
  const id = useId();
  const owner = useSyncExternalStore(subscribeOwnership, readOwner);
  // oxlint-disable-next-line t3code/react-effect-budget -- Register this mounted toolbar candidate with the shared native-toggle ownership store.
  useEffect(() => {
    candidates.set(id, priority);
    notifyOwnership();
    return () => {
      candidates.delete(id);
      notifyOwnership();
    };
  }, [id, priority]);
  const state = useSyncExternalStore(subscribeMonitorNativeState, readMonitorNativeState);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  if (owner !== id || !window.desktopBridge?.threadMonitor || state === null) return null;
  const retry = state.supported && state.reason !== null;
  const reason =
    error ??
    state.reason ??
    (!state.supported ? "Thread monitor is unavailable on this desktop backend" : null);
  return (
    <Tooltip>
      <TooltipTrigger render={<span className="flex shrink-0" />}>
        <Toggle
          className="shrink-0 [-webkit-app-region:no-drag]"
          variant="ghost"
          size="sm"
          pressed={state.enabled && state.supported && !retry}
          disabled={pending || !state.supported}
          aria-label={
            retry
              ? "Retry thread monitor"
              : state.enabled
                ? "Hide thread monitor"
                : "Show thread monitor"
          }
          data-thread-monitor-toggle
          aria-description={
            reason ?? "Show or hide the floating thread monitor. Runs keep working."
          }
          onPressedChange={() => {
            if (pending || !state.supported) return;
            setPending(true);
            setError(null);
            void window
              .desktopBridge!.threadMonitor!.setEnabled(retry || !state.enabled)
              .then(setMonitorNativeState, () =>
                setError("Could not change thread monitor visibility"),
              )
              .finally(() => setPending(false));
          }}
        >
          <PanelRightDashedIcon className="size-4" />
        </Toggle>
      </TooltipTrigger>
      <TooltipPopup side="bottom">
        {reason ??
          (state.enabled ? "Hide thread monitor · runs keep working" : "Show thread monitor")}
      </TooltipPopup>
    </Tooltip>
  );
}
