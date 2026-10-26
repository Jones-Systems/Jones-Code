import { PROVIDER_DISPLAY_NAMES } from "@t3tools/contracts";
import { settlePromise } from "@t3tools/client-runtime/state/runtime";
import { useRef, useState } from "react";

import { Button } from "./ui/button";

import { Tooltip, TooltipPopup, TooltipTrigger } from "./ui/tooltip";

export { useJonesProviderUpdates as useProviderUpdates } from "../jones/updates/useProviderUpdates";
import { useJonesProviderUpdates as useProviderUpdates } from "../jones/updates/useProviderUpdates";

export function ProviderUpdatesAction() {
  const { machines, updateAll, completed, canManage } = useProviderUpdates();
  const pending = useRef(false);
  const capturedMachines = useRef(machines);
  const [isPending, setIsPending] = useState(false);
  if (machines.length === 0 && !isPending) {
    return null;
  }

  const handleUpdate = async () => {
    if (pending.current) return;
    pending.current = true;
    capturedMachines.current = machines;
    setIsPending(true);
    await settlePromise(() => updateAll());
    pending.current = false;
    setIsPending(false);
  };

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            size="xs"
            variant="ghost-muted"
            disabled={isPending || !canManage}
            onClick={() => void handleUpdate()}
          >
            {isPending ? "Updating…" : "Update all"}
          </Button>
        }
      />
      <TooltipPopup side="top">
        {!canManage ? <p>This connection cannot manage provider accounts.</p> : null}
        {(isPending ? capturedMachines.current : machines).map((machine) => (
          <div key={machine.environmentId}>
            {machine.label}
            {isPending
              ? ` · ${completed.filter((run) => run.environmentId === machine.environmentId).length}/${machine.candidates.length} finished`
              : ""}
            :{" "}
            {machine.candidates
              .map((candidate) => PROVIDER_DISPLAY_NAMES[candidate.driver] ?? candidate.driver)
              .join(", ")}
          </div>
        ))}
      </TooltipPopup>
    </Tooltip>
  );
}
