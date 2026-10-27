import { useSyncExternalStore } from "react";
import type {
  JonesRecoveryPolicy,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import {
  Dialog,
  DialogPopup,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "../../components/ui/dialog";
import { Button } from "../../components/ui/button";

let pending: {
  preflight: JonesRecoveryPreflight;
  resolve: (policy: JonesRecoveryPolicy | null) => void;
} | null = null;
const listeners = new Set<() => void>();
const publish = () => {
  for (const listener of listeners) listener();
};
export async function chooseRecoveryPolicy(
  preflight: JonesRecoveryPreflight,
): Promise<JonesRecoveryPolicy | null> {
  if (!preflight.firstInstall && preflight.policies.cloneOrCopy.admissible) return "clone-or-copy";
  if (pending !== null) return null;
  return new Promise((resolve) => {
    pending = { preflight, resolve };
    publish();
  });
}
function finish(policy: JonesRecoveryPolicy | null) {
  const p = pending;
  pending = null;
  publish();
  p?.resolve(policy);
}
function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) finish(null);
  };
}
export function RecoveryReview() {
  const choice = useSyncExternalStore(
    subscribe,
    () => pending,
    () => null,
  );
  const preflight = choice?.preflight;
  const clone = preflight?.clone.status === "proved" && preflight.policies.cloneRequired.admissible;
  const first = preflight?.firstInstall === true;
  const policy = preflight?.policies.cloneOrCopy.admissible
    ? ("clone-or-copy" as const)
    : clone
      ? ("clone-required" as const)
      : null;
  return (
    <Dialog
      open={choice !== null}
      onOpenChange={(open) => {
        if (!open) finish(null);
      }}
    >
      <DialogPopup>
        <DialogTitle>
          {first
            ? "First install"
            : clone
              ? "Not enough space for a full backup copy"
              : "Installation cannot continue"}
        </DialogTitle>
        <DialogDescription>
          {first
            ? "There is no existing data to back up."
            : clone
              ? "A clone was proved for this target. Clone the database, WAL, SHM and required settings, or stop this installation. A failed clone never falls back to copying."
              : (preflight?.policies.cloneOrCopy.reason ??
                preflight?.clone.reason ??
                "Recovery could not be proved for this target.")}
        </DialogDescription>
        {preflight !== undefined ? (
          <div className="space-y-1 text-xs text-muted-foreground">
            {preflight.volumes.map((volume) => (
              <p key={volume.role}>
                {volume.role}: {volume.availableBytes.toLocaleString()} bytes available · copy needs{" "}
                {volume.copyRequiredBytes.toLocaleString()} bytes · clone needs{" "}
                {volume.cloneRequiredBytes.toLocaleString()} bytes
              </p>
            ))}
          </div>
        ) : null}
        <DialogFooter>
          <Button variant="outline" onClick={() => finish(null)}>
            Stop
          </Button>
          {first && policy !== null ? (
            <Button onClick={() => finish(policy)}>Continue</Button>
          ) : clone ? (
            <Button onClick={() => finish("clone-required")}>Clone</Button>
          ) : null}
        </DialogFooter>
      </DialogPopup>
    </Dialog>
  );
}
