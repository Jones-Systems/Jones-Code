import { randomUUID } from "../../lib/utils";
import type { DesktopUpdateState, EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlanState,
  JonesInstallPlanSelection,
} from "@t3tools/contracts/jones/installPlan";
import type { InstallPlanDeviceObservation } from "@t3tools/client-runtime/jones/install-plan";

export interface InstallStatusDevice extends InstallPlanDeviceObservation {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly lastObservedAt?: string;
}
export interface InstallStatusSnapshot {
  readonly updatedAt: number;
  readonly previewId: string;
  readonly previewTargetSource: string | null;
  readonly native: JonesInstallPlanState | null;
  readonly desktop: DesktopUpdateState | null;
  readonly devices: readonly InstallStatusDevice[];
  readonly open: boolean;
  readonly collapsed: boolean;
  readonly showLast: boolean;
  readonly previewSelection: readonly JonesInstallPlanSelection[];
  readonly error: string | null;
}

export function createInstallStatusStore() {
  let snapshot: InstallStatusSnapshot = {
    updatedAt: Date.now(),
    previewId: randomUUID(),
    previewTargetSource: null,
    native: null,
    desktop: null,
    devices: [],
    open: false,
    collapsed: false,
    showLast: false,
    previewSelection: [],
    error: null,
  };
  const listeners = new Set<() => void>();
  const collapsedByPlan = new Map<string, boolean>();
  return {
    get: () => snapshot,
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    update(patch: Partial<InstallStatusSnapshot>) {
      const incoming = patch.native?.current;
      const current = snapshot.native?.current;
      const dismissed = snapshot.native?.last;
      const stale =
        incoming != null &&
        ((current?.planId === incoming.planId && current.revision > incoming.revision) ||
          (dismissed?.planId === incoming.planId && dismissed.dismissedAt !== undefined) ||
          (current != null &&
            current.planId !== incoming.planId &&
            Date.parse(current.createdAt) > Date.parse(incoming.createdAt)));
      snapshot = {
        ...snapshot,
        ...patch,
        updatedAt: Date.now(),
        ...(stale ? { native: snapshot.native } : {}),
      };
      for (const listener of listeners) listener();
    },
    open(showLast = false) {
      const id = (showLast ? snapshot.native?.last : snapshot.native?.current)?.planId ?? "preview";
      this.update({ open: true, showLast, collapsed: collapsedByPlan.get(id) ?? false });
    },
    collapse(collapsed: boolean) {
      const id =
        (snapshot.showLast ? snapshot.native?.last : snapshot.native?.current)?.planId ?? "preview";
      collapsedByPlan.set(id, collapsed);
      this.update({ collapsed });
    },
  };
}
export type InstallStatusStore = ReturnType<typeof createInstallStatusStore>;
export const installStatusStore = createInstallStatusStore();
