import { randomUUID } from "../../lib/utils";
import type { DesktopUpdateState, EnvironmentId } from "@t3tools/contracts";
import type {
  JonesInstallPlan,
  JonesInstallPlanState,
  JonesInstallPlanSelection,
} from "@t3tools/contracts/jones/installPlan";
import type {
  InstallPlanDeviceObservation,
  StatusPollHealth,
} from "@t3tools/client-runtime/jones/install-plan";

export interface InstallStatusDevice extends InstallPlanDeviceObservation {
  readonly environmentId: EnvironmentId;
  readonly label: string;
  readonly lastObservedAt?: string;
  readonly directStatus?: true;
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
  readonly historyPlanId: string | null;
  readonly historyPlan: JonesInstallPlan | null;
  readonly historyDocumentRevision: number | undefined;
  readonly nativeRefusal: string | null;
  readonly previewSelection: readonly JonesInstallPlanSelection[];
  readonly error: string | null;
  /** Automatic status refresh results by environment, written by the status poller. */
  readonly pollHealth: Readonly<Partial<Record<EnvironmentId, StatusPollHealth>>>;
}

export function archivedPlanFrom(state: JonesInstallPlanState | null, planId: string) {
  return state?.last?.planId === planId ? state.last : null;
}

export function selectedHistoryPlan(snapshot: InstallStatusSnapshot) {
  if (snapshot.historyPlanId === null) return snapshot.native?.last ?? null;
  const last = archivedPlanFrom(snapshot.native, snapshot.historyPlanId);
  const detail = snapshot.historyPlan;
  return last !== null && (detail === null || last.revision > detail.revision) ? last : detail;
}

export function isOlderNativeState(
  previous: JonesInstallPlanState | null,
  incoming: JonesInstallPlanState,
) {
  if (previous === null) return false;
  if (previous.documentRevision !== undefined || incoming.documentRevision !== undefined) {
    if (incoming.documentRevision === undefined) return true;
    if (previous.documentRevision !== undefined) {
      return incoming.documentRevision < previous.documentRevision;
    }
    return false;
  }
  const { current, last } = previous;
  const next = incoming.current;
  if (incoming.readOnly !== undefined) return false;
  if (next === null && current !== null && incoming.last?.planId !== current.planId) return true;
  if (last !== null) {
    if (next?.planId === last.planId) return true;
    if (incoming.last === null) return true;
    if (
      incoming.last.planId === last.planId
        ? incoming.last.revision < last.revision
        : Date.parse(incoming.last.createdAt) < Date.parse(last.createdAt)
    )
      return true;
  }
  return (
    current !== null &&
    next !== null &&
    (current.planId === next.planId
      ? current.revision > next.revision
      : Date.parse(current.createdAt) > Date.parse(next.createdAt))
  );
}

export function statusTurn(observation: InstallStatusDevice["observation"]) {
  return observation.unsupportedAt ?? observation.heardAt ?? Number.NEGATIVE_INFINITY;
}

function mergeDeviceStatus(previous: InstallStatusDevice | undefined, next: InstallStatusDevice) {
  if (previous === undefined) return next;
  const before = statusTurn(previous.observation);
  const after = statusTurn(next.observation);
  if (
    after < before &&
    (previous.directStatus ||
      next.observation.freshness === "fresh" ||
      next.observation.freshness === "unsupported")
  )
    return {
      ...next,
      observation: previous.observation,
      ...(previous.directStatus ? { directStatus: true as const } : {}),
    };
  return next;
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
    historyPlanId: null,
    historyPlan: null,
    historyDocumentRevision: undefined,
    nativeRefusal: null,
    previewSelection: [],
    error: null,
    pollHealth: {},
  };
  const listeners = new Set<() => void>();
  const collapsedByPlan = new Map<string, boolean>();
  let desk: unknown;
  let historySelection = 0;
  const visiblePlanId = () =>
    (snapshot.showLast
      ? (snapshot.historyPlanId ?? snapshot.native?.last?.planId)
      : snapshot.native?.current?.planId) ?? "preview";
  return {
    get: () => snapshot,
    historySelection: () => historySelection,
    bindDesk(key: unknown) {
      historySelection++;
      if (desk !== undefined && key !== desk) {
        collapsedByPlan.clear();
        this.update({
          native: null,
          nativeRefusal: null,
          devices: [],
          desktop: null,
          showLast: false,
          historyPlanId: null,
          historyPlan: null,
          historyDocumentRevision: undefined,
          previewSelection: [],
          previewTargetSource: null,
          error: null,
        });
      } else if (snapshot.historyPlan !== null) this.update({ historyPlan: null });
      desk = key;
    },
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    update(patch: Partial<InstallStatusSnapshot>) {
      const { native: offered, nativeRefusal, historyPlan: detail, devices, ...rest } = patch;
      const previousState = snapshot.native;
      // A storage/schema refusal can withhold the ledger and its revision. Keep the last known
      // history visible while closing admission until the desk supplies a usable state again.
      const incoming =
        offered?.readOnly !== undefined &&
        offered.documentRevision === undefined &&
        previousState !== null
          ? { ...previousState, laptop: offered.laptop, readOnly: offered.readOnly }
          : offered;
      const withheldLedger = incoming !== offered;
      const stale =
        !withheldLedger && incoming != null && isOlderNativeState(previousState, incoming);
      const olderLaptop =
        !withheldLedger &&
        incoming != null &&
        previousState != null &&
        incoming.documentRevision === previousState.documentRevision &&
        Date.parse(incoming.laptop.observedAt) < Date.parse(previousState.laptop.observedAt);
      const native =
        stale || incoming === undefined
          ? previousState
          : olderLaptop
            ? { ...incoming, laptop: previousState.laptop }
            : incoming;
      const planId = rest.historyPlanId === undefined ? snapshot.historyPlanId : rest.historyPlanId;
      const previous = snapshot.historyPlan?.planId === planId ? snapshot.historyPlan : null;
      const acceptsDetail =
        !stale &&
        (detail === null ||
          (detail !== undefined &&
            detail.planId === planId &&
            detail.revision >= (previous?.revision ?? 0)));
      const historyPlan = acceptsDetail ? (detail ?? null) : previous;
      snapshot = {
        ...snapshot,
        ...rest,
        updatedAt: Date.now(),
        native,
        historyPlan,
        historyDocumentRevision:
          historyPlan === null
            ? undefined
            : acceptsDetail
              ? native?.documentRevision
              : snapshot.historyDocumentRevision,
        ...(!stale && !olderLaptop && nativeRefusal !== undefined ? { nativeRefusal } : {}),
        ...(devices === undefined
          ? {}
          : {
              devices: devices.map((device) =>
                mergeDeviceStatus(
                  snapshot.devices.find((item) => item.environmentId === device.environmentId),
                  device,
                ),
              ),
            }),
      };
      for (const listener of listeners) listener();
      return !stale;
    },
    open(showLast = false, planId = showLast ? snapshot.native?.last?.planId : undefined) {
      historySelection++;
      const id = (showLast ? planId : snapshot.native?.current?.planId) ?? "preview";
      this.update({
        open: true,
        showLast,
        historyPlanId: planId ?? null,
        collapsed: collapsedByPlan.get(id) ?? false,
      });
    },
    collapse(collapsed: boolean) {
      collapsedByPlan.set(visiblePlanId(), collapsed);
      this.update({ collapsed });
    },
  };
}
export type InstallStatusStore = ReturnType<typeof createInstallStatusStore>;
export const installStatusStore = createInstallStatusStore();
