import {
  installPlanPresentation,
  startPlan,
  type StatusPollHealth,
} from "@t3tools/client-runtime/jones/install-plan";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import { targetForDesktop } from "./entrypoints";
import type { InstallStatusDevice, InstallStatusSnapshot } from "./panelUi";
import { lastHeardLabel, updateCheckFailedLabel } from "./copy";

export function panelPresentation(snapshot: InstallStatusSnapshot, now: number) {
  const native = snapshot.native;
  if (native === null) return null;
  let state: JonesInstallPlanState = snapshot.showLast ? { ...native, current: null } : native;
  let preview = false;
  if (state.current === null && !snapshot.showLast) {
    const target = targetForDesktop(snapshot.desktop);
    const environmentId = native.laptop.environmentId;
    if (target === undefined || environmentId === undefined) return null;
    preview = true;
    state = {
      ...native,
      current: startPlan({
        planId: snapshot.previewId,
        origin: "update-button",
        target,
        from: { version: snapshot.desktop!.currentVersion },
        laptopEnvironmentId: environmentId,
        select: snapshot.previewSelection,
        now: native.laptop.observedAt,
      }),
    };
  }
  const devices = new Map(snapshot.devices.map((d) => [d.environmentId, d]));
  const presentation = installPlanPresentation(state, {
    now,
    devices,
    // The desktop main process pushes Laptop's own state over local IPC; no network read can lag.
    ...(snapshot.desktop?.jones === undefined
      ? {}
      : { laptop: { state: snapshot.desktop.jones, freshness: "fresh" as const } }),
  });
  return {
    ...presentation,
    summary:
      !presentation.complete && presentation.plan?.devices.every((d) => d.membership === "removed")
        ? "Laptop only"
        : presentation.summary,
    preview,
    headerDoneVisible: !preview && presentation.headerDoneVisible,
  };
}

/** A device heard within this window shows no indicator. */
const LIVE_WITHIN_MS = 3 * 60_000;
/** Past this silence, or after this many failed refreshes in a row, a device is unreachable. */
const UNREACHABLE_AFTER_MS = 15 * 60_000;
const UNREACHABLE_AFTER_FAILURES = 3;

export type DeviceLiveness =
  | { readonly kind: "live" }
  | { readonly kind: "quiet"; readonly text: string }
  | { readonly kind: "unreachable"; readonly text: string };

/**
 * How recently this client heard from a device. The update status long-poll heartbeat, a fleet
 * status read and a successful automatic refresh all count as hearing from it.
 */
export function deviceLiveness(
  device: Pick<InstallStatusDevice, "observation" | "lastObservedAt"> | undefined,
  health: StatusPollHealth | undefined,
  now: number,
): DeviceLiveness {
  if (device === undefined) return { kind: "live" };
  const { freshness } = device.observation;
  if (freshness === "unsupported" || freshness === "loading") return { kind: "live" };
  const observedAt =
    device.lastObservedAt === undefined ? Number.NaN : Date.parse(device.lastObservedAt);
  const heard = [device.observation.heardAt, observedAt, health?.lastSuccessAt].filter(
    (at): at is number => at !== undefined && Number.isFinite(at),
  );
  const heardAt = heard.length === 0 ? undefined : Math.max(...heard);
  const age = heardAt === undefined ? undefined : now - heardAt;
  if (
    (health?.failures ?? 0) >= UNREACHABLE_AFTER_FAILURES ||
    (age === undefined && freshness !== "fresh") ||
    (age !== undefined && age > UNREACHABLE_AFTER_MS)
  )
    return { kind: "unreachable", text: `Can't reach · ${lastHeardLabel(heardAt, now)}` };
  if (age === undefined || age < LIVE_WITHIN_MS) return { kind: "live" };
  return { kind: "quiet", text: lastHeardLabel(heardAt, now) };
}

type PresentedRow = NonNullable<ReturnType<typeof panelPresentation>>["rows"][number];

/**
 * The host's last package-check result, only while it can matter: no operation is active, nothing
 * is staged, and the device is not already running the target. A failure is dated.
 */
export function packageCheckLine(
  row: PresentedRow | undefined,
  device: Pick<InstallStatusDevice, "observation"> | undefined,
  active: boolean,
  now: number,
) {
  const check = row?.status?.check;
  if (check === undefined || row === undefined || active) return undefined;
  if (row.phase === "staged" || device?.observation.state?.stagedHandle !== undefined)
    return undefined;
  if (row.targetBuildRunning || row.phase === "done" || row.phase === "updated-elsewhere")
    return undefined;
  return check.kind === "failure" ? updateCheckFailedLabel(check.checkedAt, now) : check.text;
}
