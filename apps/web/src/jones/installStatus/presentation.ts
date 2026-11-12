import {
  hostElapsedMs,
  installPlanPresentation,
  startPlan,
  type InstallProgressView,
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
    planless: preview,
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
 * When this client last heard from a device: the update status long-poll heartbeat, a fleet
 * status read or a successful automatic refresh, whichever is latest.
 */
export function deviceHeardAt(
  device: Pick<InstallStatusDevice, "observation" | "lastObservedAt"> | undefined,
  health: StatusPollHealth | undefined,
): number | undefined {
  if (device === undefined) return undefined;
  const observedAt =
    device.lastObservedAt === undefined ? Number.NaN : Date.parse(device.lastObservedAt);
  const heard = [device.observation.heardAt, observedAt, health?.lastSuccessAt].filter(
    (at): at is number => at !== undefined && Number.isFinite(at),
  );
  return heard.length === 0 ? undefined : Math.max(...heard);
}

/**
 * How recently this client heard from a device. Failed automatic refreshes count only while the
 * status stream is also quiet: a refresh's operation read waits in the host's serial operation
 * queue and can time out behind other work while the stream still hears the host.
 */
export function deviceLiveness(
  device: Pick<InstallStatusDevice, "observation" | "lastObservedAt"> | undefined,
  health: StatusPollHealth | undefined,
  now: number,
): DeviceLiveness {
  if (device === undefined) return { kind: "live" };
  const { freshness } = device.observation;
  if (freshness === "unsupported" || freshness === "loading") return { kind: "live" };
  const heardAt = deviceHeardAt(device, health);
  const age = heardAt === undefined ? undefined : now - heardAt;
  const streamHeardAt = device.observation.heardAt;
  const streamLive = streamHeardAt !== undefined && now - streamHeardAt < LIVE_WITHIN_MS;
  if (
    (!streamLive && (health?.failures ?? 0) >= UNREACHABLE_AFTER_FAILURES) ||
    (age === undefined && freshness !== "fresh") ||
    (age !== undefined && age > UNREACHABLE_AFTER_MS)
  )
    return { kind: "unreachable", text: `Can't reach · ${lastHeardLabel(heardAt, now)}` };
  if (age === undefined || age < LIVE_WITHIN_MS) return { kind: "live" };
  return { kind: "quiet", text: lastHeardLabel(heardAt, now) };
}

/** A running step with no host update for this long shows a muted warning. */
const STALLED_AFTER_MS = 3 * 60_000;
/**
 * A restarting host is expected to go silent. The client has no launcher deadline to read, so
 * this default bounds the quiet period before the row says so.
 */
const EXPECTED_RESTART_MS = 10 * 60_000;

export type DeviceAttention =
  | { readonly kind: "none" }
  | { readonly kind: "quiet"; readonly text: string }
  | { readonly kind: "stalled"; readonly text: string }
  | { readonly kind: "unreachable"; readonly text: string }
  | { readonly kind: "restart-silent"; readonly text: string };

/**
 * What the row says about a device's silence or stalled progress. A restart suppresses the
 * liveness flags until the expected restart window ends.
 */
export function deviceAttention(
  progress: InstallProgressView | undefined,
  liveness: DeviceLiveness,
  now: number,
): DeviceAttention {
  if (progress?.restarting) {
    const sinceRestart = hostElapsedMs(progress.stepStartedAt, now, progress.skewMs);
    if (sinceRestart < EXPECTED_RESTART_MS) return { kind: "none" };
    if (liveness.kind !== "live")
      return {
        kind: "restart-silent",
        text: `No word since restart · ${Math.floor(sinceRestart / 60_000)}m`,
      };
  }
  if (liveness.kind !== "live") return liveness;
  if (progress?.active) {
    const idle = hostElapsedMs(progress.updatedAt, now, progress.skewMs);
    if (idle >= STALLED_AFTER_MS)
      return { kind: "stalled", text: `No progress for ${Math.floor(idle / 60_000)}m` };
  }
  return { kind: "none" };
}

type PresentedRow = NonNullable<ReturnType<typeof panelPresentation>>["rows"][number];

/**
 * The host's last package-check result, only while it can matter: no operation is active, nothing
 * is staged, and the device is not already running the target. A failure is dated by the host's
 * check record, or by the older classification time from hosts without one.
 */
export function packageCheckLine(
  row: PresentedRow | undefined,
  device: Pick<InstallStatusDevice, "observation"> | undefined,
  active: boolean,
  now: number,
) {
  const check = row?.status?.check;
  const lastCheck = device?.observation.state?.lastCheck;
  if ((check === undefined && lastCheck === undefined) || row === undefined || active)
    return undefined;
  if (row.phase === "staged" || device?.observation.state?.stagedHandle !== undefined)
    return undefined;
  if (row.targetBuildRunning || row.phase === "done" || row.phase === "updated-elsewhere")
    return undefined;
  if (lastCheck !== undefined)
    return lastCheck.outcome === "failed"
      ? updateCheckFailedLabel(lastCheck.at, now)
      : check?.kind === "failure"
        ? undefined
        : check?.text;
  return check?.kind === "failure" ? updateCheckFailedLabel(check.checkedAt, now) : check?.text;
}
