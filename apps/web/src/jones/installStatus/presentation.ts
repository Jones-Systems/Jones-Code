import { installPlanPresentation, startPlan } from "@t3tools/client-runtime/jones/install-plan";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import { targetForDesktop } from "./entrypoints";
import type { InstallStatusSnapshot } from "./panelUi";

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
    state = { ...native, current: startPlan({ planId: snapshot.previewId, origin: "update-button", target,
      from: { version: snapshot.desktop!.currentVersion }, laptopEnvironmentId: environmentId,
      select: snapshot.previewSelection, now: native.laptop.observedAt }) };
  }
  const devices = new Map(snapshot.devices.map((d) => [d.environmentId, d]));
  const presentation = installPlanPresentation(state, { now, devices,
    ...(snapshot.desktop?.jones === undefined ? {} : { laptop: { state: snapshot.desktop.jones, freshness: "fresh" as const } }) });
  return { ...presentation, summary: !presentation.complete && presentation.plan?.devices.every((d) => d.membership === "removed") ? "Laptop only" : presentation.summary, preview, headerDoneVisible: !preview && presentation.headerDoneVisible };
}
