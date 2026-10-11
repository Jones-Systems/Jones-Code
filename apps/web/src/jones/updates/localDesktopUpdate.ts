import type {
  DesktopBridge,
  DesktopUpdateActionResult,
  DesktopUpdateState,
} from "@t3tools/contracts";

export function getJonesDesktopUpdateBlockedMessage(
  state: DesktopUpdateState | null,
  action: "download" | "install" | "none",
): string | null {
  const jones = state?.jones;
  if (!jones || action === "none") return null;
  if (action === "install" && (jones.phase === "preparing" || jones.phase === "installing")) {
    return (
      jones.message ??
      `Update ${jones.updateId ?? jones.stagedHandle ?? ""} is restarting Jones Code; its outcome appears after restart.`
    );
  }
  if (jones.capability[action] && (action !== "install" || jones.stagedHandle)) return null;
  if (action === "install" && jones.capability.install && !jones.stagedHandle) {
    return "The downloaded update is no longer ready to install. Check for updates again.";
  }
  return (
    jones.message ??
    state?.message ??
    (jones.capability.reason === "bootstrap-required"
      ? "Automatic installation needs setup on this Mac before you can restart to update."
      : "This update cannot be applied right now.")
  );
}

export function getJonesDesktopUpdateOutcomeMessage(state: DesktopUpdateState): string | null {
  const jones = state.jones;
  if (jones?.phase !== "committed" && jones?.phase !== "rolled-back") return null;
  const outcome = jones.outcome;
  if (outcome?.status === jones.phase) {
    const update = jones.updateId === undefined ? "Update" : `Update ${jones.updateId}`;
    const reason = outcome.reason ? ` ${outcome.reason}` : "";
    return outcome.status === "committed"
      ? `${update} committed: ${outcome.fromVersion} → ${outcome.targetVersion}.${reason}`
      : `${update} rolled back: ${outcome.targetVersion} did not commit; ${outcome.fromVersion} remains active.${reason}`;
  }
  return (
    jones.message ??
    state.message ??
    (jones.phase === "committed"
      ? "The update committed successfully."
      : "The update rolled back; the previous version remains active.")
  );
}

export async function installLocalDesktopUpdate(
  bridge: Pick<DesktopBridge, "installUpdate" | "fleetUpdates">,
  state: DesktopUpdateState | null,
): Promise<DesktopUpdateActionResult> {
  const blocked = getJonesDesktopUpdateBlockedMessage(state, "install");
  if (blocked) return Promise.reject(new Error(blocked));
  if (!state?.jones) return bridge.installUpdate();
  const campaignId = await prepareLocalFleetCampaign(bridge, state);
  return campaignId === undefined
    ? bridge.installUpdate(state.jones.stagedHandle)
    : bridge.installUpdate(state.jones.stagedHandle, campaignId);
}

export function getJonesDesktopUpdateRefusal(result: DesktopUpdateActionResult): string | null {
  if (result.accepted || !result.state.jones) return null;
  return result.state.jones.message ?? result.state.message ?? "The update could not be applied.";
}

export async function downloadLocalDesktopUpdate(
  bridge: Pick<DesktopBridge, "downloadUpdate" | "fleetUpdates">,
  state: DesktopUpdateState | null,
): Promise<DesktopUpdateActionResult> {
  if (!state?.jones) return bridge.downloadUpdate();
  const blocked = getJonesDesktopUpdateBlockedMessage(state, "download");
  if (blocked) return Promise.reject(new Error(blocked));
  const provenance = state.jones.provenance;
  if (!provenance) return Promise.reject(new Error("Check for builds before downloading."));
  const result = await bridge.downloadUpdate({
    artifactId: provenance.artifactId,
    sourceSha: provenance.sourceSha,
  });
  if (result.completed) await prepareLocalFleetCampaign(bridge, result.state);
  return result;
}

export function isJonesDesktopUpdatePending(result: DesktopUpdateActionResult): boolean {
  return (
    result.accepted &&
    !result.completed &&
    result.state.jones !== undefined &&
    ["checking", "downloading", "verifying", "preparing", "installing"].includes(
      result.state.jones.phase,
    )
  );
}

export function getJonesDesktopUpdateBuildUrl(state: DesktopUpdateState): string | null {
  const provenance = state.jones?.provenance;
  return provenance
    ? `https://github.com/Jones-Systems/Jones-Code/actions/runs/${provenance.runId}`
    : null;
}

export function canDiscardLocalDesktopUpdate(state: DesktopUpdateState | null): boolean {
  const jones = state?.jones;
  return (
    jones !== undefined &&
    jones.stagedHandle !== undefined &&
    !["checking", "downloading", "verifying", "preparing", "installing"].includes(jones.phase) &&
    jones.capability.reason !== "blocked" &&
    jones.capability.reason !== "bootstrap-required"
  );
}

export function discardLocalDesktopUpdate(
  bridge: Pick<DesktopBridge, "discardUpdate">,
  state: DesktopUpdateState | null,
): Promise<DesktopUpdateActionResult> {
  if (!bridge.discardUpdate || !canDiscardLocalDesktopUpdate(state) || !state?.jones?.stagedHandle)
    return Promise.reject(new Error("The downloaded selection cannot be discarded right now."));
  return bridge.discardUpdate(state.jones.stagedHandle);
}

async function prepareLocalFleetCampaign(
  bridge: Pick<DesktopBridge, "fleetUpdates">,
  state: DesktopUpdateState,
): Promise<string | undefined> {
  if (!bridge.fleetUpdates) return undefined;
  const fleet = await bridge.fleetUpdates({ action: "read" });
  const stagedHandle = state.jones?.stagedHandle;
  const targetSource = state.jones?.provenance?.sourceSha;
  // Preparing another campaign retires prior members; reselecting that build needs fresh operation IDs.
  const selected = fleet.campaigns.at(-1);
  if (
    selected !== undefined &&
    (selected.phase === "prepared" || selected.phase === "installing") &&
    selected.desktopStagedHandle === stagedHandle &&
    selected.targetSource === targetSource
  )
    return selected.campaignId;
  if (!fleet.enrollments.some((entry) => entry.enabled)) return undefined;
  if (stagedHandle === undefined || targetSource === undefined)
    throw new Error(
      "The staged desktop source is unavailable; enrolled updates cannot be prepared.",
    );
  const campaignId = globalThis.crypto.randomUUID();
  const prepared = await bridge.fleetUpdates({
    action: "prepare",
    input: {
      campaignId,
      targetSource,
      desktopStagedHandle: stagedHandle,
    },
  });
  if (
    !prepared.campaigns.some(
      (campaign) =>
        campaign.campaignId === campaignId &&
        campaign.targetSource === targetSource &&
        campaign.desktopStagedHandle === stagedHandle &&
        campaign.phase === "prepared",
    )
  )
    throw new Error("The enrolled update campaign was not saved.");
  return campaignId;
}
