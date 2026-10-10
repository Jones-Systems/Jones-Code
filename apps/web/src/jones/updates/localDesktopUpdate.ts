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

export function installLocalDesktopUpdate(
  bridge: Pick<DesktopBridge, "installUpdate">,
  state: DesktopUpdateState | null,
): Promise<DesktopUpdateActionResult> {
  const blocked = getJonesDesktopUpdateBlockedMessage(state, "install");
  if (blocked) return Promise.reject(new Error(blocked));
  return state?.jones ? bridge.installUpdate(state.jones.stagedHandle) : bridge.installUpdate();
}

export function getJonesDesktopUpdateRefusal(result: DesktopUpdateActionResult): string | null {
  if (result.accepted || !result.state.jones) return null;
  return result.state.jones.message ?? result.state.message ?? "The update could not be applied.";
}
