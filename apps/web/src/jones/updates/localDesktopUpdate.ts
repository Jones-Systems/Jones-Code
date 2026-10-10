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
