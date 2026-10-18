import type { CdpRelayPolicy } from "../../preview/CdpRelay.ts";

const pageDomains = new Set([
  "Page",
  "Runtime",
  "DOM",
  "Input",
  "Network",
  "Fetch",
  "Emulation",
  "Log",
  "CSS",
  "Accessibility",
  "Overlay",
  "Performance",
  "Security",
]);
const browserMethods = new Set([
  "Browser.getVersion",
  "Browser.grantPermissions",
  "Browser.resetPermissions",
  "Browser.cancelDownload",
  "Target.setDiscoverTargets",
  "Target.getTargetInfo",
  "Target.getBrowserContexts",
  "Target.getTargets",
  "Target.attachToBrowserTarget",
  "Target.attachToTarget",
  "Target.detachFromTarget",
  "Target.setAutoAttach",
]);
const childTargetMethods = new Set([
  "Target.setAutoAttach",
  "Target.detachFromTarget",
  "Target.getTargetInfo",
]);
const storageMethods = new Set([
  "Storage.clearCookies",
  "Storage.clearDataForOrigin",
  "Storage.clearDataForStorageKey",
]);
const forbiddenMethods = new Set([
  "DOM.setFileInputFiles",
  "DOM.getFileInfo",
  "Input.setInterceptDrags",
  "Page.setDownloadBehavior",
]);

export function isCompanionNavigationAllowed(url: unknown): boolean {
  if (url === "about:blank") return true;
  if (typeof url !== "string") return false;
  try {
    const parsed = new URL(url);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

export const companionCdpPolicy: CdpRelayPolicy = (command, route) => {
  const deny = () => ({
    type: "deny" as const,
    message: `Not supported for a companion preview tab: ${command.method}`,
  });
  if (command.method === "Browser.setDownloadBehavior") {
    return { type: "rewrite", params: { behavior: "deny" } };
  }
  if (forbiddenMethods.has(command.method)) return deny();
  if (command.method === "Page.navigate" && !isCompanionNavigationAllowed(command.params?.url)) {
    return deny();
  }
  if (
    command.method === "Network.loadNetworkResource" &&
    !isCompanionNavigationAllowed(command.params?.url)
  )
    return deny();
  if (
    (command.method === "Fetch.continueRequest" ||
      command.method === "Network.continueInterceptedRequest") &&
    command.params?.url !== undefined &&
    !isCompanionNavigationAllowed(command.params.url)
  )
    return deny();
  if (command.method === "Page.createIsolatedWorld") {
    // CDP's misspelled grantUniveralAccess is the actual protocol field. Clear
    // both spellings so evaluated scripts keep normal origin restrictions.
    const {
      grantUniversalAccess: _correctSpelling,
      grantUniveralAccess: _protocolSpelling,
      ...params
    } = command.params ?? {};
    return { type: "rewrite", params: { ...params, grantUniveralAccess: false } };
  }
  if (command.method === "Input.dispatchDragEvent") {
    const data = command.params?.data;
    if (typeof data !== "object" || data === null) return deny();
    const files = (data as Record<string, unknown>).files;
    if (files !== undefined && (!Array.isArray(files) || files.length > 0)) return deny();
  }
  const domain = command.method.split(".")[0]!;
  if (domain === "Browser" || domain === "Target") {
    // Only the relay can answer browser-wide queries. Never forward those via
    // a page or child session, which would expose the real Electron browser.
    return (route === "browser" ? browserMethods : childTargetMethods).has(command.method)
      ? { type: "allow" }
      : deny();
  }
  if (domain === "Storage") {
    return storageMethods.has(command.method) ? { type: "allow" } : deny();
  }
  return pageDomains.has(domain) ? { type: "allow" } : deny();
};
