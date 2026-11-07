import { installPreviewBridge } from "./install-preview-bridge";

let startupPhase = "guard";
async function start() {
  if (
    import.meta.env.MODE !== "jones-install-fixture" ||
    !/^[a-f0-9]{40}$/.test(import.meta.env.JONES_INSTALL_FIXTURE_SOURCE_SHA ?? "") ||
    window.location.protocol !== "http:" ||
    !["127.0.0.1", "localhost", "[::1]"].includes(window.location.hostname) ||
    !window.location.pathname.endsWith("/jones-install-fixture.html") ||
    window.desktopBridge !== undefined
  ) {
    throw new Error(
      "Refusing to install a synthetic bridge outside the isolated renderer fixture.",
    );
  }
  const parameters = new URLSearchParams(window.location.search);
  startupPhase = "bridge";
  const fixture = installPreviewBridge(
    parameters.get("run") ?? "",
    parameters.get("scenario") ?? "available",
    import.meta.env.JONES_INSTALL_FIXTURE_SOURCE_SHA,
    parameters.get("installPlans") !== "legacy",
  );
  // env.ts captures bridge presence at module evaluation, before any component renders.
  startupPhase = "renderer-import";
  const { mountInstallPreview } = await import("./InstallPreview");
  startupPhase = "mount";
  await mountInstallPreview(fixture);
}

void start().catch((error: unknown) => {
  const root = document.getElementById("root")!;
  root.dataset.fixtureStartupPhase = startupPhase;
  root.dataset.fixtureStartupError =
    error instanceof TypeError ? "type" : error instanceof ReferenceError ? "reference" : "other";
  root.textContent = `Fixture startup failed: ${error instanceof Error ? error.message : String(error)}`;
});
