import type { DesktopUpdateActionResult, DesktopUpdateState } from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  canCheckForUpdate,
  getDesktopUpdateActionError,
  getDesktopUpdateButtonTooltip,
  isDesktopUpdateButtonDisabled,
} from "../../components/desktopUpdate.logic";
import { showDesktopUpdateDownloadedToast } from "../../components/desktopUpdate.toast";
import { canDiscardLocalDesktopUpdate, discardLocalDesktopUpdate, downloadLocalDesktopUpdate, getJonesDesktopUpdateBuildUrl, installLocalDesktopUpdate } from "./localDesktopUpdate";

const toast = vi.hoisted(() => ({ add: vi.fn() }));
vi.mock("../../components/ui/toast", () => ({ toastManager: toast }));

const downloaded: DesktopUpdateState = {
  enabled: true,
  status: "downloaded",
  channel: "latest",
  currentVersion: "1.0.0",
  hostArch: "arm64",
  appArch: "arm64",
  runningUnderArm64Translation: false,
  availableVersion: "1.2.0",
  downloadedVersion: "1.1.0",
  releaseNotes: [],
  omittedReleaseCount: 0,
  downloadPercent: 100,
  checkedAt: null,
  message: null,
  errorContext: null,
  canRetry: false,
  jones: {
    source: "jones-actions",
    channel: "jones-main",
    phase: "staged",
    capability: { check: true, download: true, install: true },
    stagedHandle: "downloaded-1.1.0",
  },
};

function result(state = downloaded): DesktopUpdateActionResult {
  return { accepted: true, completed: true, state };
}

describe("Jones local desktop update UI", () => {
  it("forwards the displayed artifact and source for Download", async () => {
    const provenance = {
      repository: "Jones-Systems/Jones-Code", sourceSha: "a".repeat(40), sourceTree: "b".repeat(40),
      workflow: "artifact-desktop-mac.yml", runId: 123, runAttempt: 1, artifactId: 456,
      artifactDigest: "c".repeat(64), platform: "darwin", architecture: "arm64",
    } as const;
    const state: DesktopUpdateState = { ...downloaded, jones: { ...downloaded.jones!, provenance } };
    const bridge = { downloadUpdate: vi.fn().mockResolvedValue(result(state)) };
    await downloadLocalDesktopUpdate(bridge, state);
    expect(bridge.downloadUpdate).toHaveBeenCalledExactlyOnceWith({ artifactId: 456, sourceSha: "a".repeat(40) });
    expect(getJonesDesktopUpdateBuildUrl(state)).toBe("https://github.com/Jones-Systems/Jones-Code/actions/runs/123");
    await expect(downloadLocalDesktopUpdate(bridge, downloaded)).rejects.toThrow("Check for builds");
    expect(bridge.downloadUpdate).toHaveBeenCalledOnce();
  });

  it("keeps upstream Download argument-free", async () => {
    const { jones: _jones, ...state } = downloaded;
    const bridge = { downloadUpdate: vi.fn().mockResolvedValue(result(state)) };
    await downloadLocalDesktopUpdate(bridge, state);
    expect(bridge.downloadUpdate).toHaveBeenCalledExactlyOnceWith();
  });

  it.each(["preparing", "installing"] as const)("does not report accepted %s as an error", (phase) => {
    expect(getDesktopUpdateActionError({
      accepted: true, completed: false,
      state: { ...downloaded, message: "Restart underway", jones: { ...downloaded.jones!, phase } },
    })).toBeNull();
  });

  it("reports a fulfilled failed Download result", () => {
    expect(getDesktopUpdateActionError({
      accepted: true, completed: false,
      state: { ...downloaded, message: "Could not stage", jones: { ...downloaded.jones!, phase: "error" } },
    })).toBe("Could not stage");
  });

  it("discards the displayed handle but refuses an activation in progress", async () => {
    const bridge = { discardUpdate: vi.fn().mockResolvedValue(result()) };
    expect(canDiscardLocalDesktopUpdate(downloaded)).toBe(true);
    await discardLocalDesktopUpdate(bridge, downloaded);
    expect(bridge.discardUpdate).toHaveBeenCalledExactlyOnceWith("downloaded-1.1.0");
    const installing = { ...downloaded, jones: { ...downloaded.jones!, phase: "installing" as const } };
    expect(canDiscardLocalDesktopUpdate(installing)).toBe(false);
    await expect(discardLocalDesktopUpdate(bridge, installing)).rejects.toThrow("cannot be discarded");
    await expect(discardLocalDesktopUpdate({}, downloaded)).rejects.toThrow("cannot be discarded");
    expect(bridge.discardUpdate).toHaveBeenCalledOnce();
  });

  it("installs the downloaded handle even when another version is available", async () => {
    const bridge = { installUpdate: vi.fn().mockResolvedValue(result()) };
    await installLocalDesktopUpdate(bridge, downloaded);
    expect(bridge.installUpdate).toHaveBeenCalledExactlyOnceWith("downloaded-1.1.0");
    expect(isDesktopUpdateButtonDisabled(downloaded)).toBe(false);
  });

  it.each(["preparing", "installing"] as const)(
    "blocks a second Restart while %s",
    async (phase) => {
      const state = {
        ...downloaded,
        jones: { ...downloaded.jones!, phase, updateId: "desktop-update" },
      };
      const bridge = { installUpdate: vi.fn() };
      expect(isDesktopUpdateButtonDisabled(state)).toBe(true);
      expect(getDesktopUpdateButtonTooltip(state)).toContain("outcome appears after restart");
      await expect(installLocalDesktopUpdate(bridge, state)).rejects.toThrow(
        "restarting Jones Code",
      );
      expect(bridge.installUpdate).not.toHaveBeenCalled();
    },
  );

  it.each(["committed", "rolled-back"] as const)(
    "shows the %s Restart outcome without disabling Check",
    (status) => {
      const state: DesktopUpdateState = {
        ...downloaded,
        status: "up-to-date",
        downloadedVersion: null,
        availableVersion: null,
        jones: {
          ...downloaded.jones!,
          phase: status,
          updateId: "desktop-update",
          outcome: {
            status,
            fromVersion: "1.0.0",
            targetVersion: "1.1.0",
            reason: "Native transaction finished.",
          },
        },
      };
      const tooltip = getDesktopUpdateButtonTooltip(state);
      expect(tooltip).toContain("desktop-update");
      expect(tooltip).toContain(status === "committed" ? "committed" : "rolled back");
      expect(tooltip).toContain("1.0.0");
      expect(tooltip).toContain("1.1.0");
      expect(tooltip).toContain("Native transaction finished.");
      expect(isDesktopUpdateButtonDisabled(state)).toBe(false);
      expect(canCheckForUpdate(state)).toBe(true);
      const nextDownload: DesktopUpdateState = {
        ...state,
        status: "available",
        availableVersion: "1.2.0",
      };
      expect(getDesktopUpdateButtonTooltip(nextDownload)).toBe("Update 1.2.0 ready to download");
      expect(isDesktopUpdateButtonDisabled(nextDownload)).toBe(false);
    },
  );

  it("keeps upstream installs argument-free", async () => {
    const { jones: _jones, ...upstream } = downloaded;
    const bridge = { installUpdate: vi.fn().mockResolvedValue(result(upstream)) };
    await installLocalDesktopUpdate(bridge, upstream);
    expect(bridge.installUpdate).toHaveBeenCalledExactlyOnceWith();
  });

  it("blocks restart and explains missing installation setup", async () => {
    const state: DesktopUpdateState = {
      ...downloaded,
      jones: {
        ...downloaded.jones!,
        capability: { check: true, download: true, install: false, reason: "bootstrap-required" },
        message: "Automatic installation requires setup on this Mac.",
      },
    };
    const bridge = { installUpdate: vi.fn() };
    expect(isDesktopUpdateButtonDisabled(state)).toBe(true);
    expect(getDesktopUpdateButtonTooltip(state)).toBe(state.jones!.message);
    await expect(installLocalDesktopUpdate(bridge, state)).rejects.toThrow(state.jones!.message);
    expect(bridge.installUpdate).not.toHaveBeenCalled();
  });

  it("refuses a Jones install without a staged handle", async () => {
    const { stagedHandle: _handle, ...jones } = downloaded.jones!;
    const state = { ...downloaded, jones };
    const bridge = { installUpdate: vi.fn() };
    expect(isDesktopUpdateButtonDisabled(state)).toBe(true);
    expect(getDesktopUpdateButtonTooltip(state)).toContain("Check for updates again");
    await expect(installLocalDesktopUpdate(bridge, state)).rejects.toThrow("no longer ready");
    expect(bridge.installUpdate).not.toHaveBeenCalled();
  });

  it("surfaces a native refusal instead of silently finishing", () => {
    expect(
      getDesktopUpdateActionError({
        accepted: false,
        completed: false,
        state: { ...downloaded, message: "The staged build changed. Download again." },
      }),
    ).toBe("The staged build changed. Download again.");
  });

  it("shows the installation blocker in the downloaded notification", () => {
    toast.add.mockClear();
    showDesktopUpdateDownloadedToast(
      { openExternal: vi.fn() },
      {
        ...downloaded,
        jones: {
          ...downloaded.jones!,
          capability: { check: true, download: true, install: false },
          message: "Automatic installation requires setup on this Mac.",
        },
      },
    );
    const html = renderToStaticMarkup(toast.add.mock.calls[0]![0].description);
    expect(html).toContain("Automatic installation requires setup on this Mac.");
    expect(html).not.toContain("Restart the app from the update button");
  });

  it("does not disable a permitted download just because installation needs setup", () => {
    const state: DesktopUpdateState = {
      ...downloaded,
      status: "available",
      jones: {
        ...downloaded.jones!,
        capability: { check: true, download: true, install: false, reason: "bootstrap-required" },
      },
    };
    expect(isDesktopUpdateButtonDisabled(state)).toBe(false);
  });
});
