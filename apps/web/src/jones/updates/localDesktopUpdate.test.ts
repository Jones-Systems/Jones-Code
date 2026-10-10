import type { DesktopUpdateActionResult, DesktopUpdateState } from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  getDesktopUpdateActionError,
  getDesktopUpdateButtonTooltip,
  isDesktopUpdateButtonDisabled,
} from "../../components/desktopUpdate.logic";
import { showDesktopUpdateDownloadedToast } from "../../components/desktopUpdate.toast";
import { installLocalDesktopUpdate } from "./localDesktopUpdate";

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
  it("installs the downloaded handle even when another version is available", async () => {
    const bridge = { installUpdate: vi.fn().mockResolvedValue(result()) };
    await installLocalDesktopUpdate(bridge, downloaded);
    expect(bridge.installUpdate).toHaveBeenCalledExactlyOnceWith("downloaded-1.1.0");
    expect(isDesktopUpdateButtonDisabled(downloaded)).toBe(false);
  });

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
