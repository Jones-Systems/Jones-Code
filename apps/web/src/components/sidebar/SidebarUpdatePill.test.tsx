import type {
  DesktopUpdateActionResult,
  DesktopUpdateCheckResult,
  DesktopUpdateState,
} from "@t3tools/contracts";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  checkForSidebarUpdate,
  handleSidebarUpdateReleaseNotesPopoverOpenChange,
  installLocalUpdate,
  openSidebarUpdateReleaseNotesPopoverOnForwardTab,
  resolveSidebarUpdateCheckFeedback,
  shouldUseSidebarUpdateReleaseNotesPopover,
} from "./SidebarUpdatePill";
import {
  isDesktopUpdateButtonDisabled,
  resolveDesktopUpdateButtonAction,
} from "../desktopUpdate.logic";
import { runUpdateEverything } from "./updateEverything.logic";

const toast = vi.hoisted(() => ({ add: vi.fn() }));
vi.mock("../ui/toast", async () => {
  const { stackedThreadToast } = await import("../ui/toastHelpers");
  return { toastManager: toast, stackedThreadToast };
});

const nightlyState: DesktopUpdateState = {
  enabled: true,
  status: "available",
  channel: "nightly",
  currentVersion: "0.0.35",
  hostArch: "arm64",
  appArch: "arm64",
  runningUnderArm64Translation: false,
  availableVersion: "0.0.36-nightly.3",
  downloadedVersion: null,
  releaseNotes: [{ version: "0.0.36-nightly.3", items: ["Newest change"], totalItems: 1 }],
  omittedReleaseCount: 0,
  downloadPercent: null,
  checkedAt: null,
  message: null,
  errorContext: null,
  canRetry: false,
};

describe("sidebar update release notes popover", () => {
  it("uses the popover only for visible nightly release notes", () => {
    expect(shouldUseSidebarUpdateReleaseNotesPopover(true, nightlyState)).toBe(true);
    expect(shouldUseSidebarUpdateReleaseNotesPopover(false, nightlyState)).toBe(false);
    expect(
      shouldUseSidebarUpdateReleaseNotesPopover(true, {
        ...nightlyState,
        channel: "latest",
      }),
    ).toBe(false);
    expect(
      shouldUseSidebarUpdateReleaseNotesPopover(true, {
        ...nightlyState,
        releaseNotes: [],
      }),
    ).toBe(false);
  });

  it("cancels trigger presses without canceling other open reasons", () => {
    const cancelTriggerPress = vi.fn();
    const cancelHover = vi.fn();

    handleSidebarUpdateReleaseNotesPopoverOpenChange(true, {
      reason: "trigger-press",
      cancel: cancelTriggerPress,
    });
    handleSidebarUpdateReleaseNotesPopoverOpenChange(true, {
      reason: "trigger-hover",
      cancel: cancelHover,
    });

    expect(cancelTriggerPress).toHaveBeenCalledOnce();
    expect(cancelHover).not.toHaveBeenCalled();
  });

  it("promotes forward Tab without preventing native navigation", () => {
    const open = vi.fn();
    const preventDefault = vi.fn();
    const event = { key: "Tab", shiftKey: false, preventDefault };

    openSidebarUpdateReleaseNotesPopoverOnForwardTab(event, { open }, "nightly-release-notes");

    expect(open).toHaveBeenCalledWith("nightly-release-notes");
    expect(preventDefault).not.toHaveBeenCalled();
  });

  it("does not promote backward Tab", () => {
    const open = vi.fn();

    openSidebarUpdateReleaseNotesPopoverOnForwardTab(
      { key: "Tab", shiftKey: true },
      { open },
      "nightly-release-notes",
    );

    expect(open).not.toHaveBeenCalled();
  });
});

function checkedState(overrides: Partial<DesktopUpdateState> = {}): DesktopUpdateState {
  return {
    ...nightlyState,
    status: "up-to-date",
    availableVersion: null,
    jones: {
      source: "jones-actions",
      channel: "jones-main",
      phase: "no-new",
      capability: { check: true, download: false, install: false },
    },
    ...overrides,
  };
}

function jonesState(
  phase: NonNullable<DesktopUpdateState["jones"]>["phase"],
  message?: string,
): DesktopUpdateState {
  const state = checkedState();
  return { ...state, jones: { ...state.jones!, phase, ...(message ? { message } : {}) } };
}

describe("sidebar manual update check feedback", () => {
  beforeEach(() => toast.add.mockClear());

  it.each([
    ["building", "info", "Update still being qualified", "Qualification is running."],
    ["blocked", "error", "Update check blocked", "The source is not qualified."],
  ] as const)(
    "explains a %s result without offering stale candidates",
    async (phase, type, title, message) => {
      const state: DesktopUpdateState = {
        ...jonesState(phase, message),
        status: phase === "blocked" ? "error" : "idle",
        errorContext: "check",
        availableVersion: "99.0.0",
        downloadedVersion: "98.0.0",
      };
      await checkForSidebarUpdate({
        checkForUpdate: vi.fn().mockResolvedValue({ checked: true, state }),
      });
      expect(toast.add).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ type, title, description: message }),
      );
      expect(toast.add.mock.calls[0]![0]).not.toHaveProperty("actionProps");
      expect(resolveDesktopUpdateButtonAction(state)).toBe("none");
    },
  );

  it.each(["Connection lost.", null])(
    "reports a completed error with message %s",
    async (message) => {
      const state = checkedState({ status: "error", message });
      await checkForSidebarUpdate({
        checkForUpdate: vi.fn().mockResolvedValue({ checked: true, state }),
      });
      expect(toast.add).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          type: "error",
          title: "Could not check for updates",
          description: message ?? "Update check failed.",
        }),
      );
    },
  );

  it.each(["no-new", "committed", "rolled-back"] as const)(
    "reports no new update without replaying a retained %s transaction",
    async (phase) => {
      const state = {
        ...jonesState(phase, "Previous transaction finished."),
        message: "Previous outcome.",
      };
      await checkForSidebarUpdate({
        checkForUpdate: vi.fn().mockResolvedValue({ checked: true, state }),
      });
      expect(toast.add).toHaveBeenCalledExactlyOnceWith({
        type: "info",
        title: "No new update available",
        data: { actionLayout: "stacked-end" },
      });
    },
  );

  it.each(["available", "downloaded"] as const)(
    "leaves %s actions available without redundant feedback",
    async (status) => {
      const state = checkedState({
        status,
        availableVersion: "2.0.0",
        downloadedVersion: status === "downloaded" ? "1.5.0" : null,
        jones: {
          ...jonesState(status === "downloaded" ? "staged" : "available").jones!,
          capability: { check: true, download: true, install: true },
          stagedHandle: "staged-1.5.0",
        },
      });
      await checkForSidebarUpdate({
        checkForUpdate: vi.fn().mockResolvedValue({ checked: true, state }),
      });
      expect(toast.add).not.toHaveBeenCalled();
      expect(resolveDesktopUpdateButtonAction(state)).toBe(
        status === "downloaded" ? "install" : "download",
      );
      expect(isDesktopUpdateButtonDisabled(state)).toBe(false);
    },
  );

  it.each(["building", "blocked"] as const)(
    "preserves a staged candidate while the next check reports %s",
    async (phase) => {
      const state: DesktopUpdateState = {
        ...jonesState(phase, "The newer candidate is not qualified."),
        status: "downloaded",
        availableVersion: "2.0.0",
        downloadedVersion: "1.5.0",
        jones: {
          ...jonesState(phase).jones!,
          capability: { check: true, download: false, install: true },
          stagedHandle: "staged-1.5.0",
        },
      };
      await checkForSidebarUpdate({
        checkForUpdate: vi.fn().mockResolvedValue({ checked: true, state }),
      });
      expect(toast.add).not.toHaveBeenCalled();
      expect(resolveDesktopUpdateButtonAction(state)).toBe("install");
      expect(isDesktopUpdateButtonDisabled(state)).toBe(false);
    },
  );

  it.each(["building", "blocked"] as const)(
    "keeps active statuses quiet despite a retained %s phase",
    async (phase) => {
      for (const status of [
        "available",
        "downloaded",
        "downloading",
        "checking",
        "disabled",
      ] as const) {
        await checkForSidebarUpdate({
          checkForUpdate: vi.fn().mockResolvedValue({
            checked: true,
            state: { ...jonesState(phase), status },
          }),
        });
      }
      expect(toast.add).not.toHaveBeenCalled();
    },
  );

  it.each(["checking", "downloading", "idle", "disabled"] as const)(
    "does not announce completion for %s",
    async (status) => {
      await checkForSidebarUpdate({
        checkForUpdate: vi.fn().mockResolvedValue({
          checked: true,
          state: checkedState({ status }),
        }),
      });
      expect(toast.add).not.toHaveBeenCalled();
    },
  );

  it("waits for the manual result and announces its returned state exactly once", async () => {
    let complete!: (result: DesktopUpdateCheckResult) => void;
    const request = new Promise<DesktopUpdateCheckResult>((resolve) => {
      complete = resolve;
    });
    const bridge = { checkForUpdate: vi.fn(() => request) };
    const pending = checkForSidebarUpdate(bridge);
    expect(bridge.checkForUpdate).toHaveBeenCalledOnce();
    expect(toast.add).not.toHaveBeenCalled();
    resolveSidebarUpdateCheckFeedback(jonesState("blocked", "A background result."));
    expect(toast.add).not.toHaveBeenCalled();
    complete({ checked: true, state: jonesState("building", "Returned qualification result.") });
    await pending;
    expect(toast.add).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        title: "Update still being qualified",
        description: "Returned qualification result.",
      }),
    );
  });

  it.each(["Checks are disabled.", null])(
    "preserves unchecked feedback with message %s",
    async (message) => {
      await checkForSidebarUpdate({
        checkForUpdate: vi.fn().mockResolvedValue({
          checked: false,
          state: checkedState({ message }),
        }),
      });
      expect(toast.add).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          type: "error",
          title: "Could not check for updates",
          description: message ?? "Automatic updates are not available in this build.",
        }),
      );
    },
  );

  it.each([new Error("Bridge failed."), "unknown rejection"])(
    "preserves rejected request feedback: %s",
    async (error) => {
      await checkForSidebarUpdate({ checkForUpdate: vi.fn().mockRejectedValue(error) });
      expect(toast.add).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          type: "error",
          title: "Could not check for updates",
          description: error instanceof Error ? error.message : "Update check failed.",
        }),
      );
    },
  );
});

describe("sidebar combined update install", () => {
  beforeEach(() => toast.add.mockClear());

  const staged: DesktopUpdateState = {
    ...nightlyState,
    status: "downloaded",
    downloadedVersion: "0.0.36-nightly.3",
    jones: {
      source: "jones-actions",
      channel: "jones-main",
      phase: "staged",
      capability: { check: true, download: true, install: true },
      stagedHandle: "staged-1",
    },
  };

  function installBridge(
    state: DesktopUpdateState,
    installed: DesktopUpdateActionResult = {
      accepted: true,
      completed: false,
      state: { ...state, jones: { ...state.jones!, phase: "installing" } },
    },
  ) {
    return {
      getUpdateState: vi.fn().mockResolvedValue(state),
      installUpdate: vi.fn().mockResolvedValue(installed),
      openExternal: vi.fn(),
    };
  }

  it("reads the current selection and forwards its staged handle", async () => {
    const bridge = installBridge(staged);
    const reportError = vi.fn();
    expect(await installLocalUpdate(bridge, reportError)).toBe(true);
    expect(bridge.getUpdateState).toHaveBeenCalledOnce();
    expect(bridge.installUpdate).toHaveBeenCalledExactlyOnceWith("staged-1");
    expect(reportError).not.toHaveBeenCalled();
  });

  it("joins a prepared fleet campaign and still installs when fleet bookkeeping fails", async () => {
    const campaign = {
      campaignId: "campaign-1",
      phase: "prepared",
      desktopStagedHandle: "staged-1",
      targetSource: undefined,
    };
    const tracked = {
      ...installBridge(staged),
      fleetUpdates: vi.fn().mockResolvedValue({ campaigns: [campaign], enrollments: [] }),
    };
    expect(await installLocalUpdate(tracked, vi.fn())).toBe(true);
    expect(tracked.installUpdate).toHaveBeenCalledExactlyOnceWith("staged-1", "campaign-1");

    const untracked = {
      ...installBridge(staged),
      fleetUpdates: vi.fn().mockRejectedValue(new Error("Fleet store unavailable.")),
    };
    expect(await installLocalUpdate(untracked, vi.fn())).toBe(true);
    expect(untracked.installUpdate).toHaveBeenCalledExactlyOnceWith("staged-1");
  });

  it("reports a refused install instead of announcing a download", async () => {
    const refused = {
      ...staged,
      jones: { ...staged.jones!, message: "Startup gate unavailable." },
    };
    const bridge = installBridge(staged, { accepted: false, completed: false, state: refused });
    const reportError = vi.fn();
    expect(await installLocalUpdate(bridge, reportError)).toBe(false);
    expect(reportError).toHaveBeenCalledExactlyOnceWith(
      "Could not install update",
      "Startup gate unavailable.",
    );
    expect(toast.add).not.toHaveBeenCalled();
  });

  it("refuses without a staged handle before reaching the installer", async () => {
    const { stagedHandle: _stagedHandle, ...jones } = staged.jones!;
    const bridge = installBridge({ ...staged, jones });
    const reportError = vi.fn();
    expect(await installLocalUpdate(bridge, reportError)).toBe(false);
    expect(bridge.installUpdate).not.toHaveBeenCalled();
    expect(reportError).toHaveBeenCalledExactlyOnceWith(
      "Could not install update",
      "The downloaded update is no longer ready to install. Check for updates again.",
    );
  });

  it("still restarts this app after provider updates fail", async () => {
    const bridge = installBridge(staged);
    const outcome = await runUpdateEverything({
      downloadLocal: async () => true,
      updateProviders: async () => [{ status: "failed" as const, line: "Codex: update failed" }],
      updateServers: async () => [],
    });
    expect(outcome.localReady).toBe(true);
    expect(await installLocalUpdate(bridge, vi.fn())).toBe(true);
    expect(bridge.installUpdate).toHaveBeenCalledExactlyOnceWith("staged-1");
  });

  it("keeps the upstream install argument-free and its unaccepted downloaded notice", async () => {
    const { jones: _jones, ...upstream } = staged;
    const bridge = installBridge(upstream, { accepted: false, completed: false, state: upstream });
    const reportError = vi.fn();
    expect(await installLocalUpdate(bridge, reportError)).toBe(false);
    expect(bridge.installUpdate).toHaveBeenCalledExactlyOnceWith();
    expect(reportError).not.toHaveBeenCalled();
    expect(toast.add).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ type: "success", title: "Update downloaded" }),
    );
  });
});
