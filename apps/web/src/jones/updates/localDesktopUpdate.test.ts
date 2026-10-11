import { FleetDesktopState } from "@t3tools/contracts/jones/fleet-updates";
import * as Schema from "effect/Schema";
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
import {
  canDiscardLocalDesktopUpdate,
  discardLocalDesktopUpdate,
  downloadLocalDesktopUpdate,
  getJonesDesktopUpdateBuildUrl,
  installLocalDesktopUpdate,
} from "./localDesktopUpdate";

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
      repository: "Jones-Systems/Jones-Code",
      sourceSha: "a".repeat(40),
      sourceTree: "b".repeat(40),
      workflow: "artifact-desktop-mac.yml",
      runId: 123,
      runAttempt: 1,
      artifactId: 456,
      artifactDigest: "c".repeat(64),
      platform: "darwin",
      architecture: "arm64",
    } as const;
    const state: DesktopUpdateState = {
      ...downloaded,
      jones: { ...downloaded.jones!, provenance },
    };
    const bridge = { downloadUpdate: vi.fn().mockResolvedValue(result(state)) };
    await downloadLocalDesktopUpdate(bridge, state);
    expect(bridge.downloadUpdate).toHaveBeenCalledExactlyOnceWith({
      artifactId: 456,
      sourceSha: "a".repeat(40),
    });
    expect(getJonesDesktopUpdateBuildUrl(state)).toBe(
      "https://github.com/Jones-Systems/Jones-Code/actions/runs/123",
    );
    await expect(downloadLocalDesktopUpdate(bridge, downloaded)).rejects.toThrow(
      "Check for builds",
    );
    expect(bridge.downloadUpdate).toHaveBeenCalledOnce();
  });

  it("keeps upstream Download argument-free", async () => {
    const { jones: _jones, ...state } = downloaded;
    const bridge = { downloadUpdate: vi.fn().mockResolvedValue(result(state)) };
    await downloadLocalDesktopUpdate(bridge, state);
    expect(bridge.downloadUpdate).toHaveBeenCalledExactlyOnceWith();
  });

  it.each(["preparing", "installing"] as const)(
    "does not report accepted %s as an error",
    (phase) => {
      expect(
        getDesktopUpdateActionError({
          accepted: true,
          completed: false,
          state: {
            ...downloaded,
            message: "Restart underway",
            jones: { ...downloaded.jones!, phase },
          },
        }),
      ).toBeNull();
    },
  );

  it("reports a fulfilled failed Download result", () => {
    expect(
      getDesktopUpdateActionError({
        accepted: true,
        completed: false,
        state: {
          ...downloaded,
          message: "Could not stage",
          jones: { ...downloaded.jones!, phase: "error" },
        },
      }),
    ).toBe("Could not stage");
  });

  it("discards the displayed handle but refuses an activation in progress", async () => {
    const bridge = { discardUpdate: vi.fn().mockResolvedValue(result()) };
    expect(canDiscardLocalDesktopUpdate(downloaded)).toBe(true);
    await discardLocalDesktopUpdate(bridge, downloaded);
    expect(bridge.discardUpdate).toHaveBeenCalledExactlyOnceWith("downloaded-1.1.0");
    const installing = {
      ...downloaded,
      jones: { ...downloaded.jones!, phase: "installing" as const },
    };
    expect(canDiscardLocalDesktopUpdate(installing)).toBe(false);
    await expect(discardLocalDesktopUpdate(bridge, installing)).rejects.toThrow(
      "cannot be discarded",
    );
    await expect(discardLocalDesktopUpdate({}, downloaded)).rejects.toThrow("cannot be discarded");
    expect(bridge.discardUpdate).toHaveBeenCalledOnce();
  });

  it("persists an enrolled campaign before installing its exact selected build", async () => {
    const campaignId = "11111111-1111-4111-8111-111111111111";
    const fleet = Schema.decodeUnknownSync(FleetDesktopState)({
      schema: 1,
      campaigns: [],
      enrollments: [
        {
          enrollmentId: campaignId,
          environmentId: "remote-host",
          enabled: true,
          continueRunningThreads: false,
        },
      ],
    });
    const state: DesktopUpdateState = {
      ...downloaded,
      jones: {
        ...downloaded.jones!,
        provenance: {
          repository: "Jones-Systems/Jones-Code",
          sourceSha: "a".repeat(40),
          sourceTree: "b".repeat(40),
          workflow: "artifact-desktop-mac.yml",
          runId: 123,
          runAttempt: 1,
          artifactId: 456,
          artifactDigest: "c".repeat(64),
          platform: "darwin",
          architecture: "arm64",
        },
      },
    };
    const order: string[] = [];
    const bridge = {
      fleetUpdates: vi.fn(
        async (
          request: Parameters<
            NonNullable<import("@t3tools/contracts").DesktopBridge["fleetUpdates"]>
          >[0],
        ) => {
          order.push(request.action);
          return request.action === "prepare"
            ? {
                ...fleet,
                campaigns: [{ ...request.input, phase: "prepared" as const, members: [] }],
              }
            : fleet;
        },
      ),
      installUpdate: vi.fn(async (_handle?: string, _campaign?: string) => {
        order.push("install");
        return result(state);
      }),
    };
    await installLocalDesktopUpdate(bridge, state);
    const prepare = bridge.fleetUpdates.mock.calls[1]![0];
    expect(prepare.action).toBe("prepare");
    if (prepare.action !== "prepare") throw new Error("Expected campaign preparation.");
    expect(prepare.input).toMatchObject({
      targetSource: "a".repeat(40),
      desktopStagedHandle: "downloaded-1.1.0",
    });
    expect(bridge.installUpdate).toHaveBeenCalledExactlyOnceWith(
      "downloaded-1.1.0",
      prepare.input.campaignId,
    );
    expect(order).toEqual(["read", "prepare", "install"]);
  });

  it("creates fresh campaign IDs when reselecting a superseded build, then reuses the latest campaign", async () => {
    const oldCampaignId = "11111111-1111-4111-8111-111111111111";
    const otherCampaignId = "22222222-2222-4222-8222-222222222222";
    let fleet = Schema.decodeUnknownSync(FleetDesktopState)({
      schema: 1,
      enrollments: [
        {
          enrollmentId: oldCampaignId,
          environmentId: "remote-host",
          enabled: true,
          continueRunningThreads: false,
        },
      ],
      campaigns: [
        {
          campaignId: oldCampaignId,
          targetSource: "a".repeat(40),
          desktopStagedHandle: "downloaded-1.1.0",
          phase: "prepared",
          members: [],
        },
        {
          campaignId: otherCampaignId,
          targetSource: "b".repeat(40),
          desktopStagedHandle: "downloaded-1.2.0",
          phase: "prepared",
          members: [],
        },
      ],
    });
    const state: DesktopUpdateState = {
      ...downloaded,
      jones: {
        ...downloaded.jones!,
        provenance: {
          repository: "Jones-Systems/Jones-Code",
          sourceSha: "a".repeat(40),
          sourceTree: "b".repeat(40),
          workflow: "artifact-desktop-mac.yml",
          runId: 123,
          runAttempt: 1,
          artifactId: 456,
          artifactDigest: "c".repeat(64),
          platform: "darwin",
          architecture: "arm64",
        },
      },
    };
    const bridge = {
      fleetUpdates: vi.fn(
        async (
          request: Parameters<
            NonNullable<import("@t3tools/contracts").DesktopBridge["fleetUpdates"]>
          >[0],
        ) => {
          if (request.action === "prepare")
            fleet = {
              ...fleet,
              campaigns: [...fleet.campaigns, { ...request.input, phase: "prepared", members: [] }],
            };
          return fleet;
        },
      ),
      installUpdate: vi.fn().mockResolvedValue(result(state)),
    };
    await installLocalDesktopUpdate(bridge, state);
    const prepared = bridge.fleetUpdates.mock.calls[1]![0];
    if (prepared.action !== "prepare") throw new Error("Expected a fresh campaign.");
    expect(prepared.input.campaignId).not.toBe(oldCampaignId);
    expect(prepared.input.campaignId).not.toBe(otherCampaignId);
    expect(bridge.installUpdate).toHaveBeenLastCalledWith(
      "downloaded-1.1.0",
      prepared.input.campaignId,
    );
    await installLocalDesktopUpdate(bridge, state);
    expect(bridge.fleetUpdates.mock.calls.map(([request]) => request.action)).toEqual([
      "read",
      "prepare",
      "read",
    ]);
    expect(bridge.installUpdate).toHaveBeenLastCalledWith(
      "downloaded-1.1.0",
      prepared.input.campaignId,
    );
    expect(fleet.campaigns.slice(0, 2).map((campaign) => campaign.campaignId)).toEqual([
      oldCampaignId,
      otherCampaignId,
    ]);
  });

  it("blocks installation when durable fleet state cannot be read", async () => {
    const bridge = {
      fleetUpdates: vi.fn().mockRejectedValue(new Error("journal unavailable")),
      installUpdate: vi.fn(),
    };
    await expect(installLocalDesktopUpdate(bridge, downloaded)).rejects.toThrow(
      "journal unavailable",
    );
    expect(bridge.installUpdate).not.toHaveBeenCalled();
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
