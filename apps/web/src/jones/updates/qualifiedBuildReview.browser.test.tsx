import { describe, expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { useSyncExternalStore } from "react";
import { EnvironmentId, type DesktopBridge, type DesktopUpdateState } from "@t3tools/contracts";
import type {
  JonesUpdateBuildList,
  JonesUpdateProvenance,
} from "@t3tools/contracts/jones/jonesUpdates";
import type {
  JonesInstallPlanRequest,
  JonesInstallPlanState,
} from "@t3tools/contracts/jones/installPlan";
import { startPlan } from "@t3tools/client-runtime/jones/install-plan";
import { act, render, stubGlobal } from "../../../test/render";
import { SidebarUpdatePill } from "../../components/sidebar/SidebarUpdatePill";
import { setUpdateEverythingRun } from "../../components/sidebar/updateEverythingRun";
import { ToastProvider } from "../../components/ui/toast";
import { bindInstallPlanServices } from "../installStatus/services";
import { installStatusStore } from "../installStatus/panelUi";
import "~/index.css";

const mocks = vi.hoisted(() => ({
  desktop: null as DesktopUpdateState | null,
  listeners: new Set<() => void>(),
  providers: true,
  updateProviders: vi.fn(async () => []),
  checkProviders: vi.fn(async () => {}),
}));
vi.mock("../../env", async (original) => ({
  ...(await original<typeof import("../../env")>()),
  isElectron: true,
}));
vi.mock("../../state/desktopUpdate", async (original) => ({
  ...(await original<typeof import("../../state/desktopUpdate")>()),
  useDesktopUpdateState: () =>
    useSyncExternalStore(
      (listener: () => void) => {
        mocks.listeners.add(listener);
        return () => {
          mocks.listeners.delete(listener);
        };
      },
      () => mocks.desktop,
    ),
}));
vi.mock("../../state/environments", async (original) => ({
  ...(await original<typeof import("../../state/environments")>()),
  useEnvironments: () => ({ environments: [] }),
}));
vi.mock("../../components/ProviderUpdatesAction", () => ({
  useCheckProviderUpdates: () => mocks.checkProviders,
  useProviderUpdates: () => ({
    machines: mocks.providers
      ? [
          {
            environmentId: "providers-test",
            label: "Laptop",
            candidates: [{ driver: "codex" }],
          },
        ]
      : [],
    updateAll: mocks.updateProviders,
  }),
}));

const laptop = EnvironmentId.make("picker-laptop");
const latest: JonesUpdateProvenance = {
  repository: "Jones-Systems/Jones-Code",
  sourceSha: "a".repeat(40),
  sourceTree: "b".repeat(40),
  version: "0.1.2-preview.20261112.2.1",
  artifactId: 9001,
  artifactDigest: "c".repeat(64),
  workflow: "artifact-desktop-mac.yml",
  runId: 2,
  runAttempt: 1,
  platform: "darwin",
  architecture: "arm64",
};
const older: JonesUpdateProvenance = {
  ...latest,
  sourceSha: "d".repeat(40),
  sourceTree: "e".repeat(40),
  artifactId: 9002,
  version: "0.1.2-preview.20261112.1.1",
  runId: 1,
};
const initialDesktop = (): DesktopUpdateState => ({
  enabled: true,
  status: "available",
  channel: "latest",
  currentVersion: "0.1.1",
  hostArch: "arm64",
  appArch: "arm64",
  runningUnderArm64Translation: false,
  availableVersion: latest.version!,
  downloadedVersion: null,
  releaseNotes: [],
  omittedReleaseCount: 0,
  downloadPercent: null,
  checkedAt: null,
  message: null,
  errorContext: null,
  canRetry: false,
  jones: {
    source: "jones-actions",
    channel: "jones-main",
    phase: "available",
    capability: { check: true, download: true, install: true },
    provenance: latest,
  },
});
function publish(build: JonesUpdateProvenance) {
  mocks.desktop = {
    ...mocks.desktop!,
    availableVersion: build.version!,
    jones: { ...mocks.desktop!.jones!, provenance: build },
  };
  for (const listener of mocks.listeners) listener();
}

function fixture(catalog: JonesUpdateBuildList = { builds: [latest, older], unavailable: false }) {
  mocks.desktop = initialDesktop();
  mocks.providers = true;
  mocks.updateProviders.mockClear();
  mocks.checkProviders.mockClear();
  setUpdateEverythingRun(() => null);
  installStatusStore.update({
    native: null,
    desktop: mocks.desktop,
    devices: [],
    open: false,
    error: null,
  });
  let native: JonesInstallPlanState = {
    schema: 1,
    current: null,
    last: null,
    laptop: { environmentId: laptop, observedAt: "2026-11-12T15:00:00Z", activationBlocked: false },
    view: { complete: false },
  };
  const installPlan = vi.fn(async (request: JonesInstallPlanRequest) => {
    if (request.type === "start") {
      native = {
        ...native,
        current: startPlan({
          planId: "11111111-1111-4111-8111-111111111111",
          origin: request.origin,
          target: request.target,
          from: { version: "0.1.1" },
          laptopEnvironmentId: laptop,
          select: request.select,
          now: "2026-11-12T15:00:00Z",
        }),
      };
    }
    return { state: native };
  });
  const select = vi.fn(async (build: JonesUpdateProvenance) => {
    publish(build);
    return true;
  });
  const download = vi.fn();
  const install = vi.fn();
  const bridge = {
    installPlan,
    getUpdateState: async () => mocks.desktop!,
    onUpdateState: () => () => {},
    listJonesUpdateBuilds: async () => catalog,
    selectJonesUpdateBuild: select,
    downloadUpdate: download,
    installUpdate: install,
  } satisfies Partial<DesktopBridge>;
  const noHostEffect = async (): Promise<never> => {
    throw new Error("Unexpected host effect");
  };
  const unbind = bindInstallPlanServices({
    bridge,
    devices: () => [],
    observe: noHostEffect,
    legacy: noHostEffect,
    operation: noHostEffect,
    stop: noHostEffect,
    host: noHostEffect,
    confirm: noHostEffect,
    chooseRecovery: noHostEffect,
    now: () => 0,
  });
  const desktopGlobal = stubGlobal("desktopBridge", bridge);
  return {
    bridge,
    select,
    installPlan,
    download,
    install,
    starts: () => installPlan.mock.calls.map(([r]) => r).filter((r) => r.type === "start"),
    [Symbol.dispose]() {
      desktopGlobal[Symbol.dispose]();
      unbind();
      setUpdateEverythingRun(() => null);
      installStatusStore.update({
        native: null,
        desktop: null,
        devices: [],
        open: false,
        error: null,
      });
    },
  };
}
const openReview = async () => {
  await page.getByRole("button", { name: /^Review Jones Code/ }).click();
  await expect.element(page.getByRole("combobox", { name: "Jones Code build" })).toBeVisible();
};

describe("bottom-right qualified build review through the real bulk handoff", () => {
  it("reviews first, retains the alternate identity, and latches repeated combined/providers clicks", async () => {
    using f = fixture();
    let finish!: (accepted: boolean) => void;
    const gate = new Promise<boolean>((resolve) => {
      finish = resolve;
    });
    f.select.mockImplementation(async (build) => {
      const accepted = await gate;
      if (accepted) publish(build);
      return accepted;
    });
    await using _screen = await render(<SidebarUpdatePill />);
    await openReview();
    expect(f.starts()).toEqual([]);
    expect(f.select).not.toHaveBeenCalled();
    const select = page
      .getByRole("combobox", { name: "Jones Code build" })
      .element() as HTMLSelectElement;
    await act(() => {
      select.value = "9002";
      select.dispatchEvent(new Event("change", { bubbles: true }));
    });
    const combined = page.getByRole("button", { name: "Download selected build" }).element();
    const providers = page.getByRole("button", { name: "Update providers only" }).element();
    if (!(combined instanceof HTMLButtonElement) || !(providers instanceof HTMLButtonElement))
      throw new Error("Expected update action buttons.");
    try {
      await act(() => {
        combined.click();
        combined.click();
        providers.click();
      });
      expect(f.select).toHaveBeenCalledExactlyOnceWith(older);
      expect(mocks.updateProviders).not.toHaveBeenCalled();
    } finally {
      await act(() => finish(true));
    }
    await vi.waitFor(() => expect(f.starts()).toHaveLength(1));
    expect(f.starts()[0]).toMatchObject({
      target: {
        repository: older.repository,
        sourceSha: older.sourceSha,
        sourceTree: older.sourceTree,
        version: older.version,
        laptopArtifactId: older.artifactId,
      },
      laptop: { action: "download" },
    });
    expect(mocks.updateProviders).toHaveBeenCalledOnce();
    expect(f.download).not.toHaveBeenCalled();
    expect(f.install).not.toHaveBeenCalled();
  });

  it.each([false, true])(
    "keeps providers usable for an empty catalog (unavailable=%s) without a Jones plan",
    async (unavailable) => {
      using f = fixture({ builds: [], unavailable });
      await using _screen = await render(<SidebarUpdatePill />);
      await openReview();
      await expect
        .element(
          page.getByText(
            unavailable
              ? "The build list could not be fully checked."
              : "No other qualified builds found.",
            { exact: false },
          ),
        )
        .toBeVisible();
      await page.getByRole("button", { name: "Update providers only" }).click();
      await vi.waitFor(() => expect(mocks.updateProviders).toHaveBeenCalledOnce());
      expect(f.installPlan.mock.calls.every(([r]) => r.type === "read")).toBe(true);
      expect(f.select).not.toHaveBeenCalled();
      expect(f.download).not.toHaveBeenCalled();
      expect(f.install).not.toHaveBeenCalled();
    },
  );

  it("refuses identity drift and requires another review before any plan or provider dispatch", async () => {
    using f = fixture();
    f.select.mockResolvedValue(false);
    await using _screen = await render(
      <ToastProvider>
        <SidebarUpdatePill />
      </ToastProvider>,
    );
    await openReview();
    await page.getByRole("button", { name: "Download selected build" }).click();
    await expect
      .element(
        page.getByText(
          "The reviewed build could not be reverified. Open Updates and review again.",
        ),
      )
      .toBeVisible();
    expect(f.starts()).toEqual([]);
    expect(mocks.updateProviders).not.toHaveBeenCalled();
    f.select.mockImplementation(async (build) => {
      publish(build);
      return true;
    });
    await openReview();
    await page.getByRole("button", { name: "Download selected build" }).click();
    await vi.waitFor(() => expect(f.starts()).toHaveLength(1));
  });

  it("refuses eligibility lost after native selection before writing the Laptop plan", async () => {
    using f = fixture();
    f.select.mockImplementation(async (build) => {
      publish(build);
      mocks.desktop = {
        ...mocks.desktop!,
        jones: {
          ...mocks.desktop!.jones!,
          capability: { check: true, download: false, install: false },
        },
      };
      return true;
    });
    await using _screen = await render(
      <ToastProvider>
        <SidebarUpdatePill />
      </ToastProvider>,
    );
    await openReview();
    await page.getByRole("button", { name: "Download selected build" }).click();
    await expect
      .element(
        page.getByText(
          "The reviewed build or install plan changed. Open Updates and review again.",
        ),
      )
      .toBeVisible();
    expect(f.starts()).toEqual([]);
    expect(mocks.updateProviders).not.toHaveBeenCalled();
    expect(f.download).not.toHaveBeenCalled();
    expect(f.install).not.toHaveBeenCalled();
  });
});
