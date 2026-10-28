import { act, render, fakeTimers } from "../../../test/render";
import "~/index.css";
import { page, userEvent } from "vite-plus/test/browser";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import { collectProviderUpdateCandidates } from "@t3tools/client-runtime/jones/provider-updates";
import { AsyncResult } from "effect/reactivity";
import type { useJonesProviderUpdates } from "./useProviderUpdates";
import { ToastProvider, toastManager } from "~/components/ui/toast";

const state = vi.hoisted(() => ({
  updates: null as ReturnType<typeof useJonesProviderUpdates> | null,
  navigate: vi.fn(),
  updateEnvironment: vi.fn(async () => []),
}));
vi.mock(import("./useProviderUpdates"), () => ({ useJonesProviderUpdates: () => state.updates! }));
vi.mock("@tanstack/react-router", async (original) => ({
  ...(await original<typeof import("@tanstack/react-router")>()),
  useNavigate: () => state.navigate,
}));
import { ProviderUpdateEnvironmentRows, ProviderUpdateNotice } from "./ProviderUpdateNotice";

const wsl = EnvironmentId.make("ui-update-wsl");
const laptop = EnvironmentId.make("ui-update-laptop");
function provider(id: string, manual = false): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(id),
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: "1.0.0",
    status: "ready",
    auth: { status: "authenticated" },
    checkedAt: "2026-10-26T01:00:00.000Z",
    models: [],
    skills: [],
    slashCommands: [],
    versionAdvisory: {
      status: "behind_latest",
      currentVersion: "1.0.0",
      latestVersion: "2.0.0",
      updateCommand: manual ? null : "npm install -g @openai/codex@latest",
      canUpdate: !manual,
      checkedAt: "2026-10-26T01:00:00.000Z",
      message: "Update available.",
    },
  };
}
function updates() {
  const groups = [
    { environmentId: wsl, label: "WSL", providers: [provider("personal"), provider("work")] },
    { environmentId: laptop, label: "Laptop", providers: [provider("manual", true)] },
  ].map((group) => ({
    ...group,
    isPrimary: group.environmentId === wsl,
    isSettling: false,
    allCandidates: collectProviderUpdateCandidates(group.providers),
    candidates: collectProviderUpdateCandidates(group.providers).filter(
      (candidate) => candidate.versionAdvisory.canUpdate,
    ),
    attempt: undefined as ReturnType<typeof useJonesProviderUpdates>["groups"][number]["attempt"],
    canManage: true,
    connected: true,
  }));
  return {
    groups,
    machines: groups.filter((group) => group.candidates.length > 0),
    noticeKey: "ui-fixture",
    isAnySettling: false,
    updateAll: async () => [],
    updateEnvironment: state.updateEnvironment,
    attempts: new Map(),
    completed: [],
    canManage: true,
  };
}

// Match the real external store: snapshots change identity on publication.
function publishGroups(group: ReturnType<typeof useJonesProviderUpdates>["groups"][number]) {
  state.updates = {
    ...state.updates!,
    groups: state.updates!.groups.map((entry) =>
      entry.environmentId === group.environmentId ? { ...group } : entry,
    ),
  };
}

describe("current provider update notice rows", () => {
  it("collapses devices independently and keeps update, retry and settings actions separate", async () => {
    state.updates = updates();
    state.updateEnvironment.mockClear();
    state.navigate.mockReset();
    await using screen = await render(<ProviderUpdateEnvironmentRows />, { router: false });
    const toggle = page.getByRole("button", { name: "Provider updates on WSL", exact: true });
    const row = page.getByRole("region", { name: "Provider updates on WSL", exact: true });
    await toggle.click();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
    await expect.element(page.getByText("Codex · personal", { exact: true })).not.toBeVisible();
    await expect.element(page.getByText("Codex · manual", { exact: true })).toBeVisible();
    await expect
      .element(row.getByRole("button", { name: "Settings", exact: true }))
      .not.toBeInTheDocument();
    await row.getByRole("button", { name: "Update", exact: true }).click();
    expect(state.updateEnvironment).toHaveBeenCalledWith(wsl);
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
    const group = state.updates.groups[0]!;
    publishGroups({
      ...group,
      attempt: {
        generation: 1,
        machine: group,
        phase: "failed",
        runs: [],
        message: "Long diagnostic that should stay hidden when this device is collapsed.",
      },
    });
    await screen.rerender(<ProviderUpdateEnvironmentRows />);
    await expect.element(row.getByRole("status")).toHaveTextContent("Update needs attention");
    await expect
      .element(
        row.getByText("Long diagnostic that should stay hidden when this device is collapsed."),
      )
      .not.toBeVisible();
    await row.getByRole("button", { name: "Retry", exact: true }).click();
    expect(state.updateEnvironment).toHaveBeenCalledTimes(2);
    toggle.element().focus();
    await userEvent.keyboard("{Enter}");
    await expect.element(toggle).toHaveAttribute("aria-expanded", "true");
    await expect
      .element(
        row.getByText("Long diagnostic that should stay hidden when this device is collapsed."),
      )
      .toBeVisible();
    await row.getByRole("button", { name: "Settings", exact: true }).click();
    expect(state.navigate).toHaveBeenCalledWith({
      to: "/settings/providers",
      search: { machine: wsl },
    });
  });

  it("fully collapses the toast while retaining device choices through progress and refreshed targets", async () => {
    localStorage.removeItem("t3code:provider-update-dismissals:v1");
    state.updates = { ...updates(), noticeKey: "collapse-first" };
    const notice = () => (
      <ToastProvider>
        <ProviderUpdateNotice />
      </ToastProvider>
    );
    await using screen = await render(<ToastProvider />);
    await screen.rerender(notice());
    const toggle = page.getByRole("button", { name: "Provider updates available", exact: true });
    const device = page.getByRole("button", { name: "Provider updates on WSL", exact: true });
    await expect.element(toggle).toBeVisible();
    await device.click();
    await toggle.click();
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
    await expect.element(device).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Settings", exact: true }))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Dismiss notification", exact: true }))
      .toBeVisible();
    const group = state.updates.groups[0]!;
    publishGroups({
      ...group,
      attempt: {
        generation: 1,
        machine: group,
        phase: "running",
        runs: [],
        message: "Updating providers…",
      },
    });
    await screen.rerender(notice());
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
    publishGroups({
      ...group,
      attempt: {
        generation: 1,
        machine: group,
        phase: "succeeded",
        runs: [],
        message: "Synthetic update finished",
      },
    });
    state.updates = { ...state.updates!, noticeKey: "collapse-refreshed" };
    await screen.rerender(notice());
    await expect.element(toggle).toHaveAttribute("aria-expanded", "false");
    toggle.element().focus();
    await userEvent.keyboard(" ");
    await expect.element(toggle).toHaveAttribute("aria-expanded", "true");
    await expect.element(device).toHaveAttribute("aria-expanded", "false");
    await expect.element(page.getByText("Providers updated", { exact: true })).toBeVisible();
    await expect
      .element(page.getByText("Synthetic update finished", { exact: true }))
      .not.toBeVisible();
    await toggle.click();
    await page.getByRole("button", { name: "Dismiss notification", exact: true }).click();
    await expect.element(toggle).not.toBeInTheDocument();
    expect(
      JSON.parse(localStorage.getItem("t3code:provider-update-dismissals:v1") ?? "{}").keys,
    ).toContain("collapse-refreshed");
    localStorage.removeItem("t3code:provider-update-dismissals:v1");
  });

  it("keeps a single-device notice collapsible and preserves its global Settings action", async () => {
    localStorage.removeItem("t3code:provider-update-dismissals:v1");
    state.updates = updates();
    state.updates.groups = state.updates.groups.slice(0, 1);
    state.updates.noticeKey = "collapse-primary";
    state.navigate.mockReset();
    await using screen = await render(<ToastProvider />);
    await screen.rerender(
      <ToastProvider>
        <ProviderUpdateNotice />
      </ToastProvider>,
    );
    const toggle = page.getByRole("button", { name: "Provider updates available", exact: true });
    await toggle.click();
    await expect.element(page.getByText("Codex · personal", { exact: true })).not.toBeVisible();
    await toggle.click();
    await page.elementLocator(document.querySelector('[data-slot="toast-action"]')!).click();
    expect(state.navigate).toHaveBeenCalledWith({ to: "/settings/providers" });
    localStorage.removeItem("t3code:provider-update-dismissals:v1");
  });

  it("gives each new settling period its own grace timer and retires the old timer", async () => {
    using _timers = fakeTimers();
    using addToast = vi.spyOn(toastManager, "add").mockReturnValue("provider-update-toast");
    using updateToast = vi.spyOn(toastManager, "update").mockImplementation(() => {});
    using closeToast = vi.spyOn(toastManager, "close").mockImplementation(() => {});
    state.updates = { ...updates(), noticeKey: "grace-first", isAnySettling: true };
    {
      await using screen = await render(<ProviderUpdateNotice />);
      await act(() => vi.advanceTimersByTimeAsync(29_999));
      expect(addToast).not.toHaveBeenCalled();
      await act(() => vi.advanceTimersByTimeAsync(1));
      expect(addToast).toHaveBeenCalledTimes(1);

      state.updates = { ...state.updates!, noticeKey: "", isAnySettling: false };
      await screen.rerender(<ProviderUpdateNotice />);
      state.updates = { ...state.updates!, noticeKey: "grace-second", isAnySettling: true };
      await screen.rerender(<ProviderUpdateNotice />);
      await act(() => vi.advanceTimersByTimeAsync(29_999));
      expect(addToast).toHaveBeenCalledTimes(1);
      await act(() => vi.advanceTimersByTimeAsync(1));
      expect(addToast).toHaveBeenCalledTimes(1);
      expect(updateToast).toHaveBeenCalledTimes(1);

      state.updates = { ...state.updates!, noticeKey: "", isAnySettling: false };
      await screen.rerender(<ProviderUpdateNotice />);
      state.updates = { ...state.updates!, noticeKey: "grace-cancelled", isAnySettling: true };
      await screen.rerender(<ProviderUpdateNotice />);
      await act(() => vi.advanceTimersByTimeAsync(10_000));
      state.updates = { ...state.updates!, noticeKey: "", isAnySettling: false };
      await screen.rerender(<ProviderUpdateNotice />);
      await act(() => vi.advanceTimersByTimeAsync(30_000));
      expect(addToast).toHaveBeenCalledTimes(1);
      expect(updateToast).toHaveBeenCalledTimes(1);
    }
    expect(closeToast).toHaveBeenCalled();
  });

  it.each(["instance", "version"])(
    "resurfaces current %s targets after a completed attempt while preserving active capture",
    async (change) => {
      state.updates = updates();
      state.updates.groups = state.updates.groups.slice(0, 1);
      const group = state.updates.groups[0]!;
      const personal = collectProviderUpdateCandidates([provider("personal")]);
      group.candidates = personal;
      group.allCandidates = personal;
      group.attempt = {
        generation: 1,
        machine: { environmentId: wsl, label: "WSL", candidates: personal },
        phase: "succeeded",
        message: "Personal completed",
        runs: [],
      };
      await using screen = await render(<ProviderUpdateEnvironmentRows />, { router: false });
      await expect.element(page.getByText("Personal completed", { exact: true })).toBeVisible();
      const baseProvider = provider(change === "instance" ? "work" : "personal");
      const offered = {
        ...baseProvider,
        versionAdvisory: { ...baseProvider.versionAdvisory!, latestVersion: "3.0.0" },
      };
      group.allCandidates = collectProviderUpdateCandidates([offered]);
      group.candidates = group.allCandidates;
      publishGroups(group);
      await screen.rerender(<ProviderUpdateEnvironmentRows />);
      await expect
        .element(page.getByText("Personal completed", { exact: true }))
        .not.toBeInTheDocument();
      await expect
        .element(page.getByText(`Codex · ${offered.instanceId}`, { exact: true }))
        .toBeVisible();
      state.updateEnvironment.mockClear();
      await page.getByRole("button", { name: "Update", exact: true }).click();
      expect(state.updateEnvironment).toHaveBeenCalledWith(wsl);
      expect(
        group.candidates.map((candidate) => [
          candidate.instanceId,
          candidate.versionAdvisory.latestVersion,
        ]),
      ).toEqual([[offered.instanceId, "3.0.0"]]);
      group.attempt = { ...group.attempt!, phase: "running" };
      publishGroups(group);
      await screen.rerender(<ProviderUpdateEnvironmentRows />);
      await expect
        .element(page.getByText("Codex · personal · updating", { exact: true }))
        .toBeVisible();
      await expect
        .element(page.getByRole("button", { name: "Loading", exact: true }))
        .toBeDisabled();
    },
  );

  it("groups every instance, exposes manual settings, and retains retry and diagnostic feedback", async () => {
    state.navigate.mockReset();
    state.updateEnvironment.mockClear();
    state.updates = updates();
    await using screen = await render(<ProviderUpdateEnvironmentRows />, { router: false });
    await expect
      .element(page.getByRole("region", { name: "Provider updates on WSL" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("region", { name: "Provider updates on Laptop" }))
      .toBeVisible();
    await expect.element(page.getByText("Codex · personal", { exact: true })).toBeVisible();
    await expect.element(page.getByText("Codex · work", { exact: true })).toBeVisible();
    await expect
      .element(
        page.getByText(
          "Update manually with the app or tool that installed this provider. Review provider settings for details.",
        ),
      )
      .toBeVisible();
    const wslRow = page.getByRole("region", { name: "Provider updates on WSL" });
    await wslRow.getByRole("button", { name: "Update", exact: true }).click();
    expect(state.updateEnvironment).toHaveBeenCalledWith(wsl);
    await wslRow.getByRole("button", { name: "Settings", exact: true }).click();
    expect(state.navigate).toHaveBeenCalledWith({
      to: "/settings/providers",
      search: { machine: wsl },
    });
    const group = state.updates.groups[0]!;
    group.attempt = {
      generation: 1,
      machine: group,
      phase: "unchanged",
      message: "Provider update could not be verified.",
      runs: group.candidates.map((target) => ({
        environmentId: wsl,
        machineLabel: "WSL",
        driver: target.driver,
        instanceId: target.instanceId,
        result: AsyncResult.success({
          providers: [
            {
              ...target,
              updateState: {
                status: "unchanged",
                startedAt: target.checkedAt,
                finishedAt: target.checkedAt,
                message: "Installed version could not be verified.",
                output: null,
              },
            },
          ],
        }),
      })),
    };
    await act(async () => {
      publishGroups(group);
      await screen.rerender(<ProviderUpdateEnvironmentRows />);
    });
    await expect
      .element(wslRow.getByRole("status"))
      .toHaveTextContent("Provider update could not be verified.");
    await expect
      .element(page.getByText("Codex · personal · update could not be verified", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("Codex · work · update could not be verified", { exact: true }))
      .toBeVisible();
    await wslRow.getByRole("button", { name: "Retry", exact: true }).click();
    expect(state.updateEnvironment).toHaveBeenCalledTimes(2);
    group.connected = false;
    await act(async () => {
      publishGroups(group);
      await screen.rerender(<ProviderUpdateEnvironmentRows />);
    });
    await expect.element(wslRow.getByRole("button", { name: "Retry", exact: true })).toBeDisabled();
    await expect
      .element(wslRow.getByText("Connect this environment before updating providers."))
      .toBeVisible();
  });
});
