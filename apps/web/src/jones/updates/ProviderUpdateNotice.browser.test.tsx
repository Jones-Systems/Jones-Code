import { act, render } from "../../../test/render";
import "~/index.css";
import { page } from "vite-plus/test/browser";
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
import { ProviderUpdateEnvironmentRows } from "./ProviderUpdateNotice";

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

describe("current provider update notice rows", () => {
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
      await screen.rerender(<ProviderUpdateEnvironmentRows />);
    });
    await expect.element(wslRow.getByRole("button", { name: "Retry", exact: true })).toBeDisabled();
    await expect
      .element(wslRow.getByText("Connect this environment before updating providers."))
      .toBeVisible();
  });
});
