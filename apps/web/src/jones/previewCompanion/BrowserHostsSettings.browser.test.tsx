import { RegistryContext } from "@effect/atom-react";
import { EnvironmentId, type DesktopCompanionState } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";
import { act, render } from "../../../test/render";
import { appAtomRegistry } from "~/rpc/atomRegistry";
import { BrowserHostsSettings } from "./BrowserHostsSettings";
import { companionStateAtom } from "./state";

const environmentId = EnvironmentId.make("vps");

vi.mock(import("../../components/settings/SettingsScopeContext"), async (original) => ({
  ...(await original()),
  useSettingsScope: (() => ({ environments: [] })) as never,
}));
vi.mock(import("../../state/environments"), async (original) => ({
  ...(await original()),
  useEnvironments: (() => ({
    environments: [{ environmentId, label: "VPS", entry: { enabled: true } }],
  })) as never,
}));

function companionState(
  config: Partial<DesktopCompanionState["config"]>,
  status: DesktopCompanionState["status"],
): DesktopCompanionState {
  return {
    config: {
      enabled: false,
      environmentId: null,
      hostId: "host-1" as DesktopCompanionState["config"]["hostId"],
      label: "Desk",
      browserOnly: false,
      ...config,
    },
    status,
    connectionGeneration: null,
    assignments: [],
  };
}

async function renderWith(state: DesktopCompanionState) {
  const configure = vi.fn(async () => state);
  window.desktopBridge = { previewCompanion: { configure, retry: vi.fn(async () => {}) } } as never;
  await act(async () => appAtomRegistry.set(companionStateAtom, state));
  const screen = await render(
    <RegistryContext.Provider value={appAtomRegistry}>
      <BrowserHostsSettings />
    </RegistryContext.Provider>,
  );
  return { screen, configure };
}

describe("BrowserHostsSettings", () => {
  it("shows only the host switch until hosting is enabled", async () => {
    const { screen } = await renderWith(companionState({}, "disabled"));
    await using _ = screen;
    await expect
      .element(page.getByRole("switch", { name: "Use this desktop as a browser host" }))
      .toBeDisabled();
    expect(page.getByRole("textbox", { name: "Browser host label" }).query()).toBeNull();
    expect(page.getByRole("switch", { name: "Browser-only desktop" }).query()).toBeNull();
    expect(page.getByRole("button", { name: "Retry connection" }).query()).toBeNull();
  });

  it("enables hosting for the chosen environment", async () => {
    const { screen, configure } = await renderWith(companionState({ environmentId }, "disabled"));
    await using _ = screen;
    await userEvent.click(page.getByRole("switch", { name: "Use this desktop as a browser host" }));
    expect(configure).toHaveBeenCalledWith(
      expect.objectContaining({ enabled: true, environmentId }),
    );
  });

  it("shows configuration and retry while enabled and not online", async () => {
    const { screen } = await renderWith(
      companionState({ enabled: true, environmentId }, "reconnecting"),
    );
    await using _ = screen;
    await expect.element(page.getByText("Reconnecting…")).toBeVisible();
    await expect.element(page.getByRole("textbox", { name: "Browser host label" })).toBeVisible();
    await expect.element(page.getByRole("switch", { name: "Browser-only desktop" })).toBeVisible();
    await expect.element(page.getByRole("button", { name: "Retry connection" })).toBeVisible();
  });
});
