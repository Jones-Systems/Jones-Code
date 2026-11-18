import { expect, it } from "vite-plus/test";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts";
import { render } from "../../../test/render";
import {
  readBrowserClientSettings,
  writeBrowserClientSettings,
} from "../../clientPersistenceStorage";
import {
  __resetClientSettingsPersistenceForTests,
  __setClientSettingsForTests,
} from "../../hooks/useSettings";
import { searchSettings } from "../../components/settings/settingsSearch";
import { WorkButtonSettings } from "./WorkButtonSettings";
import "~/index.css";

it("finds the appearance control, persists hiding, and restores visibility after remount", async () => {
  const stored = localStorage.getItem("t3code:client-settings:v1");
  try {
    expect(searchSettings("Show Work button")[0]).toMatchObject({
      id: "show-work-button",
      to: "/settings/jones-code/appearance",
    });
    writeBrowserClientSettings(DEFAULT_CLIENT_SETTINGS);
    __setClientSettingsForTests(DEFAULT_CLIENT_SETTINGS);
    await using first = await render(<WorkButtonSettings />);
    const toggle = first.getByRole("switch", { name: "Show Work button" });
    await expect.element(toggle).toBeChecked();
    await toggle.click();
    await expect.poll(() => readBrowserClientSettings()?.showWorkButton).toBe(false);
    expect(readBrowserClientSettings()).not.toHaveProperty("workModeEnabled");
    await first.unmount();
    __resetClientSettingsPersistenceForTests();
    await using second = await render(<WorkButtonSettings />);
    const restored = second.getByRole("switch", { name: "Show Work button" });
    await expect.element(restored).not.toBeChecked();
    await restored.click();
    await expect.element(restored).toBeChecked();
    await expect.poll(() => readBrowserClientSettings()?.showWorkButton).toBe(true);
    expect(readBrowserClientSettings()).not.toHaveProperty("workModeEnabled");
  } finally {
    __resetClientSettingsPersistenceForTests();
    if (stored === null) localStorage.removeItem("t3code:client-settings:v1");
    else localStorage.setItem("t3code:client-settings:v1", stored);
  }
});
