import { expect, it } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
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
import { ThreadActivitySettings } from "./ThreadActivitySettings";
import "~/index.css";

it("persists a window, rejects invalid amounts, and restores controls after remount", async () => {
  const stored = localStorage.getItem("t3code:client-settings:v1");
  try {
    writeBrowserClientSettings(DEFAULT_CLIENT_SETTINGS);
    __setClientSettingsForTests(DEFAULT_CLIENT_SETTINGS);
    await using first = await render(<ThreadActivitySettings />);
    const amount = first.getByRole("spinbutton", { name: "Activity window amount" });
    await expect.element(amount).toBeDisabled();
    await first.getByRole("switch", { name: "Thread activity window" }).click();
    await amount.fill("0");
    await userEvent.keyboard("{Tab}");
    await expect
      .element(first.getByRole("alert"))
      .toHaveTextContent("Enter a whole number from 1 to 10,000.");
    expect(readBrowserClientSettings()?.threadActivityWindowAmount).toBe(3);
    await amount.fill("12");
    await userEvent.keyboard("{Tab}");
    await first.getByRole("combobox", { name: "Activity window unit" }).click();
    await page.getByRole("option", { name: "days", exact: true }).click();
    await expect.poll(() => readBrowserClientSettings()?.threadActivityWindowUnit).toBe("days");
    expect(readBrowserClientSettings()?.threadActivityWindowAmount).toBe(12);
    expect(readBrowserClientSettings()?.threadActivityWindowEnabled).toBe(true);
    await first.unmount();
    __resetClientSettingsPersistenceForTests();
    await using second = await render(<ThreadActivitySettings />);
    await expect
      .element(second.getByRole("spinbutton", { name: "Activity window amount" }))
      .toHaveValue(12);
    await expect
      .element(second.getByRole("combobox", { name: "Activity window unit" }))
      .toHaveTextContent("days");
    await second.getByRole("switch", { name: "Thread activity window" }).click();
    await expect.poll(() => readBrowserClientSettings()?.threadActivityWindowEnabled).toBe(false);
    await expect
      .element(second.getByRole("spinbutton", { name: "Activity window amount" }))
      .toBeDisabled();
  } finally {
    __resetClientSettingsPersistenceForTests();
    if (stored === null) localStorage.removeItem("t3code:client-settings:v1");
    else localStorage.setItem("t3code:client-settings:v1", stored);
  }
});
