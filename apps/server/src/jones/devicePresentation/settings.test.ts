import { expect, it } from "vite-plus/test";
import { DEFAULT_SERVER_SETTINGS } from "@t3tools/contracts";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";
import { decodePersistedServerSettings } from "../../ServerSettingsPersistence.ts";

it("persists a device-only override through existing settings patches and restores automatic naming", () => {
  const before = { ...DEFAULT_SERVER_SETTINGS, threadTitleInstructions: "Retain this" };
  const renamed = applyServerSettingsPatch(before, { deviceDisplayName: "Office" });
  expect(renamed.deviceDisplayName).toBe("Office");
  expect(renamed.threadTitleInstructions).toBe("Retain this");
  // JSON persistence uses the settings encoder in production; a minimal persisted file
  // proves the added field's lenient load and older settings defaults independently.
  expect(
    decodePersistedServerSettings('{"deviceDisplayName":"Office"}').settings.deviceDisplayName,
  ).toBe("Office");
  expect(decodePersistedServerSettings("{}").settings.deviceDisplayName).toBeNull();
  expect(
    applyServerSettingsPatch(renamed, { deviceDisplayName: null }).deviceDisplayName,
  ).toBeNull();
});
