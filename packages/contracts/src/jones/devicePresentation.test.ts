import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerSettings, ServerSettingsPatch } from "../settings.ts";
import { DeviceDisplayName, deviceDisplayLabel } from "./devicePresentation.ts";

const decodeSettings = Schema.decodeSync(ServerSettings);
const decodePatch = Schema.decodeSync(ServerSettingsPatch);
const isName = Schema.is(DeviceDisplayName);

describe("device presentation contracts", () => {
  it("decodes settings from older servers and resets to automatic", () => {
    const old = decodeSettings({});
    expect(old.deviceDisplayName).toBeNull();
    const renamed = decodeSettings({ deviceDisplayName: "Office" });
    expect(deviceDisplayLabel("Mac Mini", "mac-mini", renamed.deviceDisplayName)).toBe("Office");
    const reset = decodePatch({ deviceDisplayName: null });
    expect(deviceDisplayLabel("Mac Mini", "mac-mini", reset.deviceDisplayName)).toBe("Mini");
  });
  it("bounds names and rejects control and invisible formatting characters", () => {
    for (const name of ["", "a".repeat(65), "bad\nname", "bad\u202ename"]) {
      expect(isName(name)).toBe(false);
    }
    expect(isName("Studio 🍎")).toBe(true);
  });
  it.each([
    ["VPS", "server", "VPS"],
    ["Test Server", "server", "Test Server"],
    ["Computer", "laptop", "Laptop"],
    ["Computer", "mac-mini", "Mini"],
    ["Unknown host", "cloud", "Unknown host"],
  ] as const)("preserves meaningful automatic labels for %s", (label, kind, expected) => {
    expect(deviceDisplayLabel(label, kind)).toBe(expected);
  });
});
