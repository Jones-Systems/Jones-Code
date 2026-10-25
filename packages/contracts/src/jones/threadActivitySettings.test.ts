import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ClientSettingsSchema, ClientSettingsPatch } from "../settings.ts";

const decodeSettings = Schema.decodeUnknownSync(ClientSettingsSchema);
const decodePatch = Schema.decodeUnknownSync(ClientSettingsPatch);
const encodeSettings = Schema.encodeSync(ClientSettingsSchema);

describe("thread activity settings", () => {
  it("preserves existing clients with disabled three-hour defaults and round trips device settings", () => {
    const defaults = decodeSettings({});
    expect([
      defaults.threadActivityWindowEnabled,
      defaults.threadActivityWindowAmount,
      defaults.threadActivityWindowUnit,
    ]).toEqual([false, 3, "hours"]);
    for (const unit of ["minutes", "hours", "days", "weeks"] as const) {
      const patch = decodePatch({
        threadActivityWindowEnabled: true,
        threadActivityWindowAmount: 12,
        threadActivityWindowUnit: unit,
      });
      const settings = { ...defaults, ...patch };
      expect(decodeSettings(encodeSettings(settings))).toEqual(settings);
    }
  });
  it("rejects invalid or unsafe durations in stored settings and patches", () => {
    for (const decode of [decodeSettings, decodePatch]) {
      for (const amount of [0, -1, 1.5, 10_001, Infinity, NaN]) {
        expect(() => decode({ threadActivityWindowAmount: amount })).toThrow();
      }
      expect(() => decode({ threadActivityWindowUnit: "months" })).toThrow();
    }
  });
});
