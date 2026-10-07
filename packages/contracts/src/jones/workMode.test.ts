import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerSettings, ServerSettingsPatch } from "../settings.ts";

describe("Work mode environment settings", () => {
  it("defaults off for existing settings and persists explicit on/off", () => {
    const decode = Schema.decodeUnknownSync(ServerSettings);
    const patch = Schema.decodeUnknownSync(ServerSettingsPatch);
    const encode = Schema.encodeSync(ServerSettings);
    const original = decode({});
    expect(original.workModeEnabled).toBe(false);
    const enabled = decode({ ...encode(original), ...patch({ workModeEnabled: true }) });
    expect(decode(encode(enabled)).workModeEnabled).toBe(true);
    expect(
      decode({ ...encode(enabled), ...patch({ workModeEnabled: false }) }).workModeEnabled,
    ).toBe(false);
  });
  it("rejects non-boolean writes and leaves unrelated patches alone", () => {
    const patch = Schema.decodeUnknownSync(ServerSettingsPatch);
    expect(() => patch({ workModeEnabled: "true" })).toThrow();
    expect(patch({ enableProviderUpdateChecks: false })).not.toHaveProperty("workModeEnabled");
  });
});
