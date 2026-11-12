import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ClientSettingsSchema, ClientSettingsPatch } from "../settings.ts";

const decodeClientSettings = Schema.decodeUnknownSync(ClientSettingsSchema);
const decodeClientSettingsPatch = Schema.decodeUnknownSync(ClientSettingsPatch);
const encodeClientSettings = Schema.encodeSync(ClientSettingsSchema);

describe("ClientSettings default prompt view", () => {
  it("defaults existing client settings to Pending without adding a patch", () => {
    expect(decodeClientSettings({}).promptsDefaultStage).toBe("pending");
    expect(decodeClientSettingsPatch({})).not.toHaveProperty("promptsDefaultStage");
  });

  it.each(["pending", "queued", "sent"])("round-trips the %s preference", (stage) => {
    const input = { promptsDefaultStage: stage };
    expect(encodeClientSettings(decodeClientSettings(input))).toMatchObject(input);
    expect(decodeClientSettingsPatch(input)).toEqual(input);
  });

  it.each(["unknown", "Pending", true, null])("rejects invalid stage %s", (stage) => {
    expect(() => decodeClientSettings({ promptsDefaultStage: stage })).toThrow();
    expect(() => decodeClientSettingsPatch({ promptsDefaultStage: stage })).toThrow();
  });
});
