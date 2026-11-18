import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ServerSettings, ServerSettingsPatch } from "../settings.ts";

describe("Workstream auto-route environment settings", () => {
  it("defaults on with no exclusions, and the patch accepts off and an exclusion list", () => {
    const decode = Schema.decodeUnknownSync(ServerSettings);
    const patch = Schema.decodeUnknownSync(ServerSettingsPatch);
    const encode = Schema.encodeSync(ServerSettings);
    const original = decode({});
    expect(original.workstreamAutoRouteEnabled).toBe(true);
    expect(original.workstreamAutoRouteExcludedIds).toEqual([]);
    const off = decode({
      ...encode(original),
      ...patch({ workstreamAutoRouteEnabled: false, workstreamAutoRouteExcludedIds: ["w1"] }),
    });
    expect(decode(encode(off)).workstreamAutoRouteEnabled).toBe(false);
    expect(decode(encode(off)).workstreamAutoRouteExcludedIds).toEqual(["w1"]);
    expect(() => patch({ workstreamAutoRouteEnabled: "false" })).toThrow();
    expect(patch({ workModeEnabled: true })).not.toHaveProperty("workstreamAutoRouteEnabled");
  });
});
