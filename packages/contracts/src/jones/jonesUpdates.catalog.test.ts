import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { JonesUpdateBuildList } from "./jonesUpdates.ts";

describe("qualified build catalog wire bounds", () => {
  it("admits five retained builds and explicit empty/unavailable lists, but refuses a sixth", () => {
    const decode = Schema.decodeUnknownSync(JonesUpdateBuildList);
    const builds = Array.from({ length: 5 }, (_, index) => ({
      repository: "Jones-Systems/Jones-Code",
      sourceSha: "a".repeat(40),
      sourceTree: "b".repeat(40),
      workflow: "artifact-desktop-mac.yml",
      runId: index + 1,
      runAttempt: 1,
      artifactId: index + 1,
      artifactDigest: "c".repeat(64),
      platform: "darwin",
      architecture: "arm64",
      version: "0.1.2",
    }));
    expect(decode({ builds, unavailable: false }).builds).toEqual(builds);
    expect(decode({ builds: [], unavailable: true })).toEqual({ builds: [], unavailable: true });
    expect(decode({ builds: [], unavailable: false })).toEqual({ builds: [], unavailable: false });
    expect(() => decode({ builds: [...builds, builds[0]], unavailable: false })).toThrow();
  });
});
