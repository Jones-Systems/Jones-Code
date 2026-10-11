import { describe, expect, it } from "vite-plus/test";
import { qualifiedServerCapability } from "./serverCapability.ts";
import { JonesUpdater } from "./JonesUpdater.ts";

const ready = {
  supported: true,
  qualifiedRuntime: true,
  launcherManaged: true,
  qualifiedStaging: true,
  qualifiedUpdates: true,
  hasCurrentVersion: true,
  hasStage: true,
  hasInstall: true,
  hasEnvironment: true,
  restoreFailed: false,
};

describe("qualified server update capability", () => {
  it("permits checking a receipt-bearing foreground server but never downloads", async () => {
    const capability = qualifiedServerCapability({ ...ready, launcherManaged: false });
    expect(capability).toEqual({
      check: true,
      download: false,
      install: false,
      reason: "bootstrap-required",
    });
    let nativeEffects = 0;
    const updater = new JonesUpdater(
      {
        initialState: { source: "jones-actions", channel: "jones-main", phase: "no-new", capability },
        platform: "linux",
        architecture: "x64",
        cacheRoot: "unused-capability-fixture",
        installedSource: async () => "a".repeat(40),
        stage: async () => {
          nativeEffects += 1;
          throw new Error("unavailable staging must never run");
        },
        install: async () => {
          nativeEffects += 1;
        },
      },
      {
        check: async () => {
          throw new Error("no network in this test");
        },
        stage: async () => {
          throw new Error("download must never run");
        },
      },
    );
    expect((await updater.download({ artifactId: 1, sourceSha: "b".repeat(40) })).phase).toBe(
      "no-new",
    );
    expect(nativeEffects).toBe(0);
  });

  it("requires every staging guard while retaining checks", () => {
    for (const missing of ["qualifiedStaging", "hasCurrentVersion", "hasStage"] as const) {
      expect(qualifiedServerCapability({ ...ready, [missing]: false })).toMatchObject({
        check: true,
        download: false,
        reason: "bootstrap-required",
      });
    }
    expect(qualifiedServerCapability(ready)).toEqual({ check: true, download: true, install: true });
  });

  it("keeps staging separate from activation and rejects unsupported/unqualified hosts", () => {
    for (const patch of [
      { qualifiedUpdates: false },
      { hasInstall: false },
      { hasEnvironment: false },
      { restoreFailed: true },
    ]) {
      expect(qualifiedServerCapability({ ...ready, ...patch })).toEqual({
        check: true,
        download: true,
        install: false,
        reason: "bootstrap-required",
      });
    }
    for (const patch of [{ supported: false }, { qualifiedRuntime: false }]) {
      expect(qualifiedServerCapability({ ...ready, ...patch })).toMatchObject({
        check: false,
        download: false,
        install: false,
      });
    }
  });
});
