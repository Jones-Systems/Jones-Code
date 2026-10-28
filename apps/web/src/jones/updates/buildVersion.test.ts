import { describe, expect, it } from "vite-plus/test";
import { clientServerBuildLabels, outcomeBuildRefs } from "./buildPairs";
import { jonesBuildLabel } from "./buildVersion";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";

describe("web build presentation", () => {
  it("uses compact day labels for client and server without changing identity", () => {
    const builds = {
      clientVersion: "client",
      serverVersion: "server",
      clientBuildStartedAt: "2026-10-20T21:13:43Z",
      serverBuildStartedAt: "2026-10-21T22:14:00Z",
    };
    const labels = clientServerBuildLabels(builds);
    expect(labels.clientLabel.text).toBe("20·1713");
    expect(labels.serverLabel.text).toBe("21·1814");
    expect(builds.clientVersion).toBe("client");
  });
  it("disambiguates paired builds and falls back without using workflow digits", () => {
    const labels = clientServerBuildLabels({
      clientVersion: "first",
      serverVersion: "second",
      clientBuildStartedAt: "2026-10-20T21:13:43Z",
      serverBuildStartedAt: "2026-10-20T21:13:47Z",
    });
    expect(labels.clientLabel.text).toBe("1020·1713:43");
    expect(labels.serverLabel.text).toBe("1020·1713:47");
    expect(
      jonesBuildLabel({
        version: "0.0.45-preview.20261016.38974945620.1",
        sourceSha: "a".repeat(40),
      }).text,
    ).toBe("aaaaaaa");
  });
  it("associates outcomes only with an exact current or offered full version", () => {
    const state = {
      currentVersion: "current",
      currentBuildStartedAt: "2026-10-20T21:13:43Z",
      provenance: {
        version: "offered",
        buildStartedAt: "2026-10-20T21:13:47Z",
        sourceSha: "b".repeat(40),
      },
      outcome: { fromVersion: "current", targetVersion: "offered" },
    } as unknown as JonesUpdateState;
    expect(outcomeBuildRefs(state).map((ref) => ref.startedAt)).toEqual([
      state.currentBuildStartedAt,
      state.provenance!.buildStartedAt,
    ]);
    expect(
      outcomeBuildRefs({
        ...state,
        outcome: {
          status: "committed",
          fromVersion: "current-other",
          targetVersion: "offered-other",
        },
      }),
    ).toEqual([{ version: "current-other" }, { version: "offered-other" }]);
  });
});
