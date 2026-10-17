import { describe, expect, it } from "vite-plus/test";
import type { TrustedT3PlacementEnvironment } from "@t3tools/contracts";
import {
  buildConnectedEnvironmentTrust,
  type ConnectedEnvironmentTrustInput,
} from "./environmentTrust.ts";

const descriptor = (
  environmentId: string,
  authorityNamespace = `t3:${environmentId}`,
  storeGeneration = 1,
): TrustedT3PlacementEnvironment => ({ environmentId, authorityNamespace, storeGeneration });

const entry = (
  environmentId: string,
  nativeStoreAuthority: TrustedT3PlacementEnvironment | undefined,
  live = true,
): ConnectedEnvironmentTrustInput => ({
  environmentId,
  config: {
    environment: { environmentId },
    ...(nativeStoreAuthority === undefined ? {} : { nativeStoreAuthority }),
  },
  live,
});

const entries = (trust: ReturnType<typeof buildConnectedEnvironmentTrust>) => ({
  trusted: [...trust.trusted],
  conflicts: [...trust.conflicts],
});

describe("buildConnectedEnvironmentTrust", () => {
  it("trusts a live descriptor for its own environment", () => {
    expect(
      entries(
        buildConnectedEnvironmentTrust({ environments: [entry("remote", descriptor("remote"))] }),
      ),
    ).toEqual({
      trusted: [["remote", { authorityNamespace: "t3:remote", storeGeneration: 1 }]],
      conflicts: [],
    });
  });

  it("ignores a cached or restored descriptor", () => {
    const trust = buildConnectedEnvironmentTrust({
      environments: [entry("remote", descriptor("remote"), false)],
    });
    expect(entries(trust)).toEqual({ trusted: [], conflicts: [] });
  });

  it("never applies one environment's descriptor to another", () => {
    const trust = buildConnectedEnvironmentTrust({
      environments: [
        entry("remote", descriptor("other")),
        {
          environmentId: "second",
          config: {
            environment: { environmentId: "third" },
            nativeStoreAuthority: descriptor("second"),
          },
          live: true,
        },
      ],
    });
    expect(entries(trust)).toEqual({ trusted: [], conflicts: [] });
  });

  it("trusts once when placement and live config agree", () => {
    const trust = buildConnectedEnvironmentTrust({
      environments: [entry("primary", descriptor("primary", "t3:store", 9))],
      placementTrustedEnvironments: [descriptor("primary", "t3:store", 9)],
    });
    expect(entries(trust)).toEqual({
      trusted: [["primary", { authorityNamespace: "t3:store", storeGeneration: 9 }]],
      conflicts: [],
    });
  });

  it("reports a conflict and withholds trust when placement and live config disagree", () => {
    for (const live of [
      descriptor("primary", "t3:other", 9),
      descriptor("primary", "t3:store", 10),
    ]) {
      const trust = buildConnectedEnvironmentTrust({
        environments: [entry("primary", live)],
        placementTrustedEnvironments: [descriptor("primary", "t3:store", 9)],
      });
      expect(entries(trust)).toEqual({ trusted: [], conflicts: ["primary"] });
    }
  });

  it("does not let a cached descriptor contradict placement trust", () => {
    const trust = buildConnectedEnvironmentTrust({
      environments: [entry("primary", descriptor("primary", "t3:stale", 1), false)],
      placementTrustedEnvironments: [descriptor("primary", "t3:store", 9)],
    });
    expect(entries(trust)).toEqual({
      trusted: [["primary", { authorityNamespace: "t3:store", storeGeneration: 9 }]],
      conflicts: [],
    });
  });

  it("leaves absent, null and malformed descriptors unknown", () => {
    const trust = buildConnectedEnvironmentTrust({
      environments: [
        entry("absent", undefined),
        { environmentId: "missing", config: null, live: true },
        entry("malformed", { ...descriptor("malformed"), storeGeneration: 0 }),
      ],
      placementTrustedEnvironments: [],
    });
    expect(entries(trust)).toEqual({ trusted: [], conflicts: [] });
  });

  it("combines several environments deterministically", () => {
    const environments = [
      entry("zeta", descriptor("zeta")),
      entry("alpha", descriptor("alpha")),
      entry("cached", descriptor("cached"), false),
      entry("primary", descriptor("primary", "t3:new", 2)),
      entry("absent", undefined),
    ];
    const placementTrustedEnvironments = [descriptor("primary", "t3:store", 9)];
    const trust = buildConnectedEnvironmentTrust({ environments, placementTrustedEnvironments });
    expect(entries(trust)).toEqual({
      trusted: [
        ["alpha", { authorityNamespace: "t3:alpha", storeGeneration: 1 }],
        ["zeta", { authorityNamespace: "t3:zeta", storeGeneration: 1 }],
      ],
      conflicts: ["primary"],
    });
    expect(
      entries(
        buildConnectedEnvironmentTrust({
          environments: environments.toReversed(),
          placementTrustedEnvironments,
        }),
      ),
    ).toEqual(entries(trust));
  });

  it("treats disagreeing duplicate entries for one environment as a conflict", () => {
    const trust = buildConnectedEnvironmentTrust({
      environments: [
        entry("remote", descriptor("remote", "t3:a", 1)),
        entry("remote", descriptor("remote", "t3:b", 1)),
      ],
    });
    expect(entries(trust)).toEqual({ trusted: [], conflicts: ["remote"] });
  });

  it("trusts primary-only placement entries with no configs", () => {
    for (const environments of [[], [{ environmentId: "primary", config: null, live: false }]]) {
      const trust = buildConnectedEnvironmentTrust({
        environments,
        placementTrustedEnvironments: [descriptor("primary", "t3:store", 9)],
      });
      expect(entries(trust)).toEqual({
        trusted: [["primary", { authorityNamespace: "t3:store", storeGeneration: 9 }]],
        conflicts: [],
      });
    }
  });
});
