import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { EnvironmentId } from "@t3tools/contracts";
import {
  nativeWorkstreamThreadKey,
  WORKSTREAM_TINT_PALETTE,
} from "@t3tools/client-runtime/state/workstreams";
import {
  sameMobileWorkstreamAuthority,
  projectMobileWorkstreams,
  type MobileWorkstreamSnapshot,
} from "./projection";
import { assignmentIndexFor, data, placements, now, primary, thread } from "./actions.fixtures";

function snapshot(
  environmentId: string,
  overrides: Partial<MobileWorkstreamSnapshot> = {},
): MobileWorkstreamSnapshot {
  return {
    prepared: {
      environmentId: Schema.decodeUnknownSync(EnvironmentId)(environmentId),
      label: environmentId,
      httpBaseUrl: "https://test.invalid",
      socketUrl: "wss://test.invalid",
      httpAuthorization: null,
      target: {
        _tag: "PrimaryConnectionTarget",
        environmentId: Schema.decodeUnknownSync(EnvironmentId)(environmentId),
        label: environmentId,
        httpBaseUrl: "https://test.invalid",
        wsBaseUrl: "wss://test.invalid",
      },
    },
    generation: 1,
    data,
    identityKeys: new Set([nativeWorkstreamThreadKey(environmentId, "thread")]),
    placements: {
      ...placements,
      items: placements.items.map((item) => ({ ...item, source_instance_id: environmentId })),
      trustedEnvironments: [{ environmentId, authorityNamespace: "authority", storeGeneration: 1 }],
    },
    index: null,
    stale: false,
    ...overrides,
  };
}
describe("mobile registry projection", () => {
  it("keeps in-flight authority valid across appearance-only refreshes and rejects replaced core state", () => {
    const original = snapshot("env:a");
    const appearanceOnly = {
      ...original,
      appearance: {
        owner_id: data.binding.ownerId,
        server_generation: data.binding.serverGeneration,
        permissions: ["workstreams:read"] as const,
        items: [],
      },
    };
    expect(sameMobileWorkstreamAuthority(appearanceOnly, original)).toBe(true);
    expect(
      sameMobileWorkstreamAuthority(
        { ...appearanceOnly, generation: original.generation + 1 },
        original,
      ),
    ).toBe(false);
    expect(
      sameMobileWorkstreamAuthority({ ...appearanceOnly, data: { ...original.data } }, original),
    ).toBe(false);
    expect(sameMobileWorkstreamAuthority({ ...appearanceOnly, placements: null }, original)).toBe(
      false,
    );
    expect(
      sameMobileWorkstreamAuthority({ ...appearanceOnly, identityKeys: new Set() }, original),
    ).toBe(false);
    expect(sameMobileWorkstreamAuthority(snapshot("env:b"), original)).toBe(false);
  });
  it("uses shared saved colors for borders while preserving automatic indicators", () => {
    const original = snapshot("env:a");
    const before = projectMobileWorkstreams([original], [thread], now);
    const saved = {
      ...original,
      appearance: {
        owner_id: data.binding.ownerId,
        server_generation: data.binding.serverGeneration,
        permissions: ["workstreams:read"] as const,
        items: data.items.map((item) => ({
          workstream_id: item.workstreamId,
          border_color: "#123ABC",
          version: 1,
        })),
      },
    };
    const after = projectMobileWorkstreams([saved], [thread], now);
    expect(after.groups[0]?.borderColor).toBe("#123ABC");
    expect(after.groups[0]?.color).toBe(before.groups[0]?.color);
    expect(after.groups[0]?.threadKeys).toEqual(before.groups[0]?.threadKeys);
  });
  it("groups cross-environment repositories without conflating repeated thread IDs", () => {
    const other = { ...thread, environmentId: "env:b", projectId: "another-repo" };
    const result = projectMobileWorkstreams(
      [snapshot("env:a"), snapshot("env:b")],
      [thread, other],
      now,
    );
    expect(result.groups[0]?.threadKeys).toEqual(
      new Set([
        nativeWorkstreamThreadKey("env:a", "thread"),
        nativeWorkstreamThreadKey("env:b", "thread"),
      ]),
    );
    expect(result.secondaryLabelsByKey.size).toBe(2);
  });
  it("never combines registry/owner namespaces merely because IDs and names match", () => {
    const other = snapshot("env:b");
    const result = projectMobileWorkstreams(
      [
        snapshot("env:a"),
        { ...other, data: { ...data, binding: { ...data.binding, registryId: "other-registry" } } },
      ],
      [thread, { ...thread, environmentId: "env:b" }],
      now,
    );
    expect(result.groups.filter((group) => group.workstream.workstreamId === "alpha")).toHaveLength(
      2,
    );
  });
  it("keeps a conflict rejected after a third environment repeats the first version", () => {
    const second = snapshot("env:b");
    const result = projectMobileWorkstreams(
      [
        snapshot("env:a"),
        {
          ...second,
          data: { ...data, items: data.items.map((item) => ({ ...item, version: 4 })) },
        },
        snapshot("env:c"),
      ],
      [thread, { ...thread, environmentId: "env:b" }, { ...thread, environmentId: "env:c" }],
      now,
    );
    expect(result.groups).toEqual([]);
  });
  it("expired or missing attestations leave all native keys ungrouped", () => {
    const result = projectMobileWorkstreams([snapshot("env:a")], [thread], now + 2 * 86400000);
    expect(result.groups.flatMap((group) => [...group.threadKeys])).toEqual([]);
    expect(
      projectMobileWorkstreams([{ ...snapshot("env:a"), placements: null }], [thread], now).groups,
    ).toEqual([]);
  });
  it("hides stale authorization and cached metadata", () => {
    const current = snapshot("env:a");
    expect(
      projectMobileWorkstreams([{ ...current, data: { ...data, stale: true } }], [thread], now)
        .groups,
    ).toEqual([]);
    expect(
      projectMobileWorkstreams(
        [
          {
            ...current,
            placements: {
              ...placements,
              context: { ...placements.context, authorization_revision: 2 },
            },
          },
        ],
        [thread],
        now,
      ).groups,
    ).toEqual([]);
  });
  it("uses valid colors with the shared five-family palette", () => {
    expect(WORKSTREAM_TINT_PALETTE).toHaveLength(5);
    expect(
      projectMobileWorkstreams([snapshot("env:a")], [thread], now).groups.every((group) =>
        /^#[0-9a-f]{6}$/.test(group.color),
      ),
    ).toBe(true);
  });
});

describe("mobile durable assignment projection", () => {
  const key = nativeWorkstreamThreadKey("env:a", "thread");
  const indexed = (overrides: Partial<MobileWorkstreamSnapshot> = {}) =>
    snapshot("env:a", { index: assignmentIndexFor(), ...overrides });
  const keysOf = (result: ReturnType<typeof projectMobileWorkstreams>, workstreamId: string) =>
    result.groups.find((group) => group.workstream.workstreamId === workstreamId)?.threadKeys ??
    new Set<string>();

  it("keeps a stored assignment grouped after its proof expires, downgraded to recorded", () => {
    const later = projectMobileWorkstreams([indexed()], [thread], now + 2 * 86400000);
    expect(keysOf(later, "alpha")).toEqual(new Set([key]));
    expect(later.assuranceByKey.get(key)).toBe("recorded");
    expect(projectMobileWorkstreams([indexed()], [thread], now).assuranceByKey.get(key)).toBe(
      "verified",
    );
  });

  it("groups from the index when the placement read failed", () => {
    const result = projectMobileWorkstreams([indexed({ placements: null })], [thread], now);
    expect(keysOf(result, "alpha")).toEqual(new Set([key]));
    expect(result.assuranceByKey.get(key)).toBe("recorded");
  });

  it("groups assignments beyond the first 1,000 of more than 1,000 native threads", () => {
    const threads = Array.from({ length: 1_500 }, (_, position) => ({
      ...thread,
      id: `thread-${String(position).padStart(4, "0")}`,
    }));
    const assigned = threads.slice(1_000);
    const result = projectMobileWorkstreams(
      [
        snapshot("env:a", {
          placements: { ...placements, items: [] },
          index: assignmentIndexFor(
            assigned.map((item) => ({
              environmentId: "env:a",
              threadId: item.id,
              workstreamId: "beta",
            })),
          ),
        }),
      ],
      threads,
      now + 2 * 86400000,
    );
    expect(keysOf(result, "beta").size).toBe(500);
    expect(keysOf(result, "beta").has(nativeWorkstreamThreadKey("env:a", "thread-1499"))).toBe(
      true,
    );
    expect(result.confirmedUnassignedKeys.size).toBe(1_000);
  });

  it("groups a proof attested slightly after a coarse client clock", () => {
    const fresh = {
      ...primary,
      attested_at: new Date(now + 1_000).toISOString(),
      expires_at: new Date(now + 301_000).toISOString(),
    };
    const result = projectMobileWorkstreams(
      [indexed({ placements: { ...placements, items: [fresh] } })],
      [thread],
      now,
    );
    expect(keysOf(result, "alpha")).toEqual(new Set([key]));
    expect(result.confirmedUnassignedKeys.has(key)).toBe(false);
  });

  it("confirms Unassigned only from a complete, live index", () => {
    const loose = { ...thread, id: "loose" };
    const looseKey = nativeWorkstreamThreadKey("env:a", "loose");
    const complete = projectMobileWorkstreams([indexed()], [thread, loose], now);
    expect(complete.confirmedUnassignedKeys).toEqual(new Set([looseKey]));
    expect(complete.assuranceByKey.get(looseKey)).toBe("unassigned");

    const partial = projectMobileWorkstreams(
      [indexed({ index: assignmentIndexFor(undefined, false) })],
      [thread, loose],
      now,
    );
    expect(keysOf(partial, "alpha")).toEqual(new Set([key]));
    expect(partial.confirmedUnassignedKeys.size).toBe(0);
    expect(partial.assuranceByKey.get(looseKey)).toBe("unknown");

    const stale = projectMobileWorkstreams([indexed({ stale: true })], [thread, loose], now);
    expect(keysOf(stale, "alpha")).toEqual(new Set([key]));
    expect(stale.confirmedUnassignedKeys.size).toBe(0);
    expect(stale.stale).toBe(true);

    const cached = projectMobileWorkstreams(
      [indexed({ data: { ...data, stale: true } })],
      [thread, loose],
      now,
    );
    expect(keysOf(cached, "alpha")).toEqual(new Set([key]));
    expect(cached.confirmedUnassignedKeys.size).toBe(0);

    const legacy = projectMobileWorkstreams([snapshot("env:a")], [thread, loose], now);
    expect(legacy.confirmedUnassignedKeys.size).toBe(0);
  });

  it("flags conflicting stored primaries instead of grouping them", () => {
    const result = projectMobileWorkstreams(
      [
        indexed({
          index: assignmentIndexFor([
            { environmentId: "env:a", threadId: "thread", workstreamId: "alpha" },
            { environmentId: "env:a", threadId: "thread", workstreamId: "beta" },
          ]),
        }),
      ],
      [thread],
      now,
    );
    expect(result.groups.flatMap((group) => [...group.threadKeys])).toEqual([]);
    expect(result.conflictingThreadKeys).toEqual(new Set([key]));
    expect(result.confirmedUnassignedKeys.size).toBe(0);
  });
});

describe("mobile connected-environment display trust", () => {
  const remote = { ...thread, environmentId: "env:b" };
  const key = nativeWorkstreamThreadKey("env:b", "thread");
  const remoteSnapshot = (overrides: Partial<MobileWorkstreamSnapshot> = {}) =>
    snapshot("env:b", {
      placements: null,
      index: assignmentIndexFor([
        { environmentId: "env:b", threadId: "thread", workstreamId: "alpha" },
      ]),
      ...overrides,
    });
  const live = (environmentId: string, authorityNamespace = "authority", storeGeneration = 1) => ({
    environmentId,
    config: {
      environment: { environmentId },
      nativeStoreAuthority: { environmentId, authorityNamespace, storeGeneration },
    },
    live: true,
  });
  const keysOf = (result: ReturnType<typeof projectMobileWorkstreams>, workstreamId: string) =>
    result.groups.find((group) => group.workstream.workstreamId === workstreamId)?.threadKeys ??
    new Set<string>();

  it("verifies a remote environment's thread from its live descriptor, recorded otherwise", () => {
    const verified = projectMobileWorkstreams([remoteSnapshot()], [remote], now, [live("env:b")]);
    expect(keysOf(verified, "alpha")).toEqual(new Set([key]));
    expect(verified.assuranceByKey.get(key)).toBe("verified");

    const recorded = projectMobileWorkstreams([remoteSnapshot()], [remote], now);
    expect(keysOf(recorded, "alpha")).toEqual(new Set([key]));
    expect(recorded.assuranceByKey.get(key)).toBe("recorded");

    const disconnected = projectMobileWorkstreams(
      [remoteSnapshot({ stale: true })],
      [remote],
      now,
      [{ environmentId: "env:b", config: null, live: false }],
    );
    expect(keysOf(disconnected, "alpha")).toEqual(new Set([key]));
    expect(disconnected.assuranceByKey.get(key)).toBe("recorded");
  });

  it("never lets one environment's descriptor verify another environment's thread", () => {
    const result = projectMobileWorkstreams([remoteSnapshot()], [remote], now, [live("env:a")]);
    expect(result.assuranceByKey.get(key)).toBe("recorded");
  });

  it("flags a descriptor that disagrees with placement trust and blocks changes", () => {
    const placed = snapshot("env:b", {
      index: assignmentIndexFor([
        { environmentId: "env:b", threadId: "thread", workstreamId: "alpha" },
      ]),
    });
    const loose = { ...remote, id: "loose" };
    const result = projectMobileWorkstreams([placed], [remote, loose], now, [
      live("env:b", "authority", 2),
    ]);
    expect(result.groups.flatMap((group) => [...group.threadKeys])).toEqual([]);
    expect(result.assuranceByKey.get(key)).toBe("conflict");
    expect(result.conflictingThreadKeys).toEqual(
      new Set([key, nativeWorkstreamThreadKey("env:b", "loose")]),
    );
    expect(result.confirmedUnassignedKeys.size).toBe(0);
  });

  it("conflicts only the remote thread whose stored proof a live descriptor contradicts", () => {
    const result = projectMobileWorkstreams(
      [snapshot("env:a", { index: assignmentIndexFor() }), remoteSnapshot()],
      [thread, remote],
      now,
      [live("env:b", "other")],
    );
    expect(keysOf(result, "alpha")).toEqual(
      new Set([nativeWorkstreamThreadKey("env:a", "thread")]),
    );
    expect(result.assuranceByKey.get(key)).toBe("conflict");
    expect(result.conflictingThreadKeys).toEqual(new Set([key]));
  });
});
