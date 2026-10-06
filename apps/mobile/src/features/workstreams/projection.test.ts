import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { EnvironmentId } from "@t3tools/contracts";
import {
  nativeWorkstreamThreadKey,
  WORKSTREAM_TINT_PALETTE,
} from "@t3tools/client-runtime/state/workstreams";
import { projectMobileWorkstreams, type MobileWorkstreamSnapshot } from "./projection";
import { data, placements, now, thread } from "./actions.fixtures";

function snapshot(environmentId: string): MobileWorkstreamSnapshot {
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
  };
}
describe("mobile registry projection", () => {
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
