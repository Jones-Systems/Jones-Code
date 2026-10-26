import { describe, expect, it } from "vite-plus/test";
import type { T3WorkstreamListResult } from "@t3tools/contracts";
import type { WorkstreamsNativeReadEnvelope } from "@t3tools/contracts/jones/workstreamsNativeRead";
import { memoryWorkstreamOperationStorage } from "../../jones/workstreams/operations.ts";
import {
  nativeWorkstreamReadLabel,
  nativeWorkstreamsCapability,
  nativeWorkstreamsBlock,
  nativeWorkstreamGroupKey,
  nativeWorkstreamsMessage,
  assertNativeWorkstreamBinding,
  validateNativeWorkstreamRead,
  NativeWorkstreamReadCache,
  type NativeWorkstreamsCapability,
} from "./native.ts";

export const capability: NativeWorkstreamsCapability = {
  contract: "workstreams-native/1.0.0",
  mode: "native-home",
  reads: ["workstreams-read/1.0.0"],
  placement_proofs: ["workstreams-t3-placement/1.0.0", "workstreams-t3-placement/2.0.0"],
  registry: {
    registry_instance_id: "8cdd84b6-81c5-4c88-9d13-5d324abaf662",
    owner_id: "owner",
    canonical_origin: "https://registry.test",
    home_environment_id: "env:a",
    server_generation: 7,
  },
};
const data: T3WorkstreamListResult = {
  binding: {
    registryId: "https://registry.test",
    ownerId: "owner",
    principalId: "principal",
    authorizationRevision: 1,
    serverGeneration: 7,
    registryVersion: 11,
    permissions: ["workstreams:read"],
    contractVersion: "workstreams/1.0.0",
    contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557",
  },
  items: [],
  nextCursor: null,
  source: "live",
  stale: false,
};
const read: WorkstreamsNativeReadEnvelope = {
  contract: "workstreams-read/1.0.0",
  registry: {
    registry_instance_id: capability.registry!.registry_instance_id,
    owner_id: "owner",
    canonical_origin: "https://registry.test",
    server_generation: 7,
    registry_version: 11,
  },
  provenance: {
    authority: "native-home",
    home_environment_id: "env:a",
    served_by_environment_id: "env:a",
    source: "live",
    observed_at: "2026-10-25T00:00:00Z",
    stale: false,
  },
  coverage: {
    registry: "complete",
    sources: [
      {
        source_instance_id: "env:a",
        mapping: "mapped",
        attestation: "current",
        observed_at: "2026-10-25T00:00:00Z",
      },
    ],
  },
};
describe("native Workstream client authority", () => {
  it("keeps missing provenance unknown and labels stale coverage explicitly", () => {
    expect(nativeWorkstreamReadLabel(null)).toContain("unavailable");
    expect(
      nativeWorkstreamReadLabel({
        ...read,
        provenance: { ...read.provenance, source: "cache", stale: true },
        coverage: { ...read.coverage, registry: "stale" },
      }),
    ).toContain("Stale · Read-only");
  });

  it("keeps old capability decoding on the legacy path", () => {
    expect(nativeWorkstreamsCapability({})).toBeNull();
    expect(nativeWorkstreamsBlock(null)).toBeNull();
  });
  it("groups home and member by registry, owner and generation", () => {
    const member = { ...capability, mode: "native-member" as const, home_link: "ok" as const };
    expect(nativeWorkstreamGroupKey(member)).toBe(nativeWorkstreamGroupKey(capability));
    for (const change of [
      { owner_id: "other" },
      { server_generation: 8 },
      { registry_instance_id: "another" },
    ])
      expect(
        nativeWorkstreamGroupKey({
          ...capability,
          registry: { ...capability.registry!, ...change },
        }),
      ).not.toBe(nativeWorkstreamGroupKey(capability));
  });
  it.each(["missing", "expired", "unknown"] as const)(
    "refuses a %s home link without claiming absence",
    (home_link) => {
      expect(nativeWorkstreamsBlock({ ...capability, mode: "native-member", home_link })).toContain(
        `home link ${home_link}`,
      );
    },
  );
  it("fences transition and disabled modes", () => {
    expect(nativeWorkstreamsBlock({ ...capability, mode: "transition-fenced" })).toContain(
      "fenced",
    );
    expect(nativeWorkstreamsBlock({ ...capability, mode: "disabled" })).toContain("disabled");
  });
  it("rejects mismatched native identity and generation before publication", () => {
    for (const change of [
      { registryId: "https://other.test" },
      { ownerId: "other" },
      { serverGeneration: 8 },
    ])
      expect(() =>
        assertNativeWorkstreamBinding(capability, { ...data.binding, ...change }),
      ).toThrow("changed");
  });
  it("requires native provenance at the same home, generation and registry version", () => {
    expect(validateNativeWorkstreamRead(read, capability, data)).toEqual(read);
    for (const changed of [
      { ...read, registry: { ...read.registry, registry_version: 12 } },
      { ...read, registry: { ...read.registry, server_generation: 8 } },
      { ...read, provenance: { ...read.provenance, home_environment_id: "env:b" } },
      { ...read, provenance: { ...read.provenance, source: "cache", stale: true } },
    ])
      expect(() => validateNativeWorkstreamRead(changed, capability, data)).toThrow();
    expect(() =>
      validateNativeWorkstreamRead(read, capability, { ...data, nextCursor: "next" }),
    ).toThrow();
  });
  it("retains only stale memory projections and purges on authorization loss", () => {
    const cache = new NativeWorkstreamReadCache();
    cache.write(capability, data, read);
    const cached = cache.read(capability, data.binding)!;
    expect(cached.data.binding).not.toBe(data.binding);
    expect(cached.data.binding.permissions).not.toBe(data.binding.permissions);
    expect(cached.read.coverage.sources[0]).not.toBe(read.coverage.sources[0]);
    expect(cached.data).toMatchObject({ source: "cache", stale: true });
    expect(cached.read).toMatchObject({
      provenance: { source: "cache", stale: true },
      coverage: { registry: "stale" },
    });
    expect(cache.read(capability, { ...data.binding, principalId: "other" })).toBeNull();
    expect(cache.read(capability, { ...data.binding, authorizationRevision: 2 })).toBeNull();
    expect(
      cache.read(
        { ...capability, registry: { ...capability.registry!, server_generation: 8 } },
        data.binding,
      ),
    ).toBeNull();
    cache.purge();
    expect(cache.read(capability, data.binding)).toBeNull();
  });
  it("does not persist operation requests between native memory stores", async () => {
    const storage = memoryWorkstreamOperationStorage();
    await storage.save("protected request");
    expect(await storage.load()).toBe("protected request");
    expect(await memoryWorkstreamOperationStorage().load()).toBeNull();
  });
  it("gives actionable native failures and preserves unknown codes", () => {
    expect(
      nativeWorkstreamsMessage({ code: "home_route_required", home_environment_id: "env:home" }),
    ).toContain("env:home");
    expect(nativeWorkstreamsMessage({ code: "key_unavailable" })).toContain("configured key");
    expect(nativeWorkstreamsMessage({ code: "generation_mismatch" })).toContain("original home");
    expect(nativeWorkstreamsMessage({ code: "new-code" })).toBeNull();
  });
});
