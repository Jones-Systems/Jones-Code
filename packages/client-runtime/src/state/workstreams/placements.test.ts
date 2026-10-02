import { describe, expect, it } from "vite-plus/test";
import type {
  T3PlacementResult,
  T3ThreadPlacement,
  T3WorkstreamListResult,
} from "@t3tools/contracts";
import { loadLiveT3Placements } from "./placements.ts";

const now = Date.parse("2026-09-12T12:00:00Z");
const metadata: T3WorkstreamListResult = {
  binding: {
    registryId: "registry",
    ownerId: "owner",
    principalId: "principal",
    authorizationRevision: 3,
    serverGeneration: 2,
    registryVersion: 8,
    permissions: ["workstreams:read"],
    contractVersion: "workstreams/1.0.0",
    contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557",
  },
  items: [],
  nextCursor: null,
  source: "live",
  stale: false,
};
const item = (index: number): T3ThreadPlacement => ({
  membership_id: `membership:${String(index).padStart(3, "0")}`,
  workstream_id: "workstream:1",
  native_reference_id: `reference:${String(index).padStart(3, "0")}`,
  kind: "primary",
  source_instance_id: "environment:1",
  native_thread_id: `thread:${index}`,
  attestation_version: 2,
  attested_at: "2026-09-12T11:00:00.000Z",
  expires_at: "2026-09-12T13:00:00.000Z",
  evidence_sha256: "a".repeat(64),
  source_binding_version: 4,
  authority_namespace: "t3:store",
  store_generation: 9,
});
const identities = [0, 1].map((index) => ({
  source_instance_id: "environment:1",
  native_thread_id: `thread:${index}`,
}));
const page = (index: number): T3PlacementResult => ({
  page: {
    inventory_sha256: "572b250634831167046e991dee3563bc2e35b9325033ac71dcd332940484b85d",
    context: {
      owner_id: "owner",
      principal_id: "principal",
      authorization_revision: 3,
      server_generation: 2,
      registry_version: 8,
    },
    items: [item(index)],
    next_cursor: null,
  },
  trustedEnvironments: [],
  readiness: "trust-provider-required",
});

describe("live complete placement assembly", () => {
  it("hashes the same canonical inventory bytes with browser and injected SHA-256", async () => {
    const expectedBytes = new TextEncoder().encode(
      '[["environment:1","thread:0"],["environment:1","thread:1"]]',
    );
    const observedBytes: Uint8Array[] = [];
    const nativeSha256 = async (bytes: Uint8Array) => {
      observedBytes.push(bytes);
      return Uint8Array.from(
        "572b250634831167046e991dee3563bc2e35b9325033ac71dcd332940484b85d".match(/../g)!,
        (hex) => Number.parseInt(hex, 16),
      );
    };
    const browser = await loadLiveT3Placements(
      metadata,
      identities,
      async () => page(0),
      () => now,
    );
    const injected = await loadLiveT3Placements(
      metadata,
      identities.toReversed(),
      async () => page(0),
      () => now,
      nativeSha256,
    );
    expect(observedBytes).toEqual([expectedBytes]);
    expect(injected).toEqual(browser);
  });

  it("rejects an injected wrong digest and rejects invalid digest lengths before loading", async () => {
    await expect(
      loadLiveT3Placements(
        metadata,
        identities,
        async () => page(0),
        () => now,
        async () => new Uint8Array(32),
      ),
    ).rejects.toThrow("Placement revision changed.");
    let loads = 0;
    for (const length of [0, 31, 33]) {
      await expect(
        loadLiveT3Placements(
          metadata,
          identities,
          async () => {
            loads++;
            return page(0);
          },
          () => now,
          async () => new Uint8Array(length),
        ),
      ).rejects.toThrow("Invalid SHA-256 digest length.");
    }
    expect(loads).toBe(0);
  });

  it("loads one complete result and never derives native trust from registry evidence", async () => {
    let calls = 0;
    const result = await loadLiveT3Placements(
      metadata,
      identities,
      async () => {
        calls++;
        return { ...page(0), page: { ...page(0).page, items: [item(0), item(1)] } };
      },
      () => now,
    );
    expect(result.items).toHaveLength(2);
    expect(result.trustedEnvironments).toEqual([]);
    expect(result.readiness).toBe("trust-provider-required");
    expect(calls).toBe(1);
  });
  it("rejects every changed binding field and wrong inventory digest", async () => {
    for (const field of [
      "owner_id",
      "principal_id",
      "authorization_revision",
      "server_generation",
      "registry_version",
    ] as const) {
      const value = page(0);
      await expect(
        loadLiveT3Placements(
          metadata,
          identities,
          async () => ({
            ...value,
            page: {
              ...value.page,
              context: {
                ...value.page.context,
                [field]: typeof value.page.context[field] === "string" ? "foreign" : 99,
              },
            },
          }),
          () => now,
        ),
      ).rejects.toThrow("revision changed");
    }
    await expect(
      loadLiveT3Placements(
        metadata,
        identities,
        async () => ({
          ...page(0),
          page: { ...page(0).page, inventory_sha256: "b".repeat(64) },
        }),
        () => now,
      ),
    ).rejects.toThrow("revision changed");
  });
  it("rejects incomplete, repeated, unordered, expired, unrelated, or excess results", async () => {
    for (const value of [
      { ...page(0), page: { ...page(0).page, next_cursor: "next" } },
      { ...page(0), page: { ...page(0).page, items: [item(0), item(0)] } },
      { ...page(0), page: { ...page(0).page, items: [item(1), item(0)] } },
      {
        ...page(0),
        page: { ...page(0).page, items: [{ ...item(0), expires_at: "2026-09-12T12:00:00Z" }] },
      },
      { ...page(0), page: { ...page(0).page, items: [item(2)] } },
      { ...page(0), page: { ...page(0).page, items: [{ ...item(0), content: "private" }] } },
      { ...page(0), content: "private" },
    ]) {
      let calls = 0;
      await expect(
        loadLiveT3Placements(
          metadata,
          identities,
          async () => {
            calls++;
            return value as T3PlacementResult;
          },
          () => now,
        ),
      ).rejects.toThrow();
      expect(calls).toBe(1);
    }
  });
  it("rejects invalid trust snapshots and stale metadata without loading", async () => {
    const trust = {
      environmentId: "environment:1",
      authorityNamespace: "store",
      storeGeneration: 1,
    };
    for (const value of [
      { ...page(0), trustedEnvironments: [trust] },
      { ...page(0), readiness: "ready" as const, trustedEnvironments: [trust, trust] },
    ])
      await expect(
        loadLiveT3Placements(
          metadata,
          identities,
          async () => value,
          () => now,
        ),
      ).rejects.toThrow();
    let calls = 0;
    await expect(
      loadLiveT3Placements(
        { ...metadata, source: "cache" },
        identities,
        async () => {
          calls++;
          return page(0);
        },
        () => now,
      ),
    ).rejects.toThrow();
    expect(calls).toBe(0);
  });
  it("rejects an over-bound inventory before calling the server", async () => {
    let calls = 0;
    await expect(
      loadLiveT3Placements(
        metadata,
        Array.from({ length: 1001 }, (_, i) => ({
          source_instance_id: "env",
          native_thread_id: String(i),
        })),
        async () => {
          calls++;
          return page(0);
        },
        () => now,
      ),
    ).rejects.toThrow();
    expect(calls).toBe(0);
  });
});
