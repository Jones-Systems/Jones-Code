import { describe, expect, it } from "vite-plus/test";
import {
  EnvironmentId,
  t3PlacementInventoryJson,
  type T3PlacementIdentity,
  type T3WorkstreamListResult,
} from "@t3tools/contracts";
import {
  NativeWorkstreamReader,
  type NativeReadScope,
} from "../../state/workstreams/nativeRead.ts";
import { liveT3PlacementsCurrent } from "../../state/workstreams/placements.ts";
import { loadNativePlacements, type NativePlacementConnection } from "./nativePlacements.ts";
import { PrimaryConnectionTarget } from "../../connection/model.ts";

const at = "2026-11-03T00:00:00Z";
const now = Date.parse(at);
const identity = { source_instance_id: "env:a", native_thread_id: "thread:a" };
const scope: NativeReadScope = {
  capability: {
    contract: "workstreams-native/1.0.0",
    mode: "native-member",
    home_link: "ok",
    reads: ["workstreams-read/1.0.0"],
    placement_proofs: ["workstreams-t3-placement/2.0.0"],
    registry: {
      registry_instance_id: "8cdd84b6-81c5-4c88-9d13-5d324abaf662",
      owner_id: "owner",
      canonical_origin: "https://registry.invalid",
      home_environment_id: "env:home",
      server_generation: 7,
    },
  },
  binding: {
    registryId: "https://registry.invalid",
    ownerId: "owner",
    principalId: "principal",
    authorizationRevision: 1,
    serverGeneration: 7,
    registryVersion: 11,
    permissions: ["workstreams:read"],
    contractVersion: "workstreams/1.0.0",
    contractManifest: "6d8da23d51c1bba024dddc4b8d4dd9a594d2f474affd73e4cc7de508b797f557",
  },
};
const metadata: T3WorkstreamListResult = {
  binding: scope.binding,
  items: [],
  nextCursor: null,
  source: "live",
  stale: false,
};
const sha256 = async (bytes: Uint8Array) =>
  Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes))), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
const item = () => ({
  membership_id: "membership:a",
  workstream_id: "workstream:a",
  native_reference_id: "reference:a",
  kind: "primary",
  ...identity,
  authority_namespace: "namespace:a",
  store_generation: 3,
  attestation_version: 1,
  source_binding_version: 1,
  evidence_sha256: "a".repeat(64),
  attested_at: "2026-11-02T23:59:00Z",
  expires_at: "2026-11-03T00:05:00Z",
  attestation_source: "native",
  evidence_contract: "workstreams-t3-provider/2.0.0",
});
async function response(identities: readonly T3PlacementIdentity[] = [identity]) {
  return {
    contract: "workstreams-native-home-read-result/1.0.0",
    route: "placements",
    placement_contract: "workstreams-t3-placement/2.0.0",
    authorization_revision: 1,
    value: {
      context: {
        owner_id: "owner",
        principal_id: "principal",
        authorization_revision: 1,
        server_generation: 7,
        registry_version: 11,
      },
      inventory_sha256: await sha256(
        new TextEncoder().encode(t3PlacementInventoryJson(identities)),
      ),
      items: [item()],
      next_cursor: null as string | null,
      provenance: {
        authority: "native-home",
        registry_instance_id: scope.capability.registry!.registry_instance_id,
        home_environment_id: "env:home",
        home_generation: 7,
        served_by_environment_id: "env:home",
        observed_at: at,
      },
    },
    envelope: {
      contract: "workstreams-read/1.0.0",
      registry: {
        registry_instance_id: scope.capability.registry!.registry_instance_id,
        owner_id: "owner",
        canonical_origin: "https://registry.invalid",
        server_generation: 7,
        registry_version: 11,
      },
      provenance: {
        authority: "native-home",
        home_environment_id: "env:home",
        served_by_environment_id: "env:home",
        source: "live",
        stale: false,
        observed_at: at,
      },
      coverage: {
        registry: "complete",
        sources: [
          {
            source_instance_id: "env:a",
            mapping: "mapped",
            attestation: "current",
            observed_at: at,
          },
        ],
      },
    },
  };
}
function harness() {
  const descriptor = {
    environmentId: "env:a",
    authorityNamespace: "namespace:a",
    storeGeneration: 3,
  };
  const original: NativePlacementConnection = {
    prepared: {
      environmentId: EnvironmentId.make("env:a"),
      label: "Source",
      httpBaseUrl: "https://source.invalid",
      socketUrl: "wss://source.invalid/ws",
      httpAuthorization: { _tag: "Bearer", token: "fixture" },
      target: new PrimaryConnectionTarget({
        environmentId: EnvironmentId.make("env:a"),
        label: "Source",
        httpBaseUrl: "https://source.invalid",
        wsBaseUrl: "wss://source.invalid",
      }),
    },
    generation: 2,
    session: { authenticated: true },
    config: { environment: { environmentId: "env:a" }, nativeStoreAuthority: descriptor },
  };
  const state = { current: true, source: original as NativePlacementConnection | null };
  const calls: unknown[] = [];
  let reply = async () => response();
  const reader = new NativeWorkstreamReader({
    scope,
    sha256,
    read: async (request) => {
      calls.push(request);
      return reply();
    },
  });
  const load = (overrides: Partial<Parameters<typeof loadNativePlacements>[0]> = {}) =>
    loadNativePlacements({
      scope,
      metadata,
      identities: [identity],
      sha256,
      servedByEnvironmentId: "env:member",
      lookup: (id) => (id === "env:a" ? state.source : null),
      checkCurrent: () => {
        if (!state.current) throw new Error("reader replaced");
      },
      complete: (request, signal) => reader.complete<"placements">(request, signal),
      now: () => now,
      ...overrides,
    });
  return {
    state,
    original,
    descriptor,
    calls,
    load,
    setReply: (next: typeof reply) => {
      reply = next;
    },
  };
}
describe("native placement consumer with independent live source trust", () => {
  it("negotiates v2 through the frozen reader and retains native evidence and home observations", async () => {
    const h = harness();
    const proof = await h.load();
    expect(proof.items).toEqual([item()]);
    expect(proof.trustedEnvironments).toEqual([h.descriptor]);
    expect(proof.native?.observations).toEqual([(await response()).value.provenance]);
    expect(liveT3PlacementsCurrent(proof)).toBe(true);
    expect(h.calls).toMatchObject([{ placement_contract: "workstreams-t3-placement/2.0.0" }]);
    expect(liveT3PlacementsCurrent(JSON.parse(JSON.stringify(proof)))).toBe(false);
  });
  it.each(["missing", "prepared", "generation", "session", "config", "descriptor"] as const)(
    "does not release a proof after original source %s replacement",
    async (kind) => {
      const h = harness();
      const started = Promise.withResolvers<void>();
      const reply = Promise.withResolvers<Awaited<ReturnType<typeof response>>>();
      h.setReply(() => {
        started.resolve();
        return reply.promise;
      });
      const reading = h.load();
      await started.promise;
      if (kind === "missing") h.state.source = null;
      if (kind === "prepared")
        h.state.source = { ...h.original, prepared: { ...h.original.prepared } };
      if (kind === "generation") h.state.source = { ...h.original, generation: 3 };
      if (kind === "session") h.state.source = { ...h.original, session: { authenticated: true } };
      if (kind === "config") h.state.source = { ...h.original, config: { ...h.original.config } };
      if (kind === "descriptor") h.descriptor.storeGeneration = 4;
      reply.resolve(await response());
      await expect(reading).rejects.toMatchObject({ code: "scope_changed" });
    },
  );
  it("downgrades a retained proof when its source or reader lifetime closes", async () => {
    const h = harness();
    const proof = await h.load();
    h.state.current = false;
    expect(liveT3PlacementsCurrent(proof)).toBe(false);
    h.state.current = true;
    h.state.source = null;
    expect(liveT3PlacementsCurrent(proof)).toBe(false);
  });
  it.each([
    "authority_namespace",
    "store_generation",
    "native_thread_id",
    "evidence_contract",
  ] as const)("rejects changed %s instead of trusting the page", async (field) => {
    const h = harness();
    h.setReply(async () => {
      const page = await response();
      if (field === "authority_namespace")
        page.value.items[0]!.authority_namespace = "namespace:forged";
      if (field === "store_generation") page.value.items[0]!.store_generation = 4;
      if (field === "native_thread_id") page.value.items[0]!.native_thread_id = "thread:forged";
      if (field === "evidence_contract")
        page.value.items[0]!.evidence_contract = "workstreams-t3-provider/1.0.0";
      return page;
    });
    await expect(h.load()).rejects.toThrow();
  });
  it.each(["principal", "generation", "registry", "stale"] as const)(
    "refuses changed metadata %s before dispatch",
    async (field) => {
      const h = harness();
      const data = { ...metadata, binding: { ...metadata.binding } };
      if (field === "principal") data.binding.principalId = "other";
      if (field === "generation") data.binding.serverGeneration = 8;
      if (field === "registry") data.binding.registryVersion = 12;
      if (field === "stale") data.stale = true;
      await expect(h.load({ metadata: data })).rejects.toMatchObject({ code: "scope_changed" });
      expect(h.calls).toHaveLength(0);
    },
  );
  it("requires local live trust and negotiated v2 before dispatch, without legacy fallback", async () => {
    const h = harness();
    h.state.source = null;
    await expect(h.load()).rejects.toThrow();
    expect(h.calls).toHaveLength(0);
    h.state.source = h.original;
    await expect(
      h.load({
        scope: {
          ...scope,
          capability: { ...scope.capability, placement_proofs: ["workstreams-t3-placement/1.0.0"] },
        },
      }),
    ).rejects.toMatchObject({ code: "incomplete" });
    expect(h.calls).toHaveLength(0);
  });
  it("completes every page, keeps its observations and rejects changed snapshots", async () => {
    const h = harness();
    let calls = 0;
    h.setReply(async () => {
      const page = await response();
      if (++calls % 2 === 1) page.value.next_cursor = "next";
      else {
        page.value.items[0]!.membership_id = "membership:b";
        page.value.items[0]!.kind = "secondary";
      }
      return page;
    });
    const proof = await h.load();
    expect(proof.items).toHaveLength(2);
    expect(proof.native?.observations).toHaveLength(2);
    calls = 0;
    h.setReply(async () => {
      const page = await response();
      if (++calls === 1) page.value.next_cursor = "next";
      else {
        page.value.items[0]!.membership_id = "membership:b";
        page.envelope.provenance.observed_at = "2026-11-03T00:00:01Z";
      }
      return page;
    });
    await expect(h.load()).rejects.toThrow();
  });
  it("rejects a foreign serving environment even with self-consistent page provenance", async () => {
    const h = harness();
    h.setReply(async () => {
      const page = await response();
      page.value.provenance.served_by_environment_id = "env:forged";
      page.envelope.provenance.served_by_environment_id = "env:forged";
      return page;
    });
    await expect(h.load()).rejects.toMatchObject({ code: "invalid_response" });
  });
  it.each(["missing", "unmapped", "unavailable"] as const)(
    "does not infer empty placement coverage from %s source coverage",
    async (kind) => {
      const h = harness();
      h.setReply(async () => {
        const page = await response();
        page.value.items = [];
        if (kind === "missing") page.envelope.coverage.sources = [];
        if (kind === "unmapped") {
          page.envelope.coverage.sources[0]!.mapping = "unmapped";
          page.envelope.coverage.sources[0]!.attestation = "unknown";
        }
        if (kind === "unavailable") page.envelope.coverage.sources[0]!.attestation = "unavailable";
        return page;
      });
      await expect(h.load()).rejects.toMatchObject({ code: "incomplete" });
      expect(h.calls).toHaveLength(1);
    },
  );
  it("accepts an empty placement result only with current mapped coverage for the requested source", async () => {
    const h = harness();
    h.setReply(async () => {
      const page = await response();
      page.value.items = [];
      return page;
    });
    const proof = await h.load();
    expect(proof.items).toEqual([]);
    expect(liveT3PlacementsCurrent(proof)).toBe(true);
  });
  it("rejects a future home observation even when it contains no placements", async () => {
    const h = harness();
    h.setReply(async () => {
      const page = await response();
      page.value.items = [];
      page.value.provenance.observed_at = "2026-11-03T00:00:02Z";
      page.envelope.provenance.observed_at = "2026-11-03T00:00:02Z";
      return page;
    });
    await expect(h.load()).rejects.toMatchObject({ code: "invalid_response" });
  });
  it("drops proof expired against the caller clock and rejects future local observations", async () => {
    const h = harness();
    expect((await h.load({ now: () => now + 600_000 })).items).toEqual([]);
    h.setReply(async () => {
      const page = await response();
      page.value.items[0]!.attested_at = "2026-11-03T00:00:01Z";
      page.value.provenance.observed_at = "2026-11-03T00:00:02Z";
      page.envelope.provenance.observed_at = "2026-11-03T00:00:02Z";
      return page;
    });
    await expect(h.load()).rejects.toMatchObject({ code: "invalid_response" });
  });
});
