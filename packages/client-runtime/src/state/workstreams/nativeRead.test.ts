import { describe, expect, it, vi } from "vite-plus/test";
import { t3PlacementInventoryJson } from "@t3tools/contracts";
import { NativeWorkstreamReadCache } from "./native.ts";
import {
  NativeWorkstreamReader,
  nativeReadRequest,
  validateNativeReadResult,
  type NativeReadScope,
  type NativeReadSha256,
} from "./nativeRead.ts";
const at = "2026-10-25T00:00:00Z";
const scope: NativeReadScope = {
  capability: {
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
  },
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
};
const envelope = {
  contract: "workstreams-read/1.0.0" as const,
  registry: { ...scope.capability.registry!, registry_version: 11 },
  provenance: {
    authority: "native-home" as const,
    home_environment_id: "env:a",
    served_by_environment_id: "env:a",
    source: "live" as const,
    observed_at: at,
    stale: false,
  },
  coverage: {
    registry: "complete" as const,
    sources: [
      {
        source_instance_id: "env:a",
        mapping: "mapped" as const,
        attestation: "unknown" as const,
        observed_at: null,
      },
    ],
  },
};
// The envelope registry intentionally excludes the capability-only home field.
delete (
  envelope.registry as Partial<typeof scope.capability.registry> & { home_environment_id?: string }
).home_environment_id;
const context = { owner_id: "owner", server_generation: 7, registry_version: 11 };
const workstream = (id: string) => ({
  workstream_id: id,
  owner_id: "owner",
  name: id,
  lifecycle: "active" as const,
  progress: { state: "unknown" as const },
  delivery: "none-observed" as const,
  freshness: "unknown" as const,
  sort_order: 0,
  version: 1,
  created_at: at,
  updated_at: at,
  created_by: { principal_id: "principal" },
  updated_by: { principal_id: "principal" },
});
const projectionItem = (id: string) => ({
  workstream: workstream(id),
  active_memberships: { primary: 0, secondary: 0 },
  appearance: { workstream_id: id, border_color: null, version: 0 },
  lifecycle: null,
});
const result = (route: string, value: unknown, e = envelope) => ({
  contract: "workstreams-native-home-read-result/1.0.0",
  route,
  value,
  envelope: e,
  authorization_revision: 1,
});
const digest: NativeReadSha256 = async (bytes) =>
  Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes))), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");

const listPage = (ids: string[], next: string | null = null) =>
  result(
    "workstreams",
    { context, items: ids.map(workstream), next_cursor: next },
    structuredClone(envelope),
  );
const deferred = () => {
  let resolve!: (value: unknown) => void;
  const promise = new Promise<unknown>((r) => {
    resolve = r;
  });
  return { promise, resolve };
};
describe("native consumer reads", () => {
  it("publishes only the complete list, preserves envelopes and does not infer source attestation", async () => {
    const read = vi
      .fn()
      .mockResolvedValueOnce(listPage(["a"], "next"))
      .mockResolvedValueOnce(listPage(["b"]));
    const client = new NativeWorkstreamReader({ scope, read });
    const snapshot = await client.list();
    expect(snapshot.items.map((x) => x.workstream_id)).toEqual(["a", "b"]);
    expect(snapshot.pages).toHaveLength(2);
    expect(read.mock.calls[1]![0].request.params.cursor).toBe("next");
    expect(snapshot.envelope.coverage.sources[0]!.attestation).toBe("unknown");
    expect(snapshot.stale).toBe(false);
  });
  it.each([
    "context",
    "observed_at",
    "coverage",
    "owner",
    "home",
    "generation",
    "authorization",
    "route",
    "origin",
    "registry",
  ])("refuses a second-page %s mismatch", async (field) => {
    const second = structuredClone(listPage(["b"]));
    const changed = second as unknown as {
      value: { context: typeof context };
      route: string;
      authorization_revision: number;
      envelope: typeof envelope;
    };
    if (field === "context") changed.value.context.registry_version++;
    if (field === "observed_at") changed.envelope.provenance.observed_at = "2026-10-25T00:00:01Z";
    if (field === "coverage") changed.envelope.coverage.sources = [];
    if (field === "owner") changed.envelope.registry.owner_id = "other";
    if (field === "home") changed.envelope.provenance.home_environment_id = "env:b";
    if (field === "generation") changed.envelope.registry.server_generation++;
    if (field === "authorization") changed.authorization_revision++;
    if (field === "route") changed.route = "references";
    if (field === "origin") changed.envelope.registry.canonical_origin = "https://other.test";
    if (field === "registry")
      changed.envelope.registry.registry_instance_id = "9cdd84b6-81c5-4c88-9d13-5d324abaf662";
    const client = new NativeWorkstreamReader({
      scope,
      read: vi
        .fn()
        .mockResolvedValueOnce(listPage(["a"], "next"))
        .mockResolvedValueOnce(second),
    });
    await expect(client.list()).rejects.toThrow();
    await expect(client.list()).rejects.toThrow("Refresh native authorization");
  });
  it.each(["duplicate", "cycle", "incomplete", "limit"])(
    "never publishes %s pages",
    async (kind) => {
      const second =
        kind === "duplicate"
          ? listPage(["a"])
          : kind === "cycle"
            ? listPage(["b"], "next")
            : listPage(["b"]);
      if (kind === "incomplete")
        Object.assign(second.envelope, {
          provenance: { ...envelope.provenance, stale: true, source: "cache" },
          coverage: { ...envelope.coverage, registry: "stale" },
        });
      const client = new NativeWorkstreamReader({
        scope,
        maxPages: kind === "limit" ? 1 : 10,
        read: vi
          .fn()
          .mockResolvedValueOnce(listPage(["a"], "next"))
          .mockResolvedValueOnce(second),
      });
      await expect(client.list()).rejects.toThrow();
    },
  );
  it("loads projection and detail through the public transport API and rejects wrong entities", async () => {
    const client = new NativeWorkstreamReader({
      scope,
      read: vi
        .fn()
        .mockResolvedValueOnce(
          result("read-projection", { items: [projectionItem("a")], next_cursor: "next" }),
        )
        .mockResolvedValueOnce(
          result("read-projection", { items: [projectionItem("b")], next_cursor: null }),
        )
        .mockResolvedValueOnce(result("workstream", { context, workstream: workstream("a") }))
        .mockResolvedValueOnce(result("workstream", { context, workstream: workstream("b") })),
    });
    expect((await client.projection()).items.map((x) => x.appearance.workstream_id)).toEqual([
      "a",
      "b",
    ]);
    expect((await client.detail("a")).pages[0]!.value.workstream.workstream_id).toBe("a");
    await expect(client.detail("a")).rejects.toThrow("Wrong workstream");
  });
  it("rejects incoherent projection appearance and lifecycle", async () => {
    for (const item of [
      {
        ...projectionItem("a"),
        appearance: { workstream_id: "b", border_color: null, version: 0 },
      },
      { ...projectionItem("a"), workstream: { ...workstream("a"), owner_id: "other" } },
    ]) {
      const client = new NativeWorkstreamReader({
        scope,
        read: async () => result("read-projection", { items: [item], next_cursor: null }),
      });
      await expect(client.projection()).rejects.toThrow();
    }
  });
  it("loads scoped history pages and rejects another workstream's history", async () => {
    const event = (id: string, ws = "a") => ({
      event_id: id,
      command_id: "command-history-0001",
      actor: { principal_id: "principal" },
      operation: "create_workstream",
      occurred_at: at,
      registry_version: 11,
      changed: true,
      workstream_versions: [{ workstream_id: ws, version: 1 }],
      native_reference_id: null,
      membership_ids: [],
      declaration_id: null,
      declaration_revision: null,
      edge_id: null,
      lifecycle_declaration: null,
    });
    const read = vi
      .fn()
      .mockResolvedValueOnce(
        result("history", { context, items: [event("e1")], next_cursor: "next" }),
      )
      .mockResolvedValueOnce(
        result("history", { context, items: [event("e2")], next_cursor: null }),
      )
      .mockResolvedValueOnce(
        result("history", { context, items: [event("e3", "b")], next_cursor: null }),
      );
    const client = new NativeWorkstreamReader({ scope, read });
    expect((await client.history("a")).items).toHaveLength(2);
    await expect(client.history("a")).rejects.toThrow("Wrong workstream history");
  });
  it("retains stale read-only memory only for an explicitly transient same-scope outage", async () => {
    const cache = new NativeWorkstreamReadCache(),
      outage = { code: "offline" };
    const read = vi
      .fn()
      .mockResolvedValueOnce(listPage(["a"]))
      .mockRejectedValue(outage);
    const client = new NativeWorkstreamReader({
      scope,
      read,
      cache,
      isTransientError: (e) => e === outage,
    });
    const live = await client.list();
    (live.items[0] as { name: string }).name = "mutated";
    const stale = await client.list();
    expect(stale).toMatchObject({
      stale: true,
      readOnly: true,
      envelope: { coverage: { registry: "stale" } },
    });
    expect(stale.items[0]!.name).toBe("a");
    client.setScope({ ...scope, binding: { ...scope.binding, authorizationRevision: 2 } });
    await expect(client.list()).rejects.toBe(outage);
  });
  it.each(["principal", "generation", "owner", "home"])(
    "purges cache on %s scope replacement",
    async (kind) => {
      const outage = { code: "offline" },
        cache = new NativeWorkstreamReadCache();
      const client = new NativeWorkstreamReader({
        scope,
        cache,
        read: vi
          .fn()
          .mockResolvedValueOnce(listPage(["a"]))
          .mockRejectedValue(outage),
        isTransientError: (e) => e === outage,
      });
      await client.list();
      const next = structuredClone(scope);
      if (kind === "principal") Object.assign(next.binding, { principalId: "other" });
      if (kind === "generation") {
        Object.assign(next.binding, { serverGeneration: 8 });
        Object.assign(next.capability.registry!, { server_generation: 8 });
      }
      if (kind === "owner") {
        Object.assign(next.binding, { ownerId: "other" });
        Object.assign(next.capability.registry!, { owner_id: "other" });
      }
      if (kind === "home")
        Object.assign(next.capability.registry!, { home_environment_id: "env:b" });
      client.setScope(next);
      await expect(client.list()).rejects.toBe(outage);
    },
  );
  it("purges on forbidden and refuses further reads until a scope refresh", async () => {
    const client = new NativeWorkstreamReader({
      scope,
      read: vi
        .fn()
        .mockResolvedValueOnce(listPage(["a"]))
        .mockRejectedValue({ code: "forbidden" }),
    });
    await client.list();
    await expect(client.list()).rejects.toEqual({ code: "forbidden" });
    await expect(client.list()).rejects.toThrow("Refresh native authorization");
  });
  it.each(["invalidate", "abort", "replace", "scope"])(
    "fences late results after %s",
    async (kind) => {
      const pending = deferred(),
        controller = new AbortController();
      const read = vi
        .fn()
        .mockReturnValueOnce(pending.promise)
        .mockResolvedValue(listPage(["b"]));
      const client = new NativeWorkstreamReader({ scope, read });
      const first = client.list(controller.signal);
      const rejected = expect(first).rejects.toThrow("cancelled");
      if (kind === "invalidate") client.invalidate();
      if (kind === "abort") controller.abort();
      if (kind === "replace") expect((await client.list()).items[0]!.workstream_id).toBe("b");
      if (kind === "scope")
        client.setScope({ ...scope, binding: { ...scope.binding, principalId: "other" } });
      pending.resolve(listPage(["a"]));
      await rejected;
    },
  );
  it("does not let an overly broad outage classifier retain data after revocation", async () => {
    const client = new NativeWorkstreamReader({
      scope,
      read: vi
        .fn()
        .mockResolvedValueOnce(listPage(["a"]))
        .mockRejectedValue({ code: "revoked" }),
      isTransientError: () => true,
    });
    await client.list();
    await expect(client.list()).rejects.toEqual({ code: "revoked" });
    await expect(client.list()).rejects.toThrow("Refresh native authorization");
  });
  it("does not publish or reuse the first page after an outage on the second page", async () => {
    const outage = { code: "offline" },
      read = vi
        .fn()
        .mockResolvedValueOnce(listPage(["a"], "next"))
        .mockRejectedValue(outage);
    const client = new NativeWorkstreamReader({
      scope,
      read,
      isTransientError: (e) => e === outage,
    });
    await expect(client.list()).rejects.toBe(outage);
    await expect(client.list()).rejects.toBe(outage);
  });
  it("applies the requested page and byte bounds before publication", async () => {
    const client = new NativeWorkstreamReader({
      scope,
      maxBytes: 10,
      read: async () => listPage(["a"]),
    });
    await expect(client.list()).rejects.toThrow("byte bound");
    const oversized = new NativeWorkstreamReader({ scope, read: async () => listPage(["a", "b"]) });
    await expect(
      oversized.complete(nativeReadRequest({ route: "workstreams", params: { limit: 1 } })),
    ).rejects.toThrow("limit");
  });
  it("keeps a known lost member-home link on stale read-only memory without a live read", async () => {
    const read = vi.fn().mockResolvedValue(listPage(["a"]));
    const client = new NativeWorkstreamReader({ scope, read });
    await client.list();
    client.setScope({
      ...scope,
      capability: {
        ...scope.capability,
        mode: "native-member",
        home_link: "missing",
        registry: scope.capability.registry!,
      },
    });
    expect(await client.list()).toMatchObject({ stale: true, readOnly: true });
    expect(read).toHaveBeenCalledTimes(1);
    client.invalidate();
    client.setScope({
      ...scope,
      capability: {
        ...scope.capability,
        mode: "native-member",
        home_link: "missing",
        registry: scope.capability.registry!,
      },
    });
    await expect(client.list()).rejects.toThrow("no complete memory");
    expect(read).toHaveBeenCalledTimes(1);
  });
  it("does not send aborted reads or accept resumed cursors as complete", async () => {
    const read = vi.fn(),
      c = new AbortController();
    c.abort();
    const client = new NativeWorkstreamReader({ scope, read });
    await expect(client.list(c.signal)).rejects.toThrow();
    await expect(
      client.complete(nativeReadRequest({ route: "workstreams", params: { cursor: "next" } })),
    ).rejects.toThrow("first page");
    expect(read).not.toHaveBeenCalled();
  });
});
describe("detail collections and native placement proofs", () => {
  it("collects workstream-scoped membership history and rejects foreign entries", async () => {
    const membership = (id: string, workstreamId = "a") => ({
      membership_id: id,
      workstream_id: workstreamId,
      native_reference_id: "ref:a",
      kind: "primary",
      opened: {
        at,
        actor: { principal_id: "principal" },
        command_id: "command-membership-0001",
        registry_version: 1,
      },
      closed: null,
    });
    const request = nativeReadRequest({ route: "memberships", params: { workstream_id: "a" } });
    const client = new NativeWorkstreamReader({
      scope,
      read: vi
        .fn()
        .mockResolvedValueOnce(
          result("memberships", { context, items: [membership("m1")], next_cursor: "next" }),
        )
        .mockResolvedValueOnce(
          result("memberships", { context, items: [membership("m2")], next_cursor: null }),
        )
        .mockResolvedValueOnce(
          result("memberships", { context, items: [membership("m3", "b")], next_cursor: null }),
        ),
    });
    expect((await client.complete(request)).items).toHaveLength(2);
    await expect(client.complete(request)).rejects.toThrow("Wrong workstream collection");
  });
  it("requires v2 markers for nonempty native placement proofs and rejects expired or foreign proof identity", async () => {
    const identities = [{ source_instance_id: "env:a", native_thread_id: "thread:a" }];
    const request = nativeReadRequest(
      { route: "placements", params: { identities } },
      "workstreams-t3-placement/2.0.0",
    );
    const item = {
      membership_id: "m1",
      workstream_id: "a",
      native_reference_id: "ref:a",
      kind: "primary",
      source_instance_id: "env:a",
      native_thread_id: "thread:a",
      attestation_version: 1,
      attested_at: "2026-10-24T23:59:00Z",
      expires_at: "2026-10-25T00:01:00Z",
      evidence_sha256: "a".repeat(64),
      source_binding_version: 1,
      authority_namespace: "namespace:a",
      store_generation: 1,
      attestation_source: "native",
      evidence_contract: "workstreams-t3-provider/2.0.0",
    };
    const value = {
      context: { ...context, principal_id: "principal", authorization_revision: 1 },
      inventory_sha256: await digest(
        new TextEncoder().encode(t3PlacementInventoryJson(identities)),
      ),
      items: [item],
      next_cursor: null,
      provenance: {
        authority: "native-home",
        registry_instance_id: envelope.registry.registry_instance_id,
        home_environment_id: "env:a",
        home_generation: 7,
        served_by_environment_id: "env:a",
        observed_at: at,
      },
    };
    const page = {
      ...result("placements", value),
      placement_contract: "workstreams-t3-placement/2.0.0",
    };
    expect((await validateNativeReadResult(page, request, scope, digest)).route).toBe("placements");
    for (const change of [
      { native_thread_id: "thread:other" },
      { expires_at: at },
      { attestation_source: undefined },
      { evidence_contract: "workstreams-t3-provider/1.0.0" },
    ])
      await expect(
        validateNativeReadResult(
          { ...page, value: { ...value, items: [{ ...item, ...change }] } },
          request,
          scope,
          digest,
        ),
      ).rejects.toThrow();
    const { attestation_source: _s, evidence_contract: _e, ...legacy } = item;
    const { provenance: _p, ...legacyPage } = value;
    await expect(
      validateNativeReadResult(
        {
          ...page,
          placement_contract: "workstreams-t3-placement/1.0.0",
          value: { ...legacyPage, items: [legacy] },
        },
        { ...request, placement_contract: "workstreams-t3-placement/1.0.0" },
        scope,
        digest,
      ),
    ).rejects.toThrow("nonempty frozen v1");
  });
});
describe("closed route validation", () => {
  it("rejects unknown fields and counts with the wrong principal", async () => {
    const req = nativeReadRequest({ route: "counts", params: {} });
    const counts = {
      context,
      principal_id: "other",
      observed_at: at,
      authority_effect: "none",
      total: 0,
      active: 0,
      unknown_lifecycle: 0,
    };
    await expect(validateNativeReadResult(result("counts", counts), req, scope)).rejects.toThrow();
    await expect(
      validateNativeReadResult(
        { ...listPage([]), extra: true },
        nativeReadRequest({ route: "workstreams", params: {} }),
        scope,
      ),
    ).rejects.toThrow();
  });
  it.each(["workstreams-t3-placement/1.0.0", "workstreams-t3-placement/2.0.0"] as const)(
    "validates negotiated %s without upgrading a legacy proof",
    async (contract) => {
      const identities = [{ source_instance_id: "env:a", native_thread_id: "thread:a" }];
      const req = nativeReadRequest({ route: "placements", params: { identities } }, contract);
      const value = {
        context: { ...context, principal_id: "principal", authorization_revision: 1 },
        inventory_sha256: await digest(
          new TextEncoder().encode(t3PlacementInventoryJson(identities)),
        ),
        items: [],
        next_cursor: null,
        ...(contract.endsWith("2.0.0")
          ? {
              provenance: {
                authority: "native-home",
                registry_instance_id: envelope.registry.registry_instance_id,
                home_environment_id: "env:a",
                home_generation: 7,
                served_by_environment_id: "env:a",
                observed_at: at,
              },
            }
          : {}),
      };
      const page = { ...result("placements", value), placement_contract: contract };
      expect((await validateNativeReadResult(page, req, scope, digest)).route).toBe("placements");
      await expect(
        validateNativeReadResult(
          page,
          {
            ...req,
            placement_contract: contract.endsWith("2.0.0")
              ? "workstreams-t3-placement/1.0.0"
              : "workstreams-t3-placement/2.0.0",
          },
          scope,
          digest,
        ),
      ).rejects.toThrow();
      await expect(
        validateNativeReadResult(
          { ...page, value: { ...value, inventory_sha256: "0".repeat(64) } },
          req,
          scope,
          digest,
        ),
      ).rejects.toThrow();
      if (contract.endsWith("1.0.0"))
        await expect(
          validateNativeReadResult(
            { ...page, value: { ...value, provenance: {} } },
            req,
            scope,
            digest,
          ),
        ).rejects.toThrow();
    },
  );
});
