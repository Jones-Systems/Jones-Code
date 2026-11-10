import { describe, expect, it } from "vite-plus/test";
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import { validateNativeReadExport } from "./nativeExport.ts";
import type { NativeReadScope, NativeReadSha256 } from "./nativeRead.ts";
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
const digest: NativeReadSha256 = async (bytes) =>
  Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes))), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");

const counts = {
  context,
  principal_id: "principal",
  observed_at: at,
  authority_effect: "none",
  total: 2,
  active: 2,
  unknown_lifecycle: 0,
};
const line = (id: string, offset: number) =>
  canonicalWorkstreamsSnapshotJson({ envelope, counts, offset, items: [projectionItem(id)] }) +
  "\n";
const trailer = async (body: string, change: Record<string, unknown> = {}) =>
  JSON.stringify({
    complete: true,
    registry_version: 11,
    sha256: await digest(new TextEncoder().encode(body)),
    ...change,
  }) + "\n";
async function* chunks(parts: readonly (string | Uint8Array)[]) {
  yield* parts;
}
describe("native export completion", () => {
  it("accepts actual producer canonical page format and hashes original UTF-8 through arbitrary byte boundaries", async () => {
    const body = line("a", 0) + line("b", 1),
      full = new TextEncoder().encode(body + (await trailer(body)));
    const parts = Array.from(full, (_, i) => full.slice(i, i + 1));
    const value = await validateNativeReadExport(chunks(parts), { scope, sha256: digest });
    expect(value.complete).toBe(true);
    expect(value.pages.map((p) => p.offset)).toEqual([0, 1]);
    expect(value.preTrailerBytes).toBe(new TextEncoder().encode(body).length);
  });
  it("hashes original whitespace, escapes, literal unicode and newline bytes without recanonicalizing", async () => {
    const first = {
      envelope,
      counts: { ...counts, total: 1, active: 1 },
      offset: 0,
      items: [
        { ...projectionItem("a"), workstream: { ...workstream("a"), name: '雪 😀 "quote"\nnext' } },
      ],
    };
    const body = "  " + JSON.stringify(first).replace("quote", "\\u0071uote") + " \r\n";
    const encoded = new TextEncoder().encode(body + (await trailer(body)));
    const value = await validateNativeReadExport(
      chunks(Array.from(encoded, (_, i) => encoded.slice(i, i + 1))),
      { scope, sha256: digest },
    );
    expect(value.pages[0]!.items[0]!.workstream.name).toBe('雪 😀 "quote"\nnext');
    const changed = body.replace("  ", "");
    await expect(
      validateNativeReadExport(chunks([changed, await trailer(body)]), { scope, sha256: digest }),
    ).rejects.toThrow("digest");
  });
  it.each([
    "missing",
    "false",
    "digest",
    "version",
    "truncated",
    "extra",
    "blank",
    "malformed",
    "no-newline",
  ])("rejects %s trailer/stream without completion", async (kind) => {
    const body = line("a", 0) + line("b", 1);
    let end = await trailer(body);
    if (kind === "missing") end = "";
    if (kind === "false") end = await trailer(body, { complete: false });
    if (kind === "digest") end = await trailer(body, { sha256: "0".repeat(64) });
    if (kind === "version") end = await trailer(body, { registry_version: 12 });
    if (kind === "truncated") end = end.slice(0, -10);
    if (kind === "extra") end += "{}\n";
    if (kind === "blank") end += "\n";
    if (kind === "malformed") end += "{";
    if (kind === "no-newline") end = end.trimEnd();
    await expect(
      validateNativeReadExport(chunks([body, end]), { scope, sha256: digest }),
    ).rejects.toThrow();
  });
  it.each([
    "offset",
    "duplicate",
    "principal",
    "coverage",
    "home",
    "owner",
    "generation",
    "counts",
    "incomplete",
  ])("rejects %s export incoherence even with a matching digest", async (kind) => {
    const second = JSON.parse(line("b", 1));
    if (kind === "offset") second.offset = 0;
    if (kind === "duplicate") second.items = [projectionItem("a")];
    if (kind === "principal") second.counts.principal_id = "other";
    if (kind === "coverage") second.envelope.coverage.sources = [];
    if (kind === "home") second.envelope.provenance.home_environment_id = "env:b";
    if (kind === "owner") second.envelope.registry.owner_id = "other";
    if (kind === "generation") second.envelope.registry.server_generation++;
    if (kind === "counts") second.counts.active = 1;
    let body = line("a", 0) + JSON.stringify(second) + "\n";
    if (kind === "incomplete") body = line("a", 0);
    await expect(
      validateNativeReadExport(chunks([body, await trailer(body)]), { scope, sha256: digest }),
    ).rejects.toThrow();
  });
  it("rejects invalid UTF-8 and bounded overflows", async () => {
    await expect(
      validateNativeReadExport(chunks([new Uint8Array([0xff, 10])]), { scope, sha256: digest }),
    ).rejects.toThrow();
    const body = line("a", 0) + line("b", 1),
      end = await trailer(body);
    await expect(
      validateNativeReadExport(chunks([body, end]), { scope, sha256: digest, maxBytes: 64 }),
    ).rejects.toThrow();
    await expect(
      validateNativeReadExport(chunks([body, end]), { scope, sha256: digest, maxPages: 1 }),
    ).rejects.toThrow();
  });
  it("preserves cancelled/revoked transport failures and cancels during hashing", async () => {
    const controller = new AbortController(),
      body = line("a", 0) + line("b", 1);
    async function* cancelled() {
      yield line("a", 0);
      controller.abort();
      yield line("b", 1);
    }
    await expect(
      validateNativeReadExport(cancelled(), { scope, sha256: digest, signal: controller.signal }),
    ).rejects.toThrow("cancelled");
    const revoked = new Error("forbidden");
    async function* revocation() {
      yield body;
      throw revoked;
    }
    await expect(validateNativeReadExport(revocation(), { scope, sha256: digest })).rejects.toBe(
      revoked,
    );
    const c = new AbortController();
    await expect(
      validateNativeReadExport(chunks([body, await trailer(body)]), {
        scope,
        signal: c.signal,
        sha256: async (bytes) => {
          c.abort();
          return digest(bytes);
        },
      }),
    ).rejects.toThrow("cancelled");
  });
  it("accepts the producer's empty registry page followed by a complete trailer", async () => {
    const body =
      canonicalWorkstreamsSnapshotJson({
        envelope,
        counts: { ...counts, total: 0, active: 0 },
        offset: 0,
        items: [],
      }) + "\n";
    expect(
      (
        await validateNativeReadExport(chunks([body, await trailer(body)]), {
          scope,
          sha256: digest,
        })
      ).complete,
    ).toBe(true);
  });
});
