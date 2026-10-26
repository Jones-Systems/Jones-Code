import { expect, it } from "vite-plus/test";
import { syntheticFixture } from "./synthetic.ts";
import {
  createManifest,
  createProviderInput,
  createSummary,
  validateManifest,
  validateSummary,
  summaryState,
} from "./generation.ts";
import { canonicalJson, digest } from "./evidence.ts";
const f = syntheticFixture();
it("ports frozen CP revision canonicalization, immutable copies and source provenance", () => {
  const m = createManifest({
    ...f.manifest,
    declaration_revisions: [
      { declaration_id: "z", revision: 2 },
      { declaration_id: "a", revision: 1 },
    ],
    relationship_revisions: [
      { edge_id: "z", opened_registry_version: 1, closed_registry_version: null },
      { edge_id: "a", opened_registry_version: 1, closed_registry_version: 2 },
    ],
  });
  expect(
    createManifest({
      ...m,
      declaration_revisions: m.declaration_revisions.toReversed(),
      relationship_revisions: m.relationship_revisions.toReversed(),
    }),
  ).toEqual(m);
  expect(Object.isFrozen(m.sources[0])).toBe(true);
  validateManifest(m);
  for (const change of [
    { workstream_version: 2 },
    { registry_version: 2 },
    {
      sources: [
        { ...m.sources[0]!, authorization_decision_id: "changed", authorization_revision: 2 },
      ],
    },
  ])
    expect(createManifest({ ...m, ...change }).manifest_sha256 === m.manifest_sha256).toBe(false);
  const { manifest_sha256: _hash, ...body } = m;
  expect(m.manifest_sha256).toBe(digest("workstream-rollup-v1\n" + canonicalJson(body)));
});
it("rejects duplicate and included/excluded evidence and inconsistent per-account witnesses", () => {
  expect(() =>
    createManifest({ ...f.manifest, sources: [...f.manifest.sources, ...f.manifest.sources] }),
  ).toThrow();
  expect(() =>
    createManifest({
      ...f.manifest,
      exclusions: [{ native_reference_id: "ref:test", reason: "stale" }],
    }),
  ).toThrow();
  expect(() =>
    createManifest({
      ...f.manifest,
      sources: [
        ...f.manifest.sources,
        {
          ...f.manifest.sources[0]!,
          membership_id: "membership:other",
          source_set_id: "source:other",
          authorization_revision: 2,
        },
      ],
    }),
  ).toThrow();
});
it("validates extraction result bytes, account, source, fence-bound manifest and citations", () => {
  expect(createProviderInput(f.manifest, [f.input]).extractions).toEqual([f.input]);
  for (const input of [
    { ...f.input, account_id: "account:other" },
    { ...f.input, output_sha256: "0".repeat(64) },
    { ...f.input, result: { ...f.result, confidence: 0.2 } },
  ])
    expect(() => createProviderInput(f.manifest, [input])).toThrow();
  expect(() =>
    createProviderInput(
      createManifest({
        ...f.manifest,
        sources: [{ ...f.manifest.sources[0]!, extraction_output_bytes: 1 }],
      }),
      [f.input],
    ),
  ).toThrow();
  const input = {
    workstream_id: "ws:test",
    rollup_version: 1,
    manifest_sha256: f.manifest.manifest_sha256,
    generated_at: "2026-01-01T00:00:00.000Z",
    result: f.result,
  };
  const summary = createSummary(f.manifest, input);
  validateSummary(f.manifest, summary);
  expect(summary.output_sha256).toBe(digest(canonicalJson(f.result)));
  for (const sources of [
    [{ source_set_id: "source:unbound", member_ordinal: 0 }],
    [{ source_set_id: "source:test", member_ordinal: 1 }],
  ])
    expect(() =>
      createSummary(f.manifest, {
        ...input,
        result: { ...f.result, outcome: { ...f.result.outcome, sources } },
      }),
    ).toThrow();
  expect(() =>
    validateSummary(f.manifest, { ...summary, output_sha256: "0".repeat(64) }),
  ).toThrow();
});
it("preserves coverage and worst freshness rather than claiming partial input complete", () => {
  expect(summaryState(f.manifest)).toEqual({ coverage: "complete", freshness: "current" });
  expect(
    summaryState(
      createManifest({
        ...f.manifest,
        exclusions: [{ native_reference_id: "r:excluded", reason: "content_unavailable" }],
      }),
    ),
  ).toEqual({ coverage: "partial", freshness: "unknown" });
  expect(
    summaryState(
      createManifest({
        ...f.manifest,
        exclusions: [{ native_reference_id: "r:excluded", reason: "not_authorized" }],
      }),
    ),
  ).toEqual({ coverage: "partial", freshness: "inaccessible" });
  expect(
    summaryState(
      createManifest({
        ...f.manifest,
        sources: [
          {
            ...f.manifest.sources[0]!,
            freshness: "stale",
            coverage: { ...f.evidence.coverage, collapsed_count: 1 },
          },
        ],
      }),
    ),
  ).toEqual({ coverage: "partial", freshness: "stale" });
});
it("retains the 1000-revision boundary and rejects 1001 without altering the frozen contract", () => {
  const declarations = Array.from({ length: 1000 }, (_, i) => ({
    declaration_id: "d:" + i,
    revision: 1,
  }));
  expect(
    createManifest({ ...f.manifest, declaration_revisions: declarations }).declaration_revisions,
  ).toHaveLength(1000);
  expect(() =>
    createManifest({
      ...f.manifest,
      declaration_revisions: [...declarations, { declaration_id: "overflow", revision: 1 }],
    }),
  ).toThrow();
});
