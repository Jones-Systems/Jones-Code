import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  WorkstreamsRollupManifest,
  WorkstreamsRollupClaim,
  WorkstreamsRollupSummary,
  WorkstreamsRollupExtractionResult,
} from "./workstreamsRollups.ts";
const hash = "a".repeat(64);
const source = {
  account_id: "account-" + "x".repeat(200),
  native_reference_id: "ref-1",
  membership_id: "member-1",
  membership_registry_version: 1,
  native_observation_version: null,
  source_set_id: "sources-1",
  source_set_sha256: hash,
  source_member_count: 1,
  extraction_id: "extraction-1",
  extraction_attempt_id: "attempt-1",
  extraction_fence: 1,
  extraction_output_sha256: hash,
  extraction_output_bytes: 100,
  authorization_policy_sha256: hash,
  authorization_principal_id: "principal-1",
  authorization_grant_id: "grant-1",
  authorization_decision_id: "decision-1",
  authorization_account_generation: 1,
  authorization_revision: 1,
  coverage: {
    unsupported_visible_reference_count: 0,
    collapsed_count: 0,
    hidden_content_count: 0,
    unresolved_citation_count: 0,
    unavailable_attachment_body_count: 0,
    ordinary_citation_count: 1,
  },
  freshness: "current",
};
const manifest = {
  manifest_version: "workstream-rollup-v1",
  owner_id: "owner-1",
  workstream_id: "ws-1",
  workstream_version: 1,
  registry_version: 1,
  declaration_revisions: [],
  relationship_revisions: [],
  contribution_policy: "active-primary-and-secondary-v1",
  sources: [source],
  exclusions: [],
  extractor: {
    extractor_version: "v2",
    provider: "synthetic",
    model: "fixture",
    prompt_template_sha256: hash,
    result_schema_version: "2.0.0",
  },
  manifest_sha256: hash,
};
const statement = {
  text: "Synthetic",
  sources: [{ source_set_id: "sources-1", member_ordinal: 0 }],
};
const result = {
  outcome: statement,
  semantic_status: "active",
  blockers: [],
  decisions: [],
  next_steps: [],
  follow_up_candidates: [],
  confidence: 0.75,
};
describe("native rollup preservation", () => {
  it("retains authorization, coverage and longer extraction identifiers", () => {
    const decode = decodeWorkstreamsRollupManifest;
    expect(decode(manifest)).toEqual(manifest);
    for (const value of [
      { ...manifest, sources: [source, source] },
      { ...manifest, exclusions: [{ native_reference_id: "ref-1", reason: "not_authorized" }] },
      { ...manifest, sources: [{ ...source, authorization_revision: 0 }] },
      {
        ...manifest,
        relationship_revisions: [
          { edge_id: "edge-1", opened_registry_version: 2, closed_registry_version: 1 },
        ],
      },
      {
        ...manifest,
        sources: [{ ...source, coverage: { ...source.coverage, invented_count: 0 } }],
      },
    ])
      expect(() => decode(value)).toThrow();
  });
  it("preserves fractional confidence and existing Unicode statement bounds", () => {
    const decode = decodeWorkstreamsRollupExtractionResult;
    expect(decode(result)).toEqual(result);
    expect(
      decode({ ...result, outcome: { ...statement, text: "😀".repeat(8192) } }).outcome.text,
    ).toHaveLength(16384);
    for (const value of [
      { ...result, confidence: Infinity },
      { ...result, confidence: 1.1 },
      {
        ...result,
        outcome: { ...statement, sources: Array.from({ length: 101 }, () => statement.sources[0]) },
      },
      { ...result, blockers: Array.from({ length: 101 }, () => statement) },
    ])
      expect(() => decode(value)).toThrow();
    const summary = {
      workstream_id: "ws-1",
      rollup_version: 1,
      manifest_sha256: hash,
      generated_at: "2026-10-25T12:00:00Z",
      output_sha256: hash,
      result,
    };
    expect(decodeWorkstreamsRollupSummary(summary)).toEqual(summary);
  });
  it("binds the leased claim to its workstream and positive fence", () => {
    const claim = {
      workstream_id: "ws-1",
      manifest,
      attempt_id: "attempt-1",
      fence: 1,
      lease_expires_at: "2026-10-25T12:01:00Z",
    };
    const decode = decodeWorkstreamsRollupClaim;
    expect(decode(claim)).toEqual(claim);
    expect(() => decode({ ...claim, workstream_id: "ws-other" })).toThrow();
    expect(() => decode({ ...claim, fence: 0 })).toThrow();
  });
});

const decodeWorkstreamsRollupClaim = Schema.decodeUnknownSync(WorkstreamsRollupClaim);
const decodeWorkstreamsRollupExtractionResult = Schema.decodeUnknownSync(
  WorkstreamsRollupExtractionResult,
);
const decodeWorkstreamsRollupManifest = Schema.decodeUnknownSync(WorkstreamsRollupManifest);
const decodeWorkstreamsRollupSummary = Schema.decodeUnknownSync(WorkstreamsRollupSummary);
