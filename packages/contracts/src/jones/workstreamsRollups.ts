import * as Schema from "effect/Schema";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeDigest as Digest,
  WorkstreamsNativeVersion as Version,
  WorkstreamsNativeSequence as Sequence,
  WorkstreamsNativeTimestamp as Timestamp,
  WORKSTREAMS_ROLLUPS_CONTRACT,
} from "./workstreamsNative.ts";

const Id = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._:-]{1,512}$/));
export const WorkstreamsRollupFreshness = Schema.Literals([
  "current",
  "stale",
  "unknown",
  "inaccessible",
]);
const Count = Sequence.check(Schema.isLessThanOrEqualTo(1_000_000));
export const WorkstreamsRollupCoverage = closed(
  Schema.Struct({
    unsupported_visible_reference_count: Count,
    collapsed_count: Count,
    hidden_content_count: Count,
    unresolved_citation_count: Count,
    unavailable_attachment_body_count: Count,
    ordinary_citation_count: Count,
  }),
);
export const WorkstreamsRollupExtractor = closed(
  Schema.Struct({
    extractor_version: Id,
    provider: Id,
    model: Id,
    prompt_template_sha256: Digest,
    result_schema_version: Schema.Literal("2.0.0"),
  }),
);
export const WorkstreamsRollupSource = closed(
  Schema.Struct({
    account_id: Id,
    native_reference_id: Id,
    membership_id: Id,
    membership_registry_version: Version,
    native_observation_version: Schema.NullOr(Version),
    source_set_id: Id,
    source_set_sha256: Digest,
    source_member_count: Version.check(Schema.isLessThanOrEqualTo(100_000)),
    extraction_id: Id,
    extraction_attempt_id: Id,
    extraction_fence: Version,
    extraction_output_sha256: Digest,
    extraction_output_bytes: Version.check(Schema.isLessThanOrEqualTo(8_388_608)),
    authorization_policy_sha256: Digest,
    authorization_principal_id: Id,
    authorization_grant_id: Id,
    authorization_decision_id: Id,
    authorization_account_generation: Version,
    authorization_revision: Version,
    coverage: WorkstreamsRollupCoverage,
    freshness: WorkstreamsRollupFreshness,
  }),
);
export type WorkstreamsRollupSource = typeof WorkstreamsRollupSource.Type;
export const WorkstreamsRollupExclusion = closed(
  Schema.Struct({
    native_reference_id: Id,
    reason: Schema.Literals([
      "not_authorized",
      "content_unavailable",
      "input_budget_exceeded",
      "unsupported",
      "stale",
    ]),
  }),
);
export const WorkstreamsRollupManifest = closed(
  Schema.Struct({
    manifest_version: Schema.Literal("workstream-rollup-v1"),
    owner_id: Id,
    workstream_id: Id,
    workstream_version: Version,
    registry_version: Version,
    declaration_revisions: Schema.Array(
      closed(Schema.Struct({ declaration_id: Id, revision: Version })),
    ).check(Schema.isMaxLength(1000)),
    relationship_revisions: Schema.Array(
      closed(
        Schema.Struct({
          edge_id: Id,
          opened_registry_version: Version,
          closed_registry_version: Schema.NullOr(Version),
        }),
      ).check(
        Schema.makeFilter(
          (edge) =>
            edge.closed_registry_version === null ||
            edge.closed_registry_version >= edge.opened_registry_version,
        ),
      ),
    ).check(Schema.isMaxLength(1000)),
    contribution_policy: Schema.Literal("active-primary-and-secondary-v1"),
    sources: Schema.Array(WorkstreamsRollupSource).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(1000),
    ),
    exclusions: Schema.Array(WorkstreamsRollupExclusion).check(Schema.isMaxLength(1000)),
    extractor: WorkstreamsRollupExtractor,
    manifest_sha256: Digest,
  }),
).check(
  Schema.makeFilter((manifest) => {
    const unique = (xs: readonly string[]) => new Set(xs).size === xs.length;
    const included = new Set(manifest.sources.map((source) => source.native_reference_id));
    return (
      unique(manifest.declaration_revisions.map((x) => x.declaration_id)) &&
      unique(manifest.relationship_revisions.map((x) => x.edge_id)) &&
      unique(manifest.sources.map((x) => x.membership_id)) &&
      unique(manifest.sources.map((x) => x.source_set_id)) &&
      unique(manifest.exclusions.map((x) => x.native_reference_id)) &&
      manifest.exclusions.every((exclusion) => !included.has(exclusion.native_reference_id))
    );
  }),
);
export type WorkstreamsRollupManifest = typeof WorkstreamsRollupManifest.Type;
export const WorkstreamsRollupStatement = closed(
  Schema.Struct({
    text: Schema.String.check(
      Schema.makeFilter((text) => {
        const length = [...text].length;
        return length >= 1 && length <= 8192;
      }),
    ),
    sources: Schema.Array(
      closed(Schema.Struct({ source_set_id: Id, member_ordinal: Sequence })),
    ).check(Schema.isMinLength(1), Schema.isMaxLength(100)),
  }),
);
const Statements = Schema.Array(WorkstreamsRollupStatement).check(Schema.isMaxLength(100));
export const WorkstreamsRollupExtractionResult = closed(
  Schema.Struct({
    outcome: WorkstreamsRollupStatement,
    semantic_status: Schema.Literals(["unknown", "active", "blocked", "complete"]),
    blockers: Statements,
    decisions: Statements,
    next_steps: Statements,
    follow_up_candidates: Statements,
    confidence: Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  }),
);
export const WorkstreamsRollupSummary = closed(
  Schema.Struct({
    workstream_id: Id,
    rollup_version: Version,
    manifest_sha256: Digest,
    generated_at: Timestamp,
    output_sha256: Digest,
    result: WorkstreamsRollupExtractionResult,
  }),
);
export type WorkstreamsRollupSummary = typeof WorkstreamsRollupSummary.Type;
export const WorkstreamsRollupExtractionInput = closed(
  Schema.Struct({
    account_id: Id,
    source_set_id: Id,
    extraction_id: Id,
    output_sha256: Digest,
    result: WorkstreamsRollupExtractionResult,
  }),
);
export const WorkstreamsRollupProviderInput = closed(
  Schema.Struct({
    manifest: WorkstreamsRollupManifest,
    extractions: Schema.Array(WorkstreamsRollupExtractionInput).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(1000),
    ),
  }),
);
export const WorkstreamsRollupClaim = closed(
  Schema.Struct({
    workstream_id: Id,
    manifest: WorkstreamsRollupManifest,
    attempt_id: Id,
    fence: Version,
    lease_expires_at: Timestamp,
  }),
).check(Schema.makeFilter((claim) => claim.workstream_id === claim.manifest.workstream_id));
export const WorkstreamsRollupRead = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_ROLLUPS_CONTRACT),
    state: Schema.Literals(["ready", "provider_unconfigured", "unavailable"]),
    summary: Schema.NullOr(WorkstreamsRollupSummary),
    freshness: WorkstreamsRollupFreshness,
  }),
).check(Schema.makeFilter((read) => read.state === "ready" || read.summary === null));
