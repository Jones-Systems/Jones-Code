import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Wire from "@t3tools/contracts/jones/workstreamsRollups";
import {
  canonicalJson,
  digest,
  immutable,
  requireRollup,
  RollupError,
  type Manifest,
  type Summary,
  type Result,
  type ProviderInput,
  type ExtractionInput,
  type Freshness,
} from "./evidence.ts";

const decodeWorkstreamsRollupManifest = Schema.decodeUnknownSync(Wire.WorkstreamsRollupManifest);
const decodeWorkstreamsRollupExtractionResult = Schema.decodeUnknownSync(
  Wire.WorkstreamsRollupExtractionResult,
);
const decodeWorkstreamsRollupProviderInput = Schema.decodeUnknownSync(
  Wire.WorkstreamsRollupProviderInput,
);
const decodeWorkstreamsRollupSummary = Schema.decodeUnknownSync(Wire.WorkstreamsRollupSummary);

export class RollupGenerationProvider extends Context.Service<
  RollupGenerationProvider,
  {
    readonly configured: boolean;
    readonly identity: Manifest["extractor"];
    readonly generate: (
      input: ProviderInput,
      signal: AbortSignal,
    ) => Effect.Effect<Result, RollupError>;
  }
>()("t3/jones/workstreams/native/rollups/generation/RollupGenerationProvider") {}
/** @public Explicit live-provider default, independent of imported summary reads. */
export const unconfiguredLayer = Layer.succeed(RollupGenerationProvider, {
  configured: false,
  identity: {
    extractor_version: "unconfigured",
    provider: "unconfigured",
    model: "unconfigured",
    prompt_template_sha256: "0".repeat(64),
    result_schema_version: "2.0.0",
  },
  generate: () => Effect.fail(new RollupError({ code: "provider_unconfigured" })),
});
export function createManifest(
  input: Omit<Manifest, "manifest_version" | "manifest_sha256">,
): Manifest {
  const value = {
    manifest_version: "workstream-rollup-v1" as const,
    owner_id: input.owner_id,
    workstream_id: input.workstream_id,
    workstream_version: input.workstream_version,
    registry_version: input.registry_version,
    declaration_revisions: [...input.declaration_revisions].sort((a, b) =>
      a.declaration_id.localeCompare(b.declaration_id),
    ),
    relationship_revisions: [...input.relationship_revisions].sort((a, b) =>
      a.edge_id.localeCompare(b.edge_id),
    ),
    contribution_policy: input.contribution_policy,
    sources: [...input.sources].sort((a, b) =>
      `${a.membership_id}\0${a.source_set_id}`.localeCompare(
        `${b.membership_id}\0${b.source_set_id}`,
      ),
    ),
    exclusions: [...input.exclusions].sort((a, b) =>
      a.native_reference_id.localeCompare(b.native_reference_id),
    ),
    extractor: input.extractor,
  };
  const manifest = decodeWorkstreamsRollupManifest(
    { ...value, manifest_sha256: digest(`workstream-rollup-v1\n${canonicalJson(value)}`) },
    { onExcessProperty: "error" },
  );
  for (const account of new Set(manifest.sources.map((s) => s.account_id))) {
    const witnesses = manifest.sources
      .filter((s) => s.account_id === account)
      .map((s) =>
        canonicalJson([
          s.authorization_policy_sha256,
          s.authorization_principal_id,
          s.authorization_grant_id,
          s.authorization_decision_id,
          s.authorization_account_generation,
          s.authorization_revision,
        ]),
      );
    requireRollup(new Set(witnesses).size === 1);
  }
  return immutable(manifest);
}
export function validateManifest(value: Manifest): void {
  const decoded = decodeWorkstreamsRollupManifest(value, {
    onExcessProperty: "error",
  });
  requireRollup(canonicalJson(createManifest(decoded)) === canonicalJson(value));
}
function validateResult(result: Result, manifest: Manifest): void {
  decodeWorkstreamsRollupExtractionResult(result, {
    onExcessProperty: "error",
  });
  const counts = new Map(manifest.sources.map((s) => [s.source_set_id, s.source_member_count]));
  for (const statement of [
    result.outcome,
    ...result.blockers,
    ...result.decisions,
    ...result.next_steps,
    ...result.follow_up_candidates,
  ])
    for (const source of statement.sources)
      requireRollup(
        counts.has(source.source_set_id) &&
          source.member_ordinal < counts.get(source.source_set_id)!,
      );
  requireRollup(Buffer.byteLength(canonicalJson(result), "utf8") <= 8_388_608);
}
export function createProviderInput(
  manifest: Manifest,
  inputs: readonly ExtractionInput[],
): ProviderInput {
  validateManifest(manifest);
  requireRollup(inputs.length === manifest.sources.length, "input_unavailable");
  const key = (s: { account_id: string; source_set_id: string; extraction_id: string }) =>
    `${s.account_id}\0${s.source_set_id}\0${s.extraction_id}`;
  const byKey = new Map(manifest.sources.map((s) => [key(s), s]));
  requireRollup(new Set(inputs.map(key)).size === byKey.size, "input_unavailable");
  for (const input of inputs) {
    const source = byKey.get(key(input));
    requireRollup(
      source &&
        source.extraction_output_sha256 === input.output_sha256 &&
        digest(canonicalJson(input.result)) === input.output_sha256 &&
        Buffer.byteLength(canonicalJson(input.result), "utf8") === source.extraction_output_bytes,
      "input_unavailable",
    );
    validateResult(input.result, { ...manifest, sources: [source] });
  }
  return immutable(
    decodeWorkstreamsRollupProviderInput(
      { manifest, extractions: [...inputs].sort((a, b) => key(a).localeCompare(key(b))) },
      { onExcessProperty: "error" },
    ),
  );
}
export function createSummary(manifest: Manifest, input: Omit<Summary, "output_sha256">): Summary {
  validateManifest(manifest);
  requireRollup(
    input.workstream_id === manifest.workstream_id &&
      input.manifest_sha256 === manifest.manifest_sha256,
  );
  validateResult(input.result, manifest);
  return immutable(
    decodeWorkstreamsRollupSummary(
      { ...input, output_sha256: digest(canonicalJson(input.result)) },
      { onExcessProperty: "error" },
    ),
  );
}
export function validateSummary(manifest: Manifest, value: Summary): void {
  const { output_sha256: _digest, ...input } = value;
  requireRollup(canonicalJson(createSummary(manifest, input)) === canonicalJson(value));
}
export function summaryState(manifest: Manifest): {
  coverage: "complete" | "partial";
  freshness: Freshness;
} {
  const coverage =
    manifest.exclusions.length === 0 &&
    manifest.sources.every((s) =>
      Object.entries(s.coverage).every(([k, v]) => k === "ordinary_citation_count" || v === 0),
    )
      ? "complete"
      : "partial";
  const rank = { current: 0, stale: 1, unknown: 2, inaccessible: 3 } as const;
  let freshness: Freshness = "current";
  for (const next of [
    ...manifest.sources.map((s) => s.freshness),
    ...manifest.exclusions.map((e) =>
      e.reason === "stale"
        ? ("stale" as const)
        : e.reason === "not_authorized"
          ? ("inaccessible" as const)
          : ("unknown" as const),
    ),
  ])
    if (rank[next] > rank[freshness]) freshness = next;
  return { coverage, freshness };
}
