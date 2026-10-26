import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  RollupEvidenceProvider,
  EVIDENCE_CONTRACT,
  RollupError,
  canonicalJson,
  immutable,
  digest,
  type SourceEvidence,
  type ExtractionInput,
  type Manifest,
  type Result,
  type ProviderInput,
} from "./evidence.ts";
import { RollupGenerationProvider, createProviderInput, createManifest } from "./generation.ts";
import type { RollupBinding } from "./bindings.ts";
export interface SyntheticEvidenceScenario {
  readonly binding: RollupBinding;
  readonly evidence: SourceEvidence;
  readonly input: ExtractionInput;
  readonly authorized?: (() => boolean) | undefined;
}
/** @public Explicit synthetic data only; matching includes owner, account, selection and provider. */
export const syntheticEvidenceLayer = (
  scenarios: readonly SyntheticEvidenceScenario[],
  id = "synthetic",
) => {
  const fixed = scenarios.map((s) => ({
    ...s,
    binding: immutable(s.binding),
    evidence: immutable(s.evidence),
    input: immutable(s.input),
  }));
  const allowed = (s: (typeof fixed)[number]) => s.authorized?.() ?? true;
  const scenario = (m: Manifest, source: Manifest["sources"][number]) =>
    fixed.find(
      (s) =>
        s.binding.owner_id === m.owner_id &&
        s.binding.workstream_id === m.workstream_id &&
        s.binding.native_reference_id === source.native_reference_id &&
        s.binding.account_id === source.account_id &&
        s.binding.provider_id === id &&
        s.input.source_set_id === source.source_set_id &&
        canonicalJson(s.evidence) ===
          canonicalJson(
            (({
              membership_id: _m,
              membership_registry_version: _r,
              native_observation_version: _o,
              native_reference_id: _n,
              ...value
            }) => value)(source),
          ),
    );
  return Layer.succeed(RollupEvidenceProvider, {
    contract: EVIDENCE_CONTRACT,
    id,
    configured: true,
    describe: Effect.fn("SyntheticRollupEvidence.describe")((bindings: readonly RollupBinding[]) =>
      Effect.sync(() =>
        bindings.map((binding) => {
          const s = fixed.find((s) => canonicalJson(s.binding) === canonicalJson(binding));
          if (!s)
            return {
              kind: "excluded" as const,
              native_reference_id: binding.native_reference_id,
              reason: "content_unavailable" as const,
            };
          if (!allowed(s))
            return {
              kind: "excluded" as const,
              native_reference_id: binding.native_reference_id,
              reason: "not_authorized" as const,
            };
          return {
            kind: "available" as const,
            native_reference_id: binding.native_reference_id,
            evidence: s.evidence,
          };
        }),
      ),
    ),
    verifyCurrentAuthorization: Effect.fn("SyntheticRollupEvidence.verify")((m: Manifest) =>
      Effect.sync(() =>
        m.sources.every((source) => {
          const s = scenario(m, source);
          return s !== undefined && allowed(s);
        }),
      ),
    ),
    readExtractionInputs: Effect.fn("SyntheticRollupEvidence.read")(
      (m: Manifest, signal: AbortSignal) =>
        Effect.gen(function* () {
          if (signal.aborted) return yield* new RollupError({ code: "input_unavailable" });
          const inputs: ExtractionInput[] = [];
          for (const source of m.sources) {
            const s = scenario(m, source);
            if (!s || !allowed(s)) return yield* new RollupError({ code: "input_unavailable" });
            inputs.push(s.input);
          }
          return yield* Effect.try({
            try: () => createProviderInput(m, inputs).extractions,
            catch: () => new RollupError({ code: "input_unavailable" }),
          });
        }),
    ),
  });
};
/** @public Deterministic synthetic inference, without any provider process or account lookup. */
export const syntheticGenerationLayer = (identity: Manifest["extractor"], result: Result) =>
  Layer.succeed(RollupGenerationProvider, {
    identity: immutable(identity),
    configured: true,
    generate: Effect.fn("SyntheticRollupGeneration.generate")(function* (
      _input: ProviderInput,
      signal: AbortSignal,
    ) {
      if (signal.aborted) return yield* new RollupError({ code: "provider_failure" });
      return immutable(result);
    }),
  });

/** @public Small deterministic configuration for isolated synthetic consumers. */
export function syntheticFixture(workstreamId = "ws:test", accountId = "account:test") {
  const result: Result = {
    outcome: {
      text: "Synthetic private summary",
      sources: [{ source_set_id: "source:test", member_ordinal: 0 }],
    },
    semantic_status: "active",
    blockers: [],
    decisions: [],
    next_steps: [],
    follow_up_candidates: [],
    confidence: 0.8,
  };
  const identity: Manifest["extractor"] = {
    extractor_version: "rollup-v1",
    provider: "synthetic",
    model: "synthetic",
    prompt_template_sha256: digest("synthetic-template"),
    result_schema_version: "2.0.0",
  };
  const binding: RollupBinding = {
    owner_id: "owner:test",
    workstream_id: workstreamId,
    native_reference_id: "ref:test",
    provider_id: "synthetic",
    account_id: accountId,
    selection: {
      kind: "thread",
      conversation_id: "conversation:test",
      thread_id: "thread:test",
      selected_branch_id: "branch:test",
    },
    binding_version: 1,
    state: "active",
  };
  const input: ExtractionInput = {
    account_id: accountId,
    source_set_id: "source:test",
    extraction_id: "extraction:test",
    output_sha256: digest(canonicalJson(result)),
    result,
  };
  const evidence: SourceEvidence = {
    account_id: accountId,
    source_set_id: input.source_set_id,
    source_set_sha256: digest("synthetic-source"),
    source_member_count: 1,
    extraction_id: input.extraction_id,
    extraction_attempt_id: "attempt:test",
    extraction_fence: 1,
    extraction_output_sha256: input.output_sha256,
    extraction_output_bytes: Buffer.byteLength(canonicalJson(result)),
    authorization_policy_sha256: digest("synthetic-policy"),
    authorization_principal_id: "principal:test",
    authorization_grant_id: "grant:test",
    authorization_decision_id: "decision:test",
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
  const manifest = createManifest({
    owner_id: binding.owner_id,
    workstream_id: workstreamId,
    workstream_version: 1,
    registry_version: 1,
    declaration_revisions: [],
    relationship_revisions: [],
    contribution_policy: "active-primary-and-secondary-v1",
    exclusions: [],
    extractor: identity,
    sources: [
      {
        ...evidence,
        native_reference_id: binding.native_reference_id,
        membership_id: "membership:test",
        membership_registry_version: 1,
        native_observation_version: null,
      },
    ],
  });
  return { binding, input, evidence, manifest, result, identity };
}
