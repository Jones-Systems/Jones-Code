// @effect-diagnostics nodeBuiltinImport:off -- SHA-256 preserves frozen rollup canonical bytes.
import * as NodeCrypto from "node:crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Wire from "@t3tools/contracts/jones/workstreamsRollups";
import type { RollupBinding } from "./bindings.ts";
import { canonicalJson } from "../domain/validation.ts";
export { canonicalJson };
export const digest = (value: string) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
export const EVIDENCE_CONTRACT = "workstreams-rollup-evidence/1.0.0" as const;
export type Manifest = Wire.WorkstreamsRollupManifest;
export type Summary = Wire.WorkstreamsRollupSummary;
export type ExtractionInput = typeof Wire.WorkstreamsRollupExtractionInput.Type;
export type ProviderInput = typeof Wire.WorkstreamsRollupProviderInput.Type;
export type Result = typeof Wire.WorkstreamsRollupExtractionResult.Type;
export type Claim = typeof Wire.WorkstreamsRollupClaim.Type;
export type Freshness = typeof Wire.WorkstreamsRollupFreshness.Type;
export type Exclusion = typeof Wire.WorkstreamsRollupExclusion.Type;
export type SourceEvidence = Omit<
  Wire.WorkstreamsRollupSource,
  | "membership_id"
  | "membership_registry_version"
  | "native_observation_version"
  | "native_reference_id"
>;
export type EvidenceDescriptor =
  | {
      readonly kind: "available";
      readonly native_reference_id: string;
      readonly evidence: SourceEvidence;
    }
  | {
      readonly kind: "excluded";
      readonly native_reference_id: string;
      readonly reason: Exclusion["reason"];
    };
export class RollupError extends Schema.TaggedError<RollupError>()("WorkstreamsRollupError", {
  code: Schema.Literals([
    "invalid_request",
    "content_unavailable",
    "provider_unconfigured",
    "not_found",
    "forbidden",
    "input_unavailable",
    "provider_failure",
    "provider_timeout",
    "stale_claim",
    "overloaded",
  ]),
}) {
  override get message() {
    return "Workstream summary operation unavailable.";
  }
}
export class RollupEvidenceProvider extends Context.Service<
  RollupEvidenceProvider,
  {
    readonly contract: typeof EVIDENCE_CONTRACT;
    readonly id: string;
    readonly configured: boolean;
    readonly describe: (
      bindings: readonly RollupBinding[],
    ) => Effect.Effect<readonly EvidenceDescriptor[], RollupError>;
    readonly readExtractionInputs: (
      manifest: Manifest,
      signal: AbortSignal,
    ) => Effect.Effect<readonly ExtractionInput[], RollupError>;
    readonly verifyCurrentAuthorization: (
      manifest: Manifest,
    ) => Effect.Effect<boolean, RollupError>;
  }
>()("t3/jones/workstreams/native/rollups/evidence/RollupEvidenceProvider") {}
/** @public Explicit default; missing content never becomes an empty complete rollup. */
export const unconfiguredLayer = Layer.succeed(RollupEvidenceProvider, {
  contract: EVIDENCE_CONTRACT,
  id: "unconfigured",
  configured: false,
  describe: Effect.fn("RollupEvidence.unconfigured.describe")(
    (bindings: readonly RollupBinding[]) =>
      Effect.succeed(
        bindings.map((binding) => ({
          kind: "excluded" as const,
          native_reference_id: binding.native_reference_id,
          reason: "content_unavailable" as const,
        })),
      ),
  ),
  readExtractionInputs: () => Effect.fail(new RollupError({ code: "provider_unconfigured" })),
  verifyCurrentAuthorization: () => Effect.succeed(false),
});
export function immutable<T>(value: T): T {
  const copied = structuredClone(value);
  const freeze = (item: unknown): void => {
    if (item !== null && typeof item === "object") {
      Object.values(item).forEach(freeze);
      Object.freeze(item);
    }
  };
  freeze(copied);
  return copied;
}
export function requireRollup(
  condition: unknown,
  code: RollupError["code"] = "content_unavailable",
): asserts condition {
  if (!condition) throw new RollupError({ code });
}
export const attempt = <A>(f: () => A, code: RollupError["code"] = "content_unavailable") =>
  Effect.try({ try: f, catch: () => new RollupError({ code }) });
