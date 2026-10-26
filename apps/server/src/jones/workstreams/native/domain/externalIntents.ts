// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous digests preserve the frozen cursor and exact-reservation wire bytes without I/O.
import * as NodeCrypto from "node:crypto";
import type {
  CommittedReceipt,
  NativeSettlementEffect,
  ReadContext,
  Receipt,
  RegistryExternalReservation,
  RegistryExternalResult,
  NativeReference,
  PrObservation,
  PrObservationCache,
  RegistrySourceBinding,
  StoredPrObservation,
} from "./state.ts";
import { requireDomain } from "./errors.ts";
import { canonicalJson, validTimestamp, validateExternalValue } from "./validation.ts";

function digest(value: unknown): string {
  return NodeCrypto.createHash("sha256").update(canonicalJson(value)).digest("hex");
}

// Bind only a trusted attempt/readback result; these hashes prove association, not provider success.
export function nativeSettlementResult(
  reservation: RegistryExternalReservation,
  outcome: NativeSettlementEffect["outcome"],
): RegistryExternalResult {
  const action = reservation.command.action;
  requireDomain(
    reservation.kind === "native_t3_settlement" &&
      action.operation === "request_native_t3_settlement" &&
      reservation.owner_id === reservation.authorization.owner_id &&
      reservation.owner_id === reservation.reference.owner_id &&
      canonicalJson(reservation.actor) === canonicalJson(reservation.authorization.actor) &&
      reservation.actor.principal_id === reservation.authorization.grant.principal_id &&
      reservation.owner_id === reservation.authorization.grant.owner_id &&
      action.native_reference_id === reservation.reference.native_reference_id,
    "unknown_effect",
  );
  const binding = {
    owner_id: reservation.owner_id,
    principal_id: reservation.actor.principal_id,
    command_id: reservation.command.command_id,
    native_reference_id: reservation.reference.native_reference_id,
    native_action: action.native_action,
    request_sha256: reservation.request_sha256,
    reservation_sha256: digest(reservation),
  };
  return {
    native_outcome: outcome,
    native_evidence: { ...binding, result_sha256: digest({ ...binding, native_outcome: outcome }) },
  };
}

export function requireNativeResult(
  reservation: RegistryExternalReservation,
  result: RegistryExternalResult,
): NativeSettlementEffect {
  requireDomain(
    result.native_outcome !== undefined &&
      ["committed", "denied", "unsupported", "failed"].includes(result.native_outcome),
    "unknown_effect",
  );
  requireDomain(
    canonicalJson(result) ===
      canonicalJson(nativeSettlementResult(reservation, result.native_outcome)),
    "unknown_effect",
  );
  return {
    native_reference_id: reservation.reference.native_reference_id,
    native_action: result.native_evidence!.native_action,
    outcome: result.native_outcome,
  };
}

export function requireNativeAdmission(
  prior: Receipt | null | undefined,
  reservation: RegistryExternalReservation,
): asserts prior is Receipt {
  requireDomain(
    prior &&
      ["pending", "unresolved"].includes(prior.state) &&
      prior.owner_id === reservation.owner_id &&
      prior.command_id === reservation.command.command_id &&
      canonicalJson(prior.actor) === canonicalJson(reservation.actor) &&
      prior.operation === "request_native_t3_settlement" &&
      prior.operation === reservation.command.action.operation &&
      prior.request_sha256 === reservation.request_sha256 &&
      prior.accepted_at === reservation.accepted_at &&
      prior.server_generation === reservation.authorization.server_generation &&
      prior.server_generation === reservation.command.expected_server_generation,
    "unknown_effect",
  );
}

export function requireNativeRecoveryReceipt(
  prior: Receipt | null | undefined,
  reservation: RegistryExternalReservation | null | undefined,
  result: RegistryExternalResult | null | undefined,
  value: CommittedReceipt,
  context: ReadContext,
): void {
  requireDomain(reservation && result, "unknown_effect");
  requireNativeAdmission(prior, reservation);
  const effect = requireNativeResult(reservation, result);
  requireDomain(
    value.owner_id === context.owner_id &&
      value.owner_id === prior.owner_id &&
      value.command_id === prior.command_id &&
      canonicalJson(value.actor) === canonicalJson(prior.actor) &&
      value.operation === prior.operation &&
      value.request_sha256 === prior.request_sha256 &&
      value.accepted_at === prior.accepted_at &&
      value.state === "committed" &&
      value.server_generation === context.server_generation &&
      value.registry_version === context.registry_version &&
      value.changed === (effect.outcome === "committed") &&
      validTimestamp(value.completed_at) &&
      Date.parse(value.completed_at) >= Date.parse(value.accepted_at) &&
      canonicalJson(value.effects) ===
        canonicalJson({
          workstream_versions: [],
          native_reference_id: effect.native_reference_id,
          membership_ids: [],
          declaration_id: null,
          declaration_revision: null,
          edge_id: null,
          observation: null,
          registration: null,
          lifecycle_declaration: null,
          coordination_disposition: null,
          native_settlement: effect,
        }),
    "unknown_effect",
  );
}

export function publicPrObservation(value: PrObservation): PrObservation {
  const snapshot = value.last_success;
  return {
    native_reference_id: value.native_reference_id,
    observation_version: value.observation_version,
    attempted_at: value.attempted_at,
    outcome: value.outcome,
    retry_after_seconds: value.retry_after_seconds,
    last_success:
      snapshot === null
        ? null
        : {
            state: snapshot.state,
            draft: snapshot.draft,
            observed_at: snapshot.observed_at,
            provider_updated_at: snapshot.provider_updated_at,
          },
    command_id: value.command_id,
  };
}

function decodeStoredPrObservation(
  value: StoredPrObservation,
  ownerId: string,
  referenceId: string,
): StoredPrObservation {
  const observation = publicPrObservation(value);
  validateExternalValue("observation", observation);
  requireDomain(observation.native_reference_id === referenceId, "identity_conflict");
  const cache = value.observation_cache;
  const keys = [
    "schema_version",
    "owner_id",
    "native_reference_id",
    "source_instance_id",
    "provider",
    "locator",
    "server_generation",
    "authority_namespace",
    "store_generation",
    "observation_version",
    "etag",
    "representation",
  ];
  return {
    ...observation,
    observation_cache:
      cache &&
      typeof cache === "object" &&
      !Array.isArray(cache) &&
      Object.keys(cache).length === keys.length &&
      Object.keys(cache).every((key) => keys.includes(key)) &&
      cache.schema_version === 1 &&
      cache.owner_id === ownerId &&
      cache.native_reference_id === referenceId &&
      observation.native_reference_id === referenceId &&
      cache.observation_version === observation.observation_version &&
      Number.isSafeInteger(cache.observation_version) &&
      cache.observation_version > 0 &&
      typeof cache.etag === "string" &&
      cache.etag.length > 0 &&
      [...cache.etag].every((character) => {
        const code = character.codePointAt(0)!;
        return code > 31 && code !== 127;
      }) &&
      Buffer.byteLength(cache.etag, "utf8") <= 512 &&
      observation.last_success !== null &&
      canonicalJson(cache.representation) === canonicalJson(observation.last_success)
        ? cache
        : null,
  };
}

export function matchingPrCache(
  observation: StoredPrObservation | null,
  reference: NativeReference,
  serverGeneration: number,
  binding: RegistrySourceBinding,
): PrObservationCache | null {
  if (!observation) return null;
  const cache = decodeStoredPrObservation(
    observation,
    reference.owner_id,
    reference.native_reference_id,
  ).observation_cache;
  return cache &&
    reference.pr_locator !== null &&
    cache.provider === reference.identity.provider &&
    cache.source_instance_id === reference.identity.source_instance_id &&
    canonicalJson(cache.locator) === canonicalJson(reference.pr_locator) &&
    cache.server_generation === serverGeneration &&
    cache.authority_namespace === binding.authority_namespace &&
    cache.store_generation === binding.store_generation
    ? cache
    : null;
}

export function observationCache(
  intent: RegistryExternalReservation,
  observation: PrObservation,
  etag: string | null,
): PrObservationCache | null {
  if (!etag || !observation.last_success || !intent.reference.pr_locator) return null;
  const candidate: StoredPrObservation = {
    ...observation,
    observation_cache: {
      schema_version: 1,
      owner_id: intent.owner_id,
      native_reference_id: intent.reference.native_reference_id,
      provider: intent.reference.identity.provider,
      source_instance_id: intent.reference.identity.source_instance_id,
      locator: intent.reference.pr_locator,
      server_generation: intent.authorization.server_generation,
      authority_namespace: intent.source_binding.authority_namespace,
      store_generation: intent.source_binding.store_generation,
      observation_version: observation.observation_version,
      etag,
      representation: observation.last_success,
    },
  };
  return matchingPrCache(
    candidate,
    intent.reference,
    intent.authorization.server_generation,
    intent.source_binding,
  );
}
