// @effect-diagnostics preferSchemaOverJson:off -- Original archive payload spelling is retained separately from decoded legacy evidence.
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Optional byte witnesses compare an exact byte digest.
import * as NodeCrypto from "node:crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SqlError } from "effect/sql/SqlError";
import * as Wire from "@t3tools/contracts";
import {
  WORKSTREAMS_LEGACY_RECONCILIATION_CONTRACT as contract,
  LegacyReservation,
  LegacyResult,
  LegacyStoredObservation,
  LegacySourceTerminal,
  LegacyReconciliationRequest,
  LegacyTerminalReceipt,
  LegacyProjectionTarget,
  type LegacyReconciliationBinding,
  type LegacyReconciliationReply,
  type LegacyReconciliationDelta,
} from "../../../../../../../packages/contracts/src/jones/workstreamsLegacyReconciliation.ts";
import * as NativeSql from "../store/sql.ts";
import * as Archive from "../store/archiveStore.ts";
import * as Registry from "../store/registryStore.ts";
import {
  readLegacyLedger,
  appendLegacyDelta,
  reconciliationDigest as hash,
} from "../store/legacyReconciliationStore.ts";
import { nativeSettlementResult, publicPrObservation } from "../domain/externalIntents.ts";
import { requireAttested } from "../domain/decider.ts";
import { validTimestamp } from "../domain/validation.ts";
import { canonical, TransitionError } from "./canonical.ts";
import { archiveScope, readTarget, type ArchivedRow } from "./archive.ts";
import { loadManifest } from "./apply.ts";
import { readback } from "./readback.ts";
import type { ImportPlan } from "./plan.ts";

/** Host-owned authenticated lookup only. Never expose this port or accept its evidence from command JSON. */
export const LegacySourceLookup = Context.Reference<{
  readonly lookup: (input: {
    readonly binding: LegacyReconciliationBinding;
    readonly reservation: LegacyReservation;
  }) => Effect.Effect<
    LegacySourceTerminal | { readonly kind: "unknown"; readonly reason: string },
    TransitionError
  >;
}>("jones/workstreams/LegacySourceLookup", {
  defaultValue: () => ({
    lookup: () => Effect.succeed({ kind: "unknown" as const, reason: "source_unconfigured" }),
  }),
});
const decodeReservation = Schema.decodeUnknownEffect(LegacyReservation);
const decodeResult = Schema.decodeUnknownEffect(LegacyResult);
const decodeSource = Schema.decodeUnknownEffect(LegacySourceTerminal);
const decodeRequest = Schema.decodeUnknownEffect(LegacyReconciliationRequest);
const decodeTarget = Schema.decodeUnknownEffect(LegacyProjectionTarget);
const decodeReceipt = Schema.decodeUnknownEffect(LegacyTerminalReceipt);
const decodeReference = Schema.decodeUnknownEffect(Wire.NativeReference);
const decodeMembership = Schema.decodeUnknownEffect(Wire.MembershipEpisode);
const decodeWorkstream = Schema.decodeUnknownEffect(Wire.Workstream);
const checked = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.mapError(() => new TransitionError({ reason: "legacy_evidence_conflict" })));
const parsePayload = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const entry = Effect.fn("LegacyReconciliation.entry")(function* (
  p: ImportPlan,
  scope: NativeSql.OwnerScope,
  className: string,
  id: string,
) {
  const archive = yield* Archive.ArchiveStore;
  const raw = yield* archive.readArchive(
    scope,
    p.import_run_id,
    className,
    canonical([scope.owner_id, id]),
  );
  if (!raw || typeof raw !== "object" || !("source" in raw) || !("targets" in raw))
    return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
  // The immutable source is independently checked against the pinned plan and its digest by readback.
  const saved = raw as ArchivedRow;
  const original = p.snapshot.rows.find(
    (r) => r.class === className && canonical(r.key) === canonical([scope.owner_id, id]),
  );
  if (!original || canonical(saved.source) !== canonical(original))
    return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
  return saved;
});
const prepare = Effect.fn("LegacyReconciliation.prepare")(function* (
  p: ImportPlan,
  ownerId: string,
  principalId: string,
  request: LegacyReconciliationRequest,
) {
  const registry = yield* Registry.RegistryStore,
    archive = yield* Archive.ArchiveStore;
  const scope = { ...archiveScope(p), owner_id: ownerId };
  const run = yield* loadManifest(p);
  const context = yield* registry.context(scope);
  if (
    !context ||
    !["applied", "verified", "activated"].includes(run.state) ||
    context.home_environment_id !== p.home_environment_id ||
    context.home_environment_id !== request.home_environment_id ||
    context.active_key_id !== p.active_key_id ||
    context.server_generation !== request.expected_server_generation
  )
    return yield* new TransitionError({ reason: "legacy_import_fence" });
  const ledger = yield* readLegacyLedger(scope, p.import_run_id);
  const existing = ledger.deltas.find((d) => d.binding.command_id === request.command_id);
  if (existing) {
    if (
      existing.binding.principal_id !== principalId ||
      existing.request_sha256 !== hash(request) ||
      existing.binding.home_environment_id !== context.home_environment_id ||
      existing.binding.home_generation !== context.server_generation
    )
      return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
    yield* archive.evidence(scope, request.command_id);
    for (const change of existing.changes.filter((c) => c.after.table !== "references")) {
      const stored = yield* readTarget(change.after);
      if (
        canonical(stored.data) !== canonical(change.after.data) ||
        (change.after.context && canonical(stored.value) !== canonical(change.after.value))
      )
        return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
    }
    if (context.state === "importing") yield* readback(p);
    else if (run.state !== "activated" || !["active", "fenced"].includes(context.state))
      return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
    return { scope, context, ledger, existing };
  }
  if (context.state !== "importing" || !["applied", "verified"].includes(run.state))
    return yield* new TransitionError({ reason: "legacy_import_fence" });
  yield* readback(p);
  const original = yield* archive.evidence(scope, request.command_id);
  if (
    !original ||
    original.principal_id !== principalId ||
    !["pending", "unresolved"].includes(original.state) ||
    original.request_sha256 !== request.request_sha256
  )
    return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
  const receiptEntry = yield* entry(p, scope, "workstream_command_receipts", request.command_id);
  const reservationEntry = yield* entry(
    p,
    scope,
    "workstream_external_reservations",
    request.command_id,
  );
  const reservation = yield* checked(
    decodeReservation(yield* checked(parsePayload(reservationEntry.source.row.payload))),
  );
  const r = reservation,
    a = r.command.action,
    source = r.source_binding;
  if (
    hash(r) !== request.reservation_sha256 ||
    r.owner_id !== ownerId ||
    r.actor.principal_id !== principalId ||
    r.authorization.owner_id !== ownerId ||
    canonical(r.actor) !== canonical(r.authorization.actor) ||
    r.authorization.grant.owner_id !== ownerId ||
    r.authorization.grant.principal_id !== principalId ||
    r.command.command_id !== request.command_id ||
    r.request_sha256 !== request.request_sha256 ||
    r.accepted_at !== JSON.parse(original.payload).accepted_at ||
    r.authorization.server_generation !== context.server_generation ||
    r.command.expected_server_generation !== context.server_generation ||
    original.server_generation !== context.server_generation ||
    r.command.expected_registry_version !== r.admitted_registry_version ||
    r.reference.owner_id !== ownerId ||
    source.state !== "active" ||
    source.source_instance_id !== r.reference.identity.source_instance_id ||
    source.provider !== r.reference.identity.provider ||
    reservationEntry.source.row.state !== "active" ||
    reservationEntry.source.row.principal_id !== principalId ||
    reservationEntry.source.row.effect !== r.kind ||
    reservationEntry.source.row.native_reference_id !== r.reference.native_reference_id ||
    reservationEntry.source.row.admitted_registry_version !== r.admitted_registry_version ||
    reservationEntry.source.row.server_generation !== context.server_generation ||
    original.operation !== a.operation ||
    ((a.operation === "verify_reference" || a.operation === "request_native_t3_settlement") &&
      a.native_reference_id !== r.reference.native_reference_id) ||
    (r.kind === "native_t3_settlement"
      ? a.operation !== "request_native_t3_settlement"
      : a.operation !== r.kind)
  )
    return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
  if (request.original_byte_witness !== undefined) {
    if (hashBytes(request.original_byte_witness) !== request.request_sha256)
      return yield* new TransitionError({ reason: "original_witness_mismatch" });
    return yield* new TransitionError({ reason: "original_witness_unconfigured" });
  }
  const binding: LegacyReconciliationBinding = {
    ...scope,
    principal_id: principalId,
    command_id: request.command_id,
    import_run_id: p.import_run_id,
    snapshot_id: p.snapshot.header.snapshot_id,
    snapshot_sha256: p.snapshot.snapshot_sha256,
    receipt_row_sha256: hash(receiptEntry.source),
    reservation_row_sha256: hash(reservationEntry.source),
    request_sha256: request.request_sha256,
    reservation_sha256: hash(r),
    source_identity: r.reference.identity,
    authority_namespace: source.authority_namespace,
    store_generation: source.store_generation,
    home_environment_id: context.home_environment_id,
    home_generation: context.server_generation,
  };
  const receiptTarget = yield* checked(decodeTarget(receiptEntry.targets[0])),
    reservationTarget = yield* checked(decodeTarget(reservationEntry.targets[0]));
  if (
    receiptTarget?.table !== "receipts" ||
    receiptTarget.data.origin !== "legacy_open" ||
    reservationTarget?.table !== "external_reservations" ||
    reservationTarget.data.state !== "active"
  )
    return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
  const resultSource = p.snapshot.rows.find(
    (x) =>
      x.class === "workstream_external_results" &&
      x.row.owner_id === ownerId &&
      x.row.command_id === request.command_id,
  );
  const retained = resultSource
    ? yield* checked(decodeResult(yield* checked(parsePayload(resultSource.row.payload))))
    : null;
  return {
    scope,
    context,
    ledger,
    existing: null,
    reservation,
    binding,
    receiptTarget,
    reservationTarget,
    retained,
    resultSource,
  };
});
const hashBytes = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");

function matchingLegacyCache(
  value: typeof LegacyStoredObservation.Type | null,
  r: LegacyReservation,
  generation: number,
) {
  const cache = value?.observation_cache;
  return cache &&
    value &&
    value.last_success &&
    r.reference.pr_locator &&
    cache.owner_id === r.owner_id &&
    cache.native_reference_id === r.reference.native_reference_id &&
    cache.source_instance_id === r.reference.identity.source_instance_id &&
    cache.provider === r.reference.identity.provider &&
    canonical(cache.locator) === canonical(r.reference.pr_locator) &&
    cache.server_generation === generation &&
    cache.authority_namespace === r.source_binding.authority_namespace &&
    cache.store_generation === r.source_binding.store_generation &&
    cache.observation_version === value.observation_version &&
    canonical(cache.representation) === canonical(value.last_success) &&
    cache.etag.length > 0 &&
    Buffer.byteLength(cache.etag, "utf8") <= 512 &&
    [...cache.etag].every((c) => {
      const code = c.codePointAt(0)!;
      return code > 31 && code !== 127;
    })
    ? cache
    : null;
}
function legacyObservationCache(
  r: LegacyReservation,
  value: NonNullable<LegacyResult["observation"]>,
  etag: string,
) {
  if (!value.last_success || !r.reference.pr_locator) return null;
  return matchingLegacyCache(
    {
      ...value,
      observation_cache: {
        schema_version: 1,
        owner_id: r.owner_id,
        native_reference_id: r.reference.native_reference_id,
        source_instance_id: r.reference.identity.source_instance_id,
        provider: r.reference.identity.provider,
        locator: r.reference.pr_locator,
        server_generation: r.authorization.server_generation,
        authority_namespace: r.source_binding.authority_namespace,
        store_generation: r.source_binding.store_generation,
        observation_version: value.observation_version,
        etag,
        representation: value.last_success,
      },
    },
    r,
    r.authorization.server_generation,
  );
}
function validateResult(r: LegacyReservation, result: LegacyResult) {
  if (r.kind === "native_t3_settlement") {
    if (
      !result.native_outcome ||
      canonical(result) !== canonical(nativeSettlementResult(r, result.native_outcome))
    )
      throw new TransitionError({ reason: "legacy_evidence_conflict" });
  } else if (r.kind === "verify_reference") {
    if (!result.registration || Object.keys(result).length !== 1)
      throw new TransitionError({ reason: "legacy_evidence_conflict" });
  } else if (
    !result.observation ||
    Object.keys(result).some((k) => !["observation", "observation_cache"].includes(k))
  )
    throw new TransitionError({ reason: "legacy_evidence_conflict" });
}
function validateSource(
  r: LegacyReservation,
  b: LegacyReconciliationBinding,
  e: LegacySourceTerminal,
) {
  if (canonical(e.binding) !== canonical(b))
    throw new TransitionError({ reason: "legacy_evidence_conflict" });
  validateResult(r, e.result);
  if (r.kind !== "native_t3_settlement") {
    if (e.settlement_request !== null || e.native_receipt !== null || e.settlement_event !== null)
      throw new TransitionError({ reason: "legacy_evidence_conflict" });
    return;
  }
  const receipt = e.native_receipt,
    event = e.settlement_event,
    action = r.command.action;
  if (
    action.operation !== "request_native_t3_settlement" ||
    (receipt &&
      (receipt.aggregateKind !== "thread" ||
        receipt.aggregateId !== r.reference.identity.native_id))
  )
    throw new TransitionError({ reason: "legacy_evidence_conflict" });
  if (
    canonical(e.settlement_request) !==
    canonical({
      owner_id: r.owner_id,
      principal_id: r.actor.principal_id,
      command_id: r.command.command_id,
      native_reference_id: r.reference.native_reference_id,
      native_action: action.native_action,
      request_sha256: r.request_sha256,
      reservation_sha256: hash(r),
      identity: r.reference.identity,
      expected_authority_namespace: r.source_binding.authority_namespace,
      expected_store_generation: r.source_binding.store_generation,
    })
  )
    throw new TransitionError({ reason: "legacy_evidence_conflict" });
  if (e.result.native_outcome === "committed") {
    if (
      !receipt ||
      receipt.status !== "accepted" ||
      !event ||
      event.commandId !== receipt.commandId ||
      event.aggregateKind !== "thread" ||
      event.aggregateId !== r.reference.identity.native_id ||
      event.sequence > receipt.resultSequence ||
      event.type !== "thread." + (action.native_action === "settle" ? "settled" : "unsettled")
    )
      throw new TransitionError({ reason: "legacy_evidence_conflict" });
  } else if (event !== null || (receipt && receipt.status !== "rejected"))
    throw new TransitionError({ reason: "legacy_evidence_conflict" });
}
export const reconcileLegacyOpen = Effect.fn("WorkstreamsTransition.reconcileLegacyOpen")(
  function* (
    p: ImportPlan,
    ownerId: string,
    principalId: string,
    input: LegacyReconciliationRequest,
  ): Effect.fn.Return<
    LegacyReconciliationReply,
    SqlError | NativeSql.StoreError | NativeSql.CryptoError | TransitionError,
    SqlClient.SqlClient | NativeSql.NativeSql | Archive.ArchiveStore | Registry.RegistryStore
  > {
    const request = yield* checked(decodeRequest(input));
    const sql = yield* SqlClient.SqlClient,
      db = yield* NativeSql.NativeSql;
    const first = yield* sql.withTransaction(prepare(p, ownerId, principalId, request));
    if (first.existing)
      return {
        contract: contract as typeof contract,
        result: { kind: "reconciled", proof: first.existing, delta_root: hash(first.existing) },
      };
    if (!("reservation" in first) || !first.reservation)
      return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
    let result: LegacyResult,
      provenance: LegacyReconciliationDelta["provenance"],
      completedAt: string;
    if (first.retained) {
      yield* Effect.try({
        try: () => validateResult(first.reservation, first.retained!),
        catch: () => new TransitionError({ reason: "legacy_evidence_conflict" }),
      });
      result = first.retained;
      // Completion is recorded at reconciliation time; the historical provider completion time is unknown.
      completedAt = DateTime.formatIso(yield* DateTime.now);
      provenance = {
        kind: "retained_terminal_result",
        evidence_sha256: hash(first.resultSource),
        result_row_sha256: hash(first.resultSource),
        evidence: first.resultSource,
      };
    } else {
      const port = yield* LegacySourceLookup;
      const observed = yield* port.lookup({
        binding: first.binding,
        reservation: first.reservation,
      });
      if ("kind" in observed)
        return {
          contract: contract as typeof contract,
          result: { kind: "unknown", reason: observed.reason },
        };
      const evidence = yield* checked(decodeSource(observed));
      yield* Effect.try({
        try: () => validateSource(first.reservation, first.binding, evidence),
        catch: () => new TransitionError({ reason: "legacy_evidence_conflict" }),
      });
      result = evidence.result;
      completedAt = evidence.completed_at;
      provenance = {
        kind: "authenticated_source_lookup",
        evidence_sha256: hash(evidence),
        result_row_sha256: null,
        evidence,
      };
    }
    if (
      !validTimestamp(completedAt) ||
      Date.parse(completedAt) < Date.parse(first.reservation.accepted_at)
    )
      return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        // Whole-import readback spans owners; keep it inside SQL but outside ActiveOwner.
        const current = yield* prepare(p, ownerId, principalId, request);
        if (current.existing)
          return {
            contract: contract as typeof contract,
            result: {
              kind: "reconciled" as const,
              proof: current.existing,
              delta_root: hash(current.existing),
            },
          };
        if (
          !("reservation" in current) ||
          !current.reservation ||
          canonical(current.binding) !== canonical(first.binding) ||
          canonical(current.retained) !== canonical(first.retained)
        )
          return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
        const completed = yield* Effect.gen(function* () {
          const r = current.reservation,
            a = r.command.action,
            registry = yield* Registry.RegistryStore;
          const changes: LegacyReconciliationDelta["changes"][number][] = [];
          let registration: LegacyResult["registration"] | null = null,
            observation: LegacyResult["observation"] | null = null;
          const versions: { workstream_id: string; version: number }[] = [],
            memberships: string[] = [];
          const add = (before: LegacyProjectionTarget | null, after: LegacyProjectionTarget) =>
            changes.push({ before, after });
          if (r.kind !== "native_t3_settlement") {
            if (current.context.registry_version !== r.admitted_registry_version)
              return yield* new TransitionError({ reason: "legacy_stale_fence" });
            const refs = yield* registry.list(current.scope, "references");
            const saved = refs.find(
              (x) => x.metadata.native_reference_id === r.reference.native_reference_id,
            );
            if (!saved) return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
            const reference = yield* checked(decodeReference(saved.value));
            const bindings = yield* registry.listPlain(current.scope, "source_bindings");
            const binding = bindings
              .filter(
                (x) =>
                  x.source_instance_id === r.source_binding.source_instance_id &&
                  x.provider === r.source_binding.provider,
              )
              .sort((x, y) => Number(y.binding_version) - Number(x.binding_version))[0];
            if (
              canonical(reference) !== canonical(r.reference) ||
              !binding ||
              binding.state !== "active" ||
              binding.authority_namespace !== r.source_binding.authority_namespace ||
              binding.store_generation !== r.source_binding.store_generation
            )
              return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
            if (
              r.kind === "verify_reference" &&
              a.operation === "verify_reference" &&
              result.registration
            ) {
              registration = result.registration;
              if (
                reference.registration.attestation_version !== a.expected_attestation_version ||
                registration.attestation_version !== a.expected_attestation_version + 1
              )
                return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
              if (registration.state === "attested")
                yield* Effect.try({
                  try: () =>
                    requireAttested(
                      { ...reference, registration: result.registration! },
                      completedAt,
                      r.source_binding,
                    ),
                  catch: () => new TransitionError({ reason: "legacy_evidence_conflict" }),
                });
              const key = { ...current.scope, native_reference_id: reference.native_reference_id };
              const before: LegacyProjectionTarget = {
                table: "references",
                key,
                data: { ...current.scope, ...saved.metadata },
                context: {
                  purpose: "reference",
                  record_id: canonical([reference.native_reference_id]),
                  revision: reference.registration.attestation_version,
                },
                value: reference,
              };
              const evidence =
                reference.identity.provider === "t3" &&
                reference.identity.resource_kind === "thread" &&
                reference.identity.id_kind === "internal"
                  ? registration.evidence
                  : null;
              add(before, {
                ...before,
                data: {
                  ...before.data,
                  attestation_version: registration.attestation_version,
                  registration_state: registration.state,
                  routing_source_instance_id: evidence?.source_instance_id ?? null,
                  routing_native_id: evidence?.native_id ?? null,
                  routing_authority_namespace: evidence?.authority_namespace ?? null,
                  routing_store_generation: evidence?.store_generation ?? null,
                  routing_attested_at: evidence ? registration.attested_at : null,
                  routing_expires_at: evidence ? registration.expires_at : null,
                  routing_evidence_sha256: evidence?.evidence_sha256 ?? null,
                },
                context: { ...before.context!, revision: registration.attestation_version },
                value: { ...reference, registration },
              });
            } else if (
              r.kind === "refresh_linked_pr" &&
              a.operation === "refresh_linked_pr" &&
              result.observation
            ) {
              yield* Effect.try({
                try: () => requireAttested(reference, completedAt, r.source_binding),
                catch: () => new TransitionError({ reason: "legacy_evidence_conflict" }),
              });
              const member = (yield* registry.list(current.scope, "memberships")).find(
                (x) => x.metadata.membership_id === a.membership_id,
              );
              const work = (yield* registry.list(current.scope, "records")).find(
                (x) => x.metadata.workstream_id === a.workstream_id,
              );
              const m = member ? yield* checked(decodeMembership(member.value)) : null,
                w = work ? yield* checked(decodeWorkstream(work.value)) : null;
              const previous = (yield* registry.list(current.scope, "pr_observations"))
                .filter((x) => x.metadata.native_reference_id === reference.native_reference_id)
                .sort(
                  (x, y) =>
                    Number(y.metadata.observation_version) - Number(x.metadata.observation_version),
                )[0];
              observation = result.observation;
              if (
                !m ||
                m.closed !== null ||
                m.native_reference_id !== reference.native_reference_id ||
                m.workstream_id !== a.workstream_id ||
                canonical(m) !== canonical(r.membership) ||
                !w ||
                w.version !== a.expected_version ||
                Number(previous?.metadata.observation_version ?? 0) !==
                  a.expected_observation_version ||
                canonical(previous?.value ?? null) !== canonical(r.previous_observation) ||
                observation.command_id !== r.command.command_id ||
                observation.native_reference_id !== reference.native_reference_id ||
                observation.observation_version !== a.expected_observation_version + 1
              )
                return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
              const previousCache = matchingLegacyCache(
                r.previous_observation,
                r,
                current.context.server_generation,
              );
              let cache = matchingLegacyCache(
                { ...observation, observation_cache: result.observation_cache ?? null },
                r,
                current.context.server_generation,
              );
              if (observation.outcome === "not_modified") {
                if (
                  !previousCache ||
                  !observation.last_success ||
                  canonical({
                    ...observation.last_success,
                    observed_at: previousCache.representation.observed_at,
                  }) !== canonical(previousCache.representation)
                )
                  return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
                cache ??= legacyObservationCache(r, observation, previousCache.etag);
              } else if (observation.outcome !== "observed") {
                cache =
                  previousCache &&
                  canonical(observation.last_success) === canonical(previousCache.representation)
                    ? legacyObservationCache(r, observation, previousCache.etag)
                    : null;
              }
              observation = publicPrObservation(observation);
              versions.push({ workstream_id: w.workstream_id, version: w.version });
              memberships.push(m.membership_id);
              add(null, {
                table: "pr_observations",
                key: {
                  ...current.scope,
                  native_reference_id: reference.native_reference_id,
                  observation_version: observation.observation_version,
                },
                data: {
                  ...current.scope,
                  native_reference_id: reference.native_reference_id,
                  observation_version: observation.observation_version,
                  attempted_at: observation.attempted_at,
                  outcome: observation.outcome,
                },
                context: {
                  purpose: "pr_observation",
                  record_id: canonical([
                    reference.native_reference_id,
                    observation.observation_version,
                  ]),
                  revision: observation.observation_version,
                },
                value: { ...observation, observation_cache: cache },
              });
            } else return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
          }
          const changed =
            r.kind !== "native_t3_settlement" || result.native_outcome === "committed";
          const version = current.context.registry_version + (changed ? 1 : 0);
          const receipt = yield* checked(
            decodeReceipt({
              command_id: r.command.command_id,
              owner_id: ownerId,
              actor: r.actor,
              operation: a.operation,
              request_sha256: r.request_sha256,
              server_generation: current.context.server_generation,
              accepted_at: r.accepted_at,
              completed_at: completedAt,
              state: "committed",
              registry_version: version,
              changed,
              effects: {
                workstream_versions: versions,
                native_reference_id: r.reference.native_reference_id,
                membership_ids: memberships,
                declaration_id: null,
                declaration_revision: null,
                edge_id: null,
                observation,
                registration,
                lifecycle_declaration: null,
                coordination_disposition: null,
                native_settlement:
                  r.kind === "native_t3_settlement" &&
                  a.operation === "request_native_t3_settlement"
                    ? {
                        native_reference_id: r.reference.native_reference_id,
                        native_action: a.native_action,
                        outcome: result.native_outcome,
                      }
                    : null,
              },
            }),
          );
          add(current.receiptTarget, {
            ...current.receiptTarget,
            table: "receipts",
            data: {
              ...current.receiptTarget.data,
              state: "committed",
              registry_version: version,
              completed_at: completedAt,
            },
            context: { purpose: "receipt", record_id: r.command.command_id, revision: version },
            value: {
              receipt,
              request_body: "",
              legacy_request_bytes: "unknown",
              legacy_command: r.command,
              reconciliation_contract: contract,
            },
          });
          if (!current.retained)
            add(null, {
              table: "external_results",
              key: { ...current.scope, command_id: r.command.command_id },
              data: { ...current.scope, command_id: r.command.command_id },
              context: {
                purpose: "external_result",
                record_id: r.command.command_id,
                revision: r.admitted_registry_version,
              },
              value: result,
            });
          const high = yield* sql<{
            high: number;
          }>`SELECT coalesce(max(event_sequence),0) AS high FROM jones_workstreams_history WHERE registry_instance_id=${current.scope.registry_instance_id} AND owner_id=${ownerId}`;
          const eventId = "event:legacy:" + hash([p.import_run_id, ownerId, r.command.command_id]),
            sequence = high[0]!.high + 1;
          const event = {
            event_id: eventId,
            command_id: r.command.command_id,
            actor: r.actor,
            operation: a.operation,
            occurred_at: completedAt,
            registry_version: version,
            changed,
            workstream_versions: versions,
            native_reference_id: r.reference.native_reference_id,
            membership_ids: memberships,
            declaration_id: null,
            declaration_revision: null,
            edge_id: null,
            lifecycle_declaration: null,
          };
          const workstreamIds = versions.map((x) => x.workstream_id);
          add(null, {
            table: "history",
            key: { ...current.scope, event_id: eventId },
            data: {
              ...current.scope,
              event_id: eventId,
              event_sequence: sequence,
              legacy_event_sequence: null,
              command_id: r.command.command_id,
              registry_version: version,
              occurred_at: completedAt,
              workstream_ids: canonical(workstreamIds),
            },
            context: { purpose: "history_event", record_id: eventId, revision: version },
            value: {
              event_id: eventId,
              command_id: r.command.command_id,
              registry_version: version,
              occurred_at: completedAt,
              workstream_ids: workstreamIds,
              value: event,
            },
          });
          for (const scopeKey of ["global", ...workstreamIds.map((x) => "workstream:" + x)])
            add(null, {
              table: "history_scopes",
              key: {
                ...current.scope,
                scope_key: scopeKey,
                registry_version: version,
                event_id: eventId,
              },
              data: {
                ...current.scope,
                scope_key: scopeKey,
                registry_version: version,
                event_id: eventId,
                event_sequence: sequence,
              },
              context: null,
              value: null,
            });
          add(current.reservationTarget, {
            ...current.reservationTarget,
            table: "external_reservations",
            data: { ...current.reservationTarget.data, state: "released" },
          });
          for (const change of changes) {
            const t = change.after,
              before = change.before;
            const encrypted =
              t.context &&
              (!before ||
                canonical(before.value) !== canonical(t.value) ||
                canonical(before.context) !== canonical(t.context))
                ? yield* db.sealJson({ ...current.scope, ...t.context }, t.value)
                : null;
            if (!before)
              yield* sql`INSERT INTO ${sql("jones_workstreams_" + t.table)} ${sql.insert(encrypted ? { ...t.data, envelope: encrypted } : t.data)}`;
            else {
              const fields = Object.fromEntries(
                Object.entries(t.data).filter(([k, v]) => before.data[k] !== v),
              );
              if (encrypted) fields.envelope = encrypted;
              if (Object.keys(fields).length)
                yield* sql`UPDATE ${sql("jones_workstreams_" + t.table)} SET ${sql.update(fields)} WHERE ${sql.and(Object.entries(t.key).map(([k, v]) => sql`${sql(k)}=${v}`))}`;
            }
          }
          const updated = changed
            ? yield* sql`UPDATE jones_workstreams_registries SET registry_version=${version} WHERE registry_instance_id=${current.scope.registry_instance_id} AND owner_id=${ownerId} AND state='importing' AND home_environment_id=${current.context.home_environment_id} AND active_key_id=${p.active_key_id} AND server_generation=${current.context.server_generation} AND registry_version=${current.context.registry_version} RETURNING owner_id`
            : yield* sql`SELECT owner_id FROM jones_workstreams_registries WHERE registry_instance_id=${current.scope.registry_instance_id} AND owner_id=${ownerId} AND state='importing' AND home_environment_id=${current.context.home_environment_id} AND active_key_id=${p.active_key_id} AND server_generation=${current.context.server_generation} AND registry_version=${current.context.registry_version}`;
          if (updated.length !== 1)
            return yield* new TransitionError({ reason: "legacy_evidence_conflict" });
          const proof: LegacyReconciliationDelta = {
            contract,
            binding: current.binding,
            request_sha256: hash(request),
            sequence: current.ledger.deltas.length + 1,
            previous_root: current.ledger.root,
            before_registry_version: current.context.registry_version,
            after_registry_version: version,
            provenance,
            receipt,
            changes,
          };
          const root = yield* appendLegacyDelta(current.scope, proof);
          return {
            contract: contract as typeof contract,
            result: { kind: "reconciled" as const, proof, delta_root: root },
          };
        }).pipe((effect) => db.transaction(current.scope, effect));
        yield* readback(p);
        return completed;
      }),
    );
  },
);
