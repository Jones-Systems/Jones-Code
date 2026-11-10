import * as Schema from "effect/Schema";
import {
  WorkstreamCommand,
  WorkstreamCommittedReceipt,
  WorkstreamReceiptEffects,
  WorkstreamPrObservation,
  WorkstreamActor,
  WorkstreamOwnerGrant,
  NativeReference,
  MembershipEpisode,
  WorkstreamRegistrationVerification,
  WorkstreamPrLocator,
} from "../workstreams.ts";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeDigest as Digest,
  WorkstreamsNativeId as Id,
  WorkstreamsNativeVersion as Version,
  WorkstreamsNativeSequence as Sequence,
} from "./workstreamsNative.ts";

export const WORKSTREAMS_LEGACY_RECONCILIATION_CONTRACT = "workstreams-legacy-reconciliation/1.0.0";
const Address = Schema.String;
export const LegacyCommand = closed(
  Schema.Struct({
    ...WorkstreamCommand.fields,
    command_id: Address,
  }),
);
export const LegacyObservation = closed(
  Schema.Struct({
    ...WorkstreamPrObservation.fields,
    command_id: Address,
  }),
);
const Cache = closed(
  Schema.Struct({
    schema_version: Schema.Literal(1),
    owner_id: Id,
    native_reference_id: Id,
    source_instance_id: Id,
    provider: Id,
    locator: WorkstreamPrLocator,
    server_generation: Version,
    authority_namespace: Id,
    store_generation: Version,
    observation_version: Version,
    etag: Schema.String,
    representation: Schema.Struct({
      state: Schema.Literals(["open", "closed", "merged"]),
      draft: Schema.Boolean,
      observed_at: Schema.String,
      provider_updated_at: Schema.NullOr(Schema.String),
    }),
  }),
);
export const LegacyStoredObservation = closed(
  Schema.Struct({
    ...WorkstreamPrObservation.fields,
    command_id: Address,
    observation_cache: Schema.optionalKey(Schema.NullOr(Cache)),
  }),
);
const Binding = closed(
  Schema.Struct({
    provider: Id,
    source_instance_id: Id,
    authority_namespace: Id,
    store_generation: Version,
    state: Schema.Literals(["active", "revoked"]),
  }),
);
export const LegacyReservation = closed(
  Schema.Struct({
    kind: Schema.Literals(["verify_reference", "native_t3_settlement", "refresh_linked_pr"]),
    command: LegacyCommand,
    owner_id: Id,
    actor: WorkstreamActor,
    authorization: closed(
      Schema.Struct({
        owner_id: Id,
        actor: WorkstreamActor,
        grant: WorkstreamOwnerGrant,
        server_generation: Version,
      }),
    ),
    request_sha256: Digest,
    accepted_at: Schema.String,
    admitted_registry_version: Sequence,
    reference: NativeReference,
    source_binding: Binding,
    membership: Schema.NullOr(MembershipEpisode),
    previous_observation: Schema.NullOr(LegacyStoredObservation),
  }),
);
export type LegacyReservation = typeof LegacyReservation.Type;
export const LegacyResult = closed(
  Schema.Struct({
    registration: Schema.optionalKey(WorkstreamRegistrationVerification),
    observation: Schema.optionalKey(LegacyObservation),
    observation_cache: Schema.optionalKey(Schema.NullOr(Cache)),
    native_outcome: Schema.optionalKey(
      Schema.Literals(["committed", "denied", "unsupported", "failed"]),
    ),
    native_evidence: Schema.optionalKey(
      closed(
        Schema.Struct({
          owner_id: Id,
          principal_id: Id,
          command_id: Address,
          native_reference_id: Id,
          native_action: Schema.Literals(["settle", "unsettle"]),
          request_sha256: Digest,
          reservation_sha256: Digest,
          result_sha256: Digest,
        }),
      ),
    ),
  }),
);
export type LegacyResult = typeof LegacyResult.Type;
export const LegacyTerminalReceipt = closed(
  Schema.Struct({
    ...WorkstreamCommittedReceipt.fields,
    command_id: Address,
    effects: closed(
      Schema.Struct({
        ...WorkstreamReceiptEffects.fields,
        observation: Schema.NullOr(LegacyObservation),
      }),
    ),
  }),
);
export type LegacyTerminalReceipt = typeof LegacyTerminalReceipt.Type;
export const LegacyReconciliationRequest = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_LEGACY_RECONCILIATION_CONTRACT),
    command_id: Address,
    request_sha256: Digest,
    reservation_sha256: Digest,
    expected_server_generation: Version,
    home_environment_id: Id,
    original_byte_witness: Schema.optionalKey(Schema.String),
  }),
);
export type LegacyReconciliationRequest = typeof LegacyReconciliationRequest.Type;
export const LegacyReconciliationBinding = closed(
  Schema.Struct({
    registry_instance_id: Id,
    owner_id: Id,
    principal_id: Id,
    command_id: Address,
    import_run_id: Id,
    snapshot_id: Id,
    snapshot_sha256: Digest,
    receipt_row_sha256: Digest,
    reservation_row_sha256: Digest,
    request_sha256: Digest,
    reservation_sha256: Digest,
    source_identity: NativeReference.fields.identity,
    authority_namespace: Id,
    store_generation: Version,
    home_environment_id: Id,
    home_generation: Version,
  }),
);
export type LegacyReconciliationBinding = typeof LegacyReconciliationBinding.Type;
const SettlementLookupRequest = closed(
  Schema.Struct({
    owner_id: Id,
    principal_id: Id,
    command_id: Address,
    native_reference_id: Id,
    native_action: Schema.Literals(["settle", "unsettle"]),
    request_sha256: Digest,
    reservation_sha256: Digest,
    identity: NativeReference.fields.identity,
    expected_authority_namespace: Id,
    expected_store_generation: Version,
  }),
);
export const LegacySourceTerminal = closed(
  Schema.Struct({
    binding: LegacyReconciliationBinding,
    result: LegacyResult,
    completed_at: Schema.String,
    settlement_request: Schema.NullOr(SettlementLookupRequest),
    native_receipt: Schema.NullOr(
      Schema.Struct({
        commandId: Schema.String,
        aggregateKind: Schema.String,
        aggregateId: Schema.String,
        status: Schema.Literals(["accepted", "rejected"]),
        resultSequence: Sequence,
      }),
    ),
    settlement_event: Schema.NullOr(
      Schema.Struct({
        commandId: Schema.String,
        aggregateKind: Schema.String,
        aggregateId: Schema.String,
        sequence: Sequence,
        type: Schema.String,
      }),
    ),
  }),
);
export type LegacySourceTerminal = typeof LegacySourceTerminal.Type;
const Cell = Schema.Union([Schema.String, Schema.Number, Schema.Null]);
export const LegacyProjectionTarget = closed(
  Schema.Struct({
    table: Schema.Literals([
      "receipts",
      "external_reservations",
      "external_results",
      "references",
      "pr_observations",
      "history",
      "history_scopes",
    ]),
    key: Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Number])),
    data: Schema.Record(Schema.String, Cell),
    context: Schema.NullOr(
      closed(Schema.Struct({ purpose: Id, record_id: Address, revision: Sequence })),
    ),
    value: Schema.Unknown,
  }),
);
export type LegacyProjectionTarget = typeof LegacyProjectionTarget.Type;
export const LegacyReconciliationDelta = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_LEGACY_RECONCILIATION_CONTRACT),
    binding: LegacyReconciliationBinding,
    request_sha256: Digest,
    sequence: Version,
    previous_root: Digest,
    before_registry_version: Sequence,
    after_registry_version: Sequence,
    provenance: closed(
      Schema.Struct({
        kind: Schema.Literals(["retained_terminal_result", "authenticated_source_lookup"]),
        evidence_sha256: Digest,
        result_row_sha256: Schema.NullOr(Digest),
        evidence: Schema.Unknown,
      }),
    ),
    receipt: LegacyTerminalReceipt,
    changes: Schema.Array(
      closed(
        Schema.Struct({
          before: Schema.NullOr(LegacyProjectionTarget),
          after: LegacyProjectionTarget,
        }),
      ),
    ),
  }),
);
export type LegacyReconciliationDelta = typeof LegacyReconciliationDelta.Type;
export const LegacyReconciliationReply = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_LEGACY_RECONCILIATION_CONTRACT),
    result: Schema.Union([
      closed(
        Schema.Struct({
          kind: Schema.Literal("reconciled"),
          proof: LegacyReconciliationDelta,
          delta_root: Digest,
        }),
      ),
      closed(Schema.Struct({ kind: Schema.Literal("unknown"), reason: Schema.String })),
    ]),
  }),
);
export type LegacyReconciliationReply = typeof LegacyReconciliationReply.Type;

export const LegacyReconciliationCapability = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_LEGACY_RECONCILIATION_CONTRACT),
    reconcile_imported_open: Schema.Literal(true),
    lookup_only_source: Schema.Literal(true),
  }),
);
