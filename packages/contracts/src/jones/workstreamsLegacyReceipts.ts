import * as Schema from "effect/Schema";
import {
  closedWorkstreamsNative as closed,
  WorkstreamsNativeDigest as Digest,
  WorkstreamsNativeId as Id,
  WorkstreamsNativeVersion as Version,
  WorkstreamsNativeSequence as Sequence,
  WorkstreamsNativeReceiptLookup,
} from "./workstreamsNative.ts";

export const WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT = "workstreams-legacy-receipts/1.0.0";

// Historical snapshot identifiers are evidence addresses, never command admission.
export const WorkstreamsLegacyReceiptId = Schema.String;
export const WorkstreamsLegacyReceiptIdentity = Schema.Struct({
  command_id: WorkstreamsLegacyReceiptId,
  owner_id: Id,
  actor: Schema.Struct({ principal_id: Id }),
  operation: Schema.String,
  request_sha256: Digest,
  server_generation: Version,
  state: Schema.Literals(["committed", "rejected", "pending", "unresolved"]),
  registry_version: Schema.optionalKey(Sequence),
  accepted_at: Schema.String,
});
export type WorkstreamsLegacyReceiptIdentity = typeof WorkstreamsLegacyReceiptIdentity.Type;

export const WorkstreamsLegacyReceiptProof = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT),
    command_id: WorkstreamsLegacyReceiptId,
    owner_id: Id,
    principal_id: Id,
    operation: Schema.String,
    state: WorkstreamsLegacyReceiptIdentity.fields.state,
    request_sha256: Digest,
    server_generation: Version,
    registry_version: Schema.NullOr(Sequence),
    payload: Schema.String,
    request_bytes: Schema.Literal("unknown"),
    provenance: closed(
      Schema.Struct({
        registry_instance_id: Id,
        import_run_id: Id,
        snapshot_id: Id,
        archive_version: Schema.Literal(1),
        record_key: Schema.String,
        canonical_row_sha256: Digest,
        ciphertext_sha256: Digest,
      }),
    ),
  }),
);
export type WorkstreamsLegacyReceiptProof = typeof WorkstreamsLegacyReceiptProof.Type;

export const WorkstreamsLegacyReceiptLookupRequest = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT),
    command_id: WorkstreamsLegacyReceiptId,
    expected_server_generation: Version,
    request_sha256: Schema.optionalKey(Digest),
  }),
);
export type WorkstreamsLegacyReceiptLookupRequest =
  typeof WorkstreamsLegacyReceiptLookupRequest.Type;

// Advertise separately; the closed native v1 capability and lookup remain unchanged.
export const WorkstreamsLegacyReceiptCapability = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT),
    lookup: Schema.Literal(true),
  }),
);
export const WorkstreamsLegacyReceiptLookupResult = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAMS_LEGACY_RECEIPTS_CONTRACT),
    result: Schema.Union([
      closed(
        Schema.Struct({
          kind: Schema.Literal("legacy_present"),
          source: Schema.Literals(["legacy_archive", "legacy_open"]),
          proof: WorkstreamsLegacyReceiptProof,
        }),
      ),
      WorkstreamsNativeReceiptLookup,
    ]),
  }),
);
export type WorkstreamsLegacyReceiptLookupResult = typeof WorkstreamsLegacyReceiptLookupResult.Type;
