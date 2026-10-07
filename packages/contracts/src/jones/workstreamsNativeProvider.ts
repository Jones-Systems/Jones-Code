import * as Schema from "effect/Schema";

export const WORKSTREAMS_T3_PROVIDER_FAMILY = "workstreams-t3-provider";
export const WORKSTREAMS_T3_PROVIDER_VERSION = "1.0.0";
export const WORKSTREAMS_T3_PROVIDER_PROTOCOL = "workstreams-t3-provider/1.0.0";
export const WORKSTREAMS_T3_PROVIDER_MANIFEST_SHA256 =
  "08abb3a7753e22e5511ba440a2ac70006a4710a0644f29dd7e3eb1a9b321ff61";
export const WORKSTREAMS_T3_PROVIDER_MAX_REQUEST_BYTES = 32_768;
export const WORKSTREAMS_T3_PROVIDER_MAX_RESPONSE_BYTES = 32_768;
export const WORKSTREAMS_T3_PROVIDER_MAX_JSON_DEPTH = 10;
export const WORKSTREAMS_T3_PROVIDER_METADATA_TIMEOUT_MS = 5_000;
export const WORKSTREAMS_T3_PROVIDER_SETTLEMENT_TIMEOUT_MS = 15_000;
export const WORKSTREAMS_T3_PROVIDER_ATTESTATION_TTL_MS = 300_000;

// Effect Struct discards excess keys; checking its input preserves closed wire records.
const closed = <Fields extends Schema.Struct.Fields>(schema: Schema.Struct<Fields>) =>
  Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter(
        (value) =>
          typeof value === "object" &&
          value !== null &&
          !Array.isArray(value) &&
          Reflect.ownKeys(value).every((key) => Object.hasOwn(schema.fields, key)),
      ),
    ),
  );

const Id = Schema.String.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/),
);
const CommandId = Id.check(Schema.isMinLength(16));
const Sha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const GitObjectId = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
const Version = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
const Sequence = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);
const Timestamp = Schema.String.check(
  Schema.isMaxLength(32),
  Schema.isPattern(
    /^(?:(?!0000)[0-9]{4}-(?:(?:01|03|05|07|08|10|12)-(?:0[1-9]|[12][0-9]|3[01])|(?:04|06|09|11)-(?:0[1-9]|[12][0-9]|30)|02-(?:0[1-9]|1[0-9]|2[0-8]))|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]{1,6})?Z$/,
  ),
);
const Protocol = Schema.Literal(WORKSTREAMS_T3_PROVIDER_PROTOCOL);

export const WorkstreamsNativeIdentity = closed(
  Schema.Struct({
    provider: Schema.Literal("t3"),
    source_instance_id: Id,
    resource_kind: Schema.Literal("thread"),
    id_kind: Schema.Literal("internal"),
    native_id: Id,
    account_provenance: closed(Schema.Struct({ kind: Schema.Literal("not_account_scoped") })),
  }),
);
export type WorkstreamsNativeIdentity = typeof WorkstreamsNativeIdentity.Type;

export const WorkstreamsNativeBuild = closed(
  Schema.Struct({
    repository: Schema.Literal("Jones-Systems/Jones-Code"),
    sha: GitObjectId,
    tree: GitObjectId,
  }),
);
export type WorkstreamsNativeBuild = typeof WorkstreamsNativeBuild.Type;

export const WorkstreamsNativeContext = closed(
  Schema.Struct({
    owner_id: Id,
    principal_id: Id,
    source_instance_id: Id,
    authority_namespace: Id,
    store_generation: Version,
    enrollment_id: Id,
    protocol: Protocol,
    build: WorkstreamsNativeBuild,
  }),
);
export type WorkstreamsNativeContext = typeof WorkstreamsNativeContext.Type;
export const WorkstreamsNativeContextRequest = closed(Schema.Struct({}));
export type WorkstreamsNativeContextRequest = typeof WorkstreamsNativeContextRequest.Type;

const RejectionReason = Schema.Literals([
  "unauthorized",
  "forbidden",
  "caller_mismatch",
  "source_mismatch",
  "unsupported_identity",
  "contract_mismatch",
  "generation_conflict",
  "authority_fenced",
  "thread_not_found",
  "idempotency_conflict",
  "invalid_request",
]);
const Rejected = closed(
  Schema.Struct({ protocol: Protocol, state: Schema.Literal("rejected"), reason: RejectionReason }),
);
export const WorkstreamsNativeContextResponse = Schema.Union([
  closed(
    Schema.Struct({
      protocol: Protocol,
      state: Schema.Literal("ready"),
      context: WorkstreamsNativeContext,
    }),
  ),
  closed(
    Schema.Struct({
      protocol: Protocol,
      state: Schema.Literal("unavailable"),
      reason: Schema.Literals([
        "provider_unconfigured",
        "authority_unavailable",
        "build_unavailable",
      ]),
    }),
  ),
  Rejected,
]);
export type WorkstreamsNativeContextResponse = typeof WorkstreamsNativeContextResponse.Type;

const AttestationRequest = Schema.Struct({
  owner_id: Id,
  principal_id: Id,
  identity: WorkstreamsNativeIdentity,
  expected_authority_namespace: Id,
  expected_store_generation: Version,
});
export const WorkstreamsNativeAttestationRequest = closed(AttestationRequest);
export type WorkstreamsNativeAttestationRequest = typeof WorkstreamsNativeAttestationRequest.Type;
export const WorkstreamsNativeRegistrationAttestation = closed(
  Schema.Struct({
    provider: Schema.Literal("t3"),
    source_instance_id: Id,
    authority_namespace: Id,
    native_id: Id,
    store_generation: Version,
    evidence_sha256: Sha256,
  }),
);
export type WorkstreamsNativeRegistrationAttestation =
  typeof WorkstreamsNativeRegistrationAttestation.Type;
export const WorkstreamsNativeAttestationResponse = Schema.Union([
  closed(
    Schema.Struct({
      protocol: Protocol,
      state: Schema.Literal("attested"),
      request: WorkstreamsNativeAttestationRequest,
      attestation: WorkstreamsNativeRegistrationAttestation,
      attested_at: Timestamp,
      expires_at: Timestamp,
    }),
  ),
  closed(
    Schema.Struct({
      protocol: Protocol,
      state: Schema.Literal("unknown"),
      reason: Schema.Literals([
        "authority_changed",
        "authority_unavailable",
        "observation_unavailable",
      ]),
    }),
  ),
  Rejected,
]);
export type WorkstreamsNativeAttestationResponse = typeof WorkstreamsNativeAttestationResponse.Type;

const Association = Schema.Struct({
  owner_id: Id,
  principal_id: Id,
  command_id: CommandId,
  native_reference_id: Id,
  native_action: Schema.Literals(["settle", "unsettle"]),
  request_sha256: Sha256,
  reservation_sha256: Sha256,
});
export const WorkstreamsNativeAssociation = closed(Association);
export type WorkstreamsNativeAssociation = typeof WorkstreamsNativeAssociation.Type;
export const WorkstreamsNativeSettlementRequest = closed(
  Schema.Struct({
    ...Association.fields,
    identity: WorkstreamsNativeIdentity,
    expected_authority_namespace: Id,
    expected_store_generation: Version,
  }),
);
export type WorkstreamsNativeSettlementRequest = typeof WorkstreamsNativeSettlementRequest.Type;
export const WorkstreamsNativeSettlementLookupRequest = WorkstreamsNativeSettlementRequest;
export type WorkstreamsNativeSettlementLookupRequest =
  typeof WorkstreamsNativeSettlementLookupRequest.Type;

export const WorkstreamsNativeResultEvidence = closed(
  Schema.Struct({
    ...Association.fields,
    result_sha256: Sha256,
  }),
);
export type WorkstreamsNativeResultEvidence = typeof WorkstreamsNativeResultEvidence.Type;
const CommittedResult = closed(
  Schema.Struct({
    native_outcome: Schema.Literal("committed"),
    native_evidence: WorkstreamsNativeResultEvidence,
  }),
);
const NoncommittedResult = closed(
  Schema.Struct({
    native_outcome: Schema.Literals(["denied", "unsupported", "failed"]),
    native_evidence: WorkstreamsNativeResultEvidence,
  }),
);
export const WorkstreamsNativeTerminalResult = Schema.Union([CommittedResult, NoncommittedResult]);
export type WorkstreamsNativeTerminalResult = typeof WorkstreamsNativeTerminalResult.Type;

const Receipt = Schema.Struct({
  commandId: Id,
  aggregateKind: Schema.Literal("thread"),
  aggregateId: Id,
  acceptedAt: Timestamp,
  resultSequence: Sequence,
});
export const WorkstreamsNativeAcceptedReceipt = closed(
  Schema.Struct({ ...Receipt.fields, status: Schema.Literal("accepted") }),
);
export const WorkstreamsNativeRejectedReceipt = closed(
  Schema.Struct({ ...Receipt.fields, status: Schema.Literal("rejected") }),
);
export const WorkstreamsNativeSettlementEvent = closed(
  Schema.Struct({
    eventId: Id,
    commandId: Id,
    aggregateKind: Schema.Literal("thread"),
    aggregateId: Id,
    sequence: Version,
    type: Schema.Literals(["thread.settled", "thread.unsettled"]),
    occurredAt: Timestamp,
  }),
);
export type WorkstreamsNativeSettlementEvent = typeof WorkstreamsNativeSettlementEvent.Type;
const settlementResponseBase = { protocol: Protocol, request: WorkstreamsNativeSettlementRequest };
export const WorkstreamsNativeSettlementResponse = Schema.Union([
  closed(
    Schema.Struct({
      ...settlementResponseBase,
      state: Schema.Literal("terminal"),
      result: CommittedResult,
      native_receipt: WorkstreamsNativeAcceptedReceipt,
      settlement_event: WorkstreamsNativeSettlementEvent,
    }),
  ),
  closed(
    Schema.Struct({
      ...settlementResponseBase,
      state: Schema.Literal("terminal"),
      result: NoncommittedResult,
      native_receipt: Schema.NullOr(WorkstreamsNativeRejectedReceipt),
      settlement_event: Schema.Null,
    }),
  ),
  closed(
    Schema.Struct({
      ...settlementResponseBase,
      state: Schema.Literal("unknown"),
      reason: Schema.Literals([
        "attempt_not_found",
        "dispatch_in_progress",
        "receipt_missing",
        "evidence_conflict",
        "authority_changed",
        "authority_unavailable",
        "provider_unavailable",
      ]),
    }),
  ),
  Rejected,
]);
export type WorkstreamsNativeSettlementResponse = typeof WorkstreamsNativeSettlementResponse.Type;
export const WorkstreamsNativeSettlementLookupResponse = WorkstreamsNativeSettlementResponse;
export type WorkstreamsNativeSettlementLookupResponse =
  typeof WorkstreamsNativeSettlementLookupResponse.Type;

export const WorkstreamsNativeProviderRequest = Schema.Union([
  closed(
    Schema.Struct({
      operation: Schema.Literal("context"),
      request: WorkstreamsNativeContextRequest,
    }),
  ),
  closed(
    Schema.Struct({
      operation: Schema.Literal("attestations"),
      request: WorkstreamsNativeAttestationRequest,
    }),
  ),
  closed(
    Schema.Struct({
      operation: Schema.Literal("settlements"),
      request: WorkstreamsNativeSettlementRequest,
    }),
  ),
  closed(
    Schema.Struct({
      operation: Schema.Literal("settlements/lookup"),
      request: WorkstreamsNativeSettlementLookupRequest,
    }),
  ),
]);
export type WorkstreamsNativeProviderRequest = typeof WorkstreamsNativeProviderRequest.Type;
export const WorkstreamsNativeProviderResponse = Schema.Union([
  closed(
    Schema.Struct({
      operation: Schema.Literal("context"),
      response: WorkstreamsNativeContextResponse,
    }),
  ),
  closed(
    Schema.Struct({
      operation: Schema.Literal("attestations"),
      response: WorkstreamsNativeAttestationResponse,
    }),
  ),
  closed(
    Schema.Struct({
      operation: Schema.Literal("settlements"),
      response: WorkstreamsNativeSettlementResponse,
    }),
  ),
  closed(
    Schema.Struct({
      operation: Schema.Literal("settlements/lookup"),
      response: WorkstreamsNativeSettlementLookupResponse,
    }),
  ),
]);
export type WorkstreamsNativeProviderResponse = typeof WorkstreamsNativeProviderResponse.Type;
