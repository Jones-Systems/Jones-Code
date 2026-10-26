import * as Schema from "effect/Schema";
import {
  Workstream,
  WorkstreamCommand,
  WorkstreamReceipt,
  WorkstreamReadContext,
  WorkstreamCapabilities,
  WorkstreamNativeIdentity,
} from "../workstreams.ts";
import { WorkstreamOperationOrigin, WorkstreamOperationRequest } from "./workstreamOperations.ts";
import { WorkstreamsNativeAttestationResponseV2 } from "./workstreamsNativeProvider.ts";

export const WORKSTREAMS_NATIVE_CONTRACT = "workstreams-native/1.0.0";
export const WORKSTREAMS_NATIVE_READ_CONTRACT = "workstreams-read/1.0.0";
export const WORKSTREAMS_NATIVE_PLACEMENT_CONTRACT = "workstreams-t3-placement/2.0.0";
export const WORKSTREAMS_ROLLUPS_CONTRACT = "workstreams-rollups/1.0.0";

// Validate the original wire keys before Effect's Struct decoder can strip them.
export const closedWorkstreamsNative = <Fields extends Schema.Struct.Fields>(
  schema: Schema.Struct<Fields>,
) =>
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

export const WorkstreamsNativeId = WorkstreamReadContext.fields.owner_id;
export const WorkstreamsNativeSequence = WorkstreamReadContext.fields.registry_version;
export const WorkstreamsNativeVersion = WorkstreamReadContext.fields.server_generation;
export const WorkstreamsNativeTimestamp = Workstream.fields.created_at;
export const WorkstreamsNativeCommandId = WorkstreamCommand.fields.command_id;
export const WorkstreamsNativeDigest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const WorkstreamsNativeUuid = Schema.String.check(
  Schema.isPattern(/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/),
);
export const WorkstreamsNativeTextId = WorkstreamNativeIdentity.fields.native_id;
export const WorkstreamsNativeOrigin = Schema.String.check(
  Schema.isMaxLength(512),
  Schema.makeFilter((value) => {
    try {
      const url = new URL(value);
      return url.protocol === "https:" && url.origin === value;
    } catch {
      return false;
    }
  }),
);
const closed = closedWorkstreamsNative;
const Id = WorkstreamsNativeId;
const Version = WorkstreamsNativeVersion;
const Sequence = WorkstreamsNativeSequence;

const registryFields = {
  registry_instance_id: WorkstreamsNativeUuid,
  owner_id: Id,
  canonical_origin: WorkstreamsNativeOrigin,
  home_environment_id: Id,
  server_generation: Version,
};
export const WorkstreamsNativeRegistry = closed(Schema.Struct(registryFields));
export type WorkstreamsNativeRegistry = typeof WorkstreamsNativeRegistry.Type;
export const WorkstreamsNativeRegistryContext = closed(
  Schema.Struct({ ...registryFields, registry_version: Sequence }),
);
export type WorkstreamsNativeRegistryContext = typeof WorkstreamsNativeRegistryContext.Type;
export const WorkstreamsNativeHomeTarget = closed(
  Schema.Struct({
    registry_instance_id: WorkstreamsNativeUuid,
    owner_id: Id,
    home_environment_id: Id,
    server_generation: Version,
  }),
);
export type WorkstreamsNativeHomeTarget = typeof WorkstreamsNativeHomeTarget.Type;

const capabilityFields = {
  contract: Schema.Literal(WORKSTREAMS_NATIVE_CONTRACT),
  reads: Schema.Array(Schema.Literal(WORKSTREAMS_NATIVE_READ_CONTRACT)).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(1),
  ),
  placement_proofs: Schema.Array(
    Schema.Literals(["workstreams-t3-placement/1.0.0", WORKSTREAMS_NATIVE_PLACEMENT_CONTRACT]),
  ).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(2),
    Schema.makeFilter((xs) => new Set(xs).size === xs.length),
  ),
  rollups: Schema.optionalKey(
    closed(
      Schema.Struct({
        contract: Schema.Literal(WORKSTREAMS_ROLLUPS_CONTRACT),
        producer: Schema.Literals(["configured", "unconfigured"]),
      }),
    ),
  ),
};
export const WorkstreamsNativeCapability = Schema.Union([
  closed(
    Schema.Struct({
      ...capabilityFields,
      mode: Schema.Literal("native-home"),
      registry: WorkstreamsNativeRegistry,
    }),
  ),
  closed(
    Schema.Struct({
      ...capabilityFields,
      mode: Schema.Literal("native-member"),
      registry: WorkstreamsNativeRegistry,
      home_link: Schema.Literals(["ok", "missing", "expired", "unknown"]),
    }),
  ),
  closed(
    Schema.Struct({
      ...capabilityFields,
      mode: Schema.Literals(["legacy", "transition-fenced", "disabled"]),
      registry: Schema.NullOr(WorkstreamsNativeRegistry),
    }),
  ),
]);
export type WorkstreamsNativeCapability = typeof WorkstreamsNativeCapability.Type;

export const WorkstreamsNativeReceiptLookup = Schema.Union([
  closed(
    Schema.Struct({
      kind: Schema.Literal("present"),
      source: Schema.Literals(["native", "legacy_archive", "legacy_open"]),
      receipt: WorkstreamReceipt,
    }),
  ),
  closed(
    Schema.Struct({
      kind: Schema.Literal("proven_absent"),
      witness: closed(
        Schema.Struct({
          server_generation: Version,
          registry_version: Sequence,
          archive: Schema.Literal("complete"),
        }),
      ),
    }),
  ),
  closed(
    Schema.Struct({
      kind: Schema.Literal("unknown"),
      reason: Schema.Literals([
        "import_incomplete",
        "archive_class_missing",
        "key_unavailable",
        "generation_mismatch",
        "home_unreachable",
      ]),
    }),
  ),
]);
export type WorkstreamsNativeReceiptLookup = typeof WorkstreamsNativeReceiptLookup.Type;
export const WorkstreamsNativeRefusal = Schema.Union([
  closed(Schema.Struct({ code: Schema.Literal("home_route_required"), home_environment_id: Id })),
  closed(
    Schema.Struct({
      code: Schema.Literals([
        "generation_mismatch",
        "registry_mismatch",
        "not_granted",
        "hop_limit",
        "foreign_thread_claim",
        "key_unavailable",
        "fenced",
        "import_incomplete",
      ]),
    }),
  ),
]);
export type WorkstreamsNativeRefusal = typeof WorkstreamsNativeRefusal.Type;
export const WorkstreamsNativeRelay = closed(
  Schema.Struct({
    source_environment_id: Id,
    origin: WorkstreamOperationOrigin,
    linked_origin_environment_id: Schema.NullOr(Id),
    hop: Schema.Literal(1),
  }),
);
export type WorkstreamsNativeRelay = typeof WorkstreamsNativeRelay.Type;
export const WorkstreamsNativeSuppliedAttestation =
  WorkstreamsNativeAttestationResponseV2.members[0];
const decodeCommand = Schema.decodeUnknownSync(WorkstreamCommand);
// Keep the exact command bytes for digest verification and durable reconciliation.
const CommandBody = Schema.String.check(
  Schema.makeFilter((body) => {
    if (new TextEncoder().encode(body).length > 1_048_576) return false;
    try {
      decodeCommand(JSON.parse(body));
      return true;
    } catch {
      return false;
    }
  }),
);
const relayFields = { expected_home: WorkstreamsNativeHomeTarget, relay: WorkstreamsNativeRelay };
export const WorkstreamsNativeHomeCommand = closed(
  Schema.Struct({
    ...relayFields,
    body: CommandBody,
    body_sha256: WorkstreamsNativeDigest,
    evidence: Schema.optionalKey(WorkstreamsNativeSuppliedAttestation),
  }),
);
export type WorkstreamsNativeHomeCommand = typeof WorkstreamsNativeHomeCommand.Type;
export const WorkstreamsNativeHomeOperation = closed(
  Schema.Struct({
    ...relayFields,
    request: WorkstreamOperationRequest,
    request_sha256: WorkstreamsNativeDigest,
    evidence: Schema.optionalKey(WorkstreamsNativeSuppliedAttestation),
  }),
);
export type WorkstreamsNativeHomeOperation = typeof WorkstreamsNativeHomeOperation.Type;
export const WorkstreamsNativeHomeReceiptRequest = closed(
  Schema.Struct({
    command_ids: Schema.Array(WorkstreamsNativeCommandId).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(16),
      Schema.makeFilter((xs) => new Set(xs).size === xs.length),
    ),
  }),
);
export const WorkstreamsNativeHomeBindRequest = closed(
  Schema.Struct({
    source: closed(
      Schema.Struct({
        environment_id: Id,
        authority_namespace: WorkstreamsNativeTextId,
        store_generation: Version,
      }),
    ),
  }),
);
export const WorkstreamsNativeHomeBindResult = closed(
  Schema.Struct({ principal_id: Id, binding_version: Version }),
);
export const WorkstreamsNativeHomeContext = closed(
  Schema.Struct({
    capabilities: WorkstreamCapabilities,
    principal_id: Id,
    grant_version: Version,
    home_environment_id: Id,
  }),
);
export const WorkstreamsNativeOperationState = Schema.Literals([
  "accepted",
  "dispatching",
  "in_progress",
  "reconciling",
  "committed",
  "already_satisfied",
  "conflict",
  "rejected",
  "unavailable",
]);
export const WorkstreamsNativeImportState = Schema.Literals([
  "planned",
  "applying",
  "applied",
  "verified",
  "activated",
  "conflict",
  "failed",
  "abandoned",
]);
export const WorkstreamsEncryptedEnvelope = closed(
  Schema.Struct({
    v: Schema.Literal(1),
    alg: Schema.Literal("A256GCM"),
    key_id: Id,
    nonce: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9+/]{16}$/)),
    ct: Schema.String.check(
      Schema.isMaxLength(16_777_216),
      Schema.isPattern(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/),
    ),
    tag: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9+/]{22}==$/)),
  }),
);
export type WorkstreamsEncryptedEnvelope = typeof WorkstreamsEncryptedEnvelope.Type;
