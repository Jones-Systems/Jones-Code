import * as Schema from "effect/Schema";
import {
  WorkstreamAction,
  WorkstreamCommand,
  WorkstreamReadContext,
  Workstream,
  WorkstreamOperation,
  WorkstreamError,
} from "../workstreams.ts";

export const WORKSTREAM_OPERATIONS_FAMILY = "workstream-operations";
export const WORKSTREAM_OPERATIONS_VERSION = "1.0.0";
export const WORKSTREAM_OPERATIONS_MANIFEST_SHA256 =
  "c53f6e05db9f4cbd862097b772abb5bdc7a6c20e89b49e271dc36cc7ecd5eaa2";
export const WORKSTREAM_OPERATIONS_CONTRACT = "workstream-operations/1.0.0";

// Validate wire keys before Effect can discard excess properties during decoding.
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

const Id = WorkstreamReadContext.fields.owner_id;
const CommandId = WorkstreamCommand.fields.command_id;
const CreateAction = WorkstreamAction.members[0];
const Progress = Schema.Union([
  closed(CreateAction.fields.progress.members[0]),
  closed(CreateAction.fields.progress.members[1]),
  closed(CreateAction.fields.progress.members[2]),
]);

export const WorkstreamOperationThread = closed(
  Schema.Struct({ environment_id: Id, thread_id: Id }),
);
export type WorkstreamOperationThread = typeof WorkstreamOperationThread.Type;

export const WorkstreamOperationOrigin = Schema.Union([
  closed(Schema.Struct({ kind: Schema.Literals(["app", "cli", "agent"]) })),
  closed(Schema.Struct({ kind: Schema.Literal("queue"), request_id: Id })),
]);
export type WorkstreamOperationOrigin = typeof WorkstreamOperationOrigin.Type;

const Registry = closed(Schema.Struct({ registry_id: Id, owner_id: Id }));
export const WorkstreamOperationTarget = Schema.Union([
  closed(
    Schema.Struct({ kind: Schema.Literal("workstream"), ...Registry.fields, workstream_id: Id }),
  ),
  closed(Schema.Struct({ kind: Schema.Literal("unassigned"), ...Registry.fields })),
]);
export const WorkstreamOperationExpected = Schema.Union([
  closed(Schema.Struct({ kind: Schema.Literals(["any", "unassigned"]) })),
  closed(Schema.Struct({ kind: Schema.Literal("primary"), workstream_id: Id })),
]);

const identity = {
  contract: Schema.Literal(WORKSTREAM_OPERATIONS_CONTRACT),
  operation_id: CommandId,
  origin: WorkstreamOperationOrigin,
};

export const WorkstreamSetPrimaryRequest = closed(
  Schema.Struct({
    ...identity,
    kind: Schema.Literal("set_primary"),
    thread: WorkstreamOperationThread,
    target: WorkstreamOperationTarget,
    expected: WorkstreamOperationExpected,
  }),
).check(
  Schema.makeFilter(
    (request) => request.origin.kind !== "queue" || request.expected.kind === "unassigned",
  ),
);
export type WorkstreamSetPrimaryRequest = typeof WorkstreamSetPrimaryRequest.Type;

export const WorkstreamCreateRequest = closed(
  Schema.Struct({
    ...identity,
    kind: Schema.Literal("create_workstream"),
    registry: Registry,
    workstream: closed(
      Schema.Struct({
        name: CreateAction.fields.name,
        lifecycle: CreateAction.fields.lifecycle,
        progress: Progress,
        sort_order: CreateAction.fields.sort_order,
      }),
    ),
  }),
);
export type WorkstreamCreateRequest = typeof WorkstreamCreateRequest.Type;

export const WorkstreamOperationRequest = Schema.Union([
  WorkstreamSetPrimaryRequest,
  WorkstreamCreateRequest,
]);
export type WorkstreamOperationRequest = typeof WorkstreamOperationRequest.Type;
export const parseWorkstreamOperationRequest = Schema.decodeUnknownSync(WorkstreamOperationRequest);

export const WorkstreamOperationSubmitRequest = closed(
  Schema.Struct({
    request: WorkstreamOperationRequest,
    wait_ms: Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum: 0, maximum: 3000 })),
  }),
);
export type WorkstreamOperationSubmitRequest = typeof WorkstreamOperationSubmitRequest.Type;

export const WorkstreamOperationReadRequest = Schema.Union([
  closed(Schema.Struct({ operation_id: CommandId })),
  closed(Schema.Struct({ thread: WorkstreamOperationThread })),
  closed(Schema.Struct({ open: Schema.Literal(true) })),
]);
export type WorkstreamOperationReadRequest = typeof WorkstreamOperationReadRequest.Type;

export const EnvironmentWorkstreamOperationConflictError = closed(
  Schema.Struct({
    code: Schema.Literals(["operation_id_conflict", "thread_operation_in_progress"]),
    open_operation_id: Schema.optional(CommandId),
  }),
);
export type EnvironmentWorkstreamOperationConflictError =
  typeof EnvironmentWorkstreamOperationConflictError.Type;

const Timestamp = Workstream.fields.created_at;
const RegistryVersion = WorkstreamReadContext.fields.registry_version;
// UTC wire timestamps permit six fractional digits; Date.parse loses microseconds.
const timestampOrderKey = (value: string) => {
  const [seconds, fraction = ""] = value.slice(0, -1).split(".");
  return `${seconds}.${fraction.padEnd(6, "0")}`;
};
const PollAfter = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 1, maximum: 60_000 }),
);
export const WorkstreamOperationAvailability = Schema.Literals([
  "descriptor_absent",
  "build_unavailable",
  "contract_mismatch",
  "source_mismatch",
  "authority_mismatch",
  "generation_mismatch",
  "authority_fenced",
  "authority_unavailable",
]);
export const WorkstreamOperationUnavailableReason = Schema.Literals([
  "not_configured",
  "offline",
  "registration_unavailable",
  "placements_not_ready",
  "contention",
  "backpressure",
  "verification_pending",
  "verification_unresolved",
  "destination_scan_bound",
]);
export const WorkstreamOperationObserved = Schema.Union([
  closed(Schema.Struct({ kind: Schema.Literal("unassigned") })),
  closed(
    Schema.Struct({
      kind: Schema.Literal("primary"),
      workstream_id: Id,
      name: Workstream.fields.name,
    }),
  ),
]);
export const WorkstreamOperationChild = closed(
  Schema.Struct({
    command_id: CommandId,
    operation: WorkstreamOperation,
    outcome: Schema.Literals(["pending", "committed", "rejected", "unknown"]),
  }),
);
const resultIdentity = {
  ...identity,
  kind: Schema.Literals(["set_primary", "create_workstream"]),
  request_sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}(?![\s\S])/)),
  observed: Schema.NullOr(WorkstreamOperationObserved),
  children: Schema.Array(WorkstreamOperationChild).check(
    Schema.isMaxLength(16),
    Schema.makeFilter(
      (children) => new Set(children.map((child) => child.command_id)).size === children.length,
    ),
  ),
  accepted_at: Timestamp,
  updated_at: Timestamp,
};
const emptyOutcome = { result: Schema.Null, poll_after_ms: Schema.Null, detail: Schema.Null };
const ResultValue = closed(
  Schema.Struct({
    workstream_id: Schema.NullOr(Id),
    membership_id: Schema.NullOr(Id),
    registry_version: RegistryVersion,
  }),
);
export const WorkstreamOperationResult = Schema.Union([
  closed(
    Schema.Struct({
      ...resultIdentity,
      ...emptyOutcome,
      state: Schema.Literal("in_progress"),
      reason: Schema.Null,
      poll_after_ms: PollAfter,
    }),
  ),
  closed(
    Schema.Struct({
      ...resultIdentity,
      ...emptyOutcome,
      state: Schema.Literal("reconciling"),
      reason: Schema.Literals([
        "membership_effect_unknown",
        "create_effect_unknown",
        "verification_pending",
      ]),
      poll_after_ms: PollAfter,
    }),
  ),
  closed(
    Schema.Struct({
      ...resultIdentity,
      ...emptyOutcome,
      state: Schema.Literal("committed"),
      reason: Schema.Null,
      result: ResultValue,
    }),
  ),
  closed(
    Schema.Struct({
      ...resultIdentity,
      ...emptyOutcome,
      kind: Schema.Literal("set_primary"),
      state: Schema.Literal("already_satisfied"),
      reason: Schema.Null,
      observed: WorkstreamOperationObserved,
    }),
  ),
  closed(
    Schema.Struct({
      ...resultIdentity,
      ...emptyOutcome,
      kind: Schema.Literal("set_primary"),
      state: Schema.Literal("conflict"),
      reason: Schema.Literal("precondition_failed"),
      observed: WorkstreamOperationObserved,
    }),
  ),
  closed(
    Schema.Struct({
      ...resultIdentity,
      ...emptyOutcome,
      state: Schema.Literal("rejected"),
      reason: Schema.Literals([
        "foreign_thread",
        "thread_not_found",
        "registry_mismatch",
        "permission_denied",
        "workstream_not_found",
        "identity_conflict",
        "verification_rejected",
      ]),
    }),
  ),
  closed(
    Schema.Struct({
      ...resultIdentity,
      ...emptyOutcome,
      state: Schema.Literal("rejected"),
      reason: Schema.Literal("registry_rejected"),
      detail: closed(Schema.Struct({ code: WorkstreamError.fields.code })),
    }),
  ),
  closed(
    Schema.Struct({
      ...resultIdentity,
      ...emptyOutcome,
      state: Schema.Literal("unavailable"),
      reason: Schema.Literals([
        "not_configured",
        "offline",
        "placements_not_ready",
        "contention",
        "backpressure",
        "verification_pending",
        "verification_unresolved",
        "destination_scan_bound",
      ]),
    }),
  ),
  closed(
    Schema.Struct({
      ...resultIdentity,
      ...emptyOutcome,
      state: Schema.Literal("unavailable"),
      reason: Schema.Literal("registration_unavailable"),
      detail: closed(Schema.Struct({ availability: WorkstreamOperationAvailability })),
    }),
  ),
]).check(
  Schema.makeFilter((value) => {
    if (timestampOrderKey(value.updated_at) < timestampOrderKey(value.accepted_at)) return false;
    if (value.kind === "create_workstream" && value.observed !== null) return false;
    if (value.state === "reconciling") {
      if (value.reason === "create_effect_unknown" && value.kind !== "create_workstream")
        return false;
      if (value.reason !== "create_effect_unknown" && value.kind !== "set_primary") return false;
    }
    if (value.state !== "committed") return true;
    return value.kind === "create_workstream"
      ? value.result.workstream_id !== null && value.result.membership_id === null
      : value.result.membership_id !== null;
  }),
);
export type WorkstreamOperationResult = typeof WorkstreamOperationResult.Type;
export const parseWorkstreamOperationResult = Schema.decodeUnknownSync(WorkstreamOperationResult);
export const WorkstreamOperationSubmitResponse = WorkstreamOperationResult;
export type WorkstreamOperationSubmitResponse = WorkstreamOperationResult;
export const WorkstreamOpenOperation = WorkstreamOperationResult.check(
  Schema.makeFilter((value) => value.state === "in_progress" || value.state === "reconciling"),
);
export const WorkstreamOperationAssignment = Schema.Union([
  WorkstreamOperationObserved,
  closed(Schema.Struct({ kind: Schema.Literal("unverified") })),
  closed(
    Schema.Struct({
      kind: Schema.Literal("unavailable"),
      reason: WorkstreamOperationUnavailableReason,
    }),
  ),
]);
export type WorkstreamOperationAssignment = typeof WorkstreamOperationAssignment.Type;
export const WorkstreamOperationReadResponse = Schema.Union([
  closed(Schema.Struct({ operation: Schema.NullOr(WorkstreamOperationResult) })),
  closed(
    Schema.Struct({
      assignment: WorkstreamOperationAssignment,
      open_operation: Schema.NullOr(
        WorkstreamOpenOperation.check(
          Schema.makeFilter((operation) => operation.kind === "set_primary"),
        ),
      ),
    }),
  ),
  closed(
    Schema.Struct({
      operations: Schema.Array(WorkstreamOpenOperation).check(
        Schema.isMaxLength(100),
        Schema.makeFilter(
          (operations) =>
            new Set(operations.map((operation) => operation.operation_id)).size ===
            operations.length,
        ),
      ),
    }),
  ),
]);
export type WorkstreamOperationReadResponse = typeof WorkstreamOperationReadResponse.Type;
export const WorkstreamSetupActivationKey = Schema.Literals([
  "T3_WORKSTREAM_CONTROL_PLANE_URL",
  "T3_WORKSTREAM_OWNER_ID",
  "T3_WORKSTREAM_PRINCIPAL_ID",
  "T3_WORKSTREAM_KEY_ID",
  "T3_WORKSTREAM_SIGNING_SECRET",
  "T3_WORKSTREAM_AUTHORIZATION_REVISION",
  "T3_WORKSTREAM_REGISTRY_BINDING",
  "T3_WORKSTREAM_REGISTRATION_CONTEXT_VERSION",
]);
const SetupRegistry = Schema.Union([
  closed(
    Schema.Struct({
      state: Schema.Literal("ready"),
      registry_id: Id,
      owner_id: Id,
      can_write: Schema.Boolean,
    }),
  ),
  closed(
    Schema.Struct({
      state: Schema.Literals(["not_configured", "unavailable"]),
      registry_id: Schema.NullOr(Id),
      owner_id: Schema.NullOr(Id),
      can_write: Schema.Literal(false),
    }),
  ),
]);
const SetupNextStep = Schema.Union([
  closed(
    Schema.Struct({
      code: Schema.Literal("configure_activation"),
      doc: Schema.Literal("docs/operations/workstreams-activation.md"),
    }),
  ),
  closed(
    Schema.Struct({
      code: Schema.Literal("enroll_native_source"),
      doc: Schema.Literal("docs/operations/workstreams-native-enrollment.md"),
    }),
  ),
  closed(
    Schema.Struct({
      code: Schema.Literal("verify_reference"),
      doc: Schema.Literal("docs/operations/workstreams-verification.md"),
    }),
  ),
]);
export const WorkstreamSetupStatus = closed(
  Schema.Struct({
    contract: Schema.Literal(WORKSTREAM_OPERATIONS_CONTRACT),
    environment_id: Id,
    observed_at: Timestamp,
    authority_effect: Schema.Literal("none"),
    activation: closed(
      Schema.Struct({
        state: Schema.Literals(["enabled", "disabled", "invalid"]),
        missing_keys: Schema.Array(WorkstreamSetupActivationKey).check(
          Schema.isMaxLength(8),
          Schema.makeFilter((keys) => new Set(keys).size === keys.length),
        ),
      }),
    ),
    registry: SetupRegistry,
    t3_source: Schema.Union([
      closed(Schema.Struct({ state: Schema.Literal("ready"), reason: Schema.Null })),
      closed(
        Schema.Struct({
          state: Schema.Literal("unavailable"),
          reason: WorkstreamOperationAvailability,
        }),
      ),
    ]),
    placements: closed(
      Schema.Struct({
        readiness: Schema.Literals(["ready", "trust-provider-required", "unavailable"]),
      }),
    ),
    operations: closed(
      Schema.Struct({
        open: Schema.Number.check(
          Schema.isInt(),
          Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
        ),
      }),
    ),
    next_steps: Schema.Array(SetupNextStep).check(
      Schema.isMaxLength(3),
      Schema.makeFilter((steps) => new Set(steps.map((step) => step.code)).size === steps.length),
    ),
  }),
).check(
  Schema.makeFilter(
    (value) => value.activation.state !== "enabled" || value.activation.missing_keys.length === 0,
  ),
);
export type WorkstreamSetupStatus = typeof WorkstreamSetupStatus.Type;
export const parseWorkstreamSetupStatus = Schema.decodeUnknownSync(WorkstreamSetupStatus);

export const parseWorkstreamOperationSubmitRequest = Schema.decodeUnknownSync(
  WorkstreamOperationSubmitRequest,
);
export const parseWorkstreamOperationReadRequest = Schema.decodeUnknownSync(
  WorkstreamOperationReadRequest,
);
export const parseWorkstreamOperationReadResponse = Schema.decodeUnknownSync(
  WorkstreamOperationReadResponse,
);
