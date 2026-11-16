import * as Schema from "effect/Schema";

const token = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:/@+%=-]{0,127}$/));
const sha = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const oid = Schema.String.check(Schema.isPattern(/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/));
const integer = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);
const positive = integer.check(Schema.isGreaterThan(0));
const versioned = Schema.Struct({ name: token, version: token });

export const CheckpointCompatibility = Schema.Struct({
  platform: Schema.Struct({ os: token, architecture: token, abi: token }),
  toolchain: Schema.Array(versioned).check(Schema.isMinLength(1), Schema.isMaxLength(32)),
  package_manager: versioned,
});

export const CheckpointSource = Schema.Struct({
  schema: Schema.Literal("fleet-ops.checkpoint-source/v1"),
  repository: Schema.Literal("https://github.com/Jones-Systems/host-tooling"),
  commit: oid,
  tree: oid,
  manifest_sha256: sha,
  lockset_sha256: sha,
  submodule_sha256: sha,
  lfs_sha256: sha,
});

const submission = Schema.Struct({
  schema: Schema.Literal("fleet-ops.submission-binding/v1"),
  submission_id: token,
  submission_digest: sha,
  target_id: token,
  target_binding_revision: token,
  admission_epoch: token,
});

export const CheckpointWorkKey = Schema.Struct({
  schema: Schema.Literal("fleet-ops.checkpoint-work-key/v1"),
  controller_id: token,
  admission_epoch: token,
  work_id: token,
});
export type CheckpointWorkKey = typeof CheckpointWorkKey.Type;

/** The native service supplies work_id from the authenticated environment, thread and originalRequestId. */
export const FleetCheckpointInput = Schema.Struct({
  originalRequestId: token,
  request: Schema.Struct({
    schema: Schema.Literal("fleet-ops.checkpoint-placement-request/v1"),
    work_key: Schema.Struct({
      schema: Schema.Literal("fleet-ops.checkpoint-work-key/v1"),
      controller_id: token,
      admission_epoch: token,
    }),
    source: CheckpointSource,
    compatibility: CheckpointCompatibility,
    recipe: Schema.Struct({
      schema: Schema.Literal("fleet-ops.checkpoint-recipe/v1"),
      recipe_id: Schema.Literal("host-tooling.catalog"),
      version: Schema.Literal("1"),
      recipe_digest: sha,
    }),
    registry_revision: token,
    policy_ref: token,
    coverage_ref: token,
    placement_policy_digest: sha,
    demand: Schema.Struct({
      schema: Schema.Literal("fleet-ops.checkpoint-demand/v1"),
      cpu_units: positive.check(Schema.isLessThanOrEqualTo(4096)),
      ram_bytes: positive,
      output_peak_bytes: positive,
      claims: Schema.Array(
        Schema.Struct({
          schema: Schema.Literal("fleet-ops.checkpoint-claim/v1"),
          key: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:/@+%=-]{0,255}$/)),
          mode: Schema.Literals(["exclusive", "shared-read"]),
        }),
      ).check(Schema.isMaxLength(64)),
      artifact_max_bytes: positive.check(Schema.isLessThanOrEqualTo(16777216)),
    }),
    earliest_start: Schema.String,
    latest_start: Schema.String,
    acceptance_deadline: Schema.String,
    prior_costs: Schema.Array(
      Schema.Struct({
        schema: Schema.Literal("fleet-ops.checkpoint-prior-cost/v1"),
        submission,
        receipt_digest: sha,
        cost_digest: sha,
      }),
    ).check(Schema.isMaxLength(4)),
  }),
  /** Caller wait budget only; it never changes execution deadlines or cancels an accepted job. */
  timeoutMs: Schema.optional(integer.check(Schema.isBetween({ minimum: 100, maximum: 30000 }))),
});
export type FleetCheckpointInput = typeof FleetCheckpointInput.Type;

const document = Schema.Record(Schema.String, Schema.Unknown);
export const FleetCheckpointResult = Schema.Struct({
  schema: Schema.Literal("jones.fleet-checkpoint-result/v1"),
  environmentId: Schema.String,
  threadId: Schema.String,
  originalRequestId: token,
  workKey: CheckpointWorkKey,
  expectedRequestDigest: sha,
  transport: Schema.Literals(["observed", "unknown"]),
  bindingValidation: Schema.Literals(["bound", "unavailable"]),
  verificationOutcome: Schema.Literals(["passed", "failed", "pending", "unknown"]),
  resourceClosure: Schema.Literal("unknown"),
  fleet: Schema.NullOr(document),
  cancellation: Schema.NullOr(document),
  cost: Schema.NullOr(document),
  artifacts: Schema.Array(document).check(Schema.isMaxLength(64)),
});
export type FleetCheckpointResult = typeof FleetCheckpointResult.Type;
