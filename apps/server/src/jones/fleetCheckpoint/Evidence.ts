import type {
  FleetCheckpointInput,
  FleetCheckpointResult,
  CheckpointWorkKey,
} from "@t3tools/contracts/jones/fleetCheckpoint";
import { canonical, digest, document, exact, type Document } from "./Json.ts";

export const recipe = {
  id: "host-tooling.catalog",
  version: "1",
  checks: [{ id: "catalog", argv: ["python3", "tests/run_checks.py"] }],
  lockfiles: ["src/host_tooling/source_verification/dependencies.toml"],
};
export const placementPolicy = [
  "checkpoint-placement/v1",
  "test:linux-primary",
  "vps:linux-overflow-control",
  "mini:apple-platform",
  "laptop:explicit-policy-and-coverage-required",
  "fleet-checkpoint.laptop-heavy-explicit",
  "fleet-checkpoint.laptop-heavy-reviewed",
];
const check: (value: boolean) => asserts value = (value) => {
  if (!value) throw new TypeError("Checkpoint evidence mismatch.");
};
const equal = (a: unknown, b: unknown) => check(canonical(a) === canonical(b));
const sha = (value: unknown) => {
  check(typeof value === "string" && /^[0-9a-f]{64}$/.test(value));
};
const token = (value: unknown) => {
  check(typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:/@+%=-]{0,255}$/.test(value));
};
const fields = (value: unknown, names: string) => exact(value, names.split(" "));
const binding = (value: unknown): Document => {
  const v = fields(
    value,
    "schema submission_id submission_digest target_id target_binding_revision admission_epoch",
  );
  check(v.schema === "fleet-ops.submission-binding/v1");
  sha(v.submission_digest);
  for (const name of ["submission_id", "target_id", "target_binding_revision", "admission_epoch"])
    token(v[name]);
  return v;
};

export const validateRequest = (input: FleetCheckpointInput) => {
  const r = input.request;
  check(r.source.commit.length === r.source.tree.length);
  equal(r.recipe.recipe_digest, digest(recipe));
  equal(r.placement_policy_digest, digest(placementPolicy));
  const times = [r.earliest_start, r.acceptance_deadline, r.latest_start].map((s) => {
    check(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z$/.test(s));
    const n = Date.parse(s);
    check(Number.isFinite(n));
    return n;
  });
  check(times[0]! <= times[2]! && times[1]! <= times[2]!);
  const keys = r.demand.claims.map((c) => c.key);
  equal(keys, [...new Set(keys)].sort());
  check(Buffer.byteLength(canonical(r)) <= 64 * 1024);
};

export interface Expected {
  readonly input: FleetCheckpointInput;
  readonly workKey: CheckpointWorkKey;
  readonly requestDigest: string;
}
export interface Observation {
  readonly outcome: FleetCheckpointResult["verificationOutcome"];
  readonly cost: Document | null;
  readonly artifacts: readonly Document[];
}

export const receiptBinding = (value: unknown, selected: unknown): Document => {
  const v = fields(
    value,
    "schema authority service_id service_instance binding sequence accepted_at observed_at state dispatch_knowledge native_binding result native_references resolution host_observed_at resource_scope fence_generation retry",
  );
  check(v.schema === "fleet-ops.receipt/v1" && v.authority === "controller");
  equal(binding(v.binding), selected);
  check(Number.isSafeInteger(v.sequence) && Number(v.sequence) > 0);
  token(v.state);
  check(
    ["proven-not-dispatched", "may-have-dispatched", "dispatched"].includes(
      String(v.dispatch_knowledge),
    ),
  );
  return v;
};

export const observe = (value: Document, expected: Expected): Observation => {
  const v = fields(value, "schema work_key request_digest status reason selection original_result");
  check(v.schema === "fleet-ops.checkpoint-placement-result/v1");
  equal(v.work_key, expected.workKey);
  if (v.selection === null) {
    check(v.request_digest === null && v.original_result === null);
    check(v.status === "not-accepted" || v.status === "history-unavailable");
    check(
      ["epoch-closed", "work-not-accepted", "no-eligible-checkpoint"].includes(String(v.reason)),
    );
    return { outcome: "unknown", cost: null, artifacts: [] };
  }
  equal(v.request_digest, expected.requestDigest);
  check(
    ["accepted", "replayed", "original"].includes(String(v.status)) &&
      v.reason === "original-assignment",
  );
  const s = fields(
    v.selection,
    "schema submission assignment_id target_id binding_revision capsule_id source_digest recipe_digest compatibility_digest resource_demand_digest resource_snapshot_digest collection_digest approval_digest placement_policy_digest cost ranking",
  );
  check(s.schema === "fleet-ops.checkpoint-selection/v1");
  const b = binding(s.submission),
    r = expected.input.request;
  equal(
    [b.target_id, b.target_binding_revision, b.admission_epoch],
    [s.target_id, s.binding_revision, expected.workKey.admission_epoch],
  );
  equal(s.source_digest, digest(r.source));
  equal(s.recipe_digest, r.recipe.recipe_digest);
  equal(s.compatibility_digest, digest(r.compatibility));
  equal(s.placement_policy_digest, r.placement_policy_digest);
  for (const name of [
    "capsule_id",
    "resource_demand_digest",
    "resource_snapshot_digest",
    "collection_digest",
    "approval_digest",
  ])
    sha(s[name]);
  token(s.assignment_id);
  document(s.cost);
  check(["comparable-fresh-total", "host-policy-and-stable-identity"].includes(String(s.ranking)));
  const original = document(v.original_result);
  if (original.schema === "fleet-ops.retained-terminal-summary/v1") {
    fields(
      original,
      "schema binding terminal_state last_verified_effect native_references proof_digest detail_available",
    );
    equal(binding(original.binding), b);
    check(original.detail_available === false);
    sha(original.proof_digest);
    return { outcome: "unknown", cost: null, artifacts: [] };
  }
  const receipt = receiptBinding(original, b);
  if (receipt.result === null) return { outcome: "pending", cost: null, artifacts: [] };
  const result = fields(
    receipt.result,
    "schema_version binding disposition effect reason observed_postcondition native_references",
  );
  check(result.schema_version === 1);
  equal(result.binding, receipt.native_binding);
  if (result.observed_postcondition === null)
    return { outcome: "unknown", cost: null, artifacts: [] };
  const observed = fields(result.observed_postcondition, "observed_at values");
  if (!("attribution" in document(observed.values)))
    return { outcome: "unknown", cost: null, artifacts: [] };
  const report = fields(
    observed.values,
    "submission_digest native_operation_id native_request_digest attempt_id receipt validation cost artifact_hashes resource_completion attribution report_artifact",
  );
  const native = fields(receipt.native_binding, "operation_id request_digest");
  equal(
    [report.submission_digest, report.native_operation_id, report.native_request_digest],
    [b.submission_digest, native.operation_id, native.request_digest],
  );
  sha(native.request_digest);
  token(native.operation_id);
  const a = fields(
    report.attribution,
    "schema authority_effect target_id binding_revision capsule_id capsule_preimage_sha256 submission_digest native_operation_id native_request_digest reservation_id reservation_request_digest declaration_digest resource_binding resource_demand_sha256 resource_binding_id attempt_id verification_receipt_sha256 artifact_hashes_sha256 cost_sha256",
  );
  check(a.schema === "fleet-verification.attribution/v1" && a.authority_effect === "none");
  equal(
    [a.target_id, a.binding_revision, a.capsule_id, a.capsule_preimage_sha256],
    [s.target_id, s.binding_revision, s.capsule_id, s.capsule_id],
  );
  equal(
    [
      a.submission_digest,
      a.native_operation_id,
      a.native_request_digest,
      a.resource_binding_id,
      a.attempt_id,
    ],
    [
      b.submission_digest,
      native.operation_id,
      native.request_digest,
      native.request_digest,
      "attempt-" + native.request_digest,
    ],
  );
  equal(report.attempt_id, a.attempt_id);
  equal(a.resource_demand_sha256, s.resource_demand_digest);
  token(a.reservation_id);
  sha(a.reservation_request_digest);
  sha(a.declaration_digest);
  const heavy = fields(
    a.resource_binding,
    "schema authority_effect host_id boot_id source_revision execution_id recipe_id recipe_digest registry_digest policy_digest",
  );
  check(heavy.schema === "codex.heavy-binding/v1" && heavy.authority_effect === "none");
  equal(
    [
      heavy.host_id,
      heavy.source_revision,
      heavy.execution_id,
      heavy.recipe_id,
      heavy.recipe_digest,
    ],
    [s.target_id, r.source.commit, native.operation_id, r.recipe.recipe_id, r.recipe.recipe_digest],
  );
  token(heavy.boot_id);
  sha(heavy.registry_digest);
  sha(heavy.policy_digest);
  equal(a.verification_receipt_sha256, digest(report.receipt));
  equal(a.artifact_hashes_sha256, digest(report.artifact_hashes));
  equal(a.cost_sha256, digest(report.cost));
  const proof = fields(
    report.receipt,
    "schema authority_effect manifest_sha256 source lockset_sha256 submodules_sha256 lfs_sha256 destination recipe dependencies cache checks outcome artifacts",
  );
  check(proof.schema === "codex.verification-receipt/v1" && proof.authority_effect === "none");
  equal(proof.source, {
    repository: r.source.repository,
    commit: r.source.commit,
    tree: r.source.tree,
  });
  equal(
    [proof.manifest_sha256, proof.lockset_sha256, proof.submodules_sha256, proof.lfs_sha256],
    [
      r.source.manifest_sha256,
      r.source.lockset_sha256,
      r.source.submodule_sha256,
      r.source.lfs_sha256,
    ],
  );
  equal(proof.destination, r.compatibility);
  equal(proof.recipe, recipe);
  const validation = fields(
    report.validation,
    "schema authority_effect correspondence execution_proven outcome manifest_sha256 receipt_sha256 evidence_complete",
  );
  check(
    validation.schema === "codex.receipt-validation/v1" &&
      validation.authority_effect === "none" &&
      validation.correspondence === "valid" &&
      validation.execution_proven === false &&
      typeof validation.evidence_complete === "boolean",
  );
  equal(
    [validation.manifest_sha256, validation.receipt_sha256, validation.outcome],
    [r.source.manifest_sha256, digest(report.receipt), proof.outcome],
  );
  const dependencies = fields(proof.dependencies, "status sha256 evidence");
  if (dependencies.status === "known") {
    sha(dependencies.sha256);
    token(dependencies.evidence);
  } else
    check(
      dependencies.status === "unknown" &&
        dependencies.sha256 === null &&
        dependencies.evidence === null,
    );
  const cache = fields(proof.cache, "state compatibility_sha256 manifest_sha256 evidence");
  if (cache.state === "warm") {
    equal(
      [cache.compatibility_sha256, cache.manifest_sha256],
      [digest(r.compatibility), r.source.manifest_sha256],
    );
    token(cache.evidence);
  } else {
    check(cache.compatibility_sha256 === null && cache.manifest_sha256 === null);
    if (cache.state === "cold") token(cache.evidence);
    else check(cache.state === "unknown" && cache.evidence === null);
  }
  check(Array.isArray(proof.checks) && proof.checks.length === 1);
  const tested = fields(proof.checks[0], "id outcome exit_code");
  check(tested.id === "catalog" && ["passed", "failed"].includes(String(tested.outcome)));
  check(Number.isSafeInteger(tested.exit_code));
  check((tested.outcome === "passed") === (tested.exit_code === 0));
  equal(proof.outcome, tested.outcome);
  const cost = fields(
    report.cost,
    "schema authority_effect manifest_sha256 compatibility_sha256 observed_at_ms freshness_window_ms provenance uncertainty prepare_ms queue_ms run_ms return_ms missing_object_bytes preparation_bytes cache filesystems",
  );
  check(cost.schema === "codex.routing-cost/v1" && cost.authority_effect === "none");
  equal(
    [cost.manifest_sha256, cost.compatibility_sha256],
    [r.source.manifest_sha256, digest(r.compatibility)],
  );
  check(
    cost.queue_ms === null &&
      cost.return_ms === null &&
      cost.missing_object_bytes === null &&
      cost.uncertainty === "partial",
  );
  for (const key of [
    "prepare_ms",
    "run_ms",
    "observed_at_ms",
    "freshness_window_ms",
    "preparation_bytes",
  ])
    check(Number.isSafeInteger(cost[key]) && Number(cost[key]) >= 0);
  check(cost.provenance === "verification-observed-process");
  check(Array.isArray(proof.artifacts) && proof.artifacts.length <= 64);
  let bytes = 0;
  const hashes: Record<string, unknown> = {};
  const artifacts = proof.artifacts.map((item) => {
    const artifact = fields(
      item,
      "id sha256 bytes reference created_at_ms expires_at_ms retention_max_bytes",
    );
    token(artifact.id);
    sha(artifact.sha256);
    check(!(String(artifact.id) in hashes));
    hashes[String(artifact.id)] = artifact.sha256;
    check(
      typeof artifact.reference === "string" &&
        /^artifact:[A-Za-z0-9][A-Za-z0-9_-]{0,127}\/[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(
          artifact.reference,
        ),
    );
    for (const key of ["bytes", "created_at_ms", "expires_at_ms", "retention_max_bytes"])
      check(Number.isSafeInteger(artifact[key]) && Number(artifact[key]) >= 0);
    const retention = Number(artifact.expires_at_ms) - Number(artifact.created_at_ms);
    check(
      retention > 0 &&
        retention <= 30 * 86400 * 1000 &&
        Number(artifact.bytes) <= Number(artifact.retention_max_bytes),
    );
    bytes += Number(artifact.bytes);
    return artifact;
  });
  check(new Set(artifacts.map((a) => a.reference)).size === artifacts.length);
  check(bytes <= r.demand.artifact_max_bytes);
  equal(hashes, report.artifact_hashes);
  check(report.resource_completion === "pending-qualified-closure");
  const artifact = fields(report.report_artifact, "sha256 bytes");
  sha(artifact.sha256);
  check(Number.isSafeInteger(artifact.bytes) && Number(artifact.bytes) >= 0);
  const { report_artifact: _, ...stored } = report;
  equal(artifact.sha256, digest(stored));
  equal(artifact.bytes, Buffer.byteLength(canonical(stored)));
  check(result.effect === "completed");
  return { outcome: proof.outcome as "passed" | "failed", cost, artifacts };
};
