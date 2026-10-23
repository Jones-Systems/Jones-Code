import {
  WORKSTREAM_OPERATIONS_CONTRACT,
  type WorkstreamOperationResult,
  type WorkstreamSetupStatus,
} from "@t3tools/contracts/jones/workstreamOperations";
import { data } from "../../components/workstreams/nativeWorkstreamActions.fixtures";

export const operationFixtureData = {
  ...data,
  binding: { ...data.binding, registryId: "https://registry.test" },
};
export const operationFixtureResult = (
  state: "committed" | "reconciling" | "conflict" = "committed",
): WorkstreamOperationResult => {
  const base = {
    contract: WORKSTREAM_OPERATIONS_CONTRACT,
    operation_id: "operation-fixture-0001",
    kind: "set_primary" as const,
    request_sha256: "a".repeat(64),
    origin: { kind: "app" as const },
    observed: { kind: "primary" as const, workstream_id: "beta", name: "Beta" },
    children: [],
    accepted_at: "2026-10-23T00:00:00Z",
    updated_at: "2026-10-23T00:00:00Z",
    detail: null,
  };
  if (state === "committed")
    return {
      ...base,
      state,
      reason: null,
      poll_after_ms: null,
      result: { workstream_id: "beta", membership_id: "membership-beta", registry_version: 12 },
    };
  if (state === "conflict")
    return { ...base, state, reason: "precondition_failed", poll_after_ms: null, result: null };
  return { ...base, state, reason: "membership_effect_unknown", poll_after_ms: 1000, result: null };
};
export const setupFixture: WorkstreamSetupStatus = {
  contract: WORKSTREAM_OPERATIONS_CONTRACT,
  environment_id: "env:a",
  observed_at: "2026-10-23T00:00:00Z",
  authority_effect: "none",
  activation: { state: "enabled", missing_keys: [] },
  registry: {
    state: "ready",
    registry_id: "8cdd84b6-81c5-4c88-9d13-5d324abaf662",
    owner_id: "owner",
    can_write: true,
  },
  t3_source: { state: "ready", reason: null },
  placements: { readiness: "ready" },
  operations: { open: 0 },
  next_steps: [],
};
