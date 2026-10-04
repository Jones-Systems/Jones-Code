import { assert, it } from "@effect/vitest";
import { ProviderInstanceId, ProviderRequestKind, ProviderSessionId, ProviderThreadId, ThreadId, type OrchestrationV2ThreadShell } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import type { ProviderRuntimeObservation } from "./ProviderAdapter.ts";
import { providerThreadActivityObservation } from "./ProviderThreadRuntimeObservation.ts";

const thread = {
  id: ThreadId.make("observed-thread"), activeProviderThreadId: ProviderThreadId.make("observed-provider-thread"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex") }, archivedAt: null, deletedAt: null,
  pendingRuntimeRequest: null, activityRunStatus: null, interactionMode: "default", hasActionableProposedPlan: false, latestRunCompletedAt: null,
} as unknown as OrchestrationV2ThreadShell;
const sampledAt = Date.parse("2026-10-03T12:00:00Z");
function native(status: "working" | "monitoring" | "busy" | "idle"): ProviderRuntimeObservation {
  return { status, observedAt: "2026-10-03T12:00:00Z", binding: {
    threadId: thread.id, providerThreadId: thread.activeProviderThreadId!,
    providerSessionId: ProviderSessionId.make("observed-session"), instanceId: thread.modelSelection.instanceId,
    runtimeGeneration: "actual-process", nativeThreadId: "native-thread",
  } };
}

it("counts resident background working and monitoring after foreground completion", () => {
  for (const status of ["working", "monitoring"] as const)
    assert.equal(providerThreadActivityObservation(thread, native(status), sampledAt).background, status);
});

it("classifies decoded provider approval kinds while retaining current monitoring", () => {
  const decodeKind = Schema.decodeUnknownSync(ProviderRequestKind);
  for (const rawKind of ["command", "file-read", "file-change", "mcp-elicitation", "permission"]) {
    const kind = decodeKind(rawKind);
    for (const status of ["working", "monitoring"] as const) {
      const activity = providerThreadActivityObservation({ ...thread, pendingRuntimeRequest: { kind } }, native(status), sampledAt);
      assert.equal(activity.foreground, "waiting_approval");
      assert.equal(activity.background, status === "monitoring" ? "monitoring" : null);
      assert.equal(activity.backgroundStatus, "known");
    }
  }
  assert.equal(providerThreadActivityObservation({ ...thread, pendingRuntimeRequest: { kind: "user_input" } },
    native("monitoring"), sampledAt).foreground, "waiting_input");
  for (const kind of ["dynamic_tool_call", "auth_refresh"] as const)
    assert.isNull(providerThreadActivityObservation({ ...thread, pendingRuntimeRequest: { kind } }, native("idle"), sampledAt).foreground);
});

it("keeps foreground approval and input ahead of native activity", () => {
  for (const [kind, foreground] of [["command", "waiting_approval"], ["user_input", "waiting_input"]] as const) {
    const activity = providerThreadActivityObservation({ ...thread, pendingRuntimeRequest: { kind } }, native("working"), sampledAt);
    assert.equal(activity.foreground, foreground);
    assert.isNull(activity.background);
  }
});

it("preserves current monitoring while foreground approval or input wins presentation", () => {
  for (const [kind, foreground] of [["command", "waiting_approval"], ["user_input", "waiting_input"]] as const) {
    const activity = providerThreadActivityObservation({ ...thread,
      pendingRuntimeRequest: { kind },
    }, native("monitoring"), sampledAt);
    assert.equal(activity.foreground, foreground);
    assert.equal(activity.background, "monitoring");
    assert.equal(activity.backgroundStatus, "known");
  }
});

it("counts current foreground native busy without claiming complete background coverage", () => {
  const activity = providerThreadActivityObservation({ ...thread, activityRunStatus: "running" }, native("busy"), sampledAt);
  assert.equal(activity.foreground, "working");
  assert.isNull(activity.background);
  assert.equal(activity.backgroundStatus, "unknown");
});

it("rejects stale or ambiguous native background evidence and clears absent residents", () => {
  assert.equal(providerThreadActivityObservation(thread, native("working"), sampledAt + 1).reason, "native_activity_stale");
  assert.equal(providerThreadActivityObservation(thread, native("busy"), sampledAt).backgroundStatus, "unknown");
  const stopped = providerThreadActivityObservation(thread, { status: "unknown", reason: "runtime_not_resident" }, sampledAt);
  assert.isNull(stopped.background);
  assert.equal(stopped.backgroundStatus, "known");
});

it("does not count persisted running intent without current native activity", () => {
  const running = { ...thread, activityRunStatus: "running" as const };
  assert.isNull(providerThreadActivityObservation(running, { status: "unknown", reason: "runtime_not_resident" }, sampledAt).foreground);
  assert.isNull(providerThreadActivityObservation(running, native("idle"), sampledAt).foreground);
  assert.isNull(providerThreadActivityObservation(running, native("working"), sampledAt + 1).foreground);
  assert.equal(providerThreadActivityObservation(running, native("working"), sampledAt).foreground, "working");
});

it("counts the current native owner after the selected target account changes", () => {
  const selected = { ...thread, modelSelection: { ...thread.modelSelection, instanceId: ProviderInstanceId.make("next-account") } };
  for (const status of ["working", "monitoring"] as const) {
    const activity = providerThreadActivityObservation(selected, native(status), sampledAt);
    assert.equal(activity.background, status);
    assert.equal(activity.backgroundStatus, "known");
  }
});

it("excludes archived threads and rejects an old native binding", () => {
  assert.isNull(providerThreadActivityObservation({ ...thread, archivedAt: {} as never }, native("working"), sampledAt).background);
  const observation = native("monitoring");
  if (observation.status === "unknown") throw new Error("fixture");
  assert.equal(providerThreadActivityObservation(thread, { ...observation, binding: { ...observation.binding, providerThreadId: ProviderThreadId.make("old-binding") } }, sampledAt).backgroundStatus, "unknown");
});
