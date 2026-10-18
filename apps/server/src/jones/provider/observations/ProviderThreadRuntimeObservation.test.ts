import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderRequestKind,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import * as RuntimeObservation from "./ProviderThreadRuntimeObservation.ts";

const thread: RuntimeObservation.ProviderThreadActivitySource = {
  id: ThreadId.make("observed-thread"),
  activeProviderThreadId: ProviderThreadId.make("observed-provider-thread"),
  archivedAt: null,
  deletedAt: null,
  pendingRuntimeRequest: null,
  activityRunStatus: null,
  interactionMode: "default",
  hasActionableProposedPlan: false,
  latestRunCompletedAt: null,
};
const sampledAt = Date.parse("2026-10-03T12:00:00Z");
function native(status: "working" | "monitoring" | "busy" | "idle") {
  return {
    status,
    observedAt: "2026-10-03T12:00:00Z",
    backgroundCoverage: "complete" as const,
    binding: {
      threadId: thread.id,
      providerThreadId: ProviderThreadId.make("observed-provider-thread"),
      providerSessionId: ProviderSessionId.make("observed-session"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      driver: ProviderDriverKind.make("codex"),
      runtimeGeneration: "actual-process",
      nativeThreadId: "native-thread",
    },
  } satisfies RuntimeObservation.ProviderRuntimeObservation;
}
const observe = RuntimeObservation.providerThreadActivityObservation;

it("counts resident background work and monitoring after foreground completion", () => {
  for (const status of ["working", "monitoring"] as const) {
    assert.equal(observe(thread, native(status), sampledAt).background, status);
  }
});

it("classifies decoded approval kinds while retaining current monitoring", () => {
  const decodeKind = Schema.decodeUnknownSync(ProviderRequestKind);
  for (const rawKind of ["command", "file-read", "file-change", "mcp-elicitation", "permission"]) {
    const kind = decodeKind(rawKind);
    for (const status of ["working", "monitoring"] as const) {
      const activity = observe(
        { ...thread, pendingRuntimeRequest: { kind } },
        native(status),
        sampledAt,
      );
      assert.equal(activity.foreground, "waiting_approval");
      assert.equal(activity.background, status === "monitoring" ? "monitoring" : null);
      assert.equal(activity.backgroundStatus, "known");
    }
  }
  for (const kind of ["dynamic_tool_call", "auth_refresh"] as const) {
    assert.isNull(
      observe({ ...thread, pendingRuntimeRequest: { kind } }, native("idle"), sampledAt).foreground,
    );
  }
});

it("keeps foreground approval and input ahead of native working or unavailable evidence", () => {
  for (const [kind, foreground] of [
    ["command", "waiting_approval"],
    ["user_input", "waiting_input"],
  ] as const) {
    for (const observation of [
      native("working"),
      null,
      { status: "unknown", reason: "unsupported" } as const,
    ]) {
      const activity = observe(
        { ...thread, activityRunStatus: "running", pendingRuntimeRequest: { kind } },
        observation,
        sampledAt,
      );
      assert.equal(activity.foreground, foreground);
      assert.isNull(activity.background);
    }
    const monitored = observe(
      { ...thread, pendingRuntimeRequest: { kind } },
      native("monitoring"),
      sampledAt,
    );
    assert.equal(monitored.foreground, foreground);
    assert.equal(monitored.background, "monitoring");
    assert.equal(monitored.backgroundStatus, "known");
  }
});

it("keeps actionable completed plans waiting and retains independent monitoring", () => {
  const planned = {
    ...thread,
    interactionMode: "plan" as const,
    hasActionableProposedPlan: true,
    latestRunCompletedAt: DateTime.makeUnsafe(sampledAt),
  };
  assert.equal(observe(planned, native("monitoring"), sampledAt).foreground, "waiting_plan");
  assert.equal(observe(planned, native("monitoring"), sampledAt).background, "monitoring");
  assert.isNull(
    observe({ ...planned, latestRunCompletedAt: null }, native("idle"), sampledAt).foreground,
  );
  assert.isNull(
    observe({ ...planned, hasActionableProposedPlan: false }, native("idle"), sampledAt).foreground,
  );
});

it("counts current foreground native busy without claiming complete background coverage", () => {
  const activity = observe({ ...thread, activityRunStatus: "running" }, native("busy"), sampledAt);
  assert.equal(activity.foreground, "working");
  assert.isNull(activity.background);
  assert.equal(activity.backgroundStatus, "unknown");
  assert.equal(activity.reason, "native_background_coverage_incomplete");
  assert.equal(observe(thread, native("busy"), sampledAt).backgroundStatus, "unknown");
});

it("rejects stale or invalid sampling evidence and clears absent residents", () => {
  assert.equal(observe(thread, native("working"), sampledAt + 1).reason, "native_activity_stale");
  assert.equal(
    observe(thread, { ...native("working"), observedAt: "invalid" }, sampledAt).reason,
    "native_activity_stale",
  );
  assert.equal(observe(thread, native("working"), Number.NaN).reason, "native_activity_stale");
  const stopped = observe(thread, { status: "unknown", reason: "runtime_not_resident" }, sampledAt);
  assert.isNull(stopped.background);
  assert.equal(stopped.backgroundStatus, "known");
  for (const observation of [null, { status: "unknown", reason: "unsupported" } as const]) {
    const unavailable = observe(thread, observation, sampledAt);
    assert.isNull(unavailable.background);
    assert.equal(unavailable.backgroundStatus, "unknown");
  }
  assert.equal(observe(thread, null, sampledAt).reason, "native_activity_unavailable");
});

it("does not count persisted running intent without current native activity", () => {
  for (const activityRunStatus of ["preparing", "starting", "running"] as const) {
    const running = { ...thread, activityRunStatus };
    assert.isNull(
      observe(running, { status: "unknown", reason: "runtime_not_resident" }, sampledAt).foreground,
    );
    assert.isNull(observe(running, native("idle"), sampledAt).foreground);
    assert.isNull(observe(running, native("working"), sampledAt + 1).foreground);
    assert.equal(observe(running, native("working"), sampledAt).foreground, "working");
    assert.equal(observe(running, native("monitoring"), sampledAt).background, "monitoring");
  }
});

it("counts the native owner even after the selected target account changes", () => {
  const selected = {
    ...thread,
    modelSelection: { instanceId: ProviderInstanceId.make("next-account") },
  };
  for (const status of ["working", "monitoring"] as const) {
    const activity = observe(selected, native(status), sampledAt);
    assert.equal(activity.background, status);
    assert.equal(activity.backgroundStatus, "known");
  }
});

it("excludes archived or deleted threads and rejects an old native conversation binding", () => {
  for (const excluded of [
    { ...thread, archivedAt: DateTime.makeUnsafe(sampledAt) },
    { ...thread, deletedAt: DateTime.makeUnsafe(sampledAt) },
  ]) {
    assert.deepStrictEqual(observe(excluded, native("working"), sampledAt), {
      foreground: null,
      background: null,
      backgroundStatus: "known",
    });
  }
  for (const binding of [
    { ...native("monitoring").binding, providerThreadId: ProviderThreadId.make("old-binding") },
    { ...native("monitoring").binding, threadId: ThreadId.make("other-thread") },
  ]) {
    const activity = observe(
      { ...thread, activityRunStatus: "running" },
      { ...native("working"), binding },
      sampledAt,
    );
    assert.isNull(activity.foreground);
    assert.isNull(activity.background);
    assert.equal(activity.backgroundStatus, "unknown");
    assert.equal(activity.reason, "runtime_binding_changed");
  }
  assert.equal(
    observe({ ...thread, activeProviderThreadId: null }, native("monitoring"), sampledAt).reason,
    "runtime_binding_changed",
  );
});

it("preserves ordinary foreground status separately when native coverage is unavailable", () => {
  const running = { ...thread, activityRunStatus: "running" as const };
  assert.equal(RuntimeObservation.providerThreadForegroundActivity(running), "working");
  assert.equal(observe(running, null, sampledAt).backgroundStatus, "unknown");
});

it("reports positive native background work without inventing complete coverage", () => {
  const activity = observe(
    thread,
    { ...native("monitoring"), backgroundCoverage: "partial" },
    sampledAt,
  );
  assert.equal(activity.background, "monitoring");
  assert.equal(activity.backgroundStatus, "unknown");
  assert.equal(activity.reason, "native_background_coverage_incomplete");
});

it("defaults omitted native background coverage to partial", () => {
  const { backgroundCoverage: _coverage, ...observation } = native("working");
  assert.equal(observe(thread, observation, sampledAt).background, "working");
  assert.equal(observe(thread, observation, sampledAt).backgroundStatus, "unknown");
});
