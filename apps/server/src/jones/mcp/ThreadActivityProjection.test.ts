import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import type { ProviderThreadActivitySource } from "../provider/observations/ProviderThreadRuntimeObservation.ts";
import { project } from "./ThreadActivityProjection.ts";
const thread: ProviderThreadActivitySource = {
  id: ThreadId.make("thread"),
  activeProviderThreadId: null,
  archivedAt: null,
  deletedAt: null,
  pendingRuntimeRequest: null,
  activityRunStatus: "running",
  interactionMode: "default",
  hasActionableProposedPlan: false,
  latestRunCompletedAt: null,
};
it("known nonresident runtime does not revive projected working activity", () => {
  const result = project(thread, { status: "unknown", reason: "runtime_not_resident" }, 100);
  assert.isNull(result.foreground);
  assert.equal(result.backgroundStatus, "known");
});
it("unavailable evidence preserves the compatible projected foreground fallback", () => {
  for (const native of [null, { status: "unknown" as const, reason: "unsupported" }])
    assert.equal(project(thread, native, 100).foreground, "working");
});
it("nonresident runtime still preserves actionable foreground requests", () => {
  assert.equal(
    project(
      { ...thread, pendingRuntimeRequest: { kind: "user_input" } },
      { status: "unknown", reason: "runtime_not_resident" },
      100,
    ).foreground,
    "waiting_input",
  );
});
