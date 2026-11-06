import { assert, it } from "@effect/vitest";
import {
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2ShellStreamItem,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { skipUnchangedThreadShells } from "../../orchestration-v2/ShellStream.ts";
import { createShellDeliveryGuard } from "./shellDelivery.ts";

const id = ThreadId.make("delivery:thread");
const now = DateTime.makeUnsafe("2026-11-06T22:58:00Z");
const thread: OrchestrationV2ThreadShell = {
  id,
  projectId: ProjectId.make("project"),
  title: "Thread",
  providerInstanceId: ProviderInstanceId.make("codex"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { rootThreadId: id, parentThreadId: null, relationshipToParent: null },
  forkedFrom: null,
  createdBy: "user",
  creationSource: "web",
  latestRunId: null,
  activeRunId: null,
  status: "starting",
  pendingRuntimeRequest: null,
  latestVisibleMessage: null,
  latestUserMessageAt: null,
  hasActionableProposedPlan: false,
  itemCount: 1,
  visibleItemCount: 1,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  deletedAt: null,
};
const updated = (
  sequence: number,
): Extract<OrchestrationV2ShellStreamItem, { kind: "thread.updated" }> => ({
  kind: "thread.updated",
  sequence,
  location: "active",
  thread,
});
const refreshed = (sequence: number): OrchestrationV2ShellStreamItem => ({
  kind: "thread.refreshed",
  sequence,
  location: "active",
  thread: { ...thread, status: "completed" },
});

it.effect("keeps starting when an older refresh races a materialization ahead of its event", () =>
  Effect.gen(function* () {
    const delivery = createShellDeliveryGuard();
    const first = delivery.record(updated(100), 101);
    const duplicate = delivery.record(updated(101), 101);
    const live = yield* Stream.make(first, duplicate).pipe(
      skipUnchangedThreadShells,
      Stream.runCollect,
    );
    // The corrective event is suppressed: the older refresh must never overwrite the row.
    assert.deepEqual(Array.from(live), [first]);
    const output = [...live, refreshed(100)].filter(delivery.accepts);
    assert.deepEqual(output, [first]);
    assert.strictEqual(first.sequence, 100);
    assert.strictEqual(first.thread.status, "starting");
    // Materialization 101 is a per-thread floor, not a global replay cursor.
    const other = { ...updated(101), thread: { ...thread, id: ThreadId.make("other") } };
    assert.isTrue(delivery.accepts(other));
  }),
);

it("allows derived updates at equal versions and rejects stale removals and omissions", () => {
  const delivery = createShellDeliveryGuard();
  assert.isTrue(delivery.accepts(delivery.record(updated(100), 101)));
  assert.isTrue(delivery.accepts(refreshed(101)));
  for (const kind of ["thread.removed", "thread.omitted"] as const) {
    assert.isFalse(delivery.accepts({ kind, sequence: 100, location: "active", threadId: id }));
    assert.isTrue(delivery.accepts({ kind, sequence: 101, location: "active", threadId: id }));
  }
});

it("rejects an older live read delivered after a newer refresh", () => {
  const delivery = createShellDeliveryGuard();
  const pending = delivery.record(updated(100), 100);
  assert.isTrue(delivery.accepts(refreshed(101)));
  assert.isFalse(delivery.accepts(pending));
  assert.isTrue(delivery.accepts(delivery.record(updated(101), 102)));
});
