import { assert, it } from "@effect/vitest";
import {
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ShellStreamItem,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { skipUnchangedThreadShells } from "../../orchestration-v2/ShellStream.ts";
import { createShellDeliveryGuard } from "./shellDelivery.ts";
import { applyShellStreamEvent } from "../../../../../packages/client-runtime/src/state/shellReducer.ts";
import { v2ShellSnapshot } from "../../../../../packages/client-runtime/src/state/orchestrationV2TestFixtures.ts";
import { advanceThreadRefreshWatermarks } from "../../../../../packages/client-runtime/src/jones/threadResultDependencies/shellRefresh.ts";
import {
  hasUnseenCompletion,
  resolveThreadAttentionStatus,
  shellAttentionInput,
} from "@t3tools/shared/jones/threadAttention";

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
  latestRunId: RunId.make("current-run"),
  activeRunId: RunId.make("current-run"),
  latestRunCompletedAt: null,
  lastVisitedAt: DateTime.makeUnsafe("2026-11-06T22:57:00Z"),
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
  thread: { ...thread, status: "completed", latestRunCompletedAt: now, activeRunId: null },
});

it.effect.each(["live-first", "refresh-first", "refresh-last"] as const)(
  "keeps the client Working when an older refresh races a newer start (%s)",
  (order) =>
    Effect.gen(function* () {
      const delivery = createShellDeliveryGuard();
      const first = delivery.record(updated(100), 101);
      const duplicate = delivery.record(updated(101), 101);
      const stale = refreshed(100);
      const steps =
        order === "live-first"
          ? [first, stale, duplicate]
          : order === "refresh-first"
            ? [stale, first, duplicate]
            : [first, duplicate, stale];
      const output = yield* Stream.fromIterable(steps).pipe(
        Stream.rechunk(1),
        Stream.filter(delivery.accepts),
        (merged) => skipUnchangedThreadShells(merged, { canSuppress: delivery.canSuppress }),
        Stream.runCollect,
      );
      let snapshot: OrchestrationV2ShellSnapshot = {
        ...v2ShellSnapshot,
        snapshotSequence: 99,
        threads: [{ ...thread, status: "completed" as const, latestRunCompletedAt: now }],
      };
      let watermarks: ReadonlyMap<ThreadId, number> = new Map();
      for (const item of output) {
        if (item.kind === "snapshot" || item.kind === "synchronized") continue;
        const next = applyShellStreamEvent(snapshot, item, new Map(), watermarks);
        watermarks = advanceThreadRefreshWatermarks(watermarks, snapshot, item, new Map());
        snapshot = next;
      }
      assert.strictEqual(snapshot.threads[0]?.status, "starting");
      const attention = shellAttentionInput(snapshot.threads[0]!);
      assert.strictEqual(resolveThreadAttentionStatus(attention), "working");
      assert.isFalse(hasUnseenCompletion(attention));
      assert.strictEqual(first.sequence, 100);
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
