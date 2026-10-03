import { assert, it, vi } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";

import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as WorkspaceEntries from "../workspace/WorkspaceEntries.ts";
import * as CheckpointCapture from "./CheckpointCaptureService.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as RunFinalization from "./RunFinalizationService.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";

function finalizationUse(threadId: ThreadId): OrdinaryCheckout.OrdinaryCheckoutUseV1 {
  const now = DateTime.makeUnsafe("2026-10-03T00:00:00.000Z");
  const admission = { version: 1 as const, admissionId: "a".repeat(64), admissionSha256: "b".repeat(64) };
  const applicationBirth = { kind: "application_v2_thread_birth" as const, threadId,
    eventId: EventId.make("event:finalization-birth"), sequence: 1 };
  return { version: 1, kind: "ordinary_checkout_use", operationId: "operation:finalization", admission,
    lease: { resourcePath: "/repo", leaseId: "lease:finalization", ownerThreadId: threadId,
      ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(applicationBirth), branch: null,
      acquiredAtMs: 1, renewedAtMs: 1, expiresAtMs: 9999999999999 },
    source: { kind: "outbox", workerId: "worker:finalization", expectedAttempt: 1, leaseExpiresAt: DateTime.add(now, { hours: 1 }),
      link: { version: 1, effectId: "effect:finalization", commandId: CommandId.make("command:finalization"),
        threadId, requestSha256: "c".repeat(64), admission, recordedAt: now } } };
}

it.effect.each([false, true])("refreshes workspace after checkpoint capture without reading history, ordinary=%s", (ordinary) => {
  const threadId = ThreadId.make("thread_finalize");
  const runId = RunId.make("run_finalize");
  const scopeId = CheckpointScopeId.make("scope_finalize");
  const use = finalizationUse(threadId);
  const capture = vi.fn((_input: Parameters<CheckpointCapture.CheckpointCaptureServiceV2Shape["execute"]>[0]) => Effect.void);
  const refresh = vi.fn(() => Effect.void);
  const checkpointContext = {
    runs: [],
    checkpointScopes: [{ id: scopeId, runId, kind: "root_run" as const, cwd: "/repo" }],
    checkpoints: [],
  };
  const layer = RunFinalization.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(CheckpointCapture.CheckpointCaptureServiceV2)({ execute: capture }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadProjection: () =>
            Effect.die("workspace refresh must not load transcript history"),
          getCheckpointContext: () => Effect.succeed(checkpointContext),
        }),
        Layer.succeed(RunFinalization.RunFinalizationObserver, {
          refresh,
          refreshAfterTurn: () => Effect.void,
        }),
      ),
    ),
  );
  return Effect.gen(function* () {
    const service = yield* RunFinalization.RunFinalizationService;
    yield* service.finalize({ threadId, runId, scopeId,
      ...(ordinary ? { ordinaryCheckoutUse: use } : {}) });
    assert.equal(capture.mock.calls.length, 1);
    if (ordinary) assert.strictEqual(capture.mock.calls[0]?.[0].ordinaryCheckoutUse, use);
    assert.deepEqual(refresh.mock.calls[0], [{ cwd: "/repo", threadId, runId }]);
  }).pipe(Effect.provide(layer));
});

it.effect.each(["stale_admission", "claim_mismatch", "unknown_use"] as const)(
  "finalization preserves %s and stops before workspace refresh",
  (reason) => {
    const threadId = ThreadId.make("thread:rejected-finalization");
    const use = finalizationUse(threadId);
    const error = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
      reason, threadId, path: "/repo", message: "Durable checkpoint ownership revalidation rejected entry.",
    });
    const refresh = vi.fn(() => Effect.void);
    const projection = vi.fn(() => Effect.die("Rejected checkpoint must not refresh the workspace."));
    const layer = RunFinalization.layer.pipe(Layer.provide(Layer.mergeAll(
      Layer.mock(CheckpointCapture.CheckpointCaptureServiceV2)({ execute: (input) => {
        assert.strictEqual(input.ordinaryCheckoutUse, use);
        return Effect.fail(error);
      } }),
      Layer.mock(ProjectionStore.ProjectionStoreV2)({ getCheckpointContext: projection }),
      Layer.succeed(RunFinalization.RunFinalizationObserver, { refresh, refreshAfterTurn: () => Effect.void }),
    )));
    return Effect.gen(function* () {
      const service = yield* RunFinalization.RunFinalizationService;
      const actual = yield* service.finalize({ threadId, runId: RunId.make("run:rejected-finalization"),
        scopeId: CheckpointScopeId.make("scope:rejected-finalization"), ordinaryCheckoutUse: use }).pipe(Effect.flip);
      assert.strictEqual(actual, error);
      assert.equal(projection.mock.calls.length, 0);
      assert.equal(refresh.mock.calls.length, 0);
    }).pipe(Effect.provide(layer));
  },
);

for (const scenario of [
  {
    label: "discovers a new PR for the completed run's branch",
    branch: "feature",
    checkedOut: "feature",
    activeRun: null,
    expected: ["/repo"],
  },
  {
    label: "leaves the default branch's PR cache alone",
    branch: "main",
    checkedOut: "main",
    activeRun: null,
    expected: [],
  },
  {
    label: "does not refresh another thread's checkout",
    branch: "feature",
    checkedOut: "other",
    activeRun: null,
    expected: [],
  },
  {
    label: "does not refresh during a newer active run",
    branch: "feature",
    checkedOut: "feature",
    activeRun: "newer-run",
    expected: [],
  },
] as const) {
  it.effect(scenario.label, () => {
    const refreshed: string[] = [];
    const threadId = ThreadId.make("thread-pr-refresh");
    const runId = RunId.make("completed-run");
    const layer = RunFinalization.observerLive.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(WorkspaceEntries.WorkspaceEntries)({ refresh: () => Effect.void }),
          Layer.mock(PullRequestService.PullRequestService)({
            refreshAfterTurn: () => Effect.void,
          }),
          Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({
            refreshLocalStatus: () =>
              Effect.succeed({
                isRepo: true,
                hasPrimaryRemote: true,
                isDefaultRef: scenario.checkedOut === "main",
                refName: scenario.checkedOut,
                hasWorkingTreeChanges: false,
                workingTree: { files: [], insertions: 0, deletions: 0 },
              }),
            refreshStatus: () =>
              Effect.die("turn completion must preserve known PRs and lookup backoff"),
            refreshPullRequestStatus: (cwd) =>
              Effect.sync(() => {
                refreshed.push(cwd);
                return null;
              }),
          }),
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getThreadShell: () =>
              Effect.succeed({
                id: threadId,
                branch: scenario.branch,
                activeRunId: scenario.activeRun === null ? null : RunId.make(scenario.activeRun),
              } as OrchestrationV2ThreadShell),
          }),
        ),
      ),
    );
    return Effect.gen(function* () {
      const observer = yield* RunFinalization.RunFinalizationObserver;
      yield* observer.refresh({ cwd: "/repo", threadId, runId });
      assert.deepEqual(refreshed, [...scenario.expected]);
    }).pipe(Effect.provide(layer));
  });
}
