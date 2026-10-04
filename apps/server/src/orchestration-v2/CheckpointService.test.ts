import { assert, it, vi } from "@effect/vitest";
import {
  CheckpointScopeId,
  CheckpointId,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  NodeId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2CheckpointScope,
  VcsProcessTimeoutError,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as EventSink from "./EventSink.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";

it.effect.each([false, true, "interrupt"] as const)(
  "materializes baseline, lookup fails=%s",
  (lookupFails) => {
    const scope: OrchestrationV2CheckpointScope = {
      id: CheckpointScopeId.make("checkpoint-scope:materialize-baseline"),
      threadId: ThreadId.make("thread:materialize-baseline"),
      runId: RunId.make("run:materialize-baseline:3"),
      nodeId: NodeId.make("node:materialize-baseline:3"),
      parentScopeId: null,
      providerThreadId: ProviderThreadId.make("provider-thread:materialize-baseline"),
      kind: "root_run",
      ordinalWithinParent: 0,
      advancesAppRunCount: true,
      cwd: "/repo",
      createdAt: DateTime.makeUnsafe("2026-07-28T00:00:00.000Z"),
    };
    const hasCheckpointRef = vi.fn((_input: CheckpointStore.RestoreCheckpointInput) =>
      lookupFails === "interrupt"
        ? Effect.interrupt
        : lookupFails
          ? Effect.fail(
              new VcsProcessTimeoutError({
                operation: "test.ref",
                command: "git",
                cwd: "/repo",
                timeoutMs: 30000,
              }),
            )
          : Effect.succeed(true),
    );
    const testLayer = CheckpointService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          IdAllocator.layer,
          Layer.mock(CheckpointStore.CheckpointStore)({
            isGitRepository: () => Effect.succeed(true),
            hasCheckpointRef,
            captureCheckpoint: () => Effect.void,
          }),
        ),
      ),
    );

    return Effect.gen(function* () {
      const checkpoints = yield* CheckpointService.CheckpointServiceV2;
      if (lookupFails === "interrupt") {
        const exit = yield* Effect.exit(
          checkpoints.materializeBaselineCheckpoint({ scope, ordinalWithinScope: 2 }),
        );
        assert.isTrue(Exit.hasInterrupts(exit));
        const captureExit = yield* Effect.exit(
          checkpoints.capture({
            scope,
            ordinalWithinScope: 1,
            runId: scope.runId!,
            nodeId: scope.nodeId!,
            appRunOrdinal: 1,
            capturedAt: scope.createdAt,
          }),
        );
        assert.isTrue(Exit.hasInterrupts(captureExit));
        return;
      }
      const baseline = yield* checkpoints.materializeBaselineCheckpoint({
        scope,
        ordinalWithinScope: 2,
      });

      assert.equal(baseline.ordinalWithinScope, 2);
      assert.equal(
        baseline.ref,
        CheckpointService.checkpointRefForScopeOrdinal({
          scopeId: scope.id,
          ordinalWithinScope: 2,
        }),
      );
      assert.equal(baseline.status, lookupFails ? "missing" : "ready");
      assert.deepEqual(hasCheckpointRef.mock.calls[0]?.[0], {
        cwd: scope.cwd,
        checkpointRef: baseline.ref,
      });
    }).pipe(Effect.provide(testLayer));
  },
);

function ordinaryMutationFixture() {
  const now = DateTime.makeUnsafe("2026-10-03T00:00:00.000Z");
  const threadId = ThreadId.make("thread:ordinary-checkpoint");
  const runId = RunId.make("run:ordinary-checkpoint");
  const nodeId = NodeId.make("node:ordinary-checkpoint");
  const commandId = CommandId.make("command:ordinary-checkpoint");
  const birth = { kind: "application_v2_thread_birth" as const, threadId,
    eventId: EventId.make("event:ordinary-birth"), sequence: 1 };
  const lease = { resourcePath: "/repo", leaseId: "lease:ordinary-checkpoint", ownerThreadId: threadId,
    ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(birth), branch: "feature",
    acquiredAtMs: 1, renewedAtMs: 1, expiresAtMs: 9999999999999 };
  const canonicalCommand = { commandId, type: "run.start", threadId };
  const capture = { version: 1 as const, commandId, commandType: canonicalCommand.type, canonicalCommand,
    commandDigest: OrdinaryCheckout.ordinaryCheckoutCommandDigestV1(canonicalCommand), origin: { kind: "command" as const },
    threadId, applicationBirth: birth, projectId: ProjectId.make("project:ordinary-checkpoint"),
    canonicalProjectRoot: "/repo", canonicalCheckoutPath: "/repo", branch: "feature", lease };
  const admission: OrdinaryCheckout.OrdinaryCheckoutAdmissionV1 = {
    version: 1, admissionId: OrdinaryCheckout.ordinaryCheckoutAdmissionIdV1(capture), capture,
    receipt: { commandId, threadId, commandType: capture.commandType, acceptedAt: now,
      resultSequence: 2, status: "accepted", error: null },
    eventBasis: [{ eventId: EventId.make("event:ordinary-run"), sequence: 2, threadId,
      commandId, eventType: "run.created" }],
    run: { runId, runAttemptId: "attempt:ordinary-checkpoint", nodeId,
      messageId: MessageId.make("message:ordinary-checkpoint") }, recordedAt: now,
  };
  const reference = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
  const use: OrdinaryCheckout.OrdinaryCheckoutUseV1 = {
    version: 1, kind: "ordinary_checkout_use", operationId: "operation:ordinary-checkpoint",
    admission: reference, lease, source: { kind: "outbox", workerId: "worker:ordinary-checkpoint",
      expectedAttempt: 1, leaseExpiresAt: DateTime.add(now, { hours: 1 }), link: { version: 1, effectId: "effect:ordinary-checkpoint",
        commandId, threadId, requestSha256: "a".repeat(64), admission: reference, recordedAt: now } },
  };
  const scope: OrchestrationV2CheckpointScope = {
    id: CheckpointScopeId.make("scope:ordinary-checkpoint"), threadId, runId, nodeId,
    parentScopeId: null, providerThreadId: ProviderThreadId.make("provider-thread:ordinary-checkpoint"),
    kind: "root_run", ordinalWithinParent: 0, advancesAppRunCount: true, cwd: "/repo", createdAt: now,
  };
  const checkpoint = { id: CheckpointId.make("checkpoint:ordinary-checkpoint"), threadId, scopeId: scope.id,
    runId, nodeId, parentCheckpointId: null, ordinalWithinScope: 1, appRunOrdinal: 1,
    ref: CheckpointService.checkpointRefForScopeOrdinal({ scopeId: scope.id, ordinalWithinScope: 1 }),
    status: "ready" as const, files: [], capturedAt: now };
  return { now, admission, use, scope, checkpoint };
}

const ordinaryCheckpointMutations = ["baseline", "capture", "restore", "prune"] as const;
for (const mutation of ordinaryCheckpointMutations) {
  it.effect.each(["allowed", "birth", "lease", "effect", "unknown"] as const)(
    `ordinary ${mutation} revalidation=%s`,
    (outcome) => Effect.gen(function* () {
      const fixture = ordinaryMutationFixture();
      const calls: string[] = [];
      const actualUse: OrdinaryCheckout.OrdinaryCheckoutUseV1 = outcome === "birth" ? { ...fixture.use,
        lease: { ...fixture.use.lease, ownerIncarnation: "superseded-birth" } } :
        outcome === "lease" ? { ...fixture.use, lease: { ...fixture.use.lease, leaseId: "superseded-lease" } } :
        outcome === "effect" && fixture.use.source.kind === "outbox" ? { ...fixture.use,
          source: { ...fixture.use.source, link: { ...fixture.use.source.link, effectId: "unclaimed-effect" } } } : fixture.use;
      const rejected = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
        reason: outcome === "birth" ? "stale_admission" : outcome === "effect" ? "claim_mismatch" :
          outcome === "lease" ? "target_changed" : "unknown_use",
        threadId: fixture.scope.threadId, path: fixture.scope.cwd, message: "Durable ownership revalidation rejected entry.",
      });
      const sinkLayer = Layer.mock(EventSink.EventSinkV2)({
        readOrdinaryCheckoutAdmissionForRun: (input) => Effect.sync(() => {
          assert.deepEqual(input, { threadId: fixture.scope.threadId, runId: fixture.scope.runId });
          calls.push("read-admission"); return fixture.admission;
        }),
        revalidateOrdinaryCheckoutUse: (actual) => Effect.suspend(() => {
          assert.strictEqual(actual, actualUse);
          calls.push("revalidate");
          return outcome === "allowed" ? Effect.succeed({ subject: { schema: "t3.ordinary-checkout-use/v1" as const,
            use: actual, source: { projectWorkspaceRoot: "/repo", worktreePath: null } },
            state: "started" as const, startedAt: DateTime.formatIso(fixture.now) }) : Effect.fail(rejected);
        }),
      });
      const mutate = () => Effect.sync(() => { calls.push("mutate"); });
      const storeLayer = Layer.mock(CheckpointStore.CheckpointStore)({
        isGitRepository: () => Effect.sync(() => { calls.push("repository-read"); return true; }),
        hasCheckpointRef: () => Effect.sync(() => { calls.push("ref-read"); return false; }),
        captureCheckpoint: mutate,
        restoreCheckpoint: () => mutate().pipe(Effect.as(true)),
        deleteCheckpointRefs: mutate,
      });
      const testLayer = Layer.mergeAll(sinkLayer, CheckpointService.layer.pipe(
        Layer.provide(Layer.mergeAll(IdAllocator.layer, storeLayer))));
      yield* Effect.gen(function* () {
        const checkpoints = yield* CheckpointService.CheckpointServiceV2;
        const input = { scope: fixture.scope, ordinaryCheckoutUse: actualUse };
        const operation = mutation === "baseline" ? checkpoints.captureBaseline({ ...input, ordinalWithinScope: 0 }) :
          mutation === "capture" ? checkpoints.capture({ ...input, runId: fixture.scope.runId,
            nodeId: fixture.scope.nodeId!, ordinalWithinScope: 1, appRunOrdinal: 1, capturedAt: fixture.now }) :
          mutation === "restore" ? checkpoints.restore({ ...input, checkpoint: fixture.checkpoint }) :
          checkpoints.deleteStaleRefs({ ...input, checkpoints: [fixture.checkpoint] });
        if (outcome === "allowed") {
          yield* operation;
          assert.equal(calls.filter((call) => call === "mutate").length, 1);
          assert.equal(calls[calls.indexOf("mutate") - 1], "revalidate");
        } else {
          const error = yield* operation.pipe(Effect.flip);
          assert.strictEqual(error, rejected);
          assert.equal(calls.filter((call) => call === "mutate").length, 0);
        }
        assert.equal(calls.filter((call) => call === "revalidate").length, 1);
      }).pipe(Effect.provide(testLayer));
    }),
  );
}

it.effect.each(["missing-service", "wrong-target", "wrong-run"] as const)(
  "ordinary checkpoint holds %s before Store mutation",
  (reason) => Effect.gen(function* () {
    const fixture = ordinaryMutationFixture();
    const capture = vi.fn(() => Effect.void);
    const revalidate = vi.fn(() => Effect.die("A rejected target must never enter checkout use."));
    const sink = Layer.mock(EventSink.EventSinkV2)({
      readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(null), revalidateOrdinaryCheckoutUse: revalidate,
    });
    const layer = CheckpointService.layer.pipe(Layer.provide(Layer.mergeAll(IdAllocator.layer,
      Layer.mock(CheckpointStore.CheckpointStore)({ isGitRepository: () => Effect.succeed(true),
        hasCheckpointRef: () => Effect.succeed(false), captureCheckpoint: capture }))));
    const provided = reason === "missing-service" ? layer : Layer.merge(layer, sink);
    const ordinaryCheckoutUse = reason === "wrong-target" ? { ...fixture.use,
      lease: { ...fixture.use.lease, resourcePath: "/different-checkout" } } : fixture.use;
    yield* Effect.gen(function* () {
      const checkpoints = yield* CheckpointService.CheckpointServiceV2;
      const error = yield* checkpoints.capture({ scope: fixture.scope, ordinaryCheckoutUse,
        runId: fixture.scope.runId, nodeId: fixture.scope.nodeId!, ordinalWithinScope: 1,
        appRunOrdinal: 1, capturedAt: fixture.now }).pipe(Effect.flip);
      assert.instanceOf(error, OrdinaryCheckout.OrdinaryCheckoutOwnershipError);
      assert.equal(capture.mock.calls.length, 0);
      assert.equal(revalidate.mock.calls.length, 0);
    }).pipe(Effect.provide(provided));
  }),
);

it.effect("capture without an ordinary carrier keeps unrelated Store failure best-effort", () => {
  const fixture = ordinaryMutationFixture();
  const error = new VcsProcessTimeoutError({ operation: "test.capture", command: "git",
    cwd: fixture.scope.cwd, timeoutMs: 30000 });
  const layer = CheckpointService.layer.pipe(Layer.provide(Layer.mergeAll(IdAllocator.layer,
    Layer.mock(CheckpointStore.CheckpointStore)({ isGitRepository: () => Effect.succeed(true),
      captureCheckpoint: () => Effect.fail(error) }))));
  return Effect.gen(function* () {
    const checkpoints = yield* CheckpointService.CheckpointServiceV2;
    const actual = yield* checkpoints.capture({ scope: fixture.scope, runId: fixture.scope.runId,
      nodeId: fixture.scope.nodeId!, ordinalWithinScope: 1, appRunOrdinal: 1, capturedAt: fixture.now });
    assert.equal(actual.status, "error");
  }).pipe(Effect.provide(layer));
});
