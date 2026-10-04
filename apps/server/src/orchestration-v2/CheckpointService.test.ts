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
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RunId,
  ThreadId,
  type OrchestrationV2CheckpointScope,
  VcsProcessTimeoutError,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Fiber from "effect/Fiber";

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
  const birth = {
    kind: "application_v2_thread_birth" as const,
    threadId,
    eventId: EventId.make("event:ordinary-birth"),
    sequence: 1,
  };
  const lease = {
    resourcePath: "/repo",
    leaseId: "lease:ordinary-checkpoint",
    ownerThreadId: threadId,
    ownerIncarnation: OrdinaryCheckout.ordinaryApplicationIncarnationV1(birth),
    branch: "feature",
    acquiredAtMs: 1,
    renewedAtMs: 1,
    expiresAtMs: 9999999999999,
  };
  const canonicalCommand = { commandId, type: "run.start", threadId };
  const capture = {
    version: 1 as const,
    commandId,
    commandType: canonicalCommand.type,
    canonicalCommand,
    commandDigest: OrdinaryCheckout.ordinaryCheckoutCommandDigestV1(canonicalCommand),
    origin: { kind: "command" as const },
    threadId,
    applicationBirth: birth,
    projectId: ProjectId.make("project:ordinary-checkpoint"),
    canonicalProjectRoot: "/repo",
    canonicalCheckoutPath: "/repo",
    branch: "feature",
    lease,
  };
  const admission: OrdinaryCheckout.OrdinaryCheckoutAdmissionV1 = {
    version: 1,
    admissionId: OrdinaryCheckout.ordinaryCheckoutAdmissionIdV1(capture),
    capture,
    receipt: {
      commandId,
      threadId,
      commandType: capture.commandType,
      acceptedAt: now,
      resultSequence: 2,
      status: "accepted",
      error: null,
    },
    eventBasis: [
      {
        eventId: EventId.make("event:ordinary-run"),
        sequence: 2,
        threadId,
        commandId,
        eventType: "run.created",
      },
    ],
    run: {
      runId,
      runAttemptId: "attempt:ordinary-checkpoint",
      nodeId,
      messageId: MessageId.make("message:ordinary-checkpoint"),
    },
    recordedAt: now,
  };
  const reference = OrdinaryCheckout.ordinaryCheckoutAdmissionRefV1(admission);
  const use: OrdinaryCheckout.OrdinaryCheckoutUseV1 = {
    version: 1,
    kind: "ordinary_checkout_use",
    operationId: "operation:ordinary-checkpoint",
    admission: reference,
    lease,
    source: {
      kind: "outbox",
      workerId: "worker:ordinary-checkpoint",
      expectedAttempt: 1,
      leaseExpiresAt: DateTime.add(now, { hours: 1 }),
      link: {
        version: 1,
        effectId: "effect:ordinary-checkpoint",
        commandId,
        threadId,
        requestSha256: "a".repeat(64),
        admission: reference,
        recordedAt: now,
      },
    },
  };
  const scope: OrchestrationV2CheckpointScope = {
    id: CheckpointScopeId.make("scope:ordinary-checkpoint"),
    threadId,
    runId,
    nodeId,
    parentScopeId: null,
    providerThreadId: ProviderThreadId.make("provider-thread:ordinary-checkpoint"),
    kind: "root_run",
    ordinalWithinParent: 0,
    advancesAppRunCount: true,
    cwd: "/repo",
    createdAt: now,
  };
  const checkpoint = {
    id: CheckpointId.make("checkpoint:ordinary-checkpoint"),
    threadId,
    scopeId: scope.id,
    runId,
    nodeId,
    parentCheckpointId: null,
    ordinalWithinScope: 1,
    appRunOrdinal: 1,
    ref: CheckpointService.checkpointRefForScopeOrdinal({
      scopeId: scope.id,
      ordinalWithinScope: 1,
    }),
    status: "ready" as const,
    files: [],
    capturedAt: now,
  };
  return { now, admission, use, scope, checkpoint };
}

const ordinaryCheckpointMutations = ["baseline", "capture", "restore", "prune"] as const;
for (const mutation of ordinaryCheckpointMutations) {
  it.effect.each(["allowed", "birth", "lease", "effect", "unknown"] as const)(
    `ordinary ${mutation} revalidation=%s`,
    (outcome) =>
      Effect.gen(function* () {
        const fixture = ordinaryMutationFixture();
        const calls: string[] = [];
        const actualUse: OrdinaryCheckout.OrdinaryCheckoutUseV1 =
          outcome === "birth"
            ? {
                ...fixture.use,
                lease: { ...fixture.use.lease, ownerIncarnation: "superseded-birth" },
              }
            : outcome === "lease"
              ? { ...fixture.use, lease: { ...fixture.use.lease, leaseId: "superseded-lease" } }
              : outcome === "effect" && fixture.use.source.kind === "outbox"
                ? {
                    ...fixture.use,
                    source: {
                      ...fixture.use.source,
                      link: { ...fixture.use.source.link, effectId: "unclaimed-effect" },
                    },
                  }
                : fixture.use;
        const rejected = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
          reason:
            outcome === "birth"
              ? "stale_admission"
              : outcome === "effect"
                ? "claim_mismatch"
                : outcome === "lease"
                  ? "target_changed"
                  : "unknown_use",
          threadId: fixture.scope.threadId,
          path: fixture.scope.cwd,
          message: "Durable ownership revalidation rejected entry.",
        });
        const sinkLayer = Layer.mock(EventSink.EventSinkV2)({
          readOrdinaryCheckoutAdmissionForRun: (input) =>
            Effect.sync(() => {
              assert.deepEqual(input, {
                threadId: fixture.scope.threadId,
                runId: fixture.scope.runId,
              });
              calls.push("read-admission");
              return fixture.admission;
            }),
          revalidateOrdinaryCheckoutUse: (actual) =>
            Effect.suspend(() => {
              assert.strictEqual(actual, actualUse);
              calls.push("revalidate");
              return outcome === "allowed"
                ? Effect.succeed({
                    subject: {
                      schema: "t3.ordinary-checkout-use/v1" as const,
                      use: actual,
                      source: { projectWorkspaceRoot: "/repo", worktreePath: null },
                    },
                    state: "started" as const,
                    startedAt: DateTime.formatIso(fixture.now),
                  })
                : Effect.fail(rejected);
            }),
        });
        const mutate = () =>
          Effect.sync(() => {
            calls.push("mutate");
          });
        const storeLayer = Layer.mock(CheckpointStore.CheckpointStore)({
          isGitRepository: () =>
            Effect.sync(() => {
              calls.push("repository-read");
              return true;
            }),
          hasCheckpointRef: () =>
            Effect.sync(() => {
              calls.push("ref-read");
              return false;
            }),
          captureCheckpoint: mutate,
          restoreCheckpoint: () => mutate().pipe(Effect.as(true)),
          deleteCheckpointRefs: mutate,
        });
        const testLayer = Layer.mergeAll(
          sinkLayer,
          CheckpointService.layer.pipe(
            Layer.provide(Layer.mergeAll(IdAllocator.layer, storeLayer)),
          ),
        );
        yield* Effect.gen(function* () {
          const checkpoints = yield* CheckpointService.CheckpointServiceV2;
          const input = { scope: fixture.scope, ordinaryCheckoutUse: actualUse };
          const operation =
            mutation === "baseline"
              ? checkpoints.captureBaseline({ ...input, ordinalWithinScope: 0 })
              : mutation === "capture"
                ? checkpoints.capture({
                    ...input,
                    runId: fixture.scope.runId,
                    nodeId: fixture.scope.nodeId!,
                    ordinalWithinScope: 1,
                    appRunOrdinal: 1,
                    capturedAt: fixture.now,
                  })
                : mutation === "restore"
                  ? checkpoints.restore({ ...input, checkpoint: fixture.checkpoint })
                  : checkpoints.deleteStaleRefs({ ...input, checkpoints: [fixture.checkpoint] });
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
  (reason) =>
    Effect.gen(function* () {
      const fixture = ordinaryMutationFixture();
      const capture = vi.fn(() => Effect.void);
      const revalidate = vi.fn(() =>
        Effect.die("A rejected target must never enter checkout use."),
      );
      const sink = Layer.mock(EventSink.EventSinkV2)({
        readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(null),
        revalidateOrdinaryCheckoutUse: revalidate,
      });
      const layer = CheckpointService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            Layer.mock(CheckpointStore.CheckpointStore)({
              isGitRepository: () => Effect.succeed(true),
              hasCheckpointRef: () => Effect.succeed(false),
              captureCheckpoint: capture,
            }),
          ),
        ),
      );
      const provided = reason === "missing-service" ? layer : Layer.merge(layer, sink);
      const ordinaryCheckoutUse =
        reason === "wrong-target"
          ? { ...fixture.use, lease: { ...fixture.use.lease, resourcePath: "/different-checkout" } }
          : fixture.use;
      yield* Effect.gen(function* () {
        const checkpoints = yield* CheckpointService.CheckpointServiceV2;
        const error = yield* checkpoints
          .capture({
            scope: fixture.scope,
            ordinaryCheckoutUse,
            runId: fixture.scope.runId,
            nodeId: fixture.scope.nodeId!,
            ordinalWithinScope: 1,
            appRunOrdinal: 1,
            capturedAt: fixture.now,
          })
          .pipe(Effect.flip);
        assert.instanceOf(error, OrdinaryCheckout.OrdinaryCheckoutOwnershipError);
        assert.equal(capture.mock.calls.length, 0);
        assert.equal(revalidate.mock.calls.length, 0);
      }).pipe(Effect.provide(provided));
    }),
);

it.effect("capture without an ordinary carrier keeps unrelated Store failure best-effort", () => {
  const fixture = ordinaryMutationFixture();
  const error = new VcsProcessTimeoutError({
    operation: "test.capture",
    command: "git",
    cwd: fixture.scope.cwd,
    timeoutMs: 30000,
  });
  const layer = CheckpointService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        IdAllocator.layer,
        Layer.mock(CheckpointStore.CheckpointStore)({
          isGitRepository: () => Effect.succeed(true),
          captureCheckpoint: () => Effect.fail(error),
        }),
      ),
    ),
  );
  return Effect.gen(function* () {
    const checkpoints = yield* CheckpointService.CheckpointServiceV2;
    const actual = yield* checkpoints.capture({
      scope: fixture.scope,
      runId: fixture.scope.runId,
      nodeId: fixture.scope.nodeId!,
      ordinalWithinScope: 1,
      appRunOrdinal: 1,
      capturedAt: fixture.now,
    });
    assert.equal(actual.status, "error");
  }).pipe(Effect.provide(layer));
});

for (const mutation of ordinaryCheckpointMutations) {
  it.effect.each(["allowed", "lost-claim", "lost-capture"] as const)(
    `lifetime ${mutation} requesting executor=%s`,
    (outcome) =>
      Effect.gen(function* () {
        const fixture = ordinaryMutationFixture();
        if (fixture.use.source.kind !== "outbox")
          throw new Error("Fixture requires an outbox source.");
        const source = {
          ...fixture.use.source,
          workerId: "worker:joined-checkpoint",
          expectedAttempt: 2,
          link: { ...fixture.use.source.link, effectId: "effect:joined-checkpoint" },
        };
        const execution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
          originalUse: fixture.use,
          executor: { kind: "actual_outbox_claim", source },
        });
        const rejection = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
          reason: outcome === "lost-claim" ? "claim_mismatch" : "unknown_use",
          threadId: fixture.scope.threadId,
          path: fixture.scope.cwd,
          message: "The requesting executor is no longer current.",
        });
        const order: string[] = [];
        const revalidate = vi.fn((actual: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1) =>
          Effect.suspend(() => {
            assert.strictEqual(actual, execution);
            order.push("executor");
            return outcome === "allowed" ? Effect.succeed(actual) : Effect.fail(rejection);
          }),
        );
        const mutate = vi.fn(() =>
          Effect.sync(() => {
            order.push("native");
          }),
        );
        const testLayer = Layer.mergeAll(
          Layer.mock(EventSink.EventSinkV2)({
            readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(fixture.admission),
            revalidateOrdinaryCheckoutUse: () =>
              Effect.die("A transferred original actor cannot authorize its joined claimant."),
            revalidateOrdinaryCheckoutExecution: revalidate,
          }),
          CheckpointService.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                IdAllocator.layer,
                Layer.mock(CheckpointStore.CheckpointStore)({
                  isGitRepository: () => Effect.succeed(true),
                  hasCheckpointRef: () => Effect.succeed(false),
                  captureCheckpoint: mutate,
                  restoreCheckpoint: () => mutate().pipe(Effect.as(true)),
                  deleteCheckpointRefs: mutate,
                }),
              ),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const checkpoints = yield* CheckpointService.CheckpointServiceV2;
          const input = { scope: fixture.scope, ordinaryCheckoutExecution: execution };
          const operation =
            mutation === "baseline"
              ? checkpoints.captureBaseline({ ...input, ordinalWithinScope: 0 })
              : mutation === "capture"
                ? checkpoints.capture({
                    ...input,
                    runId: fixture.scope.runId,
                    nodeId: fixture.scope.nodeId!,
                    ordinalWithinScope: 1,
                    appRunOrdinal: 1,
                    capturedAt: fixture.now,
                  })
                : mutation === "restore"
                  ? checkpoints.restore({ ...input, checkpoint: fixture.checkpoint })
                  : checkpoints.deleteStaleRefs({ ...input, checkpoints: [fixture.checkpoint] });
          if (outcome === "allowed") {
            yield* operation;
            assert.deepEqual(order, ["executor", "native"]);
          } else {
            assert.strictEqual(yield* operation.pipe(Effect.flip), rejection);
            assert.deepEqual(order, ["executor"]);
          }
          assert.equal(revalidate.mock.calls.length, 1);
        }).pipe(Effect.provide(testLayer));
      }),
  );
}

it.effect(
  "lifetime mutation rejects a mismatched original use before executor or Store entry",
  () => {
    const fixture = ordinaryMutationFixture();
    if (fixture.use.source.kind !== "outbox") throw new Error("Fixture requires an outbox source.");
    const execution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
      originalUse: fixture.use,
      executor: { kind: "actual_outbox_claim", source: fixture.use.source },
    });
    const revalidate = vi.fn(() =>
      Effect.die("Mismatched original use must not enter the executor."),
    );
    const native = vi.fn(() => Effect.void);
    const layer = Layer.mergeAll(
      Layer.mock(EventSink.EventSinkV2)({ revalidateOrdinaryCheckoutExecution: revalidate }),
      CheckpointService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            Layer.mock(CheckpointStore.CheckpointStore)({ deleteCheckpointRefs: native }),
          ),
        ),
      ),
    );
    return Effect.gen(function* () {
      const service = yield* CheckpointService.CheckpointServiceV2;
      const error = yield* service
        .deleteStaleRefs({
          scope: fixture.scope,
          checkpoints: [fixture.checkpoint],
          ordinaryCheckoutExecution: execution,
          ordinaryCheckoutUse: {
            ...fixture.use,
            lease: { ...fixture.use.lease, ownerIncarnation: "another-birth" },
          },
        })
        .pipe(Effect.flip);
      assert.instanceOf(error, OrdinaryCheckout.OrdinaryCheckoutOwnershipError);
      assert.equal(revalidate.mock.calls.length, 0);
      assert.equal(native.mock.calls.length, 0);
    }).pipe(Effect.provide(layer));
  },
);

it.effect.each(["same-scope", "wrong-scope"] as const)(
  "managed checkpoint actor %s without invented runtime generation",
  (target) => {
    const fixture = ordinaryMutationFixture();
    const execution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
      originalUse: fixture.use,
      executor: {
        kind: "captured_managed_run",
        captureId: "capture:actual-managed-run",
        run: fixture.admission.run!,
        checkpointScopeId:
          target === "same-scope" ? fixture.scope.id : CheckpointScopeId.make("scope:foreign"),
        driver: ProviderDriverKind.make("codex"),
        binding: {
          threadId: fixture.scope.threadId,
          providerThreadId: fixture.scope.providerThreadId!,
          providerSessionId: ProviderSessionId.make("session:actual-managed"),
          instanceId: ProviderInstanceId.make("actual-managed"),
        },
      },
    });
    const validate = vi.fn((actual: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1) => {
      assert.strictEqual(actual, execution);
      assert.isFalse("runtimeGeneration" in actual.executor);
      return Effect.succeed(actual);
    });
    const native = vi.fn(() => Effect.void);
    const layer = Layer.mergeAll(
      Layer.mock(EventSink.EventSinkV2)({ revalidateOrdinaryCheckoutExecution: validate }),
      CheckpointService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            Layer.mock(CheckpointStore.CheckpointStore)({ deleteCheckpointRefs: native }),
          ),
        ),
      ),
    );
    return Effect.gen(function* () {
      const service = yield* CheckpointService.CheckpointServiceV2;
      const operation = service.deleteStaleRefs({
        scope: fixture.scope,
        checkpoints: [fixture.checkpoint],
        ordinaryCheckoutExecution: execution,
      });
      if (target === "same-scope") {
        yield* operation;
        assert.equal(native.mock.calls.length, 1);
        assert.equal(validate.mock.calls.length, 1);
      } else {
        assert.instanceOf(
          yield* operation.pipe(Effect.flip),
          OrdinaryCheckout.OrdinaryCheckoutOwnershipError,
        );
        assert.equal(native.mock.calls.length, 0);
        assert.equal(validate.mock.calls.length, 0);
      }
    }).pipe(Effect.provide(layer));
  },
);

it.effect("executor lost while waiting for workspace lock cannot restore afterward", () =>
  Effect.gen(function* () {
    const fixture = ordinaryMutationFixture();
    if (fixture.use.source.kind !== "outbox") throw new Error("Fixture requires an outbox source.");
    const execution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
      originalUse: fixture.use,
      executor: { kind: "actual_outbox_claim", source: fixture.use.source },
    });
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let lost = false;
    const reject = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
      reason: "claim_mismatch",
      threadId: fixture.scope.threadId,
      path: fixture.scope.cwd,
      message: "Claim changed while the second mutation waited for the workspace lock.",
    });
    const restore = vi.fn(() => Effect.succeed(true));
    const validate = vi.fn((ref: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1) =>
      Effect.suspend(() => (lost ? Effect.fail(reject) : Effect.succeed(ref))),
    );
    const layer = Layer.mergeAll(
      Layer.mock(EventSink.EventSinkV2)({ revalidateOrdinaryCheckoutExecution: validate }),
      CheckpointService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            Layer.mock(CheckpointStore.CheckpointStore)({
              isGitRepository: () => Effect.succeed(true),
              hasCheckpointRef: () => Effect.succeed(false),
              captureCheckpoint: () =>
                Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
              restoreCheckpoint: restore,
            }),
          ),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      const service = yield* CheckpointService.CheckpointServiceV2;
      const first = yield* service
        .captureBaseline({
          scope: fixture.scope,
          ordinalWithinScope: 0,
          ordinaryCheckoutExecution: execution,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const second = yield* service
        .restore({
          scope: fixture.scope,
          checkpoint: fixture.checkpoint,
          ordinaryCheckoutExecution: execution,
        })
        .pipe(Effect.flip, Effect.forkChild);
      yield* Effect.yieldNow;
      assert.equal(
        validate.mock.calls.length,
        1,
        "The waiting mutation must not authorize before taking the workspace lock.",
      );
      lost = true;
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      assert.strictEqual(yield* Fiber.join(second), reject);
      assert.equal(restore.mock.calls.length, 0);
      assert.equal(validate.mock.calls.length, 2);
    }).pipe(Effect.provide(layer));
  }),
);

for (const mutation of ["baseline", "capture", "restore", "prune"] as const) {
  it.effect.each(["current", "lost-before-first", "lost-between-native-calls"] as const)(
    `deep ${mutation} callback checks actual participant %s`,
    (loss) =>
      Effect.gen(function* () {
        const fixture = ordinaryMutationFixture();
        if (fixture.use.source.kind !== "outbox")
          throw new Error("Fixture requires an outbox source.");
        const execution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
          originalUse: fixture.use,
          executor: { kind: "actual_outbox_claim", source: fixture.use.source },
        });
        const error = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
          reason: "claim_mismatch",
          threadId: fixture.scope.threadId,
          path: fixture.scope.cwd,
          message: "Original claim was lost at the native mutation seam.",
        });
        let current = true;
        const nativeCalls: string[] = [];
        const revalidate = vi.fn((actual: OrdinaryCheckout.OrdinaryCheckoutExecutionRefV1) =>
          Effect.suspend(() => {
            assert.strictEqual(actual, execution);
            return current ? Effect.succeed(actual) : Effect.fail(error);
          }),
        );
        const simulateDriver = <E, R>(input: {
          readonly revalidateMutation?: Effect.Effect<void, E, R>;
        }) =>
          Effect.gen(function* () {
            assert.isDefined(input.revalidateMutation);
            if (loss === "lost-before-first") current = false;
            yield* input.revalidateMutation!;
            nativeCalls.push("first");
            if (loss === "lost-between-native-calls") current = false;
            yield* input.revalidateMutation!;
            nativeCalls.push("second");
          });
        const layer = Layer.mergeAll(
          Layer.mock(EventSink.EventSinkV2)({
            readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(fixture.admission),
            revalidateOrdinaryCheckoutExecution: revalidate,
            revalidateOrdinaryCheckoutUse: () =>
              Effect.die("Deep paths cannot switch to the original transferred actor."),
          }),
          CheckpointService.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                IdAllocator.layer,
                Layer.mock(CheckpointStore.CheckpointStore)({
                  isGitRepository: () => Effect.succeed(true),
                  hasCheckpointRef: () => Effect.succeed(false),
                  captureCheckpoint: <E, R>(input: CheckpointStore.CaptureCheckpointInput<E, R>) =>
                    simulateDriver(input),
                  restoreCheckpoint: <E, R>(input: CheckpointStore.RestoreCheckpointInput<E, R>) =>
                    simulateDriver(input).pipe(Effect.as(true)),
                  deleteCheckpointRefs: <E, R>(
                    input: CheckpointStore.DeleteCheckpointRefsInput<E, R>,
                  ) => simulateDriver(input),
                }),
              ),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const service = yield* CheckpointService.CheckpointServiceV2;
          const input = { scope: fixture.scope, ordinaryCheckoutExecution: execution };
          const effect =
            mutation === "baseline"
              ? service.captureBaseline({ ...input, ordinalWithinScope: 0 })
              : mutation === "capture"
                ? service.capture({
                    ...input,
                    runId: fixture.scope.runId,
                    nodeId: fixture.scope.nodeId!,
                    ordinalWithinScope: 1,
                    appRunOrdinal: 1,
                    capturedAt: fixture.now,
                  })
                : mutation === "restore"
                  ? service.restore({ ...input, checkpoint: fixture.checkpoint })
                  : service.deleteStaleRefs({ ...input, checkpoints: [fixture.checkpoint] });
          if (loss === "current") {
            yield* effect;
            assert.deepEqual(nativeCalls, ["first", "second"]);
          } else {
            assert.strictEqual(yield* effect.pipe(Effect.flip), error);
            assert.deepEqual(nativeCalls, loss === "lost-before-first" ? [] : ["first"]);
          }
          assert.equal(revalidate.mock.calls.length, loss === "lost-before-first" ? 2 : 3);
        }).pipe(Effect.provide(layer));
      }),
  );
}

it.effect.each([
  "before-close",
  "closed",
  "lost-after-lock-wait",
  "lost-in-driver",
  "wrong-scope",
  "wrong-execution",
] as const)("final physical capture validates native completion basis, %s", (state) =>
  Effect.gen(function* () {
    const fixture = ordinaryMutationFixture();
    if (fixture.use.source.kind !== "outbox") throw new Error("Fixture requires an outbox source.");
    const execution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
      originalUse: fixture.use,
      executor: { kind: "actual_outbox_claim", source: fixture.use.source },
    });
    const managedExecution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
      originalUse: fixture.use,
      executor: {
        kind: "captured_managed_run",
        captureId: "capture:last-native-cohort",
        run: fixture.admission.run!,
        checkpointScopeId: fixture.scope.id,
        driver: ProviderDriverKind.make("codex"),
        binding: {
          threadId: fixture.scope.threadId,
          providerThreadId: fixture.scope.providerThreadId!,
          instanceId: ProviderInstanceId.make("last-native-cohort"),
          providerSessionId: ProviderSessionId.make("session:last-native-cohort"),
        },
      },
    });
    const basis: EventSink.OrdinaryFinalCheckpointCompletionBasisV1 = {
      version: 1,
      schema: "t3.ordinary-final-checkpoint-basis/v1",
      checkpointExecution: execution,
      effectId: fixture.use.source.link.effectId,
      runId: fixture.scope.runId!,
      scopeId:
        state === "wrong-scope"
          ? CheckpointScopeId.make("scope:foreign-final-capture")
          : fixture.scope.id,
      joinOrdinal: 2,
      managedRetirements: [
        { managedExecution, retirementOrdinal: 3, closureSha256: "e".repeat(64) },
      ],
    };
    const rejection = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
      reason: "unknown_use",
      threadId: fixture.scope.threadId,
      path: fixture.scope.cwd,
      message: "Final native cohort or joined checkpoint basis is no longer qualified.",
    });
    let current = state !== "before-close";
    const order: string[] = current ? ["last-background-write", "atomic-join-and-retirement"] : [];
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const blockingRef = CheckpointService.checkpointRefForScopeOrdinal({
      scopeId: fixture.scope.id,
      ordinalWithinScope: 99,
    });
    const native = vi.fn(() => {
      order.push("physical-capture");
    });
    const revalidate = vi.fn((actual: EventSink.OrdinaryFinalCheckpointCompletionBasisV1) =>
      Effect.suspend(() => {
        assert.strictEqual(actual, basis);
        order.push("basis");
        return current ? Effect.succeed(actual) : Effect.fail(rejection);
      }),
    );
    const layer = Layer.mergeAll(
      Layer.mock(EventSink.EventSinkV2)({
        readOrdinaryCheckoutAdmissionForRun: () => Effect.succeed(fixture.admission),
        revalidateOrdinaryFinalCheckpointBasis: revalidate,
        revalidateOrdinaryCheckoutExecution: (actual) => Effect.succeed(actual),
        revalidateOrdinaryCheckoutUse: () =>
          Effect.die("Final capture must retain the real joined actor."),
      }),
      CheckpointService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            Layer.mock(CheckpointStore.CheckpointStore)({
              isGitRepository: () => Effect.succeed(true),
              hasCheckpointRef: () => Effect.succeed(false),
              captureCheckpoint: <E, R>(input: CheckpointStore.CaptureCheckpointInput<E, R>) =>
                input.checkpointRef === blockingRef
                  ? Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    )
                  : Effect.gen(function* () {
                      assert.isDefined(input.revalidateMutation);
                      if (state === "lost-in-driver") current = false;
                      yield* input.revalidateMutation!;
                      native();
                    }),
            }),
          ),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      const service = yield* CheckpointService.CheckpointServiceV2;
      const blocker =
        state === "lost-after-lock-wait"
          ? yield* Effect.forkChild(
              service.captureBaseline({ scope: fixture.scope, ordinalWithinScope: 99 }),
            )
          : undefined;
      if (blocker !== undefined) yield* Deferred.await(entered);
      if (fixture.use.source.kind !== "outbox")
        return yield* Effect.die("Final checkpoint fixture requires its original outbox source");
      const operation = service.capture({
        scope: fixture.scope,
        ordinaryCheckoutExecution:
          state === "wrong-execution"
            ? OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
                originalUse: fixture.use,
                executor: {
                  kind: "actual_outbox_claim",
                  source: { ...fixture.use.source, workerId: "worker:foreign-final-checkpoint" },
                },
              })
            : execution,
        ordinaryFinalCheckpointBasis: basis,
        runId: fixture.scope.runId,
        nodeId: fixture.scope.nodeId!,
        ordinalWithinScope: 1,
        appRunOrdinal: 1,
        capturedAt: fixture.now,
      });
      if (blocker !== undefined) {
        const queued = yield* Effect.forkChild(operation.pipe(Effect.exit));
        yield* Effect.yieldNow;
        assert.equal(
          revalidate.mock.calls.length,
          1,
          "The before-entry basis check occurs before waiting on the workspace lock.",
        );
        current = false;
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(blocker);
        const exit = yield* Fiber.join(queued);
        assert.isTrue(Exit.isFailure(exit));
        assert.equal(
          revalidate.mock.calls.length,
          2,
          "Lock acquisition must recheck the original basis.",
        );
      } else if (state === "closed") {
        const checkpoint = yield* operation;
        assert.equal(checkpoint.status, "ready");
        assert.deepEqual(order, [
          "last-background-write",
          "atomic-join-and-retirement",
          "basis",
          "basis",
          "basis",
          "physical-capture",
        ]);
      } else {
        const error = yield* operation.pipe(Effect.flip);
        if (state === "wrong-scope" || state === "wrong-execution") {
          assert.instanceOf(error, OrdinaryCheckout.OrdinaryCheckoutOwnershipError);
          assert.equal(revalidate.mock.calls.length, 0);
        } else assert.strictEqual(error, rejection);
      }
      assert.equal(native.mock.calls.length, state === "closed" ? 1 : 0);
    }).pipe(Effect.provide(layer));
  }),
);
