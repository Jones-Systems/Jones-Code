import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it, vi } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointScopeId,
  CommandId,
  EventId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ProviderThread,
  ProviderDriverKind,
  RunId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import * as Layer from "effect/Layer";

import { resolveCodexRollbackTurnCount } from "./Adapters/CodexAdapterV2.ts";
import { isCheckpointRestoreIsolated } from "./CheckpointRestoreSafety.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import {
  ProviderAdapterRollbackThreadError,
  type ProviderAdapterV2RollbackThreadInput,
} from "./ProviderAdapter.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as OrdinaryCheckout from "./OrdinaryCheckoutOwnership.ts";
import { OrchestrationEffectRequestV2 } from "./EffectOutbox.ts";
import { nativeCreationCanonicalJson, nativeCreationSha256 } from "./NativeCreationPreparation.ts";

// A root that does not exist never overlaps, so other owners decide isolation.
const unrelatedProject = Option.some({
  workspaceRoot: "/nonexistent/t3-rollback-project",
} as never);
const sourceEffect = Object.freeze({
  effectId: "effect:rollback-original:provider-thread.rollback",
  commandId: CommandId.make("rollback-original"),
});
const rollbackError = (
  error: Effect.Error<
    ReturnType<CheckpointRollbackService.CheckpointRollbackServiceV2Shape["execute"]>
  >,
) => {
  if (!Schema.is(CheckpointRollbackService.CheckpointRollbackExecutionError)(error))
    throw new Error("Expected a structured checkpoint rollback error");
  return error;
};
const birthForThread = (threadId: ThreadId) => ({
  kind: "application_v2_thread_birth" as const,
  threadId,
  eventId: EventId.make(`birth:${threadId}`),
  sequence: 1,
});
const receiptIdentityFor = (threadId: ThreadId) => ({
  receipt: {
    commandId: sourceEffect.commandId,
    threadId,
    commandType: "checkpoint.rollback",
    status: "accepted" as const,
    resultSequence: 1,
    acceptedAt: DateTime.makeUnsafe("2026-10-03T00:00:00Z"),
    error: null,
  },
  projectReceipt: null,
  identity: null,
  nativeCreationReservation: null,
  importedHistoryChoiceIdentity: null,
  currentRuntimeStopIdentity: null,
  capturedRestartOrigin: null,
  threadRecovery: null,
  threadDeletion: null,
  ordinaryCheckoutAdmissions: [],
  ordinaryCheckoutEffectLinks: [],
  ordinaryCheckoutCommands: [],
});
const checkpointRollbackServiceLayer = CheckpointRollbackService.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      NodeServices.layer,
      Layer.mock(ProjectStore.ProjectStoreV2)({ get: () => Effect.succeed(unrelatedProject) }),
    ),
  ),
);

it.effect("rejects a non-ready checkpoint before opening a session or restoring files", () => {
  const threadId = ThreadId.make("thread:rollback-non-ready");
  const providerThreadId = ProviderThreadId.make("provider-thread:rollback-non-ready");
  const providerSessionId = ProviderSessionId.make("provider-session:rollback-non-ready");
  const checkpointId = CheckpointId.make("checkpoint:rollback-non-ready");
  const scopeId = CheckpointScopeId.make("checkpoint-scope:rollback-non-ready");
  const providerInstanceId = ProviderInstanceId.make("provider_rollback_non_ready");
  const restore = vi.fn(() => Effect.die("checkpoint restore must not run"));
  const open = vi.fn(() => Effect.die("provider session open must not run"));
  const resolveRuntimePolicy = vi.fn(() => Effect.die("runtime policy resolution must not run"));
  const projection = {
    thread: {
      worktreePath: process.cwd(),
      activeProviderThreadId: providerThreadId,
      modelSelection: { instanceId: providerInstanceId, model: "test-model" },
    },
    providerThreads: [{ id: providerThreadId, providerSessionId, providerInstanceId }],
    checkpoints: [{ id: checkpointId, scopeId, status: "stale" }],
    checkpointScopes: [{ id: scopeId, cwd: process.cwd() }],
  } as unknown as OrchestrationV2ThreadProjection;
  const testLayer = checkpointRollbackServiceLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(CheckpointService.CheckpointServiceV2)({ restore }),
        Layer.mock(EventSink.EventSinkV2)({
          readAttachmentNamespaceCleanupTask: () => Effect.succeed(null),
        }),
        IdAllocator.layer,
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadRecords: () => Effect.succeed(projection),
          getShellSnapshot: () =>
            Effect.succeed({
              schemaVersion: 1,
              snapshotSequence: 0,
              threads: [],
              archivedThreads: [],
            }),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({ open }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({ resolve: resolveRuntimePolicy }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
    const error = yield* service
      .execute({
        threadId,
        providerThreadId,
        checkpointId,
        scopeId,
        sourceEffect,
      })
      .pipe(Effect.flip);

    assert.equal(rollbackError(error).reason, "rollback-target-invalid");
    assert.equal(
      error.message,
      `Rollback target ${checkpointId} for provider thread ${providerThreadId} on thread ${threadId} is incomplete or invalid.`,
    );
    assert.equal(rollbackError(error).cause, undefined);
    assert.equal(resolveRuntimePolicy.mock.calls.length, 0);
    assert.equal(open.mock.calls.length, 0);
    assert.equal(restore.mock.calls.length, 0);
  }).pipe(Effect.provide(testLayer));
});

it.effect("rejects a rollback when another provider thread became active", () => {
  const threadId = ThreadId.make("thread:rollback-inactive-provider-thread");
  const requestedProviderThreadId = ProviderThreadId.make(
    "provider-thread:rollback-inactive-provider-thread:requested",
  );
  const activeProviderThreadId = ProviderThreadId.make(
    "provider-thread:rollback-inactive-provider-thread:active",
  );
  const providerSessionId = ProviderSessionId.make(
    "provider-session:rollback-inactive-provider-thread",
  );
  const checkpointId = CheckpointId.make("checkpoint:rollback-inactive-provider-thread");
  const scopeId = CheckpointScopeId.make("checkpoint-scope:rollback-inactive-provider-thread");
  const providerInstanceId = ProviderInstanceId.make("provider_rollback_inactive_provider_thread");
  const restore = vi.fn(() => Effect.die("checkpoint restore must not run"));
  const open = vi.fn(() => Effect.die("provider session open must not run"));
  const resolveRuntimePolicy = vi.fn(() => Effect.die("runtime policy resolution must not run"));
  const projection = {
    thread: {
      activeProviderThreadId,
      modelSelection: { instanceId: providerInstanceId, model: "test-model" },
    },
    providerThreads: [
      {
        id: requestedProviderThreadId,
        providerSessionId,
        providerInstanceId,
      },
    ],
    checkpoints: [{ id: checkpointId, scopeId, status: "ready" }],
    checkpointScopes: [{ id: scopeId, cwd: process.cwd() }],
  } as unknown as OrchestrationV2ThreadProjection;
  const testLayer = checkpointRollbackServiceLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(CheckpointService.CheckpointServiceV2)({ restore }),
        Layer.mock(EventSink.EventSinkV2)({
          readAttachmentNamespaceCleanupTask: () => Effect.succeed(null),
        }),
        IdAllocator.layer,
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadRecords: () => Effect.succeed(projection),
          getShellSnapshot: () =>
            Effect.succeed({
              schemaVersion: 1,
              snapshotSequence: 0,
              threads: [],
              archivedThreads: [],
            }),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({ open }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({ resolve: resolveRuntimePolicy }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
    const error = yield* service
      .execute({
        threadId,
        providerThreadId: requestedProviderThreadId,
        checkpointId,
        scopeId,
        sourceEffect,
      })
      .pipe(Effect.flip);

    assert.equal(rollbackError(error).reason, "active-provider-changed");
    assert.equal(
      error.message,
      `Active provider changed before rollback target ${checkpointId} could execute on thread ${threadId}.`,
    );
    assert.equal(rollbackError(error).cause, undefined);
    assert.equal(resolveRuntimePolicy.mock.calls.length, 0);
    assert.equal(open.mock.calls.length, 0);
    assert.equal(restore.mock.calls.length, 0);
  }).pipe(Effect.provide(testLayer));
});

it.effect("rejects a rollback when provider selection changed before execution", () => {
  const threadId = ThreadId.make("thread:rollback-provider-selection-changed");
  const providerThreadId = ProviderThreadId.make(
    "provider-thread:rollback-provider-selection-changed",
  );
  const providerSessionId = ProviderSessionId.make(
    "provider-session:rollback-provider-selection-changed",
  );
  const checkpointId = CheckpointId.make("checkpoint:rollback-provider-selection-changed");
  const scopeId = CheckpointScopeId.make("checkpoint-scope:rollback-provider-selection-changed");
  const originalProviderInstanceId = ProviderInstanceId.make(
    "provider_rollback_provider_selection_changed_original",
  );
  const selectedProviderInstanceId = ProviderInstanceId.make(
    "provider_rollback_provider_selection_changed_selected",
  );
  const restore = vi.fn(() => Effect.die("checkpoint restore must not run"));
  const open = vi.fn(() => Effect.die("provider session open must not run"));
  const resolveRuntimePolicy = vi.fn(() => Effect.die("runtime policy resolution must not run"));
  const projection = {
    thread: {
      worktreePath: process.cwd(),
      activeProviderThreadId: providerThreadId,
      modelSelection: { instanceId: selectedProviderInstanceId, model: "test-model" },
    },
    providerThreads: [
      {
        id: providerThreadId,
        providerSessionId,
        providerInstanceId: originalProviderInstanceId,
      },
    ],
    checkpoints: [{ id: checkpointId, scopeId, status: "ready" }],
    checkpointScopes: [{ id: scopeId, cwd: process.cwd() }],
  } as unknown as OrchestrationV2ThreadProjection;
  const testLayer = checkpointRollbackServiceLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(CheckpointService.CheckpointServiceV2)({ restore }),
        Layer.mock(EventSink.EventSinkV2)({
          readAttachmentNamespaceCleanupTask: () => Effect.succeed(null),
        }),
        IdAllocator.layer,
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadRecords: () => Effect.succeed(projection),
          getShellSnapshot: () =>
            Effect.succeed({
              schemaVersion: 1,
              snapshotSequence: 0,
              threads: [],
              archivedThreads: [],
            }),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({ open }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({ resolve: resolveRuntimePolicy }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
    const error = yield* service
      .execute({
        threadId,
        providerThreadId,
        checkpointId,
        scopeId,
        sourceEffect,
      })
      .pipe(Effect.flip);

    assert.equal(rollbackError(error).reason, "active-provider-changed");
    assert.equal(
      error.message,
      `Active provider changed before rollback target ${checkpointId} could execute on thread ${threadId}.`,
    );
    assert.equal(rollbackError(error).cause, undefined);
    assert.equal(resolveRuntimePolicy.mock.calls.length, 0);
    assert.equal(open.mock.calls.length, 0);
    assert.equal(restore.mock.calls.length, 0);
  }).pipe(Effect.provide(testLayer));
});

it.effect("reports a missing provider turn as a structured rollback failure", () => {
  const threadId = ThreadId.make("thread:rollback-provider-turn-unavailable");
  const providerThreadId = ProviderThreadId.make(
    "provider-thread:rollback-provider-turn-unavailable",
  );
  const providerSessionId = ProviderSessionId.make(
    "provider-session:rollback-provider-turn-unavailable",
  );
  const checkpointId = CheckpointId.make("checkpoint:rollback-provider-turn-unavailable");
  const scopeId = CheckpointScopeId.make("checkpoint-scope:rollback-provider-turn-unavailable");
  const providerInstanceId = ProviderInstanceId.make("provider_rollback_provider_turn_unavailable");
  const restore = vi.fn(() => Effect.die("checkpoint restore must not run"));
  const projection = {
    thread: {
      worktreePath: process.cwd(),
      activeProviderThreadId: providerThreadId,
      modelSelection: { instanceId: providerInstanceId, model: "test-model" },
    },
    providerThreads: [{ id: providerThreadId, providerSessionId, providerInstanceId }],
    providerSessions: [],
    checkpoints: [{ id: checkpointId, scopeId, status: "ready", appRunOrdinal: 1 }],
    checkpointScopes: [{ id: scopeId, cwd: process.cwd() }],
    runs: [],
    attempts: [],
    providerTurns: [],
  } as unknown as OrchestrationV2ThreadProjection;
  const testLayer = checkpointRollbackServiceLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(CheckpointService.CheckpointServiceV2)({ restore }),
        Layer.mock(EventSink.EventSinkV2)({
          readAttachmentNamespaceCleanupTask: () => Effect.succeed(null),
          readApplicationBirthRecord: (id) => Effect.succeed(birthForThread(id)),
          readCommandReceiptIdentity: () => Effect.succeed(receiptIdentityFor(threadId)),
          readOrdinaryCheckoutEffectLink: () => Effect.succeed(null),
        }),
        IdAllocator.layer,
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadRecords: () => Effect.succeed(projection),
          getShellSnapshot: () =>
            Effect.succeed({
              schemaVersion: 1,
              snapshotSequence: 0,
              threads: [],
              archivedThreads: [],
            }),
        }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          open: () => Effect.succeed({} as never),
        }),
        Layer.mock(RuntimePolicy.RuntimePolicyV2)({
          resolve: () => Effect.succeed({} as never),
        }),
      ),
    ),
  );

  return Effect.gen(function* () {
    const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
    const error = yield* service
      .execute({
        threadId,
        providerThreadId,
        checkpointId,
        scopeId,
        sourceEffect,
      })
      .pipe(Effect.flip);

    assert.equal(rollbackError(error).reason, "provider-turn-unavailable");
    assert.equal(
      error.message,
      `Provider turn for rollback target ${checkpointId} is unavailable on provider thread ${providerThreadId}.`,
    );
    assert.equal(rollbackError(error).cause, undefined);
    assert.equal(restore.mock.calls.length, 0);
  }).pipe(Effect.provide(testLayer));
});

it.effect.each([
  { restoreFiles: true, shared: "none" },
  { restoreFiles: false, shared: "root" },
  { restoreFiles: true, shared: "root" },
  { restoreFiles: true, shared: "worktree" },
  { restoreFiles: false, shared: "worktree" },
  { restoreFiles: true, shared: "historical" },
  { restoreFiles: false, shared: "none", targetOrdinal: 1 },
  { restoreFiles: false, shared: "none", failure: "provider" },
  { restoreFiles: true, shared: "none", failure: "files" },
  { restoreFiles: false, shared: "none", failure: "birth" },
  { restoreFiles: false, shared: "none", failure: "source" },
  { restoreFiles: false, shared: "none", failure: "task_unknown" },
  { restoreFiles: false, shared: "none", failure: "task_read_failure" },
  { restoreFiles: false, shared: "none", failure: "receipt" },
  { restoreFiles: false, shared: "none", failure: "new_birth" },
  { restoreFiles: true, shared: "none", replay: "lost_response" },
  { restoreFiles: true, shared: "none", ordinary: "joined" },
  { restoreFiles: false, shared: "none", ordinary: "joined" },
  { restoreFiles: false, shared: "none", ordinary: "missing" },
  { restoreFiles: false, shared: "none", ordinary: "wrong_claim" },
  { restoreFiles: false, shared: "none", ordinary: "wrong_request" },
  { restoreFiles: false, shared: "none", ordinary: "lost_before_open" },
  { restoreFiles: false, shared: "none", ordinary: "lost_before_rollback" },
  { restoreFiles: true, shared: "none", ordinary: "lost_after_lock" },
  { restoreFiles: false, shared: "none", ordinary: "joined", pruneRefs: true },
  { restoreFiles: false, shared: "none", ordinary: "lost_at_refs", pruneRefs: true },
  { restoreFiles: false, shared: "none", ordinary: "joined", completion: "unqualified" },
  { restoreFiles: true, shared: "none", ordinary: "joined", replay: "lost_response" },
])(
  "rewinds safely with %s",
  ({
    restoreFiles,
    shared,
    targetOrdinal = 0,
    failure,
    replay,
    ordinary,
    pruneRefs,
    completion,
  }) => {
    const threadId = ThreadId.make("rewind-files");
    const providerThreadId = ProviderThreadId.make("rewind-provider");
    const providerSessionId = ProviderSessionId.make("rewind-session");
    const instanceId = ProviderInstanceId.make("rewind-instance");
    const checkpointId = CheckpointId.make("rewind-start");
    const scopeId = CheckpointScopeId.make("rewind-scope");
    const calls: string[] = [];
    let opened = 0;
    let projectionReads = 0;
    let committedTask: EventSink.AttachmentNamespaceCleanupTaskV1 | null = null;
    const taskQueries: string[] = [];
    const taskReadError = new EventSink.EventSinkWriteError({
      eventCount: 0,
      commandId: sourceEffect.commandId,
      cause: "Synthetic existing task is unqualified",
    });
    const taskReadFailure = new EventSink.EventSinkWriteError({
      eventCount: 0,
      commandId: sourceEffect.commandId,
      cause: new Error("Synthetic SQL task read failed"),
    });
    const lostReplyError = new EventSink.EventSinkWriteError({
      eventCount: 0,
      commandId: sourceEffect.commandId,
      cause: "Synthetic committed completion reply was lost",
    });
    const providerThread: OrchestrationV2ProviderThread = {
      id: providerThreadId,
      providerSessionId,
      providerInstanceId: instanceId,
      driver: ProviderDriverKind.make("codex"),
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: null,
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: 1,
      lastRunOrdinal: 2,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      contextUsage: null,
      nativeMetadata: null,
      createdAt: DateTime.makeUnsafe("2026-10-03T00:00:00Z"),
      updatedAt: DateTime.makeUnsafe("2026-10-03T00:00:00Z"),
    };
    const ordinaryLink: OrdinaryCheckout.OrdinaryCheckoutEffectLinkV1 = {
      version: 1,
      effectId: sourceEffect.effectId,
      commandId: sourceEffect.commandId,
      threadId,
      admission: { version: 1, admissionId: "a".repeat(64), admissionSha256: "b".repeat(64) },
      requestSha256: nativeCreationSha256(
        nativeCreationCanonicalJson(
          Schema.encodeSync(OrchestrationEffectRequestV2)({
            type: "provider-thread.rollback",
            providerThreadId,
            checkpointId,
            scopeId,
            restoreFiles: ordinary === "wrong_request" ? !restoreFiles : restoreFiles,
          }),
        ),
      ),
      recordedAt: DateTime.makeUnsafe("2026-10-03T00:00:00Z"),
    };
    const originalUse: OrdinaryCheckout.OrdinaryCheckoutUseV1 = {
      version: 1,
      kind: "ordinary_checkout_use",
      operationId: "original-use",
      admission: ordinaryLink.admission,
      source: {
        kind: "outbox",
        link: ordinaryLink,
        workerId: "original-worker",
        expectedAttempt: 1,
        leaseExpiresAt: DateTime.makeUnsafe("2026-10-03T00:05:00Z"),
      },
      lease: {
        resourcePath: process.cwd(),
        leaseId: "original-lease",
        ownerThreadId: threadId,
        ownerIncarnation: "original-birth",
        branch: null,
        acquiredAtMs: 1,
        renewedAtMs: 1,
        expiresAtMs: 2,
      },
    };
    const execution = OrdinaryCheckout.makeOrdinaryCheckoutExecutionRefV1({
      originalUse,
      executor: {
        kind: "actual_outbox_claim",
        source: {
          kind: "outbox",
          link:
            ordinary === "wrong_claim"
              ? {
                  ...ordinaryLink,
                  effectId: "another-rollback-effect",
                }
              : ordinaryLink,
          workerId: "rollback-worker",
          expectedAttempt: 2,
          leaseExpiresAt: DateTime.makeUnsafe("2026-10-03T00:05:00Z"),
        },
      },
    });
    const ordinaryContext =
      ordinary === undefined || ordinary === "missing"
        ? {}
        : { ordinaryCheckoutExecution: execution };
    let revalidations = 0;
    let capturedRevalidations = 0;
    const ownershipLoss = new OrdinaryCheckout.OrdinaryCheckoutOwnershipError({
      reason: "claim_mismatch",
      threadId,
      path: process.cwd(),
      message: "Actual rollback claim changed",
    });
    const projection = {
      thread: {
        worktreePath: shared === "root" ? null : process.cwd(),
        activeProviderThreadId: providerThreadId,
        modelSelection: { instanceId, model: "test" },
      },
      providerThreads: [providerThread],
      providerSessions: [],
      // Turn 3 remains in the audit history after an earlier rollback.
      providerTurns: [1, 2, 3].map((ordinal) => ({
        id: `turn-${ordinal}`,
        providerThreadId,
        runAttemptId: `attempt-${ordinal}`,
        ordinal,
        status: "completed",
      })),
      nodes: [1, 2, 3].map((ordinal) => ({
        id: `node-${ordinal}`,
        runId: `run-${ordinal}`,
        threadId,
        parentNodeId: null,
        rootNodeId: `node-${ordinal}`,
        kind: "root_turn",
        status: "completed",
        countsForRun: true,
        providerThreadId,
        providerTurnId: `turn-${ordinal}`,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: scopeId,
        startedAt: null,
        completedAt: null,
      })),
      attempts: [1, 2, 3].map((ordinal) => ({ id: `attempt-${ordinal}`, runId: `run-${ordinal}` })),
      checkpoints: [
        { id: checkpointId, scopeId, status: "ready", appRunOrdinal: targetOrdinal || null },
        ...(pruneRefs
          ? [
              {
                id: CheckpointId.make("stale-later"),
                scopeId,
                status: "ready",
                appRunOrdinal: 2,
                threadId,
                runId: "run-2",
                nodeId: "node-2",
                parentCheckpointId: checkpointId,
                ordinalWithinScope: 2,
                ref: "refs/t3/later",
                files: [],
                capturedAt: DateTime.makeUnsafe("2026-10-03T00:00:00Z"),
              },
            ]
          : []),
      ],
      checkpointScopes: [{ id: scopeId, cwd: process.cwd() }],
      runs: [1, 2, 3].map((ordinal) => ({
        id: `run-${ordinal}`,
        ordinal,
        status: ordinal === 3 ? "rolled_back" : "completed",
        rootNodeId: `node-${ordinal}`,
        activeAttemptId: `attempt-${ordinal}`,
        threadId,
        providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "test" },
        providerThreadId,
        userMessageId: `message-${ordinal}`,
        requestedAt: DateTime.makeUnsafe("2026-10-03T00:00:00Z"),
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      })),
    } as unknown as OrchestrationV2ThreadProjection;
    const testLayer = checkpointRollbackServiceLayer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(CheckpointService.CheckpointServiceV2)({
            deleteStaleRefs: (input) =>
              Effect.gen(function* () {
                assert.deepEqual(input.ordinaryCheckoutExecution, execution);
                assert.deepEqual(input.ordinaryCheckoutUse, execution.originalUse);
                assert.deepEqual(
                  input.checkpoints.map((checkpoint) => checkpoint.id),
                  [CheckpointId.make("stale-later")],
                );
                if (ordinary === "lost_at_refs") return yield* ownershipLoss;
                calls.push("refs");
              }),
            restore: (input) =>
              Effect.gen(function* () {
                if (ordinary !== undefined) {
                  assert.deepEqual(input.ordinaryCheckoutExecution, execution);
                  assert.deepEqual(input.ordinaryCheckoutUse, execution.originalUse);
                  if (ordinary === "lost_after_lock") return yield* ownershipLoss;
                }
                calls.push("files");
                if (failure === "files")
                  return yield* new CheckpointService.CheckpointRestoreError({
                    scopeId: input.scope.id,
                    checkpointId: input.checkpoint.id,
                    cause: new Error("Synthetic file restore failed"),
                  });
              }),
          }),
          Layer.mock(EventSink.EventSinkV2)({
            onCommit: (effect) => effect,
            readAttachmentNamespaceCleanupTask: (effectId) =>
              Effect.gen(function* () {
                taskQueries.push(effectId);
                if (failure === "task_unknown") return yield* taskReadError;
                if (failure === "task_read_failure") return yield* taskReadFailure;
                return committedTask;
              }),
            readCommandReceiptIdentity: () =>
              Effect.sync(() => {
                const identity = receiptIdentityFor(threadId);
                return failure === "receipt"
                  ? { ...identity, receipt: { ...identity.receipt, status: "rejected" as const } }
                  : identity;
              }),
            readOrdinaryCheckoutEffectLink: () =>
              Effect.succeed(ordinary === undefined ? null : ordinaryLink),
            revalidateOrdinaryCheckoutExecution: (ref) =>
              Effect.gen(function* () {
                assert.deepEqual(ref, execution);
                revalidations++;
                if (ordinary === "lost_before_open") return yield* ownershipLoss;
                return ref;
              }),
            readApplicationBirthRecord: (id) =>
              Effect.succeed(
                failure === "birth"
                  ? null
                  : {
                      ...birthForThread(id),
                      sequence: failure === "new_birth" ? 2 : 1,
                    },
              ),
            writeWithEffects: ({ commandId, events, effects }) =>
              Effect.gen(function* () {
                assert.ok(
                  events.some(
                    (event) =>
                      event.type === "run.updated" && event.payload.status === "rolled_back",
                  ),
                );
                assert.equal(commandId, sourceEffect.commandId);
                assert.ok(commandId);
                assert.equal(effects.length, 1);
                const trigger = events.find((event) => event.type === "provider-thread.updated");
                assert.ok(trigger);
                assert.deepEqual(effects[0], {
                  id: `${sourceEffect.effectId}:attachment.cleanup:prune`,
                  commandId: sourceEffect.commandId,
                  threadId,
                  request: { type: "attachment.cleanup", attachmentIds: [] },
                  attachmentNamespaceCleanup: {
                    version: 1,
                    mode: "prune_thread",
                    ownerBirth: birthForThread(threadId),
                    triggerEventId: trigger.id,
                    rollbackEffectId: sourceEffect.effectId,
                  },
                });
                const expectedRunIds = (targetOrdinal === 0 ? ["run-1", "run-2"] : ["run-2"]).map(
                  (id) => RunId.make(id),
                );
                assert.deepEqual(
                  events
                    .filter((event) => event.type === "run.updated")
                    .map((event) => event.runId),
                  expectedRunIds,
                );
                assert.deepEqual(
                  events
                    .filter((event) => event.type === "node.updated")
                    .map((event) => event.runId),
                  expectedRunIds,
                );
                assert.isFalse(events.some((event) => event.type === "thread.deleted"));
                calls.push("projection");
                const subject = {
                  version: 1,
                  effectId: effects[0]!.id,
                  commandId,
                  threadId,
                  reference: effects[0]!.attachmentNamespaceCleanup,
                  triggerSequence: 2,
                };
                committedTask = yield* Schema.decodeUnknownEffect(
                  EventSink.AttachmentNamespaceCleanupTaskV1,
                )({
                  ...subject,
                  bindingSha256: nativeCreationSha256(nativeCreationCanonicalJson(subject)),
                }).pipe(Effect.orDie);
                if (replay === "lost_response") return yield* lostReplyError;
                return ordinary === "joined" && completion !== "unqualified"
                  ? events.map((event, index) => ({
                      commandId,
                      sequence: index + 2,
                      event,
                    }))
                  : [];
              }),
          }),
          IdAllocator.layer,
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getThreadRecords: () =>
              Effect.sync(() => {
                projectionReads++;
                return projection;
              }),
            getThreadProviderContext: () => Effect.succeed({ providerSessions: [] } as never),
            getCheckpointContext: () =>
              Effect.succeed({
                checkpointScopes: [{ cwd: process.cwd() }],
                runs: [],
                checkpoints: [],
              } as never),
            getShellSnapshot: () =>
              Effect.succeed({
                schemaVersion: 1,
                snapshotSequence: 0,
                threads: [],
                archivedThreads:
                  shared === "worktree" || shared === "historical"
                    ? [
                        {
                          id: ThreadId.make("other-thread"),
                          deletedAt: null,
                          worktreePath: shared === "worktree" ? process.cwd() : null,
                        } as never,
                      ]
                    : [],
              }),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            captureOrdinaryExecutionAttachment: ({ runtime, providerThread: capturedThread }) =>
              Effect.sync(() => {
                assert.strictEqual(capturedThread, providerThread);
                assert.ok(runtime);
                return {
                  revalidateCaptured: Effect.gen(function* () {
                    capturedRevalidations++;
                    if (ordinary === "lost_before_rollback") return yield* ownershipLoss;
                  }),
                } as never;
              }),
            open: () =>
              Effect.sync(() => {
                opened++;
                return {
                  rollbackThread: (input: ProviderAdapterV2RollbackThreadInput) =>
                    Effect.gen(function* () {
                      const count = yield* resolveCodexRollbackTurnCount(input);
                      assert.equal(count, 2 - targetOrdinal);
                      calls.push("provider");
                      if (failure === "provider")
                        return yield* new ProviderAdapterRollbackThreadError({
                          driver: providerThread.driver,
                          providerThreadId,
                          checkpointId,
                          cause: new Error("Synthetic provider rollback failed"),
                        });
                      return { providerThread };
                    }),
                } as never;
              }),
          }),
          Layer.mock(RuntimePolicy.RuntimePolicyV2)({ resolve: () => Effect.succeed({} as never) }),
        ),
      ),
    );
    return Effect.gen(function* () {
      const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
      if (ordinary !== undefined && ordinary !== "joined") {
        const error = yield* service
          .execute({
            threadId,
            providerThreadId,
            checkpointId,
            scopeId,
            restoreFiles,
            sourceEffect,
            ...ordinaryContext,
          })
          .pipe(Effect.flip);
        assert.equal(
          Schema.is(OrdinaryCheckout.OrdinaryCheckoutOwnershipError)(error)
            ? error.reason
            : rollbackError(error).reason,
          ["missing", "wrong_claim", "wrong_request"].includes(ordinary)
            ? "rollback-target-invalid"
            : "claim_mismatch",
        );
        if (ordinary.startsWith("lost_")) assert.strictEqual(error, ownershipLoss);
        assert.equal(
          opened,
          ["lost_before_rollback", "lost_after_lock", "lost_at_refs"].includes(ordinary) ? 1 : 0,
        );
        assert.deepEqual(
          calls,
          ordinary === "lost_after_lock" || ordinary === "lost_at_refs" ? ["provider"] : [],
        );
        assert.isNull(
          CheckpointRollbackService.readIssuedCheckpointRollbackObservationForExecution(execution),
        );
        return;
      }
      if (restoreFiles && shared !== "none") {
        const error = yield* Effect.flip(
          service.execute({
            threadId,
            providerThreadId,
            checkpointId,
            scopeId,
            restoreFiles,
            sourceEffect,
          }),
        );
        assert.equal(rollbackError(error).reason, "shared-workspace");
        assert.deepEqual(calls, []);
        assert.equal(opened, 0);
        return;
      }
      if (failure !== undefined) {
        const error = yield* service
          .execute({
            threadId,
            providerThreadId,
            checkpointId,
            scopeId,
            restoreFiles,
            sourceEffect: failure === "source" ? { ...sourceEffect, effectId: "" } : sourceEffect,
          })
          .pipe(Effect.flip);
        const physicalFailure = failure === "provider" || failure === "files";
        assert.equal(
          rollbackError(error).reason,
          ["birth", "receipt", "new_birth"].includes(failure)
            ? "rollback-target-invalid"
            : "unexpected-failure",
        );
        assert.deepEqual(
          calls,
          !physicalFailure ? [] : failure === "provider" ? ["provider"] : ["provider", "files"],
        );
        assert.equal(opened, physicalFailure ? 1 : 0);
        if (failure === "task_unknown" || failure === "task_read_failure") {
          assert.equal(projectionReads, 0);
          assert.strictEqual(
            rollbackError(error).cause,
            failure === "task_unknown" ? taskReadError : taskReadFailure,
          );
          assert.deepEqual(taskQueries, [`${sourceEffect.effectId}:attachment.cleanup:prune`]);
        }
        return;
      }
      if (replay === "lost_response") {
        const input = {
          threadId,
          providerThreadId,
          checkpointId,
          scopeId,
          restoreFiles,
          sourceEffect,
          ...ordinaryContext,
        };
        const error = yield* service.execute(input).pipe(Effect.flip);
        assert.equal(rollbackError(error).reason, "unexpected-failure");
        assert.strictEqual(rollbackError(error).cause, lostReplyError);
        assert.ok(committedTask);
        assert.equal(opened, 1);
        assert.equal(projectionReads, 1);
        assert.deepEqual(calls, ["provider", "files", "projection"]);
        const mismatch = yield* service
          .execute({
            ...input,
            sourceEffect: {
              ...sourceEffect,
              commandId: CommandId.make("different-rollback-command"),
            },
          })
          .pipe(Effect.flip);
        assert.equal(rollbackError(mismatch).reason, "rollback-target-invalid");
        yield* service.execute(input);
        assert.equal(opened, 1);
        assert.equal(projectionReads, 1);
        assert.deepEqual(calls, ["provider", "files", "projection"]);
        assert.deepEqual(
          taskQueries,
          Array(3).fill(`${sourceEffect.effectId}:attachment.cleanup:prune`),
        );
        assert.isNull(
          CheckpointRollbackService.readIssuedCheckpointRollbackObservationForExecution(execution),
        );
        return;
      }
      yield* service.execute({
        threadId,
        providerThreadId,
        checkpointId,
        scopeId,
        restoreFiles,
        sourceEffect,
        ...ordinaryContext,
      });
      assert.equal(opened, 1);
      if (ordinary === "joined") {
        assert.equal(revalidations, 4);
        assert.equal(capturedRevalidations, 2);
        const observation =
          CheckpointRollbackService.readIssuedCheckpointRollbackObservationForExecution(execution);
        if (completion === "unqualified") {
          assert.isNull(observation);
          assert.deepEqual(calls, ["provider", "projection"]);
          return;
        }
        assert.ok(observation);
        assert.strictEqual(
          CheckpointRollbackService.readIssuedCheckpointRollbackObservation(observation),
          observation,
        );
        assert.equal(
          observation.completedAt,
          DateTime.formatIso(observation.storedEvents[0]!.event.occurredAt),
        );
        assert.deepEqual(observation.execution, execution);
        assert.deepEqual(observation.sourceEffect, sourceEffect);
        assert.isNull(
          CheckpointRollbackService.readIssuedCheckpointRollbackObservation({ ...observation }),
        );
        assert.isNull(
          CheckpointRollbackService.readIssuedCheckpointRollbackObservationForExecution({
            ...execution,
          }),
        );
        (observation.storedEvents as Array<unknown>).pop();
        assert.isNull(
          CheckpointRollbackService.readIssuedCheckpointRollbackObservation(observation),
        );
        assert.isNull(
          CheckpointRollbackService.readIssuedCheckpointRollbackObservationForExecution(execution),
        );
      }
      assert.deepEqual(
        calls,
        restoreFiles
          ? ["provider", "files", "projection"]
          : pruneRefs
            ? ["provider", "refs", "projection"]
            : ["provider", "projection"],
      );
    }).pipe(Effect.provide(testLayer));
  },
);

it.effect.skipIf(!symlinksSupported)(
  "rejects an archived thread sharing a worktree through a symlink",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-restore-isolation-" });
      const path = yield* Path.Path;
      const alias = path.join(cwd, "alias");
      yield* fileSystem.symlink(cwd, alias);
      const threadId = ThreadId.make("restore-alias-current");
      const otherId = ThreadId.make("restore-alias-archived");
      const projections = ProjectionStore.ProjectionStoreV2.of({
        getShellSnapshot: () =>
          Effect.succeed({
            schemaVersion: 1,
            snapshotSequence: 0,
            threads: [],
            archivedThreads: [{ id: otherId, deletedAt: null, worktreePath: alias } as never],
          }),
        getThreadProviderContext: () => Effect.succeed({ providerSessions: [] } as never),
        getCheckpointContext: () =>
          Effect.succeed({ runs: [], checkpointScopes: [], checkpoints: [] }),
      } as never);
      const isolated = yield* isCheckpointRestoreIsolated(
        { id: threadId, worktreePath: cwd },
        { cwd },
        {
          fileSystem,
          path,
          projections,
          projects: ProjectStore.ProjectStoreV2.of({
            get: () => Effect.succeed(unrelatedProject),
          } as never),
        },
      );
      assert.isFalse(isolated);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
