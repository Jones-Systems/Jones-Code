import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2Run,
  type WorktreeCleanupRules,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { DeletionWorktreeRemovalPreconditionError } from "../git/DeletionWorktreeRemoval.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as EventSink from "./EventSink.ts";
import type { WorktreeOwnershipLease } from "./WorktreeOwnershipLease.ts";
import { applyToProjection, emptyProjection } from "./ProjectionStore.ts";
import {
  makeDeletionWorktreeCleanup,
  makeLeaseCleanupLifecycle,
  planThreadDeletion,
} from "./ThreadDeletion.ts";

const threadId = ThreadId.make("thread:delete-plan");
const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const providerThreadId = ProviderThreadId.make("provider-thread:delete-plan");
const modelSelection = { instanceId: providerInstanceId, model: "gpt-5.4" };
const createdAt = DateTime.makeUnsafe("2026-09-01T00:00:00.000Z");
const deletedAt = DateTime.makeUnsafe("2026-09-04T00:00:00.000Z");
const command = {
  type: "thread.delete" as const,
  commandId: CommandId.make("command:delete-plan"),
  threadId,
};

function makeProjection(): OrchestrationV2ThreadProjection {
  const base = emptyProjection({
    id: EventId.make("event:delete-plan-created"),
    type: "thread.created",
    threadId,
    occurredAt: createdAt,
    payload: {
      id: threadId,
      createdBy: "user",
      creationSource: "web",
      projectId: ProjectId.make("project:delete-plan"),
      title: "Delete the thread",
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "feature",
      worktreePath: "/workspace/feature",
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt,
      updatedAt: createdAt,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
  });
  const runs: Array<OrchestrationV2Run> = (
    ["preparing", "queued", "starting", "running", "waiting", "completed"] as const
  ).map((status, index) => ({
    id: RunId.make(`run:delete-plan:${status}`),
    threadId,
    ordinal: index + 1,
    providerInstanceId,
    modelSelection,
    providerThreadId,
    userMessageId: MessageId.make(`message:delete-plan:${status}`),
    rootNodeId: NodeId.make(`node:delete-plan:${status}`),
    activeAttemptId: RunAttemptId.make(`attempt:delete-plan:${status}`),
    status,
    queuePosition: status === "queued" ? 1 : null,
    requestedAt: createdAt,
    startedAt: createdAt,
    completedAt: status === "completed" ? createdAt : null,
    checkpointId: null,
    contextHandoffId: null,
  }));
  return {
    ...base,
    runs,
    attempts: runs.map((run) => ({
      id: run.activeAttemptId!,
      runId: run.id,
      attemptOrdinal: 1,
      rootNodeId: run.rootNodeId!,
      providerInstanceId,
      providerThreadId,
      providerTurnId: null,
      reason: "initial",
      status: run.status === "completed" ? "completed" : "running",
      startedAt: createdAt,
      completedAt: run.completedAt,
    })),
    nodes: runs.map((run) => ({
      id: run.rootNodeId!,
      threadId,
      runId: run.id,
      parentNodeId: null,
      rootNodeId: run.rootNodeId!,
      kind: "root_turn",
      status: run.status === "completed" ? "completed" : "waiting",
      countsForRun: true,
      providerThreadId,
      providerTurnId: null,
      nativeItemRef: null,
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: createdAt,
      completedAt: run.completedAt,
    })),
    runtimeRequests: ["pending", "resolved"].map((status) => ({
      id: RuntimeRequestId.make(`request:delete-plan:${status}`),
      nodeId: runs[3]!.rootNodeId!,
      providerTurnId: null,
      nativeRequestRef: null,
      kind: "user_input",
      status: status === "pending" ? "pending" : "resolved",
      responseCapability: { type: "message" },
      createdAt,
      resolvedAt: status === "resolved" ? createdAt : null,
    })),
  };
}

it.effect("cancels active work without reviving a run while disposing delegated completion", () =>
  Effect.gen(function* () {
    const base = makeProjection();
    const parentRun = base.runs.find((run) => run.status === "running")!;
    const queuedRun = base.runs.find((run) => run.status === "queued")!;
    const taskId = NodeId.make("task:delete-plan");
    const projection: OrchestrationV2ThreadProjection = {
      ...base,
      runs: base.runs.map((run) =>
        run.id === parentRun.id
          ? {
              ...run,
              delegatedCompletion: {
                disposition: "open",
                nextGeneration: 3,
                delivery: { generation: 2, messageId: queuedRun.userMessageId, taskIds: [taskId] },
              },
            }
          : run,
      ),
      subagents: [
        {
          id: taskId,
          threadId,
          runId: parentRun.id,
          parentNodeId: parentRun.rootNodeId!,
          origin: "app_owned",
          createdBy: "agent",
          driver,
          providerInstanceId,
          providerThreadId: null,
          childThreadId: null,
          nativeTaskRef: null,
          prompt: "Inspect the project",
          title: null,
          model: null,
          completionDelivery: { state: "claimed", observedByRunId: null },
          status: "completed",
          result: "done",
          startedAt: createdAt,
          completedAt: createdAt,
          updatedAt: createdAt,
        },
      ],
    };
    const plan = yield* planThreadDeletion({
      command,
      projection,
      attachmentIds: projection.messages.flatMap((message) =>
        message.attachments.map((attachment) => attachment.id),
      ),
      now: deletedAt,
      idAllocator: yield* IdAllocator.IdAllocatorV2,
    });
    const deleted = plan.events.reduce(applyToProjection, projection);
    assert.deepEqual(deleted.thread.deletedAt, deletedAt);
    assert.equal(deleted.thread.worktreePath, "/workspace/feature");
    assert.isNull(projection.thread.deletedAt);
    for (const run of deleted.runs) {
      assert.equal(run.status, run.id.endsWith(":completed") ? "completed" : "cancelled");
      assert.deepEqual(run.completedAt, run.id.endsWith(":completed") ? createdAt : deletedAt);
    }
    for (const attempt of deleted.attempts) {
      assert.equal(
        attempt.status,
        attempt.runId.endsWith(":completed") ? "completed" : "cancelled",
      );
    }
    for (const node of deleted.nodes) {
      assert.equal(node.status, node.runId?.endsWith(":completed") ? "completed" : "cancelled");
    }
    const pending = deleted.runtimeRequests.find((request) => request.id.endsWith(":pending"))!;
    assert.equal(pending.status, "cancelled");
    assert.deepEqual(pending.responseCapability, {
      type: "not_resumable",
      reason: "The thread was deleted.",
    });
    assert.deepEqual(deleted.runtimeRequests[1], projection.runtimeRequests[1]);
    assert.deepEqual(deleted.runs.find((run) => run.id === parentRun.id)?.delegatedCompletion, {
      disposition: "disposed",
      nextGeneration: 3,
      delivery: null,
    });
    assert.equal(deleted.subagents[0]?.completionDelivery?.state, "disposed");
    const queuedRunUpdates = plan.events.filter(
      (event) => event.type === "run.updated" && event.payload.id === queuedRun.id,
    );
    assert.lengthOf(queuedRunUpdates, 1);
  }).pipe(Effect.provide(IdAllocator.layer)),
);

it.effect("queues provider and resource cleanup and preserves an earlier deletion timestamp", () =>
  Effect.gen(function* () {
    const base = makeProjection();
    const projection: OrchestrationV2ThreadProjection = {
      ...base,
      thread: { ...base.thread, deletedAt: createdAt },
      providerSessions: ["running", "stopped", "error"].map((status) => ({
        id: ProviderSessionId.make(`session:delete-plan:${status}`),
        driver,
        providerInstanceId,
        status: status === "running" ? "running" : status === "stopped" ? "stopped" : "error",
        cwd: "/workspace/feature",
        model: null,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt,
        updatedAt: createdAt,
        lastError: null,
      })),
      messages: [0, 1].map((index) => ({
        id: MessageId.make(`message:attachment:${index}`),
        threadId,
        createdBy: "user",
        creationSource: "web",
        runId: null,
        nodeId: null,
        role: "user",
        text: "Inspect this file",
        attachments: [
          {
            type: "file",
            id: "shared_file",
            name: "input.txt",
            mimeType: "text/plain",
            sizeBytes: 10,
          },
        ],
        streaming: false,
        createdAt,
        updatedAt: createdAt,
      })),
    };
    const plan = yield* planThreadDeletion({
      command,
      projection,
      attachmentIds: projection.messages.flatMap((message) =>
        message.attachments.map((attachment) => attachment.id),
      ),
      now: deletedAt,
      idAllocator: yield* IdAllocator.IdAllocatorV2,
    });
    const deleted = plan.events.reduce(applyToProjection, projection);
    assert.deepEqual(deleted.thread.deletedAt, createdAt);
    assert.deepEqual(
      deleted.providerSessions.map((session) => session.status),
      ["stopped", "error"],
    );
    assert.deepEqual(
      plan.effects.map((effect) => effect.request),
      [
        {
          type: "provider-session.detach",
          providerSessionId: ProviderSessionId.make("session:delete-plan:running"),
          detail: "Thread deleted.",
          revokeMcpCredential: true,
        },
        { type: "terminal.cleanup" },
        { type: "attachment.cleanup", attachmentIds: ["shared_file"] },
      ],
    );
  }).pipe(Effect.provide(IdAllocator.layer)),
);

const cleanupLease: WorktreeOwnershipLease = {
  resourcePath: "/workspace/original-worktree",
  leaseId: "lease-original",
  ownerThreadId: threadId,
  ownerIncarnation: JSON.stringify(["t3.orchestration-v2.thread-birth/v1", "birth-original", 1]),
  branch: "original",
  acquiredAtMs: 1,
  renewedAtMs: 2,
  expiresAtMs: 300_000,
};

function completedCleanupBasis(lease = cleanupLease): EventSink.LeaseCleanupStoreBasisV2 {
  const ownerBirth = {
    kind: "application_v2_thread_birth" as const,
    threadId: lease.ownerThreadId,
    eventId: EventId.make("birth-original"),
    sequence: 1,
  };
  const task: EventSink.LeaseCleanupTaskBindingV2 = {
    version: 2,
    effectId: "effect:original:provider-stop",
    threadId: lease.ownerThreadId,
    lease,
    ownerBirth,
    deletion: {
      commandId: CommandId.make("delete-original"),
      eventId: EventId.make("deleted-original"),
      sequence: 2,
    },
    task: {
      kind: "provider",
      evidenceRevision: 3,
      expectedBinding: {
        threadId: lease.ownerThreadId,
        providerThreadId,
        providerSessionId: ProviderSessionId.make("session-original"),
        instanceId: providerInstanceId,
        driver,
        nativeThreadId: "native-original",
        runtimeGeneration: "generation-original",
      },
    },
    bindingSha256: "a".repeat(64),
    recordedAt: "2026-09-04T00:00:00.000Z",
  };
  return {
    lease,
    leaseCurrent: true,
    ownerPresence: "absent",
    historicalOwnerBirth: ownerBirth,
    currentApplicationBirth: null,
    deletion: task.deletion,
    inventoryComplete: true,
    pendingOwnerEffectIds: [],
    tasks: [task],
    outcomes: [
      {
        ordinal: 1,
        outcome: { taskId: task.effectId, result: "succeeded", effect: "confirmed" },
        correlation: {
          workerId: "worker-original",
          expectedAttempt: 1,
          bindingSha256: task.bindingSha256,
          evidence: { completeOwnedRuntimeReceipt: "domain-fixture-receipt" },
        },
        recordedAt: task.recordedAt,
      },
    ],
  };
}

function cleanupFixture(
  read: (lease: WorktreeOwnershipLease) => EventSink.LeaseCleanupStoreBasisV2,
  initial: ReadonlyArray<WorktreeOwnershipLease> = [cleanupLease],
  releaseHasEffect = true,
  deletion: Partial<
    Pick<
      EventSink.EventSinkV2["Service"],
      | "readThreadDeletionCommand"
      | "readDeletionWorktreeTask"
      | "observeThreadDeletionCleanup"
      | "readDeletionWorktreePathAdmission"
      | "withDeletionWorktreeSqlMutation"
    >
  > = {},
) {
  let current = [...initial];
  const released: WorktreeOwnershipLease[] = [];
  let basisReads = 0;
  const service = makeLeaseCleanupLifecycle({
    sink: {
      readLeaseCleanupStoreBasis: (lease) =>
        Effect.sync(() => {
          basisReads++;
          return read(lease);
        }),
      prepareLeaseCleanupTaskBindings: ({ lease }) => Effect.sync(() => read(lease)),
      readThreadDeletionCommand: () => Effect.succeed(null),
      readDeletionWorktreeTask: () =>
        Effect.die("historical lease cleanup cannot invent worktree consent"),
      observeThreadDeletionCleanup: ({ threadId, commandId }) =>
        Effect.succeed({
          threadId,
          commandId,
          receipt: null,
          deletion: null,
          worktree: null,
          state: "unknown",
          removalOutcome: null,
          currentLease: "unavailable",
          reason: "original_worktree_consent_unavailable",
        }),
      readDeletionWorktreePathAdmission: ({ path }) =>
        Effect.succeed({ path, status: "available", reservations: [], admissions: [] }),
      withDeletionWorktreeSqlMutation: (_input, mutation) => mutation,
      withTransaction: (effect) => effect,
      ...deletion,
    },
    leases: {
      listAll: () => Effect.sync(() => current),
      release: (lease) =>
        Effect.sync(() => {
          released.push(lease);
          if (releaseHasEffect) current = current.filter((row) => row.leaseId !== lease.leaseId);
        }),
    },
  });
  return { service, released, basisReads: () => basisReads, leases: () => current };
}

it.effect("retains failed provider cleanup and releases only after a later confirmed outcome", () =>
  Effect.gen(function* () {
    let basis = completedCleanupBasis();
    basis = {
      ...basis,
      outcomes: [
        {
          ...basis.outcomes[0]!,
          outcome: {
            taskId: basis.tasks[0]!.effectId,
            result: "failed",
            effect: "no_effect",
          },
        },
      ],
    };
    const fixture = cleanupFixture(() => basis);
    const failed = yield* fixture.service.cleanupLeaseOwner(cleanupLease);
    assert.equal(failed.reason, "task_failed");
    assert.deepEqual(fixture.released, []);
    assert.deepEqual(fixture.leases(), [cleanupLease]);
    basis = completedCleanupBasis();
    const retried = yield* fixture.service.cleanupLeaseOwner(cleanupLease);
    assert.equal(retried.status, "released");
    assert.strictEqual(fixture.released[0], cleanupLease);
    assert.deepEqual(fixture.leases(), []);
    assert.equal(fixture.basisReads(), 3);
  }),
);

it.effect(
  "recovers an absent application owner only from complete confirmed cleanup inventory",
  () =>
    Effect.gen(function* () {
      const fixture = cleanupFixture(() => completedCleanupBasis());
      assert.deepEqual(
        (yield* fixture.service.reconcileLeaseOwners).map((result) => result.status),
        ["released"],
      );
      assert.deepEqual(fixture.released, [cleanupLease]);
    }),
);

it.effect(
  "an old incarnation releases only its original lease and preserves the replacement lease",
  () =>
    Effect.gen(function* () {
      const replacement = {
        ...cleanupLease,
        resourcePath: "/workspace/replacement",
        leaseId: "lease-replacement",
        ownerIncarnation: yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))([
          "t3.orchestration-v2.thread-birth/v1",
          "birth-replacement",
          3,
        ]).pipe(Effect.orDie),
      };
      const oldBasis = {
        ...completedCleanupBasis(),
        ownerPresence: "current" as const,
        currentApplicationBirth: {
          kind: "application_v2_thread_birth" as const,
          threadId,
          eventId: EventId.make("birth-replacement"),
          sequence: 3,
        },
      };
      const fixture = cleanupFixture(() => oldBasis, [cleanupLease, replacement]);
      assert.equal((yield* fixture.service.cleanupLeaseOwner(cleanupLease)).status, "released");
      assert.deepEqual(fixture.released, [cleanupLease]);
      assert.deepEqual(fixture.leases(), [replacement]);
      assert.equal(oldBasis.tasks[0]!.task.kind, "provider");
      if (oldBasis.tasks[0]!.task.kind === "provider")
        assert.equal(
          oldBasis.tasks[0]!.task.expectedBinding.runtimeGeneration,
          "generation-original",
        );
    }),
);

it.effect("one owner's unknown cleanup cannot block another owner's completed cleanup", () =>
  Effect.gen(function* () {
    const later = {
      ...cleanupLease,
      resourcePath: "/workspace/later",
      leaseId: "lease-later",
      ownerThreadId: ThreadId.make("thread-later"),
    };
    const fixture = cleanupFixture(
      (lease) =>
        lease.leaseId === cleanupLease.leaseId
          ? { ...completedCleanupBasis(lease), ownerPresence: "unavailable" }
          : completedCleanupBasis(lease),
      [cleanupLease, later],
    );
    assert.deepEqual(
      (yield* fixture.service.reconcileLeaseOwners).map((result) => result.status),
      ["retained", "released"],
    );
    assert.deepEqual(fixture.released, [later]);
    assert.deepEqual(fixture.leases(), [cleanupLease]);
  }),
);

it.effect("a void release without the exact lease postcondition retains ownership", () =>
  Effect.gen(function* () {
    const fixture = cleanupFixture(() => completedCleanupBasis(), [cleanupLease], false);
    assert.equal(
      (yield* fixture.service.cleanupLeaseOwner(cleanupLease)).reason,
      "release_unconfirmed",
    );
    assert.deepEqual(fixture.leases(), [cleanupLease]);
  }),
);

it.effect("a changed cleanup basis before release cannot remove the lease", () =>
  Effect.gen(function* () {
    let read = 0;
    const fixture = cleanupFixture(() => {
      read++;
      return read === 1
        ? completedCleanupBasis()
        : { ...completedCleanupBasis(), pendingOwnerEffectIds: ["new-owner-work"] };
    });
    assert.equal((yield* fixture.service.cleanupLeaseOwner(cleanupLease)).reason, "basis_changed");
    assert.deepEqual(fixture.released, []);
  }),
);

it.effect.each(["unknown", "no_effect"] as const)(
  "a %s task effect cannot be promoted to completed lease cleanup",
  (effect) =>
    Effect.gen(function* () {
      const basis = completedCleanupBasis();
      const fixture = cleanupFixture(() => ({
        ...basis,
        outcomes: [
          {
            ...basis.outcomes[0]!,
            outcome: { taskId: basis.tasks[0]!.effectId, result: "succeeded", effect },
          },
        ],
      }));
      assert.equal((yield* fixture.service.cleanupLeaseOwner(cleanupLease)).reason, "task_unknown");
      assert.deepEqual(fixture.released, []);
    }),
);

it.effect(
  "current owner and incomplete or empty task inventory never authorize lease release",
  () =>
    Effect.gen(function* () {
      const basis = completedCleanupBasis();
      for (const changed of [
        { ...basis, currentApplicationBirth: basis.historicalOwnerBirth },
        { ...basis, inventoryComplete: false },
        { ...basis, tasks: [] },
        { ...basis, outcomes: [] },
      ]) {
        const fixture = cleanupFixture(() => changed);
        assert.equal((yield* fixture.service.cleanupLeaseOwner(cleanupLease)).status, "retained");
        assert.deepEqual(fixture.released, []);
      }
    }),
);

it.effect.each(["captured", "unknown"] as const)(
  "terminal capture %s preserves the historical owner and never authorizes release",
  (status) =>
    Effect.gen(function* () {
      const original = completedCleanupBasis();
      const historical = original.historicalOwnerBirth!;
      const replacement = {
        ...historical,
        eventId: EventId.make("birth-replacement"),
        sequence: 3,
      };
      const basis = { ...original, inventoryComplete: false, currentApplicationBirth: replacement };
      const observations: unknown[] = [];
      const prepared: unknown[] = [];
      const released: unknown[] = [];
      const capture = {
        managerId: "original-manager",
        threadId: historical.threadId,
        ownerBirth: historical,
        status,
        managedTargetsOnly: true as const,
        targets: [
          {
            threadId: historical.threadId,
            terminalId: "shared-terminal",
            handleId: "original-handle",
            ownerBirth: historical,
          },
        ],
      };
      const lifecycle = makeLeaseCleanupLifecycle({
        sink: {
          readLeaseCleanupStoreBasis: () => Effect.succeed(basis),
          prepareLeaseCleanupTaskBindings: (input) =>
            Effect.sync(() => {
              prepared.push(input);
              return basis;
            }),
          readThreadDeletionCommand: () =>
            Effect.die("incomplete capture cannot release ownership"),
          readDeletionWorktreeTask: () =>
            Effect.die("incomplete capture cannot read removal tasks"),
          observeThreadDeletionCleanup: () =>
            Effect.die("incomplete capture cannot observe removal"),
          readDeletionWorktreePathAdmission: () =>
            Effect.die("incomplete capture cannot release a reserved path"),
          withDeletionWorktreeSqlMutation: () =>
            Effect.die("incomplete capture cannot mutate a lease"),
          withTransaction: (effect) => effect,
        },
        leases: {
          listAll: () => Effect.succeed([cleanupLease]),
          release: (lease) =>
            Effect.sync(() => {
              released.push(lease);
            }),
        },
        captureTerminals: (birth) =>
          Effect.sync(() => {
            observations.push(birth);
            return capture;
          }),
      });
      const result = yield* lifecycle.cleanupLeaseOwner(cleanupLease);
      assert.equal(result.status, "retained");
      assert.deepEqual(observations, [historical]);
      assert.deepEqual(
        prepared,
        status === "captured" ? [{ lease: cleanupLease, terminalCapture: capture }] : [],
      );
      assert.deepEqual(released, []);
    }),
);

it.effect(
  "one owner's typed cleanup read failure retains its lease while the next owner progresses",
  () =>
    Effect.gen(function* () {
      const later = {
        ...cleanupLease,
        resourcePath: "/workspace/later",
        leaseId: "lease-later",
        ownerThreadId: ThreadId.make("thread-later"),
      };
      let leases = [cleanupLease, later];
      const released: WorktreeOwnershipLease[] = [];
      const lifecycle = makeLeaseCleanupLifecycle({
        sink: {
          readLeaseCleanupStoreBasis: (lease) =>
            lease.leaseId === cleanupLease.leaseId
              ? Effect.fail(new EventSink.EventSinkStreamError({ threadId: lease.ownerThreadId }))
              : Effect.succeed(completedCleanupBasis(lease)),
          prepareLeaseCleanupTaskBindings: () => Effect.die("read failure cannot prepare cleanup"),
          readThreadDeletionCommand: () => Effect.succeed(null),
          readDeletionWorktreeTask: () =>
            Effect.die("historical lease cleanup cannot invent worktree consent"),
          observeThreadDeletionCleanup: () =>
            Effect.die("lease recovery cannot fabricate readback"),
          readDeletionWorktreePathAdmission: ({ path }) =>
            Effect.succeed({ path, status: "available", reservations: [], admissions: [] }),
          withDeletionWorktreeSqlMutation: (_input, mutation) => mutation,
          withTransaction: (effect) => effect,
        },
        leases: {
          listAll: () => Effect.sync(() => leases),
          release: (lease) =>
            Effect.sync(() => {
              released.push(lease);
              leases = leases.filter((row) => row.leaseId !== lease.leaseId);
            }),
        },
      });
      const results = yield* lifecycle.reconcileLeaseOwners;
      assert.equal(results[0]?.reason, "basis_unavailable");
      assert.equal(results[1]?.status, "released");
      assert.deepEqual(released, [later]);
      assert.deepEqual(leases, [cleanupLease]);
    }),
);

const removalBasis = completedCleanupBasis();
const removalCommandRecord: EventSink.ThreadDeletionCommandRecordV1 = {
  command: {
    type: "thread.delete",
    commandId: removalBasis.deletion!.commandId,
    threadId,
    worktreeRemoval: {
      projectId: ProjectId.make("original-project"),
      path: cleanupLease.resourcePath,
      branch: cleanupLease.branch,
      force: true,
    },
  },
  commandDigest: "d".repeat(64),
  ownerBirth: removalBasis.historicalOwnerBirth,
  inventory: {
    worktree: {
      projectId: ProjectId.make("original-project"),
      path: cleanupLease.resourcePath,
      branch: cleanupLease.branch,
    },
    projectRoot: "/workspace",
    leaseInventory: { status: "original", lease: cleanupLease },
    prerequisiteEffectIds: removalBasis.tasks.map((task) => task.effectId),
    captureStatus: "captured",
    reason: null,
  },
  deletion: removalBasis.deletion!,
  recordedAt: "2026-10-03T00:00:00.000Z",
};
const removalTask: EventSink.DeletionWorktreeTaskBindingV1 = {
  version: 1,
  effectId: EventSink.deletionWorktreeEffectIdV1(removalCommandRecord.command.commandId, threadId),
  threadId,
  leaseInventory: removalCommandRecord.inventory.leaseInventory,
  ownerBirth: removalCommandRecord.ownerBirth,
  deletion: removalCommandRecord.deletion,
  task: {
    kind: "worktree",
    canonicalCommand: removalCommandRecord.command,
    commandDigest: removalCommandRecord.commandDigest,
    consent: removalCommandRecord.command.worktreeRemoval!,
    worktree: removalCommandRecord.inventory.worktree,
    projectRoot: removalCommandRecord.inventory.projectRoot,
    prerequisiteEffectIds: removalCommandRecord.inventory.prerequisiteEffectIds,
    captureStatus: "captured",
    reason: null,
  },
  bindingSha256: "e".repeat(64),
  recordedAt: removalCommandRecord.recordedAt,
};

it.effect.each(["reserved", "unavailable"] as const)(
  "a %s canonical path retains even the same owner's fully cleaned lease",
  (status) =>
    Effect.gen(function* () {
      const fixture = cleanupFixture(() => completedCleanupBasis(), [cleanupLease], true, {
        readDeletionWorktreePathAdmission: ({ path }) =>
          Effect.succeed({ path, status, reservations: [removalTask], admissions: [] }),
        withDeletionWorktreeSqlMutation: () =>
          Effect.die("reserved path cannot enter lease release"),
      });
      assert.equal(
        (yield* fixture.service.cleanupLeaseOwner(cleanupLease)).reason,
        status === "reserved" ? "path_reserved" : "path_unavailable",
      );
      assert.deepEqual(fixture.released, []);
      assert.deepEqual(fixture.leases(), [cleanupLease]);
    }),
);

it.effect.each(["pending", "removing", "retained", "unknown"] as const)(
  "original requested worktree removal %s cannot release its lease",
  (state) =>
    Effect.gen(function* () {
      const fixture = cleanupFixture(() => completedCleanupBasis(), [cleanupLease], true, {
        readThreadDeletionCommand: () => Effect.succeed(removalCommandRecord),
        readDeletionWorktreeTask: (effectId) => {
          assert.equal(effectId, removalTask.effectId);
          return Effect.succeed(removalTask);
        },
        observeThreadDeletionCleanup: ({ threadId, commandId }) =>
          Effect.succeed({
            threadId,
            commandId,
            receipt: null,
            deletion: {
              eventId: removalTask.deletion.eventId,
              sequence: removalTask.deletion.sequence,
              resultSequence: removalTask.deletion.sequence,
            },
            worktree: {
              projectId: ProjectId.make("original-project"),
              path: cleanupLease.resourcePath,
              branch: cleanupLease.branch,
            },
            state,
            removalOutcome: state === "unknown" ? { result: null, effect: "unknown" } : null,
            currentLease: "original",
            reason: null,
          }),
        withDeletionWorktreeSqlMutation: () =>
          Effect.die("unfinished removal cannot release its lease"),
      });
      const result = yield* fixture.service.cleanupLeaseOwner(cleanupLease);
      assert.equal(
        result.reason,
        state === "unknown" ? "worktree_cleanup_unknown" : "worktree_cleanup_pending",
      );
      assert.deepEqual(fixture.released, []);
      assert.deepEqual(fixture.leases(), [cleanupLease]);
    }),
);

it.effect.each([
  null,
  {
    ...removalTask,
    leaseInventory: {
      status: "original" as const,
      lease: { ...cleanupLease, leaseId: "replacement-lease" },
    },
  },
])("missing or replaced requested cleanup task never releases original ownership", (task) =>
  Effect.gen(function* () {
    const fixture = cleanupFixture(() => completedCleanupBasis(), [cleanupLease], true, {
      readThreadDeletionCommand: () => Effect.succeed(removalCommandRecord),
      readDeletionWorktreeTask: () => Effect.succeed(task),
      observeThreadDeletionCleanup: ({ threadId, commandId }) =>
        Effect.succeed({
          threadId,
          commandId,
          receipt: null,
          deletion: {
            eventId: removalTask.deletion.eventId,
            sequence: removalTask.deletion.sequence,
            resultSequence: removalTask.deletion.sequence,
          },
          worktree: null,
          state: "unknown",
          removalOutcome: null,
          currentLease: "replacement",
          reason: "complete_worktree_removal_proof_unavailable",
        }),
    });
    assert.equal(
      (yield* fixture.service.cleanupLeaseOwner(cleanupLease)).reason,
      "worktree_cleanup_unknown",
    );
    assert.deepEqual(fixture.released, []);
  }),
);

it.effect(
  "ordinary cleanup with no requested removal uses the SQL path fence and preserves historical readback uncertainty",
  () =>
    Effect.gen(function* () {
      const fenced: string[] = [];
      const fixture = cleanupFixture(() => completedCleanupBasis(), [cleanupLease], true, {
        withDeletionWorktreeSqlMutation: ({ path }, mutation) =>
          Effect.sync(() => {
            fenced.push(path);
          }).pipe(Effect.andThen(mutation)),
      });
      const readback = yield* fixture.service.observeThreadDeletionCleanup({
        threadId,
        commandId: removalCommandRecord.command.commandId,
      });
      assert.equal(readback.state, "unknown");
      assert.deepEqual(fixture.released, []);
      assert.equal((yield* fixture.service.cleanupLeaseOwner(cleanupLease)).status, "released");
      assert.deepEqual(fenced, [cleanupLease.resourcePath]);
      assert.deepEqual(fixture.released, [cleanupLease]);
    }),
);

it.effect(
  "canonical deletion without worktree consent preserves ordinary confirmed lease cleanup",
  () =>
    Effect.gen(function* () {
      const record: EventSink.ThreadDeletionCommandRecordV1 = {
        ...removalCommandRecord,
        command: {
          type: "thread.delete",
          commandId: removalCommandRecord.command.commandId,
          threadId,
        },
      };
      const fixture = cleanupFixture(() => completedCleanupBasis(), [cleanupLease], true, {
        readThreadDeletionCommand: () => Effect.succeed(record),
        readDeletionWorktreeTask: () =>
          Effect.die("no consent cannot request physical worktree removal"),
        observeThreadDeletionCleanup: () =>
          Effect.die("ordinary release does not need a fabricated removal result"),
      });
      assert.equal((yield* fixture.service.cleanupLeaseOwner(cleanupLease)).status, "released");
      assert.deepEqual(fixture.released, [cleanupLease]);
    }),
);

it.effect(
  "a reservation detected inside the SQL release fence prevents the stale available read from releasing ownership",
  () =>
    Effect.gen(function* () {
      const fixture = cleanupFixture(() => completedCleanupBasis(), [cleanupLease], true, {
        withDeletionWorktreeSqlMutation: () =>
          Effect.fail(
            new EventSink.EventSinkWriteError({
              eventCount: 0,
              cause: "A concurrent worktree removal reserves this path",
            }),
          ),
      });
      const failure = yield* fixture.service.cleanupLeaseOwner(cleanupLease).pipe(Effect.flip);
      assert.instanceOf(failure, EventSink.EventSinkWriteError);
      assert.deepEqual(fixture.released, []);
      assert.deepEqual(fixture.leases(), [cleanupLease]);
    }),
);

const worktreeClaim = {
  effectId: removalTask.effectId,
  bindingSha256: removalTask.bindingSha256,
  workerId: "worktree-original-worker",
  expectedAttempt: 1,
};
const requiredRemovalTarget = (binding: EventSink.DeletionWorktreeTaskBindingV1) => {
  const target = EventSink.deletionWorktreeRemovalTargetV1(binding);
  if (target === null) throw new Error("Worktree removal fixture requires an exact target");
  return target;
};
const worktreeStart: EventSink.DeletionWorktreeRemovalStartV1 = {
  schema: "t3.deletion-worktree-removal-start/v1",
  ...worktreeClaim,
  target: requiredRemovalTarget(removalTask),
  startedAt: removalTask.recordedAt,
};
function worktreeExecutionBasis(): EventSink.DeletionWorktreeExecutionBasisV1 {
  return {
    binding: removalTask,
    effect: {
      id: removalTask.effectId,
      commandId: removalTask.deletion.commandId,
      threadId,
      request: { type: "worktree.cleanup" },
      status: "running",
      attemptCount: 1,
      availableAt: removalTask.recordedAt,
      leaseOwner: worktreeClaim.workerId,
      leaseExpiresAt: "2026-10-04T00:00:00.000Z",
      createdAt: removalTask.recordedAt,
      updatedAt: removalTask.recordedAt,
      completedAt: null,
      lastError: null,
    },
    admission: null,
    currentLease: "original",
    inventoryComplete: true,
    prerequisitesReady: true,
    prerequisites: [],
    latestOutcome: null,
    start: null,
    held: false,
    reason: null,
  };
}
function worktreeRunnerFixture(
  options: {
    readonly initial?: EventSink.DeletionWorktreeExecutionBasisV1;
    readonly mode?: "start_now" | "observe_only" | "retained";
    readonly outcome?: EventSink.LeaseCleanupTaskOutcomeV2;
    readonly finalStatus?: "completed" | "retained";
    readonly policy?: Effect.Effect<WorktreeCleanupRules>;
  } = {},
) {
  let basis = options.initial ?? worktreeExecutionBasis();
  const calls: string[] = [];
  const originalStart = {
    ...worktreeStart,
    target: requiredRemovalTarget(basis.binding),
  };
  const recorded: unknown[] = [];
  const finalized: unknown[] = [];
  const observe = (start: EventSink.DeletionWorktreeRemovalStartV1, ordinal: number) => ({
    version: 1 as const,
    start,
    startOrdinal: ordinal,
    operation: { kind: "already_absent" as const, completion: "not_invoked" as const },
    before: {
      registration: {
        status: "complete" as const,
        projectRoot: "/workspace",
        gitCommonDirectory: "/workspace/.git",
        entries: [],
      },
      filesystem: { status: "absent" as const, path: cleanupLease.resourcePath },
    },
    after: {
      registration: {
        status: "complete" as const,
        projectRoot: "/workspace",
        gitCommonDirectory: "/workspace/.git",
        entries: [],
      },
      filesystem: { status: "absent" as const, path: cleanupLease.resourcePath },
    },
    observedAt: removalTask.recordedAt,
  });
  const service = makeDeletionWorktreeCleanup({
    sink: {
      readDeletionWorktreeExecutionBasis: () =>
        Effect.sync(() => {
          calls.push("read");
          return basis;
        }),
      startDeletionWorktreeRemoval: (input) =>
        Effect.gen(function* () {
          calls.push("start");
          assert.deepEqual(input.target, originalStart.target);
          if (input.revalidatePolicy !== undefined)
            yield* input.revalidatePolicy.pipe(
              Effect.mapError(
                (cause) => new EventSink.EventSinkWriteError({ eventCount: 0, cause }),
              ),
            );
          const mode = options.mode ?? "start_now";
          if (mode === "retained") return { status: mode, reason: "prerequisite_unknown", basis };
          if (mode === "start_now")
            basis = {
              ...basis,
              start: { ordinal: 0, evidence: originalStart },
              latestOutcome: {
                ordinal: 0,
                outcome: { taskId: removalTask.effectId, result: null, effect: "unknown" },
              },
            };
          return mode === "start_now"
            ? { status: mode, start: originalStart, basis }
            : { status: mode, reason: "started", basis };
        }),
      revalidateDeletionWorktreeRemovalStart: (start, ordinal, policy) =>
        Effect.gen(function* () {
          calls.push("revalidate");
          assert.deepEqual(start, basis.start?.evidence);
          assert.equal(ordinal, basis.start?.ordinal);
          if (policy !== undefined)
            yield* policy.pipe(
              Effect.mapError(
                (cause) => new EventSink.EventSinkWriteError({ eventCount: 0, cause }),
              ),
            );
        }),
      readUnresolvedDeletionCleanupHolds: () => Effect.succeed([]),
      recordObservedDeletionCleanupOutcome: (input) =>
        Effect.sync(() => {
          calls.push("record");
          recorded.push(input);
          const outcome: EventSink.LeaseCleanupTaskOutcomeV2 = options.outcome ?? {
            taskId: removalTask.effectId,
            result: "succeeded",
            effect: "absent",
          };
          return {
            ordinal: input.expectedLatestOrdinal + 1,
            outcome,
            bindingSha256: removalTask.bindingSha256,
            evidence: {
              version: 1,
              schema: "t3.deletion-cleanup-observation/v1",
              producer: "worktree",
              observation: input.observation,
              coveredHolds: input.coveredHolds,
            },
          } as const;
        }),
    },
    producer: {
      inspectTarget: () => Effect.die("STORE start is required before producer observation"),
      executeStarted: (start, ordinal, revalidate) =>
        revalidate(start, ordinal).pipe(
          Effect.mapError(
            (cause) =>
              new DeletionWorktreeRemovalPreconditionError({
                reason: "recorded_start_revalidation_failed",
                cause,
              }),
          ),
          Effect.andThen(
            Effect.sync(() => {
              calls.push("execute");
              return observe(start, ordinal);
            }),
          ),
        ),
      observeStarted: (start, ordinal) =>
        Effect.sync(() => {
          calls.push("observe");
          return observe(start, ordinal);
        }),
    },
    leases: {
      finalizeDeletionWorktreeCleanup: (input) =>
        Effect.sync(() => {
          calls.push("finalize");
          finalized.push(input);
          return {
            status: options.finalStatus ?? "completed",
            reason: options.finalStatus === "retained" ? "replacement_lease" : null,
          };
        }),
    },
    revalidatePolicy: () => options.policy ?? Effect.die("explicit request cannot read policy"),
  });
  return { service, calls, recorded, finalized };
}

it.effect(
  "worktree runner persists start and revalidates before execute, qualification and exact finalization",
  () =>
    Effect.gen(function* () {
      const fixture = worktreeRunnerFixture();
      const result = yield* fixture.service(worktreeClaim);
      assert.equal(result.status, "completed");
      assert.deepEqual(fixture.calls, [
        "read",
        "start",
        "revalidate",
        "execute",
        "read",
        "record",
        "finalize",
      ]);
      assert.deepEqual(fixture.finalized, [
        {
          effectId: worktreeClaim.effectId,
          bindingSha256: worktreeClaim.bindingSha256,
          expectedLatestOrdinal: 1,
        },
      ]);
    }),
);

it.effect(
  "a persisted original worktree start is observed without a second removal invocation",
  () =>
    Effect.gen(function* () {
      const initial = {
        ...worktreeExecutionBasis(),
        start: { ordinal: 0, evidence: worktreeStart },
        latestOutcome: {
          ordinal: 2,
          outcome: { taskId: removalTask.effectId, result: null, effect: "unknown" as const },
        },
      };
      const fixture = worktreeRunnerFixture({ mode: "observe_only", initial });
      assert.equal((yield* fixture.service(worktreeClaim)).status, "completed");
      assert.deepEqual(fixture.calls, ["read", "start", "observe", "read", "record", "finalize"]);
      assert.deepEqual(fixture.finalized, [
        {
          effectId: worktreeClaim.effectId,
          bindingSha256: worktreeClaim.bindingSha256,
          expectedLatestOrdinal: 3,
        },
      ]);
    }),
);

it.effect(
  "an unproved prerequisite prevents both physical worktree invocation and finalization",
  () =>
    Effect.gen(function* () {
      const fixture = worktreeRunnerFixture({ mode: "retained" });
      const result = yield* fixture.service(worktreeClaim);
      assert.equal(result.reason, "prerequisite_unknown");
      assert.deepEqual(fixture.calls, ["read", "start"]);
      assert.deepEqual(fixture.recorded, []);
      assert.deepEqual(fixture.finalized, []);
    }),
);

it.effect("changed worktree claim or original task digest performs no physical operation", () =>
  Effect.gen(function* () {
    for (const request of [
      { ...worktreeClaim, workerId: "replacement-worker" },
      { ...worktreeClaim, bindingSha256: "f".repeat(64) },
    ]) {
      const fixture = worktreeRunnerFixture();
      assert.equal((yield* fixture.service(request)).status, "retained");
      assert.deepEqual(fixture.calls, ["read"]);
      assert.deepEqual(fixture.finalized, []);
    }
  }),
);

it.effect("unknown native readback remains retained and never reaches the finalizer", () =>
  Effect.gen(function* () {
    const fixture = worktreeRunnerFixture({
      outcome: { taskId: removalTask.effectId, result: null, effect: "unknown" },
    });
    assert.equal((yield* fixture.service(worktreeClaim)).reason, "worktree_removal_unconfirmed");
    assert.equal(fixture.recorded.length, 1);
    assert.deepEqual(fixture.finalized, []);
  }),
);

it.effect(
  "a qualified positive replay uses only the exact finalizer and preserves its retention decision",
  () =>
    Effect.gen(function* () {
      const fixture = worktreeRunnerFixture({
        finalStatus: "retained",
        initial: {
          ...worktreeExecutionBasis(),
          latestOutcome: {
            ordinal: 3,
            outcome: { taskId: removalTask.effectId, result: "succeeded", effect: "confirmed" },
          },
        },
      });
      const result = yield* fixture.service(worktreeClaim);
      assert.equal(result.reason, "replacement_lease");
      assert.deepEqual(fixture.calls, ["read", "finalize"]);
      assert.deepEqual(fixture.finalized, [
        {
          effectId: worktreeClaim.effectId,
          bindingSha256: worktreeClaim.bindingSha256,
          expectedLatestOrdinal: 3,
        },
      ]);
    }),
);

it.effect(
  "policy cleanup reads latest rules at START and again immediately before physical invocation",
  () =>
    Effect.gen(function* () {
      let policyReads = 0;
      const rules: WorktreeCleanupRules = {
        worktreeAfterDays: null,
        worktreeOnMerge: false,
        worktreeOnDelete: true,
        worktreeUnchanged: false,
      };
      const binding: EventSink.DeletionWorktreeTaskBindingV1 = {
        ...removalTask,
        task: {
          ...removalTask.task,
          request: {
            origin: "policy",
            projectId: ProjectId.make("original-project"),
            path: cleanupLease.resourcePath,
            branch: cleanupLease.branch,
            force: false,
            rules,
          },
        },
      };
      const fixture = worktreeRunnerFixture({
        initial: { ...worktreeExecutionBasis(), binding },
        policy: Effect.sync(() => {
          policyReads++;
          return rules;
        }),
      });
      assert.equal((yield* fixture.service(worktreeClaim)).status, "completed");
      assert.equal(policyReads, 2);
      assert.deepEqual(fixture.calls, [
        "read",
        "start",
        "revalidate",
        "execute",
        "read",
        "record",
        "finalize",
      ]);
    }),
);

it.effect(
  "deletion always schedules the stable attachment namespace effect for zero-ID orphan sweep",
  () =>
    Effect.gen(function* () {
      const plan = yield* planThreadDeletion({
        command,
        projection: makeProjection(),
        attachmentIds: [],
        now: deletedAt,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
      });
      const effects = plan.effects.filter((effect) => effect.request.type === "attachment.cleanup");
      assert.equal(effects.length, 1);
      assert.deepEqual(effects[0], {
        id: `effect:${command.commandId}:attachment.cleanup`,
        commandId: command.commandId,
        threadId: command.threadId,
        request: { type: "attachment.cleanup", attachmentIds: [] },
      });
      assert.equal(plan.events.filter((event) => event.type === "thread.deleted").length, 1);
      assert.deepEqual(plan.attachmentNamespaceCleanupPlans, [
        {
          mode: "delete_thread",
          effectId: effects[0]!.id,
          triggerEventId: plan.events.find((event) => event.type === "thread.deleted")!.id,
        },
      ]);
    }).pipe(Effect.provide(IdAllocator.layer)),
);
