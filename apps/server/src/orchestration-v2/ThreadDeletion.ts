import type {
  OrchestrationV2Command,
  OrchestrationV2DomainEvent,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import type * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";

import type { PendingOrchestrationEffectV2 } from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as ResourceCleanup from "./ResourceCleanupService.ts";
import type { IdAllocatorV2, IdAllocatorV2Error } from "./IdAllocator.ts";
import { forkParked } from "../serverActivation.ts";
import {
  makeWorktreeOwnershipLeaseStore,
  type WorktreeOwnershipLease,
  type WorktreeOwnershipLeaseStore,
} from "./WorktreeOwnershipLease.ts";

export interface ThreadDeletionPlan {
  readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
  readonly effects: ReadonlyArray<PendingOrchestrationEffectV2>;
}

/** Plan the same durable cleanup for direct thread deletion and project removal. */
export const planThreadDeletion = Effect.fn("ThreadDeletion.planThreadDeletion")(function* (input: {
  readonly command: Extract<OrchestrationV2Command, { readonly type: "thread.delete" }>;
  readonly projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "runs" | "attempts" | "nodes" | "runtimeRequests" | "subagents" | "providerSessions"
  >;
  readonly attachmentIds: ReadonlyArray<string>;
  readonly now: DateTime.Utc;
  readonly idAllocator: IdAllocatorV2["Service"];
}): Effect.fn.Return<ThreadDeletionPlan, IdAllocatorV2Error> {
  const { command, projection, now, idAllocator } = input;
  const events: Array<OrchestrationV2DomainEvent> = [];
  const effects: Array<PendingOrchestrationEffectV2> = [];
  let current = projection;
  const emitEvent = Effect.fn("ThreadDeletion.emitEvent")(function* <
    Event extends OrchestrationV2DomainEvent,
  >(event: Omit<Event, "id">) {
    const id = yield* idAllocator.allocate.event({
      threadId: event.threadId,
      commandId: command.commandId,
    });
    const withId = { ...event, id } as Event;
    events.push(withId);
    if (withId.type === "run.updated") {
      current = {
        ...current,
        runs: current.runs.map((run) => (run.id === withId.payload.id ? withId.payload : run)),
      };
    }
    if (withId.type === "subagent.updated") {
      current = {
        ...current,
        subagents: current.subagents.map((task) =>
          task.id === withId.payload.id ? withId.payload : task,
        ),
      };
    }
  });

  yield* emitEvent({
    type: "thread.deleted",
    threadId: command.threadId,
    providerInstanceId: projection.thread.providerInstanceId,
    occurredAt: now,
    payload: {
      ...projection.thread,
      deletedAt: projection.thread.deletedAt ?? now,
      titleRegeneration: null,
      updatedAt: now,
    },
  });

  const activeRuns = projection.runs.filter((run) =>
    ["preparing", "queued", "starting", "running", "waiting"].includes(run.status),
  );
  const activeRunById = new Map(activeRuns.map((run) => [run.id, run]));
  for (const run of activeRuns) {
    yield* emitEvent({
      type: "run.updated",
      threadId: command.threadId,
      runId: run.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: { ...run, status: "cancelled", queuePosition: null, completedAt: now },
    });
  }
  for (const attempt of projection.attempts) {
    const run = activeRunById.get(attempt.runId);
    if (run === undefined || (attempt.status !== "pending" && attempt.status !== "running")) {
      continue;
    }
    yield* emitEvent({
      type: "run-attempt.updated",
      threadId: command.threadId,
      runId: attempt.runId,
      nodeId: attempt.rootNodeId,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: { ...attempt, status: "cancelled", completedAt: now },
    });
  }
  for (const node of projection.nodes) {
    const run = node.runId === null ? undefined : activeRunById.get(node.runId);
    if (run === undefined || !["pending", "running", "waiting"].includes(node.status)) {
      continue;
    }
    yield* emitEvent({
      type: "node.updated",
      threadId: command.threadId,
      runId: run.id,
      nodeId: node.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: { ...node, status: "cancelled", completedAt: now },
    });
  }
  for (const request of projection.runtimeRequests.filter(
    (request) => request.status === "pending",
  )) {
    yield* emitEvent({
      type: "runtime-request.updated",
      threadId: command.threadId,
      nodeId: request.nodeId,
      occurredAt: now,
      payload: {
        ...request,
        status: "cancelled",
        responseCapability: { type: "not_resumable", reason: "The thread was deleted." },
        resolvedAt: now,
      },
    });
  }

  const cohortRunIds = new Set([
    ...current.runs.filter((run) => run.delegatedCompletion !== undefined).map((run) => run.id),
    ...current.subagents
      .filter((task) => task.origin === "app_owned" && task.runId !== null)
      .map((task) => task.runId!),
  ]);
  for (const parentRunId of cohortRunIds) {
    // Use the projected cancellation so disposing a cohort cannot revive its run.
    const parentRun = current.runs.find((run) => run.id === parentRunId);
    if (parentRun === undefined) continue;
    const cohort = parentRun.delegatedCompletion;
    const tasks = current.subagents.filter(
      (task) => task.origin === "app_owned" && task.runId === parentRunId,
    );
    yield* emitEvent({
      type: "run.updated",
      threadId: parentRun.threadId,
      runId: parentRun.id,
      ...(parentRun.rootNodeId === null ? {} : { nodeId: parentRun.rootNodeId }),
      providerInstanceId: parentRun.providerInstanceId,
      occurredAt: now,
      payload: {
        ...parentRun,
        delegatedCompletion: {
          disposition: "disposed",
          nextGeneration: cohort?.nextGeneration ?? 1,
          delivery: null,
        },
      },
    });
    for (const task of tasks) {
      if (
        task.completionDelivery?.state === "acknowledged" ||
        task.completionDelivery?.state === "delivered" ||
        task.completionDelivery?.state === "disposed"
      ) {
        continue;
      }
      yield* emitEvent({
        type: "subagent.updated",
        threadId: parentRun.threadId,
        ...(task.runId === null ? {} : { runId: task.runId }),
        nodeId: task.id,
        driver: task.driver,
        providerInstanceId: task.providerInstanceId,
        occurredAt: now,
        payload: {
          ...task,
          completionDelivery: { state: "disposed", observedByRunId: null },
          updatedAt: now,
        },
      });
    }
  }

  for (const session of projection.providerSessions) {
    if (session.status === "stopped" || session.status === "error") continue;
    yield* emitEvent({
      type: "provider-session.detached",
      threadId: command.threadId,
      driver: session.driver,
      providerInstanceId: session.providerInstanceId,
      occurredAt: now,
      payload: {
        providerSessionId: session.id,
        detachedAt: now,
        reason: "Thread deleted.",
      },
    });
    effects.push({
      id: `effect:${command.commandId}:provider-session.detach:${session.id}`,
      commandId: command.commandId,
      threadId: command.threadId,
      request: {
        type: "provider-session.detach",
        providerSessionId: session.id,
        detail: "Thread deleted.",
        revokeMcpCredential: true,
      },
    });
  }
  effects.push({
    id: `effect:${command.commandId}:terminal.cleanup`,
    commandId: command.commandId,
    threadId: command.threadId,
    request: { type: "terminal.cleanup" },
  });
  const attachmentIds = Array.from(new Set(input.attachmentIds));
  if (attachmentIds.length > 0) {
    effects.push({
      id: `effect:${command.commandId}:attachment.cleanup`,
      commandId: command.commandId,
      threadId: command.threadId,
      request: { type: "attachment.cleanup", attachmentIds },
    });
  }
  return { events, effects };
});

export interface LeaseCleanupResultV2 {
  readonly lease: WorktreeOwnershipLease;
  readonly status: "released" | "not_current" | "retained";
  readonly reason?: "current_owner" | "basis_unavailable" | "inventory_incomplete" |
    "pending_owner_work" | "task_failed" | "task_unknown" | "basis_changed" | "release_unconfirmed" |
    "path_reserved" | "path_unavailable" | "worktree_cleanup_unknown" | "worktree_cleanup_pending";
}

const sameLease = (left: WorktreeOwnershipLease, right: WorktreeOwnershipLease) =>
  left.resourcePath === right.resourcePath && left.leaseId === right.leaseId &&
  left.ownerThreadId === right.ownerThreadId && left.ownerIncarnation === right.ownerIncarnation &&
  left.branch === right.branch && left.acquiredAtMs === right.acquiredAtMs &&
  left.renewedAtMs === right.renewedAtMs && left.expiresAtMs === right.expiresAtMs;

const retentionReason = (basis: EventSink.LeaseCleanupStoreBasisV2): LeaseCleanupResultV2["reason"] => {
  if (basis.ownerPresence === "unavailable" || basis.historicalOwnerBirth === null || basis.deletion === null)
    return "basis_unavailable";
  if (basis.currentApplicationBirth !== null &&
      basis.currentApplicationBirth.eventId === basis.historicalOwnerBirth.eventId &&
      basis.currentApplicationBirth.sequence === basis.historicalOwnerBirth.sequence)
    return "current_owner";
  if (!basis.inventoryComplete || basis.tasks.length === 0) return "inventory_incomplete";
  if (basis.pendingOwnerEffectIds.length > 0) return "pending_owner_work";
  for (const task of basis.tasks) {
    const latest = basis.outcomes.filter((row) => row.outcome.taskId === task.effectId)
      .reduce<EventSink.LeaseCleanupStoreBasisV2["outcomes"][number] | undefined>(
        (previous, row) => previous === undefined || row.ordinal > previous.ordinal ? row : previous,
        undefined,
      );
    if (latest === undefined || latest.correlation.bindingSha256 !== task.bindingSha256)
      return "task_unknown";
    if (latest.outcome.effect === "unknown") return "task_unknown";
    if (latest.outcome.result === "failed") return "task_failed";
    if (latest.outcome.result !== "succeeded" ||
        (latest.outcome.effect !== "confirmed" && latest.outcome.effect !== "absent"))
      return "task_unknown";
  }
};

export const makeLeaseCleanupLifecycle = (input: {
  readonly sink: Pick<EventSink.EventSinkV2["Service"], "readLeaseCleanupStoreBasis" | "prepareLeaseCleanupTaskBindings" |
    "readThreadDeletionCommand" | "readDeletionWorktreeTask" | "observeThreadDeletionCleanup" |
    "readDeletionWorktreePathAdmission" | "withDeletionWorktreeSqlMutation" | "withTransaction">;
  readonly leases: Pick<WorktreeOwnershipLeaseStore, "listAll"> & {
    readonly release: (lease: WorktreeOwnershipLease) => ReturnType<WorktreeOwnershipLeaseStore["release"]>;
  };
  readonly captureTerminals?: ReturnType<typeof ResourceCleanup.makeOwnedResourceCleanup>["captureOwnedTerminalTargets"];
}) => {
  const readLeaseCleanupBasis = input.sink.readLeaseCleanupStoreBasis;
  const cleanupLeaseOwner = (lease: WorktreeOwnershipLease) => Effect.gen(function* () {
    let basis = yield* readLeaseCleanupBasis(lease);
    if (!basis.leaseCurrent || !sameLease(basis.lease, lease))
      return { lease, status: "not_current" } satisfies LeaseCleanupResultV2;
    if (!basis.inventoryComplete && basis.ownerPresence === "absent" && basis.historicalOwnerBirth !== null &&
        basis.deletion !== null && input.captureTerminals !== undefined &&
        !basis.tasks.some((task) => task.task.kind === "terminal")) {
      const capture = yield* input.captureTerminals(basis.historicalOwnerBirth);
      if (capture.status === "captured") {
        const task = yield* Schema.decodeUnknownEffect(EventSink.LeaseCleanupTaskV2)({ kind: "terminal", capture });
        if (task.kind === "terminal") basis = yield* input.sink.prepareLeaseCleanupTaskBindings({
          lease, terminalCapture: task.capture,
        });
      }
    }
    const reason = retentionReason(basis);
    if (reason !== undefined) return { lease, status: "retained", reason } satisfies LeaseCleanupResultV2;
    return yield* input.sink.withTransaction(Effect.gen(function* () {
      const current = yield* readLeaseCleanupBasis(lease);
      const currentLeases = yield* input.leases.listAll();
      if (!current.leaseCurrent || !sameLease(current.lease, lease) ||
          !currentLeases.some((candidate) => sameLease(candidate, lease)) ||
          retentionReason(current) !== undefined || JSON.stringify(current) !== JSON.stringify(basis))
        return { lease, status: "retained", reason: "basis_changed" } satisfies LeaseCleanupResultV2;
      const original = yield* input.sink.readThreadDeletionCommand(current.deletion!.commandId);
      if (original !== null && (original.command.threadId !== lease.ownerThreadId ||
          original.deletion.eventId !== current.deletion!.eventId || original.deletion.sequence !== current.deletion!.sequence))
        return { lease, status: "retained", reason: "worktree_cleanup_unknown" } satisfies LeaseCleanupResultV2;
      if (original?.command.worktreeRemoval !== undefined) {
        const task = yield* input.sink.readDeletionWorktreeTask(
          EventSink.deletionWorktreeEffectIdV1(original.command.commandId, lease.ownerThreadId),
        );
        const observation = yield* input.sink.observeThreadDeletionCleanup({
          threadId: lease.ownerThreadId, commandId: original.command.commandId,
        });
        if (task === null || task.threadId !== lease.ownerThreadId || task.deletion.eventId !== original.deletion.eventId ||
            task.deletion.sequence !== original.deletion.sequence || task.task.commandDigest !== original.commandDigest ||
            task.task.worktree.path !== lease.resourcePath || task.ownerBirth === null ||
            task.ownerBirth.eventId !== current.historicalOwnerBirth!.eventId ||
            task.ownerBirth.sequence !== current.historicalOwnerBirth!.sequence ||
            task.leaseInventory.status !== "original" || !sameLease(task.leaseInventory.lease, lease) ||
            observation.threadId !== lease.ownerThreadId || observation.commandId !== original.command.commandId ||
            observation.deletion?.eventId !== original.deletion.eventId || observation.deletion.sequence !== original.deletion.sequence)
          return { lease, status: "retained", reason: "worktree_cleanup_unknown" } satisfies LeaseCleanupResultV2;
        if (observation.state !== "completed" || observation.removalOutcome?.result !== "succeeded" ||
            (observation.removalOutcome.effect !== "confirmed" && observation.removalOutcome.effect !== "absent"))
          return { lease, status: "retained", reason: observation.state === "unknown" ? "worktree_cleanup_unknown" : "worktree_cleanup_pending" } satisfies LeaseCleanupResultV2;
      }
      const admission = yield* input.sink.readDeletionWorktreePathAdmission({ path: lease.resourcePath });
      if (admission.path !== lease.resourcePath || admission.status !== "available")
        return { lease, status: "retained", reason: admission.status === "reserved" ? "path_reserved" : "path_unavailable" } satisfies LeaseCleanupResultV2;
      return yield* input.sink.withDeletionWorktreeSqlMutation({ path: lease.resourcePath }, Effect.gen(function* () {
        yield* input.leases.release(lease);
        const remaining = yield* input.leases.listAll();
        if (remaining.some((candidate) => candidate.resourcePath === lease.resourcePath &&
            candidate.leaseId === lease.leaseId && candidate.ownerThreadId === lease.ownerThreadId &&
            candidate.ownerIncarnation === lease.ownerIncarnation))
          return { lease, status: "retained", reason: "release_unconfirmed" } satisfies LeaseCleanupResultV2;
        return { lease, status: "released" } satisfies LeaseCleanupResultV2;
      }));
    }));
  });
  const reconcileLeaseOwners = Effect.gen(function* () {
    const leases = yield* input.leases.listAll();
    return yield* Effect.forEach(leases, (lease) => cleanupLeaseOwner(lease).pipe(
      Effect.catch(() => Effect.succeed({ lease, status: "retained", reason: "basis_unavailable" } satisfies LeaseCleanupResultV2)),
    ));
  });
  return { readLeaseCleanupBasis, cleanupLeaseOwner, reconcileLeaseOwners,
    observeThreadDeletionCleanup: input.sink.observeThreadDeletionCleanup };
};

export class ThreadDeletionLeaseCleanup extends Context.Service<
  ThreadDeletionLeaseCleanup,
  ReturnType<typeof makeLeaseCleanupLifecycle>
>()("t3/orchestration-v2/ThreadDeletion/LeaseCleanup") {}

export const leaseCleanupLayer = Layer.effect(ThreadDeletionLeaseCleanup, Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const leases = yield* makeWorktreeOwnershipLeaseStore();
  const resources = yield* ResourceCleanup.ResourceCleanupService;
  return makeLeaseCleanupLifecycle({ sink, leases, captureTerminals: resources.captureOwnedTerminalTargets });
}));

export const leaseCleanupWorkerLive = Layer.effectDiscard(Effect.gen(function* () {
  const cleanup = yield* ThreadDeletionLeaseCleanup;
  yield* forkParked(cleanup.reconcileLeaseOwners.pipe(
    Effect.catch(() => Effect.logWarning("Retained worktree ownership inventory is unavailable.")),
    Effect.asVoid,
    Effect.repeat(Schedule.spaced(Duration.seconds(5))),
  ));
}));
