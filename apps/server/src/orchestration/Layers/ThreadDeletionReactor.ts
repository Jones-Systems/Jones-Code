import type { OrchestrationEvent } from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import { ProviderService } from "../../provider/Services/ProviderService.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ThreadDeletionReactor,
  type ThreadDeletionReactorShape,
} from "../Services/ThreadDeletionReactor.ts";
import type { WorktreeOwnershipLease } from "../WorktreeOwnershipLease.ts";
import { forkParked } from "../../serverActivation.ts";

type ThreadDeletedEvent = Extract<OrchestrationEvent, { type: "thread.deleted" }>;

export const WORKTREE_OWNERSHIP_CLEANUP_RETRY_INTERVAL = Duration.seconds(5);

export const logCleanupCauseUnlessInterrupted = <R, E>({
  effect,
  message,
  threadId,
}: {
  readonly effect: Effect.Effect<void, E, R>;
  readonly message: string;
  readonly threadId: ThreadDeletedEvent["payload"]["threadId"];
}): Effect.Effect<void, E, R> =>
  effect.pipe(
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterruptsOnly(cause),
      (cause) =>
        Effect.logDebug(message, {
          threadId,
          cause: Cause.pretty(cause),
        }),
    ),
  );

const make = Effect.gen(function* () {
  const orchestrationEngine = yield* OrchestrationEngineService;
  const providerService = yield* ProviderService;
  const terminalManager = yield* TerminalManager.TerminalManager;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const pendingCleanup = new Map<
    ThreadDeletedEvent["payload"]["threadId"],
    Deferred.Deferred<void>
  >();

  const stopProviderSession = (threadId: ThreadDeletedEvent["payload"]["threadId"]) =>
    providerService.stopSession({ threadId }).pipe(
      Effect.as(true),
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.logDebug("thread deletion cleanup skipped provider session stop", {
              threadId,
              cause: Cause.pretty(cause),
            }).pipe(Effect.as(false)),
      ),
    );

  const closeThreadTerminals = (threadId: ThreadDeletedEvent["payload"]["threadId"]) =>
    terminalManager.close({ threadId, deleteHistory: true }).pipe(
      Effect.as(true),
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.logDebug("thread deletion cleanup skipped terminal close", {
              threadId,
              cause: Cause.pretty(cause),
            }).pipe(Effect.as(false)),
      ),
    );

  const cleanupAndRelease = Effect.fn("cleanupAndRelease")(function* (
    threadId: ThreadDeletedEvent["payload"]["threadId"],
    leases: ReadonlyArray<WorktreeOwnershipLease>,
  ) {
    const providerStopped = yield* stopProviderSession(threadId);
    const terminalsClosed = yield* closeThreadTerminals(threadId);
    if (providerStopped && terminalsClosed) {
      yield* Effect.forEach(leases, orchestrationEngine.releaseWorktreeOwnership, {
        discard: true,
      });
      return true;
    }
    yield* Effect.logWarning("retaining worktree ownership after incomplete thread cleanup", {
      threadId,
      providerStopped,
      terminalsClosed,
    });
    return false;
  });

  const tryCleanupAndRelease = (
    threadId: ThreadDeletedEvent["payload"]["threadId"],
    leases: ReadonlyArray<WorktreeOwnershipLease>,
  ) =>
    cleanupAndRelease(threadId, leases).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.logWarning("failed to reconcile retained worktree ownership", {
              threadId,
              cause: Cause.pretty(cause),
            }).pipe(Effect.as(false)),
      ),
    );

  const cleanupUntilReleased = (
    threadId: ThreadDeletedEvent["payload"]["threadId"],
    leases: ReadonlyArray<WorktreeOwnershipLease>,
  ): Effect.Effect<void> =>
    tryCleanupAndRelease(threadId, leases).pipe(
      Effect.flatMap((released) =>
        released
          ? Effect.void
          : Effect.sleep(WORKTREE_OWNERSHIP_CLEANUP_RETRY_INTERVAL).pipe(
              Effect.andThen(Effect.suspend(() => cleanupUntilReleased(threadId, leases))),
            ),
      ),
    );

  const retryCleanupInBackground = Effect.fn("retryCleanupInBackground")(function* (
    threadId: ThreadDeletedEvent["payload"]["threadId"],
    leases: ReadonlyArray<WorktreeOwnershipLease>,
    delayFirstRetry = false,
  ) {
    if (pendingCleanup.has(threadId)) return;
    const completed = yield* Deferred.make<void>();
    pendingCleanup.set(threadId, completed);
    yield* forkParked(
      (delayFirstRetry
        ? Effect.sleep(WORKTREE_OWNERSHIP_CLEANUP_RETRY_INTERVAL).pipe(
            Effect.andThen(cleanupUntilReleased(threadId, leases)),
          )
        : cleanupUntilReleased(threadId, leases)
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (pendingCleanup.get(threadId) === completed) {
              pendingCleanup.delete(threadId);
            }
          }).pipe(Effect.andThen(Deferred.succeed(completed, undefined)), Effect.uninterruptible),
        ),
      ),
    );
  });

  const processThreadDeleted = Effect.fn("processThreadDeleted")(function* (
    event: ThreadDeletedEvent,
  ) {
    const { threadId } = event.payload;
    const leases = yield* orchestrationEngine.listWorktreeOwnershipLeases.pipe(
      Effect.map((allLeases) => allLeases.filter((lease) => lease.ownerThreadId === threadId)),
      Effect.retry({ times: 1 }),
    );
    if (!(yield* tryCleanupAndRelease(threadId, leases))) {
      yield* retryCleanupInBackground(threadId, leases, true);
    }
  });

  const processThreadDeletedSafely = (event: ThreadDeletedEvent) =>
    processThreadDeleted(event).pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterruptsOnly(cause),
        (cause) =>
          Effect.logWarning("thread deletion reactor failed to process event", {
            eventType: event.type,
            threadId: event.payload.threadId,
            cause: Cause.pretty(cause),
          }),
      ),
    );

  const worker = yield* makeDrainableWorker(processThreadDeletedSafely);

  // Highest event sequence the subscriber has handed to the worker. Waiting
  // through a successful thread.created sequence covers every deletion that
  // was ahead of that create in the engine queue; the worker drain then covers
  // the in-flight cleanup.
  const seenSequence = yield* SubscriptionRef.make(0);
  const noteSeen = (sequence: number) =>
    SubscriptionRef.update(seenSequence, (seen) => Math.max(seen, sequence));

  const start: ThreadDeletionReactorShape["start"] = Effect.fn("start")(function* () {
    yield* forkParked(
      Stream.runForEach(
        orchestrationEngine.streamDomainEvents.pipe(
          // Events that landed before the subscription are not replayed, so
          // start the watermark at the current head instead of zero.
          Stream.onStart(orchestrationEngine.latestSequence.pipe(Effect.flatMap(noteSeen))),
        ),
        (event) =>
          (event.type === "thread.deleted" ? worker.enqueue(event) : Effect.void).pipe(
            Effect.andThen(noteSeen(event.sequence)),
          ),
      ),
    );

    const recoverySnapshot = yield* Effect.all({
      readModel: projectionSnapshotQuery.getCommandReadModel(),
      leases: orchestrationEngine.listWorktreeOwnershipLeases,
    }).pipe(
      Effect.map(Option.some),
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.logWarning("failed to inspect retained worktree ownership at startup", {
              cause: Cause.pretty(cause),
            }).pipe(Effect.as(Option.none())),
      ),
    );
    if (Option.isNone(recoverySnapshot)) {
      return;
    }
    const { threads } = recoverySnapshot.value.readModel;
    const activeThreadIds = new Set(threads.map((thread) => thread.id));
    const orphanedLeases: WorktreeOwnershipLease[] = [];
    for (const lease of recoverySnapshot.value.leases) {
      if (!activeThreadIds.has(lease.ownerThreadId)) {
        orphanedLeases.push(lease);
        continue;
      }
      const currentIncarnationResult = yield* orchestrationEngine
        .getThreadOwnershipIncarnation(lease.ownerThreadId)
        .pipe(
          Effect.map((value) => Option.some(value)),
          Effect.catchCause((cause) =>
            Cause.hasInterrupts(cause)
              ? Effect.interrupt
              : Effect.logWarning("failed to inspect worktree owner incarnation", {
                  threadId: lease.ownerThreadId,
                  cause: Cause.pretty(cause),
                }).pipe(Effect.as(Option.none())),
          ),
        );
      if (Option.isNone(currentIncarnationResult)) {
        continue;
      }
      const currentIncarnation = currentIncarnationResult.value;
      if (
        Option.isNone(currentIncarnation) ||
        currentIncarnation.value !== lease.ownerIncarnation
      ) {
        orphanedLeases.push(lease);
      }
    }
    const orphanedOwners = new Map<
      ThreadDeletedEvent["payload"]["threadId"],
      typeof orphanedLeases
    >();
    for (const lease of orphanedLeases) {
      orphanedOwners.set(lease.ownerThreadId, [
        ...(orphanedOwners.get(lease.ownerThreadId) ?? []),
        lease,
      ]);
    }
    for (const [threadId, leases] of orphanedOwners) {
      yield* retryCleanupInBackground(threadId, leases);
    }
  });

  const drainThrough: ThreadDeletionReactorShape["drainThrough"] = Effect.fn(
    "ThreadDeletionReactor.drainThrough",
  )(function* (target, threadId) {
    yield* SubscriptionRef.changes(seenSequence).pipe(
      Stream.filter((seen) => seen >= target),
      Stream.runHead,
    );
    yield* worker.drain;
    if (threadId !== undefined) {
      let pending = pendingCleanup.get(threadId);
      while (pending !== undefined) {
        yield* Deferred.await(pending);
        pending = pendingCleanup.get(threadId);
      }
    }
  });

  return {
    start,
    drainThrough,
  } satisfies ThreadDeletionReactorShape;
});

export const ThreadDeletionReactorLive = Layer.effect(ThreadDeletionReactor, make);
