import {
  CommandId,
  CorrelationId,
  EventId,
  type OrchestrationEvent,
  ThreadId,
} from "@t3tools/contracts";
import { it as effectIt } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { describe, expect, it } from "vite-plus/test";

import {
  ProviderService,
  type ProviderServiceShape,
} from "../../provider/Services/ProviderService.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine.ts";
import { ThreadDeletionReactor } from "../Services/ThreadDeletionReactor.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  logCleanupCauseUnlessInterrupted,
  ThreadDeletionReactorLive,
  WORKTREE_OWNERSHIP_CLEANUP_RETRY_INTERVAL,
} from "./ThreadDeletionReactor.ts";

describe("logCleanupCauseUnlessInterrupted", () => {
  const threadId = ThreadId.make("thread-deletion-reactor-test");

  it("swallows ordinary cleanup failures", async () => {
    const exit = await Effect.runPromiseExit(
      logCleanupCauseUnlessInterrupted({
        effect: Effect.fail("cleanup failed"),
        message: "thread deletion cleanup skipped provider session stop",
        threadId,
      }),
    );

    expect(Exit.isSuccess(exit)).toBe(true);
  });

  it("preserves interrupt causes", async () => {
    const exit = await Effect.runPromiseExit(
      logCleanupCauseUnlessInterrupted({
        effect: Effect.interrupt,
        message: "thread deletion cleanup skipped provider session stop",
        threadId,
      }),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
    }
  });
});

describe("ThreadDeletionReactor drain", () => {
  const now = "2026-01-01T00:00:00.000Z";
  const threadId = ThreadId.make("thread-deletion-reactor-drain");
  const deletedEvent = (sequence: number, eventThreadId = threadId): OrchestrationEvent => ({
    sequence,
    eventId: EventId.make(`evt-deleted-${sequence}`),
    aggregateKind: "thread",
    aggregateId: eventThreadId,
    type: "thread.deleted",
    occurredAt: now,
    commandId: CommandId.make(`cmd-deleted-${sequence}`),
    causationEventId: null,
    correlationId: CorrelationId.make(`cmd-deleted-${sequence}`),
    metadata: {},
    payload: { threadId: eventThreadId, deletedAt: now },
  });
  const lease = {
    resourcePath: "/workspace/project",
    leaseId: "lease-thread-deletion",
    ownerThreadId: threadId,
    ownerIncarnation: "event-thread-deletion-created",
    branch: null,
    acquiredAtMs: 0,
    renewedAtMs: 0,
    expiresAtMs: 300_000,
  } as const;
  const projectionSnapshotQuery = {
    getCommandReadModel: () => Effect.succeed({ threads: [{ id: threadId }] } as never),
  } as unknown as ProjectionSnapshotQuery["Service"];

  effectIt.effect("waits for a published deletion the subscriber has not consumed yet", () =>
    Effect.gen(function* () {
      const stops: Array<number> = [];
      const releases: Array<string> = [];
      const firstCleanupDone = yield* Deferred.make<void>();
      // The engine has already committed and published sequence 2, but the
      // subscriber has not received it yet: the stream releases it on demand.
      const releaseSecondEvent = yield* Deferred.make<void>();
      const latestSequence = yield* Ref.make(0);
      const engine = {
        latestSequence: Ref.get(latestSequence),
        releaseWorktreeOwnership: (releasedLease: typeof lease) =>
          Effect.sync(() => {
            releases.push(releasedLease.leaseId);
          }),
        listWorktreeOwnershipLeases: Effect.succeed([lease]),
        getThreadOwnershipIncarnation: () => Effect.succeed(Option.some(lease.ownerIncarnation)),
        streamDomainEvents: Stream.concat(
          Stream.make(deletedEvent(1)),
          Stream.fromEffect(Deferred.await(releaseSecondEvent)).pipe(
            Stream.map(() => deletedEvent(2)),
          ),
        ),
      } as unknown as OrchestrationEngineShape;
      const providerService = {
        stopSession: () =>
          Effect.gen(function* () {
            stops.push(stops.length + 1);
            if (stops.length === 1) {
              yield* Deferred.succeed(firstCleanupDone, undefined);
            }
          }),
      } as unknown as ProviderServiceShape;
      const terminalManager = {
        close: () => Effect.void,
      } as unknown as TerminalManager.TerminalManager["Service"];
      const layer = ThreadDeletionReactorLive.pipe(
        Layer.provide(Layer.succeed(ProviderService, providerService)),
        Layer.provide(Layer.succeed(TerminalManager.TerminalManager, terminalManager)),
        Layer.provide(Layer.succeed(OrchestrationEngineService, engine)),
        Layer.provide(Layer.succeed(ProjectionSnapshotQuery, projectionSnapshotQuery)),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const reactor = yield* ThreadDeletionReactor;
          yield* reactor.start();
          yield* Deferred.await(firstCleanupDone);

          // Sequence 1 is fully cleaned and the worker queue is idle. Sequence
          // 2 is committed and published but still in flight to the subscriber.
          yield* Ref.set(latestSequence, 2);
          const drained = yield* Effect.forkChild(reactor.drainThrough(2, threadId));
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;
          expect(stops).toEqual([1]);
          expect(drained.pollUnsafe()).toBeUndefined();

          yield* Deferred.succeed(releaseSecondEvent, undefined);
          yield* Fiber.join(drained);
          expect(stops).toEqual([1, 2]);
          expect(releases).toEqual([lease.leaseId, lease.leaseId]);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );

  effectIt.effect("retains ownership and retries incomplete deletion cleanup", () =>
    Effect.gen(function* () {
      const releases: Array<string> = [];
      const cleanupAttempted = yield* Deferred.make<void>();
      const terminalCleanupAttempted = yield* Deferred.make<void>();
      let stopAttempts = 0;
      let terminalsClosed = false;
      const engine = {
        latestSequence: Effect.succeed(1),
        releaseWorktreeOwnership: (releasedLease: typeof lease) =>
          Effect.sync(() => {
            releases.push(releasedLease.leaseId);
          }),
        listWorktreeOwnershipLeases: Effect.succeed([lease]),
        getThreadOwnershipIncarnation: () => Effect.succeed(Option.some(lease.ownerIncarnation)),
        streamDomainEvents: Stream.make(deletedEvent(1)),
      } as unknown as OrchestrationEngineShape;
      const providerService = {
        stopSession: () =>
          Effect.gen(function* () {
            stopAttempts += 1;
            if (stopAttempts === 1) {
              yield* Deferred.succeed(cleanupAttempted, undefined);
              return yield* Effect.fail("provider stop failed");
            }
          }),
      } as unknown as ProviderServiceShape;
      const terminalManager = {
        close: () =>
          Effect.sync(() => {
            terminalsClosed = true;
          }).pipe(Effect.andThen(Deferred.succeed(terminalCleanupAttempted, undefined))),
      } as unknown as TerminalManager.TerminalManager["Service"];
      const layer = ThreadDeletionReactorLive.pipe(
        Layer.provide(Layer.succeed(ProviderService, providerService)),
        Layer.provide(Layer.succeed(TerminalManager.TerminalManager, terminalManager)),
        Layer.provide(Layer.succeed(OrchestrationEngineService, engine)),
        Layer.provide(Layer.succeed(ProjectionSnapshotQuery, projectionSnapshotQuery)),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const reactor = yield* ThreadDeletionReactor;
          yield* reactor.start();
          yield* Deferred.await(cleanupAttempted);
          yield* Deferred.await(terminalCleanupAttempted);
          expect(terminalsClosed).toBe(true);
          expect(releases).toEqual([]);
          const drained = yield* Effect.forkChild(reactor.drainThrough(1, threadId));
          yield* TestClock.adjust(WORKTREE_OWNERSHIP_CLEANUP_RETRY_INTERVAL);
          yield* Fiber.join(drained);
          expect(stopAttempts).toBe(2);
          expect(releases).toEqual([lease.leaseId]);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );

  effectIt.effect("resumes cleanup for a retained lease whose owner is absent at startup", () =>
    Effect.gen(function* () {
      const released = yield* Deferred.make<void>();
      const stops: ThreadId[] = [];
      const engine = {
        latestSequence: Effect.succeed(0),
        releaseWorktreeOwnership: () => Deferred.succeed(released, undefined),
        listWorktreeOwnershipLeases: Effect.succeed([lease]),
        getThreadOwnershipIncarnation: () => Effect.succeed(Option.none()),
        streamDomainEvents: Stream.never,
      } as unknown as OrchestrationEngineShape;
      const providerService = {
        stopSession: ({ threadId: stoppedThreadId }: { readonly threadId: ThreadId }) =>
          Effect.sync(() => {
            stops.push(stoppedThreadId);
          }),
      } as unknown as ProviderServiceShape;
      const terminalManager = {
        close: () => Effect.void,
      } as unknown as TerminalManager.TerminalManager["Service"];
      const emptyProjection = {
        getCommandReadModel: () => Effect.succeed({ threads: [] } as never),
      } as unknown as ProjectionSnapshotQuery["Service"];
      const layer = ThreadDeletionReactorLive.pipe(
        Layer.provide(Layer.succeed(ProviderService, providerService)),
        Layer.provide(Layer.succeed(TerminalManager.TerminalManager, terminalManager)),
        Layer.provide(Layer.succeed(OrchestrationEngineService, engine)),
        Layer.provide(Layer.succeed(ProjectionSnapshotQuery, emptyProjection)),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const reactor = yield* ThreadDeletionReactor;
          yield* reactor.start();
          yield* Deferred.await(released);
          expect(stops).toEqual([threadId]);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );

  effectIt.effect("recovers a retained lease from an older incarnation of the same thread id", () =>
    Effect.gen(function* () {
      const released = yield* Deferred.make<void>();
      const engine = {
        latestSequence: Effect.succeed(0),
        releaseWorktreeOwnership: () => Deferred.succeed(released, undefined),
        listWorktreeOwnershipLeases: Effect.succeed([lease]),
        getThreadOwnershipIncarnation: () => Effect.succeed(Option.some("event-new-incarnation")),
        streamDomainEvents: Stream.never,
      } as unknown as OrchestrationEngineShape;
      const layer = ThreadDeletionReactorLive.pipe(
        Layer.provide(
          Layer.succeed(ProviderService, {
            stopSession: () => Effect.void,
          } as unknown as ProviderServiceShape),
        ),
        Layer.provide(
          Layer.succeed(TerminalManager.TerminalManager, {
            close: () => Effect.void,
          } as unknown as TerminalManager.TerminalManager["Service"]),
        ),
        Layer.provide(Layer.succeed(OrchestrationEngineService, engine)),
        Layer.provide(Layer.succeed(ProjectionSnapshotQuery, projectionSnapshotQuery)),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const reactor = yield* ThreadDeletionReactor;
          yield* reactor.start();
          yield* Deferred.await(released);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );

  effectIt.effect("does not let one owner's retry block cleanup for another thread", () =>
    Effect.gen(function* () {
      const blockedThreadId = ThreadId.make("thread-cleanup-blocked");
      const laterThreadId = ThreadId.make("thread-cleanup-later");
      const blockedLease = {
        ...lease,
        leaseId: "lease-blocked",
        ownerThreadId: blockedThreadId,
        ownerIncarnation: "event-blocked-created",
      };
      const laterLease = {
        ...lease,
        leaseId: "lease-later",
        ownerThreadId: laterThreadId,
        ownerIncarnation: "event-later-created",
      };
      const laterReleased = yield* Deferred.make<void>();
      const engine = {
        latestSequence: Effect.succeed(0),
        releaseWorktreeOwnership: (releasedLease: typeof lease) =>
          releasedLease.ownerThreadId === laterThreadId
            ? Deferred.succeed(laterReleased, undefined)
            : Effect.void,
        listWorktreeOwnershipLeases: Effect.succeed([blockedLease, laterLease]),
        getThreadOwnershipIncarnation: (candidate: ThreadId) =>
          Effect.succeed(
            Option.some(
              candidate === blockedThreadId
                ? blockedLease.ownerIncarnation
                : laterLease.ownerIncarnation,
            ),
          ),
        streamDomainEvents: Stream.make(
          deletedEvent(1, blockedThreadId),
          deletedEvent(2, laterThreadId),
        ),
      } as unknown as OrchestrationEngineShape;
      const layer = ThreadDeletionReactorLive.pipe(
        Layer.provide(
          Layer.succeed(ProviderService, {
            stopSession: ({ threadId: candidate }: { readonly threadId: ThreadId }) =>
              candidate === blockedThreadId ? Effect.fail("still running") : Effect.void,
          } as unknown as ProviderServiceShape),
        ),
        Layer.provide(
          Layer.succeed(TerminalManager.TerminalManager, {
            close: () => Effect.void,
          } as unknown as TerminalManager.TerminalManager["Service"]),
        ),
        Layer.provide(Layer.succeed(OrchestrationEngineService, engine)),
        Layer.provide(
          Layer.succeed(ProjectionSnapshotQuery, {
            getCommandReadModel: () =>
              Effect.succeed({
                threads: [{ id: blockedThreadId }, { id: laterThreadId }],
              } as never),
          } as unknown as ProjectionSnapshotQuery["Service"]),
        ),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const reactor = yield* ThreadDeletionReactor;
          yield* reactor.start();
          yield* Deferred.await(laterReleased);
          yield* reactor.drainThrough(2, laterThreadId);
        }),
      ).pipe(Effect.provide(layer));
    }),
  );
});
