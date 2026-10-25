import type { OrchestrationV2ThreadShell, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as Orchestrator from "../../orchestration-v2/Orchestrator.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";

class ShellMaterializationVersionError extends Schema.TaggedError<ShellMaterializationVersionError>()(
  "ShellMaterializationVersionError",
  {
    threadId: Schema.String,
    requiredSequence: Schema.Number,
    actualSequence: Schema.Number,
  },
) {
  override get message(): string {
    return "The shell read did not reach the required committed sequence.";
  }
}

type ReadError = Orchestrator.OrchestratorV2Error | ShellMaterializationVersionError;
type Materialization = {
  readonly version: number;
  readonly shell: OrchestrationV2ThreadShell | null;
};

export class ShellMaterialization extends Context.Service<
  ShellMaterialization,
  {
    readonly forSubscription: Effect.Effect<{
      readonly get: (
        threadId: ThreadId,
        sequence: number,
      ) => Effect.Effect<OrchestrationV2ThreadShell | null, ReadError>;
    }>;
  }
>()("jones/orchestration/ShellMaterialization") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const events = yield* OrchestrationEventStore.OrchestrationEventStore;
  const scope = yield* Effect.scope;
  const newest = new Map<ThreadId, Materialization>();
  const inFlight = new Map<ThreadId, Deferred.Deferred<Materialization | undefined, ReadError>>();
  const pendingReads = new Set<Deferred.Deferred<Materialization | undefined, ReadError>>();
  let closed = false;
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      closed = true;
      newest.clear();
      inFlight.clear();
      // A fiber interrupted before its body starts may not run its onExit.
      // Release those waiters as well as clearing completed materializations.
      const pending = [...pendingReads];
      pendingReads.clear();
      yield* Effect.forEach(pending, Deferred.interrupt, { discard: true });
    }),
  );

  const remember = (threadId: ThreadId, result: Materialization) => {
    if (closed || (newest.get(threadId)?.version ?? -1) > result.version) return;
    newest.delete(threadId);
    newest.set(threadId, result);
    if (newest.size > 512) {
      const oldest = newest.keys().next().value;
      if (oldest !== undefined) newest.delete(oldest);
    }
  };

  const readVersioned = (threadId: ThreadId) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          // This first read fixes the WAL snapshot. getThreadShell's nested
          // transaction uses a savepoint on this same reader connection.
          const version = yield* events.latestApplicationSequence.pipe(Effect.option);
          if (Option.isNone(version)) return undefined;
          const shell = yield* threads.getThreadShell(threadId);
          return { version: version.value, shell };
        }),
      )
      .pipe(
        NodeSqliteClient.readOnly,
        Effect.catchTags({
          SqlError: (cause) =>
            Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId, cause })),
        }),
      );

  const read = Effect.fnUntraced(function* (
    threadId: ThreadId,
    minimum: number,
    independent: boolean,
  ) {
    while (true) {
      const selected = yield* Effect.uninterruptible(
        Effect.gen(function* () {
          if (closed) return yield* Effect.interrupt;
          if (!independent) {
            const cached = newest.get(threadId);
            if (cached !== undefined && cached.version >= minimum) {
              remember(threadId, cached);
              return { kind: "cached", result: cached } as const;
            }
            const pending = inFlight.get(threadId);
            if (pending !== undefined) return { kind: "pending", pending, started: false } as const;
          }

          const pending = Deferred.makeUnsafe<Materialization | undefined, ReadError>();
          pendingReads.add(pending);
          if (!independent) inFlight.set(threadId, pending);
          // Only the environment's scope owns this fiber. Canceling a waiter
          // must not cancel another subscription's read or leave an orphan.
          yield* readVersioned(threadId).pipe(
            Effect.onExit((exit) =>
              Effect.sync(() => {
                pendingReads.delete(pending);
                if (inFlight.get(threadId) === pending) inFlight.delete(threadId);
                if (
                  Exit.isSuccess(exit) &&
                  exit.value !== undefined &&
                  exit.value.version >= minimum
                ) {
                  remember(threadId, exit.value);
                }
              }).pipe(Effect.andThen(Deferred.done(pending, exit))),
            ),
            Effect.ignoreCause,
            Effect.forkIn(scope, { uninterruptible: false }),
          );
          return { kind: "pending", pending, started: true } as const;
        }),
      );
      if (selected.kind === "cached") return selected.result;
      const result = yield* Deferred.await(selected.pending);
      if (result === undefined || result.version >= minimum) return result;
      // A read already in flight can predate this event. Wait for it, then
      // retry against a fresh snapshot; never label its result with the event.
      if (selected.started) {
        return yield* new ShellMaterializationVersionError({
          threadId,
          requiredSequence: minimum,
          actualSequence: result.version,
        });
      }
    }
  });

  const forSubscription = Effect.sync(() => {
    const floors = new Map<ThreadId, number>();
    const unversioned = new Set<ThreadId>();
    const get = Effect.fnUntraced(function* (threadId: ThreadId, sequence: number) {
      const minimum = Math.max(sequence, floors.get(threadId) ?? 0);
      const result = yield* read(threadId, minimum, unversioned.has(threadId));
      if (result === undefined) {
        // Version failures preserve the old, unshared read path. Its version
        // is unknown, so this subscription must read afresh before reusing.
        const shell = yield* threads
          .getThreadShell(threadId)
          .pipe(NodeSqliteClient.readOnly, Effect.forkIn(scope), Effect.flatMap(Fiber.join));
        unversioned.add(threadId);
        return shell;
      }
      floors.set(threadId, Math.max(floors.get(threadId) ?? 0, result.version));
      unversioned.delete(threadId);
      return result.shell;
    });
    return { get };
  });

  return ShellMaterialization.of({ forSubscription });
});

export const layer = Layer.effect(ShellMaterialization, make);
