import * as DependencyChanges from "../threadResultDependencies/DependencyChanges.ts";
import * as Stream from "effect/Stream";
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
  readonly generation: number;
  readonly shell: OrchestrationV2ThreadShell | null;
};

export class ShellMaterialization extends Context.Service<
  ShellMaterialization,
  {
    readonly forSubscription: Effect.Effect<{
      readonly getMany: (
        requests: ReadonlyArray<{ readonly threadId: ThreadId; readonly sequence: number }>,
      ) => Effect.Effect<ReadonlyMap<ThreadId, OrchestrationV2ThreadShell | null>, ReadError>;
      readonly get: (
        threadId: ThreadId,
        sequence: number,
      ) => Effect.Effect<OrchestrationV2ThreadShell | null, ReadError>;
    }>;
  }
>()("t3/jones/orchestration/shellMaterialization") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const events = yield* OrchestrationEventStore.OrchestrationEventStore;
  const scope = yield* Effect.scope;
  const newest = new Map<ThreadId, Materialization>();
  const inFlight = new Map<ThreadId, Deferred.Deferred<Materialization | undefined, ReadError>>();
  const pendingReads = new Set<Deferred.Deferred<Materialization | undefined, ReadError>>();
  const generations = new Map<ThreadId, number>();
  const dependencyChanges = yield* Effect.serviceOption(DependencyChanges.DependencyChanges);
  if (Option.isSome(dependencyChanges)) {
    const subscription = yield* dependencyChanges.value.subscribe;
    yield* Stream.fromSubscription(subscription).pipe(
      Stream.runForEach((change) =>
        Effect.sync(() => {
          for (const threadId of change.threadIds) {
            generations.set(threadId, (generations.get(threadId) ?? 0) + 1);
            newest.delete(threadId);
            inFlight.delete(threadId);
          }
        }),
      ),
      Effect.forkScoped,
    );
  }
  const generationOf = (threadId: ThreadId) =>
    Option.isSome(dependencyChanges)
      ? dependencyChanges.value.generation(threadId)
      : (generations.get(threadId) ?? 0);
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
    if (
      closed ||
      result.generation !== generationOf(threadId) ||
      (newest.get(threadId)?.version ?? -1) > result.version
    )
      return;
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
          return { version: version.value, shell, generation: generationOf(threadId) };
        }),
      )
      .pipe(
        NodeSqliteClient.readOnly,
        Effect.catchTags({
          SqlError: (cause) =>
            Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId, cause })),
        }),
      );

  type Reservation = {
    readonly threadId: ThreadId;
    readonly minimum: number;
    readonly generation: number;
    readonly pending: Deferred.Deferred<Materialization | undefined, ReadError>;
  };

  const settle = (owned: Reservation, exit: Exit.Exit<Materialization | undefined, ReadError>) => {
    if (!pendingReads.delete(owned.pending)) return;
    if (inFlight.get(owned.threadId) === owned.pending) inFlight.delete(owned.threadId);
    if (Exit.isSuccess(exit) && exit.value !== undefined && exit.value.version >= owned.minimum) {
      remember(owned.threadId, exit.value);
    }
    Deferred.doneUnsafe(owned.pending, exit);
  };

  // Capture all rows and their committed watermark together; decode after releasing SQL custody.
  const readBatchVersioned = (owned: ReadonlyArray<Reservation>) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const version = yield* events.latestApplicationSequence.pipe(Effect.option);
          if (Option.isNone(version)) return undefined;
          const decode = yield* threads.readShellSnapshot({
            threadIds: owned.map((entry) => entry.threadId),
          });
          return { version: version.value, decode };
        }),
      )
      .pipe(
        NodeSqliteClient.readOnly,
        Effect.flatMap((captured) =>
          captured === undefined
            ? Effect.sync(() => captured)
            : captured.decode.pipe(
                Effect.map((snapshot) => ({
                  version: captured.version,
                  shells: new Map(
                    [...snapshot.threads, ...snapshot.archivedThreads].map((shell) => [
                      shell.id,
                      shell,
                    ]),
                  ),
                })),
              ),
        ),
      );

  const launch = Effect.fn("ShellMaterialization.launch")(function* (
    owned: ReadonlyArray<Reservation>,
  ) {
    const scalar = (entry: Reservation) =>
      readVersioned(entry.threadId).pipe(
        Effect.map((result) =>
          result === undefined ? undefined : { ...result, generation: entry.generation },
        ),
        Effect.exit,
        Effect.tap((exit) => Effect.sync(() => settle(entry, exit))),
        Effect.asVoid,
      );
    const recover = Effect.forEach(owned, scalar, { concurrency: 8, discard: true });
    const work =
      owned.length === 1
        ? scalar(owned[0]!)
        : readBatchVersioned(owned).pipe(
            Effect.matchEffect({
              // Only typed aggregate failures use scalar attribution. Defects and interruptions propagate.
              onFailure: () => recover,
              onSuccess: (batch) =>
                Effect.sync(() => {
                  for (const entry of owned)
                    settle(
                      entry,
                      Exit.succeed(
                        batch === undefined
                          ? undefined
                          : {
                              version: batch.version,
                              generation: entry.generation,
                              shell: batch.shells.get(entry.threadId) ?? null,
                            },
                      ),
                    );
                }),
            }),
          );
    const worker = yield* work.pipe(
      Effect.exit,
      Effect.interruptible,
      Effect.forkIn(scope, { uninterruptible: false }),
    );
    // Observer registration also covers interruption before the worker's effect can start.
    worker.addObserver((exit) => {
      // The flight reports defects through each waiter, without also failing its environment scope.
      const completed = Exit.isFailure(exit) ? Exit.failCause(exit.cause) : exit.value;
      const remaining = owned.filter((entry) => pendingReads.has(entry.pending));
      // Retire every failed reservation before any resumed waiter can retry an overlapping batch.
      for (const entry of remaining) {
        pendingReads.delete(entry.pending);
        if (inFlight.get(entry.threadId) === entry.pending) inFlight.delete(entry.threadId);
      }
      for (const entry of remaining)
        Deferred.doneUnsafe(
          entry.pending,
          Exit.isFailure(completed) ? Exit.failCause(completed.cause) : Exit.interrupt(),
        );
    });
  });

  const forSubscription = Effect.sync(() => {
    const floors = new Map<ThreadId, number>();
    const unversioned = new Set<ThreadId>();
    const getMany = Effect.fnUntraced(function* (
      requests: ReadonlyArray<{ readonly threadId: ThreadId; readonly sequence: number }>,
    ) {
      const requested = new Map<ThreadId, number>();
      for (const { threadId, sequence } of requests)
        requested.set(threadId, Math.max(sequence, requested.get(threadId) ?? 0));
      const results = new Map<ThreadId, OrchestrationV2ThreadShell | null>();
      const accepted = new Map<ThreadId, Materialization>();
      while (results.size < requested.size) {
        const selected = yield* Effect.uninterruptible(
          Effect.gen(function* () {
            if (closed) return yield* Effect.interrupt;
            const owned: Reservation[] = [];
            const entries = [];
            // No yielding occurs while checking or reserving per-thread state.
            for (const [threadId, sequence] of requested) {
              if (results.has(threadId)) continue;
              const minimum = Math.max(sequence, floors.get(threadId) ?? 0);
              const cached = unversioned.has(threadId) ? undefined : newest.get(threadId);
              if (
                cached !== undefined &&
                cached.version >= minimum &&
                cached.generation === generationOf(threadId)
              ) {
                remember(threadId, cached);
                entries.push({ threadId, minimum, started: false, result: Effect.succeed(cached) });
                continue;
              }
              const pending = unversioned.has(threadId) ? undefined : inFlight.get(threadId);
              if (pending !== undefined) {
                entries.push({
                  threadId,
                  minimum,
                  started: false,
                  result: Deferred.await(pending),
                });
                continue;
              }
              const fresh = Deferred.makeUnsafe<Materialization | undefined, ReadError>();
              pendingReads.add(fresh);
              if (!unversioned.has(threadId)) inFlight.set(threadId, fresh);
              owned.push({ threadId, minimum, generation: generationOf(threadId), pending: fresh });
              entries.push({ threadId, minimum, started: true, result: Deferred.await(fresh) });
            }
            if (owned.length > 0) yield* launch(owned);
            return entries;
          }),
        );
        yield* Effect.forEach(
          selected,
          (entry) =>
            Effect.gen(function* () {
              const result = yield* entry.result;
              if (closed) return yield* Effect.interrupt;
              if (result !== undefined && result.generation !== generationOf(entry.threadId))
                return;
              if (result === undefined) {
                // Unknown watermarks never establish a reusable version. Refresh independently next time.
                const generation = generationOf(entry.threadId);
                const shell = yield* threads
                  .getThreadShell(entry.threadId)
                  .pipe(
                    NodeSqliteClient.readOnly,
                    Effect.forkIn(scope),
                    Effect.flatMap(Fiber.join),
                  );
                if (generation !== generationOf(entry.threadId)) return;
                unversioned.add(entry.threadId);
                results.set(entry.threadId, shell);
                return;
              }
              const minimum = Math.max(
                requested.get(entry.threadId)!,
                floors.get(entry.threadId) ?? 0,
              );
              if (result.version < minimum) {
                if (entry.started && result.version < entry.minimum)
                  return yield* new ShellMaterializationVersionError({
                    threadId: entry.threadId,
                    requiredSequence: minimum,
                    actualSequence: result.version,
                  });
                return;
              }
              floors.set(entry.threadId, Math.max(floors.get(entry.threadId) ?? 0, result.version));
              unversioned.delete(entry.threadId);
              accepted.set(entry.threadId, result);
              results.set(entry.threadId, result.shell);
            }),
          { concurrency: 8, discard: true },
        );
        // Another call on this subscription can advance a floor while another member waits.
        for (const [threadId] of results) {
          if (unversioned.has(threadId)) continue;
          const proof = accepted.get(threadId);
          if (
            proof === undefined ||
            proof.generation !== generationOf(threadId) ||
            proof.version < (floors.get(threadId) ?? 0)
          )
            results.delete(threadId);
        }
      }
      return results;
    });
    const get = Effect.fnUntraced(function* (threadId: ThreadId, sequence: number) {
      return (yield* getMany([{ threadId, sequence }])).get(threadId) ?? null;
    });
    return { get, getMany };
  });

  return ShellMaterialization.of({ forSubscription });
});

export const layer = Layer.effect(ShellMaterialization, make);
