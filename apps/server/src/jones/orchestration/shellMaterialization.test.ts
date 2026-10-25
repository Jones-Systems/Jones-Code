import { assert, it } from "@effect/vitest";
import {
  ThreadId,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";

import { OrchestratorProjectionError } from "../../orchestration-v2/Orchestrator.ts";
import {
  archivedShellStreamItemFromThreadShell,
  shellStreamItemFromThreadShell,
} from "../../orchestration-v2/ShellStream.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import { PersistenceSqlError } from "../../persistence/Errors.ts";
import * as OrchestrationEventStore from "../../persistence/OrchestrationEventStore.ts";
import * as ShellMaterialization from "./shellMaterialization.ts";

const threadId = ThreadId.make("materialization:thread");
const shell = (title: string, archivedAt: string | null = null) =>
  ({ id: threadId, title, archivedAt }) as OrchestrationV2ThreadShell;

function boundedService<T extends object>(methods: Partial<T>): T {
  return new Proxy(methods, {
    get(target, key) {
      if (!(key in target)) throw new Error(`Unexpected service access: ${String(key)}`);
      return Reflect.get(target, key);
    },
  }) as T;
}

const testLayer = (input: {
  readonly latest: OrchestrationEventStore.OrchestrationEventStore["Service"]["latestApplicationSequence"];
  readonly read: ThreadManagementService.ThreadManagementService["Service"]["getThreadShell"];
}) =>
  ShellMaterialization.layer.pipe(
    Layer.provide(
      Layer.succeed(
        OrchestrationEventStore.OrchestrationEventStore,
        boundedService<OrchestrationEventStore.OrchestrationEventStore["Service"]>({
          latestApplicationSequence: input.latest,
        }),
      ),
    ),
    Layer.provide(
      Layer.succeed(
        ThreadManagementService.ThreadManagementService,
        boundedService<ThreadManagementService.ThreadManagementService["Service"]>({
          getThreadShell: input.read,
        }),
      ),
    ),
    Layer.provide(NodeSqliteClient.layer({ filename: ":memory:" })),
  );

it.effect.each([1, 2, 8, 32])(
  "%i materialization subscribers share one read and preserve output",
  (subscribers) =>
    Effect.gen(function* () {
      let reads = 0;
      const value = shell("shared");
      yield* Effect.gen(function* () {
        const service = yield* ShellMaterialization.ShellMaterialization;
        const readers = yield* Effect.forEach(
          Array.from({ length: subscribers }),
          () => service.forSubscription,
        );
        const results = yield* Effect.forEach(readers, (reader) => reader.get(threadId, 12), {
          concurrency: "unbounded",
        });
        assert.deepEqual(
          results,
          Array.from({ length: subscribers }, () => value),
        );
        assert.strictEqual(reads, 1);
      }).pipe(
        Effect.provide(
          testLayer({
            latest: Effect.succeed(20),
            read: () =>
              Effect.sync(() => {
                reads++;
                return value;
              }),
          }),
        ),
      );
    }),
);

it.effect("late subscribers reuse the committed version across different event sequences", () =>
  Effect.gen(function* () {
    let reads = 0;
    let version = 20;
    yield* Effect.gen(function* () {
      const service = yield* ShellMaterialization.ShellMaterialization;
      const first = yield* service.forSubscription;
      assert.strictEqual((yield* first.get(threadId, 3))?.title, "version:20");
      const late = yield* service.forSubscription;
      assert.strictEqual((yield* late.get(threadId, 17))?.title, "version:20");
      assert.strictEqual(reads, 1);
      version = 21;
      assert.strictEqual((yield* late.get(threadId, 21))?.title, "version:21");
      assert.strictEqual(reads, 2);
    }).pipe(
      Effect.provide(
        testLayer({
          latest: Effect.sync(() => version),
          read: () =>
            Effect.sync(() => {
              reads++;
              return shell(`version:${version}`);
            }),
        }),
      ),
    );
  }),
);

it.effect("waits for an older in-flight read then reads a fresh version for the newer event", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const joined = yield* Deferred.make<void>();
    let version = 10;
    let reads = 0;
    yield* Effect.gen(function* () {
      const service = yield* ShellMaterialization.ShellMaterialization;
      const first = yield* service.forSubscription;
      const second = yield* service.forSubscription;
      const older = yield* Effect.forkChild(first.get(threadId, 10));
      yield* Deferred.await(entered);
      version = 11;
      const newer = yield* Effect.forkChild(
        Deferred.succeed(joined, undefined).pipe(Effect.andThen(second.get(threadId, 11))),
      );
      yield* Deferred.await(joined);
      yield* Deferred.succeed(release, undefined);
      assert.strictEqual((yield* Fiber.join(older))?.title, "read:1");
      assert.strictEqual((yield* Fiber.join(newer))?.title, "read:2");
      assert.strictEqual(reads, 2);
    }).pipe(
      Effect.provide(
        testLayer({
          latest: Effect.sync(() => version),
          read: () =>
            Effect.gen(function* () {
              const index = ++reads;
              if (index === 1) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
              return shell(`read:${index}`);
            }),
        }),
      ),
    );
  }),
);

it.effect("bounds the LRU at 512 and retains a subscription floor after eviction", () =>
  Effect.gen(function* () {
    let reads = 0;
    let version = 100;
    yield* Effect.gen(function* () {
      const service = yield* ShellMaterialization.ShellMaterialization;
      const reader = yield* service.forSubscription;
      yield* reader.get(threadId, 1);
      version = 50;
      for (let index = 0; index < 511; index++) {
        yield* reader.get(ThreadId.make(`materialization:lru:${index}`), 1);
      }
      // A hit makes threadId newest, so adding entry 513 evicts lru:0.
      yield* reader.get(threadId, 2);
      yield* reader.get(ThreadId.make("materialization:lru:511"), 1);
      assert.strictEqual(reads, 513);
      yield* reader.get(ThreadId.make("materialization:lru:0"), 1);
      assert.strictEqual(reads, 514);
      for (let index = 1; index < 512; index++) {
        yield* reader.get(ThreadId.make(`materialization:lru:${index}`), 1);
      }
      const error = yield* reader.get(threadId, 3).pipe(Effect.flip);
      assert.strictEqual(error._tag, "ShellMaterializationVersionError");
      if (error._tag === "ShellMaterializationVersionError") {
        assert.strictEqual(error.requiredSequence, 100);
        assert.strictEqual(error.actualSequence, 50);
      }
      version = 101;
      assert.strictEqual((yield* reader.get(threadId, 4))?.title, "version:101");
      const late = yield* service.forSubscription;
      version = 40;
      assert.strictEqual((yield* late.get(threadId, 5))?.title, "version:101");
    }).pipe(
      Effect.provide(
        testLayer({
          latest: Effect.sync(() => version),
          read: () =>
            Effect.sync(() => {
              reads++;
              return shell(`version:${version}`);
            }),
        }),
      ),
    );
  }),
);

it.effect(
  "shares archive, removal and restore reads without changing emitted event sequences",
  () =>
    Effect.gen(function* () {
      let version = 10;
      let value: OrchestrationV2ThreadShell | null = shell("active");
      let reads = 0;
      yield* Effect.gen(function* () {
        const service = yield* ShellMaterialization.ShellMaterialization;
        const active = yield* service.forSubscription;
        const archive = yield* service.forSubscription;
        const archivedAt = "2026-10-01T00:00:00.000Z";
        const states = [value, shell("archived", archivedAt), null, shell("restored")];
        const types = [
          "thread.metadata-updated",
          "thread.archived",
          "thread.deleted",
          "thread.unarchived",
        ];
        for (let index = 0; index < states.length; index++) {
          value = states[index]!;
          version = index * 10 + 10;
          const sequence = index * 10 + 1;
          const stored = {
            sequence,
            event: { threadId, type: types[index], payload: { archivedAt } },
          } as OrchestrationV2StoredEvent;
          const activeItem = shellStreamItemFromThreadShell({
            stored,
            shell: yield* active.get(threadId, sequence),
          });
          const archiveItem = archivedShellStreamItemFromThreadShell({
            stored,
            shell: yield* archive.get(threadId, sequence),
          });
          assert.isTrue("sequence" in activeItem);
          if ("sequence" in activeItem) assert.strictEqual(activeItem.sequence, sequence);
          assert.strictEqual(
            activeItem.kind,
            index === 0 || index === 3 ? "thread.updated" : "thread.removed",
          );
          if (index === 0) {
            assert.isNull(archiveItem);
          } else {
            assert.strictEqual(archiveItem?.sequence, sequence);
            assert.strictEqual(
              archiveItem?.kind,
              index === 1 ? "thread.updated" : "thread.removed",
            );
          }
        }
        assert.strictEqual(reads, 4);
      }).pipe(
        Effect.provide(
          testLayer({
            latest: Effect.sync(() => version),
            read: () =>
              Effect.sync(() => {
                reads++;
                return value;
              }),
          }),
        ),
      );
    }),
);

it.effect("does not cache failures and retries the next request", () =>
  Effect.gen(function* () {
    let reads = 0;
    const failure = new OrchestratorProjectionError({ threadId });
    yield* Effect.gen(function* () {
      const service = yield* ShellMaterialization.ShellMaterialization;
      const reader = yield* service.forSubscription;
      assert.strictEqual(yield* reader.get(threadId, 1).pipe(Effect.flip), failure);
      assert.strictEqual((yield* reader.get(threadId, 1))?.title, "recovered");
      assert.strictEqual(reads, 2);
    }).pipe(
      Effect.provide(
        testLayer({
          latest: Effect.succeed(1),
          read: () =>
            Effect.suspend(() =>
              ++reads === 1 ? Effect.fail(failure) : Effect.succeed(shell("recovered")),
            ),
        }),
      ),
    );
  }),
);

it.effect("falls back to unshared reads on version failure and refreshes before reusing", () =>
  Effect.gen(function* () {
    let reads = 0;
    let versionAvailable = false;
    let version = 10;
    yield* Effect.gen(function* () {
      const service = yield* ShellMaterialization.ShellMaterialization;
      const first = yield* service.forSubscription;
      const second = yield* service.forSubscription;
      yield* Effect.all([first.get(threadId, 1), second.get(threadId, 1)], {
        concurrency: "unbounded",
      });
      assert.strictEqual(reads, 2);
      versionAvailable = true;
      const third = yield* service.forSubscription;
      yield* third.get(threadId, 1);
      assert.strictEqual(reads, 3);
      version = 20;
      // A fallback did not establish a floor; even a qualifying cached version
      // must be bypassed once, lest it predate that unversioned read.
      assert.strictEqual((yield* first.get(threadId, 2))?.title, "version:20");
      assert.strictEqual(reads, 4);
      yield* first.get(threadId, 3);
      assert.strictEqual(reads, 4);
    }).pipe(
      Effect.provide(
        testLayer({
          latest: Effect.suspend(() =>
            versionAvailable
              ? Effect.succeed(version)
              : Effect.fail(new PersistenceSqlError({ operation: "synthetic-version-read" })),
          ),
          read: () =>
            Effect.sync(() => {
              reads++;
              return shell(`version:${version}`);
            }),
        }),
      ),
    );
  }),
);

it.effect("canceling the first waiter does not cancel the shared read or another waiter", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const secondEntered = yield* Deferred.make<void>();
    let reads = 0;
    let finished = 0;
    yield* Effect.gen(function* () {
      const service = yield* ShellMaterialization.ShellMaterialization;
      const first = yield* service.forSubscription;
      const second = yield* service.forSubscription;
      const canceled = yield* Effect.forkChild(first.get(threadId, 1));
      yield* Deferred.await(entered);
      const survivor = yield* Effect.forkChild(
        Deferred.succeed(secondEntered, undefined).pipe(Effect.andThen(second.get(threadId, 1))),
      );
      yield* Deferred.await(secondEntered);
      yield* Fiber.interrupt(canceled);
      assert.strictEqual(finished, 0);
      yield* Deferred.succeed(release, undefined);
      assert.strictEqual((yield* Fiber.join(survivor))?.title, "survivor");
      assert.strictEqual(reads, 1);
      assert.strictEqual(finished, 1);
    }).pipe(
      Effect.provide(
        testLayer({
          latest: Effect.succeed(1),
          read: () =>
            Effect.gen(function* () {
              reads++;
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              return shell("survivor");
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  finished++;
                }),
              ),
            ),
        }),
      ),
    );
  }),
);

it.effect(
  "service close interrupts pending reads and prevents reuse of retained cached values",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        let finished = false;
        const lifetime = yield* Effect.acquireRelease(Scope.make(), (scope) =>
          Scope.close(scope, Exit.void),
        );
        const context = yield* Layer.build(
          testLayer({
            latest: Effect.succeed(1),
            read: (id) =>
              id === threadId
                ? Effect.succeed(shell("cached"))
                : Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.ensuring(
                      Effect.sync(() => {
                        finished = true;
                      }),
                    ),
                  ),
          }),
        ).pipe(Scope.provide(lifetime));
        const service = Context.get(context, ShellMaterialization.ShellMaterialization);
        const reader = yield* service.forSubscription;
        yield* reader.get(threadId, 1);
        const pending = yield* Effect.forkChild(
          reader.get(ThreadId.make("materialization:blocked"), 1),
        );
        yield* Deferred.await(entered);
        yield* Scope.close(lifetime, Exit.void);
        assert.isTrue(Exit.isFailure(yield* Fiber.await(pending)));
        assert.isTrue(finished);
        assert.isTrue(Exit.isFailure(yield* reader.get(threadId, 1).pipe(Effect.exit)));
      }),
    ),
);

it.effect("separate environment layers never reuse another environment's shell", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const contexts = yield* Effect.forEach(["first environment", "second environment"], (title) =>
        Layer.build(
          testLayer({ latest: Effect.succeed(10), read: () => Effect.succeed(shell(title)) }),
        ),
      );
      const readers = yield* Effect.forEach(
        contexts,
        (context) =>
          Context.get(context, ShellMaterialization.ShellMaterialization).forSubscription,
      );
      assert.strictEqual((yield* readers[0]!.get(threadId, 1))?.title, "first environment");
      assert.strictEqual((yield* readers[1]!.get(threadId, 1))?.title, "second environment");
    }),
  ),
);

it.effect("service close releases a waiter even when its read fiber has not started", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const lifetime = yield* Effect.acquireRelease(Scope.make(), (scope) =>
        Scope.close(scope, Exit.void),
      );
      const context = yield* Layer.build(
        testLayer({ latest: Effect.succeed(1), read: () => Effect.never }),
      ).pipe(Scope.provide(lifetime));
      const reader = yield* Context.get(context, ShellMaterialization.ShellMaterialization)
        .forSubscription;
      const waiter = yield* Effect.forkChild(reader.get(threadId, 1), { startImmediately: true });
      yield* Scope.close(lifetime, Exit.void);
      assert.isTrue(Exit.isFailure(yield* Fiber.await(waiter)));
    }),
  ),
);
