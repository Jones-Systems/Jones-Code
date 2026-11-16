import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as LayerMap from "effect/LayerMap";
import * as Result from "effect/Result";
import { vi } from "vite-plus/test";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as Entries from "../../workspace/WorkspaceEntries.ts";
import * as Paths from "../../workspace/WorkspacePaths.ts";
import * as Index from "../../workspace/WorkspaceSearchIndex.ts";

const layer = Layer.mergeAll(
  NodeServices.layer,
  Layer.mock(Paths.WorkspacePaths)({ normalizeWorkspaceRoot: () => Effect.succeed("/canonical") }),
  Layer.mock(VcsProcess.VcsProcess)({}),
);

it.effect.each(["absent", "paths"] as const)(
  "overwrite refresh keeps %s content uncreated/unscanned",
  (resident) =>
    Effect.scoped(
      Effect.gen(function* () {
        const refreshed: string[] = [];
        const lookup = vi.fn(
          (
            key: string,
          ): Layer.Layer<
            Index.WorkspaceSearchIndex,
            Index.WorkspaceSearchIndexCreateFailed | Index.WorkspaceSearchIndexScanTimedOut
          > =>
            Layer.mock(Index.WorkspaceSearchIndex)({
              refresh: () => Effect.die("ordinary refresh must not run"),
              refreshContent: () =>
                Effect.sync(() => {
                  refreshed.push(key);
                }),
            }),
        );
        const indexes = yield* LayerMap.make(lookup);
        const entries = yield* Entries.make.pipe(
          Effect.provideService(Index.WorkspaceSearchIndexMap, indexes),
        );
        if (resident === "paths")
          yield* indexes.contextEffect(Index.workspaceSearchIndexKey("/canonical", "paths"));
        const initial = lookup.mock.calls.length;
        yield* entries.refreshContentAfterWrite("/canonical/.");
        expect(lookup).toHaveBeenCalledTimes(initial);
        expect(refreshed).toEqual([]);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect(
  "retains the exact content context across invalidation until strict refresh settles",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const finalized: number[] = [];
        let creations = 0;
        const indexes = yield* LayerMap.make(
          (
            _key: string,
          ): Layer.Layer<
            Index.WorkspaceSearchIndex,
            Index.WorkspaceSearchIndexCreateFailed | Index.WorkspaceSearchIndexScanTimedOut
          > =>
            Layer.effect(
              Index.WorkspaceSearchIndex,
              Effect.gen(function* () {
                const id = ++creations;
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    finalized.push(id);
                  }),
                );
                return yield* Layer.build(
                  Layer.mock(Index.WorkspaceSearchIndex)({
                    refreshContent: () =>
                      Effect.gen(function* () {
                        yield* Deferred.succeed(entered, undefined);
                        yield* Deferred.await(release);
                      }),
                  }),
                ).pipe(Effect.map((context) => Context.get(context, Index.WorkspaceSearchIndex)));
              }),
            ),
        );
        const key = Index.workspaceSearchIndexKey("/canonical", "content");
        yield* indexes.contextEffect(key);
        const entries = yield* Entries.make.pipe(
          Effect.provideService(Index.WorkspaceSearchIndexMap, indexes),
        );
        yield* Effect.gen(function* () {
          const refresh = yield* entries
            .refreshContentAfterWrite("/canonical/.")
            .pipe(Effect.forkChild);
          yield* Deferred.await(entered);
          yield* indexes.invalidate(key);
          expect(finalized).toEqual([]);
          expect(creations).toBe(1);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(refresh);
          expect(creations).toBe(1);
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect(
  "propagates strict timeout without creating a replacement or calling best-effort refresh",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const timeout = new Index.WorkspaceSearchIndexScanTimedOut({
          cwd: "/canonical",
          timeout: "15 seconds",
        });
        const lookup = vi.fn(
          (
            _key: string,
          ): Layer.Layer<
            Index.WorkspaceSearchIndex,
            Index.WorkspaceSearchIndexCreateFailed | Index.WorkspaceSearchIndexScanTimedOut
          > =>
            Layer.mock(Index.WorkspaceSearchIndex)({
              refresh: () => Effect.die("timeout must not use best-effort refresh"),
              refreshContent: () => Effect.fail(timeout),
            }),
        );
        const indexes = yield* LayerMap.make(lookup);
        yield* indexes.contextEffect(Index.workspaceSearchIndexKey("/canonical", "content"));
        const entries = yield* Entries.make.pipe(
          Effect.provideService(Index.WorkspaceSearchIndexMap, indexes),
        );
        const result = yield* entries.refreshContentAfterWrite("/canonical").pipe(Effect.result);
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result)) expect(result.failure).toBe(timeout);
        expect(lookup).toHaveBeenCalledTimes(1);
      }),
    ).pipe(Effect.provide(layer)),
);
