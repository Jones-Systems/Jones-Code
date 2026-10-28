import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ServerCompute } from "../serverCompute/ServerCompute.ts";
import { ComputeFailure, computeFailure } from "../serverCompute/failure.ts";
import * as NativeSearchRuntime from "../workspace/nativeSearchRuntime.ts";
import type { WorkspaceJobs } from "./computeContract.ts";

/** Existing domain tests exercise real finder behavior without depending on
 * Vitest's argv being an application bin entry. Process isolation is qualified
 * separately by nativeProcess.integration.test.ts; this is no process evidence.
 */
export const nativeWorkspaceTestLayer = Layer.effect(
  ServerCompute,
  Effect.gen(function* () {
    type Runtime = Effect.Success<ReturnType<typeof NativeSearchRuntime.make>>;
    const indexes = new Map<string, Runtime>();
    let id = 0;
    return ServerCompute.of({
      acquireLogical: (key) =>
        Effect.gen(function* () {
          const index = yield* NativeSearchRuntime.make(key.cwd, key.variant).pipe(
            Effect.mapError(
              (cause) =>
                new ComputeFailure({
                  reason: "finder-create",
                  cause,
                  scanTimedOut: cause._tag === "WorkspaceSearchIndexScanTimedOut",
                }),
            ),
          );
          const handle = { handleId: `native-test-${++id}`, key };
          indexes.set(handle.handleId, index);
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              indexes.delete(handle.handleId);
            }),
          );
          return handle;
        }),
      run: <K extends keyof WorkspaceJobs>(
        handle: { readonly handleId: string },
        operation: K,
        input: WorkspaceJobs[K]["input"],
      ) =>
        Effect.suspend(() => {
          const index = indexes.get(handle.handleId);
          if (!index) return Effect.fail(computeFailure("unavailable"));
          const handlers: {
            [P in keyof WorkspaceJobs]: (
              input: WorkspaceJobs[P]["input"],
            ) => Effect.Effect<WorkspaceJobs[P]["output"], unknown>;
          } = {
            list: () => index.list(),
            search: (value) => index.search(value.query, value.limit, value.kind, value.imageOnly),
            searchContents: (value) => index.searchContents(value),
            refresh: () => index.refresh(),
          };
          return handlers[operation](input).pipe(
            Effect.mapError(
              (cause) =>
                new ComputeFailure({
                  reason: operation === "refresh" ? "finder-refresh" : "finder-search",
                  cause,
                  scanTimedOut:
                    typeof cause === "object" &&
                    cause !== null &&
                    "_tag" in cause &&
                    cause._tag === "WorkspaceSearchIndexScanTimedOut",
                }),
            ),
          );
        }),
    });
  }),
);
