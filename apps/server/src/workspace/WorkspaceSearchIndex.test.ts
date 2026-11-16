import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type {
  LogicalFinderHandle,
  WorkspaceJobs,
} from "../jones/serverParallelism/computeContract.ts";
import type { ComputeFailure } from "../jones/serverCompute/failure.ts";
import * as ServerCompute from "../jones/serverCompute/ServerCompute.ts";
import * as WorkspaceSearchIndex from "./WorkspaceSearchIndex.ts";

// Boundary exception: this small integration test stays at the upstream service hook.
// Algorithm/native fixtures moved to jones/workspace/nativeSearchRuntime.test.ts.
it.effect(
  "constructs the existing layer/key and routes every fresh call through one logical facade",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const handle = {
          handleId: "facade",
          key: { cwd: "/synthetic", variant: "paths" as const },
        };
        const calls: string[] = [];
        let acquisitions = 0;
        const compute = ServerCompute.ServerCompute.of({
          acquireLogical: (key) =>
            Effect.sync(() => {
              acquisitions++;
              expect(key).toEqual(handle.key);
              return handle;
            }),
          run: <K extends keyof WorkspaceJobs>(
            logical: LogicalFinderHandle,
            operation: K,
          ): Effect.Effect<WorkspaceJobs[K]["output"], ComputeFailure> => {
            expect(logical).toBe(handle);
            calls.push(operation);
            // This generic fake represents only the external compute boundary; service/proxy remain real.
            return Effect.succeed(
              (operation === "refresh"
                ? undefined
                : operation === "searchContents"
                  ? { matches: [], truncated: false }
                  : {
                      entries: [{ path: "src/a.ts", kind: "file" as const }],
                      truncated: false,
                    }) as WorkspaceJobs[K]["output"],
            );
          },
        });
        const program = Effect.gen(function* () {
          const index = yield* WorkspaceSearchIndex.WorkspaceSearchIndex;
          expect(yield* index.list()).toEqual({
            entries: [{ path: "src/a.ts", kind: "file" }],
            truncated: false,
          });
          yield* index.search("a", 2);
          yield* index.refresh();
          yield* index.list();
        });
        yield* program.pipe(
          Effect.provide(
            WorkspaceSearchIndex.layer(
              WorkspaceSearchIndex.workspaceSearchIndexKey("/synthetic", "paths"),
            ).pipe(Layer.provide(Layer.succeed(ServerCompute.ServerCompute, compute))),
          ),
        );
        expect(acquisitions).toBe(1);
        expect(calls).toEqual(["list", "search", "refresh", "list"]);
      }),
    ),
);
