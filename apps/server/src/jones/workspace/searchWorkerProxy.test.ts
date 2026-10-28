import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as ServerCompute from "../serverCompute/ServerCompute.ts";
import { ComputeFailure, isComputeFailure } from "../serverCompute/failure.ts";
import { ComputeSupervisor } from "../serverCompute/supervisor.ts";
import { decodeRequest, type Request } from "../serverCompute/protocol.ts";
import type { ProjectSearchContentsInput } from "@t3tools/contracts";
import { makeSearchWorkerProxy } from "./searchWorkerProxy.ts";

const handle = { handleId: "logical", key: { cwd: "/synthetic", variant: "content" as const } };

it.effect(
  "admits the ordinary caller's cwd through the proxy and actual closed supervisor validation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const requests: Request[] = [];
        const match = {
          path: "src/a.ts",
          lineNumber: 2,
          lineContent: "Needle",
          matchRanges: [{ start: 0, end: 6 }],
        };
        // Only the external child/native operation is synthetic; supervisor admission and wire decoding are real.
        const pool = yield* Effect.acquireRelease(
          Effect.sync(
            () =>
              new ComputeSupervisor({
                count: 2,
                build: "a".repeat(64),
                journal: {
                  claim: { authorityId: "authority" },
                  reserve: async () => {},
                  registered: async () => {},
                  retired: async () => {},
                  release: async () => {},
                },
                start: async (bootstrap, register) => {
                  await register(1234);
                  return {
                    bootstrap,
                    pid: 1234,
                    beginScan: async () => {},
                    endScan: async () => {},
                    retire: async () => {},
                    request: async (value) => {
                      const request = decodeRequest(value, bootstrap);
                      requests.push(request);
                      return {
                        items: request.operation === "searchContents" ? [match] : [],
                        final: {
                          type: "done",
                          ...(request.operation === "close"
                            ? {}
                            : { status: { scanning: false, warmupComplete: true } }),
                          ...(request.operation === "searchContents"
                            ? { truncated: true, regexFallbackError: "synthetic regex fallback" }
                            : {}),
                        },
                      };
                    },
                  };
                },
              }),
          ),
          (pool) => Effect.promise(() => pool.shutdown()),
        );
        const failure = (cause: unknown) =>
          isComputeFailure(cause)
            ? cause
            : new ComputeFailure({ reason: "unavailable", cause });
        const compute = ServerCompute.ServerCompute.of({
          acquireLogical: (key) =>
            Effect.acquireRelease(
              Effect.tryPromise({
                try: async () => {
                  const logical = pool.register(key);
                  await pool.ensure(logical);
                  return logical;
                },
                catch: failure,
              }),
              (logical) => Effect.sync(() => pool.release(logical.handleId)),
            ),
          run: (logical, operation, input) =>
            Effect.tryPromise({
              try: (signal) => pool.run(logical, operation, input, signal),
              catch: failure,
            }),
        });
        const proxy = yield* makeSearchWorkerProxy(handle.key).pipe(
          Effect.provideService(ServerCompute.ServerCompute, compute),
        );
        const input: ProjectSearchContentsInput = {
          cwd: handle.key.cwd,
          query: "Needle",
          limit: 3,
          caseSensitive: true,
          wholeWord: false,
          useRegex: true,
        };
        const result = yield* proxy.searchContents(input);
        expect(result).toEqual({
          matches: [match],
          truncated: true,
          regexFallbackError: "synthetic regex fallback",
        });
        expect(requests.find((request) => request.operation === "searchContents")?.input).toEqual({
          query: "Needle",
          limit: 3,
          caseSensitive: true,
          wholeWord: false,
          useRegex: true,
        });
        // This is the rejected pre-fix runtime shape, regardless of TypeScript's Omit annotation.
        const logical = pool.register(handle.key);
        yield* Effect.promise(() =>
          expect(pool.run(logical, "searchContents", input)).rejects.toMatchObject({
            reason: "protocol",
          }),
        );
        pool.release(logical.handleId);
      }),
    ),
);

it.effect(
  "keeps the public create/scan/search/refresh error tags and immediate compute cause",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cause = new ComputeFailure({ reason: "worker-exit" });
        const compute = ServerCompute.ServerCompute.of({
          acquireLogical: () => Effect.succeed(handle),
          run: () => Effect.fail(cause),
        });
        const proxy = yield* makeSearchWorkerProxy(handle.key).pipe(
          Effect.provideService(ServerCompute.ServerCompute, compute),
        );
        const search = yield* proxy.search("needle", 4, "file").pipe(Effect.result);
        expect(Result.isFailure(search)).toBe(true);
        if (Result.isFailure(search)) {
          expect(search.failure).toMatchObject({
            _tag: "WorkspaceSearchIndexSearchFailed",
            queryLength: 6,
            pageSize: 5,
          });
          expect(search.failure.cause).toBe(cause);
          expect(search.failure.message).not.toContain("worker-exit");
        }
        const refresh = yield* proxy.refresh().pipe(Effect.result);
        if (Result.isFailure(refresh))
          expect(refresh.failure._tag).toBe("WorkspaceSearchIndexRefreshFailed");
      }),
    ),
);

it.effect(
  "preserves the content initialization scan timeout and the 15-second public boundary",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const result = yield* makeSearchWorkerProxy(handle.key).pipe(
          Effect.provideService(
            ServerCompute.ServerCompute,
            ServerCompute.ServerCompute.of({
              acquireLogical: () =>
                Effect.fail(new ComputeFailure({ reason: "finder-create", scanTimedOut: true })),
              run: () => Effect.die("must not run"),
            }),
          ),
          Effect.result,
        );
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result))
          expect(result.failure).toMatchObject({
            _tag: "WorkspaceSearchIndexScanTimedOut",
            timeout: "15 seconds",
          });
      }),
    ),
);

it.effect("maps saturation into existing operation errors without a retry or hidden fallback", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let calls = 0;
      const proxy = yield* makeSearchWorkerProxy(handle.key).pipe(
        Effect.provideService(
          ServerCompute.ServerCompute,
          ServerCompute.ServerCompute.of({
            acquireLogical: () => Effect.succeed(handle),
            run: () => {
              calls++;
              return Effect.fail(new ComputeFailure({ reason: "queue-full" }));
            },
          }),
        ),
      );
      const result = yield* proxy.list().pipe(Effect.result);
      expect(Result.isFailure(result)).toBe(true);
      expect(calls).toBe(1);
      if (Result.isFailure(result))
        expect(result.failure._tag).toBe("WorkspaceSearchIndexSearchFailed");
    }),
  ),
);
