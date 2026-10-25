import { expect, it } from "@effect/vitest";
import { EnvironmentId, ThreadId, LinkedThreadsError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Threads from "../../orchestration-v2/ThreadManagementService.ts";
import type { Caller } from "../../mcp/threadAccess.ts";
import { liveThreadShell } from "../../mcp/McpToolAccess.testkit.ts";
import * as Links from "./LinkedThreads.ts";
import { claimForCaller } from "./launch.ts";

const origin = ThreadId.make("origin"),
  target = ThreadId.make("target");
const caller = Effect.gen(function* () {
  const threads = yield* Threads.ThreadManagementService;
  return {
    scope: {
      environmentId: EnvironmentId.make("here"),
      requestNamespace: "test",
      issuedAt: 0,
      capabilities: new Set(["orchestration"]),
      thread: {
        threadId: origin,
        providerInstanceId: liveThreadShell(origin).providerInstanceId,
        providerSessionId: "session",
      },
      client: undefined,
    },
    threads,
    caller: liveThreadShell(origin),
    limits: { runtimeMode: "full-access", interactionMode: "default" },
  } satisfies Caller;
});
it.effect("claims ordinary thread callers, excludes delegated and non-thread callers", () => {
  const claims: string[] = [];
  return Effect.gen(function* () {
    const context = yield* caller;
    expect((yield* claimForCaller(context, target, "create"))?.reverse).toBe("local");
    expect(yield* claimForCaller(context, target, "delegated", true)).toBeUndefined();
    expect(
      yield* claimForCaller(
        { ...context, scope: { ...context.scope, thread: undefined }, caller: undefined },
        target,
        "client",
      ),
    ).toBeUndefined();
    expect(claims).toEqual(["create"]);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.mock(Threads.ThreadManagementService)({}),
        Layer.mock(Links.LinkedThreads)({
          claimLaunch: (input) => {
            claims.push(input.launchCommandId);
            return Effect.succeed("tl1:" + "a".repeat(64));
          },
        }),
      ),
    ),
  );
});
it.effect(
  "destination recording failure preserves forwarded launch success and reports not_recorded",
  () =>
    Effect.gen(function* () {
      const context = yield* caller;
      const linked: Caller = {
        ...context,
        caller: undefined,
        scope: {
          ...context.scope,
          thread: undefined,
          client: {
            sessionId: "link",
            label: "Remote",
            access: "full-access",
            linked: true,
            remoteOrigin: {
              schemaVersion: "t3.linked-message-origin/v1",
              environmentId: "away",
              thread: { threadId: origin, title: "Origin" },
              linkSessionId: "link",
            },
          },
        },
      };
      expect((yield* claimForCaller(linked, target, "create"))?.state).toBe("not_recorded");
      expect(
        yield* claimForCaller(
          {
            ...linked,
            scope: {
              ...linked.scope,
              client: { ...linked.scope.client!, remoteOrigin: undefined },
            },
          },
          target,
          "unbound",
        ),
      ).toBeUndefined();
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(Threads.ThreadManagementService)({}),
          Layer.mock(Links.LinkedThreads)({
            claimLaunch: () =>
              Effect.fail(
                new LinkedThreadsError({ reason: "not_found", message: "Storage unavailable." }),
              ),
          }),
        ),
      ),
    ),
);
