import * as NodeAssert from "node:assert/strict";
import { it } from "@effect/vitest";
import { describe } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as Schema from "effect/Schema";
import * as CodexErrors from "effect-codex-app-server/errors";
import { readCodexGoalState } from "./providerGoal.ts";

describe("existing Codex provider goal read", () => {
  const cursor = "native-goal-thread";
  const context = Effect.succeed({ nativeThreadId: cursor, stopped: false });
  const goal = {
    threadId: cursor,
    objective: "private objective",
    status: "complete",
    createdAt: 1,
    updatedAt: 2,
    timeUsedSeconds: 0,
    tokensUsed: 1,
  };
  it.effect.each(
    (
      [
        [{ goal: null }, "inactive", "goal_null"],
        [{ goal }, "active", "goal_present"],
        [{ goal: { ...goal, status: "paused" } }, "active", "goal_present"],
        [{ goal: { ...goal, status: "active" } }, "active", "goal_present"],
        [{}, "unknown", "goal_field_omitted"],
        [{ goal: undefined }, "unknown", "malformed"],
        [{ goal: {} }, "unknown", "malformed"],
        [null, "unknown", "malformed"],
        [{ goal: { ...goal, threadId: "other" } }, "unknown", "context_changed"],
      ] as const
    ).map(([response, state, reasonCode]) => ({
      response,
      state,
      reasonCode,
      name: `classifies goal response ${reasonCode} ${JSON.stringify(response)}`,
    })),
  )("$name", ({ response, state, reasonCode }) =>
    Effect.gen(function* () {
      const result = yield* readCodexGoalState(
        {
          raw: {
            request: (method, params) => {
              NodeAssert.equal(method, "thread/goal/get");
              NodeAssert.deepEqual(params, { threadId: cursor });
              return Effect.succeed(response);
            },
          },
        },
        context,
      );
      NodeAssert.deepEqual(result, { nativeThreadId: cursor, state, reasonCode });
      const serialized = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(result);
      NodeAssert.equal(serialized.includes("private objective"), false);
    }),
  );
  it.effect.each(
    (
      [
        [null, false, "native_cursor_missing"],
        [cursor, true, "session_stopped"],
      ] as const
    ).map(([nativeThreadId, stopped, reasonCode]) => ({
      nativeThreadId,
      stopped,
      reasonCode,
      name: `does not send RPC for ${reasonCode}`,
    })),
  )("$name", ({ nativeThreadId, stopped, reasonCode }) =>
    Effect.gen(function* () {
      const result = yield* readCodexGoalState(
        { raw: { request: () => Effect.die("must not request") } },
        Effect.succeed({ nativeThreadId, stopped }),
      );
      NodeAssert.equal(result.reasonCode, reasonCode);
    }),
  );
  it.effect.each([-32601, -32000])("redacts RPC error %s", (code) =>
    Effect.gen(function* () {
      const result = yield* readCodexGoalState(
        {
          raw: {
            request: () =>
              Effect.fail(
                new CodexErrors.CodexAppServerRequestError({
                  code,
                  errorMessage: "private error",
                }),
              ),
          },
        },
        context,
      );
      NodeAssert.equal(result.reasonCode, code === -32601 ? "unsupported" : "rpc_error");
      const serialized = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(result);
      NodeAssert.equal(serialized.includes("private error"), false);
    }),
  );
  it.effect("invalidates a changed cursor after the RPC", () =>
    Effect.gen(function* () {
      let nativeThreadId = cursor;
      const result = yield* readCodexGoalState(
        {
          raw: {
            request: () =>
              Effect.sync(() => {
                nativeThreadId = "replacement";
                return { goal: null };
              }),
          },
        },
        Effect.sync(() => ({ nativeThreadId, stopped: false })),
      );
      NodeAssert.equal(result.reasonCode, "context_changed");
    }),
  );
  it.effect("times out in three seconds and interrupts the pending request", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      let interrupted = false;
      const pending = Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            interrupted = true;
          }),
        ),
      );
      const fiber = yield* readCodexGoalState({ raw: { request: () => pending } }, context).pipe(
        Effect.forkChild,
      );
      yield* Deferred.await(started);
      yield* TestClock.adjust("3 seconds");
      NodeAssert.equal((yield* Fiber.join(fiber)).reasonCode, "timeout");
      NodeAssert.equal(interrupted, true);
    }),
  );
});
