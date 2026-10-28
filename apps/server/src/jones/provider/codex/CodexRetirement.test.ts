import { assert, describe, it } from "@effect/vitest";
import { ProviderRuntimeBinding } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";

import {
  codexProducerOwnershipIsExclusive,
  makeCodexProducerRetirement,
  makeCodexThreadRetirements,
} from "./CodexRetirement.ts";

const binding = Schema.decodeUnknownSync(ProviderRuntimeBinding)({
  threadId: "application-thread",
  providerThreadId: "provider-thread",
  providerSessionId: "session",
  providerInstanceId: "codex",
  driver: "codex",
  nativeThreadId: "native-thread",
  runtimeGeneration: "producer-generation",
});

describe("retained Codex producer retirement", () => {
  it.effect(
    "joins the original scope finalizers and physical exit after deadline and cancellation",
    () =>
      Effect.gen(function* () {
        const scopeClosing = yield* Deferred.make<void>();
        const finishScope = yield* Deferred.make<void>();
        const exited = yield* Deferred.make<void>();
        let closes = 0;
        let releases = 0;
        const retirement = yield* makeCodexProducerRetirement(
          Effect.sync(() => {
            closes++;
          }).pipe(
            Effect.andThen(Deferred.succeed(scopeClosing, undefined)),
            Effect.andThen(Deferred.await(finishScope)),
          ),
          Effect.sync(() => {
            releases++;
          }),
        );
        retirement.captureProcessExit(Deferred.await(exited));
        yield* retirement.observeProcessExit;
        const first = yield* retirement.close.pipe(
          Effect.timeout("3 seconds"),
          Effect.exit,
          Effect.forkChild,
        );
        yield* Deferred.await(scopeClosing);
        const cancelled = yield* retirement.close.pipe(Effect.forkChild);
        yield* Fiber.interrupt(cancelled);
        yield* TestClock.adjust("3 seconds");
        assert.equal((yield* Fiber.join(first))._tag, "Failure");
        const late = yield* retirement.close.pipe(Effect.forkChild);
        yield* Deferred.succeed(exited, undefined);
        assert.isUndefined(
          late.pollUnsafe(),
          "physical exit cannot finish pending scope finalizers",
        );
        assert.equal(releases, 0);
        yield* Deferred.succeed(finishScope, undefined);
        yield* Fiber.join(late);
        yield* retirement.close;
        assert.equal(closes, 1);
        assert.equal(releases, 1);
      }),
  );

  it.effect("does not mistake completed finalizers for physical exit", () =>
    Effect.gen(function* () {
      const finalized = yield* Deferred.make<void>();
      const exited = yield* Deferred.make<void>();
      const retirement = yield* makeCodexProducerRetirement(
        Deferred.succeed(finalized, undefined).pipe(Effect.asVoid),
      );
      retirement.captureProcessExit(Deferred.await(exited));
      const pending = yield* retirement.close.pipe(Effect.forkChild);
      yield* Deferred.await(finalized);
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("30 seconds");
      assert.isUndefined(pending.pollUnsafe());
      yield* Deferred.succeed(exited, undefined);
      yield* Fiber.join(pending);
    }),
  );
});

describe("captured native thread retirement", () => {
  it.effect("retains matching closure and never redispatches a duplicate unsubscribe", () =>
    Effect.gen(function* () {
      const state = makeCodexThreadRetirements<object>();
      const producer = {};
      const sent = yield* Deferred.make<void>();
      let sends = 0;
      const input = {
        producer,
        binding,
        processExit: undefined,
        dispatch: Effect.gen(function* () {
          sends++;
          yield* Deferred.succeed(sent, undefined);
          return "thread" as const;
        }),
      };
      const request = yield* state.begin(input);
      assert.isDefined(request);
      yield* Deferred.await(sent);
      yield* state.observeClosed(producer, binding.nativeThreadId);
      const duplicate = yield* state.begin(input);
      assert.equal(duplicate, request);
      const receipt = yield* duplicate!.wait;
      assert.deepEqual(receipt, { binding, evidence: "thread_closed" });
      assert.equal(sends, 1);
      assert.isTrue(state.owns(producer, binding.nativeThreadId));
    }),
  );

  it.effect(
    "notLoaded ACK, wrong thread, stale producer and cancelled callers cannot release a transfer",
    () =>
      Effect.gen(function* () {
        const state = makeCodexThreadRetirements<object>();
        const producer = {};
        const acknowledged = yield* Deferred.make<void>();
        const request = yield* state.begin({
          producer,
          binding,
          processExit: undefined,
          dispatch: Deferred.succeed(acknowledged, undefined).pipe(Effect.as("thread" as const)),
        });
        yield* Deferred.await(acknowledged);
        const first = yield* request!.wait.pipe(Effect.forkChild);
        yield* state.observeClosed({}, binding.nativeThreadId);
        yield* state.observeClosed(producer, "wrong-thread");
        assert.isUndefined(first.pollUnsafe());
        yield* Fiber.interrupt(first);
        const duplicate = yield* state.begin({
          producer,
          binding,
          processExit: undefined,
          dispatch: Effect.die("must not redispatch"),
        });
        const late = yield* duplicate!.wait.pipe(Effect.forkChild);
        yield* TestClock.adjust("30 seconds");
        assert.isUndefined(late.pollUnsafe());
        yield* state.observeClosed(producer, binding.nativeThreadId);
        assert.equal((yield* Fiber.join(late)).evidence, "thread_closed");
      }),
  );

  it.effect("separates identical native IDs on different producer generations", () =>
    Effect.gen(function* () {
      const state = makeCodexThreadRetirements<object>();
      const oldProducer = {};
      const newProducer = {};
      const old = yield* state.begin({
        producer: oldProducer,
        binding,
        processExit: undefined,
        dispatch: Effect.succeed("thread" as const),
      });
      const next = yield* state.begin({
        producer: newProducer,
        binding: { ...binding, runtimeGeneration: "next" },
        processExit: undefined,
        dispatch: Effect.succeed("thread" as const),
      });
      const waiting = yield* next!.wait.pipe(Effect.forkChild);
      yield* state.observeClosed(oldProducer, binding.nativeThreadId);
      assert.equal((yield* old!.wait).binding.runtimeGeneration, binding.runtimeGeneration);
      assert.isUndefined(waiting.pollUnsafe());
      yield* state.observeClosed(newProducer, binding.nativeThreadId);
      assert.equal((yield* Fiber.join(waiting)).binding.runtimeGeneration, "next");
    }),
  );

  it.effect("exclusive producer retirement requires physical exit despite a thread closure", () =>
    Effect.gen(function* () {
      const state = makeCodexThreadRetirements<object>();
      const producer = {};
      const exited = yield* Deferred.make<void>();
      const request = yield* state.begin({
        producer,
        binding,
        processExit: Deferred.await(exited),
        dispatch: Effect.succeed("producer" as const),
      });
      const waiting = yield* request!.wait.pipe(Effect.forkChild);
      yield* state.observeClosed(producer, binding.nativeThreadId);
      assert.isUndefined(waiting.pollUnsafe());
      yield* Deferred.succeed(exited, undefined);
      assert.deepEqual(yield* Fiber.join(waiting), { binding, evidence: "producer_exited" });
    }),
  );
});

describe("native producer ownership", () => {
  it.effect.each(["exclusive", "sibling", "active", "goal", "terminal", "unknown"] as const)(
    "permits producer retirement only with complete exclusive evidence (%s)",
    (kind) =>
      Effect.gen(function* () {
        const result = yield* codexProducerOwnershipIsExclusive({
          nativeThreadId: binding.nativeThreadId,
          request: (method) => {
            if (kind === "unknown") return Effect.fail("unsupported");
            if (method === "thread/loaded/list")
              return Effect.succeed({
                data:
                  kind === "sibling"
                    ? [binding.nativeThreadId, "sibling"]
                    : [binding.nativeThreadId],
              });
            if (method === "thread/read")
              return Effect.succeed({
                thread: {
                  id: binding.nativeThreadId,
                  status: { type: kind === "active" ? "active" : "idle" },
                },
              });
            if (method === "thread/goal/get")
              return Effect.succeed({
                goal:
                  kind === "goal" ? { threadId: binding.nativeThreadId, status: "active" } : null,
              });
            if (method === "thread/backgroundTerminals/list")
              return Effect.succeed({ data: kind === "terminal" ? [{}] : [], nextCursor: null });
            return Effect.die(`Unexpected native request ${method}`);
          },
        });
        assert.equal(result, kind === "exclusive");
      }),
  );
});
