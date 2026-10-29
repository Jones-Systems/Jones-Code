import { assert, describe, it } from "@effect/vitest";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import { TestClock } from "effect/testing";

import { CODEX_STARTUP_BUDGET_MS, makeCodexStartup } from "./CodexStartup.ts";
import { makeCodexProducerRetirement } from "./CodexRetirement.ts";

const makeProducer = Effect.fn("CodexStartup.test.makeProducer")(function* (
  generation = "startup-test-generation",
) {
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  return { generation, scope };
});

describe("Codex startup caller budget", () => {
  it.effect("expires a lifecycle permit waiter without issuing or fencing a request", () =>
    Effect.gen(function* () {
      const startup = makeCodexStartup();
      const producer = yield* makeProducer();
      const permit = yield* Semaphore.make(1);
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const owner = yield* permit
        .withPermits(1)(
          Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(held);
      let dispatches = 0;
      const request = startup.observe(producer, Effect.sync(() => ++dispatches));
      const queued = yield* startup
        .run("ensure", permit.withPermits(1)(startup.withinPermit(request)))
        .pipe(Effect.result, Effect.forkChild);
      yield* TestClock.adjust(CODEX_STARTUP_BUDGET_MS);
      const expired = yield* Fiber.join(queued);
      assert.equal(expired._tag, "Failure");
      if (expired._tag === "Failure")
        assert.isFalse(ProviderAdapter.hasUnknownRuntimeBinding(expired.failure));
      assert.equal(dispatches, 0);
      assert.isFalse(startup.isUncertain(producer));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(owner);
      assert.equal(yield* startup.run("ensure", startup.withinPermit(request)), 1);
    }).pipe(Effect.scoped),
  );

  it.effect("fences before releasing the permit and observes a late ACK without binding it", () =>
    Effect.gen(function* () {
      const startup = makeCodexStartup();
      const producer = yield* makeProducer();
      const permit = yield* Semaphore.make(1);
      const dispatched = yield* Deferred.make<void>();
      const response = yield* Deferred.make<string>();
      const observed = yield* Deferred.make<void>();
      let dispatches = 0;
      let bindings = 0;
      const request = startup
        .observe(
          producer,
          Effect.sync(() => ++dispatches).pipe(
            Effect.andThen(Deferred.succeed(dispatched, undefined)),
            Effect.andThen(Deferred.await(response)),
            Effect.tap(() => Deferred.succeed(observed, undefined)),
          ),
        )
        .pipe(Effect.tap(() => Effect.sync(() => bindings++)));
      const first = yield* startup
        .run("ensure", permit.withPermits(1)(startup.withinPermit(request)))
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(dispatched);
      yield* TestClock.adjust(1);
      const second = yield* startup
        .run(
          "resume",
          permit.withPermits(1)(
            startup.withinPermit(startup.observe(producer, Effect.sync(() => ++dispatches))),
          ),
        )
        .pipe(Effect.result, Effect.forkChild);
      yield* TestClock.adjust(CODEX_STARTUP_BUDGET_MS - 1);
      const timedOut = yield* Fiber.join(first);
      const refused = yield* Fiber.join(second);
      assert.equal(timedOut._tag, "Failure");
      assert.equal(refused._tag, "Failure");
      if (timedOut._tag === "Failure")
        assert.isTrue(ProviderAdapter.hasUnknownRuntimeBinding(timedOut.failure));
      assert.equal(dispatches, 1);
      assert.isTrue(startup.isUncertain(producer));
      yield* Deferred.succeed(response, "late-native-thread");
      yield* Deferred.await(observed);
      assert.equal(bindings, 0);
      assert.isTrue(startup.isUncertain(producer));
    }).pipe(Effect.scoped),
  );

  it.effect("retains cancellation uncertainty across scope closure and physical exit", () =>
    Effect.gen(function* () {
      const startup = makeCodexStartup(20);
      const producer = yield* makeProducer();
      const sent = yield* Deferred.make<void>();
      const late = yield* Deferred.make<void>();
      const observed = yield* Deferred.make<void>();
      const exited = yield* Deferred.make<void>();
      let closes = 0;
      const retirement = yield* makeCodexProducerRetirement(
        Effect.sync(() => closes++).pipe(
          Effect.andThen(Scope.close(producer.scope, Exit.void)),
        ),
      );
      retirement.captureProcessExit(Deferred.await(exited));
      yield* retirement.observeProcessExit;
      yield* Effect.addFinalizer(() => Deferred.succeed(exited, undefined).pipe(Effect.asVoid));
      const caller = yield* startup
        .run(
          "resume",
          startup.withinPermit(
            startup.observe(
              producer,
              Deferred.succeed(sent, undefined).pipe(
                Effect.andThen(Deferred.await(late)),
                Effect.tap(() => Deferred.succeed(observed, undefined)),
              ),
            ),
          ),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(sent);
      yield* Fiber.interrupt(caller);
      assert.isTrue(startup.isUncertain(producer));
      yield* Deferred.succeed(late, undefined);
      yield* Deferred.await(observed);
      yield* TestClock.adjust(100);
      assert.equal(closes, 0);
      const closing = yield* retirement.close.pipe(Effect.forkChild);
      yield* TestClock.adjust(0);
      assert.isUndefined(closing.pollUnsafe(), "finalizers do not prove physical exit");
      assert.isTrue(startup.isUncertain(producer));
      const replacement = yield* makeProducer("replacement-generation");
      assert.isFalse(startup.isUncertain(replacement));
      yield* Deferred.succeed(exited, undefined);
      yield* Fiber.join(closing);
      assert.equal(closes, 1);
      assert.isTrue(startup.isUncertain(producer));
    }).pipe(Effect.scoped),
  );

  it.effect("preserves explicit failed-handshake retry and ends the budget before turn start", () =>
    Effect.gen(function* () {
      const startup = makeCodexStartup(10);
      const producer = yield* makeProducer();
      const rejected = yield* startup
        .run(
          "ensure",
          startup.withinPermit(
            startup.observe(
              producer,
              Effect.fail(CodexErrors.CodexAppServerRequestError.internalError("not ready")),
            ),
          ),
        )
        .pipe(Effect.result);
      assert.equal(rejected._tag, "Failure");
      assert.isFalse(startup.isUncertain(producer));
      const refusedAfterHandshake = yield* startup
        .run(
          "ensure",
          startup.withinPermit(
            startup.observe(producer, Effect.void, "reply").pipe(
              Effect.andThen(
                Effect.fail(
                  CodexErrors.CodexAppServerRequestError.invalidRequest("admission refused"),
                ),
              ),
            ),
          ),
        )
        .pipe(Effect.result);
      assert.equal(refusedAfterHandshake._tag, "Failure");
      if (refusedAfterHandshake._tag === "Failure")
        assert.isFalse(ProviderAdapter.hasUnknownRuntimeBinding(refusedAfterHandshake.failure));
      assert.isFalse(startup.isUncertain(producer));
      const turnReady = yield* Deferred.make<void>();
      const finishTurn = yield* Deferred.make<void>();
      const caller = yield* startup
        .run(
          "start",
          startup.withinPermit(
            startup.observe(producer, Effect.succeed("native-thread")).pipe(
              Effect.andThen(startup.complete),
              Effect.andThen(Deferred.succeed(turnReady, undefined)),
              Effect.andThen(Deferred.await(finishTurn)),
            ),
          ),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(turnReady);
      yield* TestClock.adjust(20);
      assert.isUndefined(caller.pollUnsafe());
      assert.isFalse(startup.isUncertain(producer));
      yield* Deferred.succeed(finishTurn, undefined);
      yield* Fiber.join(caller);
    }).pipe(Effect.scoped),
  );
});
