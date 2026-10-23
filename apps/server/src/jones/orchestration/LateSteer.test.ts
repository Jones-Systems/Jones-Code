import { assert, describe, it } from "@effect/vitest";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PlatformError from "effect/PlatformError";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Scope from "effect/Scope";
import { TestClock } from "effect/testing";

import * as ControlAdmission from "./ControlAdmission.ts";
import {
  boundedSteer,
  CODEX_STEER_TIMEOUT_MS,
  findSteerFailure,
  SteerOutcomeUnknown,
  verifyCodexProcessExit,
} from "./LateSteer.ts";

const binding: ControlAdmission.ControlDispatchBinding = {
  instanceId: "codex-a",
  providerSessionId: "session-a",
  providerThreadId: "provider-thread-a",
  providerTurnId: "provider-turn-a",
  runtimeGeneration: "generation-a",
  nativeThreadId: "thread-a",
  nativeTurnId: "turn-a",
};

describe("bounded native steering", () => {
  it.effect("shares one budget across goal transition and original acknowledgement", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const producerScope = yield* Scope.Scope;
      const goal = yield* Deferred.make<void>();
      const response = yield* Deferred.make<{ turnId: string }>();
      const sent = yield* Deferred.make<void>();
      const caller = yield* boundedSteer({
        instanceId: binding.instanceId,
        prepare: Deferred.await(goal).pipe(
          Effect.as({
            binding: { ...binding, nativeTurnId: "goal-successor" },
            producerScope,
            send: Deferred.succeed(sent, undefined).pipe(Effect.andThen(Deferred.await(response))),
          }),
        ),
      }).pipe(Effect.exit, Effect.forkScoped);
      yield* TestClock.adjust("20 seconds");
      yield* Deferred.succeed(goal, undefined);
      yield* Deferred.await(sent);
      yield* TestClock.adjust("10 seconds");
      assert.isDefined(caller.pollUnsafe());
      const exit = yield* Fiber.join(caller);
      assert.equal(exit._tag, "Failure");
      if (exit._tag === "Failure")
        assert.equal(findSteerFailure(exit.cause)?._tag, "SteerOutcomeUnknown");
      yield* admission.assertOpen(binding.instanceId).pipe(Effect.flip);
      yield* Deferred.succeed(response, { turnId: "goal-successor" });
      yield* TestClock.adjust(0);
      yield* admission.assertOpen(binding.instanceId);
    }).pipe(Effect.scoped, Effect.provide(ControlAdmission.layer)),
  );

  it.effect("expires before goal handoff without sending or holding admission", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const caller = yield* boundedSteer({
        instanceId: binding.instanceId,
        prepare: Effect.never,
      }).pipe(Effect.flip, Effect.forkScoped);
      yield* TestClock.adjust(CODEX_STEER_TIMEOUT_MS);
      assert.equal((yield* Fiber.join(caller))._tag, "SteerNotDispatched");
      yield* admission.assertOpen(binding.instanceId);
      assert.deepEqual(yield* admission.snapshot(binding.instanceId), []);
    }).pipe(Effect.scoped, Effect.provide(ControlAdmission.layer)),
  );

  it.effect.each(["deadline", "caller-cancel"] as const)(
    "preserves the original waiter after %s and resolves only its exact acknowledgement",
    (ending) =>
      Effect.gen(function* () {
        const admission = yield* ControlAdmission.ControlAdmission;
        const producerScope = yield* Scope.Scope;
        const response = yield* Deferred.make<{ turnId: string }>();
        const sent = yield* Deferred.make<void>();
        const observed = yield* Deferred.make<void>();
        let sends = 0;
        const caller = yield* boundedSteer({
          instanceId: binding.instanceId,
          prepare: Effect.succeed({
            binding,
            producerScope,
            send: Effect.sync(() => {
              sends++;
            }).pipe(
              Effect.andThen(Deferred.succeed(sent, undefined)),
              Effect.andThen(Deferred.await(response)),
              Effect.tap(() => Deferred.succeed(observed, undefined)),
            ),
          }),
        }).pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(sent);
        if (ending === "deadline") yield* TestClock.adjust(CODEX_STEER_TIMEOUT_MS);
        else yield* Fiber.interrupt(caller);
        yield* admission.assertOpen(binding.instanceId).pipe(Effect.flip);
        assert.equal(sends, 1);
        yield* Deferred.succeed(response, { turnId: binding.nativeTurnId });
        yield* Deferred.await(observed);
        yield* TestClock.adjust(0);
        yield* admission.assertOpen(binding.instanceId);
        assert.equal(sends, 1);
      }).pipe(Effect.scoped, Effect.provide(ControlAdmission.layer)),
  );

  it.effect("rejects wrong-turn acknowledgements without releasing the hold", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const producerScope = yield* Scope.Scope;
      const error = yield* boundedSteer({
        instanceId: binding.instanceId,
        prepare: Effect.succeed({
          binding,
          producerScope,
          send: Effect.succeed({ turnId: "other-turn" }),
        }),
      }).pipe(Effect.flip);
      assert.equal(error._tag, "SteerOutcomeUnknown");
      yield* admission.assertOpen(binding.instanceId).pipe(Effect.flip);
    }).pipe(Effect.scoped, Effect.provide(ControlAdmission.layer)),
  );

  it.effect.each(["validation", "decode", "server"] as const)(
    "only a received nonapplying validation rejection releases admission (%s)",
    (kind) =>
      Effect.gen(function* () {
        const admission = yield* ControlAdmission.ControlAdmission;
        const producerScope = yield* Scope.Scope;
        const error = new CodexErrors.CodexAppServerRequestError({
          code: kind === "server" ? -32603 : -32602,
          errorMessage: "synthetic",
          method: "turn/steer",
          operation: kind === "decode" ? "decode-payload" : "receive-response",
        });
        yield* boundedSteer({
          instanceId: binding.instanceId,
          prepare: Effect.succeed({ binding, producerScope, send: Effect.fail(error) }),
        }).pipe(Effect.flip);
        if (kind === "validation") yield* admission.assertOpen(binding.instanceId);
        else yield* admission.assertOpen(binding.instanceId).pipe(Effect.flip);
      }).pipe(Effect.scoped, Effect.provide(ControlAdmission.layer)),
  );

  it.effect("checks admission before goal waiting and again immediately before sending", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const producerScope = yield* Scope.Scope;
      let sends = 0;
      const other = yield* admission.begin({ ...binding, nativeTurnId: "other" });
      const error = yield* boundedSteer({
        instanceId: binding.instanceId,
        prepare: admission.markUnknown(other).pipe(
          Effect.as({
            binding,
            producerScope,
            send: Effect.sync(() => {
              sends++;
              return { turnId: binding.nativeTurnId };
            }),
          }),
        ),
      }).pipe(Effect.flip);
      assert.equal(error._tag, "ControlAdmissionHeld");
      const second = yield* boundedSteer({
        instanceId: binding.instanceId,
        prepare: Effect.never,
      }).pipe(Effect.flip);
      assert.equal(second._tag, "ControlAdmissionHeld");
      assert.equal(sends, 0);
    }).pipe(Effect.scoped, Effect.provide(ControlAdmission.layer)),
  );

  it("preserves typed classification through adapter, executor and Effect cause wrappers", () => {
    const failure = new SteerOutcomeUnknown(binding);
    assert.equal(findSteerFailure(Cause.fail({ cause: { cause: failure } })), failure);
    assert.isUndefined(findSteerFailure(new Error("SteerOutcomeUnknown")));
    const cycle: { cause?: unknown } = {};
    cycle.cause = cycle;
    assert.isUndefined(findSteerFailure(cycle));
  });
});

describe("captured child exit proof", () => {
  const signalExit = PlatformError.systemError({
    _tag: "Unknown",
    module: "ChildProcess",
    method: "exitCode",
  });

  it.effect.each(["normal", "signal"] as const)("accepts an observed %s exit", (kind) =>
    verifyCodexProcessExit({
      exitCode:
        kind === "normal"
          ? Effect.succeed(ChildProcessSpawner.ExitCode(0))
          : Effect.fail(signalExit),
      isRunning: Effect.succeed(false),
    }),
  );

  it.effect.each(["still-running", "probe-failed"] as const)(
    "rejects %s instead of treating an exitCode error as teardown",
    (kind) =>
      Effect.gen(function* () {
        const error = yield* verifyCodexProcessExit({
          exitCode: Effect.fail(signalExit),
          isRunning: kind === "still-running" ? Effect.succeed(true) : Effect.fail(signalExit),
        }).pipe(Effect.flip);
        assert.equal(error._tag, "CodexAppServerTransportError");
        assert.equal(error.operation, "read-process-exit-status");
      }),
  );

  it.effect("bounds an unavailable exit observation to three seconds", () =>
    Effect.gen(function* () {
      const readback = yield* verifyCodexProcessExit({
        exitCode: Effect.never,
        isRunning: Effect.succeed(false),
      }).pipe(Effect.flip, Effect.forkScoped);
      yield* TestClock.adjust("2999 millis");
      assert.isUndefined(readback.pollUnsafe());
      yield* TestClock.adjust("1 millis");
      assert.isDefined(readback.pollUnsafe());
      assert.equal((yield* Fiber.join(readback))._tag, "CodexAppServerTransportError");
    }).pipe(Effect.scoped),
  );
});
