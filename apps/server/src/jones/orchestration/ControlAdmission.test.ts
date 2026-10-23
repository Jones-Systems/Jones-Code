import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ControlAdmission from "./ControlAdmission.ts";

const binding: ControlAdmission.ControlDispatchBinding = {
  instanceId: "codex-a",
  providerSessionId: "session-a",
  providerThreadId: "provider-thread-a",
  providerTurnId: "provider-turn-a",
  runtimeGeneration: "generation-a",
  nativeThreadId: "thread-a",
  nativeTurnId: "turn-a",
};

describe("control admission", () => {
  it.effect("retains every unresolved request and only blocks its instance", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const first = yield* admission.begin(binding);
      const secondBinding = { ...binding, providerSessionId: "session-b", nativeTurnId: "turn-b" };
      const second = yield* admission.begin(secondBinding);
      yield* admission.markUnknown(first);
      yield* admission.markUnknown(second);
      assert.equal(
        (yield* admission.assertOpen(binding.instanceId).pipe(Effect.flip))._tag,
        "ControlAdmissionHeld",
      );
      yield* admission.assertOpen("codex-b");
      assert.isTrue(yield* admission.acknowledge(first, binding));
      assert.equal((yield* admission.snapshot(binding.instanceId)).length, 1);
      yield* admission.assertOpen(binding.instanceId).pipe(Effect.flip);
      assert.isTrue(yield* admission.acknowledge(second, secondBinding));
      yield* admission.assertOpen(binding.instanceId);
    }).pipe(Effect.provide(ControlAdmission.layer)),
  );

  it.effect("requires every routing component and the original dispatch object", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const dispatch = yield* admission.begin(binding);
      yield* admission.markUnknown(dispatch);
      for (const key of Object.keys(binding) as Array<keyof typeof binding>) {
        assert.isFalse(yield* admission.acknowledge(dispatch, { ...binding, [key]: "wrong" }));
      }
      assert.isFalse(yield* admission.acknowledge({ binding }, binding));
      yield* admission.rejectNonApplying({ binding });
      assert.equal((yield* admission.snapshot(binding.instanceId)).length, 1);
      yield* admission.rejectNonApplying(dispatch);
      yield* admission.assertOpen(binding.instanceId);
    }).pipe(Effect.provide(ControlAdmission.layer)),
  );

  it.effect("does not recreate a resolved obligation when timeout races acknowledgement", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const dispatch = yield* admission.begin(binding);
      yield* admission.acknowledge(dispatch, binding);
      yield* admission.markUnknown(dispatch);
      assert.deepEqual(yield* admission.snapshot(binding.instanceId), []);
    }).pipe(Effect.provide(ControlAdmission.layer)),
  );

  it.effect(
    "retirement is exact to instance and generation and does not resolve other obligations",
    () =>
      Effect.gen(function* () {
        const admission = yield* ControlAdmission.ControlAdmission;
        const old = yield* admission.begin(binding);
        const next = yield* admission.begin({ ...binding, runtimeGeneration: "generation-b" });
        yield* admission.markUnknown(old);
        yield* admission.markUnknown(next);
        yield* admission.retireGeneration({
          instanceId: "codex-b",
          runtimeGeneration: binding.runtimeGeneration,
        });
        assert.lengthOf(yield* admission.snapshot(binding.instanceId), 2);
        yield* admission.retireGeneration(binding);
        assert.lengthOf(yield* admission.snapshot(binding.instanceId), 1);
        yield* admission.assertOpen(binding.instanceId).pipe(Effect.flip);
      }).pipe(Effect.provide(ControlAdmission.layer)),
  );

  it.effect("shares obligations across consumers of one environment layer", () =>
    Effect.gen(function* () {
      const owner = yield* ControlAdmission.ControlAdmission;
      const dispatch = yield* owner.begin(binding);
      yield* owner.markUnknown(dispatch);
      const consumer = yield* Layer.build(
        Layer.effectDiscard(
          Effect.gen(function* () {
            const rebuilt = yield* ControlAdmission.ControlAdmission;
            assert.equal(rebuilt, owner);
            yield* rebuilt.assertOpen(binding.instanceId).pipe(Effect.flip);
          }),
        ),
      );
      assert.isDefined(consumer);
      yield* Effect.gen(function* () {
        const isolated = yield* ControlAdmission.ControlAdmission;
        yield* isolated.assertOpen(binding.instanceId);
      }).pipe(Effect.provide(Layer.fresh(ControlAdmission.layer)));
    }).pipe(Effect.scoped, Effect.provide(ControlAdmission.layer)),
  );

  it("fences mutation methods while preserving Stop and read/reconciliation methods", () => {
    for (const method of [
      "thread/start",
      "thread/resume",
      "thread/fork",
      "turn/start",
      "turn/steer",
      "thread/revert",
    ])
      assert.isTrue(ControlAdmission.isCodexMutation(method, {}));
    assert.isTrue(ControlAdmission.isCodexMutation("thread/goal/set", { status: "active" }));
    assert.isFalse(ControlAdmission.isCodexMutation("thread/goal/set", { status: "paused" }));
    assert.isTrue(
      ControlAdmission.isCodexMutation("thread/goal/set", {
        status: "paused",
        objective: "new work",
      }),
    );
    for (const method of [
      "turn/interrupt",
      "thread/read",
      "thread/goal/get",
      "thread/unsubscribe",
      "server/diagnostics",
    ])
      assert.isFalse(ControlAdmission.isCodexMutation(method, {}));
  });
});
