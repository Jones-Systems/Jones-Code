import { assert, describe, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, ProviderSessionId, ProviderThreadId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ControlAdmission from "./ControlAdmission.ts";

const binding: ControlAdmission.ControlDispatchBinding = {
  instanceId: "codex-a", threadId: "logical-a", providerSessionId: "session-a",
  providerThreadId: "provider-thread-a", providerTurnId: "provider-turn-a",
  runtimeGeneration: "generation-a", nativeThreadId: "thread-a", nativeTurnId: "turn-a",
};
const target = ControlAdmission.targetFromDispatch(binding);
const nativeBinding = {
  driver: ProviderDriverKind.make("codex"), providerInstanceId: ProviderInstanceId.make(binding.instanceId),
  providerSessionId: ProviderSessionId.make(binding.providerSessionId),
  providerThreadId: ProviderThreadId.make(binding.providerThreadId), threadId: ThreadId.make(binding.threadId),
  runtimeGeneration: binding.runtimeGeneration, nativeThreadId: binding.nativeThreadId,
};

describe("control admission", () => {
  it.effect("holds the native history across accounts and generations while same-producer siblings remain open", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const dispatch = yield* admission.begin(binding);
      yield* admission.markUnknown(dispatch);
      yield* admission.assertOpen(target).pipe(Effect.flip);
      yield* admission.assertOpen({ ...target, instanceId: "codex-b", threadId: "other-alias",
        producer: { instanceId: "codex-b", providerSessionId: "other-session", runtimeGeneration: "next" } }).pipe(Effect.flip);
      const sibling = { ...target, threadId: "logical-b", nativeThreadId: "thread-b" };
      yield* admission.assertOpen(sibling);
      const siblingDispatch = yield* admission.begin({ ...binding, threadId: "logical-b", nativeThreadId: "thread-b" });
      yield* admission.acknowledge(siblingDispatch, siblingDispatch.binding);
      assert.isFalse(yield* admission.acknowledge(dispatch, { ...binding, nativeTurnId: "wrong" }));
      yield* admission.assertOpen(target).pipe(Effect.flip);
      assert.isTrue(yield* admission.acknowledge(dispatch, binding));
      yield* admission.assertOpen(target);
    }).pipe(Effect.provide(ControlAdmission.layer)),
  );

  it.effect("requires every routing component and the original dispatch object", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const dispatch = yield* admission.begin(binding);
      yield* admission.markUnknown(dispatch);
      for (const key of Object.keys(binding) as Array<keyof typeof binding>)
        assert.isFalse(yield* admission.acknowledge(dispatch, { ...binding, [key]: "wrong" }));
      assert.isFalse(yield* admission.acknowledge({ binding }, binding));
      yield* admission.rejectNonApplying({ binding });
      assert.lengthOf(yield* admission.snapshot(binding.instanceId), 1);
      yield* admission.rejectNonApplying(dispatch);
      yield* admission.markUnknown(dispatch);
      assert.deepEqual(yield* admission.snapshot(binding.instanceId), []);
    }).pipe(Effect.provide(ControlAdmission.layer)),
  );

  it.effect("keeps unknown creation's logical owner across new producers and narrows only on validated identity", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      const hold = yield* admission.holdStartup({ ...target, nativeThreadId: null });
      yield* admission.assertOpen({ ...target, producer: { ...binding, runtimeGeneration: "new" } }).pipe(Effect.flip);
      yield* admission.assertOpen({ ...target, instanceId: "codex-b", producer: null }).pipe(Effect.flip);
      yield* admission.assertOpen({ ...target, threadId: "logical-b", nativeThreadId: "thread-b" }).pipe(Effect.flip);
      yield* admission.assertOpen({ ...target, threadId: "logical-b", nativeThreadId: "thread-b",
        producer: { ...binding, runtimeGeneration: "new" } });
      yield* admission.identifyStartup(hold, binding.nativeThreadId);
      yield* admission.assertOpen({ ...target, threadId: "logical-b", nativeThreadId: "thread-b" });
      yield* admission.assertOpen({ ...target, threadId: "different-alias", producer: null }).pipe(Effect.flip);
      yield* admission.resolveStartup(hold, { kind: "bound", binding: { ...nativeBinding, runtimeGeneration: "wrong" } }).pipe(Effect.flip);
      yield* admission.resolveStartup(hold, { kind: "bound", binding: nativeBinding });
      yield* admission.assertOpen(target);
    }).pipe(Effect.provide(ControlAdmission.layer)),
  );

  it.effect("rejects forged startup handles, selector-free requests and unrelated generation retirement", () =>
    Effect.gen(function* () {
      const admission = yield* ControlAdmission.ControlAdmission;
      yield* admission.assertOpen({ instanceId: binding.instanceId, threadId: null, nativeThreadId: null, producer: null }).pipe(Effect.flip);
      const hold = yield* admission.holdStartup(target);
      yield* admission.identifyStartup({} as ControlAdmission.StartupHold, "forged").pipe(Effect.flip);
      yield* admission.resolveStartup({} as ControlAdmission.StartupHold, { kind: "not_applied" }).pipe(Effect.flip);
      yield* admission.retireGeneration({ ...binding, providerSessionId: "wrong" });
      yield* admission.assertOpen(target).pipe(Effect.flip);
      yield* admission.retireGeneration(binding);
      yield* admission.assertOpen(target);
      yield* admission.resolveStartup(hold, { kind: "not_applied" }).pipe(Effect.flip);
    }).pipe(Effect.provide(ControlAdmission.layer)),
  );

  it.effect("shares obligations across consumers of one environment layer", () =>
    Effect.gen(function* () {
      const owner = yield* ControlAdmission.ControlAdmission;
      const dispatch = yield* owner.begin(binding);
      yield* owner.markUnknown(dispatch);
      yield* Layer.build(Layer.effectDiscard(Effect.gen(function* () {
        const rebuilt = yield* ControlAdmission.ControlAdmission;
        assert.equal(rebuilt, owner);
        yield* rebuilt.assertOpen(target).pipe(Effect.flip);
      })));
      yield* Effect.gen(function* () {
        const isolated = yield* ControlAdmission.ControlAdmission;
        yield* isolated.assertOpen(target);
      }).pipe(Effect.provide(Layer.fresh(ControlAdmission.layer)));
    }).pipe(Effect.scoped, Effect.provide(ControlAdmission.layer)),
  );

  it("fences mutation methods while preserving Stop and reconciliation", () => {
    for (const method of ["thread/start", "thread/resume", "thread/fork", "turn/start", "turn/steer", "thread/revert"])
      assert.isTrue(ControlAdmission.isCodexMutation(method, {}));
    assert.isTrue(ControlAdmission.isCodexMutation("thread/goal/set", { status: "active" }));
    assert.isFalse(ControlAdmission.isCodexMutation("thread/goal/set", { status: "paused" }));
    assert.isTrue(ControlAdmission.isCodexMutation("thread/goal/set", { status: "paused", objective: "new work" }));
    for (const method of ["turn/interrupt", "thread/read", "thread/goal/get", "thread/unsubscribe", "server/diagnostics"])
      assert.isFalse(ControlAdmission.isCodexMutation(method, {}));
  });
});
