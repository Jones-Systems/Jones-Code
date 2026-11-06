import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind, ProviderInstanceId, ProviderSessionId, ProviderThreadId, ThreadId,
  type ProviderRuntimeBinding,
} from "@t3tools/contracts";
import { ProviderRuntimeBindingError } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import { makeProviderRecoveryRetirements } from "./providerRecoveryRetirement.ts";

const binding: ProviderRuntimeBinding = {
  driver: ProviderDriverKind.make("codex"), providerInstanceId: ProviderInstanceId.make("account"),
  providerSessionId: ProviderSessionId.make("session"), providerThreadId: ProviderThreadId.make("provider-thread"),
  threadId: ThreadId.make("thread"), nativeThreadId: "native", runtimeGeneration: "generation",
};

it.effect("retains exact native proof across cleanup failure and pending-ledger removal", () =>
  Effect.gen(function* () {
    const owner = makeProviderRecoveryRetirements();
    let attempts = 0;
    const handle = owner.retain(binding, Effect.suspend(() => {
      attempts += 1;
      return attempts === 1 ? Effect.fail(new ProviderRuntimeBindingError({
        driver: binding.driver, detail: "synthetic manager cleanup failure",
      })) : Effect.void;
    }));
    const waiting = yield* handle.awaitRetired.pipe(Effect.forkChild);
    assert.isTrue(Option.isNone(yield* Fiber.poll(waiting)));
    assert.isNull(owner.capture({ ...binding, runtimeGeneration: "foreign" }));
    yield* owner.complete({ binding: { ...binding, nativeThreadId: "foreign" }, evidence: "thread_closed" });
    assert.isTrue(Option.isNone(yield* Fiber.poll(waiting)));
    const receipt = { binding, evidence: "thread_closed" as const };
    yield* owner.complete(receipt);
    assert.equal((yield* Fiber.await(waiting))._tag, "Failure");
    assert.strictEqual(owner.capture(binding), handle);
    assert.deepEqual(yield* handle.awaitRetired, receipt);
    assert.equal(attempts, 2);
  }),
);
