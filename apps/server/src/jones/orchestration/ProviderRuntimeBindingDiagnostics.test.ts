import { assert, describe, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderThreadId, RunId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";

import {
  hasUnknownRuntimeBinding,
  ProviderAdapterTurnStartError,
  ProviderRuntimeBindingError,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { ProviderEventOriginStaleError } from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import {
  classifyProviderRuntimeBindingFailure,
  providerRuntimeBindingFailureCode,
  providerRuntimeBindingFailureCodeFromCause,
} from "@t3tools/provider-core/server/jones/ProviderRuntimeBindingDiagnostics";

const driver = ProviderDriverKind.make("codex");
const canary = "PRIVATE_CANARY_prompt_settings_env_path";
const refusals = [
  ["The launch reservation was lost.", "reservation_lost"],
  ["The launch reservation or native ownership was lost.", "reservation_or_ownership_lost"],
  ["Another producer already owns this native conversation.", "previous_generation_mismatch"],
  ["The native binding changed before its boundary committed.", "boundary_commit_empty"],
  ["Native execution has no committed physical runtime binding.", "physical_binding_absent"],
  [
    "Codex cannot admit a thread to an unconfirmed or replaced runtime.",
    "runtime_unconfirmed_or_replaced",
  ],
  ["Codex cannot admit a thread while a native start is unconfirmed.", "native_start_unconfirmed"],
  [
    "Codex cannot admit a thread to an inactive or replaced runtime producer.",
    "runtime_producer_not_current",
  ],
  [
    "The native thread opened but its runtime binding is unconfirmed; do not replay it.",
    "native_open_binding_unconfirmed",
  ],
  [
    "Codex resumed a different native conversation; continuation is unconfirmed.",
    "resume_native_thread_mismatch",
  ],
  [
    "Codex rollback resumed another native conversation; its effect is unconfirmed.",
    "rollback_native_thread_mismatch",
  ],
  [
    "Codex has an unconfirmed native effect; starting a new conversation is not safe.",
    "native_effect_unconfirmed_new_thread",
  ],
  [
    "Codex has an unconfirmed native effect; another resume is not safe.",
    "native_effect_unconfirmed_resume",
  ],
] as const;
const lifecycleDetail =
  "Failed to establish the actual provider runtime binding; native effects must not be replayed.";

function bindingError(cause: unknown, detail = canary) {
  return new ProviderRuntimeBindingError({ driver, detail, cause });
}

function assertDiagnostic(error: ProviderRuntimeBindingError, reason: string) {
  assert.equal(error.message, `Provider runtime binding failed [${reason}].`);
  assert.isString(error.stack);
  assert.include(error.stack!, error.message);
  assert.notInclude(error.message, canary);
  assert.notInclude(error.stack!, canary);
}

describe("provider runtime binding diagnostics", () => {
  it.each(refusals)(
    "exposes %s through real binding error messages and stacks",
    (detail, reason) => {
      assertDiagnostic(bindingError({ payload: canary }, detail), reason);
      const original = bindingError(detail);
      const wrapped = bindingError(original);
      assertDiagnostic(wrapped, reason);
      assert.strictEqual(wrapped.cause, original);
      assert.strictEqual(original.cause, detail);
      assert.equal(wrapped.detail, canary);
      assert.equal(wrapped.driver, driver);
      assert.equal(wrapped._tag, "ProviderRuntimeBindingError");
      assert.isTrue(hasUnknownRuntimeBinding(wrapped));
      assertDiagnostic(bindingError(new Error(detail)), reason);
    },
  );

  it("classifies the actual producer-origin tag without claiming a specific predicate", () => {
    const cause = new ProviderEventOriginStaleError({});
    assertDiagnostic(bindingError(bindingError(cause)), "producer_origin_stale");
    assert.equal(
      makeProviderFailure({ cause: bindingError(cause) }).code,
      "provider_runtime_binding_producer_origin_stale",
    );
  });

  it("preserves the specific refusal beneath binding establishment wrappers", () => {
    for (const [detail, reason] of refusals) {
      const lifecycle = bindingError(detail, lifecycleDetail);
      assertDiagnostic(lifecycle, reason);
      assertDiagnostic(
        bindingError(
          lifecycle,
          "The native thread opened but its runtime binding is unconfirmed; do not replay it.",
        ),
        reason,
      );
    }
    assertDiagnostic(
      bindingError(new Error(canary), lifecycleDetail),
      "binding_establishment_failed",
    );
  });

  it.each(refusals)("persists the closed code for %s through cause wrappers", (detail, reason) => {
    const binding = bindingError({ payload: canary }, detail);
    for (const cause of [binding, new Error(canary, { cause: binding }), Cause.fail(binding)]) {
      const failure = makeProviderFailure({ cause });
      assert.equal(failure.code, `provider_runtime_binding_${reason}`);
      assert.notInclude(JSON.stringify(failure), canary);
    }
    assert.equal(
      makeProviderFailure({ cause: binding, code: "explicit_code" }).code,
      "explicit_code",
    );
    assert.strictEqual(binding.cause && (binding.cause as { payload: string }).payload, canary);
  });

  it("does not turn an unrelated matching message into a binding failure code", () => {
    assert.equal(makeProviderFailure({ cause: new Error(refusals[0][0]) }).code, null);
    assert.equal(
      makeProviderFailure({ cause: { _tag: canary, code: "existing_code" } }).code,
      "existing_code",
    );
    assert.equal(
      makeProviderFailure({ cause: bindingError(new Error(canary)) }).code,
      "provider_runtime_binding_unclassified",
    );
  });

  it("leaves arbitrary data, partial matches, and unknown tags unclassified", () => {
    for (const cause of [
      canary,
      new Error(canary),
      { _tag: canary, detail: canary, message: canary, payload: canary },
      { _tag: "ProviderEventOriginStaleError_extra" },
      { _tag: "ProviderRuntimeBindingError" },
      { payload: { cause: refusals[0][0] } },
      `${refusals[0][0]} ${canary}`,
      `prefix ${refusals[0][0]}`,
      null,
      undefined,
    ]) {
      assertDiagnostic(bindingError(cause), "unclassified");
    }
  });

  it("bounds cause depth and stops cycles without hiding a known refusal", () => {
    const cycle: { cause?: unknown } = {};
    cycle.cause = cycle;
    assertDiagnostic(bindingError(cycle), "unclassified");
    assert.isUndefined(providerRuntimeBindingFailureCode(cycle));
    const knownCycle = { detail: refusals[3][0], cause: cycle };
    cycle.cause = knownCycle;
    assertDiagnostic(bindingError(cycle), "boundary_commit_empty");

    let deep: unknown = refusals[0][0];
    for (let depth = 0; depth < 15; depth += 1) deep = { cause: deep };
    assert.equal(classifyProviderRuntimeBindingFailure(deep), "reservation_lost");
    assert.equal(classifyProviderRuntimeBindingFailure({ cause: deep }), "unclassified");
    let nestedBinding: unknown = bindingError(refusals[0][0]);
    for (let depth = 0; depth < 15; depth += 1) nestedBinding = { cause: nestedBinding };
    assert.equal(
      providerRuntimeBindingFailureCode(nestedBinding),
      "provider_runtime_binding_reservation_lost",
    );
    assert.isUndefined(providerRuntimeBindingFailureCode({ cause: nestedBinding }));
  });

  it("does not invoke own or inherited accessors, coercion, or proxy traps", () => {
    let reads = 0;
    const accessor = {};
    for (const key of ["_tag", "detail", "message", "cause", "toString"]) {
      Object.defineProperty(accessor, key, {
        get: () => {
          reads += 1;
          throw new Error(canary);
        },
      });
    }
    assertDiagnostic(bindingError(accessor), "unclassified");
    assertDiagnostic(bindingError(Object.create(accessor)), "unclassified");
    assert.isUndefined(providerRuntimeBindingFailureCode(accessor));
    assert.isUndefined(providerRuntimeBindingFailureCode(Object.create(accessor)));
    assertDiagnostic(
      bindingError({
        toString: () => {
          reads += 1;
          return refusals[0][0];
        },
      }),
      "unclassified",
    );
    const proxy = new Proxy(
      {},
      {
        getOwnPropertyDescriptor: () => {
          reads += 1;
          throw new Error(canary);
        },
      },
    );
    assertDiagnostic(bindingError(proxy), "unclassified");
    assert.isUndefined(providerRuntimeBindingFailureCode(proxy));
    assert.equal(reads, 0);
  });

  it("normalizes Effect causes without invoking accessors, iterators, or proxy traps", () => {
    let reads = 0;
    const read = () => {
      reads += 1;
      return bindingError(refusals[0][0]);
    };
    const branded = (reasons: unknown) => ({ [Cause.TypeId]: Cause.TypeId, reasons });
    const reasonsAccessor = { [Cause.TypeId]: Cause.TypeId };
    Object.defineProperty(reasonsAccessor, "reasons", { get: read });
    const indexAccessor = [undefined];
    Object.defineProperty(indexAccessor, "0", { get: read });
    const reasonAccessor = {};
    Object.defineProperty(reasonAccessor, "_tag", { get: read });
    const errorAccessor = { _tag: "Fail" };
    Object.defineProperty(errorAccessor, "error", { get: read });
    const iteratorAccessor: Array<unknown> = [];
    Object.defineProperty(iteratorAccessor, Symbol.iterator, { get: read });
    const proxy = new Proxy(
      {},
      {
        has: () => {
          reads += 1;
          return false;
        },
        get: read,
        getOwnPropertyDescriptor: () => {
          reads += 1;
          return undefined;
        },
      },
    );
    for (const value of [
      reasonsAccessor,
      branded(indexAccessor),
      branded([reasonAccessor]),
      branded([errorAccessor]),
      branded(iteratorAccessor),
      proxy,
      Object.create(proxy),
      branded(proxy),
      branded([proxy]),
    ]) {
      assert.isUndefined(providerRuntimeBindingFailureCodeFromCause(value));
    }
    assert.equal(reads, 0);
    for (const value of [
      Cause.fail(bindingError(refusals[0][0])),
      Cause.die(bindingError(refusals[0][0])),
    ]) {
      assert.equal(
        providerRuntimeBindingFailureCodeFromCause(value),
        "provider_runtime_binding_reservation_lost",
      );
    }
  });

  it("keeps Effect fail-before-defect precedence and bounds reason fan-out", () => {
    const fail = Cause.fail(bindingError(refusals[0][0]));
    const die = Cause.die(bindingError(refusals[3][0]));
    assert.equal(
      providerRuntimeBindingFailureCodeFromCause(Cause.combine(die, fail)),
      "provider_runtime_binding_reservation_lost",
    );
    const reasons = Array.from({ length: 16 }, () => die.reasons[0]);
    assert.equal(
      providerRuntimeBindingFailureCodeFromCause({ [Cause.TypeId]: Cause.TypeId, reasons }),
      "provider_runtime_binding_boundary_commit_empty",
    );
    let reads = 0;
    Object.defineProperty(reasons, "16", {
      get: () => {
        reads += 1;
        return fail.reasons[0];
      },
    });
    assert.isUndefined(
      providerRuntimeBindingFailureCodeFromCause({ [Cause.TypeId]: Cause.TypeId, reasons }),
    );
    assert.equal(reads, 0);
  });

  it("preserves generic public failures and unknown-native-effect matching", () => {
    const binding = bindingError(refusals[2][0]);
    const wrapper = new ProviderAdapterTurnStartError({
      driver,
      threadId: ThreadId.make("thread"),
      providerThreadId: ProviderThreadId.make("provider-thread"),
      runId: RunId.make("run"),
      cause: binding,
    });
    assert.deepEqual(makeProviderFailure({ cause: binding }), {
      class: "unknown",
      message: "Provider turn failed.",
      code: "provider_runtime_binding_previous_generation_mismatch",
      retryable: null,
    });
    assert.deepEqual(makeProviderFailure({ cause: wrapper }), {
      class: "unknown",
      message:
        "The provider could not start this turn. Retry the turn; if it keeps failing, check the provider setup and server logs.",
      code: "provider_runtime_binding_previous_generation_mismatch",
      retryable: null,
    });
    assert.isTrue(hasUnknownRuntimeBinding(wrapper));
    assert.isFalse(hasUnknownRuntimeBinding(new Error(canary)));
    assert.strictEqual(wrapper.cause, binding);
  });
});
