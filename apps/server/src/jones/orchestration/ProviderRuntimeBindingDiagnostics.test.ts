import { assert, describe, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderThreadId, RunId, ThreadId } from "@t3tools/contracts";

import {
  hasUnknownRuntimeBinding,
  ProviderAdapterTurnStartError,
  ProviderRuntimeBindingError,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { ProviderEventOriginStaleError } from "@t3tools/provider-core/server/jones/ProviderEventOrigin";
import { classifyProviderRuntimeBindingFailure } from "@t3tools/provider-core/server/jones/ProviderRuntimeBindingDiagnostics";

const driver = ProviderDriverKind.make("codex");
const canary = "PRIVATE_CANARY_prompt_settings_env_path";
const refusals = [
  ["The launch reservation was lost.", "reservation_lost"],
  ["The launch reservation or native ownership was lost.", "reservation_or_ownership_lost"],
  ["Another producer already owns this native conversation.", "previous_generation_mismatch"],
  ["The native binding changed before its boundary committed.", "boundary_commit_empty"],
  ["Native execution has no committed physical runtime binding.", "physical_binding_absent"],
] as const;

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
    const knownCycle = { detail: refusals[3][0], cause: cycle };
    cycle.cause = knownCycle;
    assertDiagnostic(bindingError(cycle), "boundary_commit_empty");

    let deep: unknown = refusals[0][0];
    for (let depth = 0; depth < 15; depth += 1) deep = { cause: deep };
    assert.equal(classifyProviderRuntimeBindingFailure(deep), "reservation_lost");
    assert.equal(classifyProviderRuntimeBindingFailure({ cause: deep }), "unclassified");
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
      code: null,
      retryable: null,
    });
    assert.deepEqual(makeProviderFailure({ cause: wrapper }), {
      class: "unknown",
      message:
        "The provider could not start this turn. Retry the turn; if it keeps failing, check the provider setup and server logs.",
      code: null,
      retryable: null,
    });
    assert.isTrue(hasUnknownRuntimeBinding(wrapper));
    assert.isFalse(hasUnknownRuntimeBinding(new Error(canary)));
    assert.strictEqual(wrapper.cause, binding);
  });
});
