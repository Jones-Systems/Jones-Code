import { assert, describe, it } from "@effect/vitest";
import * as CodexErrors from "effect-codex-app-server/errors";
import {
  HistoryNotApplied,
  PromptNotApplied,
  historyInjectionRejection,
  historyWasNotApplied,
  promptWasNotApplied,
} from "./HistoryDeliveryOutcome.ts";

describe("history injection evidence", () => {
  it.each([
    { code: -32600, operation: "receive-response", method: "thread/inject_items", expected: "not_applied" },
    { code: -32602, operation: "receive-response", method: "thread/inject_items", expected: "not_applied" },
    { code: -32601, operation: "receive-response", method: "thread/inject_items", expected: "unsupported" },
    { code: -32602, operation: "encode-payload", method: "thread/inject_items", expected: "not_applied" },
    { code: -32602, operation: "decode-payload", method: "thread/inject_items", expected: "ambiguous" },
    { code: -32602, operation: "handle-request", method: "thread/inject_items", expected: "ambiguous" },
    { code: -32601, operation: undefined, method: undefined, expected: "ambiguous" },
    { code: -32602, operation: "receive-response", method: "turn/start", expected: "ambiguous" },
    { code: -32603, operation: "receive-response", method: "thread/inject_items", expected: "ambiguous" },
  ] as const)("requires attributed non-applying evidence: %j", ({ expected, ...input }) => {
    const error = new CodexErrors.CodexAppServerRequestError({
      code: input.code,
      errorMessage: "Synthetic rejection",
      ...(input.method === undefined ? {} : { method: input.method }),
      ...(input.operation === undefined ? {} : { operation: input.operation }),
    });
    assert.equal(historyInjectionRejection(error), expected);
    assert.isFalse(historyWasNotApplied(error));
  });

  it.each([
    { operation: "encode-payload", code: -32602, expected: true },
    { operation: "receive-response", code: -32600, expected: true },
    { operation: "receive-response", code: -32602, expected: true },
    { operation: "decode-payload", code: -32602, expected: false },
    { operation: "receive-response", code: -32603, expected: false },
  ] as const)("attributes inline prompt refusals without admitting ambiguous failures: %j", ({ expected, ...input }) => {
    const cause = new CodexErrors.CodexAppServerRequestError({
      ...input, method: "turn/start", errorMessage: "Synthetic rejection",
    });
    assert.equal(promptWasNotApplied({ cause }), expected);
  });

  it("recognizes the private startup deadline's precluded-dispatch proof", () => {
    assert.isTrue(promptWasNotApplied({ cause: { _tag: "CodexStartupNotDispatched" } }));
    assert.isTrue(promptWasNotApplied(new PromptNotApplied({ cause: new Error("Local refusal") })));
    assert.isFalse(historyWasNotApplied(new PromptNotApplied({ cause: new Error("Local refusal") })));
    assert.isFalse(promptWasNotApplied(new Error("Timed out waiting for response")));
  });

  it("preserves the immediate cause and handles cyclic wrappers without inventing evidence", () => {
    const cause = new Error("Synthetic local admission refusal");
    const proof = new HistoryNotApplied({ cause });
    assert.strictEqual(proof.cause, cause);
    assert.isTrue(historyWasNotApplied({ cause: proof }));
    const cycle: { cause?: unknown } = {};
    cycle.cause = cycle;
    assert.isFalse(historyWasNotApplied(cycle));
    assert.equal(historyInjectionRejection(new Error("Transport lost")), "ambiguous");
  });
});
