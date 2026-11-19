import {
  CodexAppServerRequestError,
  CodexAppServerTransportError,
} from "effect-codex-app-server/errors";
import { describe, expect, it } from "vite-plus/test";
import { makeCodexLateStartRecovery } from "./CodexLateStartRecovery.ts";

const binding = { runtimeGeneration: "generation-1", nativeThreadId: "conversation-1" };

describe("Codex late start admission recovery", () => {
  it.each(["ack-first", "completion-first"])("requires both exact facts (%s)", (order) => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    ledger.markUnknown(dispatch);
    if (order === "ack-first") ledger.acknowledge(dispatch, binding.runtimeGeneration, "turn-1");
    else ledger.complete(binding, "turn-1");
    expect(ledger.blocked).toBe(true);
    if (order === "ack-first") ledger.complete(binding, "turn-1");
    else ledger.acknowledge(dispatch, binding.runtimeGeneration, "turn-1");
    expect(ledger.blocked).toBe(false);
  });

  it("rejects wrong generation, conversation, turn, retired turn and foreign dispatch", () => {
    const ledger = makeCodexLateStartRecovery();
    const foreign = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, ["retired-turn"]);
    ledger.markUnknown(dispatch);
    ledger.acknowledge(foreign.begin(binding, []), binding.runtimeGeneration, "turn-1");
    ledger.acknowledge(dispatch, "retired-generation", "turn-1");
    ledger.complete(binding, "turn-1");
    expect(ledger.blocked).toBe(true);
    ledger.acknowledge(dispatch, binding.runtimeGeneration, "retired-turn");
    ledger.complete(binding, "retired-turn");
    expect(ledger.blocked).toBe(true);
    ledger.acknowledge(dispatch, binding.runtimeGeneration, "turn-2");
    ledger.complete({ ...binding, runtimeGeneration: "retired-generation" }, "turn-2");
    ledger.complete({ ...binding, nativeThreadId: "other-conversation" }, "turn-2");
    ledger.release(dispatch);
    expect(ledger.blocked).toBe(true);
    ledger.complete(binding, "turn-2");
    expect(ledger.blocked).toBe(false);
  });

  it("keeps every unresolved dispatch fenced and isolates other sessions", () => {
    const ledger = makeCodexLateStartRecovery();
    const sibling = { ...binding, nativeThreadId: "conversation-2" };
    const first = ledger.begin(binding, []);
    const second = ledger.begin(sibling, []);
    ledger.markUnknown(first);
    ledger.markUnknown(second);
    ledger.acknowledge(first, binding.runtimeGeneration, "turn-1");
    ledger.complete(binding, "turn-1");
    expect(ledger.blocked).toBe(true);
    expect(makeCodexLateStartRecovery().blocked).toBe(false);
    ledger.acknowledge(second, binding.runtimeGeneration, "turn-2");
    ledger.complete(sibling, "turn-2");
    expect(ledger.blocked).toBe(false);
  });

  it("fails closed on early evidence overflow without growing its buffer", () => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    ledger.markUnknown(dispatch);
    for (let index = 0; index < 40; index++) ledger.complete(binding, `turn-${index}`);
    ledger.acknowledge(dispatch, binding.runtimeGeneration, "turn-1");
    ledger.complete(binding, "turn-1");
    expect(dispatch.completions.size).toBe(32);
    expect(ledger.blocked).toBe(true);
  });

  it("does not count duplicate evidence against the early buffer", () => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    for (let index = 0; index < 40; index++) ledger.complete(binding, "turn-1");
    ledger.markUnknown(dispatch);
    ledger.acknowledge(dispatch, binding.runtimeGeneration, "turn-1");
    expect(ledger.blocked).toBe(false);
  });

  it("retains conclusive facts arriving before the caller reports its timeout", () => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    ledger.acknowledge(dispatch, binding.runtimeGeneration, "turn-1");
    ledger.complete(binding, "turn-1");
    ledger.markUnknown(dispatch);
    ledger.markUnknown(dispatch);
    expect(ledger.blocked).toBe(false);
  });

  it("does not retain a fence for a released dispatch", () => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    ledger.release(dispatch);
    ledger.markUnknown(dispatch);
    expect(ledger.blocked).toBe(false);
  });

  it("recognizes a previously acknowledged turn on the same producer and conversation", () => {
    const ledger = makeCodexLateStartRecovery();
    const earlier = ledger.begin(binding, []);
    ledger.acknowledge(earlier, binding.runtimeGeneration, "earlier-turn");
    ledger.release(earlier);
    const current = ledger.begin(binding, []);
    const replacement = ledger.begin({ ...binding, runtimeGeneration: "replacement" }, []);
    const request = {
      id: "ask",
      method: "mcpServer/elicitation/request",
      params: { threadId: binding.nativeThreadId, turnId: "earlier-turn" },
    };
    ledger.observeFrame(binding.runtimeGeneration, request);
    ledger.observeFrame(replacement.runtimeGeneration, request);
    expect(current.turnActivityObserved).toBe(false);
    expect(replacement.turnActivityObserved).toBe(true);
  });

  it("orders accepted context by dispatch, including when a newer ACK clears it", () => {
    const ledger = makeCodexLateStartRecovery<string>();
    const first = ledger.begin(binding, []);
    const second = ledger.begin(binding, []);
    const third = ledger.begin(binding, []);
    const clearsContext = ledger.begin(binding, []);
    ledger.acknowledge(second, binding.runtimeGeneration, "second", "B");
    ledger.acknowledge(first, binding.runtimeGeneration, "first", "A");
    expect(ledger.acceptedContext(binding.nativeThreadId)).toBe("B");
    ledger.acknowledge(clearsContext, binding.runtimeGeneration, "fourth");
    ledger.acknowledge(third, binding.runtimeGeneration, "third", "C");
    expect(ledger.acceptedContext(binding.nativeThreadId)).toBeUndefined();
  });

  it("does not publish context from a foreign dispatch, producer, or previously owned turn", () => {
    const ledger = makeCodexLateStartRecovery<string>();
    const first = ledger.begin(binding, []);
    ledger.acknowledge(first, binding.runtimeGeneration, "current-turn", "A");
    const sibling = ledger.begin({ ...binding, nativeThreadId: "sibling" }, []);
    ledger.acknowledge(sibling, binding.runtimeGeneration, "sibling-turn", "sibling");
    const later = ledger.begin(binding, []);
    ledger.acknowledge(later, "dead-producer", "other-turn", "stale");
    ledger.acknowledge(later, binding.runtimeGeneration, "current-turn", "duplicate");
    ledger.acknowledge(later, binding.runtimeGeneration, "sibling-turn", "foreign-turn");
    ledger.acknowledge(
      makeCodexLateStartRecovery().begin(binding, []),
      binding.runtimeGeneration,
      "foreign-dispatch",
      "foreign",
    );
    expect(ledger.acceptedContext(binding.nativeThreadId)).toBe("A");
    expect(later.nativeTurnId).toBeUndefined();
  });

  it("clears only the dispatch resolved by its original rejection", () => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, ["previous-turn"]);
    const other = ledger.begin({ ...binding, nativeThreadId: "conversation-2" }, []);
    const error = { code: -32602, message: "Invalid params", data: { field: "input" } };
    const refusal = CodexAppServerRequestError.fromProtocolError(error, "turn/start", "7");
    ledger.markUnknown(dispatch);
    ledger.markUnknown(other);
    ledger.observeTurnActivity(binding.runtimeGeneration, {
      threadId: binding.nativeThreadId,
      turnId: "previous-turn",
    });
    ledger.observeFrame(binding.runtimeGeneration, { jsonrpc: "2.0", id: 7, error });
    const rejection = ledger.reject(dispatch, () => binding.runtimeGeneration, refusal);
    expect(rejection.kind).toBe("not_applied");
    if (rejection.kind !== "not_applied") return;
    expect(rejection.canReconcileLate).toBe(true);
    expect(ledger.blocked).toBe(true);
    expect(rejection.release()).toBe(true);
    expect(ledger.reject(other, () => binding.runtimeGeneration, refusal).kind).toBe("unverified");
    expect(ledger.blocked).toBe(true);
  });

  it("attributes activity after rejection release to later dispatch ownership", () => {
    const ledger = makeCodexLateStartRecovery();
    const currentGeneration = () => binding.runtimeGeneration;
    const original = ledger.begin(binding, []);
    ledger.markUnknown(original);
    const rejectionFor = (dispatch: typeof original, id: number) => {
      const error = { code: -32602, message: "Invalid params" };
      ledger.observeFrame(binding.runtimeGeneration, { id, error });
      return ledger.reject(
        dispatch,
        currentGeneration,
        CodexAppServerRequestError.fromProtocolError(error, "turn/start", String(id)),
      );
    };
    const originalProof = rejectionFor(original, 7);
    if (originalProof.kind !== "not_applied") throw new Error("Expected exact rejection proof");
    expect(originalProof.release()).toBe(true);

    const next = ledger.begin(binding, []);
    ledger.observeFrame(binding.runtimeGeneration, {
      method: "turn/started",
      params: { threadId: binding.nativeThreadId, turn: { id: "unattributed-turn" } },
    });
    ledger.markUnknown(original);
    expect(originalProof.release()).toBe(false);
    expect(ledger.blocked).toBe(false);
    ledger.markUnknown(next);
    expect(rejectionFor(next, 8).kind).toBe("unverified");
    expect(ledger.blocked).toBe(true);
  });

  it.each([-32600, -32601])("keeps code %s proof scoped to the original wait", (code) => {
    const ledger = makeCodexLateStartRecovery();
    const reject = () => {
      const dispatch = ledger.begin(binding, []);
      const error = { code, message: "Request rejected" };
      ledger.observeFrame(binding.runtimeGeneration, { id: 7, error });
      return {
        dispatch,
        rejection: ledger.reject(
          dispatch,
          () => binding.runtimeGeneration,
          CodexAppServerRequestError.fromProtocolError(error, "turn/start", "7"),
        ),
      };
    };
    const inTime = reject();
    expect(inTime.rejection.kind).toBe("not_applied");
    if (inTime.rejection.kind !== "not_applied") return;
    expect(inTime.rejection.canReconcileLate).toBe(false);
    expect(inTime.rejection.release()).toBe(true);
    ledger.markUnknown(inTime.dispatch);
    expect(ledger.blocked).toBe(false);

    const late = reject();
    ledger.markUnknown(late.dispatch);
    expect(late.rejection.kind).toBe("not_applied");
    if (late.rejection.kind !== "not_applied") return;
    expect(late.rejection.release()).toBe(false);
    expect(ledger.blocked).toBe(true);
  });

  it.each(["producer", "turn activity"] as const)(
    "revokes a qualified rejection when its %s changes",
    (change) => {
      const ledger = makeCodexLateStartRecovery();
      const dispatch = ledger.begin(binding, ["previous-turn"]);
      ledger.markUnknown(dispatch);
      let currentGeneration = binding.runtimeGeneration;
      const error = { code: -32602, message: "Invalid params" };
      ledger.observeFrame(binding.runtimeGeneration, { id: 7, error });
      const proof = ledger.reject(
        dispatch,
        () => currentGeneration,
        CodexAppServerRequestError.fromProtocolError(error, "turn/start", "7"),
      );
      expect(proof.kind).toBe("not_applied");
      if (proof.kind !== "not_applied") return;
      const activity = {
        method: "item/started",
        params: { threadId: binding.nativeThreadId, turnId: "possibly-applied-turn" },
      };
      ledger.observeFrame("foreign-generation", activity);
      ledger.observeFrame(binding.runtimeGeneration, {
        ...activity,
        params: { ...activity.params, threadId: "sibling" },
      });
      expect(proof.isCurrent()).toBe(true);
      if (change === "producer") currentGeneration = "replacement";
      else ledger.observeFrame(binding.runtimeGeneration, activity);
      expect(proof.isCurrent()).toBe(false);
      expect(proof.release()).toBe(false);
      expect(ledger.blocked).toBe(true);
    },
  );

  it.each([
    { name: "both a result and an error", fields: { result: null } },
    { name: "both a result and an invalid request", fields: { result: null }, code: -32600 },
    { name: "a string ID matching only after coercion", fields: { id: "7" } },
    { name: "a different numeric ID", fields: { id: 8 } },
    { name: "a malformed version", fields: { jsonrpc: "1.0" } },
    { name: "unexpected envelope fields", fields: { params: {} } },
  ])("keeps the fence for $name", ({ code = -32602, fields }) => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    const error = { code, message: "Request rejected" };
    const refusal = CodexAppServerRequestError.fromProtocolError(error, "turn/start", "7");
    ledger.markUnknown(dispatch);
    ledger.observeFrame(binding.runtimeGeneration, { id: 7, error, ...fields });
    expect(ledger.reject(dispatch, () => binding.runtimeGeneration, refusal).kind).toBe(
      "unverified",
    );
    expect(ledger.blocked).toBe(true);
  });

  it("requires evidence from the original producer and rejects transport failures", () => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    const error = { code: -32602, message: "Invalid params" };
    const refusal = CodexAppServerRequestError.fromProtocolError(error, "turn/start", "7");
    ledger.markUnknown(dispatch);
    ledger.observeFrame("different-generation", { id: 7, error });
    expect(ledger.reject(dispatch, () => binding.runtimeGeneration, refusal).kind).toBe(
      "unverified",
    );
    ledger.observeFrame(binding.runtimeGeneration, { id: 7, error: { ...error } });
    expect(ledger.reject(dispatch, () => binding.runtimeGeneration, refusal).kind).toBe(
      "unverified",
    );
    ledger.observeFrame(binding.runtimeGeneration, { id: 7, error });
    expect(ledger.reject(dispatch, () => undefined, refusal).kind).toBe("unverified");
    const transportFailure = new CodexAppServerTransportError({
      operation: "read-input-stream",
      cause: refusal,
    });
    expect(ledger.reject(dispatch, () => binding.runtimeGeneration, transportFailure).kind).toBe(
      "other_error",
    );
    expect(ledger.blocked).toBe(true);
  });

  it("keeps the fence when the rejection conflicts with observed native turn activity", () => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    const error = { code: -32602, message: "Invalid params" };
    const refusal = CodexAppServerRequestError.fromProtocolError(error, "turn/start", "7");
    ledger.markUnknown(dispatch);
    ledger.observeTurnActivity(binding.runtimeGeneration, {
      threadId: binding.nativeThreadId,
      turn: { id: "possible-native-turn" },
    });
    ledger.observeFrame(binding.runtimeGeneration, { id: 7, error });
    expect(ledger.reject(dispatch, () => binding.runtimeGeneration, refusal).kind).toBe(
      "unverified",
    );
    expect(ledger.blocked).toBe(true);
  });
});
