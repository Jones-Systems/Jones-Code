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

  it("releases normal successful starts without retaining their evidence", () => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    ledger.release(dispatch);
    ledger.markUnknown(dispatch);
    expect(ledger.blocked).toBe(false);
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
    ledger.observeReply(binding.runtimeGeneration, { jsonrpc: "2.0", id: 7, error });
    expect(ledger.reject(dispatch, binding.runtimeGeneration, refusal)).toBe("not_applied");
    expect(ledger.reject(other, binding.runtimeGeneration, refusal)).toBe("unverified");
    expect(ledger.blocked).toBe(true);
  });

  it.each([
    { name: "both a result and an error", fields: { result: null } },
    { name: "a string ID matching only after coercion", fields: { id: "7" } },
    { name: "a different numeric ID", fields: { id: 8 } },
    { name: "a malformed version", fields: { jsonrpc: "1.0" } },
    { name: "unexpected envelope fields", fields: { params: {} } },
    { name: "an unexpected error code", fields: {} },
  ])("keeps the fence for $name", ({ name, fields }) => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    const code = name === "an unexpected error code" ? -32601 : -32602;
    const error = { code, message: "Request rejected" };
    const refusal = CodexAppServerRequestError.fromProtocolError(error, "turn/start", "7");
    ledger.markUnknown(dispatch);
    ledger.observeReply(binding.runtimeGeneration, { id: 7, error, ...fields });
    expect(ledger.reject(dispatch, binding.runtimeGeneration, refusal)).toBe("unverified");
    expect(ledger.blocked).toBe(true);
  });

  it("requires evidence from the original producer and rejects transport failures", () => {
    const ledger = makeCodexLateStartRecovery();
    const dispatch = ledger.begin(binding, []);
    const error = { code: -32602, message: "Invalid params" };
    const refusal = CodexAppServerRequestError.fromProtocolError(error, "turn/start", "7");
    ledger.markUnknown(dispatch);
    ledger.observeReply("different-generation", { id: 7, error });
    expect(ledger.reject(dispatch, binding.runtimeGeneration, refusal)).toBe("unverified");
    ledger.observeReply(binding.runtimeGeneration, { id: 7, error: { ...error } });
    expect(ledger.reject(dispatch, binding.runtimeGeneration, refusal)).toBe("unverified");
    ledger.observeReply(binding.runtimeGeneration, { id: 7, error });
    expect(ledger.reject(dispatch, undefined, refusal)).toBe("unverified");
    const transportFailure = new CodexAppServerTransportError({
      operation: "read-input-stream",
      cause: refusal,
    });
    expect(ledger.reject(dispatch, binding.runtimeGeneration, transportFailure)).toBe(
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
    ledger.observeReply(binding.runtimeGeneration, { id: 7, error });
    expect(ledger.reject(dispatch, binding.runtimeGeneration, refusal)).toBe("unverified");
    expect(ledger.blocked).toBe(true);
  });
});
