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
});
