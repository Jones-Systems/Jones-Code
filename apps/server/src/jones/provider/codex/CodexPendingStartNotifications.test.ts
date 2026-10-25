import { describe, expect, it } from "vite-plus/test";

import { makeCodexLateStartRecovery } from "./CodexLateStartRecovery.ts";
import { makeCodexPendingStartNotifications } from "./CodexPendingStartNotifications.ts";

const binding = { runtimeGeneration: "producer", nativeThreadId: "thread" };
const payload = (turnId = "turn", delta = "output") => ({
  threadId: binding.nativeThreadId,
  turnId,
  itemId: "command",
  delta,
});

describe("Codex pending-start notifications", () => {
  it("replays only the ACK-selected turn in order, including arrivals during replay", () => {
    const buffer = makeCodexPendingStartNotifications<string>();
    const dispatch = makeCodexLateStartRecovery().begin(binding, ["historical"]);
    buffer.begin(dispatch);
    expect(buffer.capture("old-producer", "item/started", payload(), "stale")).toBeUndefined();
    expect(buffer.capture("producer", "item/started", payload("historical"), "old")).toBeUndefined();
    expect(
      buffer.capture("producer", "item/started", { ...payload(), threadId: "other" }, "other"),
    ).toBeUndefined();
    expect(
      buffer.capture(
        "producer",
        "turn/completed",
        { threadId: "thread", turn: { id: "turn" } },
        "early-terminal",
      ),
    ).toBeUndefined();
    buffer.capture("producer", "item/started", payload("unrelated"), "unrelated");
    buffer.capture("producer", "item/started", payload(), "started");
    buffer.capture("producer", "item/commandExecution/outputDelta", payload(), "delta");
    expect(buffer.take(dispatch)).toBeUndefined();
    buffer.acknowledge(dispatch, "turn");
    expect(buffer.take(dispatch)).toBe("started");
    buffer.capture("producer", "item/completed", payload(), "completed");
    buffer.capture(
      "producer",
      "turn/completed",
      { threadId: "thread", turn: { id: "turn" } },
      "terminal",
    );
    expect(buffer.take(dispatch)).toBe("delta");
    expect(buffer.take(dispatch)).toBe("completed");
    expect(buffer.take(dispatch)).toBe("terminal");
    expect(buffer.take(dispatch)).toBeUndefined();
    expect(buffer.take(dispatch)).toBeUndefined();
  });

  it("keeps an acknowledged tail owned by its dispatch when a successor starts", () => {
    const buffer = makeCodexPendingStartNotifications<string>();
    const ledger = makeCodexLateStartRecovery();
    const first = ledger.begin(binding, []);
    buffer.begin(first);
    buffer.capture("producer", "item/completed", payload(), "first-tail");
    buffer.acknowledge(first, "turn");
    const next = ledger.begin(binding, ["turn"]);
    buffer.begin(next);
    buffer.capture("producer", "item/started", payload("next"), "next-start");
    expect(buffer.take(first)).toBe("first-tail");
    expect(buffer.take(first)).toBeUndefined();
    buffer.acknowledge(next, "next");
    expect(buffer.take(next)).toBe("next-start");
    expect(buffer.take(next)).toBeUndefined();
  });

  it("clears cancelled or failed dispatches without clearing their replacement", () => {
    const buffer = makeCodexPendingStartNotifications<string>();
    const ledger = makeCodexLateStartRecovery();
    const cancelled = ledger.begin(binding, []);
    buffer.begin(cancelled);
    buffer.capture("producer", "item/started", payload(), "cancelled");
    buffer.clear(cancelled);
    buffer.acknowledge(cancelled, "turn");
    expect(buffer.take(cancelled)).toBeUndefined();
    const replacement = ledger.begin(binding, []);
    buffer.begin(replacement);
    buffer.capture("producer", "item/started", payload(), "replacement");
    buffer.clear(cancelled);
    buffer.acknowledge(cancelled, "turn");
    expect(buffer.take(cancelled)).toBeUndefined();
    buffer.acknowledge(replacement, "turn");
    expect(buffer.take(replacement)).toBe("replacement");
    buffer.clear(replacement);
    expect(buffer.take(replacement)).toBeUndefined();
  });

  it.each(["count", "bytes"] as const)("discards the whole replay on %s overflow", (limit) => {
    const buffer = makeCodexPendingStartNotifications<string>();
    const dispatch = makeCodexLateStartRecovery().begin(binding, []);
    buffer.begin(dispatch);
    buffer.capture("producer", "item/started", payload(), "partial");
    if (limit === "count") {
      for (let index = 1; index < 512; index++)
        expect(
          buffer.capture("producer", "item/commandExecution/outputDelta", payload(), "delta")?.status,
        ).toBe("buffered");
    }
    expect(
      buffer.capture(
        "producer",
        "item/commandExecution/outputDelta",
        payload("turn", limit === "bytes" ? "é".repeat(512 * 1024) : "overflow"),
        "overflow",
      ),
    ).toEqual({ status: "overflow", dispatch });
    buffer.acknowledge(dispatch, "turn");
    expect(buffer.take(dispatch)).toBeUndefined();
  });
});
