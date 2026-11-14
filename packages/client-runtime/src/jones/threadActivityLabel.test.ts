import { describe, expect, it } from "vite-plus/test";
import { hasThreadProviderActivity, threadActivityLabel } from "./threadActivityLabel.ts";
import { ProviderDriverKind, ProviderTurnId, RunId } from "@t3tools/contracts";

describe("threadActivityLabel", () => {
  it("waits for server acknowledgement on a first send or after a settled run", () => {
    expect(threadActivityLabel(null)).toBe("Connecting");
    for (const status of [
      "completed",
      "failed",
      "cancelled",
      "interrupted",
      "rolled_back",
    ] as const) {
      expect(threadActivityLabel(status)).toBe("Connecting");
    }
  });

  it("separates accepted startup from a confirmed active run", () => {
    expect(threadActivityLabel("preparing")).toBe("Starting");
    expect(threadActivityLabel("queued")).toBe("Starting");
    expect(threadActivityLabel("starting")).toBe("Starting");
    expect(threadActivityLabel("running")).toBe("Starting");
    expect(threadActivityLabel("waiting")).toBe("Starting");
    expect(threadActivityLabel("running", false, true)).toBe("Thinking");
    expect(threadActivityLabel("waiting", false, true)).toBe("Thinking");
  });

  it("recognizes confirmed runless activity without letting it hide startup", () => {
    expect(threadActivityLabel(null, true)).toBe("Thinking");
    expect(threadActivityLabel("completed", true)).toBe("Thinking");
    expect(threadActivityLabel("starting", true)).toBe("Starting");
  });
});

it("requires native work from the current run and provider turn before showing Thinking", () => {
  const runId = RunId.make("current-run");
  const providerTurnId = ProviderTurnId.make("current-turn");
  const nativeItemRef = {
    driver: ProviderDriverKind.make("codex"),
    nativeId: "item",
    strength: "strong" as const,
  };
  const item = { runId, providerTurnId, nativeItemRef, type: "reasoning" as const };
  expect(hasThreadProviderActivity([], runId, providerTurnId)).toBe(false);
  expect(hasThreadProviderActivity([item], runId, providerTurnId)).toBe(true);
  expect(hasThreadProviderActivity([item], runId, null)).toBe(false);
  expect(hasThreadProviderActivity([item], RunId.make("new-run"), providerTurnId)).toBe(false);
  expect(hasThreadProviderActivity([item], runId, ProviderTurnId.make("new-attempt-turn"))).toBe(
    false,
  );
  for (const type of ["user_message", "system_notice", "notification", "error"] as const) {
    expect(hasThreadProviderActivity([{ ...item, type }], runId, providerTurnId)).toBe(false);
  }
  expect(
    hasThreadProviderActivity(
      [{ ...item, type: "command_execution", nativeItemRef: null }],
      runId,
      providerTurnId,
    ),
  ).toBe(false);
});
