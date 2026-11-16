import { describe, expect, it } from "vite-plus/test";
import { ProviderSessionId } from "@t3tools/contracts";
import { resolveComposerDispatchMode } from "@t3tools/client-runtime/state/composer-dispatch";

import { resolveQuestionAwareDispatchMode } from "./composerDispatch";

const liveQuestion = {
  kind: "user_input" as const,
  status: "pending" as const,
  responseCapability: {
    type: "live" as const,
    providerSessionId: ProviderSessionId.make("session"),
  },
};

describe("question-aware mobile message delivery", () => {
  it.each([
    { activeTurnDefault: "steer" as const, alternateModifier: false },
    { activeTurnDefault: "queue" as const, alternateModifier: true },
    { activeTurnDefault: "queue" as const, alternateModifier: false },
    { activeTurnDefault: "steer" as const, alternateModifier: true },
  ])("queues a busy live question with composer action %j", (action) => {
    expect(
      resolveQuestionAwareDispatchMode({
        busy: true,
        setupRunning: false,
        followUpAction: resolveComposerDispatchMode({ running: true, ...action }),
        requests: [liveQuestion],
      }),
    ).toBe("queue");
  });

  it("queues even when the active turn cannot currently be steered", () => {
    expect(
      resolveQuestionAwareDispatchMode({
        busy: true,
        setupRunning: false,
        followUpAction: "auto",
        requests: [liveQuestion],
      }),
    ).toBe("queue");
  });

  it("finds a live question after a message-mode question without changing either", () => {
    const requests = Object.freeze([
      Object.freeze({ ...liveQuestion, responseCapability: { type: "message" as const } }),
      Object.freeze(liveQuestion),
    ]);
    expect(
      resolveQuestionAwareDispatchMode({
        busy: true,
        setupRunning: false,
        followUpAction: "steer",
        requests,
      }),
    ).toBe("queue");
    expect(requests.map((request) => request.status)).toEqual(["pending", "pending"]);
  });

  it.each([
    { ...liveQuestion, responseCapability: { type: "message" as const } },
    { ...liveQuestion, responseCapability: { type: "not_resumable" as const, reason: "ended" } },
    { ...liveQuestion, status: "resolved" as const },
    { ...liveQuestion, status: "cancelled" as const },
    { ...liveQuestion, status: "expired" as const },
    { ...liveQuestion, kind: "dynamic_tool_call" as const },
  ])("preserves normal steering for %j", (request) => {
    expect(
      resolveQuestionAwareDispatchMode({
        busy: true,
        setupRunning: false,
        followUpAction: "steer",
        requests: [request],
      }),
    ).toBe("auto");
  });

  it("leaves idle sends on the normal start path", () => {
    expect(
      resolveQuestionAwareDispatchMode({
        busy: false,
        setupRunning: false,
        followUpAction: "auto",
        requests: [liveQuestion],
      }),
    ).toBeNull();
  });

  it("preserves setup queueing without a question", () => {
    expect(
      resolveQuestionAwareDispatchMode({
        busy: true,
        setupRunning: true,
        followUpAction: "auto",
        requests: [],
      }),
    ).toBe("queue");
  });
});
