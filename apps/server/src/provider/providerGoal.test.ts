import { assert, describe, it } from "@effect/vitest";

import { classifyProviderGoal, unknownProviderGoal } from "./providerGoal.ts";

const nativeThreadId = "native-goal-thread";
const goal = {
  createdAt: 1782622450,
  objective: "Complete the task",
  status: "active",
  threadId: nativeThreadId,
  timeUsedSeconds: 1,
  tokensUsed: 10,
  updatedAt: 1782622451,
};

describe("provider goal observations", () => {
  it("classifies an explicitly null goal as inactive", () => {
    assert.deepEqual(classifyProviderGoal({ goal: null }, nativeThreadId), {
      nativeThreadId,
      state: "inactive",
      reasonCode: "goal_null",
    });
  });

  it("classifies a valid goal for the requested native thread as active", () => {
    assert.deepEqual(classifyProviderGoal({ goal }, nativeThreadId), {
      nativeThreadId,
      state: "active",
      reasonCode: "goal_present",
    });
  });

  it("keeps an omitted goal field unknown", () => {
    assert.deepEqual(
      classifyProviderGoal({}, nativeThreadId),
      unknownProviderGoal("goal_field_omitted", nativeThreadId),
    );
  });

  it("keeps malformed responses and goal values unknown", () => {
    for (const response of [
      null,
      undefined,
      [],
      "inactive",
      { goal: undefined },
      { goal: {} },
      { goal: { ...goal, status: "invented" } },
      { goal: { ...goal, tokensUsed: "10" } },
      { goal: { ...goal, updatedAt: 0.5 } },
    ]) {
      assert.deepEqual(
        classifyProviderGoal(response, nativeThreadId),
        unknownProviderGoal("malformed", nativeThreadId),
      );
    }
  });

  it("keeps a valid goal belonging to another native thread unknown", () => {
    assert.deepEqual(
      classifyProviderGoal({ goal: { ...goal, threadId: "other-native-thread" } }, nativeThreadId),
      unknownProviderGoal("context_changed", nativeThreadId),
    );
  });
});
