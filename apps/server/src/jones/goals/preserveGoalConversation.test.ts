import { expect, it } from "vite-plus/test";

import { nativeConversationMustBePreserved } from "./preserveGoalConversation.ts";

const codex = { driver: "codex", nativeThreadRef: null };
const message = (text: string, attachments: ReadonlyArray<unknown> = []) => ({ text, attachments });

it.each([undefined, null])("preserves established conversations with an unproved goal: %s", (goal) => {
  expect(
    nativeConversationMustBePreserved(
      { ...codex, nativeThreadRef: { nativeId: "existing" }, goal },
      message("Continue", [{}]),
    ),
  ).toBe(true);
});

it.each(["active", "paused", "complete", "blocked", "usage_limited", "budget_limited"])(
  "retained %s goal requires its native conversation even with attachments",
  (status) => {
    expect(
      nativeConversationMustBePreserved({ ...codex, goal: { status } }, message("Continue", [{}])),
    ).toBe(true);
  },
);

it.each(["/compact", " /COMPACT ", "/goal resume", "  /goal   RESUME  "])(
  "requires existing native identity for maintenance: %s",
  (text) => expect(nativeConversationMustBePreserved(codex, message(text))).toBe(true),
);

it.each(["Continue", "/goal", "/goal pause", "/compact extra", "/goal resume extra"])(
  "permits initial native creation for a new conversation: %s",
  (text) => expect(nativeConversationMustBePreserved(codex, message(text))).toBe(false),
);

it("treats an attachment-bearing command as ordinary initial input", () => {
  expect(nativeConversationMustBePreserved(codex, message("/compact", [{}]))).toBe(false);
  expect(nativeConversationMustBePreserved(codex, message("/goal resume", [{}]))).toBe(false);
});

it("leaves other providers' continuation policy unchanged", () => {
  expect(
    nativeConversationMustBePreserved(
      { driver: "claudeAgent", nativeThreadRef: { nativeId: "existing" }, goal: { status: "active" } },
      message("/compact"),
    ),
  ).toBe(false);
});
