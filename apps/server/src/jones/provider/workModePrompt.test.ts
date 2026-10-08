import { describe, expect, it } from "vite-plus/test";
import { isWorkModeKeepWarm, workModeProviderPrompt } from "./workModePrompt.ts";
import { WORK_MODE_INSTRUCTIONS } from "@t3tools/shared/jones/workMode";

describe("Work mode provider prompt", () => {
  it("gives resumed conversations the exact echo and no-tools instruction", () => {
    const prompt = workModeProviderPrompt("@@@@@");
    expect(prompt).toContain(WORK_MODE_INSTRUCTIONS);
    expect(prompt).toContain("<user_request>\n@@@@@\n</user_request>");
  });
  it.each(["hello", " @@@@@", "@@@@@\n", "@@@@@ do work", "/help"])(
    "preserves ordinary prompt %j",
    (prompt) => {
      expect(workModeProviderPrompt(prompt)).toBe(prompt);
    },
  );
});

it("recognizes only server-authored keep-warm messages without user content", () => {
  const message = {
    id: "work-mode:thread:generation",
    text: "@@@@@",
    attachments: [],
    createdBy: "system",
    creationSource: "server",
  };
  expect(isWorkModeKeepWarm(message)).toBe(true);
  expect(isWorkModeKeepWarm({ ...message, createdBy: "user" })).toBe(false);
  expect(isWorkModeKeepWarm({ ...message, attachments: [{}] })).toBe(false);
  expect(isWorkModeKeepWarm({ ...message, context: { records: [{}] } })).toBe(false);
});
