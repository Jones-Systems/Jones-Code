import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { OrchestrationV2Command } from "../orchestrationV2.ts";
import { UserResponseRequestInput } from "./userResponseRequest.ts";

const decode = Schema.decodeUnknownSync(UserResponseRequestInput);
const decodePublicCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
describe("owner response request input", () => {
  it("accepts an explicit action completion shortcut without changing review questions", () => {
    expect(
      decode({ instruction: "Open Browser.", defaultDone: true, clientRequestId: "action" })
        .defaultDone,
    ).toBe(true);
    expect(() =>
      decode({
        questions: [{ prompt: "Which approach?" }],
        defaultDone: true,
        clientRequestId: "decision",
      }),
    ).toThrow();
    expect(() =>
      decode({ instruction: "Open Browser.", defaultDone: "yes", clientRequestId: "invalid" }),
    ).toThrow();
  });
  it("accepts one instruction with open text and optional genuine choices", () => {
    expect(
      decode({
        instruction: "Review the plan and reply with changes.",
        clientRequestId: "review-1",
      }).choices,
    ).toBeUndefined();
    expect(
      decode({
        instruction: "Choose a destination.",
        clientRequestId: "choice",
        choices: ["Local", "Remote"],
      }).choices,
    ).toEqual(["Local", "Remote"]);
  });
  it.each(["", "   ", "x".repeat(1501)])("rejects invalid instruction %j", (instruction) => {
    expect(() => decode({ instruction, clientRequestId: "review" })).toThrow();
  });
  // CR-01 widens legacy input; newline/501 rejection is superseded by multiline/1500 boundaries.
  it("uses per-question code-point limits for both forms without an aggregate cap", () => {
    const prompt = "a".repeat(1499) + "😀";
    expect(decode({ instruction: prompt, clientRequestId: "emoji" }).instruction).toBe(prompt);
    expect(
      decode({ instruction: "first\n\n**second**", clientRequestId: "lines" }).instruction,
    ).toContain("\n\n");
    expect(
      decode({ instruction: "a".repeat(1500), clientRequestId: "limit" }).instruction,
    ).toHaveLength(1500);
    expect(
      decode({ clientRequestId: "batch", questions: Array.from({ length: 5 }, () => ({ prompt })) })
        .questions,
    ).toHaveLength(5);
    for (const input of [
      { instruction: prompt + "a" },
      { questions: [{ prompt: prompt + "a" }] },
      { questions: [] },
      { questions: [{ prompt: " " }] },
      { questions: [{ prompt: "Question", header: "two\nlines" }] },
      { questions: [{ prompt: "Question", choices: [{ label: "a\nb" }] }] },
      {
        questions: [
          { prompt: "Question", choices: [{ label: "A", description: "x".repeat(301) }] },
        ],
      },
      { questions: Array.from({ length: 6 }, () => ({ prompt: "Question" })) },
      {
        questions: [
          { prompt: "Question", choices: Array.from({ length: 7 }, (_, i) => ({ label: `${i}` })) },
        ],
      },
      { questions: [{ prompt: "Question", choices: [{ label: "same" }, { label: " same " }] }] },
      { instruction: "Question", questions: [{ prompt: "Question" }] },
      { questions: [{ prompt: "Question" }], choices: [] },
      {},
    ])
      expect(() => decode({ clientRequestId: "invalid", ...input })).toThrow();
  });
  it("keeps the instruction limit separate from the nonblank retry identity", () => {
    expect(
      decode({ instruction: "Review the plan.", clientRequestId: "r".repeat(501) }).clientRequestId,
    ).toHaveLength(501);
  });
  it("rejects missing retry identity and invalid choices", () => {
    for (const extra of [
      { clientRequestId: " " },
      { choices: [""] },
      { choices: ["a", "b", "c", "d", "e"] },
    ]) {
      expect(() =>
        decode({ instruction: "Reply with changes.", clientRequestId: "review", ...extra }),
      ).toThrow();
    }
  });
  it("does not expose the internal record command to public dispatch", () => {
    expect(() =>
      decodePublicCommand({
        type: "user_input_request.record",
        commandId: "forged",
      }),
    ).toThrow();
  });
});
