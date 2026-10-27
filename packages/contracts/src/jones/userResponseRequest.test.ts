import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { OrchestrationV2Command } from "../orchestrationV2.ts";
import { UserResponseRequestInput } from "./userResponseRequest.ts";

const decode = Schema.decodeUnknownSync(UserResponseRequestInput);
const decodePublicCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
describe("owner response request input", () => {
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
  it.each(["", "   ", "first\nsecond", "first\rsecond", "x".repeat(501)])(
    "rejects invalid instruction %j",
    (instruction) => {
      expect(() => decode({ instruction, clientRequestId: "review" })).toThrow();
    },
  );
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
