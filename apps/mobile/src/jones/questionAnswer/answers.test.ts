import { describe, expect, it } from "vite-plus/test";
import { formatUserResponseChoiceWithNote } from "@t3tools/contracts/jones/userResponseRequest";
import {
  buildPendingUserInputAnswers,
  isPendingUserInputOptionSelected,
  setPendingUserInputCustomAnswer,
  togglePendingUserInputOptionSelection,
} from "./answers";

const question = {
  id: "target",
  header: "Target",
  question: "Which machine?",
  options: [{ label: "Remote", description: "Use the server", value: " exact remote value " }],
  multiSelect: false,
};
describe("mobile answer notes", () => {
  it("keeps an exact option value selected while editing its note", () => {
    const selected = togglePendingUserInputOptionSelection(
      question,
      undefined,
      " exact remote value ",
      { notes: true },
    );
    const noted = setPendingUserInputCustomAnswer(
      question,
      selected,
      " Use the server workspace ",
      { notes: true },
    );
    expect(
      isPendingUserInputOptionSelected(question, noted, " exact remote value ", { notes: true }),
    ).toBe(true);
    expect(buildPendingUserInputAnswers([question], { target: noted }, { notes: true })).toEqual({
      target: formatUserResponseChoiceWithNote(" exact remote value ", "Use the server workspace"),
    });
  });
  it("retains upstream custom-answer and blocked-upload behavior", () => {
    const selected = togglePendingUserInputOptionSelection(
      question,
      undefined,
      " exact remote value ",
    );
    const custom = setPendingUserInputCustomAnswer(question, selected, "A different target");
    expect(isPendingUserInputOptionSelected(question, custom, " exact remote value ")).toBe(false);
    expect(buildPendingUserInputAnswers([question], { target: custom })).toEqual({
      target: "A different target",
    });
    expect(
      buildPendingUserInputAnswers(
        [question],
        { target: { ...custom, attachmentsBlocked: true } },
        { notes: true },
      ),
    ).toBeNull();
  });
});
