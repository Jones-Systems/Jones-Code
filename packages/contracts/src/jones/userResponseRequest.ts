import * as Schema from "effect/Schema";
import { RuntimeRequestId, TrimmedNonEmptyString } from "../baseSchemas.ts";

const nonblank = (max: number, singleLine = false) =>
  Schema.String.check(
    Schema.isMinLength(1),
    Schema.isMaxCodePoints(max),
    Schema.makeFilter(
      (value) => value.trim().length > 0 && (!singleLine || !/[\r\n\u2028\u2029]/u.test(value)),
    ),
  );

export const UserResponseChoiceInput = Schema.Struct({
  label: nonblank(120, true),
  description: Schema.optional(nonblank(300, true)),
});
export const UserResponseQuestionInput = Schema.Struct({
  header: Schema.optional(nonblank(40, true)),
  prompt: nonblank(1500),
  choices: Schema.optional(
    Schema.Array(UserResponseChoiceInput).check(
      Schema.isMaxLength(6),
      Schema.makeFilter(
        (choices) => new Set(choices.map((choice) => choice.label.trim())).size === choices.length,
      ),
    ),
  ),
});
export const UserResponseRequestInput = Schema.Struct({
  instruction: Schema.optional(nonblank(1500)),
  choices: Schema.optional(Schema.Array(TrimmedNonEmptyString).check(Schema.isMaxLength(4))),
  questions: Schema.optional(
    Schema.Array(UserResponseQuestionInput).check(Schema.isMinLength(1), Schema.isMaxLength(5)),
  ),
  clientRequestId: TrimmedNonEmptyString,
}).check(
  Schema.makeFilter(
    (input) =>
      (input.instruction !== undefined) !== (input.questions !== undefined) &&
      (input.questions === undefined || input.choices === undefined),
  ),
);
export type UserResponseRequestInput = typeof UserResponseRequestInput.Type;

export const UserResponseRequestResult = Schema.Struct({
  requestId: RuntimeRequestId,
  status: Schema.Literals(["pending", "resolved", "expired", "cancelled"]),
  created: Schema.Boolean,
});
export type UserResponseRequestResult = typeof UserResponseRequestResult.Type;

export const supportsUserResponseAnswerNotes = (capability: string): boolean =>
  capability === "message";

export function formatUserResponseChoiceWithNote(choice: string, note: string): string {
  return note.trim().length > 0 ? `${choice}\nNote: ${note.trim()}` : choice;
}
