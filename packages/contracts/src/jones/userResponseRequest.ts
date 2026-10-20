import * as Schema from "effect/Schema";
import { RuntimeRequestId, TrimmedNonEmptyString } from "../baseSchemas.ts";

const line = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(500),
  Schema.makeFilter((value) => value.trim().length > 0 && !/[\r\n\u2028\u2029]/u.test(value)),
);

export const UserResponseRequestInput = Schema.Struct({
  instruction: line,
  choices: Schema.optional(Schema.Array(TrimmedNonEmptyString).check(Schema.isMaxLength(4))),
  clientRequestId: TrimmedNonEmptyString,
});
export type UserResponseRequestInput = typeof UserResponseRequestInput.Type;

export const UserResponseRequestResult = Schema.Struct({
  requestId: RuntimeRequestId,
  status: Schema.Literals(["pending", "resolved", "expired", "cancelled"]),
  created: Schema.Boolean,
});
export type UserResponseRequestResult = typeof UserResponseRequestResult.Type;
