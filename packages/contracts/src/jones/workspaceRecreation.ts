import * as Schema from "effect/Schema";
import { MessageId, RuntimeRequestId } from "../baseSchemas.ts";

// Send only to environments advertising checkoutRecreation. Receipt binding prevents queued requests selecting a newer retirement.
export const CheckoutRecreationRequest = Schema.Struct({
  receiptId: Schema.NonEmptyString,
  mode: Schema.Literal("detached"),
});

const CheckoutRecoveryChoice = Schema.Literals(["conversation", "branch", "default"]);
export const CheckoutRecoveryAnswer = Schema.Struct({
  requestId: RuntimeRequestId,
  choice: CheckoutRecoveryChoice,
});
export const CheckoutRecoveryOffer = Schema.Struct({
  worktreePath: Schema.NonEmptyString,
  branch: Schema.NullOr(Schema.NonEmptyString),
  resumeBranch: Schema.optional(Schema.NonEmptyString),
  previousHead: Schema.optional(Schema.NonEmptyString),
  messageId: MessageId,
});
