import * as Schema from "effect/Schema";

// Send only to environments advertising checkoutRecreation. Receipt binding prevents queued requests selecting a newer retirement.
export const CheckoutRecreationRequest = Schema.Struct({
  receiptId: Schema.NonEmptyString,
  mode: Schema.Literal("detached"),
});
