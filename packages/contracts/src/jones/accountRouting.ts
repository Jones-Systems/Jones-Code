import * as Schema from "effect/Schema";

/** A fresh-launch request. A persisted model selection always names a concrete instance. */
export const AccountRouting = Schema.Struct({ mode: Schema.Literal("auto") });
export type AccountRouting = typeof AccountRouting.Type;
