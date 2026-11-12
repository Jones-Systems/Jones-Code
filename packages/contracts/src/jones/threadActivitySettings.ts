import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const ThreadActivityWindowUnit = Schema.Literals(["minutes", "hours", "days", "weeks"]);
export const ThreadActivityWindowAmount = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: 10_000 }),
);

export const threadActivitySettingsFields = {
  threadActivityWindowEnabled: Schema.Boolean.pipe(
    Schema.withDecodingDefault(Effect.succeed(false)),
  ),
  threadActivityWindowAmount: ThreadActivityWindowAmount.pipe(
    Schema.withDecodingDefault(Effect.succeed(3)),
  ),
  threadActivityWindowUnit: ThreadActivityWindowUnit.pipe(
    Schema.withDecodingDefault(Effect.succeed("hours" as const)),
  ),
};

export const threadActivitySettingsPatchFields = {
  threadActivityWindowEnabled: Schema.optionalKey(Schema.Boolean),
  threadActivityWindowAmount: Schema.optionalKey(ThreadActivityWindowAmount),
  threadActivityWindowUnit: Schema.optionalKey(ThreadActivityWindowUnit),
};
