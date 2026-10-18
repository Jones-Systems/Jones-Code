import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const WorkModeEnabled = Schema.Boolean.pipe(
  Schema.withDecodingDefault(Effect.succeed(false)),
);
