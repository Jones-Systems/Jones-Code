import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/**
 * Off by default: the decision snapshot runs an owner-installed collector over
 * account usage, so agents receive the capability only after an explicit
 * opt-in. Projects may override the environment value.
 */
export const AgentDecisionSnapshotAccessEnabled = Schema.Boolean.pipe(
  Schema.withDecodingDefault(Effect.succeed(false)),
);
