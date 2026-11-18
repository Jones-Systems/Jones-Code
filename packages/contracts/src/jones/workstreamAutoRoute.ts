import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** Environment switch for routing queue-dispatched threads to a clearly matching Workstream. */
export const WorkstreamAutoRouteEnabled = Schema.Boolean.pipe(
  Schema.withDecodingDefault(Effect.succeed(true)),
);

/** Workstream IDs never offered as automatic destinations, beside the built-in parking names. */
export const WorkstreamAutoRouteExcludedIds = Schema.Array(Schema.String).pipe(
  Schema.withDecodingDefault(Effect.succeed([])),
);
