import * as Schema from "effect/Schema";

export const PreviewRecordingState = Schema.Union([
  Schema.Struct({
    state: Schema.Literals(["idle", "starting", "abandoning", "unknown", "unsupported"]),
  }),
  Schema.Struct({
    state: Schema.Literals(["active", "stopping"]),
    startedAt: Schema.optional(Schema.String),
  }),
]).annotate({
  description:
    "This server's recorder state. Absence means not reported, never idle. Reading this state lifts no fence.",
});
