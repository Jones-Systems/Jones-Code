import * as Schema from "effect/Schema";

export const DelegatedTaskSupervisionFields = {
  supervisionRequestedByUser: Schema.optional(
    Schema.Boolean.annotate({
      description:
        "true only when the user asked for a supervised child. Required for a runtimeMode narrower than yours or interactionMode plan from default. A role or read-only scope is not a reason.",
    }),
  ),
};
