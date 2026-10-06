import * as Schema from "effect/Schema";

import { NativeCreationRejectionCode } from "./nativeCreation.ts";
import { TrimmedNonEmptyString } from "./baseSchemas.ts";

/**
 * A command dispatch failure. `bootstrapThreadDisposition` tells a client whether the
 * thread a bootstrap turn would have created was removed or never created.
 */
export class OrchestrationDispatchCommandError extends Schema.TaggedError<OrchestrationDispatchCommandError>()(
  "OrchestrationDispatchCommandError",
  {
    message: TrimmedNonEmptyString,
    cause: Schema.optional(Schema.Defect()),
    creationRejectionCode: Schema.optionalKey(NativeCreationRejectionCode),
    bootstrapThreadDisposition: Schema.optional(Schema.Literals(["deleted", "not-created"])),
  },
) {}
