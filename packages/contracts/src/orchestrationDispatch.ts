import * as Schema from "effect/Schema";

import { NativeCreationRejectionCode } from "./nativeCreation.ts";
import { TrimmedNonEmptyString } from "./baseSchemas.ts";

export class OrchestrationDispatchCommandError extends Schema.TaggedError<OrchestrationDispatchCommandError>()(
  "OrchestrationDispatchCommandError",
  {
    message: TrimmedNonEmptyString,
    cause: Schema.optional(Schema.Defect()),
    creationRejectionCode: Schema.optionalKey(NativeCreationRejectionCode),
    bootstrapThreadDisposition: Schema.optional(Schema.Literals(["deleted", "not-created"])),
  },
) {}
