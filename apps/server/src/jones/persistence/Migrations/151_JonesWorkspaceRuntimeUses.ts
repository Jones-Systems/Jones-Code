import { createRuntimeUseSchema } from "../../cleanup/RuntimeUseSchema.ts";
import { createRuntimeUseInstallation } from "../../cleanup/RuntimeUseInstallation.ts";
import { createRuntimeUseCreationSchema } from "../../cleanup/RuntimeUseCreationSchema.ts";
import * as Effect from "effect/Effect";

export default createRuntimeUseSchema.pipe(
  Effect.andThen(createRuntimeUseCreationSchema),
  Effect.andThen(createRuntimeUseInstallation),
);
