import * as Schema from "effect/Schema";
import { closedWorkstreamsNative } from "./workstreamsNative.ts";

/** The endpoint selects its own home and caller; only the bounded page size is client input. */
export const WorkstreamsNativeReadExportQuery = closedWorkstreamsNative(
  Schema.Struct({
    page_size: Schema.optionalKey(
      Schema.FiniteFromString.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 100 })),
    ),
  }),
);
