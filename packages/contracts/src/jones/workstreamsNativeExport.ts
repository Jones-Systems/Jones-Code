import * as Schema from "effect/Schema";
import {
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeVersion,
  closedWorkstreamsNative,
} from "./workstreamsNative.ts";

/** The endpoint selects its own home and caller; only the bounded page size is client input. */
export const WorkstreamsNativeReadExportQuery = closedWorkstreamsNative(
  Schema.Struct({
    page_size: Schema.optionalKey(
      Schema.FiniteFromString.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 100 })),
    ),
  }),
);

// Fixed single-hop export negotiation is independent of paged reads and capability advertisement.
export const WORKSTREAMS_HOME_READ_EXPORT_CONTRACT = "workstreams-home-read-export/1.0.0" as const;
const contract = Schema.Literal(WORKSTREAMS_HOME_READ_EXPORT_CONTRACT);
const pageSize = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 1, maximum: 100 }),
);
export const WorkstreamsHomeReadExportRequest = closedWorkstreamsNative(
  Schema.Struct({
    contract,
    expected_home: WorkstreamsNativeHomeTarget,
    page_size: pageSize,
  }),
);
export type WorkstreamsHomeReadExportRequest = typeof WorkstreamsHomeReadExportRequest.Type;
export const WorkstreamsHomeReadExportResult = closedWorkstreamsNative(
  Schema.Struct({
    contract,
    home: WorkstreamsNativeHomeTarget,
    principal_id: Schema.NonEmptyString,
    authorization_revision: WorkstreamsNativeVersion,
    jsonl: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(16 * 1024 * 1024 + 1024)),
  }),
);
export type WorkstreamsHomeReadExportResult = typeof WorkstreamsHomeReadExportResult.Type;
