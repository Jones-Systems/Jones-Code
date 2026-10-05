import * as Schema from "effect/Schema";
import { NonNegativeInt, PositiveInt, IsoDateTime } from "./baseSchemas.ts";

export const HostStatusId = Schema.Literals(["vps", "test", "mini", "home"]);
export type HostStatusId = typeof HostStatusId.Type;

export const HostStatus = Schema.Union([
  Schema.Struct({
    id: HostStatusId,
    status: Schema.Literal("available"),
    cpuUsagePercent: Schema.Number.check(
      Schema.isFinite(),
      Schema.isGreaterThanOrEqualTo(0),
      Schema.isLessThanOrEqualTo(100),
    ),
    logicalCpuCount: PositiveInt,
    // Occupied is total minus free, including reclaimable memory.
    occupiedMemoryBytes: NonNegativeInt,
    availableMemoryBytes: Schema.optional(NonNegativeInt),
    totalMemoryBytes: PositiveInt,
    sampledAt: IsoDateTime,
  }),
  Schema.Struct({
    id: HostStatusId,
    status: Schema.Literal("unavailable"),
    reason: Schema.Literals([
      "not_configured",
      "upstream_unavailable",
      "invalid_response",
      "stale",
    ]),
  }),
]);
export type HostStatus = typeof HostStatus.Type;

export const HostStatusSnapshot = Schema.Struct({ hosts: Schema.Array(HostStatus) });
export type HostStatusSnapshot = typeof HostStatusSnapshot.Type;
