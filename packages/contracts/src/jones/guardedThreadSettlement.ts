import * as Schema from "effect/Schema";

export const THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY =
  "jones.thread-monitor-guarded-settle/v1" as const;
export const ThreadMonitorGuardedSettleCapability = Schema.Literal(
  THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY,
);
export const GuardedThreadSettlement = Schema.Struct({
  policy: Schema.Literal("preserve_pending_work"),
  expectedUpdatedAt: Schema.String.check(
    Schema.makeFilter(
      (value) =>
        (/^\d{4}-\d{2}-\d{2}T/.test(value) && Number.isFinite(Date.parse(value))) ||
        "Expected an ISO timestamp",
    ),
  ),
});
export type GuardedThreadSettlement = typeof GuardedThreadSettlement.Type;
