import { THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY } from "@t3tools/contracts/jones/guardedThreadSettlement";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { getInitialServerConfig } from "../../rpc/client.ts";

export class MonitorGuardedSettleUnavailable extends Schema.TaggedError<MonitorGuardedSettleUnavailable>()(
  "MonitorGuardedSettleUnavailable",
  {},
) {
  override get message() {
    return "This server does not support safe monitor settlement.";
  }
}
export const requireMonitorGuardedSettle = Effect.fn("ThreadMonitor.requireGuardedSettle")(
  function* () {
    const config = yield* getInitialServerConfig();
    if (
      config.environment.capabilities.threadMonitorGuardedSettle !==
      THREAD_MONITOR_GUARDED_SETTLE_CAPABILITY
    ) {
      return yield* new MonitorGuardedSettleUnavailable({});
    }
  },
);
