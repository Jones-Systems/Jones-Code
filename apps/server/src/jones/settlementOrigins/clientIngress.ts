import type { OrchestrationV2Command } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SettlementOrigins from "./SettlementOrigins.ts";

// Only the client socket uses this adapter; internal agent dispatch stays unmarked.
export const withClientSettlementOrigin = Effect.fn(
  "SettlementOriginsClientIngress.withClientSettlementOrigin",
)(function* <A, E, R>(
  command: OrchestrationV2Command,
  enqueue: Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E, R | SettlementOrigins.SettlementOrigins> {
  if (command.type === "thread.settle") {
    const origins = yield* SettlementOrigins.SettlementOrigins;
    yield* origins.recordClientSettle(command);
  }
  return yield* enqueue;
});
