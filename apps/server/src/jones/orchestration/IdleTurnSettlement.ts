import type { ProviderTurnId } from "@t3tools/contracts";
import type { ProviderAdapterV2SessionRuntime } from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

/**
 * A failed interrupt leaves a still-running run with nothing left to settle it. When the live
 * runtime positively reports that it is not executing the turn, the run itself may settle.
 * Background work stays untouched: the runtime's own state is still unconfirmed. A missing
 * session or a runtime without the probe is unknown, not idle, and settles nothing.
 */
export const settleRunOfIdleTurn = Effect.fn("IdleTurnSettlement.settleRunOfIdleTurn")(function* <
  E1,
  E2,
  R,
>(input: {
  readonly session: Effect.Effect<
    Option.Option<Pick<ProviderAdapterV2SessionRuntime, "isTurnActive">>,
    E1
  >;
  readonly providerTurnId: ProviderTurnId;
  readonly settleRun: Effect.Effect<unknown, E2, R>;
}) {
  const session = yield* input.session;
  if (Option.isNone(session) || session.value.isTurnActive === undefined) return false;
  if (yield* session.value.isTurnActive(input.providerTurnId)) return false;
  yield* input.settleRun;
  return true;
});
