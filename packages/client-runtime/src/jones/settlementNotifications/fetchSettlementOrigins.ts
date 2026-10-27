import {
  SETTLEMENT_ORIGINS_CAPABILITY,
  type EnvironmentId,
  type SettlementOriginQuery,
} from "@t3tools/contracts";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";

import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import type { PreparedConnection } from "../../connection/model.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";
import type { SettlementArrival, SettlementOriginDetails } from "./model.ts";

class SettlementOriginsResponseMismatch extends Data.TaggedError(
  "SettlementOriginsResponseMismatch",
) {}
class SettlementOriginsUnavailable extends Data.TaggedError("SettlementOriginsUnavailable") {}

export const fetchSettlementOrigins = Effect.fn("clientRuntime.settlementNotifications.origins")(
  function* (input: {
    readonly prepared: PreparedConnection;
    readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
    readonly remoteAuthorization?: Option.Option<RemoteEnvironmentAuthorization["Service"]>;
    readonly items: readonly SettlementOriginQuery[];
  }) {
    const origins: SettlementOriginDetails[] = [];
    const remoteAuthorization =
      input.remoteAuthorization ?? (yield* Effect.serviceOption(RemoteEnvironmentAuthorization));
    for (let start = 0; start < input.items.length; start += 100) {
      const items = input.items.slice(start, start + 100);
      const response = yield* executeAuthenticatedEnvironmentHttpRequest({
        prepared: input.prepared,
        signer: input.signer,
        remoteAuthorization,
        group: "settlementOrigins",
        method: "POST",
        timeoutMs: 10_000,
        url: (urls) => urls.resolve(),
        request: ({ client, headers }) => client.resolve({ headers, payload: { items } }),
      });
      if (
        response.items.length !== items.length ||
        response.items.some((row, index) => {
          const requested = items[index];
          return (
            requested === undefined ||
            row.threadId !== requested.threadId ||
            row.settledAtMs !== requested.settledAtMs
          );
        })
      )
        return yield* new SettlementOriginsResponseMismatch();
      origins.push(
        ...response.items.map(({ origin, projectRemaining }) => ({
          origin,
          ...(projectRemaining === undefined ? {} : { projectRemaining }),
        })),
      );
    }
    return origins;
  },
);

// Resolve against the session at execution time, including each classification
// retry. A missing session is not evidence of an old server's absent capability.
export const resolveEnvironmentSettlementOrigins = Effect.fn(
  "clientRuntime.settlementNotifications.resolveEnvironment",
)(function* (environmentId: EnvironmentId, arrivals: readonly SettlementArrival[]) {
  const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
  const prepared = yield* SubscriptionRef.get(supervisor.prepared);
  const session = yield* SubscriptionRef.get(supervisor.session);
  if (
    Option.isNone(prepared) ||
    Option.isNone(session) ||
    prepared.value.environmentId !== environmentId ||
    arrivals.some((arrival) => arrival.environmentId !== environmentId)
  )
    return yield* new SettlementOriginsUnavailable();
  const config = yield* session.value.initialConfig;
  const currentSession = yield* SubscriptionRef.get(supervisor.session);
  const currentPrepared = yield* SubscriptionRef.get(supervisor.prepared);
  if (
    config.environment.environmentId !== environmentId ||
    Option.isNone(currentSession) ||
    currentSession.value !== session.value ||
    Option.isNone(currentPrepared) ||
    currentPrepared.value !== prepared.value
  )
    return yield* new SettlementOriginsUnavailable();
  const capability = config.environment.capabilities.settlementOrigins;
  if (capability === undefined) return null;
  if (capability.schemaVersion !== SETTLEMENT_ORIGINS_CAPABILITY.schemaVersion)
    return yield* new SettlementOriginsUnavailable();
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  return yield* fetchSettlementOrigins({
    prepared: prepared.value,
    signer,
    items: arrivals.map(({ threadId, settledAtMs }) => ({ threadId, settledAtMs })),
  });
});
