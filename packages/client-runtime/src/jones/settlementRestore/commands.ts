import {
  ORCHESTRATION_V2_WS_METHODS,
  type EnvironmentId,
  type SettlementRestoreCommand,
  type SettlementRestoreInspectQuery,
  type SettlementRestoreInspection,
} from "@t3tools/contracts";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";

import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import type { PreparedConnection } from "../../connection/model.ts";
import * as EnvironmentSupervisor from "../../connection/supervisor.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { getInitialServerConfig, request } from "../../rpc/client.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";
import { supportsSettlementRestore } from "./model.ts";

class SettlementRestoreUnsupportedError extends Data.TaggedError(
  "SettlementRestoreUnsupportedError",
) {}
class SettlementRestoreInspectUnavailable extends Data.TaggedError(
  "SettlementRestoreInspectUnavailable",
) {}
class SettlementRestoreInspectMismatch extends Data.TaggedError(
  "SettlementRestoreInspectMismatch",
) {}

/** Sends the restore only to a server that advertises it; an older server never sees the command. */
export const dispatchSettlementRestore = Effect.fn("JonesSettlementRestore.dispatch")(function* (
  input: Omit<SettlementRestoreCommand, "type">,
) {
  const config = yield* getInitialServerConfig();
  if (!supportsSettlementRestore(config.environment.capabilities))
    return yield* new SettlementRestoreUnsupportedError();
  return yield* request(ORCHESTRATION_V2_WS_METHODS.dispatchCommand, {
    ...input,
    type: "thread.settlement.restore",
  });
});

export const inspectSettlementRestores = Effect.fn("JonesSettlementRestore.inspectHttp")(
  function* (input: {
    readonly prepared: PreparedConnection;
    readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
    readonly remoteAuthorization?: Option.Option<RemoteEnvironmentAuthorization["Service"]>;
    readonly items: readonly SettlementRestoreInspectQuery[];
  }) {
    const response = yield* executeAuthenticatedEnvironmentHttpRequest({
      prepared: input.prepared,
      signer: input.signer,
      remoteAuthorization:
        input.remoteAuthorization ?? (yield* Effect.serviceOption(RemoteEnvironmentAuthorization)),
      group: "settlementRestore",
      method: "POST",
      timeoutMs: 10_000,
      url: (urls) => urls.inspect(),
      request: ({ client, headers }) =>
        client.inspect({ headers, payload: { items: input.items } }),
    });
    if (
      response.environmentId !== input.prepared.environmentId ||
      response.items.length !== input.items.length ||
      response.items.some((row, index) => {
        const requested = input.items[index];
        return (
          requested === undefined ||
          row.threadId !== requested.threadId ||
          row.requestedSettleCommandId !== requested.settleCommandId
        );
      })
    )
      return yield* new SettlementRestoreInspectMismatch();
    return response.items;
  },
);

/** Inspects one thread in the command's environment, using its current connection. */
export const inspectEnvironmentSettlementRestore = Effect.fn("JonesSettlementRestore.inspect")(
  function* (environmentId: EnvironmentId, query: SettlementRestoreInspectQuery) {
    const config = yield* getInitialServerConfig();
    if (!supportsSettlementRestore(config.environment.capabilities))
      return yield* new SettlementRestoreUnsupportedError();
    const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
    const prepared = yield* SubscriptionRef.get(supervisor.prepared);
    if (Option.isNone(prepared) || prepared.value.environmentId !== environmentId)
      return yield* new SettlementRestoreInspectUnavailable();
    const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
    const inspections: readonly SettlementRestoreInspection[] = yield* inspectSettlementRestores({
      prepared: prepared.value,
      signer,
      items: [query],
    });
    // The response was checked to answer exactly the one query.
    return inspections[0]!;
  },
);
