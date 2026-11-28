import {
  executeConnectedEnvironmentHttpRequest,
  withOrchestrationProtocolHeader,
} from "@t3tools/client-runtime/state/environmentHttpAuth";
import type { PreparedThreadTurn } from "@t3tools/client-runtime/operations";
import { ORCHESTRATION_V2_WS_METHODS } from "@t3tools/contracts";
import { initialMessageCommandId } from "@t3tools/contracts/jones/deliveryEvidence";
import * as Effect from "effect/Effect";

import { acceptedAutoLaunchSequence } from "./outbox";

export const observeAutomaticLaunch = Effect.fn("Mobile.observeAutomaticLaunch")(function* (
  prepared: PreparedThreadTurn,
) {
  if (prepared.method !== ORCHESTRATION_V2_WS_METHODS.launchThread) return undefined;
  const { payload } = prepared;
  const query = { messageId: payload.initialMessage.messageId };
  for (const commandId of [payload.commandId, initialMessageCommandId(payload.commandId)]) {
    const params = { commandId, threadId: payload.threadId };
    const observation = yield* executeConnectedEnvironmentHttpRequest({
      group: "orchestration",
      method: "GET",
      url: (urls) => urls.commandObservation({ params, query }),
      timeoutMs: 5_000,
      request: ({ client, headers }) =>
        client.commandObservation({
          headers: withOrchestrationProtocolHeader(headers),
          params,
          query,
        }),
    });
    const sequence = acceptedAutoLaunchSequence(payload, observation);
    if (sequence !== undefined) return sequence;
  }
  return undefined;
});
