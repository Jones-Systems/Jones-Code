import * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import { RemoteEnvironmentAuthorization } from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import type { ManagedRelayDpopSigner } from "../relay/managedRelay.ts";
import { makeEnvironmentHttpApiUrlBuilder } from "../rpc/http.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../state/environmentHttpAuth.ts";

export const fetchWorkQueueMetadata = Effect.fn("clientRuntime.environment.workQueueMetadata")(
  function* (input: {
    readonly prepared: PreparedConnection;
    readonly signer: Option.Option<ManagedRelayDpopSigner["Service"]>;
    readonly remoteAuthorization?: Option.Option<RemoteEnvironmentAuthorization["Service"]>;
    readonly timeoutMs?: number;
  }) {
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      ...input,
      remoteAuthorization:
        input.remoteAuthorization ?? (yield* Effect.serviceOption(RemoteEnvironmentAuthorization)),
      group: "workQueueMetadata",
      method: "GET",
      timeoutMs: input.timeoutMs ?? 10_000,
      url: (base) => makeEnvironmentHttpApiUrlBuilder(base).workQueueMetadata.snapshot(),
      request: ({ client, headers }) => client.snapshot({ headers }),
    });
  },
);
