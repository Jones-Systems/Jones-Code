import * as Effect from "effect/Effect";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import type { PreparedConnection } from "../../connection/model.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";

export const fetchEnvironmentHostStatus = Effect.fn("HostStatusHttp.fetchEnvironmentHostStatus")(
  function* (prepared: PreparedConnection) {
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      prepared,
      group: "hostStatus",
      method: "GET",
      url: (urls) => urls.snapshot(),
      timeoutMs: 8_000,
      signer: yield* Effect.serviceOption(ManagedRelayDpopSigner),
      remoteAuthorization: yield* Effect.serviceOption(RemoteEnvironmentAuthorization),
      request: ({ client, headers }) => client.snapshot({ headers }),
    });
  },
);
