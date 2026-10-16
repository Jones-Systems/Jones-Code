import type { PreviewRenderHostSelection, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type { PreparedConnection } from "../../connection/model.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";

const options = (prepared: PreparedConnection) =>
  Effect.gen(function* () {
    return {
      prepared,
      group: "previewCompanion" as const,
      timeoutMs: 8_000,
      signer: yield* Effect.serviceOption(ManagedRelayDpopSigner),
      remoteAuthorization: yield* Effect.serviceOption(RemoteEnvironmentAuthorization),
    };
  });

export const fetchCompanionHosts = (prepared: PreparedConnection) =>
  Effect.gen(function* () {
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      ...(yield* options(prepared)),
      method: "GET",
      url: (urls) => urls.hosts(),
      request: ({ client, headers }) => client.hosts({ headers }),
    });
  });

export const fetchCompanionThread = (prepared: PreparedConnection, threadId: ThreadId) =>
  Effect.gen(function* () {
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      ...(yield* options(prepared)),
      method: "GET",
      url: (urls) => urls.thread({ params: { threadId } }),
      request: ({ client, headers }) => client.thread({ headers, params: { threadId } }),
    });
  });

export const setCompanionDefault = (
  prepared: PreparedConnection,
  selection: PreviewRenderHostSelection,
) =>
  Effect.gen(function* () {
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      ...(yield* options(prepared)),
      method: "PUT",
      url: (urls) => urls.setDefault(),
      request: ({ client, headers }) => client.setDefault({ headers, payload: { selection } }),
    });
  });

export const setCompanionThread = (
  prepared: PreparedConnection,
  threadId: ThreadId,
  selection: PreviewRenderHostSelection | null,
) =>
  Effect.gen(function* () {
    return yield* executeAuthenticatedEnvironmentHttpRequest({
      ...(yield* options(prepared)),
      method: "PUT",
      url: (urls) => urls.setThread({ params: { threadId } }),
      request: ({ client, headers }) =>
        client.setThread({ headers, params: { threadId }, payload: { selection } }),
    });
  });

export function isCompanionEndpointUnsupported(cause: unknown): boolean {
  return (
    typeof cause === "object" &&
    cause !== null &&
    "_tag" in cause &&
    cause._tag === "RemoteEnvironmentAuthUndeclaredStatusError" &&
    "status" in cause &&
    cause.status === 404
  );
}
