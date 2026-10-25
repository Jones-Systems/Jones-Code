import type { RemoteAdmissionDecision, RemoteAdmissionPolicyUpdate } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Data from "effect/Data";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import { connectionRoutes, routeEntry } from "../../connection/routes.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";

export type RemoteAdmissionRequest =
  | { readonly action: "snapshot" | "attention" }
  | { readonly action: "policy"; readonly input: RemoteAdmissionPolicyUpdate }
  | { readonly action: "decide"; readonly input: RemoteAdmissionDecision };

export type RemoteAdmissionFailureKind = "unauthorized" | "unsupported" | "transient" | "other";

const failureMessages: Record<RemoteAdmissionFailureKind, string> = {
  unauthorized: "Owner access is unavailable for this session.",
  unsupported: "Remote request review is unsupported by this receiving environment.",
  transient: "Remote request review could not be reached.",
  other: "Remote request review could not be loaded.",
};

export class RemoteAdmissionError extends Data.TaggedError("RemoteAdmissionError")<{
  readonly kind: RemoteAdmissionFailureKind;
  readonly message: string;
}> {
  constructor(kind: RemoteAdmissionFailureKind) {
    super({ kind, message: failureMessages[kind] });
  }
}

export function normalizeRemoteAdmissionError(error: unknown): RemoteAdmissionError {
  if (error instanceof RemoteAdmissionError) return new RemoteAdmissionError(error.kind);
  if (typeof error !== "object" || error === null) return new RemoteAdmissionError("other");
  const tag = "_tag" in error ? error._tag : undefined;
  if (
    tag === "EnvironmentAuthInvalidError" ||
    tag === "EnvironmentScopeRequiredError" ||
    tag === "EnvironmentOperationForbiddenError"
  )
    return new RemoteAdmissionError("unauthorized");
  if (tag === "EnvironmentResourceNotFoundError") return new RemoteAdmissionError("unsupported");
  if (tag === "RemoteEnvironmentAuthUndeclaredStatusError" && "status" in error) {
    if (error.status === 401 || error.status === 403) return new RemoteAdmissionError("unauthorized");
    if (error.status === 404 || error.status === 405) return new RemoteAdmissionError("unsupported");
    if (typeof error.status === "number" && (error.status === 429 || error.status >= 500))
      return new RemoteAdmissionError("transient");
  }
  if (tag === "RemoteEnvironmentAuthFetchError" || tag === "RemoteEnvironmentAuthTimeoutError")
    return new RemoteAdmissionError("transient");
  return new RemoteAdmissionError("other");
}

const request = Effect.fn("clientRuntime.remoteAdmission.request")(function* (
  entry: ConnectionCatalogEntry,
  input: RemoteAdmissionRequest,
) {
  const resolver = yield* ConnectionResolver;
  const { prepared } = yield* Effect.firstSuccessOf(
    connectionRoutes(entry).map((route) => resolver.prepareForUpdate(routeEntry(entry, route))),
  );
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared,
    signer,
    remoteAuthorization,
    group: "remoteAdmission",
    method:
      input.action === "snapshot" || input.action === "attention"
        ? "GET"
        : input.action === "policy"
          ? "PUT"
          : "POST",
    url: (urls) => urls[input.action](),
    timeoutMs: 45_000,
    request: ({ client, headers }) => {
      const runEffect = Effect.fn("RemoteAdmission.request")(function* () {
        switch (input.action) {
          case "attention":
            return yield* client.attention({ headers });
          case "snapshot":
            return yield* client.snapshot({ headers });
          case "policy":
            return yield* client.policy({ headers, payload: input.input });
          case "decide":
            return yield* client.decide({ headers, payload: input.input });
        }
      });
      return runEffect();
    },
  });
});

export const requestRemoteAdmission = (entry: ConnectionCatalogEntry, input: RemoteAdmissionRequest) =>
  request(entry, input).pipe(Effect.mapError(normalizeRemoteAdmissionError));
