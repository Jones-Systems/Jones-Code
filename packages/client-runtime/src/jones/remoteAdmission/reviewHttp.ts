import { RemoteAdmissionReviewError, type RemoteAdmissionReviewQuery, type RemoteAdmissionReviewItemQuery, type RemoteAdmissionGrantQuery, type RemoteAdmissionReviewDecision, type RemoteAdmissionGrantUpdate, type RemoteAdmissionGrantRevoke, type RemoteAdmissionGrantRestore, type RemoteAdmissionRetryInput, type RemoteAdmissionRecheckInput, type RemoteAdmissionChangesQuery } from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { ConnectionCatalogEntry } from "../../connection/catalog.ts";
import { ConnectionResolver } from "../../connection/resolver.ts";
import { connectionRoutes, routeEntry } from "../../connection/routes.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";

type AuthenticatedRequest<A, E, R> = Parameters<typeof executeAuthenticatedEnvironmentHttpRequest<"remoteAdmissionReview", A, E, R>>[0];
type Request<A, E, R> = Pick<AuthenticatedRequest<A, E, R>, "method" | "url" | "request">;

/** Keep declared review conflicts intact without changing legacy HTTP error decoding.
 * Authentication/transport failures still pass through the shared renewal owner.
 * Route fallback prepares a route only; a mutation is never replayed here.
 */
const request = Effect.fn("clientRuntime.remoteAdmission.reviewHttp")(function* <A, E, R>(
  entry: ConnectionCatalogEntry,
  operation: Request<A, E, R>,
  signal?: AbortSignal,
) {
  const resolver = yield* ConnectionResolver;
  const prepared = yield* Effect.firstSuccessOf(
    connectionRoutes(entry).map((route) => resolver.prepare(routeEntry(entry, route))),
  );
  if (prepared.environmentId !== entry.target.environmentId) return yield* Effect.fail(new Error("Receiving environment changed."));
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
  const response = yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared, signer, remoteAuthorization, group: "remoteAdmissionReview", method: operation.method,
    url: operation.url, timeoutMs: 60_000, ...(signal ? { requestInit: { signal } } : {}),
    request: (input) => operation.request(input).pipe(
      Effect.map((value) => ({ kind: "value" as const, value })),
      Effect.catchIf(Schema.is(RemoteAdmissionReviewError), (error) => Effect.succeed({ kind: "declared-error" as const, error })),
    ),
  });
  if (response.kind === "declared-error") return yield* Effect.fail(response.error);
  return response.value;
});

export const reviewHttp = {
  capabilities: (entry: ConnectionCatalogEntry) => request(entry, { method: "GET", url: (urls) => urls.capabilities(), request: ({ client, headers }) => client.capabilities({ headers }) }),
  review: (entry: ConnectionCatalogEntry, query: RemoteAdmissionReviewQuery) => request(entry, { method: "GET", url: (urls) => urls.review({ query }), request: ({ client, headers }) => client.review({ headers, query }) }),
  item: (entry: ConnectionCatalogEntry, query: RemoteAdmissionReviewItemQuery) => request(entry, { method: "GET", url: (urls) => urls.item({ query }), request: ({ client, headers }) => client.item({ headers, query }) }),
  grants: (entry: ConnectionCatalogEntry, query: RemoteAdmissionGrantQuery) => request(entry, { method: "GET", url: (urls) => urls.grants({ query }), request: ({ client, headers }) => client.grants({ headers, query }) }),
  decision: (entry: ConnectionCatalogEntry, payload: RemoteAdmissionReviewDecision) => request(entry, { method: "POST", url: (urls) => urls.decision(), request: ({ client, headers }) => client.decision({ headers, payload }) }),
  updateGrant: (entry: ConnectionCatalogEntry, payload: RemoteAdmissionGrantUpdate) => request(entry, { method: "POST", url: (urls) => urls.updateGrant(), request: ({ client, headers }) => client.updateGrant({ headers, payload }) }),
  revokeGrant: (entry: ConnectionCatalogEntry, payload: RemoteAdmissionGrantRevoke) => request(entry, { method: "POST", url: (urls) => urls.revokeGrant(), request: ({ client, headers }) => client.revokeGrant({ headers, payload }) }),
  restoreGrant: (entry: ConnectionCatalogEntry, payload: RemoteAdmissionGrantRestore) => request(entry, { method: "POST", url: (urls) => urls.restoreGrant(), request: ({ client, headers }) => client.restoreGrant({ headers, payload }) }),
  retry: (entry: ConnectionCatalogEntry, payload: RemoteAdmissionRetryInput) => request(entry, { method: "POST", url: (urls) => urls.retry(), request: ({ client, headers }) => client.retry({ headers, payload }) }),
  recheck: (entry: ConnectionCatalogEntry, payload: RemoteAdmissionRecheckInput) => request(entry, { method: "POST", url: (urls) => urls.recheck(), request: ({ client, headers }) => client.recheck({ headers, payload }) }),
  changes: (entry: ConnectionCatalogEntry, query: RemoteAdmissionChangesQuery, signal?: AbortSignal) => request(entry, { method: "GET", url: (urls) => urls.changes({ query }), request: ({ client, headers }) => client.changes({ headers, query }) }, signal),
};
