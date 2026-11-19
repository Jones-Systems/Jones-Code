import {
  RemoteAdmissionReviewError,
  type RemoteAdmissionReviewQuery,
  type RemoteAdmissionReviewItemQuery,
  type RemoteAdmissionGrantQuery,
  type RemoteAdmissionReviewDecision,
  type RemoteAdmissionGrantUpdate,
  type RemoteAdmissionGrantRevoke,
  type RemoteAdmissionGrantRestore,
  type RemoteAdmissionRetryInput,
  type RemoteAdmissionRecheckInput,
  type RemoteAdmissionChangesQuery,
} from "@t3tools/contracts/jones/remoteAdmissionReview";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { EnvironmentSupervisor } from "../../connection/supervisor.ts";
import { environmentMismatchError } from "../../connection/errors.ts";
import { RemoteEnvironmentAuthorization } from "../../authorization/service.ts";
import { ManagedRelayDpopSigner } from "../../relay/managedRelay.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "../../state/environmentHttpAuth.ts";
import { RemoteEnvironmentAuthFetchError } from "../../rpc/http.ts";

type AuthenticatedRequest<A, E, R> = Parameters<
  typeof executeAuthenticatedEnvironmentHttpRequest<"remoteAdmissionReview", A, E, R>
>[0];
type Request<A, E, R> = Pick<AuthenticatedRequest<A, E, R>, "method" | "url" | "request">;

/** Keep declared review conflicts intact without changing legacy HTTP error decoding.
 * Authentication/transport failures still pass through the shared renewal owner.
 * Use only the supervisor's prepared route. The supervisor owns route changes.
 */
const request = Effect.fn("clientRuntime.remoteAdmission.reviewHttp")(function* <A, E, R>(
  operation: Request<A, E, R>,
  signal?: AbortSignal,
) {
  const supervisor = yield* EnvironmentSupervisor;
  const active = yield* SubscriptionRef.get(supervisor.prepared);
  if (Option.isNone(active))
    return yield* new RemoteEnvironmentAuthFetchError({
      message: "The receiver is not connected.",
      cause: supervisor.target._tag,
    });
  const prepared = active.value;
  if (prepared.environmentId !== supervisor.target.environmentId)
    return yield* environmentMismatchError({
      expected: supervisor.target.environmentId,
      actual: prepared.environmentId,
    });
  const signer = yield* Effect.serviceOption(ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(RemoteEnvironmentAuthorization);
  const response = yield* executeAuthenticatedEnvironmentHttpRequest({
    prepared,
    signer,
    remoteAuthorization,
    group: "remoteAdmissionReview",
    method: operation.method,
    url: operation.url,
    timeoutMs: 60_000,
    ...(signal ? { requestInit: { signal } } : {}),
    request: (input) =>
      operation.request(input).pipe(
        Effect.map((value) => ({ kind: "value" as const, value })),
        Effect.catchIf(Schema.is(RemoteAdmissionReviewError), (error) =>
          Effect.succeed({ kind: "declared-error" as const, error }),
        ),
      ),
  });
  if (response.kind === "declared-error") return yield* Effect.fail(response.error);
  return response.value;
});

export const reviewHttp = {
  capabilities: () =>
    request({
      method: "GET",
      url: (urls) => urls.capabilities(),
      request: ({ client, headers }) => client.capabilities({ headers }),
    }),
  review: (query: RemoteAdmissionReviewQuery) =>
    request({
      method: "GET",
      url: (urls) => urls.review({ query }),
      request: ({ client, headers }) => client.review({ headers, query }),
    }),
  item: (query: RemoteAdmissionReviewItemQuery) =>
    request({
      method: "GET",
      url: (urls) => urls.item({ query }),
      request: ({ client, headers }) => client.item({ headers, query }),
    }),
  grants: (query: RemoteAdmissionGrantQuery) =>
    request({
      method: "GET",
      url: (urls) => urls.grants({ query }),
      request: ({ client, headers }) => client.grants({ headers, query }),
    }),
  decision: (payload: RemoteAdmissionReviewDecision) =>
    request({
      method: "POST",
      url: (urls) => urls.decision(),
      request: ({ client, headers }) => client.decision({ headers, payload }),
    }),
  updateGrant: (payload: RemoteAdmissionGrantUpdate) =>
    request({
      method: "POST",
      url: (urls) => urls.updateGrant(),
      request: ({ client, headers }) => client.updateGrant({ headers, payload }),
    }),
  revokeGrant: (payload: RemoteAdmissionGrantRevoke) =>
    request({
      method: "POST",
      url: (urls) => urls.revokeGrant(),
      request: ({ client, headers }) => client.revokeGrant({ headers, payload }),
    }),
  restoreGrant: (payload: RemoteAdmissionGrantRestore) =>
    request({
      method: "POST",
      url: (urls) => urls.restoreGrant(),
      request: ({ client, headers }) => client.restoreGrant({ headers, payload }),
    }),
  retry: (payload: RemoteAdmissionRetryInput) =>
    request({
      method: "POST",
      url: (urls) => urls.retry(),
      request: ({ client, headers }) => client.retry({ headers, payload }),
    }),
  recheck: (payload: RemoteAdmissionRecheckInput) =>
    request({
      method: "POST",
      url: (urls) => urls.recheck(),
      request: ({ client, headers }) => client.recheck({ headers, payload }),
    }),
  changes: (query: RemoteAdmissionChangesQuery, signal?: AbortSignal) =>
    request(
      {
        method: "GET",
        url: (urls) => urls.changes({ query }),
        request: ({ client, headers }) => client.changes({ headers, query }),
      },
      signal,
    ),
};
