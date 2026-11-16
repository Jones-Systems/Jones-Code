import { AuthAccessWriteScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import {
  failEnvironmentInternal,
  failEnvironmentScopeRequired,
  requireEnvironmentScope,
} from "../../auth/http.ts";
import * as RemoteAdmissionIntake from "./RemoteAdmissionIntake.ts";
import { isRemoteAdmissionOwner } from "./owner.ts";

const owner = Effect.gen(function* () {
  const principal = yield* requireEnvironmentScope(AuthAccessWriteScope);
  if (!isRemoteAdmissionOwner(principal))
    return yield* failEnvironmentScopeRequired(AuthAccessWriteScope);
  yield* HttpEffect.appendPreResponseHandler((_request, response) =>
    Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
  );
  return principal;
});
export const remoteAdmissionHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "remoteAdmission",
  Effect.fnUntraced(function* (handlers) {
    const intake = yield* RemoteAdmissionIntake.RemoteAdmissionIntake;
    const internal = (error: RemoteAdmissionIntake.RemoteAdmissionIntakeError) =>
      failEnvironmentInternal("internal_error", error);
    return handlers
      .handle("attention", () =>
        owner.pipe(
          Effect.andThen(intake.attention()),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("snapshot", () =>
        owner.pipe(
          Effect.andThen(intake.snapshot()),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("policy", ({ payload }) =>
        owner.pipe(
          Effect.andThen(intake.updatePolicy(payload)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("decide", ({ payload }) =>
        owner.pipe(
          Effect.flatMap((principal) => intake.decide(payload, principal)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      );
  }),
);

// V2 errors are deliberately separate from the historic tagged V1 error channel.
import type { RemoteAdmissionReviewForbidden } from "@t3tools/contracts/jones/remoteAdmissionReview";
import { EnvironmentAuthenticatedPrincipal } from "@t3tools/contracts";
import { RemoteAdmissionReviewService } from "./ReviewService.ts";
const reviewPrincipal = Effect.gen(function* () {
  yield* HttpEffect.appendPreResponseHandler((_request, response) =>
    Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
  );
  return yield* EnvironmentAuthenticatedPrincipal;
});
const reviewOwnerPrincipal = reviewPrincipal.pipe(
  Effect.filterOrFail(
    isRemoteAdmissionOwner,
    () =>
      ({
        code: "owner_required",
        message: "Receiving owner access is required.",
      }) satisfies RemoteAdmissionReviewForbidden,
  ),
);
export const remoteAdmissionReviewHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "remoteAdmissionReview",
  Effect.fnUntraced(function* (handlers) {
    const review = yield* RemoteAdmissionReviewService;
    const internal = (error: RemoteAdmissionIntake.RemoteAdmissionIntakeError) =>
      failEnvironmentInternal("internal_error", error);
    return handlers
      .handle("capabilities", () => reviewPrincipal.pipe(Effect.flatMap(review.capabilities)))
      .handle("review", ({ query }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.page(query, p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("item", ({ query }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.item(query.id, p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("decision", ({ payload }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.decide(payload, p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("grants", ({ query }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.grants(query, p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("updateGrant", ({ payload }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.mutateGrant(payload, "update", p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("revokeGrant", ({ payload }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.mutateGrant(payload, "revoke", p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("restoreGrant", ({ payload }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.restore(payload, p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("retry", ({ payload }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.retry(payload, p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("recheck", ({ payload }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.recheck(payload, p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      )
      .handle("changes", ({ query }) =>
        reviewOwnerPrincipal.pipe(
          Effect.flatMap((p) => review.changes(query, p)),
          Effect.catchTags({ RemoteAdmissionIntakeError: internal }),
        ),
      );
  }),
);
