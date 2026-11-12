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
