import {
  EnvironmentHttpApi,
  EnvironmentAuthenticatedPrincipal,
  VoiceReviewError,
  VoiceReviewUnavailableError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ByteSize from "effect/ByteSize";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import {
  HttpIncomingMessage,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";
import { makeVoiceReviewBridge, VOICE_REVIEW_MAX_REQUEST_BYTES } from "./bridge.ts";
import { voiceReviewConfigFromEnv } from "./config.ts";

export const voiceReviewResponseHeadersLayer = HttpRouter.middleware(
  (httpEffect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const path = new URL(request.originalUrl, "http://environment.invalid").pathname;
      if (path !== "/api/voice-review/drafts" && !path?.startsWith("/api/voice-review/drafts/"))
        return yield* httpEffect;
      yield* HttpEffect.appendPreResponseHandler((_request, response) =>
        Effect.succeed(
          HttpServerResponse.setHeaders(response, {
            "cache-control": "no-store",
            "x-content-type-options": "nosniff",
          }),
        ),
      );
      return yield* httpEffect.pipe(
        Effect.provideService(
          HttpIncomingMessage.MaxBodySize,
          ByteSize.bytes(VOICE_REVIEW_MAX_REQUEST_BYTES),
        ),
      );
    }),
  { global: true },
);

const isReviewError = Schema.is(VoiceReviewError);
export const voiceReviewHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "voiceReview",
  (handlers) => {
    const bridge = makeVoiceReviewBridge(voiceReviewConfigFromEnv(process.env));
    const call = <A>(
      run: (principal: EnvironmentAuthenticatedPrincipal["Service"]) => Promise<A>,
    ) =>
      Effect.flatMap(EnvironmentAuthenticatedPrincipal, (principal) =>
        Effect.tryPromise({
          try: () => run(principal),
          catch: (error) => (isReviewError(error) ? error : new VoiceReviewUnavailableError({})),
        }),
      );
    return Effect.succeed(
      handlers
        .handle("list", ({ query }) =>
          call((principal) => bridge.list(principal, query.scope ?? "pending", query.limit ?? 50)),
        )
        .handle("get", ({ params }) => call((principal) => bridge.get(principal, params.id)))
        .handle("pause", ({ params, payload }) =>
          call((principal) => bridge.mutate(principal, params.id, "pause", payload)),
        )
        .handle("play", ({ params, payload }) =>
          call((principal) => bridge.mutate(principal, params.id, "play", payload)),
        )
        .handle("editBegin", ({ params, payload }) =>
          call((principal) => bridge.mutate(principal, params.id, "edit-begin", payload)),
        )
        .handle("editSave", ({ params, payload }) =>
          call((principal) => bridge.mutate(principal, params.id, "edit-save", payload)),
        )
        .handle("editCancel", ({ params, payload }) =>
          call((principal) => bridge.mutate(principal, params.id, "edit-cancel", payload)),
        )
        .handle("sendNow", ({ params, payload }) =>
          call((principal) => bridge.mutate(principal, params.id, "send-now", payload)),
        )
        .handle("delete", ({ params, payload }) =>
          call((principal) => bridge.mutate(principal, params.id, "delete", payload)),
        ),
    );
  },
);
