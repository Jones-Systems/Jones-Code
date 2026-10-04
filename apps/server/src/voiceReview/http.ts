import {
  EnvironmentHttpApi,
  EnvironmentAuthenticatedPrincipal,
  VoiceReviewError,
  VoiceReviewUnavailableError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as ByteSize from "effect/ByteSize";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import {
  HttpIncomingMessage,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";
import {
  makeVoiceReviewBridge,
  VOICE_REVIEW_MAX_REQUEST_BYTES,
  type VoiceReviewNativeReadPort,
} from "./bridge.ts";
import { voiceReviewConfigFromEnv, voiceReviewNativeBindingFromEnv } from "./config.ts";
import {
  makeVoiceReviewCompositionFactory,
  type VoiceReviewCompositionFactory,
} from "./composition.ts";

export const voiceReviewResponseHeadersLayer = HttpRouter.middleware(
  (httpEffect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const path = new URL(request.originalUrl, "http://environment.invalid").pathname;
      if (!path.startsWith("/api/voice-review/")) return yield* httpEffect;
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
const makeVoiceReviewHttpApiLayer = (
  native?: VoiceReviewNativeReadPort,
  compositionFactory: VoiceReviewCompositionFactory = () => Effect.succeed(undefined),
) =>
  HttpApiBuilder.group(EnvironmentHttpApi, "voiceReview", (handlers) => {
    const reviewConfig = voiceReviewConfigFromEnv(process.env);
    const binding = voiceReviewNativeBindingFromEnv(process.env);
    const call = <A>(
      run: (principal: EnvironmentAuthenticatedPrincipal["Service"]) => Promise<A>,
    ) =>
      Effect.flatMap(EnvironmentAuthenticatedPrincipal, (principal) =>
        Effect.tryPromise({
          try: () => run(principal),
          catch: (error) => (isReviewError(error) ? error : new VoiceReviewUnavailableError({})),
        }),
      );
    return Effect.map(Clock.Clock, (clock) => {
      const now = () => clock.currentTimeMillisUnsafe();
      const bridge = makeVoiceReviewBridge(reviewConfig, globalThis.fetch, native, now);
      return handlers
        .handle("recent", ({ query }) =>
          call((principal) => bridge.recent(principal, query.limit ?? 50)),
        )
        .handle("registrySnapshot", ({ query }) =>
          Effect.flatMap(EnvironmentAuthenticatedPrincipal, (principal) =>
            (native === undefined
              ? compositionFactory({ binding, reviewConfig, principal })
              : Effect.succeed(native)
            ).pipe(
              Effect.flatMap((qualifiedNative) =>
                Effect.tryPromise({
                  try: () =>
                    makeVoiceReviewBridge(
                      reviewConfig,
                      globalThis.fetch,
                      qualifiedNative,
                      now,
                    ).registrySnapshot(principal, query.cursor, query.limit ?? 50),
                  catch: (error) =>
                    isReviewError(error) ? error : new VoiceReviewUnavailableError({}),
                }),
              ),
            ),
          ),
        )
        .handle("registryWorkstreams", () =>
          call((principal) => bridge.registryWorkstreams(principal)),
        )
        .handle("registryEvents", ({ query }) =>
          call((principal) =>
            bridge.registryEvents(principal, query.after ?? 0, query.limit ?? 50),
          ),
        )
        .handle("correctAssociation", ({ payload }) =>
          call((principal) => bridge.correctAssociation(principal, payload)),
        )
        .handle("correctLabel", ({ payload }) =>
          call((principal) => bridge.correctLabel(principal, payload)),
        )
        .handle("diagnostics", ({ params }) =>
          call((principal) => bridge.diagnostics(principal, params.id)),
        )
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
        );
    });
  });

export const voiceReviewHttpApiLayer = makeVoiceReviewHttpApiLayer();

export const voiceReviewHttpApiLayerLive = Layer.unwrap(
  makeVoiceReviewCompositionFactory().pipe(
    Effect.map((factory) => makeVoiceReviewHttpApiLayer(undefined, factory)),
  ),
);
