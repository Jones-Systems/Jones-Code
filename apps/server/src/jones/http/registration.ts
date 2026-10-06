import * as Layer from "effect/Layer";
import { conversationLibraryHttpApiLayer } from "../../conversations/http.ts";
import * as HostStatus from "../../hostStatus/HostStatus.ts";
import { hostStatusHttpApiLayer as hostStatusHandlers } from "../../hostStatus/http.ts";
import * as VoiceReview from "../../voiceReview/bridge.ts";
import { voiceReviewHttpApiLayer } from "../../voiceReview/http.ts";
import * as TokenAccountingService from "../../tokenAccounting/TokenAccountingService.ts";
import { makeRuntimeReader } from "../../tokenAccounting/RuntimeReader.ts";

export { voiceReviewResponseHeadersLayer } from "../../voiceReview/http.ts";

export const provideConversationAndVoiceReview = <A, E, R>(api: Layer.Layer<A, E, R>) =>
  api.pipe(
    Layer.provide(conversationLibraryHttpApiLayer),
    Layer.provide(
      voiceReviewHttpApiLayer.pipe(
        Layer.provide(VoiceReview.layer.pipe(Layer.provide(VoiceReview.dependenciesLayer))),
      ),
    ),
  );

export const hostStatusHttpApiLayer = hostStatusHandlers.pipe(Layer.provide(HostStatus.layer));

export const tokenAccountingLayer = Layer.suspend(() =>
  TokenAccountingService.layerWithReader(makeRuntimeReader(process.env)),
);
