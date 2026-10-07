import * as ProjectionStoreV2 from "../../orchestration-v2/ProjectionStore.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as WorkQueueMetadataService from "../workQueueMetadata/WorkQueueMetadataService.ts";
import { workQueueMetadataHttpApiLayer } from "../workQueueMetadata/http.ts";
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
    Layer.provide(
      workQueueMetadataHttpApiLayer.pipe(Layer.provide(WorkQueueMetadataService.layer)),
    ),
    Layer.provide(conversationLibraryHttpApiLayer),
    Layer.provide(
      voiceReviewHttpApiLayer.pipe(
        Layer.provide(
          VoiceReview.layer.pipe(
            Layer.provide(
              VoiceReview.dependenciesLayerLive.pipe(
                Layer.provide(
                  Layer.merge(ProjectionStoreV2.layer, ServerEnvironment.identityLayer),
                ),
              ),
            ),
          ),
        ),
      ),
    ),
  );

export const hostStatusHttpApiLayer = hostStatusHandlers.pipe(Layer.provide(HostStatus.layer));

export const tokenAccountingLayer = Layer.suspend(() =>
  TokenAccountingService.layerWithReader(makeRuntimeReader(process.env)),
);
