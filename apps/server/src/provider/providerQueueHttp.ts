import {
  AuthAccessWriteScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  type ProviderInstanceId,
  type ProviderQueueRefreshResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { annotateEnvironmentRequest, requireEnvironmentScope } from "../auth/http.ts";
import { ProviderRegistry } from "./Services/ProviderRegistry.ts";
import { makeProviderQueue, makeProviderQueueStorage } from "./providerQueue.ts";

const unavailable = (instanceId: ProviderInstanceId): ProviderQueueRefreshResult => ({
  instanceId,
  status: "unavailable",
  nextRefreshAt: null,
  quota: null,
});
export const makeProviderQueueHttpApiLayer = <R>(
  queueEffect: Effect.Effect<Effect.Success<ReturnType<typeof makeProviderQueue>>, never, R>,
) =>
  HttpApiBuilder.group(
    EnvironmentHttpApi,
    "providerQueue",
    Effect.fnUntraced(function* (handlers) {
      const queue = yield* queueEffect;
      return handlers
        .handle(
          "inventory",
          Effect.fn("providerQueue.inventory")(function* (args) {
            yield* annotateEnvironmentRequest(args.endpoint.name);
            yield* requireEnvironmentScope(AuthOrchestrationReadScope);
            return yield* queue.inventory;
          }),
        )
        .handle(
          "usage",
          Effect.fn("providerQueue.usage")(function* (args) {
            yield* annotateEnvironmentRequest(args.endpoint.name);
            yield* requireEnvironmentScope(AuthOrchestrationReadScope);
            return yield* queue
              .usage(args.params.instanceId)
              .pipe(
                Effect.catchTag("ProviderQueueStorageError", () =>
                  Effect.succeed(unavailable(args.params.instanceId)),
                ),
              );
          }),
        )
        .handle(
          "refresh",
          Effect.fn("providerQueue.refresh")(function* (args) {
            yield* annotateEnvironmentRequest(args.endpoint.name);
            yield* requireEnvironmentScope(AuthAccessWriteScope);
            return yield* queue
              .refresh(args.params.instanceId)
              .pipe(
                Effect.catchTag("ProviderQueueStorageError", () =>
                  Effect.succeed(unavailable(args.params.instanceId)),
                ),
              );
          }),
        );
    }),
  );

export const providerQueueHttpApiLayer = makeProviderQueueHttpApiLayer(
  Effect.gen(function* () {
    const registry = yield* ProviderRegistry;
    const storage = yield* makeProviderQueueStorage;
    return yield* makeProviderQueue(registry, storage);
  }),
);
