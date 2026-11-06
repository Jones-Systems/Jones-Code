import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  AuthEnvironmentMaintainScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { requireEnvironmentScope } from "../../auth/http.ts";
import { JonesUpdates } from "./service.ts";
import * as SourceRelation from "./sourceRelation.ts";

const noStore = HttpEffect.appendPreResponseHandler((_request, response) =>
  Effect.succeed(HttpServerResponse.setHeader(response, "cache-control", "no-store")),
);

export const jonesUpdatesHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "jonesUpdates",
  (handlers) =>
    Effect.gen(function* () {
      const updates = yield* JonesUpdates;
      const sourceRelation = yield* SourceRelation.JonesSourceRelation;
      return handlers
        .handle("operationStart", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.operationStart(payload);
          }),
        )
        .handle("operationStop", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.operationStop(payload);
          }),
        )
        .handle("operationFence", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.operationFence(payload);
          }),
        )
        .handle("operationRead", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationReadScope);
            yield* noStore;
            return yield* updates.operationRead(payload);
          }),
        )
        .handle("recoveryPreflight", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.recoveryPreflight(payload);
          }),
        )
        .handle("reconcile", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.reconcile(payload);
          }),
        )
        .handle("recordRetired", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthEnvironmentMaintainScope);
            yield* noStore;
            return yield* updates.recordRetired(payload);
          }),
        )
        .handle("prepareNative", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.prepareNative(payload);
          }),
        )
        .handle("prepareQuit", () =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.prepareQuit;
          }),
        )
        .handle("state", ({ query }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationReadScope);
            yield* noStore;
            return yield* updates.state(query.after);
          }),
        )
        .handle("sourceRelation", ({ query }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationReadScope);
            yield* noStore;
            return yield* sourceRelation.relation(query);
          }),
        )
        .handle("check", () =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.check;
          }),
        )
        .handle("download", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.download(payload);
          }),
        )
        .handle("downloadExact", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.downloadExact(payload);
          }),
        )
        .handle("install", ({ payload }) =>
          Effect.gen(function* () {
            yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
            yield* noStore;
            return yield* updates.install(payload);
          }),
        );
    }),
);
