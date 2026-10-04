import { EnvironmentHttpApi, EnvironmentHttpBadRequestError } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import { annotateEnvironmentRequest, requireEnvironmentScope } from "../../auth/http.ts";
import { NativeStoreAuthority } from "../../environment/NativeStoreAuthority.ts";
import { EventSinkV2 } from "../../orchestration-v2/EventSink.ts";
import { ThreadManagementService } from "../../orchestration-v2/ThreadManagementService.ts";
import { ProjectionStoreV2 } from "../../orchestration-v2/ProjectionStore.ts";
import { NativeEnrollmentsLive, NativeProviderEnrollmentLive } from "../enrollment/service.ts";
import {
  NativeProviderAttempts,
  NativeProviderAttemptsLive,
} from "../nativeProvider/attemptRepository.ts";
import {
  NativeProviderBuild,
  NativeProviderEnrollment,
  NATIVE_PROVIDER_SCOPES,
} from "../nativeProvider/enrollment.ts";
import {
  createNativeProviderHandlers,
  type NativeProviderOperation,
} from "../nativeProvider/http.ts";
import {
  makeWorkstreamsNativeProvider,
  NativeProviderBuildLive,
  type WorkstreamsNativeProvider,
} from "../nativeProvider/service.ts";

export class NativeWorkstreamsRuntime extends Context.Service<
  NativeWorkstreamsRuntime,
  {
    readonly provider: WorkstreamsNativeProvider;
    readonly enrollments: NativeProviderEnrollment["Service"];
  }
>()("t3/workstreams/runtimeIntegration/native/NativeWorkstreamsRuntime") {}

export const makeNativeWorkstreamsRuntime = Effect.gen(function* () {
  const authority = yield* NativeStoreAuthority;
  const query = yield* ProjectionStoreV2;
  const orchestrator = yield* ThreadManagementService;
  const eventSink = yield* EventSinkV2;
  const attempts = yield* NativeProviderAttempts;
  const enrollments = yield* NativeProviderEnrollment;
  const build = yield* NativeProviderBuild;
  return NativeWorkstreamsRuntime.of({
    enrollments,
    provider: makeWorkstreamsNativeProvider({
      authority,
      threadExists: (threadId) =>
        query.getThreadShell(threadId).pipe(Effect.map((thread) => thread !== null)),
      orchestrator: {
        dispatchNativeWorkstreamSettlement: (input) =>
          orchestrator
            .dispatchNativeWorkstreamSettlement(input)
            .pipe(
              Effect.provideService(NativeStoreAuthority, authority),
              Effect.provideService(NativeProviderEnrollment, enrollments),
              Effect.provideService(NativeProviderAttempts, attempts),
              Effect.provideService(NativeProviderBuild, build),
            ),
        observeNativeWorkstreamSettlementBinding: (input) =>
          orchestrator
            .observeNativeWorkstreamSettlementBinding(input)
            .pipe(
              Effect.provideService(NativeStoreAuthority, authority),
              Effect.provideService(NativeProviderEnrollment, enrollments),
              Effect.provideService(NativeProviderAttempts, attempts),
              Effect.provideService(NativeProviderBuild, build),
            ),
      },
      eventSink,
      attempts,
      build: build.readCurrent,
    }),
  });
});

export const nativeWorkstreamsRuntimeLayer = Layer.effect(
  NativeWorkstreamsRuntime,
  makeNativeWorkstreamsRuntime,
).pipe(
  Layer.provide(NativeProviderAttemptsLive),
  Layer.provide(NativeProviderEnrollmentLive.pipe(Layer.provide(NativeEnrollmentsLive))),
  Layer.provide(NativeProviderBuildLive),
);

const scopes = Object.values(NATIVE_PROVIDER_SCOPES);
const createNativeWorkstreamsHttpHandler = (
  runtime: NativeWorkstreamsRuntime["Service"],
  operation: NativeProviderOperation,
) =>
  Effect.fn("environment.workstreams.native")(function* () {
    const scope =
      operation === "context" || operation === "attestations"
        ? NATIVE_PROVIDER_SCOPES.context
        : operation === "settlements"
          ? NATIVE_PROVIDER_SCOPES.settlement
          : NATIVE_PROVIDER_SCOPES.reconciliation;
    yield* annotateEnvironmentRequest(`workstreams.native.${operation}`);
    const principal = yield* requireEnvironmentScope(scope);
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (request.originalUrl.includes("?") || request.originalUrl.includes("#")) {
      return yield* new EnvironmentHttpBadRequestError({
        message: "workstreams_native_invalid_request",
      });
    }
    const enrollments = NativeProviderEnrollment.of({
      getBySessionId: (sessionId) =>
        runtime.enrollments
          .getBySessionId(sessionId)
          .pipe(
            Effect.map((binding) =>
              Option.filter(
                binding,
                (value) =>
                  principal.method === "bearer-access-token" &&
                  principal.subject === `workstreams-native:${value.enrollment_id}` &&
                  principal.scopes.size === scopes.length &&
                  scopes.every((value) => principal.scopes.has(value)),
              ),
            ),
          ),
    });
    const body =
      request.source instanceof Request && request.source.body === null
        ? Stream.empty
        : request.stream;
    return yield* createNativeProviderHandlers(runtime.provider, enrollments).handle(
      operation,
      principal.sessionId,
      body,
    );
  });

export const nativeWorkstreamsHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workstreamsNative",
  Effect.fnUntraced(function* (handlers) {
    const runtime = yield* NativeWorkstreamsRuntime;
    return handlers
      .handleRaw("context", createNativeWorkstreamsHttpHandler(runtime, "context"))
      .handleRaw("attestations", createNativeWorkstreamsHttpHandler(runtime, "attestations"))
      .handleRaw("settlements", createNativeWorkstreamsHttpHandler(runtime, "settlements"))
      .handleRaw(
        "settlementLookup",
        createNativeWorkstreamsHttpHandler(runtime, "settlements/lookup"),
      );
  }),
);
