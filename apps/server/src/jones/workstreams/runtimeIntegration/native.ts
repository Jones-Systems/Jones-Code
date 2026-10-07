import { EnvironmentHttpApi, EnvironmentHttpBadRequestError } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import { annotateEnvironmentRequest, requireEnvironmentScope } from "../../../auth/http.ts";
import { NativeStoreAuthority } from "../../../environment/NativeStoreAuthority.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import { NativeEnrollmentsLive, NativeProviderEnrollmentLive } from "../enrollment/service.ts";
import {
  NativeProviderAttempts,
  NativeProviderAttemptsLive,
} from "../nativeProvider/attemptRepository.ts";
import { NativeProviderEnrollment, NATIVE_PROVIDER_SCOPES } from "../nativeProvider/enrollment.ts";
import * as NativeEvidence from "../nativeProvider/evidence.ts";
import {
  createNativeProviderHandlers,
  type NativeProviderOperation,
} from "../nativeProvider/http.ts";
import {
  makeWorkstreamsNativeProvider,
  type WorkstreamsNativeProvider,
  type NativeProviderEvidence,
  unavailableNativeProviderEvidence,
} from "../nativeProvider/service.ts";

export class NativeWorkstreamsRuntime extends Context.Service<
  NativeWorkstreamsRuntime,
  {
    readonly provider: WorkstreamsNativeProvider;
    readonly enrollments: NativeProviderEnrollment["Service"];
  }
>()("t3/jones/workstreams/runtimeIntegration/native/NativeWorkstreamsRuntime") {}

export const makeNativeWorkstreamsRuntimeLayer = (
  evidence: NativeProviderEvidence = unavailableNativeProviderEvidence,
) =>
  Layer.effect(
    NativeWorkstreamsRuntime,
    Effect.gen(function* () {
      const authority = yield* NativeStoreAuthority;
      const engine = yield* Orchestrator.OrchestratorV2;
      const attempts = yield* NativeProviderAttempts;
      const enrollments = yield* NativeProviderEnrollment;
      return NativeWorkstreamsRuntime.of({
        enrollments,
        provider: makeWorkstreamsNativeProvider({
          authority,
          threadExists: (threadId) =>
            engine.getThreadShell(threadId).pipe(Effect.map((shell) => shell !== null)),
          engine,
          evidence,
          attempts,
        }),
      });
    }),
  ).pipe(
    Layer.provide(NativeProviderAttemptsLive),
    Layer.provide(NativeProviderEnrollmentLive.pipe(Layer.provide(NativeEnrollmentsLive))),
  );

export const nativeWorkstreamsRuntimeLayer = Layer.unwrap(
  Effect.gen(function* () {
    const evidence = yield* NativeEvidence.NativeProviderEvidence;
    return makeNativeWorkstreamsRuntimeLayer(evidence);
  }),
).pipe(Layer.provide(NativeEvidence.makeNativeProviderEvidenceLayer()));

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
