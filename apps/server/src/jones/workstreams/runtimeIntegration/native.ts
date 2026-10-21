import {
  AuthAccessWriteScope,
  EnvironmentHttpApi,
  EnvironmentHttpBadRequestError,
  type WorkstreamsNativeOwnerRequest,
  type WorkstreamsNativeOwnerResponse,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import { annotateEnvironmentRequest, requireEnvironmentScope } from "../../../auth/http.ts";
import { NativeStoreAuthority } from "../../../environment/NativeStoreAuthority.ts";
import * as ServerConfig from "../../../config.ts";
import * as DesktopNativeOwner from "../DesktopNativeOwner.ts";
import {
  assertLocalDesktopOwnerRequest,
  readDesktopNativeOwnerRequest,
  renderDesktopNativeOwnerResponse,
  runDesktopNativeOwnerOperation,
} from "../enrollment/desktopOwner.ts";
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
    readonly ownerOperations?: (
      request: WorkstreamsNativeOwnerRequest,
    ) => Effect.Effect<WorkstreamsNativeOwnerResponse, EnvironmentHttpBadRequestError>;
  }
>()("t3/jones/workstreams/runtimeIntegration/native/NativeWorkstreamsRuntime") {}

const makeNativeWorkstreamsRuntimeLayer = (
  evidence: NativeProviderEvidence = unavailableNativeProviderEvidence,
) =>
  Layer.effect(
    NativeWorkstreamsRuntime,
    Effect.gen(function* () {
      const authority = yield* NativeStoreAuthority;
      const config = yield* ServerConfig.ServerConfig;
      const owner = yield* DesktopNativeOwner.DesktopNativeOwner;
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* Orchestrator.OrchestratorV2;
      const attempts = yield* NativeProviderAttempts;
      const enrollments = yield* NativeProviderEnrollment;
      return NativeWorkstreamsRuntime.of({
        enrollments,
        ownerOperations: Effect.fn("NativeWorkstreamIntegration.ownerOperations")(function* (request) {
            yield* owner.refresh;
            const binding = owner.binding;
            if (!owner.available || binding === undefined)
              return yield* new EnvironmentHttpBadRequestError({
                message: "desktop_native_owner_unavailable",
              });
            return yield* runDesktopNativeOwnerOperation(
              request,
              { binding, assertCurrent: owner.assertCurrent, refresh: owner.refresh },
              {
                baseDir: config.baseDir,
                authorityDir: config.authorityStateDir,
                environmentId: binding.environmentId,
              },
            );
          }, (effect) => effect.pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
            Effect.mapError(
              () =>
                new EnvironmentHttpBadRequestError({ message: "desktop_native_owner_unavailable" }),
            ),
          )),
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
      .handleRaw(
        "ownerOperations",
        Effect.fnUntraced(function* () {
          yield* annotateEnvironmentRequest("workstreams.native.ownerOperations");
          const principal = yield* requireEnvironmentScope(AuthAccessWriteScope);
          const request = yield* HttpServerRequest.HttpServerRequest;
          yield* Effect.try({
            try: () => assertLocalDesktopOwnerRequest(request, principal),
            catch: () =>
              new EnvironmentHttpBadRequestError({ message: "desktop_native_owner_forbidden" }),
          });
          const operation = yield* readDesktopNativeOwnerRequest(request.stream);
          if (runtime.ownerOperations === undefined)
            return yield* new EnvironmentHttpBadRequestError({
              message: "desktop_native_owner_unavailable",
            });
          const result = yield* runtime.ownerOperations(operation).pipe(
            Effect.mapError(
              () =>
                new EnvironmentHttpBadRequestError({
                  message: "desktop_native_owner_unavailable",
                }),
            ),
          );
          return renderDesktopNativeOwnerResponse(result);
        }),
      )
      .handleRaw("context", createNativeWorkstreamsHttpHandler(runtime, "context"))
      .handleRaw("attestations", createNativeWorkstreamsHttpHandler(runtime, "attestations"))
      .handleRaw("settlements", createNativeWorkstreamsHttpHandler(runtime, "settlements"))
      .handleRaw(
        "settlementLookup",
        createNativeWorkstreamsHttpHandler(runtime, "settlements/lookup"),
      );
  }),
);
