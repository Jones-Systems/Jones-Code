import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  AuthSessionId,
  EnvironmentAuthenticatedPrincipal,
  ThreadId,
} from "@t3tools/contracts";
import type { WorkstreamOperationOrigin } from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as ThreadManagement from "../../../../orchestration-v2/ThreadManagementService.ts";
import * as ProviderSessions from "../../../../orchestration-v2/ProviderSessionManager.ts";
import {
  readCaller,
  assertLiveCaller,
  assertTargetWithinLimits,
} from "../../../../mcp/threadAccess.ts";
import { assertNotLinked, assertSameLink, callerLinkOrigin } from "../../../../mcp/linkOrigin.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as NativeRuntime from "../../runtimeIntegration/native.ts";
import { NATIVE_PROVIDER_SCOPES } from "../../nativeProvider/enrollment.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Session from "../registry/session.ts";
import * as Store from "../store/registryStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Database from "../store/sql.ts";
import * as Source from "../providers/localAttestation.ts";
import * as Startup from "./startup.ts";

export type Permission = "workstreams:read" | "workstreams:write";
export interface CallerEvidence {
  readonly origin: WorkstreamOperationOrigin;
  readonly check: (
    permission: Permission,
    threadId: string | null,
  ) => Effect.Effect<void, Source.ProviderError>;
}
/** Request-local evidence; never installed as a server default. */
export class RequestCaller extends Context.Service<RequestCaller, CallerEvidence>()(
  "t3/jones/workstreams/native/runtime/admission/RequestCaller",
) {}
/** Only an application-owned local CLI boundary may provide this context. */
export class TrustedCli extends Context.Service<
  TrustedCli,
  { readonly environmentId: string; readonly check: CallerEvidence["check"] }
>()("t3/jones/workstreams/native/runtime/admission/TrustedCli") {}
export class CallerBoundary extends Context.Service<
  CallerBoundary,
  {
    readonly current: () => Effect.Effect<CallerEvidence, Source.ProviderError>;
  }
>()("t3/jones/workstreams/native/runtime/admission/CallerBoundary") {}

const isProviderError = Schema.is(Source.ProviderError);
const denied = () => new Source.ProviderError({ code: "not_granted" });
export const callerBoundaryLayer = Layer.effect(
  CallerBoundary,
  Effect.gen(function* () {
    const sessions = yield* AuthSessions.AuthSessionRepository;
    const home = yield* NativeHome;
    const currentSession = Effect.fn("NativeCaller.currentSession")(function* (id: string) {
      const value = yield* sessions
        .getById({ sessionId: AuthSessionId.make(id) })
        .pipe(Effect.mapError(denied));
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      if (
        Option.isNone(value) ||
        value.value.revokedAt !== null ||
        value.value.issuedAt.epochMilliseconds > now ||
        value.value.expiresAt.epochMilliseconds <= now
      )
        return yield* denied();
      return value.value;
    });
    return CallerBoundary.of({
      current: Effect.fn("NativeCaller.current")(function* () {
        const captured = yield* Effect.serviceOption(RequestCaller);
        if (Option.isSome(captured)) return captured.value;
        const http = yield* Effect.serviceOption(EnvironmentAuthenticatedPrincipal);
        const mcp = yield* Effect.serviceOption(Mcp.McpInvocationContext);
        const cli = yield* Effect.serviceOption(TrustedCli);
        if (
          Number(Option.isSome(http)) + Number(Option.isSome(mcp)) + Number(Option.isSome(cli)) !==
          1
        )
          return yield* denied();
        if (Option.isSome(http)) {
          const principal = http.value;
          return {
            origin: { kind: principal.subject === "cli-issued-session" ? "cli" : "app" },
            check: Effect.fn("NativeCaller.httpCheck")(function* (permission) {
              const session = yield* currentSession(principal.sessionId);
              const scope =
                permission === "workstreams:write"
                  ? AuthOrchestrationOperateScope
                  : AuthOrchestrationReadScope;
              if (
                session.subject !== principal.subject ||
                session.method !== principal.method ||
                !session.scopes.includes(scope) ||
                !principal.scopes.has(scope)
              )
                return yield* denied();
            }),
          } satisfies CallerEvidence;
        }
        if (Option.isSome(cli)) {
          if (cli.value.environmentId !== home.served_by_environment_id) return yield* denied();
          return { origin: { kind: "cli" }, check: cli.value.check } satisfies CallerEvidence;
        }
        if (Option.isNone(mcp)) return yield* denied();
        const invocation = mcp.value;
        const threads = yield* Effect.serviceOption(ThreadManagement.ThreadManagementService);
        const providerSessions = yield* Effect.serviceOption(
          ProviderSessions.ProviderSessionManagerV2,
        );
        return {
          origin: { kind: "agent" },
          check: Effect.fn("NativeCaller.mcpCheck")(function* (permission, threadId) {
            if (
              invocation.environmentId !== home.served_by_environment_id ||
              !invocation.capabilities.has("orchestration") ||
              (!invocation.thread && !invocation.client) ||
              invocation.client?.linked ||
              invocation.client?.remoteOrigin
            )
              return yield* denied();
            if (invocation.client) {
              const session = yield* currentSession(invocation.client.sessionId);
              const scope =
                permission === "workstreams:write"
                  ? AuthOrchestrationOperateScope
                  : AuthOrchestrationReadScope;
              if (
                session.subject !== "mcp-client" ||
                session.method !== "bearer-access-token" ||
                !session.scopes.includes(scope) ||
                (permission === "workstreams:write" &&
                  (invocation.client.access === "read-only" ||
                    Mcp.clientModeCeiling(invocation.client).interactionMode === "plan"))
              )
                return yield* denied();
            }
            if (invocation.thread === undefined && threadId === null) return;
            if (Option.isNone(threads))
              return yield* new Source.ProviderError({ code: "provider_unconfigured" });
            const check = Effect.gen(function* () {
              const caller = yield* readCaller();
              if (callerLinkOrigin(caller) !== undefined) return yield* denied();
              yield* assertNotLinked(
                caller,
                "use a native home before member forwarding is supported",
              );
              if (caller.caller) yield* assertLiveCaller(caller);
              if (permission === "workstreams:write" && caller.limits.interactionMode === "plan")
                return yield* denied();
              if (threadId !== null) {
                const target = yield* caller.threads.getThreadShell(ThreadId.make(threadId));
                if (target === null || target.deletedAt !== null) return yield* denied();
                yield* assertSameLink(caller, target);
                if (permission === "workstreams:write")
                  yield* assertTargetWithinLimits(caller.limits, target);
              }
            }).pipe(
              Effect.provideService(ThreadManagement.ThreadManagementService, threads.value),
              Effect.provideService(Mcp.McpInvocationContext, invocation),
              Effect.mapError(denied),
            );
            yield* Option.isSome(providerSessions)
              ? check.pipe(
                  Effect.provideService(
                    ProviderSessions.ProviderSessionManagerV2,
                    providerSessions.value,
                  ),
                )
              : check;
          }),
        } satisfies CallerEvidence;
      }),
    });
  }),
);

export class Admission extends Context.Service<
  Admission,
  {
    readonly admit: (
      permission: Permission,
      threadId?: string | null,
    ) => Effect.Effect<AdmittedCaller, Source.ProviderError>;
  }
>()("t3/jones/workstreams/native/runtime/admission") {}

export const layer = Layer.effect(
  Admission,
  Effect.gen(function* () {
    const home = yield* NativeHome,
      bindings = yield* Bindings.BindingStore,
      session = yield* Session.Session;
    return Admission.of({
      admit: Effect.fn("NativeAdmission.admit")(function* (permission, threadId = null) {
        const request = yield* Effect.serviceOption(RequestCaller);
        if (Option.isNone(request))
          return yield* new Source.ProviderError({ code: "provider_unconfigured" });
        yield* request.value.check(permission, threadId);
        const binding = yield* bindings
          .read()
          .pipe(Effect.mapError(() => new Source.ProviderError({ code: "provider_unavailable" })));
        if (!binding) return yield* denied();
        const caller: AdmittedCaller = {
          principal_id: binding.principal_id,
          source_environment_id: home.served_by_environment_id,
          binding_version: binding.binding_version,
          origin: request.value.origin,
        };
        yield* session.authorize(caller, permission).pipe(
          Effect.mapError(
            (error) =>
              new Source.ProviderError({
                code: error.code === "fenced" ? "fenced" : "not_granted",
              }),
          ),
        );
        return caller;
      }),
    });
  }),
);

/** Reuses only a source-issued enrollment; the config contains its non-secret lookup ID. */
export const sourceAdmissionLayer = Layer.effect(
  Source.SourceAdmission,
  Effect.gen(function* () {
    const home = yield* NativeHome,
      startup = yield* Startup.Startup,
      admission = yield* Admission,
      authority = yield* Authority.NativeStoreAuthority,
      runtime = yield* NativeRuntime.NativeWorkstreamsRuntime,
      store = yield* Store.RegistryStore,
      db = yield* Database.NativeSql;
    return Source.SourceAdmission.of({
      authorize: Effect.fn("NativeSourceAdmission.authorize")(function* (input) {
        const target = input.expected_home;
        if (
          target.registry_instance_id !== home.registry_instance_id ||
          target.owner_id !== home.owner_id ||
          target.home_environment_id !== home.home_environment_id ||
          target.server_generation !== home.server_generation
        )
          return yield* new Source.ProviderError({ code: "registry_mismatch" });
        if (!startup.sourceSessionId)
          return yield* new Source.ProviderError({ code: "provider_unconfigured" });
        const permission = input.operation === "settle" ? "workstreams:write" : "workstreams:read";
        const caller = yield* admission.admit(permission, input.thread_id);
        const tuple = yield* authority.readCurrent.pipe(
          Effect.mapError(
            (error) =>
              new Source.ProviderError({
                code: error.code === "fenced" ? "fenced" : "provider_unavailable",
              }),
          ),
        );
        const enrolled = yield* runtime.enrollments
          .getBySessionId(startup.sourceSessionId)
          .pipe(Effect.mapError(() => new Source.ProviderError({ code: "provider_unavailable" })));
        if (Option.isNone(enrolled))
          return yield* new Source.ProviderError({ code: "provider_unconfigured" });
        const binding = enrolled.value;
        if (
          binding.owner_id !== home.owner_id ||
          binding.principal_id !== caller.principal_id ||
          binding.registry_origin !== home.canonical_origin
        )
          return yield* denied();
        if (
          tuple.environmentId !== caller.source_environment_id ||
          binding.source_instance_id !== tuple.environmentId ||
          binding.authority_namespace !== tuple.authorityNamespace ||
          binding.store_generation !== tuple.storeGeneration
        )
          return yield* new Source.ProviderError({ code: "generation_mismatch" });
        const scope =
          input.operation === "settle"
            ? NATIVE_PROVIDER_SCOPES.settlement
            : input.operation === "settle_lookup"
              ? NATIVE_PROVIDER_SCOPES.reconciliation
              : NATIVE_PROVIDER_SCOPES.context;
        if (!binding.scopes.includes(scope)) return yield* denied();
        yield* db
          .transaction(
            home,
            Effect.gen(function* () {
              const rows = (yield* store.listPlain(home, "source_bindings"))
                .filter(
                  (row) => row.provider === "t3" && row.source_instance_id === tuple.environmentId,
                )
                .toSorted((a, b) => Number(b.binding_version) - Number(a.binding_version));
              const current = rows[0];
              if (
                !current ||
                current.state !== "active" ||
                current.authority_namespace !== tuple.authorityNamespace ||
                current.store_generation !== tuple.storeGeneration
              )
                return yield* new Source.ProviderError({ code: "generation_mismatch" });
              yield* admission.admit(permission, input.thread_id);
            }),
          )
          .pipe(
            Effect.mapError((error) =>
              isProviderError(error)
                ? error
                : new Source.ProviderError({ code: "provider_unavailable" }),
            ),
          );
        return binding;
      }),
    });
  }),
);
