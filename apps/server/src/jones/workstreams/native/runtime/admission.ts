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
import * as Recorded from "./recordedCaller.ts";
import * as McpSessions from "../../../../mcp/McpSessionRegistry.ts";
import { canonicalJson } from "../../operations/encoding.ts";

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
        const recovered = yield* Effect.serviceOption(Recorded.RecoveryCaller);
        if (Option.isSome(recovered)) return recovered.value;
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
        const recovered = yield* Effect.serviceOption(Recorded.RecoveryCaller);
        const request = Option.isSome(recovered)
          ? recovered
          : yield* Effect.serviceOption(RequestCaller);
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

/** Adapt the native source-setup lookup here. It must validate its encrypted
 * issuance ledger and source-owned AuthSession; ordinary startup creates none.
 */
export class RecordedSourceEnrollment extends Context.Service<
  RecordedSourceEnrollment,
  {
    readonly lookup: (sessionId: string) => Effect.Effect<
      {
        readonly source: Recorded.RecordedCallerV1["source"];
        readonly destination: Recorded.RecordedCallerV1["destination"];
        readonly issued_at: number;
        readonly expires_at: number;
        readonly active: boolean;
      } | null,
      Recorded.RecordedCallerFence
    >;
  }
>()("t3/jones/workstreams/native/runtime/admission/RecordedSourceEnrollment") {}

export const recordedAuthorityLayer = Layer.effect(
  Recorded.CallerAuthority,
  Effect.gen(function* () {
    const sessions = yield* AuthSessions.AuthSessionRepository;
    const home = yield* NativeHome,
      bindingStore = yield* Bindings.BindingStore;
    const registrySession = yield* Session.Session,
      store = yield* Store.RegistryStore;
    const authority = yield* Authority.NativeStoreAuthority,
      startup = yield* Startup.Startup;
    const enrollment = yield* Effect.serviceOption(RecordedSourceEnrollment),
      db = yield* Database.NativeSql;
    const threads = yield* Effect.serviceOption(ThreadManagement.ThreadManagementService);
    const providers = yield* Effect.serviceOption(ProviderSessions.ProviderSessionManagerV2);
    const credentials = yield* Effect.serviceOption(McpSessions.McpSessionRegistry);
    const fence = () => new Recorded.RecordedCallerFence({ code: "not_granted" });
    const currentSession = Effect.fn("RecordedAuthority.session")(function* (id: string) {
      const found = yield* sessions
        .getById({ sessionId: AuthSessionId.make(id) })
        .pipe(Effect.mapError(fence));
      return Option.isSome(found) ? found.value : null;
    });
    const proveAgent = Effect.fn("RecordedAuthority.agent")(function* (
      threadId: string,
      providerInstanceId: string,
      credentialId: string,
      originalRunId?: string,
    ) {
      if (Option.isNone(threads) || Option.isNone(providers) || Option.isNone(credentials))
        return null;
      const shell = yield* threads.value
        .getThreadShell(ThreadId.make(threadId))
        .pipe(Effect.mapError(fence));
      if (!shell || shell.deletedAt !== null || shell.archivedAt !== null || shell.linkOrigin)
        return null;
      const scope = yield* credentials.value.lookupByCredentialId(credentialId);
      if (
        !scope ||
        scope.environmentId !== home.served_by_environment_id ||
        scope.thread.threadId !== threadId ||
        scope.thread.providerInstanceId !== providerInstanceId
      )
        return null;
      const records = yield* threads.value
        .getThreadRecords(shell.id, ["runs", "providerThreads"])
        .pipe(Effect.mapError(fence));
      // Only capture may select the current run. Refresh always supplies the recorded ID.
      const runId = originalRunId ?? shell.activeRunId;
      const run = records.runs.find((value) => value.id === runId);
      const provider = records.providerThreads.find((value) => value.id === run?.providerThreadId);
      if (!run || !provider?.providerSessionId || run.providerInstanceId !== providerInstanceId)
        return null;
      const attached = yield* providers.value.isMcpCallerAttached({
        threadId: shell.id,
        providerSessionId: provider.providerSessionId,
        providerInstanceId: scope.thread.providerInstanceId,
        mcpCredentialId: credentialId,
      });
      return {
        auth: {
          kind: "agent" as const,
          calling_thread_id: threadId,
          provider_instance_id: providerInstanceId,
          mcp_credential_id: credentialId,
          run_id: run.id,
          provider_thread_id: provider.id,
          provider_session_id: provider.providerSessionId,
        },
        credential: {
          credential_id: credentialId,
          calling_thread_id: threadId,
          provider_instance_id: providerInstanceId,
          active: attached,
        },
        run: {
          run_id: run.id,
          calling_thread_id: threadId,
          provider_instance_id: providerInstanceId,
          provider_thread_id: provider.id,
          provider_session_id: provider.providerSessionId,
          active: attached && shell.activeRunId === run.id,
        },
        limits: { runtimeMode: shell.runtimeMode, interactionMode: shell.interactionMode },
        capabilities: [...scope.capabilities],
      };
    });
    const snapshot = Effect.fn("RecordedAuthority.snapshot")(
      (original: Pick<Recorded.RecordedCallerV1, "auth" | "origin" | "ceiling" | "captured_at">) =>
        db
          .transaction(
            home,
            Effect.gen(function* () {
              const binding = yield* bindingStore.read().pipe(Effect.mapError(fence));
              if (!binding || !startup.sourceSessionId) return yield* fence();
              const source = yield* authority.readCurrent.pipe(Effect.mapError(fence));
              const issued = Option.isSome(enrollment)
                ? yield* enrollment.value.lookup(startup.sourceSessionId)
                : null;
              if (!issued) return yield* fence();
              const admitted = yield* registrySession
                .authorize(
                  {
                    principal_id: binding.principal_id,
                    source_environment_id: source.environmentId,
                    binding_version: binding.binding_version,
                    origin: original.origin,
                  },
                  "workstreams:read",
                )
                .pipe(Effect.mapError(fence));
              const rows = (yield* store.listPlain(home, "source_bindings"))
                .filter(
                  (row) => row.provider === "t3" && row.source_instance_id === source.environmentId,
                )
                .toSorted((a, b) => Number(b.binding_version) - Number(a.binding_version));
              const sourceBinding = rows[0];
              const destination: Recorded.RecordedCallerV1["destination"] = {
                registry_instance_id: binding.registry_instance_id,
                owner_id: binding.owner_id,
                home_environment_id: binding.home_environment_id,
                home_generation: binding.home_generation,
                principal_id: binding.principal_id,
                binding_version: binding.binding_version,
              };
              if (
                !sourceBinding ||
                sourceBinding.authority_namespace !== source.authorityNamespace ||
                sourceBinding.store_generation !== source.storeGeneration ||
                issued.source.environment_id !== source.environmentId ||
                issued.source.authority_namespace !== source.authorityNamespace ||
                issued.source.store_generation !== source.storeGeneration ||
                issued.source.binding_version !== Number(sourceBinding.binding_version) ||
                issued.source.enrollment_session_id !== startup.sourceSessionId ||
                canonicalJson(issued.destination) !== canonicalJson(destination)
              )
                return yield* fence();
              let agent: Effect.Success<ReturnType<typeof proveAgent>> = null;
              let session: AuthSessions.AuthSessionRecord | null = null;
              const auth = original.auth;
              if (auth.kind === "agent")
                agent = yield* proveAgent(
                  auth.calling_thread_id,
                  auth.provider_instance_id,
                  auth.mcp_credential_id,
                  auth.run_id,
                );
              else session = yield* currentSession(auth.session_id);
              const value: Recorded.CurrentCallerState = {
                admission: {
                  version: 1,
                  auth,
                  origin: original.origin,
                  captured_at: original.captured_at,
                  source_environment_id: source.environmentId,
                  registry_principal_id: binding.principal_id,
                  singleton_binding_version: binding.binding_version,
                  grant_version: admitted.authorization.grant.grant_version,
                  source: issued.source,
                  destination,
                  ceiling: {
                    permissions: admitted.authorization.grant.permissions,
                    scopes: session?.scopes ?? [],
                    capabilities: agent?.capabilities ?? original.ceiling.capabilities,
                    limits: agent?.limits ?? original.ceiling.limits,
                  },
                },
                registry_active: admitted.context.state === "active",
                singleton_active: binding.state === "active",
                source_binding_active: sourceBinding.state === "active",
                grant: {
                  active: admitted.authorization.grant.state === "active",
                  version: admitted.authorization.grant.grant_version,
                  permissions: admitted.authorization.grant.permissions,
                },
                enrollment: {
                  enrollment_id: issued.source.enrollment_id,
                  session_id: issued.source.enrollment_session_id,
                  active: issued.active,
                  issued_at: issued.issued_at,
                  expires_at: issued.expires_at,
                },
                session: session
                  ? {
                      session_id: session.sessionId,
                      subject: session.subject,
                      method: session.method,
                      scopes: session.scopes,
                      issued_at: session.issuedAt.epochMilliseconds,
                      expires_at: session.expiresAt.epochMilliseconds,
                      revoked: session.revokedAt !== null,
                    }
                  : null,
                credential: agent?.credential ?? null,
                run: agent?.run ?? null,
                trusted_cli_authority_id: null,
              };
              return value;
            }),
          )
          .pipe(Effect.mapError(fence)),
    );
    return Recorded.CallerAuthority.of({
      capture: Effect.fn("RecordedAuthority.capture")(function* () {
        const http = yield* Effect.serviceOption(EnvironmentAuthenticatedPrincipal);
        const mcp = yield* Effect.serviceOption(Mcp.McpInvocationContext);
        const cli = yield* Effect.serviceOption(TrustedCli);
        if (
          Number(Option.isSome(http)) + Number(Option.isSome(mcp)) + Number(Option.isSome(cli)) !==
          1
        )
          return yield* fence();
        const captured_at = DateTime.toEpochMillis(yield* DateTime.now);
        if (Option.isSome(http)) {
          const principal = http.value;
          const record = yield* snapshot({
            captured_at,
            auth: {
              kind: "bearer",
              session_id: principal.sessionId,
              subject: principal.subject,
              method: principal.method,
            },
            origin: { kind: principal.subject === "cli-issued-session" ? "cli" : "app" },
            ceiling: {
              permissions: ["workstreams:read", "workstreams:write"],
              scopes: [...principal.scopes],
              capabilities: [],
              limits: null,
            },
          });
          // The authenticated request's original scope ceiling survives a broader current session.
          return {
            ...record.admission,
            ceiling: {
              ...record.admission.ceiling,
              scopes: record.admission.ceiling.scopes.filter((scope) =>
                [...principal.scopes].some((value) => value === scope),
              ),
            },
          };
        }
        // Callback-only TrustedCli cannot establish durable recovery authority.
        if (Option.isNone(mcp)) return yield* fence();
        const invocation = mcp.value;
        if (
          invocation.environmentId !== home.served_by_environment_id ||
          !invocation.capabilities.has("orchestration") ||
          invocation.client?.linked ||
          invocation.client?.remoteOrigin
        )
          return yield* fence();
        if (invocation.client && !invocation.thread) {
          const client = invocation.client;
          const record = yield* snapshot({
            captured_at,
            origin: { kind: "agent" },
            auth: {
              kind: "mcp_client",
              session_id: client.sessionId,
              subject: "mcp-client",
              method: "bearer-access-token",
              approved_access: client.access,
              requested_limits: client.narrowedTo ?? null,
            },
            ceiling: {
              permissions: ["workstreams:read", "workstreams:write"],
              scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
              capabilities: [...invocation.capabilities],
              limits: Mcp.clientModeCeiling(client),
            },
          });
          return record.admission;
        }
        if (!invocation.thread || invocation.client) return yield* fence();
        const proof = yield* proveAgent(
          invocation.thread.threadId,
          invocation.thread.providerInstanceId,
          invocation.thread.providerSessionId,
        );
        if (!proof?.run.active) return yield* fence();
        return (yield* snapshot({
          captured_at,
          auth: proof.auth,
          origin: { kind: "agent" },
          ceiling: {
            permissions: ["workstreams:read", "workstreams:write"],
            scopes: [],
            capabilities: proof.capabilities,
            limits: proof.limits,
          },
        })).admission;
      }),
      refresh: snapshot,
      checkTarget: Effect.fn("RecordedAuthority.target")(
        function* (_record, ceiling, permission, threadId) {
          if (threadId === null) return;
          if (Option.isNone(threads)) return yield* fence();
          const target = yield* threads.value
            .getThreadShell(ThreadId.make(threadId))
            .pipe(Effect.mapError(fence));
          if (!target || target.deletedAt !== null || target.linkOrigin) return yield* fence();
          if (permission === "workstreams:write" && ceiling.limits)
            yield* assertTargetWithinLimits(ceiling.limits, target).pipe(Effect.mapError(fence));
        },
      ),
    });
  }),
);
