import { WorkstreamsTransportCeiling } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import {
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeOrigin,
  WorkstreamsNativeVersion,
  WorkstreamsNativeDigest,
} from "@t3tools/contracts/jones/workstreamsNative";
import {
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  AuthSessionId,
  EnvironmentAuthenticatedPrincipal,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Sessions from "../../../../persistence/AuthSessions.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Credentials from "../../../../mcp/McpSessionRegistry.ts";
import * as Threads from "../../../../orchestration-v2/ThreadManagementService.ts";
import * as Providers from "../../../../orchestration-v2/ProviderSessionManager.ts";
import { readCaller, assertTargetWithinLimits } from "../../../../mcp/threadAccess.ts";
import { callerLinkOrigin } from "../../../../mcp/linkOrigin.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import * as Recorded from "./recordedCaller.ts";

const Id = Schema.NonEmptyString;
const Session = Schema.Struct({ issued_at: Schema.Number, expires_at: Schema.Number });
export const MemberLocalCallerV1 = Schema.Struct({
  schema: Schema.Literal("jones-workstreams-member-local-caller/1"),
  auth: Recorded.RecordedCallerV1.fields.auth,
  origin: Recorded.RecordedCallerV1.fields.origin,
  ceiling: Recorded.RecordedCallerV1.fields.ceiling,
  session: Schema.NullOr(Session),
  environment_id: Id,
  request_namespace: Schema.NullOr(Id),
  invocation_issued_at: Schema.NullOr(Schema.Number),
  link_origin: Schema.NullOr(Id),
  link_session: Schema.NullOr(Session),
  captured_at: Schema.Number,
});
export type MemberLocalCallerV1 = typeof MemberLocalCallerV1.Type;

export const MemberWriteTarget = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("metadata") }),
  Schema.Struct({
    kind: Schema.Literal("thread"),
    environment_id: Id,
    thread_id: Id,
  }),
]);
export type MemberWriteTarget = typeof MemberWriteTarget.Type;
export const MemberLocalWriteCallerV2 = Schema.Struct({
  ...MemberLocalCallerV1.fields,
  schema: Schema.Literal("jones-workstreams-member-local-write-caller/2"),
  target: MemberWriteTarget,
});
export type MemberLocalWriteCallerV2 = typeof MemberLocalWriteCallerV2.Type;
const WriteAssociation = Schema.Struct({
  enrollment_id: Id,
  enrollment_session_id: Id,
  intent_sha256: WorkstreamsNativeDigest,
  home: WorkstreamsNativeHomeTarget,
  canonical_origin: WorkstreamsNativeOrigin,
  principal_id: Id,
  singleton_binding_version: WorkstreamsNativeVersion,
  pin_bound_revision: WorkstreamsNativeVersion,
  source_binding_version: WorkstreamsNativeVersion,
  source: Schema.Struct({
    environment_id: Id,
    authority_namespace: Id,
    store_generation: WorkstreamsNativeVersion,
  }),
  link_generation: Id,
});
export const MemberRecordedWriteCallerV2 = Schema.Struct({
  schema: Schema.Literal("jones-workstreams-member-recorded-write-caller/2"),
  local: MemberLocalWriteCallerV2,
  association: Schema.Struct({
    ...WriteAssociation.fields,
    member_pin_id: Id,
    home_pin_id: Id,
    home_pin_bound_revision: WorkstreamsNativeVersion,
  }),
  original_outbound_ceiling: WorkstreamsTransportCeiling,
});
export type MemberRecordedWriteCallerV2 = typeof MemberRecordedWriteCallerV2.Type;

const decodeWrite = Schema.decodeUnknownEffect(MemberLocalWriteCallerV2, {
  onExcessProperty: "error",
});
export interface AdmittedLocalWrite {
  readonly record: MemberLocalWriteCallerV2;
  readonly ceiling: MemberLocalCallerV1["ceiling"];
  readonly checkTarget: (
    limits?: NonNullable<MemberLocalCallerV1["ceiling"]["limits"]>,
  ) => Effect.Effect<void, Recorded.RecordedCallerFence>;
}

const decode = Schema.decodeUnknownEffect(MemberLocalCallerV1, { onExcessProperty: "error" });
const fence = () => new Recorded.RecordedCallerFence({ code: "not_granted" });
const ranks = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };
export const intersectMemberCeilings = (
  a: MemberLocalCallerV1["ceiling"],
  b: MemberLocalCallerV1["ceiling"],
): MemberLocalCallerV1["ceiling"] => ({
  permissions: a.permissions.filter((x) => b.permissions.includes(x)),
  scopes: a.scopes.filter((x) => b.scopes.includes(x)),
  capabilities: a.capabilities.filter((x) => b.capabilities.includes(x)),
  limits:
    a.limits === null
      ? b.limits
      : b.limits === null
        ? a.limits
        : {
            runtimeMode:
              ranks[a.limits.runtimeMode] <= ranks[b.limits.runtimeMode]
                ? a.limits.runtimeMode
                : b.limits.runtimeMode,
            interactionMode:
              a.limits.interactionMode === "plan" || b.limits.interactionMode === "plan"
                ? "plan"
                : "default",
          },
});
const identity = (r: MemberLocalCallerV1) => ({
  ...r,
  captured_at: undefined,
  ceiling: undefined,
  auth: r.auth.kind === "mcp_client" ? { ...r.auth, approved_access: undefined } : r.auth,
});

/** Local evidence only. No registry service, grant or home principal participates here. */
export class MemberLocalCallers extends Context.Service<
  MemberLocalCallers,
  {
    readonly captureWrite: (
      environmentId: string,
      target: MemberWriteTarget,
    ) => Effect.Effect<MemberLocalWriteCallerV2, Recorded.RecordedCallerFence>;
    readonly readmitWrite: (
      record: unknown,
    ) => Effect.Effect<AdmittedLocalWrite, Recorded.RecordedCallerFence>;
    readonly readmitWriteRetained: (
      record: unknown,
    ) => Effect.Effect<AdmittedLocalWrite, Recorded.RecordedCallerFence>;
    readonly captureOwnPlacement: (
      environmentId: string,
    ) => Effect.Effect<MemberLocalCallerV1, Recorded.RecordedCallerFence>;
    readonly capture: (
      environmentId: string,
    ) => Effect.Effect<MemberLocalCallerV1, Recorded.RecordedCallerFence>;
    readonly readmitRetained: (record: unknown) => Effect.Effect<
      {
        readonly record: MemberLocalCallerV1;
        readonly ceiling: MemberLocalCallerV1["ceiling"];
        readonly checkTarget: (
          threadId: string | null,
        ) => Effect.Effect<void, Recorded.RecordedCallerFence>;
      },
      Recorded.RecordedCallerFence
    >;
    readonly readmit: (
      record: unknown,
      ownPlacementOnly?: boolean,
    ) => Effect.Effect<
      {
        readonly record: MemberLocalCallerV1;
        readonly ceiling: MemberLocalCallerV1["ceiling"];
        readonly checkTarget: (
          threadId: string | null,
        ) => Effect.Effect<void, Recorded.RecordedCallerFence>;
      },
      Recorded.RecordedCallerFence
    >;
  }
>()("t3/jones/workstreams/native/runtime/memberRecordedCaller/MemberLocalCallers") {}

const make = Effect.gen(function* () {
  const sessions = yield* Sessions.AuthSessionRepository;
  const snapshot = Effect.fn("MemberLocalCallers.snapshot")(function* (
    environmentId: string,
    original?: MemberLocalCallerV1,
    retained = false,
    write = false,
  ) {
    const http = yield* Effect.serviceOption(EnvironmentAuthenticatedPrincipal);
    const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
    if (!retained && Number(Option.isSome(http)) + Number(Option.isSome(invocation)) !== 1)
      return yield* fence();
    const now = yield* Clock.currentTimeMillis;
    const base = {
      schema: "jones-workstreams-member-local-caller/1" as const,
      environment_id: environmentId,
      captured_at: now,
      request_namespace: null as string | null,
      invocation_issued_at: null as number | null,
      link_origin: null as string | null,
      link_session: null as typeof Session.Type | null,
    };
    const currentSession = Effect.fn("MemberLocalCallers.session")(function* (id: string) {
      const row = yield* sessions
        .getById({ sessionId: AuthSessionId.make(id) })
        .pipe(Effect.mapError(fence));
      if (
        Option.isNone(row) ||
        row.value.revokedAt !== null ||
        row.value.issuedAt.epochMilliseconds > now ||
        row.value.expiresAt.epochMilliseconds <= now
      )
        return yield* fence();
      return row.value;
    });
    if ((retained && original?.auth.kind === "bearer") || (!retained && Option.isSome(http))) {
      const principal =
        retained && original?.auth.kind === "bearer"
          ? {
              sessionId: AuthSessionId.make(original.auth.session_id),
              subject: original.auth.subject,
              method: original.auth.method,
              scopes: new Set(original.ceiling.scopes),
            }
          : Option.getOrThrow(http);
      const row = yield* currentSession(principal.sessionId);
      if (
        row.subject !== principal.subject ||
        row.method !== principal.method ||
        row.subject === "mcp-client"
      )
        return yield* fence();
      const scopes = row.scopes.filter((s) => principal.scopes.has(s));
      return {
        ...base,
        auth: {
          kind: "bearer" as const,
          session_id: row.sessionId,
          subject: row.subject,
          method: row.method,
        },
        origin: {
          kind: row.subject === "cli-issued-session" ? ("cli" as const) : ("app" as const),
        },
        session: {
          issued_at: row.issuedAt.epochMilliseconds,
          expires_at: row.expiresAt.epochMilliseconds,
        },
        ceiling: {
          permissions: [
            ...(scopes.includes(AuthOrchestrationReadScope) ? ["workstreams:read" as const] : []),
            ...(write && scopes.includes(AuthOrchestrationOperateScope)
              ? ["workstreams:write" as const]
              : []),
          ],
          scopes,
          capabilities: [],
          limits: {
            runtimeMode: "approval-required" as const,
            interactionMode: "default" as const,
          },
        },
      };
    }
    if (write && retained && original?.auth.kind === "mcp_client") {
      const row = yield* currentSession(original.auth.session_id);
      if (
        row.subject !== "mcp-client" ||
        row.method !== "bearer-access-token" ||
        !row.scopes.includes(AuthOrchestrationOperateScope)
      )
        return yield* fence();
      // Auth rows retain scopes, not the token's higher mode ceiling; recovery narrows conservatively.
      const access = "approval-required" as const;
      return {
        ...base,
        request_namespace: original.request_namespace,
        invocation_issued_at: original.invocation_issued_at,
        origin: original.origin,
        auth: { ...original.auth, approved_access: access },
        session: {
          issued_at: row.issuedAt.epochMilliseconds,
          expires_at: row.expiresAt.epochMilliseconds,
        },
        ceiling: {
          permissions: [
            ...(row.scopes.includes(AuthOrchestrationReadScope)
              ? ["workstreams:read" as const]
              : []),
            "workstreams:write" as const,
          ],
          scopes: row.scopes,
          capabilities: original.ceiling.capabilities,
          limits: intersectMemberCeilings(original.ceiling, {
            ...original.ceiling,
            limits: { runtimeMode: access, interactionMode: "default" },
          }).limits,
        },
      };
    }
    let scope: Mcp.McpInvocationScope;
    if (retained && original?.auth.kind === "agent") {
      const registry = yield* Effect.serviceOption(Credentials.McpSessionRegistry);
      if (Option.isNone(registry)) return yield* fence();
      const found = yield* registry.value.lookupByCredentialId(original.auth.mcp_credential_id);
      if (!found) return yield* fence();
      scope = found;
    } else {
      if (retained || Option.isNone(invocation)) return yield* fence();
      scope = invocation.value;
    }
    if (
      scope.environmentId !== environmentId ||
      !scope.capabilities.has("orchestration") ||
      scope.client?.linked ||
      scope.client?.remoteOrigin ||
      Boolean(scope.thread) === Boolean(scope.client)
    )
      return yield* fence();
    const common = {
      ...base,
      request_namespace: scope.requestNamespace,
      invocation_issued_at: scope.issuedAt,
      origin: { kind: "agent" as const },
    };
    if (scope.client) {
      const client = scope.client;
      const row = yield* currentSession(client.sessionId);
      if (row.subject !== "mcp-client" || row.method !== "bearer-access-token")
        return yield* fence();
      const limits = Mcp.clientModeCeiling(client);
      return {
        ...common,
        auth: {
          kind: "mcp_client" as const,
          session_id: row.sessionId,
          subject: "mcp-client" as const,
          method: "bearer-access-token" as const,
          approved_access: client.access,
          requested_limits: client.narrowedTo ?? null,
        },
        session: {
          issued_at: row.issuedAt.epochMilliseconds,
          expires_at: row.expiresAt.epochMilliseconds,
        },
        ceiling: {
          permissions: [
            ...(row.scopes.includes(AuthOrchestrationReadScope)
              ? ["workstreams:read" as const]
              : []),
            ...(write &&
            client.access !== "read-only" &&
            row.scopes.includes(AuthOrchestrationOperateScope)
              ? ["workstreams:write" as const]
              : []),
          ],
          scopes: row.scopes,
          capabilities: [...scope.capabilities],
          limits,
        },
      };
    }
    if (!scope.thread) return yield* fence();
    const threads = yield* Effect.serviceOption(Threads.ThreadManagementService);
    const credentials = yield* Effect.serviceOption(Credentials.McpSessionRegistry);
    const providers = yield* Effect.serviceOption(Providers.ProviderSessionManagerV2);
    if (Option.isNone(threads) || Option.isNone(credentials) || Option.isNone(providers))
      return yield* fence();
    const credential = yield* credentials.value.lookupByCredentialId(
      scope.thread.providerSessionId,
    );
    if (
      !credential ||
      canonicalJson({ ...credential, capabilities: undefined }) !==
        canonicalJson({ ...scope, capabilities: undefined })
    )
      return yield* fence();
    const caller = yield* readCaller().pipe(
      Effect.provideService(Mcp.McpInvocationContext, scope),
      Effect.provideService(Threads.ThreadManagementService, threads.value),
      Effect.mapError(fence),
    );
    const shell = caller.caller;
    if (
      !shell ||
      shell.archivedAt !== null ||
      shell.deletedAt !== null ||
      shell.providerInstanceId !== scope.thread.providerInstanceId
    )
      return yield* fence();
    const records = yield* threads.value
      .getThreadRecords(shell.id, ["runs", "providerThreads"])
      .pipe(Effect.mapError(fence));
    const runId = original?.auth.kind === "agent" ? original.auth.run_id : shell.activeRunId;
    const run = records.runs.find((r) => r.id === runId);
    const provider = records.providerThreads.find((p) => p.id === run?.providerThreadId);
    if (
      !run ||
      shell.activeRunId !== run.id ||
      run.providerInstanceId !== scope.thread.providerInstanceId ||
      !provider?.providerSessionId ||
      !(yield* providers.value.isMcpCallerAttached({
        threadId: shell.id,
        providerSessionId: provider.providerSessionId,
        providerInstanceId: scope.thread.providerInstanceId,
        mcpCredentialId: scope.thread.providerSessionId,
      }))
    )
      return yield* fence();
    const link = callerLinkOrigin(caller);
    const linkRow = link === undefined ? null : yield* currentSession(link.sessionId);
    if (
      linkRow &&
      (linkRow.subject !== "mcp-client" ||
        linkRow.method !== "bearer-access-token" ||
        !linkRow.scopes.includes(AuthOrchestrationReadScope))
    )
      return yield* fence();
    const linkLimits =
      link === undefined
        ? caller.limits
        : Mcp.clientModeCeiling({
            sessionId: link.sessionId,
            label: link.label,
            access: link.access ?? "read-only",
          });
    return {
      ...common,
      link_session:
        linkRow === null
          ? null
          : {
              issued_at: linkRow.issuedAt.epochMilliseconds,
              expires_at: linkRow.expiresAt.epochMilliseconds,
            },
      auth: {
        kind: "agent" as const,
        calling_thread_id: shell.id,
        provider_instance_id: scope.thread.providerInstanceId,
        mcp_credential_id: scope.thread.providerSessionId,
        run_id: run.id,
        provider_thread_id: provider.id,
        provider_session_id: provider.providerSessionId,
      },
      session: null,
      link_origin:
        callerLinkOrigin(caller) === undefined ? null : canonicalJson(callerLinkOrigin(caller)),
      ceiling: {
        permissions: [
          "workstreams:read" as const,
          ...(write &&
          (link === undefined ||
            (link.access !== undefined &&
              link.access !== "read-only" &&
              linkRow?.scopes.includes(AuthOrchestrationOperateScope)))
            ? ["workstreams:write" as const]
            : []),
        ],
        scopes: [],
        capabilities: [...scope.capabilities].filter((x) => credential.capabilities.has(x)),
        limits: intersectMemberCeilings(
          { permissions: [], scopes: [], capabilities: [], limits: caller.limits },
          { permissions: [], scopes: [], capabilities: [], limits: linkLimits },
        ).limits,
      },
    };
  });
  const readmit = Effect.fn("MemberLocalCallers.readmit")(function* (
    input: unknown,
    ownPlacementOnly = false,
    retained = false,
  ) {
    const record = yield* decode(input).pipe(
      Effect.mapError(() => new Recorded.RecordedCallerFence({ code: "malformed" })),
    );
    if (
      record.auth.kind === "trusted_cli" ||
      !record.ceiling.limits ||
      !Number.isFinite(record.captured_at) ||
      record.captured_at < 0 ||
      record.captured_at > (yield* Clock.currentTimeMillis)
    )
      return yield* fence();
    const current = yield* snapshot(record.environment_id, record, retained);
    if (canonicalJson(identity(record)) !== canonicalJson(identity(current))) return yield* fence();
    if (record.link_origin !== null && (!ownPlacementOnly || record.auth.kind !== "agent"))
      return yield* fence();
    const ceiling = intersectMemberCeilings(record.ceiling, current.ceiling);
    if (!ceiling.permissions.includes("workstreams:read")) return yield* fence();
    const checkTarget = Effect.fn("MemberLocalCallers.checkTarget")(function* (
      threadId: string | null,
    ) {
      const latest = yield* snapshot(record.environment_id, record, retained);
      if (
        canonicalJson(identity(latest)) !== canonicalJson(identity(record)) ||
        !intersectMemberCeilings(record.ceiling, latest.ceiling).permissions.includes(
          "workstreams:read",
        ) ||
        canonicalJson(intersectMemberCeilings(record.ceiling, latest.ceiling).limits) !==
          canonicalJson(ceiling.limits)
      )
        return yield* fence();
      if (
        ownPlacementOnly &&
        (record.auth.kind !== "agent" || threadId !== record.auth.calling_thread_id)
      )
        return yield* fence();
      if (threadId === null) return;
      const threads = yield* Effect.serviceOption(Threads.ThreadManagementService);
      if (Option.isNone(threads)) return yield* fence();
      const target = yield* threads.value
        .getThreadShell(ThreadId.make(threadId))
        .pipe(Effect.mapError(fence));
      if (
        !target ||
        target.deletedAt !== null ||
        target.archivedAt !== null ||
        (target.linkOrigin !== undefined &&
          (!ownPlacementOnly || canonicalJson(target.linkOrigin) !== record.link_origin))
      )
        return yield* fence();
      // This capability reads placement only; limits cap forwarding, never authorize a write.
    });
    return { record, ceiling, checkTarget };
  });

  const readmitWrite = Effect.fn("MemberLocalCallers.readmitWrite")(function* (
    input: unknown,
    retained = false,
  ) {
    const record = yield* decodeWrite(input).pipe(
      Effect.mapError(() => new Recorded.RecordedCallerFence({ code: "malformed" })),
    );
    const { target, ...fields } = record;
    const original: MemberLocalCallerV1 = {
      ...fields,
      schema: "jones-workstreams-member-local-caller/1",
    };
    if (
      record.auth.kind === "trusted_cli" ||
      !record.ceiling.limits ||
      !Number.isFinite(record.captured_at) ||
      record.captured_at < 0 ||
      record.captured_at > (yield* Clock.currentTimeMillis) ||
      (target.kind === "thread" && target.environment_id !== record.environment_id)
    )
      return yield* fence();
    // A linked write needs the later exact home-transaction proof; local evidence alone is insufficient.
    if (record.link_origin !== null) return yield* fence();
    const refresh = Effect.fn("MemberLocalCallers.refreshWrite")(function* () {
      const latest = yield* snapshot(record.environment_id, original, retained, true);
      if (canonicalJson(identity(original)) !== canonicalJson(identity(latest)))
        return yield* fence();
      const ceiling = intersectMemberCeilings(original.ceiling, latest.ceiling);
      if (
        !ceiling.permissions.includes("workstreams:write") ||
        !ceiling.limits ||
        ceiling.limits.interactionMode !== "default" ||
        (record.auth.kind === "mcp_client" &&
          (record.auth.approved_access === "read-only" ||
            latest.auth.kind !== "mcp_client" ||
            latest.auth.approved_access === "read-only")) ||
        ((record.auth.kind === "agent" || record.auth.kind === "mcp_client") &&
          !ceiling.capabilities.includes("orchestration"))
      )
        return yield* fence();
      return ceiling;
    });
    const ceiling = yield* refresh();
    const checkTarget = Effect.fn("MemberLocalCallers.checkWriteTarget")(function* (
      limits?: NonNullable<MemberLocalCallerV1["ceiling"]["limits"]>,
    ) {
      const current = yield* refresh();
      if (canonicalJson(current) !== canonicalJson(ceiling)) return yield* fence();
      const effective =
        limits === undefined
          ? current.limits
          : intersectMemberCeilings(current, { ...current, limits }).limits;
      if (!effective || effective.interactionMode !== "default") return yield* fence();
      if (target.kind === "metadata") return;
      const threads = yield* Effect.serviceOption(Threads.ThreadManagementService);
      if (Option.isNone(threads)) return yield* fence();
      const shell = yield* threads.value
        .getThreadShell(ThreadId.make(target.thread_id))
        .pipe(Effect.mapError(fence));
      if (
        !shell ||
        shell.archivedAt !== null ||
        shell.deletedAt !== null ||
        shell.linkOrigin !== undefined ||
        !current.limits
      )
        return yield* fence();
      yield* assertTargetWithinLimits(effective, shell).pipe(Effect.mapError(fence));
    });
    yield* checkTarget();
    return { record, ceiling, checkTarget } satisfies AdmittedLocalWrite;
  });
  return MemberLocalCallers.of({
    captureWrite: Effect.fn("MemberLocalCallers.captureWrite")(function* (environmentId, target) {
      const current = yield* snapshot(environmentId, undefined, false, true);
      const record: MemberLocalWriteCallerV2 = {
        ...current,
        schema: "jones-workstreams-member-local-write-caller/2",
        target,
      };
      yield* readmitWrite(record);
      return record;
    }),
    readmitWrite,
    readmitWriteRetained: (record) => readmitWrite(record, true),
    captureOwnPlacement: Effect.fn("MemberLocalCallers.captureOwnPlacement")(
      function* (environmentId) {
        const record = yield* snapshot(environmentId);
        if (record.auth.kind !== "agent") return yield* fence();
        const current = yield* readmit(record, true);
        yield* current.checkTarget(record.auth.calling_thread_id);
        return record;
      },
    ),
    capture: Effect.fn("MemberLocalCallers.capture")(function* (environmentId) {
      const record = yield* snapshot(environmentId);
      yield* readmit(record);
      return record;
    }),
    readmit,
    readmitRetained: (record) => readmit(record, false, true),
  });
});
/** @public Request-scoped adapter over authenticated invocation, session and exact run records. */
export const layer = Layer.effect(MemberLocalCallers, make);
