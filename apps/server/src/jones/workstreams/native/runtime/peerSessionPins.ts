import { AuthSessionId } from "@t3tools/contracts";
import type {
  WorkstreamsPeerSessionPinV1 as Pin,
  WorkstreamsTransportCeiling,
} from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Auth from "../../../../auth/EnvironmentAuth.ts";
import * as Sessions from "../../../../persistence/AuthSessions.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import * as Records from "../store/peerSessionPinStore.ts";
import * as Bindings from "../store/bindingStore.ts";

export class PeerSessionError extends Schema.TaggedError<PeerSessionError>()("PeerSessionError", {
  code: Schema.Literals([
    "unauthenticated",
    "not_current",
    "wrong_role",
    "authority_mismatch",
    "invalid_request",
    "owner_unqualified",
    "conflict",
    "unavailable",
  ]),
  cause: Schema.optionalKey(Schema.Defect()),
}) {
  override get message() {
    return `Native peer session refused: ${this.code}.`;
  }
}
const isPeerSessionError = Schema.is(PeerSessionError);
const decodeSessionId = Schema.decodeUnknownEffect(AuthSessionId);
export const mapFailure = (cause: unknown) =>
  isPeerSessionError(cause) ? cause : new PeerSessionError({ code: "unavailable", cause });
interface NativeSessionFacts {
  readonly session: Pin["session"];
  readonly checkCurrent: () => Effect.Effect<Pin["session"], PeerSessionError>;
}
/** Native authentication registers the exact invocation object. No row/label/ID-only path exists. */
export class NativePeerSessions extends Context.Service<
  NativePeerSessions,
  {
    readonly authenticate: (
      request: Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0],
      invocation: Mcp.McpInvocationScope,
    ) => Effect.Effect<void, PeerSessionError>;
    readonly current: (
      invocation: Mcp.McpInvocationScope,
    ) => Effect.Effect<NativeSessionFacts, PeerSessionError>;
  }
>()("t3/jones/workstreams/native/runtime/peerSessionPins/NativePeerSessions") {}
const equal = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const rank = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };
export const ceilingAllows = (
  ceiling: WorkstreamsTransportCeiling,
  permission: "workstreams:read" | "workstreams:write",
) =>
  ceiling.permissions.includes(permission) &&
  ceiling.scopes.includes("orchestration:read") &&
  (permission === "workstreams:read" ||
    (ceiling.access !== "read-only" &&
      ceiling.interaction_mode !== "plan" &&
      ceiling.scopes.includes("orchestration:operate")));
const makeNative = Effect.gen(function* () {
  const auth = yield* Auth.EnvironmentAuth,
    sessions = yield* Sessions.AuthSessionRepository;
  const admitted = new WeakMap<
    Mcp.McpInvocationScope,
    { snapshot: string; session: Pin["session"] }
  >();
  const read = Effect.fn("NativePeerSessions.read")(function* (
    scope: Mcp.McpInvocationScope,
    access: Pin["session"]["ceiling"]["access"],
  ) {
    const id = yield* decodeSessionId(scope.client?.sessionId).pipe(
      Effect.mapError(() => new PeerSessionError({ code: "unauthenticated" })),
    );
    const row = yield* sessions.getById({ sessionId: id }).pipe(Effect.mapError(mapFailure));
    const now = DateTime.toEpochMillis(yield* DateTime.now);
    if (
      Option.isNone(row) ||
      row.value.revokedAt !== null ||
      row.value.issuedAt.epochMilliseconds > now ||
      row.value.expiresAt.epochMilliseconds <= now ||
      row.value.method !== "bearer-access-token" ||
      !row.value.scopes.includes("orchestration:read")
    )
      return yield* new PeerSessionError({ code: "not_current" });
    const r = row.value,
      modes = Mcp.clientModeCeiling(scope.client);
    const write =
      access !== "read-only" &&
      r.scopes.includes("orchestration:operate") &&
      modes.interactionMode !== "plan";
    return {
      session_id: id,
      subject: r.subject,
      method: r.method,
      issued_at: DateTime.formatIso(r.issuedAt),
      expires_at: DateTime.formatIso(r.expiresAt),
      peer_link: true as const,
      ceiling: {
        access: write ? access : ("read-only" as const),
        runtime_mode: write ? modes.runtimeMode : ("approval-required" as const),
        interaction_mode: modes.interactionMode,
        scopes: r.scopes,
        permissions: write
          ? (["workstreams:read", "workstreams:write"] as const)
          : (["workstreams:read"] as const),
      },
    };
  });
  const authenticate = Effect.fn("NativePeerSessions.authenticate")(function* (
    request: Parameters<Auth.EnvironmentAuth["Service"]["authenticateMcpClient"]>[0],
    scope: Mcp.McpInvocationScope,
  ) {
    admitted.delete(scope);
    const native = yield* auth.authenticateMcpClient(request).pipe(Effect.mapError(mapFailure));
    if (
      scope.thread ||
      !scope.client?.linked ||
      !native.peerLink ||
      native.sessionId !== scope.client.sessionId ||
      native.access !== scope.client.access ||
      !scope.capabilities.has("orchestration")
    )
      return yield* new PeerSessionError({ code: "unauthenticated" });
    const session = yield* read(scope, native.access);
    admitted.set(scope, { snapshot: canonical(scopeSnapshot(scope)), session });
  });
  const current = Effect.fn("NativePeerSessions.current")(function* (
    scope: Mcp.McpInvocationScope,
  ) {
    const evidence = admitted.get(scope);
    if (!evidence || evidence.snapshot !== canonical(scopeSnapshot(scope)))
      return yield* new PeerSessionError({ code: "unauthenticated" });
    const checkCurrent = Effect.fn("NativePeerSessions.checkCurrent")(function* () {
      if (admitted.get(scope) !== evidence || evidence.snapshot !== canonical(scopeSnapshot(scope)))
        return yield* new PeerSessionError({ code: "unauthenticated" });
      const now = yield* read(scope, evidence.session.ceiling.access);
      if (
        !equal({ ...now, ceiling: null }, { ...evidence.session, ceiling: null }) ||
        now.ceiling.scopes.some((s) => !evidence.session.ceiling.scopes.includes(s))
      )
        return yield* new PeerSessionError({ code: "not_current" });
      return now;
    });
    const session = yield* checkCurrent();
    return { session, checkCurrent };
  });
  return NativePeerSessions.of({ authenticate, current });
});
const scopeSnapshot = (s: Mcp.McpInvocationScope) => ({
  environmentId: s.environmentId,
  capabilities: [...s.capabilities].sort(),
  issuedAt: s.issuedAt,
  requestNamespace: s.requestNamespace,
  thread: s.thread ?? null,
  client: s.client ?? null,
});
/** @public Mount once alongside native MCP authentication; never as a default invocation. */
export const nativeLayer = Layer.effect(NativePeerSessions, makeNative);

export interface QualifiedPeerPin {
  readonly pin: Pin;
  readonly ceiling: WorkstreamsTransportCeiling;
  readonly checkCurrent: () => Effect.Effect<void, PeerSessionError>;
}
export interface PeerPinRoleSelection {
  readonly role: Pin["roles"][number];
  readonly setupOnlyPrepared?: boolean;
}
export class PeerSessionPins extends Context.Service<
  PeerSessionPins,
  {
    readonly forInvocation: (
      invocation: Mcp.McpInvocationScope,
      selection: PeerPinRoleSelection,
    ) => Effect.Effect<QualifiedPeerPin, PeerSessionError>;
  }
>()("t3/jones/workstreams/native/runtime/peerSessionPins") {}
const make = Effect.gen(function* () {
  const native = yield* NativePeerSessions,
    records = yield* Records.PeerSessionPinStore,
    authority = yield* Authority.NativeStoreAuthority,
    bindings = yield* Bindings.BindingStore;
  const forInvocation = Effect.fn("PeerSessionPins.forInvocation")(function* (
    scope: Mcp.McpInvocationScope,
    selection: PeerPinRoleSelection,
  ) {
    const facts = yield* native.current(scope);
    const record = yield* records
      .read({ session_id: facts.session.session_id })
      .pipe(Effect.mapError(mapFailure));
    if (!record) return yield* new PeerSessionError({ code: "not_current" });
    const pin = record.pin;
    const checkCurrent = Effect.fn("PeerSessionPins.checkCurrent")(function* () {
      const current = yield* records.read({ pin_id: pin.pin_id }).pipe(Effect.mapError(mapFailure));
      const session = yield* facts.checkCurrent(),
        local = yield* authority.readCurrent.pipe(Effect.mapError(mapFailure));
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      if (
        !equal(session.ceiling, facts.session.ceiling) ||
        !current ||
        !equal(current.pin, pin) ||
        pin.state === "revoked" ||
        (pin.state === "prepared" && !selection.setupOnlyPrepared) ||
        Date.parse(pin.expires_at) <= now ||
        !equal({ ...session, ceiling: null }, { ...pin.session, ceiling: null }) ||
        session.ceiling.scopes.some((s) => !pin.session.ceiling.scopes.includes(s)) ||
        rank[session.ceiling.runtime_mode] > rank[pin.session.ceiling.runtime_mode] ||
        !pin.roles.includes(selection.role)
      )
        return yield* new PeerSessionError({ code: "not_current" });
      if (
        scope.environmentId !== local.environmentId ||
        local.environmentId !== pin.receiver_authority.environment_id ||
        local.authorityNamespace !== pin.receiver_authority.authority_namespace ||
        local.storeGeneration !== pin.receiver_authority.store_generation
      )
        return yield* new PeerSessionError({ code: "authority_mismatch" });
      if (selection.role === "home_source") {
        if (
          !equal(pin.receiver_authority, pin.source_authority) ||
          pin.peer_environment_id !== pin.expected_home.home_environment_id
        )
          return yield* new PeerSessionError({ code: "wrong_role" });
        if (pin.state === "bound") {
          const binding = yield* bindings.read().pipe(Effect.mapError(mapFailure));
          if (
            !binding ||
            binding.state !== "active" ||
            binding.registry_instance_id !== pin.expected_home.registry_instance_id ||
            binding.owner_id !== pin.expected_home.owner_id ||
            binding.home_environment_id !== pin.expected_home.home_environment_id ||
            binding.home_generation !== pin.expected_home.server_generation ||
            binding.binding_version !== pin.singleton_binding_version ||
            binding.principal_id !== pin.bind_receipt?.principal_id
          )
            return yield* new PeerSessionError({ code: "not_current" });
        }
      } else if (
        pin.peer_environment_id !== pin.source_authority.environment_id ||
        pin.receiver_authority.environment_id !== pin.expected_home.home_environment_id
      )
        return yield* new PeerSessionError({ code: "wrong_role" });
    });
    yield* checkCurrent();
    const ceiling: WorkstreamsTransportCeiling = {
      ...facts.session.ceiling,
      permissions: facts.session.ceiling.permissions.filter((p) =>
        pin.session.ceiling.permissions.includes(p),
      ),
    };
    return { pin, ceiling, checkCurrent };
  });
  return PeerSessionPins.of({ forInvocation });
});
/** @public Canonical service construction. */
export const layer = Layer.effect(PeerSessionPins, make);
