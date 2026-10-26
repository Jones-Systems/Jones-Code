import { AuthOrchestrationReadScope, AuthOrchestrationOperateScope } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import { WorkstreamOperationOrigin } from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { canonicalJson } from "../../operations/encoding.ts";
import type { CallerEvidence, Permission } from "./admission.ts";
import * as Source from "../providers/localAttestation.ts";

const Id = Schema.NonEmptyString;
const Modes = Schema.Struct({
  runtimeMode: Schema.Literals(["approval-required", "auto-accept-edits", "auto", "full-access"]),
  interactionMode: Schema.Literals(["plan", "default"]),
});
const Ceiling = Schema.Struct({
  permissions: Schema.Array(Schema.Literals(["workstreams:read", "workstreams:write"])),
  scopes: Schema.Array(Id),
  capabilities: Schema.Array(Id),
  limits: Schema.NullOr(Modes),
});
const Bearer = Schema.Struct({
  kind: Schema.Literal("bearer"),
  session_id: Id,
  subject: Id,
  method: Id,
});
const McpClient = Schema.Struct({
  kind: Schema.Literal("mcp_client"),
  session_id: Id,
  subject: Schema.Literal("mcp-client"),
  method: Schema.Literal("bearer-access-token"),
  approved_access: Schema.Literals([
    "read-only",
    "approval-required",
    "auto-accept-edits",
    "auto",
    "full-access",
  ]),
  requested_limits: Schema.NullOr(Modes),
});
const Agent = Schema.Struct({
  kind: Schema.Literal("agent"),
  calling_thread_id: Id,
  provider_instance_id: Id,
  mcp_credential_id: Id,
  run_id: Id,
  provider_thread_id: Id,
  provider_session_id: Id,
});
const TrustedCli = Schema.Struct({
  kind: Schema.Literal("trusted_cli"),
  authority_id: Id,
  session_id: Id,
});
export const RecordedCallerV1 = Schema.Struct({
  version: Schema.Literal(1),
  origin: WorkstreamOperationOrigin,
  auth: Schema.Union([Bearer, McpClient, Agent, TrustedCli]),
  source_environment_id: Id,
  registry_principal_id: Id,
  singleton_binding_version: Schema.Number,
  grant_version: Schema.Number,
  source: Schema.Struct({
    environment_id: Id,
    authority_namespace: Id,
    store_generation: Schema.Number,
    binding_version: Schema.Number,
    enrollment_id: Id,
    enrollment_session_id: Id,
  }),
  destination: Schema.Struct({
    registry_instance_id: Id,
    owner_id: Id,
    home_environment_id: Id,
    home_generation: Schema.Number,
    principal_id: Id,
    binding_version: Schema.Number,
  }),
  ceiling: Ceiling,
  captured_at: Schema.Number,
});
export type RecordedCallerV1 = typeof RecordedCallerV1.Type;
export class RecordedCallerFence extends Schema.TaggedError<RecordedCallerFence>()(
  "RecordedCallerFence",
  {
    code: Schema.Literals([
      "missing",
      "malformed",
      "identity_changed",
      "not_granted",
      "unavailable",
    ]),
  },
) {}

const CurrentCallerState = Schema.Struct({
  admission: RecordedCallerV1,
  registry_active: Schema.Boolean,
  singleton_active: Schema.Boolean,
  source_binding_active: Schema.Boolean,
  grant: Schema.Struct({
    active: Schema.Boolean,
    version: Schema.Number,
    permissions: Ceiling.fields.permissions,
  }),
  // Native source setup supplies this through its validated lookup adapter.
  enrollment: Schema.NullOr(
    Schema.Struct({
      enrollment_id: Id,
      session_id: Id,
      active: Schema.Boolean,
      issued_at: Schema.Number,
      expires_at: Schema.Number,
    }),
  ),
  session: Schema.NullOr(
    Schema.Struct({
      session_id: Id,
      subject: Id,
      method: Id,
      scopes: Schema.Array(Id),
      issued_at: Schema.Number,
      expires_at: Schema.Number,
      revoked: Schema.Boolean,
    }),
  ),
  credential: Schema.NullOr(
    Schema.Struct({
      credential_id: Id,
      calling_thread_id: Id,
      provider_instance_id: Id,
      active: Schema.Boolean,
    }),
  ),
  run: Schema.NullOr(
    Schema.Struct({
      run_id: Id,
      calling_thread_id: Id,
      provider_instance_id: Id,
      provider_thread_id: Id,
      provider_session_id: Id,
      active: Schema.Boolean,
    }),
  ),
  trusted_cli_authority_id: Schema.NullOr(Id),
});
export type CurrentCallerState = typeof CurrentCallerState.Type;

/** Application adapters read authoritative records, never tokens or config-derived authority.
 * Missing memory-only credentials or original run evidence must return null.
 * Native enrollment lookup is injected by the source-setup composition.
 */
export class CallerAuthority extends Context.Service<
  CallerAuthority,
  {
    readonly capture: () => Effect.Effect<unknown, RecordedCallerFence>;
    readonly refresh: (
      record: RecordedCallerV1,
    ) => Effect.Effect<CurrentCallerState, RecordedCallerFence>;
    readonly checkTarget: (
      record: RecordedCallerV1,
      ceiling: RecordedCallerV1["ceiling"],
      permission: Permission,
      threadId: string | null,
    ) => Effect.Effect<void, RecordedCallerFence>;
  }
>()("t3/jones/workstreams/native/runtime/recordedCaller/CallerAuthority") {}

export class RecoveryCaller extends Context.Service<RecoveryCaller, CallerEvidence>()(
  "t3/jones/workstreams/native/runtime/recordedCaller/RecoveryCaller",
) {}

/** Retains the original admission while a recovered operation forks or queues work. */
export const RecordedAdmission = Context.Reference<RecordedCallerV1 | null>(
  "t3/jones/workstreams/native/runtime/recordedCaller/RecordedAdmission",
  { defaultValue: () => null },
);
const decode = Schema.decodeUnknownEffect(RecordedCallerV1, { onExcessProperty: "error" });
const decodeCurrent = Schema.decodeUnknownEffect(CurrentCallerState, { onExcessProperty: "error" });
export const decodeRecordedCaller = Effect.fn("RecordedCaller.decode")(function* (input: unknown) {
  const value = yield* decode(input).pipe(
    Effect.mapError(() => new RecordedCallerFence({ code: "malformed" })),
  );
  const versions = [
    value.singleton_binding_version,
    value.grant_version,
    value.source.store_generation,
    value.source.binding_version,
    value.destination.home_generation,
    value.destination.binding_version,
  ];
  if (
    versions.some((n) => !Number.isSafeInteger(n) || n < 1) ||
    !Number.isFinite(value.captured_at) ||
    value.captured_at < 0 ||
    value.source_environment_id !== value.source.environment_id ||
    value.registry_principal_id !== value.destination.principal_id ||
    value.singleton_binding_version !== value.destination.binding_version ||
    ((value.auth.kind === "agent" || value.auth.kind === "mcp_client") && !value.ceiling.limits)
  )
    return yield* new RecordedCallerFence({ code: "malformed" });
  return value;
});

const immutable = (value: RecordedCallerV1) => ({
  origin: value.origin,
  // The current approval may narrow. Session and requested narrowing remain pinned.
  auth:
    value.auth.kind === "mcp_client" ? { ...value.auth, approved_access: undefined } : value.auth,
  source_environment_id: value.source_environment_id,
  registry_principal_id: value.registry_principal_id,
  singleton_binding_version: value.singleton_binding_version,
  source: value.source,
  destination: value.destination,
});
export const sameRecordedCaller = (
  a: RecordedCallerV1 | null | undefined,
  b: RecordedCallerV1 | null | undefined,
) =>
  a == null || b == null
    ? a == null && b == null
    : canonicalJson(immutable(a)) === canonicalJson(immutable(b));

const ranks = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };
const intersect = (
  original: RecordedCallerV1["ceiling"],
  current: RecordedCallerV1["ceiling"],
): RecordedCallerV1["ceiling"] => ({
  permissions: original.permissions.filter((p) => current.permissions.includes(p)),
  scopes: original.scopes.filter((p) => current.scopes.includes(p)),
  capabilities: original.capabilities.filter((p) => current.capabilities.includes(p)),
  limits:
    original.limits === null
      ? current.limits
      : current.limits === null
        ? original.limits
        : {
            runtimeMode:
              ranks[original.limits.runtimeMode] <= ranks[current.limits.runtimeMode]
                ? original.limits.runtimeMode
                : current.limits.runtimeMode,
            interactionMode:
              original.limits.interactionMode === "plan" ||
              current.limits.interactionMode === "plan"
                ? "plan"
                : "default",
          },
});
export class RecordedCallers extends Context.Service<
  RecordedCallers,
  {
    readonly captureCaller: () => Effect.Effect<RecordedCallerV1, RecordedCallerFence>;
    readonly readmitRecorded: (
      record: unknown,
    ) => Effect.Effect<CallerEvidence, RecordedCallerFence>;
    readonly visible: (
      record: RecordedCallerV1 | null | undefined,
      threadId: string | null,
    ) => Effect.Effect<boolean>;
  }
>()("t3/jones/workstreams/native/runtime/recordedCaller/RecordedCallers") {}
const make = Effect.gen(function* () {
  const authority = yield* CallerAuthority;
  const refresh = Effect.fn("RecordedCaller.refresh")(function* (record: RecordedCallerV1) {
    const state = yield* decodeCurrent(yield* authority.refresh(record)).pipe(
      Effect.mapError(() => new RecordedCallerFence({ code: "malformed" })),
    );
    const current = yield* decodeRecordedCaller(state.admission);
    if (!sameRecordedCaller(record, current))
      return yield* new RecordedCallerFence({ code: "identity_changed" });
    const now = yield* Clock.currentTimeMillis;
    const validTime = (value: { issued_at: number; expires_at: number }) =>
      Number.isFinite(value.issued_at) &&
      Number.isFinite(value.expires_at) &&
      value.issued_at <= now &&
      now < value.expires_at;
    if (
      !state.registry_active ||
      !state.singleton_active ||
      !state.source_binding_active ||
      !state.grant.active ||
      state.grant.version !== current.grant_version ||
      !state.enrollment?.active ||
      !validTime(state.enrollment) ||
      state.enrollment.enrollment_id !== record.source.enrollment_id ||
      state.enrollment.session_id !== record.source.enrollment_session_id
    )
      return yield* new RecordedCallerFence({ code: "not_granted" });
    let ceiling = intersect(record.ceiling, current.ceiling);
    ceiling = {
      ...ceiling,
      permissions: ceiling.permissions.filter((p) => state.grant.permissions.includes(p)),
    };
    const auth = record.auth;
    if (auth.kind === "agent") {
      const credential = state.credential,
        run = state.run;
      if (
        !credential?.active ||
        credential.credential_id !== auth.mcp_credential_id ||
        credential.calling_thread_id !== auth.calling_thread_id ||
        credential.provider_instance_id !== auth.provider_instance_id ||
        !run?.active ||
        run.run_id !== auth.run_id ||
        run.calling_thread_id !== auth.calling_thread_id ||
        run.provider_instance_id !== auth.provider_instance_id ||
        run.provider_thread_id !== auth.provider_thread_id ||
        run.provider_session_id !== auth.provider_session_id
      )
        return yield* new RecordedCallerFence({ code: "not_granted" });
    } else {
      const session = state.session;
      if (
        !session ||
        session.revoked ||
        !validTime(session) ||
        session.session_id !== auth.session_id ||
        (auth.kind !== "trusted_cli" &&
          (session.subject !== auth.subject || session.method !== auth.method)) ||
        (auth.kind === "trusted_cli" && state.trusted_cli_authority_id !== auth.authority_id)
      )
        return yield* new RecordedCallerFence({ code: "not_granted" });
      ceiling = {
        ...ceiling,
        scopes: ceiling.scopes.filter((scope) => session.scopes.includes(scope)),
        permissions: ceiling.permissions.filter(
          (p) =>
            session.scopes.includes(
              p === "workstreams:write"
                ? AuthOrchestrationOperateScope
                : AuthOrchestrationReadScope,
            ) &&
            ceiling.scopes.includes(
              p === "workstreams:write"
                ? AuthOrchestrationOperateScope
                : AuthOrchestrationReadScope,
            ),
        ),
      };
      if (auth.kind === "mcp_client") {
        const approved =
          current.auth.kind === "mcp_client" ? current.auth.approved_access : "read-only";
        if (auth.approved_access === "read-only" || approved === "read-only")
          ceiling = {
            ...ceiling,
            permissions: ceiling.permissions.filter((p) => p !== "workstreams:write"),
          };
        const mode = {
          runtimeMode: approved === "read-only" ? ("approval-required" as const) : approved,
          interactionMode: "default" as const,
        };
        ceiling = intersect(ceiling, { ...ceiling, limits: mode });
        if (auth.requested_limits)
          ceiling = intersect(ceiling, { ...ceiling, limits: auth.requested_limits });
      }
    }
    return ceiling;
  });
  const captureCaller = Effect.fn("RecordedCaller.captureCaller")(function* () {
    const retained = yield* RecordedAdmission;
    if (retained) {
      yield* refresh(retained);
      return retained;
    }
    const record = yield* decodeRecordedCaller(yield* authority.capture());
    yield* refresh(record);
    return record;
  });
  const readmitRecorded = Effect.fn("RecordedCaller.readmitRecorded")(function* (input: unknown) {
    if (input == null) return yield* new RecordedCallerFence({ code: "missing" });
    const record = yield* decodeRecordedCaller(input);
    yield* refresh(record);
    return {
      origin: record.origin,
      check: Effect.fn("RecordedCaller.check")(
        function* (permission, threadId) {
          const ceiling = yield* refresh(record);
          if (
            !ceiling.permissions.includes(permission) ||
            (permission === "workstreams:write" && ceiling.limits?.interactionMode === "plan") ||
            ((record.auth.kind === "agent" || record.auth.kind === "mcp_client") &&
              !ceiling.capabilities.includes("orchestration"))
          )
            return yield* new RecordedCallerFence({ code: "not_granted" });
          yield* authority.checkTarget(record, ceiling, permission, threadId);
        },
        Effect.mapError(() => new Source.ProviderError({ code: "not_granted" })),
      ),
    } satisfies CallerEvidence;
  });
  const visible = Effect.fn("RecordedCaller.visible")(
    function* (record: RecordedCallerV1 | null | undefined, threadId: string | null) {
      if (!record) return false;
      const current = yield* captureCaller();
      if (!sameRecordedCaller(record, current)) return false;
      const caller = yield* readmitRecorded(record);
      yield* caller.check("workstreams:read", threadId);
      return true;
    },
    Effect.catch(() => Effect.succeed(false)),
  );
  return RecordedCallers.of({ captureCaller, readmitRecorded, visible });
});
export const layer = Layer.effect(RecordedCallers, make);

/** A queue item must provide its own caller instead of inheriting the drainer's. */
export const withRecordedCaller = <A, E, R>(
  record: RecordedCallerV1,
  caller: CallerEvidence,
  effect: Effect.Effect<A, E, R>,
) =>
  effect.pipe(
    Effect.provideService(RecoveryCaller, caller),
    Effect.provideService(RecordedAdmission, record),
  );

// The storage wrapper is outside the original reservation. Hashes of the
// unwrapped intent and original command body retain their existing meaning.
const StoredEffectIntent = Schema.Struct({
  format: Schema.Literal("jones-native-effect-recovery/1"),
  intent_body: Schema.String,
  request_body: Schema.String,
  request_sha256: Schema.String,
  recorded_caller: RecordedCallerV1,
});
const decodeEffectIntent = Schema.decodeUnknownEffect(StoredEffectIntent, {
  onExcessProperty: "error",
});
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
export const wrapEffectIntent = Effect.fn("RecordedCaller.wrapEffectIntent")(function* (
  intentBody: string,
  requestBody: string,
  requestSha256: string,
  record: RecordedCallerV1,
) {
  yield* decodeJson(intentBody).pipe(
    Effect.mapError(() => new RecordedCallerFence({ code: "malformed" })),
  );
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(requestBody)).pipe(
    Effect.map(Hex.encode),
    Effect.mapError(() => new RecordedCallerFence({ code: "unavailable" })),
  );
  if (digest !== requestSha256) return yield* new RecordedCallerFence({ code: "malformed" });
  return {
    format: "jones-native-effect-recovery/1" as const,
    intent_body: intentBody,
    request_body: requestBody,
    request_sha256: requestSha256,
    recorded_caller: yield* decodeRecordedCaller(record),
  };
});
export const unwrapEffectIntent = Effect.fn("RecordedCaller.unwrapEffectIntent")(function* (
  stored: unknown,
) {
  if (typeof stored !== "object" || stored === null || !("format" in stored))
    return { intent: stored, recovery: null };
  const value = yield* decodeEffectIntent(stored).pipe(
    Effect.mapError(() => new RecordedCallerFence({ code: "malformed" })),
  );
  // Recheck exact request bytes as well as the closed admission on every read.
  yield* wrapEffectIntent(
    value.intent_body,
    value.request_body,
    value.request_sha256,
    value.recorded_caller,
  );
  const intent = yield* decodeJson(value.intent_body).pipe(
    Effect.mapError(() => new RecordedCallerFence({ code: "malformed" })),
  );
  return { intent, recovery: value };
});
