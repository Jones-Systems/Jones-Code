import * as Wire from "@t3tools/contracts";
import type {
  WorkstreamsNativeHomeTarget,
  WorkstreamsNativeRelay,
} from "@t3tools/contracts/jones/workstreamsNative";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Store from "../store/registryStore.ts";
import * as Database from "../store/sql.ts";
import * as Session from "../registry/session.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import type { WorkstreamsPeerSessionPinV1 } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";

/** Supplied only by the peer authenticator after resolving an owner-approved session binding.
 * Neither a tool argument, a link label nor remoteOrigin establishes this identity.
 */
export class PeerLinkAuthContext extends Context.Service<
  PeerLinkAuthContext,
  {
    readonly session_id: string;
    readonly environment_id: string;
    readonly expected_home: WorkstreamsNativeHomeTarget;
    readonly pin_id: string;
    readonly pin_revision: number;
    readonly checkCurrent: () => Effect.Effect<void, Session.RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/guard/PeerLinkAuthContext") {}
export const targetOf = (home: WorkstreamsNativeHomeTarget): WorkstreamsNativeHomeTarget => ({
  registry_instance_id: home.registry_instance_id,
  owner_id: home.owner_id,
  home_environment_id: home.home_environment_id,
  server_generation: home.server_generation,
});
export const assertTarget = (
  actual: WorkstreamsNativeHomeTarget,
  expected: WorkstreamsNativeHomeTarget,
): Effect.Effect<void, Session.RegistryError> => {
  if (
    actual.registry_instance_id !== expected.registry_instance_id ||
    actual.owner_id !== expected.owner_id ||
    actual.home_environment_id !== expected.home_environment_id
  )
    return Effect.fail(new Session.RegistryError({ code: "registry_mismatch" }));
  return actual.server_generation === expected.server_generation
    ? Effect.void
    : Effect.fail(new Session.RegistryError({ code: "generation_mismatch" }));
};
export class Guard extends Context.Service<
  Guard,
  {
    readonly authorize: (
      permission: "workstreams:read" | "workstreams:write",
      relay?: WorkstreamsNativeRelay,
      allowUnbound?: boolean,
    ) => Effect.Effect<
      {
        readonly caller: AdmittedCaller;
        readonly context: Store.RegistryContext;
        readonly grant: typeof Wire.WorkstreamOwnerGrant.Type;
        readonly pin: WorkstreamsPeerSessionPinV1;
        readonly checkCurrent: () => Effect.Effect<void, Session.RegistryError>;
      },
      Session.RegistryError
    >;
  }
>()("t3/jones/workstreams/native/forwarding/guard") {}
const denied = () => new Session.RegistryError({ code: "not_granted" });
/** @public Canonical Guard construction. */
export const layer = Layer.effect(
  Guard,
  Effect.gen(function* () {
    const home = yield* NativeHome,
      store = yield* Store.RegistryStore,
      db = yield* Database.NativeSql,
      pins = yield* Pins.PeerSessionPins;
    return Guard.of({
      authorize: Effect.fn("ForwardGuard.authorize")(
        function* (permission, relay, allowUnbound = false) {
          const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
          if (Option.isNone(invocation)) return yield* denied();
          const scope = invocation.value;
          const qualified = yield* pins
            .forInvocation(scope, { role: "member_home", setupOnlyPrepared: allowUnbound })
            .pipe(Effect.mapError(denied));
          const pin = qualified.pin;
          if (
            home.served_by_environment_id !== home.home_environment_id ||
            scope.environmentId !== home.served_by_environment_id ||
            !Pins.ceilingAllows(qualified.ceiling, permission) ||
            (pin.state === "prepared" && (!allowUnbound || permission !== "workstreams:read"))
          )
            return yield* denied();
          yield* assertTarget(pin.expected_home, targetOf(home));
          if (relay && (relay.hop !== 1 || relay.source_environment_id !== pin.peer_environment_id))
            return yield* new Session.RegistryError({
              code: relay.hop !== 1 ? "hop_limit" : "foreign_thread_claim",
            });
          if (
            relay?.linked_origin_environment_id !== null &&
            relay?.linked_origin_environment_id !== undefined &&
            relay.origin.kind !== "agent"
          )
            return yield* denied();
          // The legacy authenticator context is optional attribution only, never an authority fallback.
          const legacy = yield* Effect.serviceOption(PeerLinkAuthContext);
          if (
            Option.isSome(legacy) &&
            (legacy.value.session_id !== pin.session.session_id ||
              legacy.value.environment_id !== pin.peer_environment_id ||
              legacy.value.pin_id !== pin.pin_id ||
              legacy.value.pin_revision !== pin.revision ||
              canonical(legacy.value.expected_home) !== canonical(pin.expected_home))
          )
            return yield* denied();
          const readHome = Effect.fn("ForwardGuard.readHome")(function* () {
            yield* qualified.checkCurrent().pipe(Effect.mapError(denied));
            const context = yield* store.context(home);
            if (!context || context.state !== "active")
              return yield* new Session.RegistryError({
                code: context?.state === "importing" ? "import_incomplete" : "fenced",
              });
            yield* assertTarget(context, targetOf(home));
            const principals = (yield* store.listPlain(home, "principals")).filter(
              (p) => p.environment_id === pin.peer_environment_id && p.kind === "environment",
            );
            if (principals.length !== 1) return yield* denied();
            const principal = String(principals[0]!.principal_id);
            const row = (yield* store.listPlain(home, "grants"))
              .filter((g) => g.principal_id === principal)
              .toSorted((a, b) => Number(b.grant_version) - Number(a.grant_version))[0];
            if (!row) return yield* denied();
            const permissions = yield* decodeJsonWorkstreamOwnerGrantfieldspermissions(
              String(row.permissions),
            ).pipe(Effect.mapError(denied));
            const grant = yield* decodeWorkstreamOwnerGrant({
              ...row,
              owner_id: home.owner_id,
              permissions,
            }).pipe(Effect.mapError(denied));
            if (grant.state !== "active" || !grant.permissions.includes(permission))
              return yield* denied();
            const binding = (yield* store.listPlain(home, "source_bindings"))
              .filter(
                (b) =>
                  b.provider === "t3" &&
                  b.source_instance_id === pin.source_authority.environment_id,
              )
              .toSorted((a, b) => Number(b.binding_version) - Number(a.binding_version))[0];
            if (
              pin.state === "bound" &&
              (!binding ||
                binding.state !== "active" ||
                Number(binding.binding_version) !== pin.source_binding_version ||
                binding.authority_namespace !== pin.source_authority.authority_namespace ||
                Number(binding.store_generation) !== pin.source_authority.store_generation ||
                pin.bind_receipt?.principal_id !== principal ||
                context.registry_version < Number(pin.bind_receipt.registry_version))
            )
              return yield* denied();
            yield* qualified.checkCurrent().pipe(Effect.mapError(denied));
            return { context, grant, binding: binding ?? null, principal };
          });
          const first = yield* db
            .transaction(home, readHome())
            .pipe(Effect.mapError(mapRegistryFailure));
          const checkCurrent = Effect.fn("ForwardGuard.checkCurrent")(() =>
            db
              .transaction(
                home,
                Effect.gen(function* () {
                  const now = yield* readHome();
                  if (canonical(now) !== canonical(first)) return yield* denied();
                }),
              )
              .pipe(Effect.mapError(mapRegistryFailure)),
          );
          return {
            caller: {
              principal_id: first.principal,
              source_environment_id: pin.peer_environment_id,
              binding_version: pin.state === "bound" ? pin.source_binding_version! : 0,
              origin: relay?.origin ?? { kind: "agent" as const },
            },
            context: first.context,
            grant: first.grant,
            pin,
            checkCurrent,
          };
        },
        (effect) => effect.pipe(Effect.mapError(mapRegistryFailure)),
      ),
    });
  }),
);
const mapRegistryFailure = (cause: unknown) =>
  isRegistryError(cause) ? cause : new Session.RegistryError({ code: "unavailable", cause });
/** Wrap the existing local session at composition; linked requests can never fall back to it. */
/** @public Root composition wraps the local session exactly once. */
export const sessionLayer = Layer.effect(
  Session.Session,
  Effect.gen(function* () {
    const local = yield* Session.Session,
      guard = yield* Guard;
    return Session.Session.of({
      authorize: Effect.fn("ForwardGuard.session")(function* (caller, permission) {
        const scope = yield* Effect.serviceOption(Mcp.McpInvocationContext);
        if (Option.isNone(scope) || !scope.value.client?.linked)
          return yield* local.authorize(caller, permission);
        const admitted = yield* guard.authorize(permission);
        if (
          caller.principal_id !== admitted.caller.principal_id ||
          caller.source_environment_id !== admitted.caller.source_environment_id ||
          caller.binding_version !== admitted.caller.binding_version
        )
          return yield* denied();
        yield* admitted.checkCurrent();
        return {
          context: admitted.context,
          authorization: {
            owner_id: admitted.context.owner_id,
            actor: { principal_id: caller.principal_id },
            grant: admitted.grant,
            server_generation: admitted.context.server_generation,
          },
        };
      }),
    });
  }),
);

const decodeJsonWorkstreamOwnerGrantfieldspermissions = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Wire.WorkstreamOwnerGrant.fields.permissions),
);
const decodeWorkstreamOwnerGrant = Schema.decodeUnknownEffect(Wire.WorkstreamOwnerGrant);
const isRegistryError = Schema.is(Session.RegistryError);
