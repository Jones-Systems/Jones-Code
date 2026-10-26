import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Mcp from "../../../../mcp/McpInvocationContext.ts";
import { canonicalJson } from "../domain/validation.ts";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Session from "../registry/session.ts";
import * as Database from "../store/sql.ts";
import * as Store from "../store/registryStore.ts";
import * as Guard from "./guard.ts";
import * as Protocol from "./homeReadProtocol.ts";

type Admission = Effect.Success<ReturnType<Protocol.HomePeerReadAdmission["Service"]["current"]>>;
type Authorization = Effect.Success<ReturnType<Session.Session["Service"]["authorize"]>>;
const denied = () => new Session.RegistryError({ code: "forbidden" });
const isRegistryError = Schema.is(Session.RegistryError);
const failure = (cause: unknown) =>
  isRegistryError(cause) ? cause : new Session.RegistryError({ code: "unavailable", cause });

/** Request-local read proofs; caller fields alone never establish peer authority. */
export class PeerReadBoundary extends Context.Service<
  PeerReadBoundary,
  {
    readonly current: () => Effect.Effect<Admission, Session.RegistryError>;
    readonly authorize: (
      caller: AdmittedCaller,
      permission: "workstreams:read" | "workstreams:write",
    ) => Effect.Effect<Authorization, Session.RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/peerReadAdmission/PeerReadBoundary") {}

const make = Effect.gen(function* () {
  const guard = yield* Guard.Guard;
  const home = yield* NativeHome;
  const db = yield* Database.NativeSql;
  const registry = yield* Store.RegistryStore;
  type GuardAdmission = Effect.Success<ReturnType<Guard.Guard["Service"]["authorize"]>>;
  const proofs = new WeakMap<
    AdmittedCaller,
    {
      readonly scope: Mcp.McpInvocationScope;
      readonly callerBytes: string;
      readonly admitted: GuardAdmission;
    }
  >();
  const authorize = Effect.fn("PeerReadBoundary.authorize")(
    function* (caller: AdmittedCaller, permission: "workstreams:read" | "workstreams:write") {
      const proof = proofs.get(caller);
      const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
      if (
        permission !== "workstreams:read" ||
        !proof ||
        Option.isNone(invocation) ||
        invocation.value !== proof.scope ||
        !invocation.value.client?.linked ||
        canonicalJson(caller) !== proof.callerBytes
      )
        return yield* denied();
      return yield* db.transaction(
        home,
        Effect.gen(function* () {
          const { admitted } = proof;
          yield* admitted.checkCurrent();
          const context = yield* registry.context(home);
          const pin = admitted.pin;
          if (
            !context ||
            context.state !== "active" ||
            context.registry_instance_id !== home.registry_instance_id ||
            context.owner_id !== home.owner_id ||
            context.home_environment_id !== home.home_environment_id ||
            home.served_by_environment_id !== home.home_environment_id ||
            proof.scope.environmentId !== home.served_by_environment_id ||
            context.server_generation !== home.server_generation ||
            context.canonical_origin !== home.canonical_origin ||
            canonicalJson(context) !== canonicalJson(admitted.context) ||
            pin.state !== "bound" ||
            !pin.roles.includes("member_home") ||
            pin.expected_home.registry_instance_id !== context.registry_instance_id ||
            pin.expected_home.owner_id !== context.owner_id ||
            pin.expected_home.home_environment_id !== context.home_environment_id ||
            pin.expected_home.server_generation !== context.server_generation ||
            pin.peer_environment_id !== caller.source_environment_id ||
            pin.source_authority.environment_id !== caller.source_environment_id ||
            pin.source_binding_version !== caller.binding_version ||
            pin.bind_receipt?.principal_id !== caller.principal_id ||
            admitted.grant.owner_id !== context.owner_id ||
            admitted.grant.principal_id !== caller.principal_id ||
            admitted.grant.state !== "active" ||
            !admitted.grant.permissions.includes("workstreams:read")
          )
            return yield* denied();
          yield* admitted.checkCurrent();
          return {
            context,
            authorization: {
              owner_id: context.owner_id,
              actor: { principal_id: caller.principal_id },
              grant: admitted.grant,
              server_generation: context.server_generation,
            },
          };
        }),
      );
    },
    (effect) => effect.pipe(Effect.mapError(failure)),
  );
  const current = Effect.fn("PeerReadBoundary.current")(function* () {
    const invocation = yield* Effect.serviceOption(Mcp.McpInvocationContext);
    if (Option.isNone(invocation) || !invocation.value.client?.linked) return yield* denied();
    const admitted = yield* guard.authorize("workstreams:read");
    const caller = admitted.caller;
    Object.freeze(caller.origin);
    Object.freeze(caller);
    proofs.set(caller, { scope: invocation.value, callerBytes: canonicalJson(caller), admitted });
    const checkCurrent = Effect.fn("PeerReadBoundary.checkCurrent")(function* () {
      yield* authorize(caller, "workstreams:read");
    });
    yield* checkCurrent();
    return Object.freeze({
      caller,
      authorization_revision: admitted.grant.grant_version,
      checkCurrent,
    });
  });
  return PeerReadBoundary.of({ current, authorize });
});

/** @public Root constructs once with the genuine Guard and native home transaction. */
export const layer = Layer.effect(PeerReadBoundary, make);
/** @public Coherent native reads consume this port from the same boundary instance. */
export const homeAdmissionLayer = Layer.unwrap(
  Effect.sync(() =>
    Layer.effect(
      Protocol.HomePeerReadAdmission,
      Effect.gen(function* () {
        const boundary = yield* PeerReadBoundary;
        return Protocol.HomePeerReadAdmission.of({ current: boundary.current });
      }),
    ),
  ),
);
/** @public Wrap the existing local Session before constructing every read consumer. */
export const sessionLayer = Layer.effect(
  Session.Session,
  Effect.gen(function* () {
    const local = yield* Session.Session;
    const boundary = yield* PeerReadBoundary;
    return Session.Session.of({
      authorize: Effect.fn("PeerReadBoundary.session")(function* (caller, permission) {
        const scope = yield* Effect.serviceOption(Mcp.McpInvocationContext);
        if (Option.isNone(scope) || !scope.value.client?.linked)
          return yield* local.authorize(caller, permission);
        return yield* boundary.authorize(caller, permission);
      }),
    });
  }),
);
