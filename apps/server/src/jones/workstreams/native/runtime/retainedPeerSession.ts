import type {
  WorkstreamsPeerSessionPinV1 as Pin,
  WorkstreamsTransportCeiling as Ceiling,
} from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Wire from "@t3tools/contracts";
import * as Sessions from "../../../../persistence/AuthSessions.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Records from "../store/peerSessionPinStore.ts";
import * as Registry from "../store/registryStore.ts";
import * as Origin from "./peerEffectOrigin.ts";
import * as Pins from "./peerSessionPins.ts";

export class RetainedPeerError extends Schema.TaggedError<RetainedPeerError>()(
  "RetainedPeerError",
  {
    code: Schema.Literals([
      "preparation_in_transaction",
      "not_current",
      "not_granted",
      "unavailable",
    ]),
  },
) {}
const isRetainedPeerError = Schema.is(RetainedPeerError);
const refused = (code: RetainedPeerError["code"] = "not_current") =>
  new RetainedPeerError({ code });
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const rank = { "approval-required": 0, "auto-accept-edits": 1, auto: 2, "full-access": 3 };
const lower = <A extends keyof typeof rank>(a: A, b: A): A => (rank[a] <= rank[b] ? a : b);
const narrow = (a: Ceiling, b: Ceiling): Ceiling => ({
  access:
    a.access === "read-only" || b.access === "read-only" ? "read-only" : lower(a.access, b.access),
  runtime_mode: lower(a.runtime_mode, b.runtime_mode),
  interaction_mode:
    a.interaction_mode === "plan" || b.interaction_mode === "plan" ? "plan" : "default",
  scopes: a.scopes.filter((s) => b.scopes.includes(s)),
  permissions: a.permissions.filter((p) => b.permissions.includes(p)),
});
const decodeGrant = Schema.decodeUnknownEffect(Wire.WorkstreamOwnerGrant);
const decodePermissions = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Wire.WorkstreamOwnerGrant.fields.permissions),
);
declare const retainedBrand: unique symbol;
export interface CurrentRetainedPeer {
  readonly [retainedBrand]: true;
  readonly pin: Pin;
  readonly ceiling: Ceiling;
  readonly grant: typeof Wire.WorkstreamOwnerGrant.Type;
  readonly context: Registry.RegistryContext;
}
interface Lifetime {
  active: boolean;
  readonly token: object;
  readonly check: () => Effect.Effect<void, RetainedPeerError>;
}
const lifetimes = new WeakMap<CurrentRetainedPeer, Lifetime>();
const RetainedScope = Context.Reference<object | undefined>(
  "jones/workstreams/native/runtime/CurrentRetainedPeerScope",
  { defaultValue: () => undefined },
);
export const checkRetainedPeer = Effect.fn("OriginalReceivingSession.check")(function* (
  proof: CurrentRetainedPeer,
) {
  const f = lifetimes.get(proof);
  if (!f?.active || (yield* RetainedScope) !== f.token) return yield* refused();
  yield* f.check();
});
/** Background row verification only. This service never authenticates or installs an MCP invocation. */
export class OriginalReceivingSession extends Context.Service<
  OriginalReceivingSession,
  {
    readonly withCurrent: <A, E, R>(
      origin: Origin.ValidatedPeerReservation,
      use: (proof: CurrentRetainedPeer) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | RetainedPeerError, R>;
  }
>()("t3/jones/workstreams/native/runtime/retainedPeerSession/OriginalReceivingSession") {}

const freeze = <A>(value: A): A => {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};
export const layer = Layer.effect(
  OriginalReceivingSession,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const records = yield* Records.PeerSessionPinStore;
    const sessions = yield* Sessions.AuthSessionRepository;
    const registry = yield* Registry.RegistryStore;
    const home = yield* NativeHome;
    const pinRow = (id: string) =>
      sql<
        Record<string, unknown>
      >`SELECT * FROM jones_workstreams_peer_session_pins WHERE pin_id=${id}`.pipe(
        Effect.map((rows) => rows[0] ?? null),
      );
    return OriginalReceivingSession.of({
      withCurrent: Effect.fn("OriginalReceivingSession.withCurrent")(function* <A, E, R>(
        origin: Origin.ValidatedPeerReservation,
        use: (proof: CurrentRetainedPeer) => Effect.Effect<A, E, R>,
      ) {
        const outcome = yield* Effect.gen(function* () {
          if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
            return yield* refused("preparation_in_transaction");
          yield* Origin.checkOrigin(origin).pipe(Effect.mapError(() => refused()));
          const recovery = origin.envelope.recovery;
          const identity = recovery.source_readmission.accepted_forward;
          const before = yield* pinRow(recovery.incoming_peer_pin_id);
          const record = yield* records.read({ pin_id: recovery.incoming_peer_pin_id });
          if (!before || !record) return yield* refused();
          const pin = record.pin;
          if (
            pin.pin_id !== recovery.incoming_peer_pin_id ||
            pin.revision !== recovery.incoming_peer_pin_revision ||
            pin.state !== "bound" ||
            !pin.roles.includes("member_home") ||
            !same(pin.expected_home, identity.expected_home) ||
            !same(pin.source_authority, identity.source_authority) ||
            pin.peer_environment_id !== identity.source_authority.environment_id ||
            pin.source_enrollment_id !== identity.source_enrollment_id ||
            pin.singleton_binding_version !== identity.singleton_binding_version ||
            pin.source_binding_version !== identity.source_binding_version ||
            pin.bind_receipt?.principal_id !== identity.principal_id ||
            pin.receiver_authority.environment_id !== home.home_environment_id
          )
            return yield* refused();
          const read = Effect.fn("OriginalReceivingSession.read")(function* () {
            yield* Origin.checkOriginReceiver(origin, pin.receiver_authority).pipe(
              Effect.mapError(() => refused()),
            );
            if (!same(yield* pinRow(pin.pin_id), before)) return yield* refused();
            const row = yield* Pins.readNativeSessionRow(pin.session.session_id).pipe(
              Effect.provideService(Sessions.AuthSessionRepository, sessions),
              Effect.mapError(() => refused()),
            );
            const now = DateTime.toEpochMillis(yield* DateTime.now);
            if (
              Date.parse(pin.expires_at) <= now ||
              row.subject !== pin.session.subject ||
              row.method !== pin.session.method ||
              DateTime.formatIso(row.issuedAt) !== pin.session.issued_at ||
              DateTime.formatIso(row.expiresAt) !== pin.session.expires_at ||
              row.scopes.some((s) => !pin.session.ceiling.scopes.includes(s))
            )
              return yield* refused();
            const ceiling = narrow(narrow(pin.session.ceiling, recovery.original_ceiling), {
              ...pin.session.ceiling,
              scopes: row.scopes,
              permissions: row.scopes.includes("orchestration:operate")
                ? ["workstreams:read", "workstreams:write"]
                : ["workstreams:read"],
            });
            if (!Pins.ceilingAllows(ceiling, "workstreams:write"))
              return yield* refused("not_granted");
            const context = yield* registry.context(home);
            if (
              !context ||
              context.state !== "active" ||
              context.home_environment_id !== home.home_environment_id ||
              home.served_by_environment_id !== home.home_environment_id ||
              context.server_generation !== home.server_generation ||
              context.canonical_origin !== identity.canonical_origin ||
              context.registry_version < Number(pin.bind_receipt?.registry_version)
            )
              return yield* refused("not_granted");
            const principals = (yield* registry.listPlain(home, "principals")).filter(
              (p) => p.kind === "environment" && p.environment_id === pin.peer_environment_id,
            );
            if (principals.length !== 1 || principals[0]!.principal_id !== identity.principal_id)
              return yield* refused("not_granted");
            const grantRow = (yield* registry.listPlain(home, "grants"))
              .filter((g) => g.principal_id === identity.principal_id)
              .toSorted((a, b) => Number(b.grant_version) - Number(a.grant_version))[0];
            if (!grantRow) return yield* refused("not_granted");
            const grant = yield* decodeGrant({
              ...grantRow,
              owner_id: home.owner_id,
              permissions: yield* decodePermissions(String(grantRow.permissions)),
            }).pipe(Effect.mapError(() => refused("not_granted")));
            if (grant.state !== "active" || !grant.permissions.includes("workstreams:write"))
              return yield* refused("not_granted");
            const binding = (yield* registry.listPlain(home, "source_bindings"))
              .filter(
                (b) =>
                  b.provider === "t3" &&
                  b.source_instance_id === pin.source_authority.environment_id,
              )
              .toSorted((a, b) => Number(b.binding_version) - Number(a.binding_version))[0];
            if (
              !binding ||
              binding.state !== "active" ||
              binding.binding_version !== identity.source_binding_version ||
              binding.authority_namespace !== pin.source_authority.authority_namespace ||
              binding.store_generation !== pin.source_authority.store_generation
            )
              return yield* refused("not_granted");
            return { context, grant, binding, ceiling };
          });
          const first = yield* sql
            .withTransaction(read())
            .pipe(
              Effect.mapError((error) =>
                isRetainedPeerError(error) ? error : refused("unavailable"),
              ),
            );
          const proof = freeze(
            structuredClone({
              pin,
              ceiling: first.ceiling,
              grant: first.grant,
              context: first.context,
            }),
          ) as CurrentRetainedPeer;
          const f: Lifetime = {
            active: true,
            token: Object.freeze({}),
            check: () =>
              read().pipe(
                Effect.flatMap((current) =>
                  same(current.grant, first.grant) &&
                  same(current.binding, first.binding) &&
                  same(current.ceiling, first.ceiling)
                    ? Effect.void
                    : Effect.fail(refused("not_granted")),
                ),
                Effect.mapError((error) =>
                  isRetainedPeerError(error) ? error : refused("unavailable"),
                ),
              ),
          };
          lifetimes.set(proof, f);
          return yield* use(proof).pipe(
            Effect.provideService(RetainedScope, f.token),
            Effect.ensuring(
              Effect.sync(() => {
                f.active = false;
              }),
            ),
            Effect.exit,
          );
        }).pipe(
          Effect.mapError((error) => (isRetainedPeerError(error) ? error : refused("unavailable"))),
        );
        return yield* Exit.isFailure(outcome)
          ? Effect.failCause(outcome.cause)
          : Effect.succeed(outcome.value);
      }),
    });
  }),
);
