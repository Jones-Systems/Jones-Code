import * as Wire from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import { decodeWorkstreamsHomeBindBody } from "@t3tools/contracts/jones/workstreamsHomeBind";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Client from "../../../../peer/PeerMcpClient.ts";
import { EnvironmentId } from "@t3tools/contracts";
import * as HomeWire from "@t3tools/contracts/jones/workstreamsHomeBind";
import { makeReservedBearerSessionMaterializer } from "../../../../auth/SessionStore.ts";
import { sha256EnrollmentBytes as sha } from "../../enrollment/request.ts";
import * as Source from "./sourceEnrollment.ts";
import * as Members from "./memberEnrollment.ts";
import * as Qualification from "./memberPeerPinQualification.ts";
import * as Records from "../store/memberEnrollmentStore.ts";
import * as Pins from "../store/peerSessionPinStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Database from "../store/sql.ts";
import * as Envelope from "../crypto/envelope.ts";
import * as Keys from "../crypto/keyProvider.ts";

export class MemberSetup extends Context.Service<
  MemberSetup,
  {
    readonly apply: (
      intent: Wire.WorkstreamsMemberEnrollmentIntentV1,
    ) => Effect.Effect<Wire.WorkstreamsMemberEnrollmentBoundV1, Members.MemberEnrollmentError>;
    readonly readback: (
      intent: Wire.WorkstreamsMemberEnrollmentIntentV1,
    ) => Effect.Effect<
      Wire.WorkstreamsMemberEnrollmentBoundV1 | null,
      Members.MemberEnrollmentError
    >;
  }
>()("t3/jones/workstreams/native/runtime/memberSetup") {}
const decodeUnknownEffectWireWorkstreamsMemberEnrollmentPreparedV1 = Schema.decodeUnknownEffect(
  Wire.WorkstreamsMemberEnrollmentPreparedV1,
);
const decodeUnknownEffectWireWorkstreamsMemberEnrollmentBoundV1 = Schema.decodeUnknownEffect(
  Wire.WorkstreamsMemberEnrollmentBoundV1,
);
const isMembersMemberEnrollmentError = Schema.is(Members.MemberEnrollmentError);
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const refuse = (code: Members.MemberEnrollmentError["code"]) =>
  new Members.MemberEnrollmentError({ code });
type Local =
  | Members.MemberEnrollments
  | Records.MemberEnrollmentStore
  | Pins.PeerSessionPinStore
  | Bindings.BindingStore
  | Database.NativeSql
  | Keys.WorkstreamsKeyProvider;
const decodePrepared = decodeUnknownEffectWireWorkstreamsMemberEnrollmentPreparedV1;
const decodeBound = decodeUnknownEffectWireWorkstreamsMemberEnrollmentBoundV1;
const decodeKeyId = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ key_id: Schema.String })),
);
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    keys = yield* Keys.WorkstreamsKeyProvider,
    sessions = yield* AuthSessions.AuthSessionRepository,
    secrets = yield* Secrets.ServerSecretStore,
    links = yield* Links.PeerLinks,
    client = yield* Client.PeerMcpClient,
    qualification = yield* Qualification.MemberPeerPinQualification;
  const issuer = yield* Effect.serviceOption(Source.SourceIssuer),
    owner = yield* Effect.serviceOption(Source.SourceSetupOwner);
  const dependencies = Layer.mergeAll(
    Layer.succeed(SqlClient.SqlClient, sql),
    Layer.succeed(AuthSessions.AuthSessionRepository, sessions),
    Layer.succeed(Secrets.ServerSecretStore, secrets),
    Layer.succeed(Links.PeerLinks, links),
    Option.isSome(issuer) ? Layer.succeed(Source.SourceIssuer, issuer.value) : Layer.empty,
    Option.isSome(owner) ? Layer.succeed(Source.SourceSetupOwner, owner.value) : Layer.empty,
  );
  const prepared = (intent: Wire.WorkstreamsMemberEnrollmentIntentV1) =>
    Effect.try({
      try: () => {
        const record = Records.validateMemberEnrollment({
          schema: Wire.WORKSTREAMS_MEMBER_ENROLLMENT_SCHEMA,
          intent,
          intent_sha256: sha(Wire.canonicalWorkstreamsMemberEnrollmentIntent(intent)),
          state: "prepared",
          revision: 1,
          home_result: null,
          local_binding: null,
        });
        return record;
      },
      catch: () => refuse("invalid_request"),
    }).pipe(
      Effect.flatMap(decodePrepared),
      Effect.mapError(() => refuse("invalid_request")),
    );
  // Preload actual key material outside SQL. Inside, activeKeyId remains a local SQL fence;
  // cache misses refuse instead of consulting external or interactive key storage.
  const local = Effect.fn("MemberSetup.local")(function* <A, E>(
    r: Wire.WorkstreamsMemberEnrollmentPreparedV1,
    effect: Effect.Effect<A, E, Local>,
  ) {
    const b = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body),
      scope = { ...b.expected_home, key_scope: "local" as const };
    const active = yield* keys.activeKeyId(scope);
    const rows = yield* sql<{
      envelope: string;
    }>`SELECT envelope FROM jones_workstreams_member_enrollments
      WHERE enrollment_id=${b.source.enrollment.enrollment_id} OR session_id=${b.source.enrollment.session.session_id}
        OR setup_request_id=${b.member_setup.setup_request_id}
        OR (registry_instance_id=${b.expected_home.registry_instance_id} AND owner_id=${b.expected_home.owner_id} AND bind_id=${b.bind_id})
      UNION ALL SELECT envelope FROM jones_workstreams_peer_session_pins
      WHERE pin_id=${b.member_setup.pin_id} OR session_id=${Pins.decodeSetupRequest(r.intent.member_setup_request_body).pin.session.session_id}
        OR setup_request_id=${b.member_setup.setup_request_id}`;
    const ids = new Set([active]);
    for (const row of rows) {
      const envelope = yield* decodeKeyId(row.envelope).pipe(
        Effect.mapError(() => refuse("key_unavailable")),
      );
      const id = envelope.key_id;
      ids.add(id);
    }
    const cache = new Map<string, Uint8Array>();
    for (const id of ids) cache.set(id, Uint8Array.from(yield* keys.get(id)));
    const unavailable = () => new Keys.KeyUnavailable({ reason: "active_key_missing" });
    const cached = Layer.succeed(Keys.WorkstreamsKeyProvider, {
      activeKeyId: (s) =>
        keys
          .activeKeyId(s)
          .pipe(
            Effect.flatMap((id) =>
              s.key_scope === "local" &&
              s.registry_instance_id === scope.registry_instance_id &&
              s.owner_id === scope.owner_id &&
              id === active
                ? Effect.succeed(id)
                : Effect.fail(unavailable()),
            ),
          ),
      get: (id) =>
        cache.has(id)
          ? Effect.succeed(Uint8Array.from(cache.get(id)!))
          : Effect.fail(unavailable()),
      indexKey: () => Effect.fail(unavailable()),
    });
    const db = Database.layer.pipe(
      Layer.provideMerge(Envelope.layer.pipe(Layer.provideMerge(cached))),
    );
    const stores = Layer.mergeAll(Records.layer, Pins.layer, Bindings.layer).pipe(
      Layer.provideMerge(db),
    );
    const runtime = Members.layer.pipe(Layer.provideMerge(stores), Layer.provide(dependencies));
    return yield* effect.pipe(Effect.provide(runtime));
  });
  const retained = Effect.fn("MemberSetup.retained")(function* (
    r: Wire.WorkstreamsMemberEnrollmentPreparedV1,
  ) {
    const store = yield* Records.MemberEnrollmentStore,
      b = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body);
    const values = yield* Effect.all([
      store.read({ enrollment_id: b.source.enrollment.enrollment_id }),
      store.read({ session_id: b.source.enrollment.session.session_id }),
      store.read({ setup_request_id: b.member_setup.setup_request_id }),
      store.read({
        registry_instance_id: b.expected_home.registry_instance_id,
        owner_id: b.expected_home.owner_id,
        bind_id: b.bind_id,
      }),
    ]);
    if (values.every((v) => v === null)) return null;
    const value = values[0];
    if (
      !value ||
      values.some((v) => !same(v, value)) ||
      value.intent_sha256 !== r.intent_sha256 ||
      !same(value.intent, r.intent)
    )
      return yield* refuse("issuance_conflict");
    return value;
  });
  const preparedLocal = Effect.fn("MemberSetup.preparedLocal")(function* (
    r: Wire.WorkstreamsMemberEnrollmentPreparedV1,
    requirePin: boolean,
  ) {
    const member = yield* Members.MemberEnrollments,
      pins = yield* Pins.PeerSessionPinStore,
      bindings = yield* Bindings.BindingStore;
    yield* member.ownerCurrent;
    yield* member.validateSource(r, true);
    yield* member.inspectSession(r);
    const b = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body),
      singleton = yield* bindings.read();
    if (
      (singleton?.binding_version ?? null) !== b.member_setup.expected_singleton_version ||
      (singleton &&
        (singleton.registry_instance_id !== b.expected_home.registry_instance_id ||
          singleton.owner_id !== b.expected_home.owner_id ||
          singleton.state !== "active")) ||
      (singleton &&
        !b.rebind &&
        (singleton.home_environment_id !== b.expected_home.home_environment_id ||
          singleton.home_generation !== b.expected_home.server_generation ||
          singleton.principal_id !== b.expected.principal_id))
    )
      return yield* refuse("version_conflict");
    const expected = Pins.decodeSetupRequest(r.intent.member_setup_request_body).pin;
    const values = yield* Effect.all([
      pins.read({ pin_id: expected.pin_id }),
      pins.read({ session_id: expected.session.session_id }),
      pins.read({ setup_request_id: expected.setup_request_id }),
    ]);
    if (values.every((v) => v === null) && !requirePin) return false;
    if (
      values.some(
        (v) =>
          !v || v.request_body !== r.intent.member_setup_request_body || !same(v.pin, expected),
      )
    )
      return yield* refuse("pin_mismatch");
    return true;
  });
  const run = Effect.fn("MemberSetup.run")(function* (
    input: Wire.WorkstreamsMemberEnrollmentIntentV1,
    apply: boolean,
  ) {
    const r = yield* prepared(input),
      b = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body);
    const q = yield* qualification.qualify(r.intent).pipe(Effect.mapError(Members.memberFailure));
    // Inspection and external authentication precede the writer, including replay.
    const existing = yield* local(r, retained(r)).pipe(Effect.mapError(Members.memberFailure));
    if (existing?.state === "bound") {
      return yield* local(
        r,
        Effect.gen(function* () {
          const member = yield* Members.MemberEnrollments;
          yield* member.ownerCurrent;
          yield* member.validateLocal(existing);
          yield* qualification.checkCurrent(q).pipe(Effect.mapError(Members.memberFailure));
          return existing;
        }),
      );
    }
    if (!existing && !apply) return null;
    if (!existing) {
      yield* qualification.verifyHomeSetup(q).pipe(Effect.mapError(Members.memberFailure));
      // Signing-key acquisition is outside the writer; no bearer is materialized or forwarded.
      yield* makeReservedBearerSessionMaterializer.pipe(
        Effect.provideService(AuthSessions.AuthSessionRepository, sessions),
        Effect.provideService(Secrets.ServerSecretStore, secrets),
        Effect.mapError(() => refuse("key_unavailable")),
      );
    }
    const created = yield* local(
      r,
      Effect.gen(function* () {
        const db = yield* Database.NativeSql;
        return yield* db.transaction(
          b.expected_home,
          Effect.gen(function* () {
            const current = yield* retained(r),
              member = yield* Members.MemberEnrollments;
            if (current?.state === "bound") {
              yield* member.validateLocal(current);
              return false;
            }
            yield* qualification.checkCurrent(q).pipe(Effect.mapError(Members.memberFailure));
            const hasPin = yield* preparedLocal(r, current !== null);
            if (!current) {
              if (!hasPin)
                yield* (yield* Pins.PeerSessionPinStore).insert({
                  pin: Pins.decodeSetupRequest(r.intent.member_setup_request_body).pin,
                  request_body: r.intent.member_setup_request_body,
                });
              yield* (yield* Records.MemberEnrollmentStore).insert(r);
            }
            yield* preparedLocal(r, true);
            yield* qualification.checkCurrent(q).pipe(Effect.mapError(Members.memberFailure));
            yield* (yield* Keys.WorkstreamsKeyProvider).activeKeyId({
              ...b.expected_home,
              key_scope: "local",
            });
            return current === null;
          }),
        );
      }),
    ).pipe(Effect.mapError(Members.memberFailure));
    // Only this call's newly committed preparation permits one dispatch. Retained/unknown
    // preparation is readback-only even if the remote lookup is empty.
    if (created) {
      yield* qualification.checkCurrent(q).pipe(Effect.mapError(Members.memberFailure));
      const pin = Pins.decodeSetupRequest(r.intent.member_setup_request_body).pin;
      yield* client
        .call({
          environmentId: EnvironmentId.make(b.expected_home.home_environment_id),
          expectedGeneration: b.member_setup.outbound_link_generation,
          tool: "jones_workstreams_home_bind",
          arguments: {
            contract: HomeWire.WORKSTREAMS_HOME_BIND_CONTRACT,
            request_body: r.intent.bind_request_body,
          },
          limits: {
            runtimeMode: pin.session.ceiling.runtime_mode,
            interactionMode: pin.session.ceiling.interaction_mode,
          },
          success: HomeWire.WorkstreamsHomeBindCommittedV2,
        })
        .pipe(
          Effect.mapError(
            (cause) => new Members.MemberEnrollmentError({ code: "remote_unknown", cause }),
          ),
        );
    }
    const witness = yield* qualification.readback(q).pipe(Effect.mapError(Members.memberFailure));
    const home = yield* qualification
      .result(q, witness)
      .pipe(Effect.mapError(Members.memberFailure));
    return yield* local(
      r,
      Effect.gen(function* () {
        const db = yield* Database.NativeSql;
        return yield* db.transaction(
          b.expected_home,
          Effect.gen(function* () {
            const current = yield* retained(r),
              member = yield* Members.MemberEnrollments;
            if (current?.state === "bound") {
              if (!same(current.home_result, home)) return yield* refuse("issuance_conflict");
              yield* member.validateLocal(current);
              yield* qualification.checkCurrent(q).pipe(Effect.mapError(Members.memberFailure));
              return current;
            }
            if (!current) return yield* refuse("issuance_conflict");
            yield* preparedLocal(r, true);
            const pins = yield* Pins.PeerSessionPinStore,
              bindings = yield* Bindings.BindingStore;
            const pin = (yield* pins.read({ pin_id: b.member_setup.pin_id }))!.pin;
            yield* qualification
              .verifyBinding(
                q,
                witness,
                pin,
                home.receipt,
                b.member_setup.singleton_binding_version,
              )
              .pipe(Effect.mapError(Members.memberFailure));
            yield* member.reserveSession(r);
            yield* bindings.bind(
              {
                registry_instance_id: b.expected_home.registry_instance_id,
                owner_id: b.expected_home.owner_id,
                home_environment_id: b.expected_home.home_environment_id,
                home_generation: b.expected_home.server_generation,
                principal_id: home.receipt.principal_id,
                binding_version: b.member_setup.singleton_binding_version,
                state: "active",
              },
              b.member_setup.expected_singleton_version,
            );
            yield* pins.transition(
              {
                ...pin,
                state: "bound",
                revision: 2,
                bind_receipt: home.receipt,
                source_binding_version: home.receipt.binding_version,
                singleton_binding_version: b.member_setup.singleton_binding_version,
              },
              1,
            );
            const actualPin = yield* pins.read({ pin_id: pin.pin_id }),
              actualBinding = yield* bindings.read();
            if (
              !actualPin ||
              actualPin.pin.state !== "bound" ||
              !actualBinding ||
              actualBinding.state !== "active"
            )
              return yield* refuse("pin_mismatch");
            yield* qualification
              .verifyBinding(q, witness, actualPin.pin, home.receipt, actualBinding.binding_version)
              .pipe(Effect.mapError(Members.memberFailure));
            const bound = yield* decodeBound({
              ...r,
              state: "bound",
              revision: 2,
              home_result: home,
              local_binding: {
                pin_bound_revision: actualPin.pin.revision,
                singleton_binding_version: actualBinding.binding_version,
              },
            }).pipe(Effect.mapError(() => refuse("invalid_request")));
            yield* (yield* Records.MemberEnrollmentStore).accept(bound, 1);
            yield* member.validateLocal(bound);
            yield* member.validateSource(bound, true);
            yield* member.ownerCurrent;
            yield* qualification.checkCurrent(q).pipe(Effect.mapError(Members.memberFailure));
            yield* (yield* Keys.WorkstreamsKeyProvider).activeKeyId({
              ...b.expected_home,
              key_scope: "local",
            });
            return bound;
          }),
        );
      }),
    ).pipe(
      Effect.mapError((cause) =>
        isMembersMemberEnrollmentError(cause)
          ? cause
          : new Members.MemberEnrollmentError({ code: "unknown_commit", cause }),
      ),
    );
  });
  const apply = Effect.fn("MemberSetup.apply")(function* (
    i: Wire.WorkstreamsMemberEnrollmentIntentV1,
  ) {
    const result = yield* run(i, true).pipe(Effect.mapError(Members.memberFailure));
    if (!result) return yield* refuse("unknown_commit");
    return result;
  });
  const readback = Effect.fn("MemberSetup.readback")(
    (i: Wire.WorkstreamsMemberEnrollmentIntentV1) =>
      run(i, false).pipe(Effect.mapError(Members.memberFailure)),
  );
  return MemberSetup.of({ apply, readback });
});
/** Root mounts genuine CLI/bootstrap/auth adapters separately; no synthetic production defaults. */
export const layer = Layer.effect(MemberSetup, make);
