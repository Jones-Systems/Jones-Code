import { AuthSessionId, EnvironmentId, WorkstreamsNativeBuild } from "@t3tools/contracts";
import * as Wire from "@t3tools/contracts/jones/workstreamsMemberEnrollment";
import { decodeWorkstreamsHomeBindBody } from "@t3tools/contracts/jones/workstreamsHomeBind";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import {
  matchesReservedAuthSession,
  makeReservedBearerSessionMaterializer,
} from "../../../../auth/SessionStore.ts";
import {
  deriveNativeEnrollmentBinding,
  deriveNativeEnrollmentSession,
} from "../../enrollment/request.ts";
import type { NativeProviderEnrollmentBinding } from "../../nativeProvider/enrollment.ts";
import * as Source from "./sourceEnrollment.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import * as Records from "../store/memberEnrollmentStore.ts";
import * as Pins from "../store/peerSessionPinStore.ts";
import * as Bindings from "../store/bindingStore.ts";

export class MemberEnrollmentError extends Schema.TaggedError<MemberEnrollmentError>()(
  "MemberEnrollmentError",
  {
    code: Schema.Literals([
      "invalid_request",
      "owner_unqualified",
      "authority_unavailable",
      "authority_mismatch",
      "build_unavailable",
      "build_mismatch",
      "session_conflict",
      "pin_mismatch",
      "link_changed",
      "version_conflict",
      "issuance_conflict",
      "key_unavailable",
      "remote_unknown",
      "unavailable",
      "unknown_commit",
    ]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message() {
    return `Native member enrollment refused: ${this.code}.`;
  }
}
const isMemberEnrollmentError = Schema.is(MemberEnrollmentError);
const isWorkstreamsNativeBuild = Schema.is(WorkstreamsNativeBuild);
export const memberFailure = (cause: unknown): MemberEnrollmentError => {
  if (isMemberEnrollmentError(cause)) return cause;
  const code = typeof cause === "object" && cause !== null && "code" in cause ? cause.code : null;
  return new MemberEnrollmentError({
    code:
      code === "owner_unqualified" || code === "link_changed" || code === "remote_unknown"
        ? code
        : typeof cause === "object" &&
            cause !== null &&
            "_tag" in cause &&
            cause._tag === "KeyUnavailable"
          ? "key_unavailable"
          : "unavailable",
    cause,
  });
};
export class MemberEnrollments extends Context.Service<
  MemberEnrollments,
  {
    readonly ownerCurrent: Effect.Effect<void, MemberEnrollmentError>;
    readonly validateSource: (
      record: Wire.WorkstreamsMemberEnrollmentV1,
      exactBuild: boolean,
    ) => Effect.Effect<void, MemberEnrollmentError>;
    readonly inspectSession: (
      record: Wire.WorkstreamsMemberEnrollmentV1,
    ) => Effect.Effect<void, MemberEnrollmentError>;
    readonly reserveSession: (
      record: Wire.WorkstreamsMemberEnrollmentPreparedV1,
    ) => Effect.Effect<void, MemberEnrollmentError>;
    readonly validateLocal: (
      record: Wire.WorkstreamsMemberEnrollmentBoundV1,
    ) => Effect.Effect<void, MemberEnrollmentError>;
    readonly readProof: (
      selector: Records.MemberEnrollmentSelector,
    ) => Effect.Effect<Wire.WorkstreamsMemberEnrollmentV1 | null, MemberEnrollmentError>;
    readonly getValidatedBySessionId: (
      sessionId: string,
    ) => Effect.Effect<
      Option.Option<Wire.WorkstreamsMemberEnrollmentBoundV1>,
      MemberEnrollmentError
    >;
    readonly getValidatedByEnrollmentId: (
      enrollmentId: string,
    ) => Effect.Effect<
      Option.Option<Wire.WorkstreamsMemberEnrollmentBoundV1>,
      MemberEnrollmentError
    >;
    readonly getBySessionId: (
      sessionId: string,
    ) => Effect.Effect<Option.Option<NativeProviderEnrollmentBinding>, MemberEnrollmentError>;
    readonly materialize: (sessionId: string) => Effect.Effect<string, MemberEnrollmentError>;
  }
>()("t3/jones/workstreams/native/runtime/memberEnrollment/MemberEnrollments") {}

const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const refuse = (code: MemberEnrollmentError["code"]) => new MemberEnrollmentError({ code });
const make = Effect.gen(function* () {
  const sessions = yield* AuthSessions.AuthSessionRepository,
    records = yield* Records.MemberEnrollmentStore,
    pins = yield* Pins.PeerSessionPinStore,
    bindings = yield* Bindings.BindingStore,
    sql = yield* SqlClient.SqlClient,
    secrets = yield* Secrets.ServerSecretStore;
  const links = yield* Links.PeerLinks;
  const issuer = yield* Effect.serviceOption(Source.SourceIssuer),
    owner = yield* Effect.serviceOption(Source.SourceSetupOwner);
  const ownerCurrent = Effect.gen(function* () {
    if (Option.isNone(owner)) return yield* refuse("owner_unqualified");
    yield* owner.value.verifyUnchanged.pipe(Effect.mapError(memberFailure));
  });
  const validateSource = Effect.fn("MemberEnrollments.validateSource")(function* (
    input: Wire.WorkstreamsMemberEnrollmentV1,
    exactBuild: boolean,
  ) {
    const r = yield* Effect.try({
      try: () => Records.validateMemberEnrollment(input),
      catch: () => refuse("invalid_request"),
    });
    const b = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body),
      c = b.source.enrollment.context;
    if (Option.isNone(issuer)) return yield* refuse("authority_unavailable");
    const authority = yield* issuer.value.authority.readCurrent.pipe(
      Effect.mapError(() => refuse("authority_unavailable")),
    );
    if (
      authority.environmentId !== c.source_instance_id ||
      authority.authorityNamespace !== c.authority_namespace ||
      authority.storeGeneration !== c.store_generation
    )
      return yield* refuse("authority_mismatch");
    const build = yield* issuer.value.build;
    if (Option.isNone(build) || !isWorkstreamsNativeBuild(build.value))
      return yield* refuse("build_unavailable");
    if (exactBuild && !same(build.value, c.build)) return yield* refuse("build_mismatch");
    const now = DateTime.toEpochMillis(yield* DateTime.now);
    if (
      Date.parse(b.source.enrollment.session.issued_at) > now ||
      Date.parse(b.source.enrollment.session.expires_at) <= now
    )
      return yield* refuse("session_conflict");
    const generation = yield* links
      .currentGeneration(EnvironmentId.make(b.expected_home.home_environment_id))
      .pipe(Effect.mapError(memberFailure));
    if (Option.isNone(generation) || generation.value !== b.member_setup.outbound_link_generation)
      return yield* refuse("link_changed");
  });
  const inspectSession = Effect.fn("MemberEnrollments.inspectSession")(function* (
    r: Wire.WorkstreamsMemberEnrollmentV1,
  ) {
    const b = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body),
      e = b.source.enrollment;
    const row = yield* sessions
      .getById({ sessionId: e.session.session_id })
      .pipe(Effect.mapError(memberFailure));
    const legacy = yield* sql`SELECT enrollment_id FROM jones_workstreams_source_enrollments
      WHERE enrollment_id=${e.enrollment_id} OR session_id=${e.session.session_id}`.pipe(
      Effect.mapError(memberFailure),
    );
    if (Option.isSome(row) || legacy.length !== 0) return yield* refuse("session_conflict");
  });
  const activeSession = Effect.fn("MemberEnrollments.activeSession")(function* (
    r: Wire.WorkstreamsMemberEnrollmentV1,
  ) {
    const e = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body).source.enrollment,
      expected = deriveNativeEnrollmentSession(e);
    const row = yield* sessions
      .getById({ sessionId: expected.sessionId })
      .pipe(Effect.mapError(memberFailure));
    const now = DateTime.toEpochMillis(yield* DateTime.now);
    if (
      Option.isNone(row) ||
      row.value.revokedAt !== null ||
      row.value.issuedAt.epochMilliseconds > now ||
      row.value.expiresAt.epochMilliseconds <= now ||
      !matchesReservedAuthSession(row.value, expected)
    )
      return yield* refuse("session_conflict");
  });
  const reserveSession = Effect.fn("MemberEnrollments.reserveSession")(function* (
    r: Wire.WorkstreamsMemberEnrollmentPreparedV1,
  ) {
    yield* ownerCurrent;
    yield* inspectSession(r);
    yield* sessions
      .createIfAbsent(
        deriveNativeEnrollmentSession(
          decodeWorkstreamsHomeBindBody(r.intent.bind_request_body).source.enrollment,
        ),
      )
      .pipe(Effect.mapError(memberFailure));
    yield* activeSession(r);
  });
  const validateLocal = Effect.fn("MemberEnrollments.validateLocal")(function* (
    r: Wire.WorkstreamsMemberEnrollmentBoundV1,
  ) {
    yield* validateSource(r, false);
    const b = decodeWorkstreamsHomeBindBody(r.intent.bind_request_body);
    const pin = yield* pins
      .read({ pin_id: b.member_setup.pin_id })
      .pipe(Effect.mapError(memberFailure));
    const original = Pins.decodeSetupRequest(r.intent.member_setup_request_body).pin;
    if (
      !pin ||
      pin.request_body !== r.intent.member_setup_request_body ||
      !same(pin.pin, {
        ...original,
        revision: r.local_binding.pin_bound_revision,
        state: "bound",
        bind_receipt: r.home_result.receipt,
        source_binding_version: r.home_result.receipt.binding_version,
        singleton_binding_version: r.local_binding.singleton_binding_version,
      })
    )
      return yield* refuse("pin_mismatch");
    const now = DateTime.toEpochMillis(yield* DateTime.now),
      p = pin.pin;
    const peer = yield* sessions
      .getById({ sessionId: AuthSessionId.make(p.session.session_id) })
      .pipe(Effect.mapError(memberFailure));
    if (
      Date.parse(p.expires_at) <= now ||
      Option.isNone(peer) ||
      peer.value.revokedAt !== null ||
      peer.value.subject !== p.session.subject ||
      peer.value.method !== p.session.method ||
      DateTime.formatIso(peer.value.issuedAt) !== p.session.issued_at ||
      DateTime.formatIso(peer.value.expiresAt) !== p.session.expires_at ||
      !same(peer.value.scopes, p.session.ceiling.scopes) ||
      peer.value.issuedAt.epochMilliseconds > now ||
      peer.value.expiresAt.epochMilliseconds <= now
    )
      return yield* refuse("pin_mismatch");
    // Historical association validation must also work on a query-only startup connection.
    // Mutation callers retain their own transactional destination checks.
    const binding = yield* bindings.read().pipe(Effect.mapError(() => refuse("pin_mismatch")));
    if (
      !binding ||
      binding.state !== "active" ||
      binding.registry_instance_id !== b.expected_home.registry_instance_id ||
      binding.owner_id !== b.expected_home.owner_id ||
      binding.home_environment_id !== b.expected_home.home_environment_id ||
      binding.home_generation !== b.expected_home.server_generation ||
      binding.principal_id !== r.home_result.receipt.principal_id ||
      binding.binding_version !== r.local_binding.singleton_binding_version
    )
      return yield* refuse("pin_mismatch");
    yield* activeSession(r);
  });
  const readProof = Effect.fn("MemberEnrollments.readProof")(
    (selector: Records.MemberEnrollmentSelector) =>
      records.read(selector).pipe(Effect.mapError(memberFailure)),
  );
  const validated = Effect.fn("MemberEnrollments.validated")(function* (
    selector: Records.MemberEnrollmentSelector,
  ) {
    const record = yield* readProof(selector);
    if (!record || record.state !== "bound")
      return Option.none<Wire.WorkstreamsMemberEnrollmentBoundV1>();
    // This projection is historical association only; A separately obtains current home permission.
    yield* validateLocal(record);
    return Option.some(record);
  });
  const getValidatedBySessionId = Effect.fn("MemberEnrollments.getValidatedBySessionId")(
    (session_id: string) => validated({ session_id }),
  );
  const getValidatedByEnrollmentId = Effect.fn("MemberEnrollments.getValidatedByEnrollmentId")(
    (enrollment_id: string) => validated({ enrollment_id }),
  );
  const getBySessionId = Effect.fn("MemberEnrollments.getBySessionId")((id: string) =>
    getValidatedBySessionId(id).pipe(
      Effect.map(
        Option.map((p) =>
          deriveNativeEnrollmentBinding(
            decodeWorkstreamsHomeBindBody(p.intent.bind_request_body).source.enrollment,
          ),
        ),
      ),
    ),
  );
  const materialize = Effect.fn("MemberEnrollments.materialize")(function* (id: string) {
    yield* ownerCurrent;
    const proof = yield* getValidatedBySessionId(id);
    if (Option.isNone(proof)) return yield* refuse("session_conflict");
    const sign = yield* makeReservedBearerSessionMaterializer.pipe(
      Effect.provideService(AuthSessions.AuthSessionRepository, sessions),
      Effect.provideService(Secrets.ServerSecretStore, secrets),
      Effect.mapError(memberFailure),
    );
    const token = (yield* sign(
      deriveNativeEnrollmentSession(
        decodeWorkstreamsHomeBindBody(proof.value.intent.bind_request_body).source.enrollment,
      ),
    ).pipe(Effect.mapError(memberFailure))).token;
    yield* validateLocal(proof.value);
    yield* ownerCurrent;
    return token;
  });
  return MemberEnrollments.of({
    ownerCurrent,
    validateSource,
    inspectSession,
    reserveSession,
    validateLocal,
    readProof,
    getValidatedBySessionId,
    getValidatedByEnrollmentId,
    getBySessionId,
    materialize,
  });
});
/** Source-owned member evidence only; it does not install caller/grant admission. */
export const layer = Layer.effect(MemberEnrollments, make);
