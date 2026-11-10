import { WorkstreamsNativeBuild } from "@t3tools/contracts";
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import type { NativeStoreAuthority } from "../../../../environment/NativeStoreAuthority.ts";
import {
  matchesReservedAuthSession,
  makeReservedBearerSessionMaterializer,
} from "../../../../auth/SessionStore.ts";
import {
  NativeEnrollmentRequest,
  deriveNativeEnrollmentBinding,
  deriveNativeEnrollmentSession,
  sha256EnrollmentBytes,
} from "../../enrollment/request.ts";
import {
  NativeProviderEnrollment,
  NativeProviderEnrollmentError,
  type NativeProviderEnrollmentBinding,
} from "../../nativeProvider/enrollment.ts";
import * as Records from "../store/sourceEnrollmentStore.ts";
import * as Registries from "../store/registryStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Keys from "../crypto/keyProvider.ts";

const closed = <F extends Schema.Struct.Fields>(schema: Schema.Struct<F>) =>
  Schema.flip(
    Schema.flip(schema).check(
      Schema.makeFilter(
        (v) =>
          typeof v === "object" &&
          v !== null &&
          !Array.isArray(v) &&
          Reflect.ownKeys(v).every((k) => Object.hasOwn(schema.fields, k)),
      ),
    ),
  );
const Positive = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));
const Nonnegative = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const Id = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(512));
const SourceBinding = closed(
  Schema.Struct({
    binding_version: Positive,
    authority_namespace: Id,
    store_generation: Positive,
    state: Schema.Literals(["active", "revoked"]),
  }),
);
export const NativeSourceSetupRequest = closed(
  Schema.Struct({
    schema: Schema.Literal("jones-code.native-source-setup/v1"),
    enrollment: NativeEnrollmentRequest,
    expected: closed(
      Schema.Struct({
        registry_instance_id: Id,
        owner_id: Id,
        home_environment_id: Id,
        home_generation: Positive,
        registry_version: Nonnegative,
        singleton_binding_version: Positive,
        grant_id: Id,
        grant_version: Positive,
        source_binding: Schema.NullOr(SourceBinding),
      }),
    ),
    rebind: Schema.Boolean,
  }),
).check(
  Schema.makeFilter(
    (r) =>
      r.expected.owner_id === r.enrollment.context.owner_id &&
      r.expected.home_environment_id === r.enrollment.context.source_instance_id,
  ),
);
export type NativeSourceSetupRequest = typeof NativeSourceSetupRequest.Type;
const Receipt = closed(
  Schema.Struct({ registry_version: Nonnegative, source_binding_version: Positive }),
);
export const NativeSourceEnrollmentV1 = closed(
  Schema.Struct({
    schema: Schema.Literal("jones-code.native-source-enrollment/v1"),
    request: NativeSourceSetupRequest,
    request_sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
    receipt: Receipt,
  }),
).check(
  Schema.makeFilter(
    (r) =>
      r.request_sha256 === sourceSetupDigest(r.request) &&
      r.receipt.registry_version === r.request.expected.registry_version + 1 &&
      r.receipt.source_binding_version ===
        (r.request.expected.source_binding?.binding_version ?? 0) + 1,
  ),
);
export type NativeSourceEnrollmentV1 = typeof NativeSourceEnrollmentV1.Type;
export const sourceSetupDigest = (request: NativeSourceSetupRequest): string =>
  sha256EnrollmentBytes(canonicalWorkstreamsSnapshotJson(request));
export class SourceEnrollmentError extends Schema.TaggedError<SourceEnrollmentError>()(
  "NativeSourceEnrollmentError",
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
      "version_conflict",
      "rebind_required",
      "issuance_conflict",
      "key_unavailable",
      "unavailable",
      "unknown_commit",
    ]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message() {
    return `Native source setup refused: ${this.code}.`;
  }
}
/** Only a qualified standalone CLI or exclusive desktop owner may provide these ports. */
export class SourceIssuer extends Context.Service<
  SourceIssuer,
  {
    readonly authority: Pick<NativeStoreAuthority["Service"], "readCurrent">;
    readonly build: Effect.Effect<Option.Option<WorkstreamsNativeBuild>>;
  }
>()("t3/jones/workstreams/native/runtime/sourceEnrollment/SourceIssuer") {}
export class SourceSetupOwner extends Context.Service<
  SourceSetupOwner,
  {
    readonly verifyUnchanged: Effect.Effect<void, SourceEnrollmentError>;
  }
>()("t3/jones/workstreams/native/runtime/sourceEnrollment/SourceSetupOwner") {}
export class NativeSourceEnrollments extends Context.Service<
  NativeSourceEnrollments,
  {
    readonly validateSource: (
      request: NativeSourceSetupRequest,
      exactBuild: boolean,
    ) => Effect.Effect<void, SourceEnrollmentError>;
    readonly validatePins: (
      request: NativeSourceSetupRequest,
      receipt: NativeSourceEnrollmentV1["receipt"] | null,
    ) => Effect.Effect<void, SourceEnrollmentError>;
    readonly inspectSession: (
      request: NativeSourceSetupRequest,
    ) => Effect.Effect<void, SourceEnrollmentError>;
    readonly reserveSession: (
      request: NativeSourceSetupRequest,
    ) => Effect.Effect<void, SourceEnrollmentError>;
    readonly readProof: (
      selector: { readonly enrollment_id: string } | { readonly session_id: string },
    ) => Effect.Effect<NativeSourceEnrollmentV1 | null, SourceEnrollmentError>;
    readonly getValidatedBySessionId: (
      sessionId: string,
    ) => Effect.Effect<Option.Option<NativeSourceEnrollmentV1>, SourceEnrollmentError>;
    readonly getBySessionId: (
      sessionId: string,
    ) => Effect.Effect<Option.Option<NativeProviderEnrollmentBinding>, SourceEnrollmentError>;
    readonly materialize: (sessionId: string) => Effect.Effect<string, SourceEnrollmentError>;
  }
>()("t3/jones/workstreams/native/runtime/sourceEnrollment/NativeSourceEnrollments") {}
const decodeProof = Schema.decodeUnknownEffect(NativeSourceEnrollmentV1);
const isError = Schema.is(SourceEnrollmentError);
const isBuild = Schema.is(WorkstreamsNativeBuild);
const decodePermissions = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(Schema.String)),
);
const failure = (e: unknown) =>
  isError(e)
    ? e
    : new SourceEnrollmentError({
        code:
          typeof e === "object" && e !== null && "_tag" in e && e._tag === "KeyUnavailable"
            ? "key_unavailable"
            : "unavailable",
        cause: e,
      });
const same = (a: unknown, b: unknown) =>
  canonicalWorkstreamsSnapshotJson(a) === canonicalWorkstreamsSnapshotJson(b);
const make = Effect.gen(function* () {
  const issuer = yield* SourceIssuer,
    sessions = yield* AuthSessions.AuthSessionRepository,
    records = yield* Records.SourceEnrollmentStore,
    registries = yield* Registries.RegistryStore,
    bindings = yield* Bindings.BindingStore,
    sql = yield* SqlClient.SqlClient,
    secrets = yield* Secrets.ServerSecretStore,
    keys = yield* Keys.WorkstreamsKeyProvider;
  const owner = yield* Effect.serviceOption(SourceSetupOwner);
  const qualifyOwner = Effect.fn("NativeSourceEnrollments.qualifyOwner")(function* () {
    if (Option.isNone(owner))
      return yield* new SourceEnrollmentError({ code: "owner_unqualified" });
    yield* owner.value.verifyUnchanged;
  });
  const validateSource = Effect.fn("NativeSourceEnrollments.validateSource")(function* (
    request: NativeSourceSetupRequest,
    exactBuild: boolean,
  ) {
    const input = request.enrollment;
    if (exactBuild) {
      yield* keys
        .get(
          yield* keys
            .activeKeyId({ ...request.expected, key_scope: "home" })
            .pipe(Effect.mapError(() => new SourceEnrollmentError({ code: "key_unavailable" }))),
        )
        .pipe(Effect.mapError(() => new SourceEnrollmentError({ code: "key_unavailable" })));
      yield* makeReservedBearerSessionMaterializer.pipe(
        Effect.provideService(AuthSessions.AuthSessionRepository, sessions),
        Effect.provideService(Secrets.ServerSecretStore, secrets),
        Effect.mapError(() => new SourceEnrollmentError({ code: "key_unavailable" })),
      );
    }
    const now = DateTime.toEpochMillis(yield* DateTime.now);
    if (Date.parse(input.session.issued_at) > now || Date.parse(input.session.expires_at) <= now)
      return yield* new SourceEnrollmentError({ code: "session_conflict" });
    const authority = yield* issuer.authority.readCurrent.pipe(
      Effect.mapError(() => new SourceEnrollmentError({ code: "authority_unavailable" })),
    );
    if (
      authority.environmentId !== input.context.source_instance_id ||
      authority.authorityNamespace !== input.context.authority_namespace ||
      authority.storeGeneration !== input.context.store_generation
    )
      return yield* new SourceEnrollmentError({ code: "authority_mismatch" });
    const build = yield* issuer.build;
    if (Option.isNone(build) || !isBuild(build.value))
      return yield* new SourceEnrollmentError({ code: "build_unavailable" });
    if (exactBuild && !same(build.value, input.context.build))
      return yield* new SourceEnrollmentError({ code: "build_mismatch" });
  });
  const validatePins = Effect.fn("NativeSourceEnrollments.validatePins")(
    function* (
      request: NativeSourceSetupRequest,
      receipt: NativeSourceEnrollmentV1["receipt"] | null,
    ) {
      const p = request.expected,
        c = request.enrollment.context;
      const registry = yield* registries.context(p),
        singleton = yield* bindings.read();
      if (
        !registry ||
        registry.state !== "active" ||
        registry.home_environment_id !== p.home_environment_id ||
        registry.server_generation !== p.home_generation ||
        registry.canonical_origin !== request.enrollment.registry_origin ||
        !singleton ||
        singleton.state !== "active" ||
        singleton.registry_instance_id !== p.registry_instance_id ||
        singleton.owner_id !== p.owner_id ||
        singleton.home_environment_id !== p.home_environment_id ||
        singleton.home_generation !== p.home_generation ||
        singleton.principal_id !== c.principal_id ||
        singleton.binding_version !== p.singleton_binding_version
      )
        return yield* new SourceEnrollmentError({ code: "pin_mismatch" });
      if (
        receipt
          ? registry.registry_version < receipt.registry_version
          : registry.registry_version !== p.registry_version
      )
        return yield* new SourceEnrollmentError({ code: "version_conflict" });
      const principals = yield* registries.listPlain(p, "principals");
      const principal = principals.find((r) => r.principal_id === c.principal_id);
      if (
        !principal ||
        principal.kind !== "environment" ||
        principal.environment_id !== c.source_instance_id
      )
        return yield* new SourceEnrollmentError({ code: "pin_mismatch" });
      const grants = (yield* registries.listPlain(p, "grants"))
        .filter((r) => r.principal_id === c.principal_id)
        .toSorted((a, b) => Number(b.grant_version) - Number(a.grant_version));
      const grant = grants[0];
      if (
        !grant ||
        grant.state !== "active" ||
        grant.grant_id !== p.grant_id ||
        grant.grant_version !== p.grant_version ||
        typeof grant.permissions !== "string"
      )
        return yield* new SourceEnrollmentError({ code: "pin_mismatch" });
      const permissions = yield* decodePermissions(grant.permissions).pipe(
        Effect.mapError(() => new SourceEnrollmentError({ code: "pin_mismatch" })),
      );
      if (
        !Array.isArray(permissions) ||
        !["workstreams:read", "workstreams:write"].every((v) => permissions.includes(v))
      )
        return yield* new SourceEnrollmentError({ code: "pin_mismatch" });
      const source = (yield* registries.listPlain(p, "source_bindings"))
        .filter((r) => r.provider === "t3" && r.source_instance_id === c.source_instance_id)
        .toSorted((a, b) => Number(b.binding_version) - Number(a.binding_version))[0];
      const current = source
        ? {
            binding_version: Number(source.binding_version),
            authority_namespace: source.authority_namespace,
            store_generation: Number(source.store_generation),
            state: source.state,
          }
        : null;
      const expected = receipt
        ? {
            binding_version: receipt.source_binding_version,
            authority_namespace: c.authority_namespace,
            store_generation: c.store_generation,
            state: "active",
          }
        : p.source_binding;
      if (!same(current, expected))
        return yield* new SourceEnrollmentError({ code: "version_conflict" });
      if (
        !receipt &&
        current &&
        (current.authority_namespace !== c.authority_namespace ||
          current.store_generation !== c.store_generation ||
          current.state !== "active")
      ) {
        if (!request.rebind) return yield* new SourceEnrollmentError({ code: "rebind_required" });
        if (
          current.authority_namespace === c.authority_namespace &&
          c.store_generation < current.store_generation
        )
          return yield* new SourceEnrollmentError({ code: "pin_mismatch" });
      }
    },
    (effect) => effect.pipe(Effect.mapError(failure)),
  );
  const activeSession = Effect.fn("NativeSourceEnrollments.activeSession")(function* (
    request: NativeSourceSetupRequest,
  ) {
    const expected = deriveNativeEnrollmentSession(request.enrollment);
    const row = yield* sessions
      .getById({ sessionId: expected.sessionId })
      .pipe(Effect.mapError(failure));
    const now = DateTime.toEpochMillis(yield* DateTime.now);
    if (
      Option.isNone(row) ||
      row.value.revokedAt !== null ||
      row.value.issuedAt.epochMilliseconds > now ||
      row.value.expiresAt.epochMilliseconds <= now ||
      !matchesReservedAuthSession(row.value, expected)
    )
      return yield* new SourceEnrollmentError({ code: "session_conflict" });
  });
  const inspectSession = Effect.fn("NativeSourceEnrollments.inspectSession")(function* (
    request: NativeSourceSetupRequest,
  ) {
    const existing = yield* sessions
      .getById({ sessionId: request.enrollment.session.session_id })
      .pipe(Effect.mapError(failure));
    // An unrelated preexisting session is never converted into native issuance.
    if (Option.isSome(existing))
      return yield* new SourceEnrollmentError({ code: "session_conflict" });
  });
  const reserveSession = Effect.fn("NativeSourceEnrollments.reserveSession")(function* (
    request: NativeSourceSetupRequest,
  ) {
    yield* qualifyOwner();
    yield* inspectSession(request);
    yield* sessions
      .createIfAbsent(deriveNativeEnrollmentSession(request.enrollment))
      .pipe(Effect.mapError(failure));
    yield* activeSession(request);
  });
  const readProof: NativeSourceEnrollments["Service"]["readProof"] = Effect.fn(
    "NativeSourceEnrollments.readProof",
  )(function* (selector) {
    const row = yield* records.read(selector).pipe(Effect.mapError(failure));
    if (!row) return null;
    const proof = yield* decodeProof(row.proof, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() => new SourceEnrollmentError({ code: "issuance_conflict" })),
    );
    if (
      row.enrollment_id !== proof.request.enrollment.enrollment_id ||
      row.session_id !== proof.request.enrollment.session.session_id ||
      row.registry_instance_id !== proof.request.expected.registry_instance_id ||
      row.owner_id !== proof.request.expected.owner_id ||
      row.request_sha256 !== proof.request_sha256
    )
      return yield* new SourceEnrollmentError({ code: "issuance_conflict" });
    return proof;
  });
  const getValidatedBySessionId = Effect.fn("NativeSourceEnrollments.getValidatedBySessionId")(
    (sessionId: string) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const proof = yield* readProof({ session_id: sessionId });
            if (!proof) return Option.none<NativeSourceEnrollmentV1>();
            yield* validateSource(proof.request, false);
            yield* validatePins(proof.request, proof.receipt);
            yield* activeSession(proof.request);
            return Option.some(proof);
          }),
        )
        .pipe(Effect.mapError(failure)),
  );
  const getBySessionId = Effect.fn("NativeSourceEnrollments.getBySessionId")((sessionId: string) =>
    getValidatedBySessionId(sessionId).pipe(
      Effect.map(Option.map((proof) => deriveNativeEnrollmentBinding(proof.request.enrollment))),
    ),
  );
  const materialize = Effect.fn("NativeSourceEnrollments.materialize")(function* (
    sessionId: string,
  ) {
    yield* qualifyOwner();
    const proof = yield* getValidatedBySessionId(sessionId);
    if (Option.isNone(proof)) return yield* new SourceEnrollmentError({ code: "session_conflict" });
    const sign = yield* makeReservedBearerSessionMaterializer.pipe(
      Effect.provideService(AuthSessions.AuthSessionRepository, sessions),
      Effect.provideService(Secrets.ServerSecretStore, secrets),
      Effect.mapError(failure),
    );
    const token = (yield* sign(deriveNativeEnrollmentSession(proof.value.request.enrollment)).pipe(
      Effect.mapError(failure),
    )).token;
    yield* qualifyOwner();
    return token;
  });
  return NativeSourceEnrollments.of({
    validateSource,
    validatePins,
    inspectSession,
    reserveSession,
    readProof,
    getValidatedBySessionId,
    getBySessionId,
    materialize,
  });
});
/** @public Selected native composition only; never supplies legacy issuance. */
export const layer = Layer.effect(NativeSourceEnrollments, make);
/** @public Compatibility lookup over source-issued native evidence. */
export const providerLayer = Layer.effect(
  NativeProviderEnrollment,
  Effect.map(NativeSourceEnrollments, (source) =>
    NativeProviderEnrollment.of({
      getBySessionId: (id) =>
        source.getBySessionId(id).pipe(Effect.mapError(() => new NativeProviderEnrollmentError())),
    }),
  ),
);
