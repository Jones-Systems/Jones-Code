import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Console from "effect/Console";
import { Command, Flag } from "effect/cli";
import * as Enrollments from "./sourceEnrollment.ts";
import * as Records from "../store/sourceEnrollmentStore.ts";
import * as Registries from "../store/registryStore.ts";
import * as Database from "../store/sql.ts";
import { canonicalWorkstreamsSnapshotJson } from "@t3tools/contracts/jones/workstreamsSnapshot";
import type { InitSelection } from "../cli.ts";

export class NativeSourceSetup extends Context.Service<
  NativeSourceSetup,
  {
    readonly plan: (
      request: Enrollments.NativeSourceSetupRequest,
    ) => Effect.Effect<
      { readonly state: "ready" | "applied"; readonly request_sha256: string },
      Enrollments.SourceEnrollmentError
    >;
    readonly apply: (
      request: Enrollments.NativeSourceSetupRequest,
    ) => Effect.Effect<Enrollments.NativeSourceEnrollmentV1, Enrollments.SourceEnrollmentError>;
    readonly readback: (
      request: Enrollments.NativeSourceSetupRequest,
    ) => Effect.Effect<
      Enrollments.NativeSourceEnrollmentV1 | null,
      Enrollments.SourceEnrollmentError
    >;
  }
>()("t3/jones/workstreams/native/runtime/sourceSetup/NativeSourceSetup") {}
const decode = Schema.decodeUnknownEffect(Enrollments.NativeSourceSetupRequest);
const isError = Schema.is(Enrollments.SourceEnrollmentError);
const mapFailure = (e: unknown) =>
  isError(e)
    ? e
    : new Enrollments.SourceEnrollmentError({
        code:
          typeof e === "object" && e !== null && "_tag" in e && e._tag === "KeyUnavailable"
            ? "key_unavailable"
            : "unknown_commit",
        cause: e,
      });
const make = Effect.gen(function* () {
  const source = yield* Enrollments.NativeSourceEnrollments,
    records = yield* Records.SourceEnrollmentStore,
    registries = yield* Registries.RegistryStore,
    db = yield* Database.NativeSql;
  const ownerEvidence = yield* Effect.serviceOption(Enrollments.SourceSetupOwner);
  const validate = Effect.fn("NativeSourceSetup.validate")(
    (input: Enrollments.NativeSourceSetupRequest) =>
      decode(input, { onExcessProperty: "error" }).pipe(
        Effect.mapError(() => new Enrollments.SourceEnrollmentError({ code: "invalid_request" })),
      ),
  );
  const owner = Effect.fn("NativeSourceSetup.owner")(function* () {
    const value = ownerEvidence;
    if (value._tag === "None")
      return yield* new Enrollments.SourceEnrollmentError({ code: "owner_unqualified" });
    yield* value.value.verifyUnchanged;
  });
  const retained = Effect.fn("NativeSourceSetup.retained")(function* (
    request: Enrollments.NativeSourceSetupRequest,
  ) {
    const proof = yield* source.readProof({ enrollment_id: request.enrollment.enrollment_id });
    const sessionProof = yield* source.readProof({
      session_id: request.enrollment.session.session_id,
    });
    if (!proof && !sessionProof) return null;
    if (
      !proof ||
      !sessionProof ||
      proof.request_sha256 !== Enrollments.sourceSetupDigest(request) ||
      canonicalWorkstreamsSnapshotJson(proof.request) !==
        canonicalWorkstreamsSnapshotJson(request) ||
      canonicalWorkstreamsSnapshotJson(proof) !== canonicalWorkstreamsSnapshotJson(sessionProof)
    )
      return yield* new Enrollments.SourceEnrollmentError({ code: "issuance_conflict" });
    const current = yield* source.getValidatedBySessionId(request.enrollment.session.session_id);
    if (current._tag === "None")
      return yield* new Enrollments.SourceEnrollmentError({ code: "session_conflict" });
    return proof;
  });
  const readback = Effect.fn("NativeSourceSetup.readback")(function* (
    input: Enrollments.NativeSourceSetupRequest,
  ) {
    const request = yield* validate(input);
    yield* owner();
    return yield* db
      .transaction(
        request.expected,
        Effect.gen(function* () {
          const proof = yield* retained(request);
          yield* owner();
          return proof;
        }),
      )
      .pipe(Effect.mapError(mapFailure));
  });
  const plan = Effect.fn("NativeSourceSetup.plan")(function* (
    input: Enrollments.NativeSourceSetupRequest,
  ) {
    const request = yield* validate(input);
    yield* owner();
    return yield* db
      .transaction(
        request.expected,
        Effect.gen(function* () {
          const proof = yield* retained(request);
          if (!proof) {
            yield* source.validateSource(request, true);
            yield* source.validatePins(request, null);
            yield* source.inspectSession(request);
          }
          yield* owner();
          return {
            state: proof ? ("applied" as const) : ("ready" as const),
            request_sha256: Enrollments.sourceSetupDigest(request),
          };
        }),
      )
      .pipe(Effect.mapError(mapFailure));
  });
  const apply = Effect.fn("NativeSourceSetup.apply")(function* (
    input: Enrollments.NativeSourceSetupRequest,
  ) {
    const request = yield* validate(input);
    yield* owner();
    return yield* db
      .transaction(
        request.expected,
        Effect.gen(function* () {
          const existing = yield* retained(request);
          if (existing) {
            yield* owner();
            return existing;
          }
          yield* source.validateSource(request, true);
          yield* source.validatePins(request, null);
          yield* source.reserveSession(request);
          const c = request.enrollment.context;
          const proof: Enrollments.NativeSourceEnrollmentV1 = {
            schema: "jones-code.native-source-enrollment/v1",
            request,
            request_sha256: Enrollments.sourceSetupDigest(request),
            receipt: {
              registry_version: request.expected.registry_version + 1,
              source_binding_version: (request.expected.source_binding?.binding_version ?? 0) + 1,
            },
          };
          yield* registries.appendPlain(
            {
              registry_instance_id: request.expected.registry_instance_id,
              owner_id: request.expected.owner_id,
            },
            "source_bindings",
            {
              provider: "t3",
              source_instance_id: c.source_instance_id,
              binding_version: proof.receipt.source_binding_version,
              authority_namespace: c.authority_namespace,
              store_generation: c.store_generation,
              state: "active",
            },
          );
          yield* records.insert({
            ...request.expected,
            enrollment_id: request.enrollment.enrollment_id,
            session_id: request.enrollment.session.session_id,
            request_sha256: proof.request_sha256,
            proof,
          });
          yield* source.validatePins(request, proof.receipt);
          // Source-owned authority and exclusive owner evidence are checked again before COMMIT.
          yield* source.validateSource(request, true);
          yield* owner();
          return proof;
        }),
      )
      .pipe(Effect.mapError(mapFailure));
  });
  return NativeSourceSetup.of({ plan, apply, readback });
});
/** @public Canonical native setup service; does not acquire owner authority. */
export const layer = Layer.effect(NativeSourceSetup, make);
const decodeJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Enrollments.NativeSourceSetupRequest),
);
/** @public Thin explicit-owner CLI; bootstrap must qualify selected SQL, authority and existing keys. */
export const command = <E, R>(
  bootstrap: (
    selection: InitSelection,
    action: "plan" | "apply" | "readback",
  ) => Layer.Layer<NativeSourceSetup, E, R>,
) =>
  Command.make("source-setup").pipe(
    Command.withSubcommands(
      (["plan", "apply", "readback"] as const).map((action) =>
        Command.make(action, {
          home: Flag.String("home"),
          keyProvider: Flag.String("key-provider"),
          request: Flag.String("request"),
        }).pipe(
          Command.withHandler((flags) =>
            Effect.gen(function* () {
              const request = yield* decodeJson(flags.request, { onExcessProperty: "error" });
              const result = yield* Effect.gen(function* () {
                return yield* (yield* NativeSourceSetup)[action](request);
              }).pipe(
                Effect.provide(
                  bootstrap({ home: flags.home, keyProvider: flags.keyProvider }, action),
                ),
              );
              yield* Console.log(canonicalWorkstreamsSnapshotJson(result));
            }),
          ),
        ),
      ),
    ),
  );
