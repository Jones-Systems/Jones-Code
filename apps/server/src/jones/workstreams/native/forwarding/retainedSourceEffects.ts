// @effect-diagnostics nodeBuiltinImport:off -- Exact retained command and provider-request byte digests.
import * as NodeCrypto from "node:crypto";
import { WorkstreamCommand, WorkstreamsNativeSettlementRequest } from "@t3tools/contracts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Wire from "@t3tools/contracts/jones/workstreamsRetainedSourceEffect";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  NativeProviderDispatchAdmission,
  NativeProviderDispatchRefused,
} from "../../nativeProvider/dispatchAdmission.ts";
import * as Provider from "../providers/localAttestation.ts";
import { RegistryError } from "../registry/session.ts";
import * as Members from "../runtime/memberEnrollment.ts";
import * as Pins from "../runtime/peerSessionPins.ts";
import * as Operations from "../store/operationStore.ts";
import * as Source from "./sourceReadmissionPort.ts";
import { RetainedSourceEffects } from "./retainedSourceEffectPort.ts";

const sha = (body: string) => NodeCrypto.createHash("sha256").update(body, "utf8").digest("hex");
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const refused = (code = "request_conflict") => new RegistryError({ code });
const decodeRequest = Schema.decodeUnknownEffect(Wire.WorkstreamsRetainedSourceEffectRequestV1, {
  onExcessProperty: "error",
});
const decodeResult = Schema.decodeUnknownEffect(Wire.WorkstreamsRetainedSourceEffectResultV1, {
  onExcessProperty: "error",
});
const decodeForward = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Forward.WorkstreamsForwardRequest),
  { onExcessProperty: "error" },
);
const decodeCommand = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkstreamCommand), {
  onExcessProperty: "error",
});
const encodeSettlement = Schema.encodeEffect(
  Schema.fromJsonString(WorkstreamsNativeSettlementRequest),
);
const denyReasons = new Set([
  "not_found",
  "not_granted",
  "caller_changed",
  "source_changed",
  "home_changed",
  "request_conflict",
  "historical_evidence_missing",
]);
const unavailableReasons = new Set([
  "authority_unavailable",
  "home_unreachable",
  "key_unavailable",
  "provider_unconfigured",
  "unsupported_protocol",
]);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Operations.OperationStore;
  const readmission = yield* Source.SourceReadmission;
  const members = yield* Members.MemberEnrollments;
  const pins = yield* Pins.PeerSessionPins;
  const provider = yield* Provider.NativeProvider;
  return RetainedSourceEffects.of({
    invoke: Effect.fn("RetainedSourceEffects.invoke")(function* (input, exactInvocation) {
      // Never hold an application writer while authenticating, preparing crypto or invoking a provider.
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* refused("preparation_in_transaction");
      const request = yield* decodeRequest(input).pipe(
        Effect.mapError(() => refused("invalid_request")),
      );
      const execute = Effect.gen(function* () {
        if (
          exactInvocation.thread ||
          !exactInvocation.client?.linked ||
          exactInvocation.client.remoteOrigin
        )
          return yield* refused("home_changed");
        const qualified = yield* pins
          .forInvocation(exactInvocation, { role: "home_source" })
          .pipe(Effect.mapError(() => refused("home_changed")));
        const observe = Effect.fn("RetainedSourceEffects.observe")(function* () {
          const result = yield* readmission.lookupRaw(request.source_readmission, exactInvocation);
          if (result.state !== "authorized") return yield* refused(result.reason);
          if (
            !same(result.request, request.source_readmission) ||
            !Pins.ceilingAllows(result.current_ceiling, "workstreams:write")
          )
            return yield* refused("not_granted");
          return result;
        });
        const first = yield* observe();
        const row = yield* store
          .withPrepared(
            [{ kind: "read", id: request.source_readmission.accepted_forward.source_operation_id }],
            (prepared) =>
              prepared.readMemberForward(
                request.source_readmission.accepted_forward.source_operation_id,
              ),
          )
          .pipe(
            Effect.mapError((cause) =>
              refused(
                cause._tag === "KeyUnavailable" ? "key_unavailable" : "historical_evidence_missing",
              ),
            ),
          );
        if (!row) return yield* refused("not_found");
        const forward = yield* decodeForward(row.member_forward.forward_request_body).pipe(
          Effect.mapError(() => refused("historical_evidence_missing")),
        );
        if (forward.kind !== "command" || forward.request.evidence !== undefined)
          return yield* refused();
        const caller = row.member_forward.recorded_caller;
        const a = caller.association;
        const command = yield* decodeCommand(forward.request.body).pipe(
          Effect.mapError(() => refused("historical_evidence_missing")),
        );
        const target = caller.local.target;
        const p = request.request;
        if (
          !same(forward.identity, request.source_readmission.accepted_forward) ||
          sha(row.member_forward.forward_request_body) !==
            row.member_forward.forward_request_sha256 ||
          forward.request.body !== row.request_body ||
          sha(forward.request.body) !== request.source_readmission.command_request_sha256 ||
          command.command_id !== request.source_readmission.command_id ||
          request.receiving_home_pin_id !== a.home_pin_id ||
          request.receiving_home_pin_revision !== a.home_pin_bound_revision ||
          qualified.pin.pin_id !== a.member_pin_id ||
          qualified.pin.revision !== a.pin_bound_revision ||
          target.kind !== "thread" ||
          target.environment_id !== a.source.environment_id ||
          p.owner_id !== a.home.owner_id ||
          p.principal_id !== a.principal_id ||
          p.identity.provider !== "t3" ||
          p.identity.resource_kind !== "thread" ||
          p.identity.id_kind !== "internal" ||
          p.identity.account_provenance.kind !== "not_account_scoped" ||
          p.identity.source_instance_id !== a.source.environment_id ||
          p.identity.native_id !== target.thread_id ||
          p.expected_authority_namespace !== a.source.authority_namespace ||
          p.expected_store_generation !== a.source.store_generation
        )
          return yield* refused();
        if (request.kind === "attest") {
          if (command.action.operation !== "verify_reference") return yield* refused();
        } else if (
          command.action.operation !== "request_native_t3_settlement" ||
          request.request.command_id !== command.command_id ||
          request.request.native_reference_id !== command.action.native_reference_id ||
          request.request.native_action !== command.action.native_action ||
          request.request.request_sha256 !== sha(forward.request.body)
        ) {
          return yield* refused();
        }
        const readBinding = Effect.fn("RetainedSourceEffects.binding")(function* () {
          const value = yield* members
            .getBySessionId(a.enrollment_session_id)
            .pipe(Effect.mapError(() => refused("source_changed")));
          if (Option.isNone(value)) return yield* refused("source_changed");
          const b = value.value;
          if (
            b.enrollment_id !== a.enrollment_id ||
            b.session_id !== a.enrollment_session_id ||
            b.registry_origin !== a.canonical_origin ||
            b.owner_id !== a.home.owner_id ||
            b.principal_id !== a.principal_id ||
            b.source_instance_id !== a.source.environment_id ||
            b.authority_namespace !== a.source.authority_namespace ||
            b.store_generation !== a.source.store_generation
          )
            return yield* refused("source_changed");
          return b;
        });
        const binding = yield* readBinding();
        let active = true;
        const check = Effect.fn("RetainedSourceEffects.check")(function* () {
          if (!active) return yield* refused("caller_changed");
          // This closure retains the ORIGINAL authenticator registration, even if the same
          // invocation object is subsequently re-authenticated with identical session facts.
          yield* qualified.checkCurrent().pipe(Effect.mapError(() => refused("home_changed")));
          const now = yield* observe();
          if (
            !same(now.original_ceiling, first.original_ceiling) ||
            !same(now.current_ceiling, first.current_ceiling) ||
            !same(yield* readBinding(), binding)
          )
            return yield* refused("caller_changed");
        });
        return yield* Effect.gen(function* () {
          yield* check();
          const result =
            request.kind === "attest"
              ? yield* Provider.validateAttestation(
                  request.request,
                  yield* provider.attest(binding, request.request),
                ).pipe(Effect.mapError(() => refused("authority_unavailable")))
              : yield* encodeSettlement(request.request).pipe(
                  Effect.mapError(() => refused()),
                  Effect.flatMap((bytes) =>
                    request.kind === "settle_lookup"
                      ? provider.lookup(binding, request.request, sha(bytes))
                      : provider.settle(binding, request.request, sha(bytes)),
                  ),
                );
          yield* check();
          return yield* decodeResult({
            contract: request.contract,
            request,
            state: "result",
            result,
          }).pipe(Effect.mapError(() => refused("authority_unavailable")));
        }).pipe(
          Effect.provideService(NativeProviderDispatchAdmission, (actualBinding, actualRequest) =>
            Effect.gen(function* () {
              if (
                request.kind !== "settle" ||
                !same(actualBinding, binding) ||
                !same(actualRequest, request.request)
              )
                return yield* refused();
              yield* check();
            }).pipe(Effect.mapError(() => new NativeProviderDispatchRefused())),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              active = false;
            }),
          ),
        );
      });
      return yield* execute.pipe(
        Effect.catch((error) =>
          decodeResult({
            contract: request.contract,
            request,
            state:
              unavailableReasons.has(error.code) || !denyReasons.has(error.code)
                ? "unavailable"
                : "denied",
            reason:
              denyReasons.has(error.code) || unavailableReasons.has(error.code)
                ? error.code
                : "authority_unavailable",
          }).pipe(Effect.mapError(() => refused("invalid_response"))),
        ),
      );
    }),
  });
});

/** Genuine source lookup/admission and the application provider are required; there is no dispatch fallback. */
export const layer = Layer.effect(RetainedSourceEffects, make);
