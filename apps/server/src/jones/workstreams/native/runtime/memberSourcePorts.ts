import { WorkstreamCommand, WorkstreamNativeIdentity } from "@t3tools/contracts";
import { WorkstreamOperationRequest } from "@t3tools/contracts/jones/workstreamOperations";
import {
  WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
  WorkstreamsHomeReadResult,
} from "@t3tools/contracts/jones/workstreamsHomeReadResult";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Environment from "../../../../environment/ServerEnvironment.ts";
import * as Forwarder from "../forwarding/forwarder.ts";
import * as Operations from "../operations/memberOperations.ts";
import * as Reads from "../read/memberRead.ts";
import { RegistryError } from "../registry/session.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import * as Attestation from "../providers/localAttestation.ts";
import * as Admission from "./memberAdmission.ts";
import * as Enrollments from "./memberEnrollment.ts";
import type { MemberWriteTarget } from "./memberRecordedCaller.ts";

const decodeCommand = Schema.decodeUnknownEffect(WorkstreamCommand, {
  onExcessProperty: "error",
});
const decodeOperation = Schema.decodeUnknownEffect(WorkstreamOperationRequest, {
  onExcessProperty: "error",
});
const decodeRead = Schema.decodeUnknownEffect(WorkstreamsHomeReadResult, {
  onExcessProperty: "error",
});
const invalid = (cause: unknown) => new RegistryError({ code: "invalid_request", cause });
const refused = () => new RegistryError({ code: "forbidden" });

const makeTargets = Effect.gen(function* () {
  const environment = yield* Environment.ServerEnvironmentIdentity;
  const reads = yield* Reads.MemberReads;
  const environmentId = yield* environment.getEnvironmentId;
  const target = (
    identity: typeof WorkstreamNativeIdentity.Type,
  ): Effect.Effect<MemberWriteTarget, RegistryError> =>
    identity.provider !== "t3"
      ? Effect.succeed({ kind: "metadata" })
      : identity.source_instance_id !== environmentId ||
          identity.resource_kind !== "thread" ||
          identity.id_kind !== "internal" ||
          identity.account_provenance.kind !== "not_account_scoped"
        ? Effect.fail(refused())
        : Effect.succeed({
            kind: "thread",
            environment_id: environmentId,
            thread_id: identity.native_id,
          });
  const read = Effect.fn("MemberCommandTargets.read")(function* (
    request: Parameters<Reads.MemberReads["Service"]["read"]>[0],
  ) {
    const result = yield* reads.read(request);
    return yield* decodeRead({
      contract: WORKSTREAMS_HOME_READ_RESULT_CONTRACT,
      ...result,
    }).pipe(Effect.mapError((cause) => new RegistryError({ code: "invalid_response", cause })));
  });
  return Forwarder.MemberCommandTargets.of({
    resolve: Effect.fn("MemberCommandTargets.resolve")(function* (input) {
      const command = yield* decodeCommand(input).pipe(Effect.mapError(invalid));
      const action = command.action;
      if (action.operation === "register_reference") return yield* target(action.identity);
      let referenceId: string | null =
        "native_reference_id" in action ? action.native_reference_id : null;
      if ("membership_id" in action || "source_membership_id" in action) {
        const workstreamId =
          "workstream_id" in action ? action.workstream_id : action.source_workstream_id;
        const membershipId =
          "membership_id" in action ? action.membership_id : action.source_membership_id;
        let cursor: string | undefined;
        const seen = new Set<string>();
        // Match complete-read bounds; a missing or truncated lookup never becomes metadata authority.
        for (let page = 0; page < 1000; page++) {
          const result = yield* read({
            route: "memberships",
            params: { workstream_id: workstreamId, limit: 100, ...(cursor ? { cursor } : {}) },
          });
          if (result.route !== "memberships")
            return yield* new RegistryError({ code: "invalid_response" });
          const found = result.value.items.find((row) => row.membership_id === membershipId);
          if (found) {
            if (found.workstream_id !== workstreamId)
              return yield* new RegistryError({ code: "invalid_response" });
            referenceId = found.native_reference_id;
            break;
          }
          const next = result.value.next_cursor;
          if (next === null) return yield* new RegistryError({ code: "not_found" });
          if (seen.has(next)) return yield* new RegistryError({ code: "cursor_invalid" });
          seen.add(next);
          cursor = next;
        }
        if (referenceId === null) return yield* new RegistryError({ code: "unavailable" });
      }
      if (referenceId === null) return { kind: "metadata" };
      const result = yield* read({
        route: "reference",
        params: { native_reference_id: referenceId },
      });
      if (
        result.route !== "reference" ||
        result.value.reference.native_reference_id !== referenceId
      )
        return yield* new RegistryError({ code: "invalid_response" });
      return yield* target(result.value.reference.identity);
    }),
  });
});

const makeEvidence = Effect.gen(function* () {
  const environment = yield* Environment.ServerEnvironmentIdentity;
  const environmentId = yield* environment.getEnvironmentId;
  const admission = yield* Admission.MemberAdmission;
  const members = yield* Enrollments.MemberEnrollments;
  const provider = yield* Attestation.NativeProvider;
  return Operations.MemberOperationEvidence.of({
    capture: Effect.fn("MemberOperationEvidence.capture")(function* (input) {
      const request = yield* decodeOperation(input).pipe(Effect.mapError(invalid));
      if (request.kind !== "set_primary" || request.thread.environment_id !== environmentId)
        return yield* refused();
      const record = yield* admission.captureWrite({ kind: "thread", ...request.thread });
      const admitted = yield* admission.readmitWrite(record);
      const association = record.association;
      if (
        record.local.environment_id !== environmentId ||
        association.source.environment_id !== environmentId ||
        request.target.registry_id !== association.home.registry_instance_id ||
        request.target.owner_id !== association.home.owner_id ||
        canonicalJson(record.local.origin) !== canonicalJson(request.origin) ||
        canonicalJson(record.local.target) !== canonicalJson({ kind: "thread", ...request.thread })
      )
        return yield* refused();
      const current = yield* members
        .getBySessionId(association.enrollment_session_id)
        .pipe(Effect.mapError((cause) => new RegistryError({ code: "forbidden", cause })));
      if (Option.isNone(current)) return yield* refused();
      const binding = current.value;
      if (
        binding.source_instance_id !== environmentId ||
        binding.authority_namespace !== association.source.authority_namespace ||
        binding.store_generation !== association.source.store_generation ||
        binding.owner_id !== association.home.owner_id ||
        binding.principal_id !== association.principal_id ||
        binding.enrollment_id !== association.enrollment_id ||
        binding.session_id !== association.enrollment_session_id ||
        binding.registry_origin !== association.canonical_origin
      )
        return yield* refused();
      yield* admitted.checkCurrent();
      const attestationRequest = {
        owner_id: binding.owner_id,
        principal_id: binding.principal_id,
        identity: {
          provider: "t3" as const,
          resource_kind: "thread" as const,
          source_instance_id: environmentId,
          id_kind: "internal" as const,
          native_id: request.thread.thread_id,
          account_provenance: { kind: "not_account_scoped" as const },
        },
        expected_authority_namespace: binding.authority_namespace,
        expected_store_generation: binding.store_generation,
      };
      const response = yield* Attestation.validateAttestation(
        attestationRequest,
        yield* provider.attest(binding, attestationRequest),
      ).pipe(Effect.mapError((cause) => new RegistryError({ code: cause.code, cause })));
      const refreshed = yield* admission.readmitWrite(record);
      if (canonicalJson(yield* refreshed.checkCurrent()) !== canonicalJson(admitted.limits))
        return yield* refused();
      yield* admitted.checkCurrent();
      if (response.state !== "attested")
        return yield* new RegistryError({ code: "provider_unavailable" });
      return response;
    }),
  });
});

/** Request-scoped adapters consume validated member reads, enrollment and the genuine source provider. */
export const layer = Layer.unwrap(
  Effect.sync(() =>
    Layer.mergeAll(
      Layer.effect(Forwarder.MemberCommandTargets, makeTargets),
      Layer.effect(Operations.MemberOperationEvidence, makeEvidence),
    ),
  ),
);

/** Preserve the first original write caller across evidence acquisition and durable acceptance. */
export const requestAdmissionLayer = Layer.unwrap(
  Effect.sync(() =>
    Layer.effect(
      Admission.MemberAdmission,
      Effect.gen(function* () {
        const admission = yield* Admission.MemberAdmission;
        const serial = yield* Semaphore.make(1);
        let original: Admission.MemberRecordedWriteCallerV2 | null = null;
        return Admission.MemberAdmission.of({
          ...admission,
          captureWrite: (target) =>
            serial.withPermits(1)(
              Effect.gen(function* () {
                if (original === null) original = yield* admission.captureWrite(target);
                if (canonicalJson(original.local.target) !== canonicalJson(target))
                  return yield* refused();
                yield* (yield* admission.readmitWrite(original)).checkCurrent();
                return original;
              }),
            ),
        });
      }),
    ),
  ),
);
