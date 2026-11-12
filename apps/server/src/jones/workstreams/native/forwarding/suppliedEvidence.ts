// @effect-diagnostics nodeBuiltinImport:off -- Exact child reservation bytes are hashed synchronously.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import type { WorkstreamsForwardRequest } from "@t3tools/contracts/jones/workstreamsForwarding";
import type { WorkstreamsPeerSessionPinV1 } from "@t3tools/contracts/jones/workstreamsPeerAuthority";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Domain from "../domain/state.ts";
import * as Attestation from "../providers/localAttestation.ts";
import { RegistryError } from "../registry/session.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import * as Boundary from "./peerWriteAdmission.ts";

/** Lookup-only source qualification of the immutable accepted forward, performed BEFORE SQL.
 * The current source authority/enrollment/original caller and exact request must match.
 * The remote response is a fresh observation. checkCurrent rechecks local scope, original
 * registration and captured link only; it never performs a remote callback inside SQL.
 */
export class SourceForwardQualification extends Context.Service<
  SourceForwardQualification,
  {
    /** Unknown permits fixed invocation negotiation only; it cannot release retained data. */
    readonly prepareReadback: (
      request: import("@t3tools/contracts/jones/workstreamsForwarding").WorkstreamsForwardReadbackRequest,
      receivingPin: WorkstreamsPeerSessionPinV1,
    ) => Effect.Effect<
      | {
          readonly state: "authorized";
          readonly forward_request_sha256: string;
          readonly checkCurrent: () => Effect.Effect<void, RegistryError>;
        }
      | {
          readonly state: "unknown";
          readonly reason: Extract<
            import("@t3tools/contracts/jones/workstreamsForwarding").WorkstreamsForwardReadbackResult,
            { state: "unknown" }
          >["reason"];
        },
      RegistryError
    >;
    readonly prepare: (
      request: WorkstreamsForwardRequest,
      receivingPin: WorkstreamsPeerSessionPinV1,
    ) => Effect.Effect<
      {
        readonly forward_request_sha256: string;
        readonly checkCurrent: () => Effect.Effect<void, RegistryError>;
      },
      RegistryError
    >;
  }
>()("t3/jones/workstreams/native/forwarding/suppliedEvidence/SourceForwardQualification") {}

/** Root's narrow Registry hook. Only SuppliedEvidence.withVerification installs this scope.
 * It is consumed before persistence by the pure decider; never enqueue this verification.
 */
export class AtomicSuppliedVerification extends Context.Service<
  AtomicSuppliedVerification,
  {
    readonly resolve: (
      intent: Domain.ExternalIntent,
      body: string,
    ) => Effect.Effect<Domain.RegistryExternalResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/suppliedEvidence/AtomicSuppliedVerification") {}
const evidenceBrand: unique symbol = Symbol("SuppliedEvidence");
export interface QualifiedSuppliedEvidence {
  readonly [evidenceBrand]: true;
}
export class SuppliedEvidence extends Context.Service<
  SuppliedEvidence,
  {
    readonly qualify: (
      request: Extract<WorkstreamsForwardRequest, { kind: "operation" }>,
      admission: Boundary.HomeWriteAdmission,
      commandId: string,
    ) => Effect.Effect<QualifiedSuppliedEvidence, RegistryError>;
    readonly withVerification: <A, E, R>(
      proof: QualifiedSuppliedEvidence,
      body: string,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | RegistryError, R>;
  }
>()("t3/jones/workstreams/native/forwarding/suppliedEvidence") {}
const denied = () => new RegistryError({ code: "supplied_evidence_mismatch" });
export const layer = Layer.effect(
  SuppliedEvidence,
  Effect.gen(function* () {
    const boundary = yield* Boundary.PeerWriteBoundary;
    type Evidence = {
      request: Extract<WorkstreamsForwardRequest, { kind: "operation" }>;
      admission: Boundary.HomeWriteAdmission;
      commandId: string;
      evidence: NonNullable<
        Extract<WorkstreamsForwardRequest, { kind: "operation" }>["request"]["evidence"]
      >;
    };
    const proofs = new WeakMap<QualifiedSuppliedEvidence, Evidence>();
    const qualify = Effect.fn("SuppliedEvidence.qualify")(function* (
      request: Extract<WorkstreamsForwardRequest, { kind: "operation" }>,
      admission: Boundary.HomeWriteAdmission,
      commandId: string,
    ) {
      yield* boundary.checkCurrent(admission);
      if (
        request.request.request.kind !== "set_primary" ||
        !request.request.evidence ||
        canonicalJson(admission.identity) !== canonicalJson(request.identity) ||
        !request.identity.command_ids.includes(commandId)
      )
        return yield* denied();
      const op = request.request.request,
        source = request.identity.source_authority;
      const expected = {
        owner_id: request.identity.expected_home.owner_id,
        principal_id: request.identity.principal_id,
        identity: {
          provider: "t3" as const,
          source_instance_id: source.environment_id,
          native_id: op.thread.thread_id,
          resource_kind: "thread" as const,
          id_kind: "internal" as const,
          account_provenance: { kind: "not_account_scoped" as const },
        },
        expected_authority_namespace: source.authority_namespace,
        expected_store_generation: source.store_generation,
      };
      const response = yield* Attestation.validateAttestation(
        expected,
        request.request.evidence,
      ).pipe(Effect.mapError(denied));
      if (response.state !== "attested") return yield* denied();
      const proof: QualifiedSuppliedEvidence = Object.freeze({ [evidenceBrand]: true as const });
      proofs.set(proof, {
        request: structuredClone(request),
        admission,
        commandId,
        evidence: response,
      });
      return proof;
    });
    const withVerification = Effect.fn("SuppliedEvidence.withVerification")(function* <A, E, R>(
      proof: QualifiedSuppliedEvidence,
      body: string,
      effect: Effect.Effect<A, E, R>,
    ) {
      const p = proofs.get(proof);
      if (!p) return yield* denied();
      yield* boundary.checkCurrent(p.admission);
      const resolve = Effect.fn("SuppliedEvidence.resolve")(function* (
        intent: Domain.ExternalIntent,
        exact: string,
      ) {
        yield* boundary.checkCurrent(p.admission);
        const command = intent.command,
          action = command.action,
          e = p.evidence;
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        const op = p.request.request.request;
        if (
          op.kind !== "set_primary" ||
          exact !== body ||
          intent.kind !== "verify_reference" ||
          action.operation !== "verify_reference" ||
          command.command_id !== p.commandId ||
          intent.request_sha256 !== NodeCrypto.createHash("sha256").update(exact).digest("hex") ||
          intent.owner_id !== p.request.identity.expected_home.owner_id ||
          intent.actor.principal_id !== p.request.identity.principal_id ||
          command.expected_server_generation !==
            p.request.identity.expected_home.server_generation ||
          action.native_reference_id !== intent.reference.native_reference_id ||
          canonicalJson(intent.reference.identity) !== canonicalJson(e.request.identity) ||
          intent.reference.identity.native_id !== op.thread.thread_id ||
          intent.reference.identity.source_instance_id !==
            p.request.identity.source_authority.environment_id ||
          intent.source_binding.state !== "active" ||
          intent.source_binding.authority_namespace !== e.request.expected_authority_namespace ||
          intent.source_binding.store_generation !== e.request.expected_store_generation ||
          Date.parse(e.attested_at) > now ||
          Date.parse(e.expires_at) <= now
        )
          return yield* denied();
        const registration: Wire.NativeReference["registration"] = {
          state: "attested",
          attestation_version: action.expected_attestation_version + 1,
          evidence: e.attestation,
          attested_at: e.attested_at,
          expires_at: e.expires_at,
        };
        return { registration };
      });
      return yield* effect.pipe(Effect.provideService(AtomicSuppliedVerification, { resolve }));
    });
    return SuppliedEvidence.of({ qualify, withVerification });
  }),
);
