// @effect-diagnostics nodeBuiltinImport:off -- Frozen raw intent and public canonical result have separate digests.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import type { WorkstreamsForwardRequest } from "@t3tools/contracts/jones/workstreamsForwarding";
import {
  WORKSTREAM_OPERATIONS_CONTRACT,
  WorkstreamOperationResult,
  WorkstreamOperationRequest,
} from "@t3tools/contracts/jones/workstreamOperations";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { workstreamChildId } from "../../operations/childIds.ts";
import { canonicalJson } from "../../operations/encoding.ts";
import { planSetPrimary } from "../../operations/planner.ts";
import * as Registry from "../registry/service.ts";
import { RegistryError } from "../registry/session.ts";
import * as Boundary from "../forwarding/peerWriteAdmission.ts";
import * as Supplied from "../forwarding/suppliedEvidence.ts";
type Request = Extract<WorkstreamsForwardRequest, { kind: "operation" }>;
const decodeOperationBody = Schema.decodeUnknownEffect(
  Schema.fromJsonString(WorkstreamOperationRequest),
);
export const digest = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
/** @public Shared frozen ID plan; the source and home must independently check the same list. */
export const forwardChildIds = Effect.fn("ForwardedHomeOperation.childIds")(function* (
  request: Request,
) {
  const op = request.request.request,
    i = request.identity;
  const input = {
    environment_id: i.source_authority.environment_id,
    operation_id: op.operation_id,
    request_sha256: digest(request.request_body),
  };
  const kinds =
    op.kind === "create_workstream"
      ? [{ kind: "cre" as const, attempt: 1 }]
      : [
          { kind: "reg" as const, attempt: 1 },
          { kind: "ver" as const, attempt: 1 },
          ...Array.from({ length: 9 }, (_, n) => ({ kind: "mem" as const, attempt: n + 1 })),
        ];
  return yield* Effect.forEach(kinds, (k) => workstreamChildId({ ...input, ...k }));
});
export class ForwardedHomeOperation extends Context.Service<
  ForwardedHomeOperation,
  {
    /** Caller owns ONE enclosing SQL transaction including the parent ledger. */
    readonly execute: (
      request: Request,
      admission: Boundary.HomeWriteAdmission,
    ) => Effect.Effect<WorkstreamOperationResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/operations/forwardedHomeOperation") {}
export const layer = Layer.effect(
  ForwardedHomeOperation,
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const registry = yield* Registry.Registry,
      boundary = yield* Boundary.PeerWriteBoundary,
      supplied = yield* Supplied.SuppliedEvidence;
    const execute = Effect.fn("ForwardedHomeOperation.execute")(
      function* (request: Request, admission: Boundary.HomeWriteAdmission) {
        const op = request.request.request,
          i = request.identity;
        const retainedOperation = yield* decodeOperationBody(request.request_body, {
          onExcessProperty: "error",
        }).pipe(Effect.mapError(() => new RegistryError({ code: "invalid_request" })));
        if (
          digest(request.request_body) !== i.request_sha256 ||
          canonicalJson(retainedOperation) !== canonicalJson(op) ||
          canonicalJson(admission.identity) !== canonicalJson(i) ||
          i.parent_operation_id !== null ||
          i.source_operation_id !== op.operation_id
        )
          return yield* new RegistryError({ code: "invalid_request" });
        const ids = yield* forwardChildIds(request).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
        );
        if (canonicalJson(ids) !== canonicalJson(i.command_ids))
          return yield* new RegistryError({ code: "invalid_child_ids" });
        const target = op.kind === "set_primary" ? op.target : op.registry;
        if (
          target.registry_id !== i.expected_home.registry_instance_id ||
          target.owner_id !== i.expected_home.owner_id
        )
          return yield* new RegistryError({ code: "registry_mismatch" });
        const at = DateTime.formatIso(yield* DateTime.now);
        const children: WorkstreamOperationResult["children"][number][] = [];
        let observed: WorkstreamOperationResult["observed"] = null;
        const base = () => ({
          contract: WORKSTREAM_OPERATIONS_CONTRACT,
          operation_id: op.operation_id,
          origin: op.origin,
          kind: op.kind,
          request_sha256: digest(canonicalJson(op)),
          observed,
          children,
          accepted_at: at,
          updated_at: at,
          result: null,
          poll_after_ms: null,
          detail: null,
        });
        const send = Effect.fn("ForwardedHomeOperation.child")(function* (
          id: string,
          action: Wire.WorkstreamCommand["action"],
          evidence?: Supplied.QualifiedSuppliedEvidence,
        ) {
          const before = yield* registry.snapshot(admission.caller, "workstreams:write");
          const body = canonicalJson({
            command_id: id,
            expected_server_generation: before.context.server_generation,
            expected_registry_version: before.context.registry_version,
            action,
          });
          const accepted = evidence
            ? yield* supplied.withVerification(
                evidence,
                body,
                registry.submit(admission.caller, body),
              )
            : yield* registry.submit(admission.caller, body);
          if (
            accepted.intent ||
            (accepted.receipt.state !== "committed" && accepted.receipt.state !== "rejected")
          )
            return yield* new RegistryError({ code: "supplied_verification_hook_unmounted" });
          yield* boundary.observe(admission, accepted.receipt);
          children.push({
            command_id: id,
            operation: action.operation,
            outcome: accepted.receipt.state,
          });
          if (accepted.receipt.state !== "committed")
            return yield* new RegistryError({ code: accepted.receipt.error.code });
          return accepted.receipt;
        });
        if (op.kind === "create_workstream") {
          const receipt = yield* send(ids[0]!, {
            operation: "create_workstream",
            ...op.workstream,
          });
          const workstreamId = receipt.effects.workstream_versions[0]?.workstream_id;
          if (!workstreamId) return yield* new RegistryError({ code: "invalid_receipt" });
          return {
            ...base(),
            kind: "create_workstream" as const,
            state: "committed" as const,
            reason: null,
            result: {
              workstream_id: workstreamId,
              membership_id: null,
              registry_version: receipt.registry_version,
            },
          };
        }
        if (op.thread.environment_id !== i.source_authority.environment_id)
          return yield* new RegistryError({ code: "foreign_thread" });
        // Qualify exact supplied bytes and child association before the first child.
        const proof = yield* supplied.qualify(request, admission, ids[1]!);
        // Check the semantic precondition before any registration or verification child.
        // The outer transaction's snapshot is complete, so a conflict need not mutate it.
        const initial = yield* registry.snapshot(admission.caller, "workstreams:write");
        const existing = [...initial.state.references.values()].find(
          (r) =>
            r.identity.provider === "t3" &&
            r.identity.source_instance_id === op.thread.environment_id &&
            r.identity.native_id === op.thread.thread_id &&
            r.identity.resource_kind === "thread" &&
            r.identity.id_kind === "internal",
        );
        const primaries = [...initial.state.memberships.values()].filter(
          (m) =>
            m.native_reference_id === existing?.native_reference_id &&
            !m.closed &&
            m.kind === "primary",
        );
        if (primaries.length > 1) return yield* new RegistryError({ code: "placements_not_ready" });
        const prior = primaries[0];
        observed = prior
          ? {
              kind: "primary",
              workstream_id: prior.workstream_id,
              name: initial.state.workstreams.get(prior.workstream_id)!.name,
            }
          : { kind: "unassigned" };
        if (
          (prior?.workstream_id ?? null) ===
          (op.target.kind === "workstream" ? op.target.workstream_id : null)
        )
          return {
            ...base(),
            kind: "set_primary" as const,
            state: "already_satisfied" as const,
            reason: null,
            observed,
          };
        if (
          (op.expected.kind === "unassigned" && prior) ||
          (op.expected.kind === "primary" && prior?.workstream_id !== op.expected.workstream_id)
        )
          return {
            ...base(),
            kind: "set_primary" as const,
            state: "conflict" as const,
            reason: "precondition_failed" as const,
            observed,
          };
        const reg = yield* send(ids[0]!, {
          operation: "register_reference",
          identity: {
            provider: "t3",
            source_instance_id: op.thread.environment_id,
            native_id: op.thread.thread_id,
            resource_kind: "thread",
            id_kind: "internal",
            account_provenance: { kind: "not_account_scoped" },
          },
          pr_locator: null,
        });
        const refId = reg.effects.native_reference_id;
        let s = yield* registry.snapshot(admission.caller, "workstreams:write");
        let reference = refId ? s.state.references.get(refId) : undefined;
        if (!reference) return yield* new RegistryError({ code: "identity_conflict" });
        // Always reverify against the prequalified source response for this exact parent.
        yield* send(
          ids[1]!,
          {
            operation: "verify_reference",
            native_reference_id: reference.native_reference_id,
            expected_attestation_version: reference.registration.attestation_version,
          },
          proof,
        );
        s = yield* registry.snapshot(admission.caller, "workstreams:write");
        reference = s.state.references.get(reference.native_reference_id)!;
        const memberships = [...s.state.memberships.values()];
        const open = memberships.filter(
          (m) => m.native_reference_id === reference.native_reference_id && !m.closed,
        );
        const e = reference.registration.evidence;
        if (!e || reference.registration.state !== "attested")
          return yield* new RegistryError({ code: "registration_unverified" });
        const placements: Wire.T3ThreadPlacement[] = open.map((m) => ({
          membership_id: m.membership_id,
          workstream_id: m.workstream_id,
          native_reference_id: m.native_reference_id,
          kind: m.kind,
          source_instance_id: op.thread.environment_id,
          native_thread_id: op.thread.thread_id,
          attested_at: reference.registration.attested_at!,
          expires_at: reference.registration.expires_at!,
          source_binding_version: i.source_binding_version,
          attestation_version: reference.registration.attestation_version,
          evidence_sha256: e.evidence_sha256,
          authority_namespace: e.authority_namespace,
          store_generation: e.store_generation,
        }));
        const primary = open.find((m) => m.kind === "primary");
        observed = primary
          ? {
              kind: "primary",
              workstream_id: primary.workstream_id,
              name: s.state.workstreams.get(primary.workstream_id)!.name,
            }
          : { kind: "unassigned" };
        const plan = yield* Effect.try({
          try: () =>
            planSetPrimary({
              request: op,
              registry: op.target,
              reference,
              placements,
              workstreams: [...s.state.workstreams.values()],
              destinationMemberships: memberships,
            }),
          catch: (cause) => new RegistryError({ code: "placements_not_ready", cause }),
        });
        if (plan.kind !== "action")
          return {
            ...base(),
            kind: "set_primary" as const,
            state: plan.kind,
            observed: observed!,
            reason: plan.kind === "conflict" ? ("precondition_failed" as const) : null,
          };
        const receipt = yield* send(ids[2]!, plan.action);
        return {
          ...base(),
          kind: "set_primary" as const,
          state: "committed" as const,
          reason: null,
          result: {
            workstream_id: op.target.kind === "workstream" ? op.target.workstream_id : null,
            membership_id: receipt.effects.membership_ids[0] ?? null,
            registry_version: receipt.registry_version,
          },
        };
      },
      (effect) =>
        effect.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(WorkstreamOperationResult)),
          Effect.mapError(Boundary.mapFailure),
        ),
    );
    return ForwardedHomeOperation.of({ execute });
  }),
);
