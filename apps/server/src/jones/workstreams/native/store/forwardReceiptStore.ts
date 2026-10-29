// @effect-diagnostics nodeBuiltinImport:off -- The ledger binds exact UTF-8 bytes independently of public canonical digests.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Db from "./sql.ts";
import * as Registry from "../registry/service.ts";
import * as Appearance from "../registry/appearance.ts";
import * as AppearanceStore from "./registryStore.ts";
import * as Operation from "../operations/forwardedHomeOperation.ts";
import * as Boundary from "../forwarding/peerWriteAdmission.ts";
import { RegistryError } from "../registry/session.ts";
import { canonicalJson } from "../../operations/encoding.ts";
const hash = (body: string) => NodeCrypto.createHash("sha256").update(body).digest("hex");
const decodeRequest = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardRequest);
const decodeResult = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardResult);
const decodeReadback = Schema.decodeUnknownEffect(Forward.WorkstreamsForwardReadbackResult);
const decodeResultBody = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Forward.WorkstreamsForwardResult),
);
const decodeCommandBody = Schema.decodeUnknownEffect(Schema.fromJsonString(Wire.WorkstreamCommand));
interface Row {
  readonly registry_instance_id: string;
  readonly owner_id: string;
  readonly source_environment_id: string;
  readonly forward_id: string;
  readonly request_sha256: string;
  readonly identity_sha256: string;
  readonly wire_sha256: string;
  readonly state: "accepted" | "completed";
  readonly request_envelope: string;
  readonly result_envelope: string | null;
}
export class ForwardReceiptStore extends Context.Service<
  ForwardReceiptStore,
  {
    /** Executes domain services and verifies their authoritative receipts; no raw result insertion API. */
    readonly execute: (
      request: Forward.WorkstreamsForwardRequest,
      exactWireBody: string,
      admission: Boundary.HomeWriteAdmission,
    ) => Effect.Effect<Forward.WorkstreamsForwardResult, RegistryError>;
    readonly readback: (
      identity: Forward.WorkstreamsForwardIdentity,
      admission: Boundary.HomeWriteAdmission,
    ) => Effect.Effect<Forward.WorkstreamsForwardReadbackResult, RegistryError>;
  }
>()("t3/jones/workstreams/native/store/forwardReceiptStore") {}
export const layer = Layer.effect(
  ForwardReceiptStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient,
      db = yield* Db.NativeSql,
      registry = yield* Registry.Registry,
      appearance = yield* Appearance.Appearance,
      store = yield* AppearanceStore.RegistryStore,
      operations = yield* Operation.ForwardedHomeOperation,
      boundary = yield* Boundary.PeerWriteBoundary;
    const checkSource = Effect.fn("ForwardReceiptStore.checkSource")(function* (
      exactWireBody?: string,
    ) {
      const sourceFence = yield* Effect.serviceOption(Boundary.CurrentSourceForwardFence);
      if (Option.isNone(sourceFence))
        return yield* new RegistryError({ code: "source_qualification_unmounted" });
      yield* sourceFence.value.checkCurrent();
      if (
        exactWireBody !== undefined &&
        sourceFence.value.forward_request_sha256 !== hash(exactWireBody)
      )
        return yield* new RegistryError({ code: "source_request_conflict" });
    });
    const readRow = (i: Forward.WorkstreamsForwardIdentity) =>
      sql<Row>`SELECT * FROM jones_workstreams_forward_receipts
    WHERE registry_instance_id=${i.expected_home.registry_instance_id} AND owner_id=${i.expected_home.owner_id}
      AND source_environment_id=${i.source_authority.environment_id} AND forward_id=${i.forward_id}`.pipe(
        Effect.map((rows) => rows[0] ?? null),
      );
    const context = (i: Forward.WorkstreamsForwardIdentity, purpose: string) => ({
      registry_instance_id: i.expected_home.registry_instance_id,
      owner_id: i.expected_home.owner_id,
      purpose,
      record_id: canonicalJson([i.source_authority.environment_id, i.forward_id]),
      revision: 1,
    });
    const validateRetained = Effect.fn("ForwardReceiptStore.validateRetained")(function* (
      row: Row,
      i: Forward.WorkstreamsForwardIdentity,
    ) {
      if (row.request_sha256 !== i.request_sha256 || row.identity_sha256 !== hash(canonicalJson(i)))
        return yield* new RegistryError({ code: "idempotency_conflict" });
      const wire = yield* db.openJson(context(i, "forward_request"), row.request_envelope);
      if (typeof wire !== "string" || hash(wire) !== row.wire_sha256)
        return yield* new RegistryError({ code: "invalid_receipt" });
      const request = yield* Effect.try({
        try: () => Forward.decodeWorkstreamsForwardBody(wire),
        catch: () => new RegistryError({ code: "invalid_receipt" }),
      });
      if (canonicalJson(request.identity) !== canonicalJson(i))
        return yield* new RegistryError({ code: "idempotency_conflict" });
      if (row.state !== "completed" || !row.result_envelope) return { request, wire, result: null };
      const raw = yield* db.openJson(context(i, "forward_result"), row.result_envelope);
      if (typeof raw !== "string") return yield* new RegistryError({ code: "invalid_receipt" });
      const result = yield* decodeResultBody(raw).pipe(
        Effect.mapError(() => new RegistryError({ code: "invalid_receipt" })),
      );
      if (
        result.kind !== request.kind ||
        canonicalJson(result.identity) !== canonicalJson(i) ||
        (request.kind === "operation" &&
          result.kind === "operation" &&
          result.result.request_sha256 !== hash(canonicalJson(request.request.request)))
      )
        return yield* new RegistryError({ code: "invalid_receipt" });
      return { request, wire, result };
    });
    const authoritative = Effect.fn("ForwardReceiptStore.authoritative")(function* (
      request: Forward.WorkstreamsForwardRequest,
      result: Forward.WorkstreamsForwardResult,
      admission: Boundary.HomeWriteAdmission,
    ) {
      if (
        request.kind !== result.kind ||
        canonicalJson(result.identity) !== canonicalJson(request.identity)
      )
        return yield* new RegistryError({ code: "invalid_receipt" });
      const receipts = result.kind === "command" ? [result.result] : [];
      if (result.kind === "operation" && request.kind === "operation") {
        if (result.result.request_sha256 !== hash(canonicalJson(request.request.request)))
          return yield* new RegistryError({ code: "invalid_receipt" });
        for (const child of result.result.children) {
          const lookup = yield* registry.lookup(
            admission.caller,
            child.command_id,
            request.identity.expected_home.server_generation,
          );
          if (
            lookup.kind !== "present" ||
            lookup.receipt.operation !== child.operation ||
            lookup.receipt.state !== child.outcome ||
            !["committed", "rejected"].includes(lookup.receipt.state)
          )
            return yield* new RegistryError({ code: "invalid_receipt" });
        }
      }
      for (const receipt of receipts) {
        const lookup = yield* registry.lookup(
          admission.caller,
          receipt.command_id,
          request.identity.expected_home.server_generation,
        );
        if (lookup.kind !== "present" || canonicalJson(lookup.receipt) !== canonicalJson(receipt))
          return yield* new RegistryError({ code: "invalid_receipt" });
      }
      if (result.kind === "appearance" && request.kind === "appearance") {
        const saved = (yield* store.list(
          request.identity.expected_home,
          "appearance_receipts",
        )).find((r) => r.metadata.command_id === request.request.request.command_id);
        if (
          !saved ||
          saved.metadata.principal_id !== admission.caller.principal_id ||
          canonicalJson(saved.value) !== canonicalJson(result.result)
        )
          return yield* new RegistryError({ code: "invalid_receipt" });
      }
      yield* boundary.checkCurrent(admission);
    });
    const execute = Effect.fn("ForwardReceiptStore.execute")(
      function* (
        input: Forward.WorkstreamsForwardRequest,
        exactWireBody: string,
        admission: Boundary.HomeWriteAdmission,
      ) {
        const request = yield* decodeRequest(input).pipe(
          Effect.mapError(() => new RegistryError({ code: "invalid_request" })),
        );
        const decoded = yield* Effect.try({
          try: () => Forward.decodeWorkstreamsForwardBody(exactWireBody),
          catch: () => new RegistryError({ code: "invalid_request" }),
        });
        const i = request.identity;
        const raw =
          request.kind === "command"
            ? request.request.body
            : request.kind === "operation"
              ? request.request_body
              : request.request.request_body;
        if (
          canonicalJson(decoded) !== canonicalJson(request) ||
          canonicalJson(admission.identity) !== canonicalJson(i) ||
          hash(raw) !== i.request_sha256 ||
          i.parent_operation_id !== null ||
          (request.kind !== "operation" && i.source_operation_id !== i.command_ids[0])
        )
          return yield* new RegistryError({ code: "invalid_request" });
        return yield* db.transaction(
          i.expected_home,
          Effect.gen(function* () {
            yield* checkSource(exactWireBody);
            yield* boundary.checkCurrent(admission);
            const old = yield* readRow(i);
            if (old) {
              const prior = yield* validateRetained(old, i);
              if (prior.wire !== exactWireBody)
                return yield* new RegistryError({ code: "idempotency_conflict" });
              if (!prior.result) return yield* new RegistryError({ code: "forward_in_flight" });
              yield* checkSource(exactWireBody);
              yield* boundary.checkCurrent(admission);
              return prior.result;
            }
            const envelope = yield* db.sealJson(context(i, "forward_request"), exactWireBody);
            yield* sql`INSERT INTO jones_workstreams_forward_receipts ${sql.insert({
              registry_instance_id: i.expected_home.registry_instance_id,
              owner_id: i.expected_home.owner_id,
              source_environment_id: i.source_authority.environment_id,
              forward_id: i.forward_id,
              request_sha256: i.request_sha256,
              identity_sha256: hash(canonicalJson(i)),
              wire_sha256: hash(exactWireBody),
              state: "accepted",
              request_envelope: envelope,
              result_envelope: null,
            })}`;
            let result: Forward.WorkstreamsForwardResult;
            if (request.kind === "command") {
              const command = yield* decodeCommandBody(request.request.body, {
                onExcessProperty: "error",
              }).pipe(Effect.mapError(() => new RegistryError({ code: "invalid_request" })));
              // These raw intents need genuine retained PEER recovery, never a home-local caller.
              if (
                ["verify_reference", "request_native_t3_settlement", "refresh_linked_pr"].includes(
                  command.action.operation,
                ) ||
                request.request.evidence
              )
                return yield* new RegistryError({ code: "peer_recovery_unmounted" });
              const snapshot = yield* registry.snapshot(admission.caller, "workstreams:write");
              const action = command.action;
              if (
                action.operation === "register_reference" &&
                action.identity.provider === "t3" &&
                action.identity.source_instance_id !== i.source_authority.environment_id
              )
                return yield* new RegistryError({ code: "foreign_thread_claim" });
              const referencedId =
                "native_reference_id" in action
                  ? action.native_reference_id
                  : "source_membership_id" in action
                    ? snapshot.state.memberships.get(action.source_membership_id)
                        ?.native_reference_id
                    : "membership_id" in action
                      ? snapshot.state.memberships.get(action.membership_id)?.native_reference_id
                      : undefined;
              const reference = referencedId
                ? snapshot.state.references.get(referencedId)
                : undefined;
              if (
                reference?.identity.provider === "t3" &&
                reference.identity.source_instance_id !== i.source_authority.environment_id
              )
                return yield* new RegistryError({ code: "foreign_thread_claim" });
              const accepted = yield* registry.submit(admission.caller, request.request.body);
              if (accepted.intent)
                return yield* new RegistryError({ code: "peer_recovery_unmounted" });
              yield* boundary.observe(admission, accepted.receipt);
              result = {
                contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
                kind: "command",
                identity: i,
                home: yield* boundary.provenance(admission),
                result: accepted.receipt,
              };
            } else if (request.kind === "appearance") {
              const value = yield* appearance.save(admission.caller, request.request.request);
              result = {
                contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
                kind: "appearance",
                identity: i,
                home: yield* boundary.provenance(admission),
                result: value,
              };
            } else {
              const value = yield* operations.execute(request, admission);
              result = {
                contract: Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
                kind: "operation",
                identity: i,
                home: yield* boundary.provenance(admission),
                result: value,
              };
            }
            const checked = yield* decodeResult(result).pipe(
              Effect.mapError(() => new RegistryError({ code: "invalid_receipt" })),
            );
            yield* authoritative(request, checked, admission);
            const resultEnvelope = yield* db.sealJson(
              context(i, "forward_result"),
              canonicalJson(checked),
            );
            const changed =
              yield* sql`UPDATE jones_workstreams_forward_receipts SET state='completed',result_envelope=${resultEnvelope}
        WHERE registry_instance_id=${i.expected_home.registry_instance_id} AND owner_id=${i.expected_home.owner_id}
        AND source_environment_id=${i.source_authority.environment_id} AND forward_id=${i.forward_id} AND state='accepted' RETURNING forward_id`;
            if (changed.length !== 1)
              return yield* new RegistryError({ code: "idempotency_conflict" });
            yield* checkSource(exactWireBody);
            yield* boundary.checkCurrent(admission);
            return checked;
          }),
        );
      },
      (effect) => effect.pipe(Effect.mapError(Boundary.mapFailure)),
    );
    const readback = Effect.fn("ForwardReceiptStore.readback")(
      function* (
        identity: Forward.WorkstreamsForwardIdentity,
        admission: Boundary.HomeWriteAdmission,
      ) {
        if (canonicalJson(identity) !== canonicalJson(admission.identity))
          return yield* new RegistryError({ code: "not_granted" });
        return yield* db.transaction(
          identity.expected_home,
          Effect.gen(function* () {
            yield* checkSource();
            yield* boundary.checkCurrent(admission);
            const row = yield* readRow(identity),
              common = {
                contract:
                  Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT as typeof Forward.WORKSTREAMS_NATIVE_FORWARDING_CONTRACT,
                identity,
              };
            if (!row) return { ...common, state: "unknown" as const, reason: "not_found" as const };
            const retained = yield* validateRetained(row, identity);
            yield* checkSource(retained.wire);
            yield* boundary.checkCurrent(admission);
            return yield* decodeReadback(
              retained.result
                ? { ...common, state: "present", result: retained.result }
                : { ...common, state: "unknown", reason: "in_flight" },
            ).pipe(Effect.mapError(() => new RegistryError({ code: "invalid_receipt" })));
          }),
        );
      },
      (effect) => effect.pipe(Effect.mapError(Boundary.mapFailure)),
    );
    return ForwardReceiptStore.of({ execute, readback });
  }),
);
