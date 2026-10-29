// @effect-diagnostics nodeBuiltinImport:off -- Bind encrypted recovery rows to exact UTF-8 request bytes.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import * as Forward from "@t3tools/contracts/jones/workstreamsForwarding";
import { canonicalWorkstreamsSnapshotJson as canonical } from "@t3tools/contracts/jones/workstreamsSnapshot";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { NativeHome } from "../mode/authorityMode.ts";
import type { ExternalIntent } from "../domain/state.ts";
import * as Db from "../store/sql.ts";
import * as Authority from "./currentAuthority.ts";
import * as Envelope from "./peerEffectEnvelope.ts";

export class PeerOriginError extends Schema.TaggedError<PeerOriginError>()("PeerOriginError", {
  code: Schema.Literals([
    "preparation_in_transaction",
    "not_found",
    "invalid_origin",
    "not_current",
    "unavailable",
  ]),
}) {}
const isPeerOriginError = Schema.is(PeerOriginError);
const failed = (code: PeerOriginError["code"] = "invalid_origin") => new PeerOriginError({ code });
const hash = (body: string) => NodeCrypto.createHash("sha256").update(body, "utf8").digest("hex");
const json = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeForwardResult = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Forward.WorkstreamsForwardResult),
);
const decodeReceipt = Schema.decodeUnknownEffect(
  Schema.Struct({
    receipt: Wire.WorkstreamReceipt,
    request_body: Schema.String,
  }),
);
const decodeIntent = Schema.decodeUnknownEffect(
  Schema.Struct({
    kind: Schema.Literals(["verify_reference", "native_t3_settlement", "refresh_linked_pr"]),
    command: Wire.WorkstreamCommand,
    owner_id: Schema.String,
    actor: Wire.WorkstreamActor,
    authorization: Schema.Struct({
      owner_id: Schema.String,
      actor: Wire.WorkstreamActor,
      grant: Wire.WorkstreamOwnerGrant,
      server_generation: Schema.Int,
    }),
    request_sha256: Schema.String,
    accepted_at: Schema.String,
    admitted_registry_version: Schema.Int,
    reference: Wire.NativeReference,
    source_binding: Schema.Struct({
      provider: Schema.String,
      source_instance_id: Schema.String,
      authority_namespace: Schema.String,
      store_generation: Schema.Int,
      state: Schema.Literals(["active", "revoked"]),
    }),
    membership: Schema.NullOr(Wire.MembershipEpisode),
    // Cache hints do not confer authority. A recovered peer refresh may fetch a fresh representation.
    previous_observation: Schema.NullOr(Wire.WorkstreamPrObservation),
  }),
);
interface ReservationRow {
  readonly registry_instance_id: string;
  readonly owner_id: string;
  readonly command_id: string;
  readonly principal_id: string;
  readonly native_reference_id: string;
  readonly effect: string;
  readonly server_generation: number;
  readonly admitted_registry_version: number;
  readonly state: string;
  readonly envelope: string;
}
interface ReceiptRow {
  readonly registry_instance_id: string;
  readonly owner_id: string;
  readonly command_id: string;
  readonly principal_id: string;
  readonly operation: string;
  readonly request_sha256: string;
  readonly server_generation: number;
  readonly registry_version: number | null;
  readonly accepted_at: string;
  readonly state: string;
  readonly origin: string;
  readonly envelope: string;
}
interface ParentRow {
  readonly registry_instance_id: string;
  readonly owner_id: string;
  readonly source_environment_id: string;
  readonly forward_id: string;
  readonly request_sha256: string;
  readonly identity_sha256: string;
  readonly wire_sha256: string;
  readonly state: string;
  readonly request_envelope: string;
  readonly result_envelope: string | null;
}
type Parent = Extract<Forward.WorkstreamsForwardRequest, { kind: "command" }>;
declare const originBrand: unique symbol;
/** checkOrigin verifies private membership; tool JSON cannot mint this proof. */
export interface ValidatedPeerReservation {
  readonly [originBrand]: true;
  readonly command_id: string;
  readonly envelope: Envelope.StoredPeerEffectV2;
  readonly intent: ExternalIntent;
  readonly receipt: Wire.WorkstreamReceipt;
  readonly parent: Parent;
  readonly parent_result: Extract<Forward.WorkstreamsForwardResult, { kind: "command" }> | null;
}
interface Facts {
  active: boolean;
  readonly token: object;
  readonly sql: SqlClient.SqlClient;
  readonly authority: Authority.PreparedRuntimeAuthority;
  readonly snapshot: () => Effect.Effect<void, PeerOriginError>;
}
const facts = new WeakMap<ValidatedPeerReservation, Facts>();
const OriginScope = Context.Reference<object | undefined>(
  "jones/workstreams/native/runtime/PeerEffectOriginScope",
  { defaultValue: () => undefined },
);
export const checkOrigin = Effect.fn("PeerEffectOrigin.check")(function* (
  proof: ValidatedPeerReservation,
) {
  const f = facts.get(proof);
  const scope = yield* OriginScope;
  const selected = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (!f?.active || scope !== f.token || Option.isNone(selected) || selected.value !== f.sql)
    return yield* failed("not_current");
  yield* Authority.checkPreparedAuthority(f.authority).pipe(
    Effect.mapError(() => failed("not_current")),
  );
});
/** Compare a retained pin receiver with the genuine prepared owner; a tuple is never a proof. */
export const checkOriginReceiver = Effect.fn("PeerEffectOrigin.checkReceiver")(function* (
  proof: ValidatedPeerReservation,
  receiver: {
    readonly environment_id: string;
    readonly authority_namespace: string;
    readonly store_generation: number;
  },
) {
  yield* checkOrigin(proof);
  const current = facts.get(proof)!.authority.authority;
  if (
    current.environmentId !== receiver.environment_id ||
    current.authorityNamespace !== receiver.authority_namespace ||
    current.storeGeneration !== receiver.store_generation
  )
    return yield* failed("not_current");
});
/** Exact current row comparison for a prepared writer; no key refill or absence inference. */
export const checkOriginSnapshot = Effect.fn("PeerEffectOrigin.checkSnapshot")(function* (
  proof: ValidatedPeerReservation,
) {
  yield* checkOrigin(proof);
  yield* facts.get(proof)!.snapshot();
});
export class PeerEffectOrigins extends Context.Service<
  PeerEffectOrigins,
  {
    readonly withOrigin: <A, E, R>(
      commandId: string,
      use: (proof: ValidatedPeerReservation) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | PeerOriginError, R>;
  }
>()("t3/jones/workstreams/native/runtime/peerEffectOrigin/PeerEffectOrigins") {}
const freeze = <A>(value: A): A => {
  if (value && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};
export const layer = Layer.effect(
  PeerEffectOrigins,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const db = yield* Db.NativeSql;
    const home = yield* NativeHome;
    const authority = yield* Authority.CurrentAuthority;
    const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
    const reservation = (id: string) =>
      sql<ReservationRow>`SELECT * FROM jones_workstreams_external_reservations WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND command_id=${id}`.pipe(
        Effect.map((rows) => rows[0] ?? null),
      );
    const receipt = (id: string) =>
      sql<ReceiptRow>`SELECT * FROM jones_workstreams_receipts WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND command_id=${id}`.pipe(
        Effect.map((rows) => rows[0] ?? null),
      );
    const parent = (i: Forward.WorkstreamsForwardIdentity) =>
      sql<ParentRow>`SELECT * FROM jones_workstreams_forward_receipts WHERE registry_instance_id=${scope.registry_instance_id} AND owner_id=${scope.owner_id} AND source_environment_id=${i.source_authority.environment_id} AND forward_id=${i.forward_id}`.pipe(
        Effect.map((rows) => rows[0] ?? null),
      );
    return PeerEffectOrigins.of({
      withOrigin: Effect.fn("PeerEffectOrigins.withOrigin")(function* <A, E, R>(
        commandId: string,
        use: (proof: ValidatedPeerReservation) => Effect.Effect<A, E, R>,
      ) {
        if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
          return yield* failed("preparation_in_transaction");
        const outcome = yield* authority
          .withPrepared((prepared) =>
            Effect.gen(function* () {
              yield* Authority.checkPreparedAuthority(prepared);
              if (
                prepared.authority.environmentId !== home.home_environment_id ||
                home.served_by_environment_id !== home.home_environment_id
              )
                return yield* failed();
              const row = yield* reservation(commandId);
              const saved = yield* receipt(commandId);
              if (!row || !saved) return yield* failed("not_found");
              // Advisory selectors are released before any external key lookup.
              const raw = yield* db.openJson(
                {
                  ...scope,
                  purpose: "external_intent",
                  record_id: commandId,
                  revision: row.admitted_registry_version,
                },
                row.envelope,
              );
              const retained = yield* Envelope.decodeStoredPeerEffect(raw);
              const i = retained.recovery.source_readmission.accepted_forward;
              const wireRow = yield* parent(i);
              if (!wireRow) return yield* failed();
              const payload = yield* decodeReceipt(
                yield* db.openJson(
                  {
                    ...scope,
                    purpose: "receipt",
                    record_id: commandId,
                    revision: saved.registry_version ?? 0,
                  },
                  saved.envelope,
                ),
              );
              const wire = yield* db.openJson(
                {
                  ...scope,
                  purpose: "forward_request",
                  record_id: canonical([i.source_authority.environment_id, i.forward_id]),
                  revision: 1,
                },
                wireRow.request_envelope,
              );
              if (typeof wire !== "string") return yield* failed();
              const forward = yield* Effect.try({
                try: () => Forward.decodeWorkstreamsForwardBody(wire),
                catch: () => failed(),
              });
              const intent = yield* decodeIntent(yield* json(retained.intent_body));
              const r = payload.receipt;
              if (
                forward.kind !== "command" ||
                forward.request.evidence !== undefined ||
                forward.request.body !== retained.request_body ||
                canonical(forward.identity) !== canonical(i) ||
                i.source_operation_id !== commandId ||
                i.parent_operation_id !== null ||
                i.expected_home.registry_instance_id !== home.registry_instance_id ||
                i.expected_home.owner_id !== home.owner_id ||
                i.expected_home.home_environment_id !== home.home_environment_id ||
                i.expected_home.server_generation !== home.server_generation ||
                i.canonical_origin !== home.canonical_origin ||
                row.command_id !== commandId ||
                row.principal_id !== i.principal_id ||
                row.effect !== intent.command.action.operation ||
                row.native_reference_id !== intent.reference.native_reference_id ||
                row.server_generation !== home.server_generation ||
                row.admitted_registry_version !== intent.admitted_registry_version ||
                !["active", "released"].includes(row.state) ||
                saved.origin !== "native" ||
                saved.command_id !== commandId ||
                saved.principal_id !== i.principal_id ||
                saved.operation !== row.effect ||
                saved.request_sha256 !== retained.request_sha256 ||
                saved.server_generation !== home.server_generation ||
                (r.state === "committed" && r.registry_version !== saved.registry_version) ||
                saved.state !== r.state ||
                saved.accepted_at !== r.accepted_at ||
                payload.request_body !== retained.request_body ||
                r.command_id !== commandId ||
                r.owner_id !== home.owner_id ||
                r.actor.principal_id !== i.principal_id ||
                r.operation !== row.effect ||
                r.request_sha256 !== retained.request_sha256 ||
                r.server_generation !== home.server_generation ||
                r.accepted_at !== intent.accepted_at ||
                wireRow.request_sha256 !== retained.request_sha256 ||
                wireRow.identity_sha256 !== hash(canonical(i)) ||
                wireRow.wire_sha256 !== hash(wire) ||
                wireRow.wire_sha256 !== retained.forward_request_sha256 ||
                (row.state === "active" &&
                  (!["pending", "unresolved"].includes(r.state) ||
                    wireRow.state !== "accepted" ||
                    wireRow.result_envelope !== null)) ||
                (row.state === "released" &&
                  (!["committed", "rejected"].includes(r.state) ||
                    wireRow.state !== "completed" ||
                    wireRow.result_envelope === null)) ||
                intent.source_binding.provider !== intent.reference.identity.provider ||
                intent.source_binding.source_instance_id !==
                  intent.reference.identity.source_instance_id
              )
                return yield* failed();
              let parentResult: Extract<
                Forward.WorkstreamsForwardResult,
                { kind: "command" }
              > | null = null;
              if (wireRow.state === "completed") {
                if (!wireRow.result_envelope) return yield* failed();
                const body = yield* db.openJson(
                  {
                    ...scope,
                    purpose: "forward_result",
                    record_id: canonical([i.source_authority.environment_id, i.forward_id]),
                    revision: 1,
                  },
                  wireRow.result_envelope,
                );
                if (typeof body !== "string") return yield* failed();
                const result = yield* decodeForwardResult(body);
                if (
                  result.kind !== "command" ||
                  canonical(result.identity) !== canonical(i) ||
                  canonical(result.result) !== canonical(r) ||
                  result.home.incoming_peer_pin_id !== retained.recovery.incoming_peer_pin_id ||
                  result.home.incoming_peer_pin_revision !==
                    retained.recovery.incoming_peer_pin_revision
                )
                  return yield* failed();
                parentResult = result;
              }
              const snapshot = Effect.gen(function* () {
                const currentRow = yield* reservation(commandId);
                const currentReceipt = yield* receipt(commandId);
                const currentParent = yield* parent(i);
                if (
                  canonical(currentRow) !== canonical(row) ||
                  canonical(currentReceipt) !== canonical(saved) ||
                  canonical(currentParent) !== canonical(wireRow)
                )
                  return yield* failed("not_current");
              }).pipe(Effect.mapError(() => failed("not_current")));
              yield* sql.withTransaction(snapshot);
              const proof = freeze({
                command_id: commandId,
                envelope: retained,
                intent,
                receipt: r,
                parent: forward,
                parent_result: parentResult,
              }) as ValidatedPeerReservation;
              const f: Facts = {
                active: true,
                token: Object.freeze({}),
                sql,
                authority: prepared,
                snapshot: () => snapshot,
              };
              facts.set(proof, f);
              return yield* use(proof).pipe(
                Effect.provideService(OriginScope, f.token),
                Effect.ensuring(
                  Effect.sync(() => {
                    f.active = false;
                  }),
                ),
                Effect.exit,
              );
            }),
          )
          .pipe(
            Effect.mapError((error) => (isPeerOriginError(error) ? error : failed("unavailable"))),
          );
        return yield* Exit.isFailure(outcome)
          ? Effect.failCause(outcome.cause)
          : Effect.succeed(outcome.value);
      }),
    });
  }),
);
