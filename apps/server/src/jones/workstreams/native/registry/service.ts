// @effect-diagnostics nodeBuiltinImport:off -- Transaction-local UUID allocation and exact request-byte hashing.
import * as NodeCrypto from "node:crypto";
import * as Wire from "@t3tools/contracts";
import type { WorkstreamsNativeReceiptLookup } from "@t3tools/contracts/jones/workstreamsNative";
import type {
  WorkstreamsLegacyReceiptLookupRequest,
  WorkstreamsLegacyReceiptLookupResult,
} from "@t3tools/contracts/jones/workstreamsLegacyReceipts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Option from "effect/Option";
import * as Recorded from "../runtime/recordedCaller.ts";
import * as Supplied from "../forwarding/suppliedEvidence.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { NativeHome, type AdmittedCaller } from "../mode/authorityMode.ts";
import * as Session from "./session.ts";
import * as RegistryStore from "../store/registryStore.ts";
import * as ReceiptStore from "../store/receiptStore.ts";
import * as HistoryStore from "../store/historyStore.ts";
import * as EffectStore from "../store/effectStore.ts";
import * as NativeSql from "../store/sql.ts";
import * as IdentityIndex from "../crypto/identityIndex.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Domain from "../domain/state.ts";
import * as Decider from "../domain/decider.ts";
import { WorkstreamDomainError } from "../domain/errors.ts";
import { canonicalJson, validateCommand } from "../domain/validation.ts";
import type { DomainIdSource } from "../domain/ids.ts";

export interface Snapshot {
  readonly context: RegistryStore.RegistryContext;
  readonly authorization: Domain.AuthorizedOwner;
  readonly state: Domain.WorkstreamDomainState;
}
export interface AcceptedCommand {
  readonly receipt: Wire.WorkstreamReceipt;
  readonly intent: Domain.ExternalIntent | null;
}
export class Registry extends Context.Service<
  Registry,
  {
    readonly snapshot: (
      caller: AdmittedCaller,
      permission?: "workstreams:read" | "workstreams:write",
    ) => Effect.Effect<Snapshot, Session.RegistryError>;
    readonly submit: (
      caller: AdmittedCaller,
      body: string,
    ) => Effect.Effect<AcceptedCommand, Session.RegistryError>;
    readonly lookup: (
      caller: AdmittedCaller,
      id: string,
      generation: number,
    ) => Effect.Effect<WorkstreamsNativeReceiptLookup, Session.RegistryError>;
    readonly lookupLegacy: (
      caller: AdmittedCaller,
      request: WorkstreamsLegacyReceiptLookupRequest,
    ) => Effect.Effect<WorkstreamsLegacyReceiptLookupResult, Session.RegistryError>;
    readonly complete: (
      intent: Domain.ExternalIntent,
      result: Domain.RegistryExternalResult,
      recovery?: boolean,
    ) => Effect.Effect<Wire.WorkstreamReceipt, Session.RegistryError>;
  }
>()("t3/jones/workstreams/native/registry/service/Registry") {}

const isRegistryError = Schema.is(Session.RegistryError);
const isKeyUnavailable = Schema.is(Keys.KeyUnavailable);
const decodeCommand = Schema.decodeUnknownSync(Wire.WorkstreamCommand);
const failure = (cause: unknown): Session.RegistryError =>
  isRegistryError(cause)
    ? cause
    : new Session.RegistryError({
        code: isKeyUnavailable(cause)
          ? "key_unavailable"
          : cause instanceof WorkstreamDomainError
            ? cause.code
            : "unavailable",
        cause,
      });
const ids: DomainIdSource = { next: (kind) => `${kind}:${NodeCrypto.randomUUID()}` };
const attempt = <A>(f: () => A) => Effect.try({ try: f, catch: failure });
const now = DateTime.now.pipe(Effect.map((t) => DateTime.formatIso(t)));
const different = (a: unknown, b: unknown) => canonicalJson(a) !== canonicalJson(b);

const make = Effect.gen(function* () {
  const recoveryCrypto = yield* Crypto.Crypto;
  const home = yield* NativeHome;
  const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
  const db = yield* NativeSql.NativeSql;
  const registry = yield* RegistryStore.RegistryStore;
  const receipts = yield* ReceiptStore.ReceiptStore;
  const history = yield* HistoryStore.HistoryStore;
  const effects = yield* EffectStore.EffectStore;
  const session = yield* Session.Session;
  const index = yield* IdentityIndex.WorkstreamsIdentityIndex;
  const keys = yield* Keys.WorkstreamsKeyProvider;

  const load = Effect.fn("NativeRegistry.load")(function* (context: RegistryStore.RegistryContext) {
    yield* keys.get(yield* keys.activeKeyId(home));
    const state = Domain.createWorkstreamDomainState(home.owner_id, context.server_generation);
    state.registry_version = context.registry_version;
    const decode = <S extends Schema.Top>(schema: S, value: unknown) =>
      Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(failure));
    for (const row of yield* registry.list(scope, "records")) {
      const v = yield* decode(Wire.Workstream, row.value);
      state.workstreams.set(v.workstream_id, v);
    }
    for (const row of yield* registry.list(scope, "references")) {
      const v = yield* decode(Wire.NativeReference, row.value);
      state.references.set(v.native_reference_id, v);
    }
    for (const row of yield* registry.list(scope, "memberships")) {
      const v = yield* decode(Wire.MembershipEpisode, row.value);
      state.memberships.set(v.membership_id, v);
    }
    for (const row of yield* registry.list(scope, "declarations")) {
      const v = yield* decode(Wire.DeclarationRevision, row.value);
      state.declarations.set(
        v.declaration_id,
        [...(state.declarations.get(v.declaration_id) ?? []), v].toSorted(
          (a, b) => a.revision - b.revision,
        ),
      );
    }
    for (const row of yield* registry.listPlain(scope, "lifecycle_declarations")) {
      const v = yield* decode(
        Wire.WorkstreamLifecycleDeclaration,
        yield* attempt(() => JSON.parse(String(row.value))),
      );
      state.lifecycle_declarations.set(
        v.workstream_id,
        [...(state.lifecycle_declarations.get(v.workstream_id) ?? []), v].toSorted(
          (a, b) => a.revision - b.revision,
        ),
      );
    }
    for (const row of yield* registry.listPlain(scope, "edges")) {
      const v = yield* decode(
        Wire.WorkstreamEdge,
        yield* attempt(() => JSON.parse(String(row.value))),
      );
      state.edges.set(v.edge_id, v);
    }
    for (const row of yield* registry.list(scope, "pr_observations")) {
      const v = row.value as Domain.StoredPrObservation;
      yield* decode(Wire.WorkstreamPrObservation, v);
      state.observations.set(
        v.native_reference_id,
        [...(state.observations.get(v.native_reference_id) ?? []), v].toSorted(
          (a, b) => a.observation_version - b.observation_version,
        ),
      );
    }
    const bindings = (yield* registry.listPlain(scope, "source_bindings")).toSorted(
      (a, b) => Number(a.binding_version) - Number(b.binding_version),
    );
    for (const row of bindings)
      state.source_bindings.set(String(row.source_instance_id), {
        provider: String(row.provider),
        source_instance_id: String(row.source_instance_id),
        authority_namespace: String(row.authority_namespace),
        store_generation: Number(row.store_generation),
        state: row.state === "active" ? "active" : "revoked",
      });
    for (const reservation of yield* effects.active(scope)) {
      const intent = reservation.intent as Domain.ExternalIntent;
      if (
        intent.command?.command_id !== reservation.command_id ||
        intent.owner_id !== home.owner_id
      )
        return yield* new Session.RegistryError({ code: "unknown_effect" });
      state.external_intents.set(reservation.command_id, intent);
      const prior = yield* receipts.read(scope, reservation.command_id);
      if (!prior) return yield* new Session.RegistryError({ code: "unknown_effect" });
      state.receipts.set(reservation.command_id, prior.receipt);
      const result = yield* effects.result(scope, reservation.command_id);
      if (result)
        state.external_results.set(reservation.command_id, result as Domain.RegistryExternalResult);
    }
    return state;
  });
  const snapshot = Effect.fn("NativeRegistry.snapshot")(
    (
      caller: AdmittedCaller,
      permission: "workstreams:read" | "workstreams:write" = "workstreams:read",
    ) =>
      db
        .transaction(
          scope,
          Effect.gen(function* () {
            const admitted = yield* session.authorize(caller, permission);
            return { ...admitted, state: yield* load(admitted.context) };
          }),
        )
        .pipe(Effect.mapError(failure)),
  );
  const persist = Effect.fn("NativeRegistry.persist")(function* (
    before: Domain.WorkstreamDomainState,
    state: Domain.WorkstreamDomainState,
    nativeAttestation = false,
  ) {
    if (state.registry_version !== before.registry_version) {
      if (state.registry_version !== before.registry_version + 1)
        return yield* new Session.RegistryError({ code: "version_conflict" });
      yield* registry.advance(scope, {
        server_generation: before.server_generation,
        registry_version: before.registry_version,
      });
    }
    for (const [id, v] of state.workstreams)
      if (different(before.workstreams.get(id) ?? null, v))
        yield* registry.put(
          scope,
          "records",
          {
            metadata: { workstream_id: id, version: v.version, sort_order: v.sort_order },
            value: v,
          },
          before.workstreams.get(id)?.version ?? null,
        );
    const referenceRows = yield* registry.list(scope, "references");
    for (const [id, v] of state.references)
      if (different(before.references.get(id) ?? null, v)) {
        const r = v.registration,
          e = r.state === "attested" && v.identity.provider === "t3" ? r.evidence : null;
        yield* registry.put(
          scope,
          "references",
          {
            metadata: {
              native_reference_id: id,
              provider: v.identity.provider,
              resource_kind: v.identity.resource_kind,
              id_kind: v.identity.id_kind,
              identity_index: yield* index.digest(scope, v.identity),
              legacy_identity_sha256:
                referenceRows.find((row) => row.metadata.native_reference_id === id)?.metadata
                  .legacy_identity_sha256 ?? null,
              attestation_version: r.attestation_version,
              registration_state: r.state,
              created_by_principal: v.created_by.principal_id,
              routing_source_instance_id: e?.source_instance_id ?? null,
              routing_native_id: e?.native_id ?? null,
              routing_authority_namespace: e?.authority_namespace ?? null,
              routing_store_generation: e?.store_generation ?? null,
              routing_attested_at: e ? r.attested_at : null,
              routing_expires_at: e ? r.expires_at : null,
              routing_evidence_sha256: e?.evidence_sha256 ?? null,
            },
            value:
              nativeAttestation && e
                ? {
                    ...v,
                    native_attestation: {
                      contract: "workstreams-t3-provider/2.0.0",
                      evidence_sha256: e.evidence_sha256,
                      authority_namespace: e.authority_namespace,
                      store_generation: e.store_generation,
                      attested_at: r.attested_at,
                      expires_at: r.expires_at,
                    },
                  }
                : v,
          },
          before.references.get(id)?.registration.attestation_version ?? null,
        );
      }
    // Close existing episodes before opening replacements, preserving the partial primary index in one transaction.
    const memberships = [...state.memberships].toSorted(
      (a, b) => Number(b[1].closed !== null) - Number(a[1].closed !== null),
    );
    for (const [id, v] of memberships)
      if (different(before.memberships.get(id) ?? null, v))
        yield* registry.put(
          scope,
          "memberships",
          {
            metadata: {
              membership_id: id,
              workstream_id: v.workstream_id,
              native_reference_id: v.native_reference_id,
              kind: v.kind,
              opened_registry_version: v.opened.registry_version,
              closed_registry_version: v.closed?.registry_version ?? null,
              closed_at: v.closed?.at ?? null,
            },
            value: v,
          },
          before.memberships.get(id)?.opened.registry_version ?? null,
        );
    for (const [id, values] of state.declarations)
      for (const v of values)
        if (!before.declarations.get(id)?.some((x) => x.revision === v.revision))
          yield* registry.put(
            scope,
            "declarations",
            {
              metadata: {
                declaration_id: id,
                revision: v.revision,
                workstream_id: v.workstream_id,
                registry_version: v.registry_version,
                state: v.state,
              },
              value: v,
            },
            null,
          );
    for (const [id, values] of state.observations)
      for (const v of values)
        if (
          !before.observations.get(id)?.some((x) => x.observation_version === v.observation_version)
        )
          yield* registry.put(
            scope,
            "pr_observations",
            {
              metadata: {
                native_reference_id: id,
                observation_version: v.observation_version,
                attempted_at: v.attempted_at,
                outcome: v.outcome,
              },
              value: v,
            },
            null,
          );
    for (const [id, values] of state.lifecycle_declarations)
      for (const v of values)
        if (!before.lifecycle_declarations.get(id)?.some((x) => x.revision === v.revision))
          yield* registry.appendPlain(scope, "lifecycle_declarations", {
            lifecycle_declaration_id: v.lifecycle_declaration_id,
            workstream_id: v.workstream_id,
            revision: v.revision,
            registry_version: v.registry_version,
            recorded_at: v.recorded_at,
            value: canonicalJson(v),
          });
    for (const [id, v] of state.edges)
      if (different(before.edges.get(id) ?? null, v)) {
        if (before.edges.has(id)) {
          if (!v.closed) return yield* new Session.RegistryError({ code: "relationship_conflict" });
          yield* registry.closeEdge(scope, id, v.closed.registry_version, v.closed.at, v);
        } else
          yield* registry.appendPlain(scope, "edges", {
            edge_id: id,
            from_workstream_id: v.from_workstream_id,
            to_workstream_id: v.to_workstream_id,
            relation: v.relation,
            opened_registry_version: v.opened.registry_version,
            closed_registry_version: v.closed?.registry_version ?? null,
            closed_at: v.closed?.at ?? null,
            value: canonicalJson(v),
          });
      }
  });
  const auditEvents = (state: Domain.WorkstreamDomainState): HistoryStore.HistoryEvent[] =>
    state.audit.map((v) => ({
      event_id: v.event_id,
      command_id: v.command_id,
      registry_version: v.registry_version,
      occurred_at: v.occurred_at,
      workstream_ids: v.workstream_versions.map((x) => x.workstream_id),
      value: v,
    }));
  const lookup = Effect.fn("NativeRegistry.lookup")(
    (caller: AdmittedCaller, id: string, generation: number) =>
      db
        .transaction(
          scope,
          Effect.gen(function* () {
            yield* session.authorize(caller, "workstreams:read");
            const value = yield* receipts.lookup(scope, id, generation);
            if (
              value.kind === "present" &&
              value.receipt.actor.principal_id !== caller.principal_id
            )
              return yield* new Session.RegistryError({ code: "not_found" });
            return value;
          }),
        )
        .pipe(Effect.mapError(failure)),
  );
  const lookupLegacy = Effect.fn("NativeRegistry.lookupLegacy")(
    (caller: AdmittedCaller, request: WorkstreamsLegacyReceiptLookupRequest) =>
      db
        .transaction(
          scope,
          Effect.gen(function* () {
            yield* session.authorize(caller, "workstreams:read");
            return yield* receipts.lookupLegacy(scope, caller.principal_id, request);
          }),
        )
        .pipe(Effect.mapError(failure)),
  );
  const submit = Effect.fn("NativeRegistry.submit")((caller: AdmittedCaller, body: string) =>
    db
      .transaction(
        scope,
        Effect.gen(function* () {
          const admitted = yield* session.authorize(caller, "workstreams:write");
          const command = yield* attempt(() => {
            if (Buffer.byteLength(body) > 32768)
              throw new WorkstreamDomainError("payload_too_large");
            const value = JSON.parse(body);
            validateCommand(value);
            return decodeCommand(value);
          });
          if (command.expected_server_generation !== admitted.context.server_generation)
            return yield* new Session.RegistryError({ code: "generation_conflict" });
          const digest = NodeCrypto.createHash("sha256").update(body).digest("hex");
          const prior = yield* receipts.lookup(
            scope,
            command.command_id,
            command.expected_server_generation,
          );
          if (prior.kind === "unknown")
            return yield* new Session.RegistryError({
              code: prior.reason === "key_unavailable" ? "key_unavailable" : "unknown_effect",
            });
          if (prior.kind === "present") {
            if (
              prior.receipt.actor.principal_id !== caller.principal_id ||
              prior.receipt.request_sha256 !== digest
            )
              return yield* new Session.RegistryError({ code: "idempotency_conflict" });
            const saved = yield* receipts.read(scope, command.command_id);
            if (saved && saved.request_body !== body)
              return yield* new Session.RegistryError({ code: "idempotency_conflict" });
            return { receipt: prior.receipt, intent: null };
          }
          const before = yield* load(admitted.context);
          const acceptedAt = yield* now;
          const { state, result } = yield* attempt(() =>
            Decider.decideNativeCommand(
              before,
              {
                authorization: admitted.authorization,
                current_grant: admitted.authorization.grant,
                command,
                request_sha256: digest,
                accepted_at: acceptedAt,
              },
              ids,
              acceptedAt,
            ),
          );
          // Home forwarding installs a private, exact-child supplied verification scope.
          // Complete it through the pure decider in this enclosing home transaction;
          // it never becomes a provider callback, local caller or recoverable outbox effect.
          if (result.state === "external") {
            const supplied = yield* Effect.serviceOption(Supplied.AtomicSuppliedVerification);
            if (Option.isSome(supplied)) {
              const externalResult = yield* supplied.value.resolve(result.intent, body);
              const completed = yield* attempt(() =>
                Decider.completeNativeExternalIntent(
                  state,
                  result.intent,
                  ids,
                  externalResult,
                  acceptedAt,
                ),
              );
              yield* persist(before, completed.state, true);
              yield* receipts.save(
                scope,
                { receipt: completed.receipt, request_body: body, origin: "native" },
                completed.state.registry_version,
              );
              for (const event of auditEvents(completed.state)) yield* history.append(scope, event);
              return { receipt: completed.receipt, intent: null };
            }
          }
          yield* persist(before, state);
          if (result.state === "external") {
            const receipt = state.receipts.get(command.command_id)!;
            const recording = yield* Effect.serviceOption(Recorded.RecordedCallers);
            const record = Option.isSome(recording) ? yield* recording.value.captureCaller() : null;
            const recovery = record
              ? yield* Recorded.wrapEffectIntent(
                  canonicalJson(result.intent),
                  body,
                  digest,
                  record,
                ).pipe(Effect.provideService(Crypto.Crypto, recoveryCrypto))
              : null;
            if (record && Option.isSome(recording)) {
              const evidence = yield* recording.value.readmitRecorded(record);
              yield* evidence.check(
                "workstreams:write",
                result.intent.reference.identity.provider === "t3"
                  ? result.intent.reference.identity.native_id
                  : null,
              );
            }
            yield* effects.reserve(
              scope,
              { receipt, request_body: body, origin: "native" },
              {
                command_id: command.command_id,
                principal_id: caller.principal_id,
                native_reference_id: result.intent.reference.native_reference_id,
                effect: command.action.operation as EffectStore.Reservation["effect"],
                server_generation: state.server_generation,
                admitted_registry_version: state.registry_version,
                intent: result.intent,
                ...(recovery ? { recovery } : {}),
              },
            );
            // A failed last authority check rolls reservation and state back together.
            if (record && Option.isSome(recording)) {
              const evidence = yield* recording.value.readmitRecorded(record);
              yield* evidence.check(
                "workstreams:write",
                result.intent.reference.identity.provider === "t3"
                  ? result.intent.reference.identity.native_id
                  : null,
              );
            }
            return { receipt, intent: result.intent };
          }
          yield* receipts.save(
            scope,
            { receipt: result.receipt, request_body: body, origin: "native" },
            state.registry_version,
          );
          for (const event of auditEvents(state)) yield* history.append(scope, event);
          return { receipt: result.receipt, intent: null };
        }),
      )
      .pipe(Effect.mapError(failure)),
  );
  const complete = Effect.fn("NativeRegistry.complete")(
    (intent: Domain.ExternalIntent, result: Domain.RegistryExternalResult, recovery = false) =>
      db
        .transaction(
          scope,
          Effect.gen(function* () {
            const context = yield* registry.context(scope);
            if (
              !context ||
              context.state !== "active" ||
              context.server_generation !== home.server_generation
            )
              return yield* new Session.RegistryError({ code: "fenced" });
            const saved = yield* receipts.read(scope, intent.command.command_id);
            if (!saved) return yield* new Session.RegistryError({ code: "unknown_effect" });
            if (saved.receipt.state === "committed" || saved.receipt.state === "rejected")
              return saved.receipt;
            const before = yield* load(context);
            if (recovery && intent.kind === "native_t3_settlement") {
              if (!before.external_results.has(intent.command.command_id))
                return yield* new Session.RegistryError({ code: "unknown_effect" });
            } else yield* effects.saveResult(scope, intent.command.command_id, result);
            const completedAt = yield* now;
            const { state, receipt } = yield* attempt(() =>
              Decider.completeNativeExternalIntent(
                before,
                intent,
                ids,
                result,
                completedAt,
                recovery,
              ),
            );
            yield* persist(before, state, intent.kind === "verify_reference");
            if (recovery && intent.kind === "native_t3_settlement")
              yield* effects.completeRetainedNativeSettlement(
                scope,
                { ...saved, receipt },
                {
                  server_generation: before.server_generation,
                  registry_version: before.registry_version,
                },
                auditEvents(state),
              );
            else
              yield* effects.complete(
                scope,
                { ...saved, receipt },
                state.registry_version,
                auditEvents(state),
              );
            return receipt;
          }),
        )
        .pipe(Effect.mapError(failure)),
  );
  return Registry.of({ snapshot, submit, lookup, lookupLegacy, complete });
});
export const layer = Layer.effect(Registry, make);
