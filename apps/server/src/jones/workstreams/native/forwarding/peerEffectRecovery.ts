import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Wire from "@t3tools/contracts";
import { NativeHome } from "../mode/authorityMode.ts";
import type { RegistryExternalResult } from "../domain/state.ts";
import { observationCache } from "../domain/externalIntents.ts";
import * as Origin from "../runtime/peerEffectOrigin.ts";
import * as Receiving from "../runtime/retainedPeerSession.ts";
import * as Source from "./peerEffectSource.ts";
import * as Initial from "./peerEffectAdmission.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Envelope from "../crypto/envelope.ts";
import * as Index from "../crypto/identityIndex.ts";
import * as Db from "../store/sql.ts";
import * as Store from "../store/registryStore.ts";
import * as Effects from "../store/effectStore.ts";
import * as Receipts from "../store/receiptStore.ts";
import * as History from "../store/historyStore.ts";
import * as Archive from "../store/archiveStore.ts";
import * as Completion from "../store/peerEffectCompletion.ts";
import * as Registry from "../registry/service.ts";
import * as Session from "../registry/session.ts";

export class PeerEffectRecovery extends Context.Service<
  PeerEffectRecovery,
  {
    readonly runInitial: (
      ticket: Initial.InitialPeerTicket,
    ) => Effect.Effect<Wire.WorkstreamReceipt | null, Session.RegistryError>;
    /** Uses this owner's prepared-key graph; no key retrieval occurs inside the selection writer. */
    readonly active: () => Effect.Effect<
      Effect.Success<ReturnType<Effects.EffectStore["Service"]["active"]>>,
      Session.RegistryError
    >;
    /** Lookup-only recovery. Unknown outcomes never authorize dispatch or replacement IDs. */
    readonly recoverOne: (
      commandId: string,
    ) => Effect.Effect<Wire.WorkstreamReceipt | null, Session.RegistryError>;
  }
>()("t3/jones/workstreams/native/forwarding/peerEffectRecovery") {}

const isRegistryError = Schema.is(Session.RegistryError);
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Digest.Digest;
  const home = yield* NativeHome;
  const scope = { registry_instance_id: home.registry_instance_id, owner_id: home.owner_id };
  const keys = yield* Keys.WorkstreamsKeyProvider;
  const origins = yield* Origin.PeerEffectOrigins;
  const receiving = yield* Receiving.OriginalReceivingSession;
  const source = yield* Source.PeerEffectSource;
  const withStores = Effect.fn("PeerEffectRecovery.withStores")(function* <A, E, R>(
    use: (
      effects: Effects.EffectStore["Service"],
      db: Db.NativeSql["Service"],
      completion: Completion.PeerEffectCompletion["Service"],
    ) => Effect.Effect<A, E, R>,
  ) {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new Session.RegistryError({ code: "preparation_in_transaction" });
    // Select metadata, release the selector, then wait for external keys before any writer.
    const rows = yield* sql<{ key_id: string }>`SELECT key_id FROM jones_workstreams_keys
      WHERE registry_instance_id=${home.registry_instance_id}`;
    const activeHome = yield* keys.activeKeyId(home);
    const content = new Map<string, Uint8Array>();
    for (const id of new Set([...rows.map((r) => r.key_id), activeHome]))
      content.set(id, Uint8Array.from(yield* keys.get(id)));
    const index = Uint8Array.from(yield* keys.indexKey(home.registry_instance_id));
    const cached = Keys.WorkstreamsKeyProvider.of({
      activeKeyId: Effect.fn("PeerEffectRecovery.activeKeyId")(function* (scope) {
        if (
          scope.registry_instance_id !== home.registry_instance_id ||
          scope.owner_id !== home.owner_id ||
          (scope.key_scope !== undefined && scope.key_scope !== "home")
        )
          return yield* new Keys.KeyUnavailable({ reason: "active_key_missing" });
        const current = yield* sql<{ key_id: string }>`SELECT k.key_id
          FROM jones_workstreams_keys k JOIN jones_workstreams_registries r
            ON r.registry_instance_id=k.registry_instance_id
          WHERE k.registry_instance_id=${home.registry_instance_id} AND r.owner_id=${home.owner_id}
            AND k.scope='home' AND k.state='active' AND r.active_key_id=k.key_id`.pipe(
          Effect.mapError(() => new Keys.KeyUnavailable({ reason: "store_unavailable" })),
        );
        if (current.length !== 1 || current[0]?.key_id !== activeHome)
          return yield* new Keys.KeyUnavailable({ reason: "active_key_missing" });
        return activeHome;
      }),
      get: Effect.fn("Keys.WorkstreamsKeyProvider.get")((id) =>
        Effect.suspend(() =>
          content.has(id)
            ? Effect.succeed(Uint8Array.from(content.get(id)!))
            : Effect.fail(new Keys.KeyUnavailable({ reason: "missing" })),
        ),
      ),
      indexKey: Effect.fn("Keys.WorkstreamsKeyProvider.indexKey")(function* (id) {
        if (id !== home.registry_instance_id)
          return yield* new Keys.KeyUnavailable({ reason: "missing" });
        return Uint8Array.from(index);
      }),
    });
    const db = Db.layer.pipe(
      Layer.provideMerge(
        Envelope.layer.pipe(Layer.provideMerge(Layer.succeed(Keys.WorkstreamsKeyProvider, cached))),
      ),
    );
    const receipts = Receipts.layer.pipe(Layer.provideMerge(Archive.layer));
    const stores = Layer.mergeAll(
      Store.layer,
      Index.layer,
      Effects.layer.pipe(Layer.provideMerge(Layer.mergeAll(receipts, History.layer))),
    ).pipe(Layer.provideMerge(db));
    // Background completion has no invocation and cannot acquire submit/snapshot authority.
    // The completion method separately verifies private original-receiver and source proofs.
    const denyInvocation = Layer.succeed(
      Session.Session,
      Session.Session.of({
        authorize: Effect.fn("Session.Session.authorize")(() =>
          Effect.fail(new Session.RegistryError({ code: "home_route_required" })),
        ),
      }),
    );
    const graph = Completion.layer.pipe(
      Layer.provideMerge(Registry.layer.pipe(Layer.provide(denyInvocation))),
      Layer.provideMerge(stores),
      Layer.provideMerge(Layer.succeed(NativeHome, home)),
      Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
      Layer.provideMerge(Layer.succeed(Digest.Digest, crypto)),
      Layer.fresh,
    );
    return yield* Effect.gen(function* () {
      return yield* use(
        yield* Effects.EffectStore,
        yield* Db.NativeSql,
        yield* Completion.PeerEffectCompletion,
      );
    }).pipe(Effect.provide(graph));
  });
  const retain = Effect.fn("PeerEffectRecovery.retain")(function* (
    origin: Origin.ValidatedPeerReservation,
    evidence: Source.AuthenticatedPeerResult,
  ) {
    yield* Source.checkAuthenticatedPeerResult(evidence, origin);
    let result: RegistryExternalResult;
    if (evidence.kind === "github") {
      if (origin.intent.kind !== "refresh_linked_pr")
        return yield* new Session.RegistryError({ code: "invalid_response" });
      const value = evidence.observation;
      const observation = {
        native_reference_id: origin.intent.reference.native_reference_id,
        observation_version: (origin.intent.previous_observation?.observation_version ?? 0) + 1,
        attempted_at: value.observed_at,
        outcome: value.outcome,
        retry_after_seconds: value.retry_after_seconds,
        last_success: value.last_success,
        command_id: origin.intent.command.command_id,
      };
      result = {
        observation,
        observation_cache:
          value.etag && value.last_success
            ? observationCache(origin.intent, observation, value.etag)
            : null,
      };
    } else {
      const response = evidence.response;
      if (response.state !== "result")
        return yield* new Session.RegistryError({ code: "invalid_response" });
      if (response.request.kind === "attest" && response.result.state === "attested") {
        const value = response.result;
        result = {
          registration: {
            state: "attested",
            attestation_version: origin.intent.reference.registration.attestation_version + 1,
            attested_at: value.attested_at,
            expires_at: value.expires_at,
            evidence: value.attestation,
          },
        };
      } else if (
        response.result.state === "terminal" &&
        origin.intent.kind === "native_t3_settlement"
      )
        result = response.result.result;
      else return yield* new Session.RegistryError({ code: "invalid_response" });
    }
    // Successful authenticated transport evidence survives receiver/source revocation.
    // Only owner/origin currentness is required for this encrypted retention, never client release.
    yield* withStores((effects, db) =>
      db.transaction(
        home,
        Effect.gen(function* () {
          yield* Source.checkAuthenticatedPeerResult(evidence, origin);
          yield* effects.saveResult(scope, origin.command_id, result);
          yield* Source.checkAuthenticatedPeerResult(evidence, origin);
        }),
      ),
    );
  });
  const execute = Effect.fn("PeerEffectRecovery.execute")(
    (commandId: string, ticket?: Initial.InitialPeerTicket) =>
      origins.withOrigin(commandId, (origin) =>
        Effect.gen(function* () {
          if (ticket) yield* Initial.beginInitialPeerTicket(ticket, origin);
          if (!origin.parent_result) {
            const available = yield* receiving.withCurrent(origin, (receiver) =>
              source.withReadmission(origin, receiver, (prepared) =>
                Effect.gen(function* () {
                  const previous = yield* withStores((effects, db) =>
                    db.transaction(
                      home,
                      Effect.gen(function* () {
                        yield* Origin.checkOriginSnapshot(origin);
                        yield* Source.checkPeerSourceOrigin(prepared, origin, receiver);
                        return yield* effects.result(scope, commandId);
                      }),
                    ),
                  );
                  if (previous !== null) return true;
                  const observed = yield* ticket
                    ? Source.dispatchInitialPeerSourceEffect(prepared, ticket)
                    : Source.lookupPeerSourceEffect(prepared);
                  if (observed.kind === "observation") return false;
                  yield* retain(origin, observed.evidence);
                  return true;
                }),
              ),
            );
            if (!available) return null;
          }
          // A distinct source observation and freshly qualified original receiver are mandatory
          // after provider work and before the command and parent completion writer.
          return yield* receiving.withCurrent(origin, (receiver) =>
            source.withReadmission(origin, receiver, (prepared) =>
              withStores((_effects, db, completion) =>
                ticket
                  ? db.transaction(
                      home,
                      Effect.gen(function* () {
                        yield* Initial.checkInitialPeerCurrent(ticket, origin);
                        const result = yield* completion.complete(origin, receiver, prepared);
                        yield* Initial.checkInitialPeerCurrent(ticket, origin);
                        return result.result;
                      }),
                    )
                  : completion
                      .complete(origin, receiver, prepared)
                      .pipe(Effect.map((r) => r.result)),
              ),
            ),
          );
        }),
      ),
    (effect) =>
      effect.pipe(
        Effect.mapError((cause) =>
          isRegistryError(cause)
            ? cause
            : new Session.RegistryError({ code: "unavailable", cause }),
        ),
      ),
  );
  return PeerEffectRecovery.of({
    recoverOne: Effect.fn("PeerEffectRecovery.recoverOne")((id) => execute(id)),
    runInitial: Effect.fn("PeerEffectRecovery.runInitial")((ticket) =>
      execute(ticket.command_id, ticket),
    ),
    active: Effect.fn("PeerEffectRecovery.active")(() =>
      withStores((effects) => effects.active(scope)).pipe(
        Effect.mapError((cause) =>
          isRegistryError(cause)
            ? cause
            : new Session.RegistryError({ code: "unavailable", cause }),
        ),
      ),
    ),
  });
});
export const layer = Layer.effect(PeerEffectRecovery, make);
