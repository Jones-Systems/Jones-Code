import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sessions from "../../../../persistence/AuthSessions.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as Links from "../../../../peer/PeerLinks.ts";
import { NativeStoreAuthorityPersistenceError } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Pins from "../store/peerSessionPinStore.ts";
import * as Records from "../store/memberEnrollmentStore.ts";
import { RegistryError } from "../registry/session.ts";
import * as Authority from "./currentAuthority.ts";
import * as Source from "./sourceEnrollment.ts";
import * as Members from "./memberEnrollment.ts";

export class MemberPreparation extends Context.Service<
  MemberPreparation,
  {
    readonly withPrepared: <A, E, R>(
      use: (members: Members.MemberEnrollments["Service"]) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | RegistryError, R>;
  }
>()("t3/jones/workstreams/native/runtime/memberPreparation") {}

const Invocation = Context.Reference<object | undefined>(
  "t3/jones/workstreams/native/runtime/PreparedMemberInvocation",
  { defaultValue: () => undefined },
);
const refused = () => new Members.MemberEnrollmentError({ code: "owner_unqualified" });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const authority = yield* Effect.serviceOption(Authority.CurrentAuthority);
  const application = Context.make(SqlClient.SqlClient, sql).pipe(
    Context.add(Sessions.AuthSessionRepository, yield* Sessions.AuthSessionRepository),
    Context.add(Secrets.ServerSecretStore, yield* Secrets.ServerSecretStore),
    Context.add(Links.PeerLinks, yield* Links.PeerLinks),
    Context.add(Bindings.BindingStore, yield* Bindings.BindingStore),
    Context.add(Pins.PeerSessionPinStore, yield* Pins.PeerSessionPinStore),
    Context.add(Records.MemberEnrollmentStore, yield* Records.MemberEnrollmentStore),
  );
  return MemberPreparation.of({
    withPrepared: Effect.fn("MemberPreparation.withPrepared")(function* <A, E, R>(
      use: (members: Members.MemberEnrollments["Service"]) => Effect.Effect<A, E, R>,
    ) {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* new RegistryError({ code: "preparation_in_transaction" });
      const current = yield* Effect.serviceOption(SqlClient.SqlClient);
      if (Option.isNone(current) || current.value !== sql || Option.isNone(authority))
        return yield* new RegistryError({ code: "authority_fenced" });
      const outcome = yield* authority.value
        .withPrepared((prepared) =>
          Effect.gen(function* () {
            let live = true;
            const invocation = Object.freeze({});
            const check = Effect.gen(function* () {
              const selected = yield* Effect.serviceOption(SqlClient.SqlClient);
              if (
                !live ||
                (yield* Invocation) !== invocation ||
                Option.isNone(selected) ||
                selected.value !== sql
              )
                return yield* refused();
              yield* Authority.checkPreparedAuthority(prepared).pipe(Effect.mapError(refused));
            });
            const context = application.pipe(
              Context.add(Source.SourceIssuer, {
                authority: {
                  readCurrent: check.pipe(
                    Effect.mapError(
                      (cause) =>
                        new NativeStoreAuthorityPersistenceError(
                          "source_unavailable",
                          "Prepared member authority unavailable.",
                          cause,
                        ),
                    ),
                    Effect.as(prepared.authority),
                  ),
                },
                build: Effect.succeed(Option.some(prepared.build)),
              }),
              Context.add(Source.SourceSetupOwner, {
                verifyUnchanged: check.pipe(
                  Effect.mapError(
                    () => new Source.SourceEnrollmentError({ code: "owner_unqualified" }),
                  ),
                ),
              }),
            );
            const members = yield* Members.MemberEnrollments.pipe(
              Effect.provide(Layer.fresh(Members.layer)),
              Effect.provide(context),
            );
            const guarded = Effect.fn("MemberPreparation.guarded")(function* <B, F, S>(
              effect: Effect.Effect<B, F, S>,
            ) {
              yield* check;
              const value = yield* effect;
              yield* check;
              return value;
            });
            const qualified = Members.MemberEnrollments.of({
              ownerCurrent: guarded(members.ownerCurrent),
              validateSource: (record, exact) => guarded(members.validateSource(record, exact)),
              inspectSession: (record) => guarded(members.inspectSession(record)),
              validateLocal: (record) => guarded(members.validateLocal(record)),
              readProof: (selector) => guarded(members.readProof(selector)),
              getValidatedBySessionId: (id) => guarded(members.getValidatedBySessionId(id)),
              getValidatedByEnrollmentId: (id) => guarded(members.getValidatedByEnrollmentId(id)),
              getBySessionId: (id) => guarded(members.getBySessionId(id)),
              // Runtime qualification never grants setup mutation or credential materialization.
              reserveSession: () => Effect.fail(refused()),
              materialize: () => Effect.fail(refused()),
            });
            return yield* Effect.gen(function* () {
              const current = check.pipe(
                Effect.mapError((cause) => new RegistryError({ code: "authority_fenced", cause })),
              );
              yield* current;
              const result = yield* use(qualified);
              yield* current;
              return result;
            }).pipe(
              Effect.awaitAllChildren,
              Effect.provideService(Invocation, invocation),
              Effect.result,
              Effect.ensuring(
                Effect.sync(() => {
                  live = false;
                }),
              ),
            );
          }),
        )
        .pipe(Effect.mapError((cause) => new RegistryError({ code: "authority_fenced", cause })));
      if (Result.isFailure(outcome)) return yield* Effect.fail(outcome.failure);
      return outcome.success;
    }),
  });
});

/** Uses the original application resources and private current-authority lifetime for one invocation. */
export const layer = Layer.effect(MemberPreparation, make);
