import { RecordedCallerPreparation } from "./recordedCallerPreparationPort.ts";
export { RecordedCallerPreparation } from "./recordedCallerPreparationPort.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import * as AuthSessions from "../../../../persistence/AuthSessions.ts";
import * as NativeAuthority from "../../../../environment/NativeStoreAuthority.ts";
import * as Secrets from "../../../../auth/ServerSecretStore.ts";
import * as ProviderSessions from "../../../../orchestration-v2/ProviderSessionManager.ts";
import * as McpSessions from "../../../../mcp/McpSessionRegistry.ts";
import { NativeStoreAuthorityPersistenceError } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import { NativeHome } from "../mode/authorityMode.ts";
import * as Keys from "../crypto/keyProvider.ts";
import * as Database from "../store/sql.ts";
import * as Records from "../store/sourceEnrollmentStore.ts";
import * as Bindings from "../store/bindingStore.ts";
import * as Registries from "../store/registryStore.ts";
import * as Session from "../registry/session.ts";
import * as Source from "../providers/localAttestation.ts";
import * as Admission from "./admission.ts";
import * as Startup from "./startup.ts";
import * as Enrollments from "./sourceEnrollment.ts";
import * as Recorded from "./recordedCaller.ts";
import * as Authority from "./currentAuthority.ts";

const PreparedInvocation = Context.Reference<object | undefined>(
  "t3/jones/workstreams/native/runtime/PreparedCallerInvocation",
  { defaultValue: () => undefined },
);
const unavailable = () => new Recorded.RecordedCallerFence({ code: "unavailable" });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const authority = yield* Effect.serviceOption(Authority.CurrentAuthority);
  const selectedAuthority = yield* NativeAuthority.NativeStoreAuthority;
  const records = yield* Records.SourceEnrollmentStore;
  const sessions = yield* AuthSessions.AuthSessionRepository;
  const registries = yield* Registries.RegistryStore;
  const bindings = yield* Bindings.BindingStore;
  const db = yield* Database.NativeSql;
  const secrets = yield* Secrets.ServerSecretStore;
  const keys = yield* Keys.WorkstreamsKeyProvider;
  const home = yield* NativeHome;
  const startup = yield* Startup.Startup;
  const registrySession = yield* Session.Session;
  const threads = yield* Effect.serviceOption(Admission.RecordedThreadLookup);
  const providers = yield* Effect.serviceOption(ProviderSessions.ProviderSessionManagerV2);
  const credentials = yield* Effect.serviceOption(McpSessions.McpSessionRegistry);
  const application = Context.make(SqlClient.SqlClient, sql).pipe(
    Context.add(AuthSessions.AuthSessionRepository, sessions),
    Context.add(Registries.RegistryStore, registries),
    Context.add(Bindings.BindingStore, bindings),
    Context.add(Database.NativeSql, db),
    Context.add(NativeAuthority.NativeStoreAuthority, selectedAuthority),
    Context.add(NativeHome, home),
    Context.add(Startup.Startup, startup),
    Context.add(Session.Session, registrySession),
  );
  // Optional genuine application ports stay absent when unavailable; never synthesize them.
  const selected = Context.mergeAll(
    application,
    Option.isSome(threads)
      ? Context.make(Admission.RecordedThreadLookup, threads.value)
      : Context.empty(),
    Option.isSome(providers)
      ? Context.make(ProviderSessions.ProviderSessionManagerV2, providers.value)
      : Context.empty(),
    Option.isSome(credentials)
      ? Context.make(McpSessions.McpSessionRegistry, credentials.value)
      : Context.empty(),
  );
  const withPrepared: RecordedCallerPreparation["Service"]["withPrepared"] = Effect.fn(
    "RecordedCallerPreparation.withPrepared",
  )(function* <A, E, R>(
    use: (callers: Recorded.RecordedCallers["Service"]) => Effect.Effect<A, E, R>,
  ) {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* unavailable();
    const currentSql = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (
      Option.isNone(currentSql) ||
      currentSql.value !== sql ||
      Option.isNone(authority) ||
      !startup.sourceSessionId
    )
      return yield* unavailable();
    const sessionId = startup.sourceSessionId;
    const currentAuthority = authority.value;
    return yield* Effect.scoped(
      Effect.gen(function* () {
        // Preserve the original request object/ceiling and original run IDs before ANY writer.
        const request = yield* Effect.context<never>();
        const requestApplication = Context.merge(request, selected);
        const seed = yield* Admission.prepareRecordedAuthenticationSeed().pipe(
          Effect.provide(requestApplication),
        );
        const outcome = yield* currentAuthority
          .withPrepared((prepared) =>
            records.withPreparedRead({ session_id: sessionId }, (read) =>
              Effect.gen(function* () {
                let live = true;
                const invocation = Object.freeze({});
                const check = Effect.fn("RecordedCallerPreparation.check")(function* () {
                  const current = yield* Effect.serviceOption(SqlClient.SqlClient);
                  if (
                    !live ||
                    (yield* PreparedInvocation) !== invocation ||
                    Option.isNone(current) ||
                    current.value !== sql
                  )
                    return yield* new Recorded.RecordedCallerFence({ code: "identity_changed" });
                  yield* Authority.checkPreparedAuthority(prepared).pipe(
                    Effect.mapError(
                      () => new Recorded.RecordedCallerFence({ code: "not_granted" }),
                    ),
                  );
                });
                // Only this freshly constructed lookup sees the prepared reader. No setup owner is provided.
                const lookupContext = Context.make(
                  Enrollments.SourceIssuer,
                  Enrollments.SourceIssuer.of({
                    authority: {
                      readCurrent: Authority.checkPreparedAuthority(prepared).pipe(
                        Effect.mapError(
                          (cause) =>
                            new NativeStoreAuthorityPersistenceError(
                              "source_unavailable",
                              "Prepared source authority unavailable.",
                              cause,
                            ),
                        ),
                        Effect.as(prepared.authority),
                      ),
                    },
                    build: Effect.succeed(Option.some(prepared.build)),
                  }),
                ).pipe(
                  Context.add(SqlClient.SqlClient, sql),
                  Context.add(AuthSessions.AuthSessionRepository, sessions),
                  Context.add(
                    Records.SourceEnrollmentStore,
                    Records.SourceEnrollmentStore.of({ ...records, read }),
                  ),
                  Context.add(Registries.RegistryStore, registries),
                  Context.add(Bindings.BindingStore, bindings),
                  Context.add(Secrets.ServerSecretStore, secrets),
                  Context.add(Keys.WorkstreamsKeyProvider, keys),
                );
                // Providing an exact constructor context excludes an ambient SourceSetupOwner as well.
                const source = yield* Enrollments.NativeSourceEnrollments.pipe(
                  Effect.provide(Layer.fresh(Enrollments.layer)),
                  Effect.provide(lookupContext),
                );
                const lookup = { getValidatedBySessionId: source.getValidatedBySessionId };
                const environment = Context.merge(yield* Effect.context<never>(), selected);
                const freshAuthority = yield* Admission.makePreparedRecordedAuthority(
                  seed,
                  prepared,
                ).pipe(
                  Effect.provide(Startup.recordedSourceEnrollmentLayer(lookup)),
                  Effect.provide(environment),
                );
                const callers = yield* Recorded.RecordedCallers.pipe(
                  Effect.provide(Layer.fresh(Recorded.layer)),
                  Effect.provideService(Recorded.CallerAuthority, freshAuthority),
                );
                const guarded = Effect.fn("RecordedCallerPreparation.guarded")(function* <B, F, S>(
                  effect: Effect.Effect<B, F, S>,
                ) {
                  yield* check();
                  const result = yield* effect;
                  yield* check();
                  return result;
                });
                const scopedCallers = Recorded.RecordedCallers.of({
                  captureCaller: () => guarded(Effect.suspend(() => callers.captureCaller())),
                  readmitRecorded: (input) =>
                    guarded(
                      Effect.gen(function* () {
                        const caller = yield* callers.readmitRecorded(input);
                        return {
                          origin: caller.origin,
                          check: (permission, threadId) =>
                            guarded(caller.check(permission, threadId)).pipe(
                              Effect.mapError(
                                () => new Source.ProviderError({ code: "not_granted" }),
                              ),
                            ),
                        } satisfies Admission.CallerEvidence;
                      }),
                    ),
                  visible: (record, threadId) =>
                    guarded(callers.visible(record, threadId)).pipe(
                      Effect.catch(() => Effect.succeed(false)),
                    ),
                });
                return yield* guarded(Effect.suspend(() => use(scopedCallers))).pipe(
                  Effect.awaitAllChildren,
                  Effect.provideService(PreparedInvocation, invocation),
                  Effect.ensuring(
                    Effect.sync(() => {
                      live = false;
                    }),
                  ),
                );
              }).pipe(Effect.result),
            ),
          )
          .pipe(Effect.mapError(unavailable));
        if (Result.isFailure(outcome)) return yield* Effect.fail(outcome.failure);
        return outcome.success;
      }),
    );
  });
  return RecordedCallerPreparation.of({ withPrepared });
});

/** Invocation-scoped composition over the selected application resources; no mount or authority fallback. */
export const layer = Layer.effect(RecordedCallerPreparation, make);
