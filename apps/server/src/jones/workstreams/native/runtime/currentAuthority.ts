import type { WorkstreamsNativeBuild } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Connection from "effect/sql/SqlConnection";
import * as Config from "../../../../config.ts";
import * as Authority from "../../../../environment/NativeStoreAuthority.ts";
import { acquireExistingNativeAuthorityReadUse } from "../../../../environment/nativeStoreAuthorityPersistence.ts";
import * as Ownership from "../../../../serverOwnership.ts";
import { ServiceLauncherHostProcess } from "../../../../cloud/serviceLauncherClient.ts";
import * as Desktop from "../../DesktopNativeOwner.ts";
import * as Updates from "../../../updates/UpdateAdmissionFence.ts";
import { makeTrustedNativeEnrollmentSelectedContext } from "../../enrollment/trustedCliContext.ts";

export class AuthorityPreparationError extends Schema.TaggedError<AuthorityPreparationError>()(
  "AuthorityPreparationError",
  {
    code: Schema.Literals([
      "ambient_transaction",
      "owner_unqualified",
      "authority_changed",
      "update_pending",
      "wrong_client",
      "expired",
      "unavailable",
    ]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message() {
    return `Native current authority refused: ${this.code}.`;
  }
}
export interface PreparedRuntimeAuthority {
  readonly authority: Authority.NativeStoreAuthorityTuple;
  readonly build: WorkstreamsNativeBuild;
  readonly checkCurrent: Effect.Effect<void, AuthorityPreparationError>;
}
export class CurrentAuthority extends Context.Service<
  CurrentAuthority,
  {
    readonly withPrepared: <A, E, R>(
      use: (prepared: PreparedRuntimeAuthority) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | AuthorityPreparationError, R>;
  }
>()("t3/jones/workstreams/native/runtime/currentAuthority") {}

type PreparedFacts = {
  readonly invocation: object;
  readonly check: Effect.Effect<void, AuthorityPreparationError>;
  active: boolean;
};
const PreparedInvocation = Context.Reference<object | undefined>(
  "jones/workstreams/native/runtime/PreparedAuthorityInvocation",
  { defaultValue: () => undefined },
);
const prepared = new WeakMap<PreparedRuntimeAuthority, PreparedFacts>();
const isAuthorityPreparationError = Schema.is(AuthorityPreparationError);
const refused = (code: AuthorityPreparationError["code"], cause?: unknown) =>
  new AuthorityPreparationError({ code, ...(cause === undefined ? {} : { cause }) });
export const checkPreparedAuthority = Effect.fn("CurrentAuthority.checkPrepared")(function* (
  proof: PreparedRuntimeAuthority,
) {
  const facts = prepared.get(proof);
  const invocation = yield* PreparedInvocation;
  if (
    !facts ||
    !facts.active ||
    invocation !== facts.invocation ||
    proof.checkCurrent !== facts.check
  )
    return yield* refused("expired");
  yield* facts.check;
});

const make = Effect.gen(function* () {
  const config = yield* Config.ServerConfig;
  const sql = yield* SqlClient.SqlClient;
  const authority = yield* Authority.NativeStoreAuthority;
  const desktop = yield* Desktop.DesktopNativeOwner;
  const owner = yield* Ownership.ServerOwnershipLifetime;
  const launcher = yield* ServiceLauncherHostProcess;
  const updates = yield* Updates.UpdateAdmissionFence;

  const withPrepared: CurrentAuthority["Service"]["withPrepared"] = (use) =>
    Effect.scoped(
      Effect.gen(function* () {
        // Inspect the actual client's transaction tag before any owner/build/filesystem lookup.
        if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
          return yield* refused("ambient_transaction");
        const selected = yield* Effect.serviceOption(SqlClient.SqlClient);
        if (Option.isNone(selected) || selected.value !== sql)
          return yield* refused("wrong_client");
        const nativeUse = yield* updates.nativeUse.pipe(
          Effect.mapError((cause) => refused("update_pending", cause)),
        );
        let active = true;
        let invalid = false;
        const invalidate = () => {
          invalid = true;
        };
        // A disconnect permanently invalidates this incarnation, even if a host adapter changes later.
        if (config.mode !== "desktop") {
          if (owner === undefined || !launcher.connected)
            return yield* refused("owner_unqualified");
          yield* Effect.acquireRelease(
            Effect.sync(() => {
              launcher.on("disconnect", invalidate);
            }),
            () =>
              Effect.sync(() => {
                invalidate();
                launcher.off("disconnect", invalidate);
              }),
          );
        }
        const desktopUse =
          config.mode === "desktop"
            ? yield* desktop.prepareCurrent.pipe(
                Effect.mapError((cause) => refused("owner_unqualified", cause)),
              )
            : undefined;
        const checkOwner = () => {
          if (!active || invalid) throw refused("expired");
          if (desktopUse !== undefined) desktopUse.assertCurrent();
          else {
            if (owner === undefined || !launcher.connected) throw refused("owner_unqualified");
            owner.assertCurrent();
          }
        };
        yield* Effect.try({
          try: checkOwner,
          catch: (cause) => refused("owner_unqualified", cause),
        });
        const binding = desktopUse?.binding;
        if (
          binding !== undefined &&
          (binding.home !== config.baseDir || binding.databasePath !== config.dbPath)
        )
          return yield* refused("owner_unqualified");

        // All build, owner IPC and pathname qualification is outside the application reservation.
        const trusted = yield* makeTrustedNativeEnrollmentSelectedContext(
          { baseDir: config.baseDir, authorityDir: config.authorityStateDir },
          binding === undefined
            ? undefined
            : {
                binding,
                assertCurrent: desktop.assertCurrent,
                refresh: desktop.refresh,
              },
        ).pipe(Effect.mapError((cause) => refused("owner_unqualified", cause)));
        if (trusted.dbPath !== config.dbPath) return yield* refused("wrong_client");
        const qualified = yield* trusted.authority.readCurrent.pipe(
          Effect.mapError((cause) => refused("owner_unqualified", cause)),
        );
        const live = yield* authority.readCurrent.pipe(
          Effect.mapError((cause) => refused("owner_unqualified", cause)),
        );
        if (
          qualified.environmentId !== live.environmentId ||
          qualified.authorityNamespace !== live.authorityNamespace ||
          qualified.storeGeneration !== live.storeGeneration
        )
          return yield* refused("authority_changed");
        // Only bounded SQLite reads occur while this exact connection is reserved.
        const connection: Connection.Connection = yield* Effect.gen(function* () {
          const connection = yield* sql.reserve;
          const databases = yield* connection.execute("PRAGMA database_list", [], undefined);
          const modes = yield* connection.execute("PRAGMA main.journal_mode", [], undefined);
          const versions = yield* connection.execute("PRAGMA main.schema_version", [], undefined);
          if (
            databases.filter(
              (row) => row.name === "main" && row.seq === 0 && row.file === trusted.dbPath,
            ).length !== 1 ||
            modes.length !== 1 ||
            !["delete", "truncate", "persist", "wal"].includes(String(modes[0]?.journal_mode)) ||
            versions.length !== 1 ||
            typeof versions[0]?.schema_version !== "number"
          )
            return yield* refused("wrong_client");
          return connection;
        }).pipe(
          Effect.scoped,
          Effect.mapError((cause) =>
            isAuthorityPreparationError(cause) ? cause : refused("unavailable", cause),
          ),
        );
        // The existing mutation lock is retained until all callback work and rollback finish.
        const readUse = yield* Effect.acquireRelease(
          Effect.try({
            try: () => acquireExistingNativeAuthorityReadUse(config.authorityStateDir),
            catch: (cause) => refused("authority_changed", cause),
          }),
          (value) => Effect.sync(value.release),
        );
        const state = readUse.state;
        if (
          state.environment_id !== qualified.environmentId ||
          state.authority_namespace !== qualified.authorityNamespace ||
          state.store_generation !== qualified.storeGeneration
        )
          return yield* refused("authority_changed");
        const invocation = Object.freeze({});
        let proof!: PreparedRuntimeAuthority;
        const check = Effect.gen(function* () {
          const facts = prepared.get(proof);
          if (!active || invalid || !facts?.active || (yield* PreparedInvocation) !== invocation) {
            invalid = true;
            return yield* refused("expired");
          }
          const selectedClient = yield* Effect.serviceOption(SqlClient.SqlClient);
          if (Option.isNone(selectedClient) || selectedClient.value !== sql) {
            invalid = true;
            return yield* refused("wrong_client");
          }
          const transaction = yield* Effect.serviceOption(sql.transactionService);
          if (Option.isSome(transaction) && transaction.value[0] !== connection) {
            invalid = true;
            return yield* refused("wrong_client");
          }
          yield* nativeUse.checkCurrent.pipe(
            Effect.mapError((cause) => {
              invalid = true;
              return refused("update_pending", cause);
            }),
          );
          yield* Effect.try({
            try: () => {
              checkOwner();
              readUse.assertHeld();
            },
            catch: (cause) => {
              invalid = true;
              return refused("authority_changed", cause);
            },
          });
        });
        proof = Object.freeze({
          authority: Object.freeze({ ...qualified }),
          build: Object.freeze({ ...trusted.build }),
          checkCurrent: check,
        });
        const facts: PreparedFacts = { invocation, check, active: true };
        prepared.set(proof, facts);
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            active = false;
            invalid = true;
            facts.active = false;
            prepared.delete(proof);
          }),
        );
        const body = Effect.gen(function* () {
          yield* checkPreparedAuthority(proof);
          const result = yield* use(proof);
          yield* checkPreparedAuthority(proof);
          return result;
        }).pipe(Effect.awaitAllChildren, Effect.provideService(PreparedInvocation, invocation));
        // raceFirst waits for the interrupted branch's finalizers (including SQL rollback).
        return yield* Effect.raceFirst(
          body,
          nativeUse.invalidated.pipe(Effect.andThen(Effect.fail(refused("update_pending")))),
        );
      }),
    );
  return CurrentAuthority.of({ withPrepared });
});

/** Captures selected app resources once; construction performs no external qualification. */
export const layer = Layer.effect(CurrentAuthority, make);
