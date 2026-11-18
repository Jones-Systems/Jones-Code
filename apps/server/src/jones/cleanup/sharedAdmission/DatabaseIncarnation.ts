// @effect-diagnostics nodeBuiltinImport:off -- This cooperative incarnation handshake observes owned files before the actual SQLite worker opens them.
import * as NodeFS from "node:fs";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as Owner from "../CheckedRuntimeOwner.ts";
import { validateInfrastructurePermit, type InfrastructurePermit } from "./EnrollmentJournal.ts";
import {
  AdmissionError,
  canonical,
  storageError,
  DirectoryIdentity,
  PhysicalScope,
} from "./Protocol.ts";

// Defer the cross-module schema through the CheckedRuntimeOwner/Sqlite import cycle.
const isCheckedRuntimeOwnerError = Schema.is(Schema.suspend(() => Owner.CheckedRuntimeOwnerError));
const decodePhysicalScope = Schema.decodeUnknownSync(Schema.fromJsonString(PhysicalScope));

const openBrand: unique symbol = Symbol("database-open-incarnation");
const checkedBrand: unique symbol = Symbol("checked-database-owner");
export type DatabaseOpenIncarnation = { readonly [openBrand]: true };
export type CheckedDatabaseOwnerToken = { readonly [checkedBrand]: true };
export interface DatabasePhysicalBinding {
  readonly version: 1;
  readonly canonicalPath: string;
  readonly database: typeof DirectoryIdentity.Type;
  readonly stateDirectory: PhysicalScope;
}
interface OpenState {
  readonly owner: Owner.HeldRuntimeOwnerToken;
  readonly scope: Scope.Scope;
  readonly binding: DatabasePhysicalBinding;
  readonly infrastructure: Effect.Effect<void, AdmissionError>;
  active: boolean;
  attempted: boolean;
  checked?: CheckedDatabaseOwnerToken;
  sql?: SqlClient.SqlClient;
}
const opens = new WeakMap<object, OpenState>();
const checked = new WeakMap<object, OpenState>();
const clients = new WeakMap<SqlClient.SqlClient, OpenState>();
const refuse = (reason: string): never => {
  throw new Owner.CheckedRuntimeOwnerError({ reason });
};
const objectKey = (value: unknown): object | undefined =>
  typeof value === "object" && value !== null ? value : undefined;

const freeze = <A>(value: A): A => {
  if (typeof value === "object" && value !== null) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};
const observe = (path: string, stateDirectory: PhysicalScope): DatabasePhysicalBinding => {
  try {
    const entry = NodeFS.lstatSync(path, { bigint: true });
    if (!entry.isFile() || entry.nlink !== 1n || NodeFS.realpathSync(path) !== path)
      return refuse("unsupported_database_alias");
    return Object.freeze({
      version: 1,
      canonicalPath: path,
      stateDirectory,
      database: Object.freeze({
        version: 1,
        device: String(entry.dev),
        inode: String(entry.ino),
        birth: String(entry.birthtimeNs),
      }),
    });
  } catch (cause) {
    if (isCheckedRuntimeOwnerError(cause)) throw cause;
    throw new Owner.CheckedRuntimeOwnerError({ reason: "database_identity_unavailable", cause });
  }
};
const current = (state: OpenState): void => {
  try {
    const owner = Owner.inspectHeldDatabaseOwner(state.owner);
    if (!state.active) return refuse("database_binding_invalidated");
    if (state.scope.state._tag === "Closed") return refuse("database_open_scope_closed");
    if (
      owner.dbPath !== state.binding.canonicalPath ||
      canonical(observe(owner.dbPath, state.binding.stateDirectory)) !== canonical(state.binding)
    )
      return refuse("database_identity_changed");
  } catch (cause) {
    state.active = false;
    throw cause;
  }
};
const validateOutside = Effect.fn("DatabaseIncarnation.validateOutside")(function* (
  state: OpenState,
) {
  yield* Effect.try({ try: () => current(state), catch: storageError });
  yield* state.infrastructure.pipe(
    Effect.tapError(() =>
      Effect.sync(() => {
        state.active = false;
      }),
    ),
  );
  yield* Effect.try({ try: () => current(state), catch: storageError });
});

/** Root supplies the actual bootstrap-issued permit; no default can enroll a raw CLI client. */
export class OpenInfrastructure extends Context.Service<
  OpenInfrastructure,
  {
    readonly permit: InfrastructurePermit;
    readonly scope: PhysicalScope;
    // Root's real durable infrastructure binding follows the sequential bootstrap
    // -> existing-pin connection handoff. It must never become a positive default.
    readonly assertRetained: Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/DatabaseIncarnation/OpenInfrastructure") {}

/** Import must have completely closed before this creates/captures the final single-link inode. */
export const prepareOpen = Effect.fn("DatabaseIncarnation.prepareOpen")(function* (
  heldOwner: Owner.HeldRuntimeOwnerToken,
  infrastructurePermit: InfrastructurePermit,
) {
  const infrastructure = yield* OpenInfrastructure;
  const scope = yield* Effect.scope;
  const owner = yield* Effect.try({
    try: () => Owner.inspectHeldDatabaseOwner(heldOwner),
    catch: storageError,
  });
  if (
    infrastructure.permit !== infrastructurePermit ||
    infrastructure.scope.requestedPath !== owner.stateDir ||
    infrastructure.scope.canonicalPath !== owner.stateDir ||
    infrastructure.scope.unresolved.length !== 0
  )
    return yield* new AdmissionError({ reason: "database_infrastructure_coordinate_mismatch" });
  const validate = validateInfrastructurePermit(infrastructurePermit, infrastructure.scope);
  yield* validate;
  yield* infrastructure.assertRetained;
  const directory = freeze(decodePhysicalScope(canonical(infrastructure.scope)));
  const binding = yield* Effect.try({
    try: () => {
      Owner.inspectHeldDatabaseOwner(heldOwner);
      // Exclusive creation never adopts/truncates a losing contender's file. Sync
      // failure is retained as failure; it never selects another incarnation.
      let fd: number | undefined;
      try {
        fd = NodeFS.openSync(owner.dbPath, "wx", 0o600);
        NodeFS.fsyncSync(fd);
      } catch (cause) {
        if (
          !(
            typeof cause === "object" &&
            cause !== null &&
            "code" in cause &&
            cause.code === "EEXIST"
          )
        )
          throw cause;
      } finally {
        if (fd !== undefined) NodeFS.closeSync(fd);
      }
      const result = observe(owner.dbPath, directory);
      const parent = NodeFS.openSync(owner.stateDir, "r");
      try {
        NodeFS.fsyncSync(parent);
      } finally {
        NodeFS.closeSync(parent);
      }
      Owner.inspectHeldDatabaseOwner(heldOwner);
      if (canonical(observe(owner.dbPath, directory)) !== canonical(result))
        return refuse("database_identity_changed");
      return result;
    },
    catch: storageError,
  });
  yield* validate;
  const token: DatabaseOpenIncarnation = Object.freeze({ [openBrand]: true });
  const state: OpenState = {
    owner: heldOwner,
    scope,
    binding,
    infrastructure: infrastructure.assertRetained,
    active: true,
    attempted: false,
  };
  opens.set(token, state);
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      state.active = false;
    }),
  );
  return token;
});

/** Runs on the actual app SqlClient, including its writer worker, before setup/migrations. */
export const bindOpen = Effect.fn("DatabaseIncarnation.bindOpen")(function* (
  token: DatabaseOpenIncarnation,
) {
  const sql = yield* SqlClient.SqlClient;
  const key = objectKey(token);
  const state = key === undefined ? undefined : opens.get(key);
  if (state === undefined)
    return yield* new AdmissionError({ reason: "unknown_database_open_incarnation" });
  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* new AdmissionError({ reason: "database_open_binding_inside_transaction" });
  if (state.attempted || clients.has(sql))
    return yield* new AdmissionError({ reason: "database_open_already_bound" });
  state.attempted = true;
  return yield* Effect.gen(function* () {
    yield* validateOutside(state);
    const databases = yield* sql<{ name: string; file: string }>`PRAGMA database_list`;
    const main = databases.filter((entry) => entry.name === "main");
    if (main.length !== 1 || main[0]?.file !== state.binding.canonicalPath)
      return yield* new AdmissionError({ reason: "application_sql_client_binding_mismatch" });
    yield* validateOutside(state);
    const result: CheckedDatabaseOwnerToken = Object.freeze({ [checkedBrand]: true });
    state.checked = result;
    state.sql = sql;
    checked.set(result, state);
    clients.set(sql, state);
    return result;
  }).pipe(
    Effect.mapError(storageError),
    Effect.onExit((exit) =>
      exit._tag === "Failure"
        ? Effect.sync(() => {
            state.active = false;
          })
        : Effect.void,
    ),
  );
});

/** Revalidate after migrations; a graph rebuild always needs a new pre-open token. */
export const revalidateOpen = Effect.fn("DatabaseIncarnation.revalidateOpen")(function* (
  token: DatabaseOpenIncarnation,
) {
  const sql = yield* SqlClient.SqlClient;
  const state = opens.get(token);
  if (state === undefined || clients.get(sql) !== state || state.checked === undefined)
    return yield* new AdmissionError({ reason: "database_open_not_bound_to_client" });
  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* new AdmissionError({ reason: "database_open_validation_inside_transaction" });
  yield* validateCheckedOwner(state.checked);
});

/** Fresh outside-SQL validation includes the retained actual writer and infrastructure lifetime. */
export const validateCheckedOwner = Effect.fn("DatabaseIncarnation.validateCheckedOwner")(
  function* (token: CheckedDatabaseOwnerToken) {
    const sql = yield* SqlClient.SqlClient;
    const state = checked.get(token);
    if (state === undefined || state.sql !== sql)
      return yield* new AdmissionError({ reason: "application_sql_client_binding_mismatch" });
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new AdmissionError({ reason: "database_open_validation_inside_transaction" });
    yield* Effect.gen(function* () {
      yield* validateOutside(state);
      const rows = yield* sql<{ name: string; file: string }>`PRAGMA database_list`.pipe(
        Effect.mapError(storageError),
      );
      if (
        rows.filter((row) => row.name === "main" && row.file === state.binding.canonicalPath)
          .length !== 1
      )
        return yield* new AdmissionError({ reason: "application_sql_client_binding_mismatch" });
      yield* validateOutside(state);
    }).pipe(
      Effect.onExit((exit) =>
        exit._tag === "Failure"
          ? Effect.sync(() => {
              state.active = false;
            })
          : Effect.void,
      ),
    );
  },
);

/** Compatibility lookup only: it cannot issue authority for an already-open raw client. */
export function checkedForSql(
  owner: Owner.HeldRuntimeOwnerToken,
  sql: SqlClient.SqlClient,
): CheckedDatabaseOwnerToken {
  Owner.inspectHeldDatabaseOwner(owner);
  const state = clients.get(sql);
  if (state === undefined || state.owner !== owner || state.checked === undefined)
    return refuse("application_sql_client_binding_mismatch");
  current(state);
  return state.checked;
}
export function assertBoundDatabaseOwner(token: unknown): void {
  const key = objectKey(token);
  const state = key === undefined ? undefined : checked.get(key);
  if (state === undefined) return refuse("unknown_database_owner_capability");
  current(state);
}
/** Identity is a frozen observation from BEFORE open, never a new pathname sample. */
export function bindingForSql(
  token: CheckedDatabaseOwnerToken,
  sql: SqlClient.SqlClient,
): DatabasePhysicalBinding {
  assertBoundDatabaseOwner(token);
  const state = checked.get(token)!;
  if (clients.get(sql) !== state) return refuse("application_sql_client_binding_mismatch");
  return state.binding;
}
