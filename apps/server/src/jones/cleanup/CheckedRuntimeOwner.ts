// @effect-diagnostics nodeBuiltinImport:off -- This capability checks the retained native server-owner handle without opening a second lock descriptor.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import type * as NodeSqlite from "node:sqlite";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as Incarnation from "./sharedAdmission/DatabaseIncarnation.ts";

export class CheckedRuntimeOwnerError extends Schema.TaggedError<CheckedRuntimeOwnerError>()(
  "CheckedRuntimeOwnerError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}

const isCheckedRuntimeOwnerError = Schema.is(CheckedRuntimeOwnerError);

const heldBrand: unique symbol = Symbol("held-server-owner");
export type HeldRuntimeOwnerToken = { readonly [heldBrand]: true };
const localDatabaseBrand: unique symbol = Symbol("local-database-owner");
type LocalDatabaseOwnerToken = { readonly [localDatabaseBrand]: true };
export type CheckedDatabaseOwnerToken =
  | LocalDatabaseOwnerToken
  | Incarnation.CheckedDatabaseOwnerToken;
type Identity = Pick<NodeFS.BigIntStats, "dev" | "ino">;
type DirectoryBinding = { readonly path: string; readonly identity: Identity };
interface HeldState {
  readonly connection: NodeSqlite.DatabaseSync;
  readonly scope: Scope.Scope;
  readonly stateDir: string;
  readonly lockPath: string;
  readonly lockIdentity: Identity;
  readonly directory: DirectoryBinding | undefined;
  readonly initialFailure: unknown;
  active: boolean;
}
const heldOwners = new WeakMap<object, HeldState>();
type LocalDatabaseBinding = {
  readonly owner: HeldRuntimeOwnerToken;
  readonly scope: Scope.Scope;
  readonly path: string;
  readonly identity: Pick<NodeFS.BigIntStats, "dev" | "ino" | "birthtimeNs">;
  active: boolean;
};
const localDatabaseOwners = new WeakMap<object, LocalDatabaseBinding>();
const localDatabaseClients = new WeakMap<SqlClient.SqlClient, LocalDatabaseOwnerToken>();
const matches = (left: Identity, right: Identity) =>
  left.dev === right.dev && left.ino === right.ino;
const objectKey = (value: unknown): object | undefined =>
  typeof value === "object" && value !== null ? value : undefined;
const refuse = (reason: string, cause?: unknown): never => {
  throw new CheckedRuntimeOwnerError({ reason, ...(cause === undefined ? {} : { cause }) });
};

/** Called only at the existing native acquisition boundary, before publication. */
export function beginCheckedServerOwnership(
  connection: NodeSqlite.DatabaseSync,
  stateDir: string,
  lockPath: string,
  scope: Scope.Scope,
): HeldRuntimeOwnerToken {
  if (connection.isTransaction) refuse("unexpected_existing_owner_transaction");
  // This is the original ownership transaction, not another lease or connection.
  // Keep native busy errors intact for ServerAlreadyRunningError classification.
  // An idle service launcher briefly probes this same lock. Preserve startup's
  // 250 ms tolerance while issuing the one original ownership transaction.
  connection.exec("PRAGMA busy_timeout = 250; BEGIN EXCLUSIVE;");
  const lockIdentity = NodeFS.statSync(lockPath, { bigint: true });
  let directory: DirectoryBinding | undefined;
  let initialFailure: unknown;
  try {
    if (!NodePath.isAbsolute(stateDir) || NodePath.normalize(stateDir) !== stateDir)
      refuse("noncanonical_state_directory");
    if (NodeFS.realpathSync(stateDir) !== stateDir) refuse("aliased_state_directory");
    const identity = NodeFS.lstatSync(stateDir, { bigint: true });
    if (!identity.isDirectory()) refuse("invalid_state_directory");
    const lockEntry = NodeFS.lstatSync(lockPath, { bigint: true });
    if (
      lockPath !== NodePath.join(stateDir, "server-owner.sqlite") ||
      !lockEntry.isFile() ||
      lockEntry.nlink !== 1n ||
      !matches(lockIdentity, lockEntry)
    )
      refuse("unsupported_owner_lock_binding");
    const main = connection
      .prepare("PRAGMA database_list")
      .all()
      .find((row) => row.name === "main");
    if (main?.file !== lockPath) refuse("owner_connection_path_mismatch");
    directory = { path: stateDir, identity };
  } catch (cause) {
    // Existing startup routes retain their lock behavior. Unsupported bindings
    // cannot later supply a cleanup capability; they are not startup authority.
    initialFailure = cause;
  }
  const token: HeldRuntimeOwnerToken = Object.freeze({ [heldBrand]: true });
  heldOwners.set(token, {
    connection,
    scope,
    stateDir,
    lockPath,
    lockIdentity,
    directory,
    initialFailure,
    active: true,
  });
  return token;
}

/** Revoke before the owning scope starts finalization, not after its handle closes. */
export function invalidateCheckedServerOwnership(owner: HeldRuntimeOwnerToken): void {
  const state = heldOwners.get(owner);
  if (state === undefined) return refuse("unknown_owner_capability");
  state.active = false;
}

const currentOwner = (owner: unknown): HeldState => {
  const key = objectKey(owner);
  const state = key === undefined ? undefined : heldOwners.get(key);
  if (state === undefined) return refuse("unknown_owner_capability");
  // Effect 4 closes the real scope before running any of its finalizers. A
  // release finalizer alone would leave authority live during earlier teardown.
  if (
    !state.active ||
    state.scope.state._tag === "Closed" ||
    !state.connection.isOpen ||
    !state.connection.isTransaction
  ) {
    state.active = false;
    return refuse("owner_scope_closed");
  }
  if (state.directory === undefined)
    return refuse("owner_binding_unavailable", state.initialFailure);
  try {
    const directory = NodeFS.lstatSync(state.stateDir, { bigint: true });
    if (
      !directory.isDirectory() ||
      !matches(directory, state.directory.identity) ||
      NodeFS.realpathSync(state.stateDir) !== state.directory.path
    )
      return refuse("state_directory_identity_changed");
    // Stat only. Opening then closing any other descriptor for this inode could
    // release the retained connection's POSIX locks.
    const lock = NodeFS.lstatSync(state.lockPath, { bigint: true });
    if (!lock.isFile() || lock.nlink !== 1n || !matches(lock, state.lockIdentity))
      return refuse("owner_lock_identity_changed");
  } catch (cause) {
    // Loss is permanent for this incarnation, even if a path is restored later.
    state.active = false;
    if (isCheckedRuntimeOwnerError(cause)) throw cause;
    return refuse("owner_identity_unavailable", cause);
  }
  return state;
};

/** Observe the original held owner, without another lock connection or acquisition. */
export function inspectHeldDatabaseOwner(owner: HeldRuntimeOwnerToken) {
  const state = currentOwner(owner);
  return Object.freeze({
    stateDir: state.stateDir,
    dbPath: NodePath.join(state.stateDir, "statev2.sqlite"),
    scope: state.scope,
  });
}

const databaseIdentity = (path: string) => {
  const entry = NodeFS.lstatSync(path, { bigint: true });
  if (!entry.isFile() || entry.nlink !== 1n || NodeFS.realpathSync(path) !== path)
    return refuse("unsupported_database_alias");
  return entry;
};

const bindingError = (cause: unknown) =>
  isCheckedRuntimeOwnerError(cause)
    ? cause
    : new CheckedRuntimeOwnerError({ reason: "database_owner_binding_failed", cause });

const currentLocalDatabase = (binding: LocalDatabaseBinding) => {
  try {
    currentOwner(binding.owner);
    if (!binding.active) return refuse("database_binding_invalidated");
    if (binding.scope.state._tag === "Closed") return refuse("database_open_scope_closed");
    const identity = databaseIdentity(binding.path);
    if (
      !matches(identity, binding.identity) ||
      identity.birthtimeNs !== binding.identity.birthtimeNs
    )
      return refuse("database_identity_changed");
  } catch (cause) {
    binding.active = false;
    throw bindingError(cause);
  }
};

const checkLocalDatabase = Effect.fn("CheckedRuntimeOwner.checkLocalDatabase")(function* (
  binding: LocalDatabaseBinding,
) {
  const sql = yield* SqlClient.SqlClient;
  const checkIdentity = Effect.try({
    try: () => currentLocalDatabase(binding),
    catch: bindingError,
  });
  yield* checkIdentity;
  const rows = yield* sql<{ name: string; file: string }>`PRAGMA database_list`.pipe(
    Effect.mapError(bindingError),
  );
  if (rows.find((row) => row.name === "main")?.file !== binding.path)
    return yield* new CheckedRuntimeOwnerError({
      reason: "application_sql_client_binding_mismatch",
    });
  yield* checkIdentity;
});

const reportUnavailableLocalOwner = ({ reason }: CheckedRuntimeOwnerError) =>
  Effect.logWarning("Runtime database owner proof unavailable.", { reason });

/** Capture before the managed worker opens. Only server persistence invokes the returned binder. */
export const prepareLocalDatabaseOpen = Effect.fn("CheckedRuntimeOwner.prepareLocalDatabaseOpen")(
  function* (owner: HeldRuntimeOwnerToken, dbPath: string) {
    const scope = yield* Effect.scope;
    const binding: LocalDatabaseBinding = yield* Effect.try({
      try: () => {
        const coordinates = inspectHeldDatabaseOwner(owner);
        if (coordinates.dbPath !== dbPath) return refuse("configured_owner_binding_mismatch");
        // SQLite creates this file on first open. Create it exclusively so we can pin
        // its identity before opening the actual worker, without replacing an existing file.
        try {
          NodeFS.closeSync(NodeFS.openSync(dbPath, "wx", 0o600));
        } catch (cause) {
          if (!(cause instanceof Error && "code" in cause && cause.code === "EEXIST")) throw cause;
        }
        const identity = databaseIdentity(dbPath);
        return { owner, scope, path: dbPath, identity, active: true };
      },
      catch: bindingError,
    });
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* checkLocalDatabase(binding);
      const token: LocalDatabaseOwnerToken = Object.freeze({ [localDatabaseBrand]: true });
      localDatabaseOwners.set(token, binding);
      localDatabaseClients.set(sql, token);
    }).pipe(Effect.catch(reportUnavailableLocalOwner));
  },
  // Retain startup compatibility for unsupported aliases. Runtime use still requires proof.
  Effect.catch((error) => reportUnavailableLocalOwner(error).pipe(Effect.as(Effect.void))),
);

/** Only clients certified by a server persistence open can bind. */
export const bindCheckedDatabaseOwner = Effect.fn("CheckedRuntimeOwner.bindDatabase")(function* (
  owner: HeldRuntimeOwnerToken,
  paths: {
    readonly stateDir: string;
    readonly dbPath: string;
    readonly serverRuntimeStatePath: string;
  },
) {
  const sql = yield* SqlClient.SqlClient;
  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* new CheckedRuntimeOwnerError({ reason: "owner_binding_inside_sql_transaction" });
  yield* Effect.try({
    try: () => {
      const state = inspectHeldDatabaseOwner(owner);
      if (
        paths.stateDir !== state.stateDir ||
        paths.dbPath !== state.dbPath ||
        paths.serverRuntimeStatePath !== NodePath.join(state.stateDir, "server-runtime.json")
      )
        return refuse("configured_owner_binding_mismatch");
    },
    catch: bindingError,
  });
  const localToken = localDatabaseClients.get(sql);
  if (localToken !== undefined) {
    const binding = localDatabaseOwners.get(localToken);
    if (binding?.owner !== owner)
      return yield* new CheckedRuntimeOwnerError({
        reason: "application_sql_client_binding_mismatch",
      });
    yield* checkLocalDatabase(binding);
    return localToken;
  }
  const token = yield* Effect.try({
    try: () => Incarnation.checkedForSql(owner, sql),
    catch: bindingError,
  });
  yield* Incarnation.validateCheckedOwner(token).pipe(
    Effect.mapError(
      (cause) => new CheckedRuntimeOwnerError({ reason: "database_open_validation_failed", cause }),
    ),
  );
  return token;
});

/** Must run outside SQL at admission and immediately before the guarded effect. */
export function assertCheckedDatabaseOwner(owner: unknown): void {
  const key = objectKey(owner);
  const local = key === undefined ? undefined : localDatabaseOwners.get(key);
  if (local !== undefined) currentLocalDatabase(local);
  else Incarnation.assertBoundDatabaseOwner(owner);
}

// No default: raw layers and serialized/PID/runtime-record claims provide none.
export class HeldRuntimeOwner extends Context.Service<
  HeldRuntimeOwner,
  { readonly token: HeldRuntimeOwnerToken }
>()("t3/jones/cleanup/CheckedRuntimeOwner/HeldRuntimeOwner") {}

/** Derive coordinates from the retained owner itself, rather than a caller's
 * runtime JSON or path assertion. The actual SqlClient still binds independently. */
export const bindHeldDatabaseOwner = Effect.fn("CheckedRuntimeOwner.bindHeldDatabase")(function* (
  owner: HeldRuntimeOwnerToken,
) {
  const sql = yield* SqlClient.SqlClient;
  if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
    return yield* new CheckedRuntimeOwnerError({ reason: "owner_binding_inside_sql_transaction" });
  const paths = yield* Effect.try({
    try: () => {
      const state = currentOwner(owner);
      return {
        stateDir: state.stateDir,
        dbPath: NodePath.join(state.stateDir, "statev2.sqlite"),
        serverRuntimeStatePath: NodePath.join(state.stateDir, "server-runtime.json"),
      };
    },
    catch: (cause) =>
      isCheckedRuntimeOwnerError(cause)
        ? cause
        : new CheckedRuntimeOwnerError({ reason: "owner_binding_unavailable", cause }),
  });
  return yield* bindCheckedDatabaseOwner(owner, paths);
});
