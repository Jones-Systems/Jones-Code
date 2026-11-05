// @effect-diagnostics nodeBuiltinImport:off -- This capability checks the retained native server-owner handle without opening a second lock descriptor.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import type * as NodeSqlite from "node:sqlite";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export class CheckedRuntimeOwnerError extends Schema.TaggedError<CheckedRuntimeOwnerError>()(
  "CheckedRuntimeOwnerError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}

const isCheckedRuntimeOwnerError = Schema.is(CheckedRuntimeOwnerError);

const heldBrand: unique symbol = Symbol("held-server-owner");
const databaseBrand: unique symbol = Symbol("checked-database-owner");
export type HeldRuntimeOwnerToken = { readonly [heldBrand]: true };
export type CheckedDatabaseOwnerToken = { readonly [databaseBrand]: true };
type Identity = Pick<NodeFS.BigIntStats, "dev" | "ino">;
type DirectoryBinding = { readonly path: string; readonly identity: Identity };
interface HeldState {
  readonly connection: NodeSqlite.DatabaseSync;
  readonly stateDir: string;
  readonly lockPath: string;
  readonly lockIdentity: Identity;
  readonly directory: DirectoryBinding | undefined;
  readonly initialFailure: unknown;
  active: boolean;
}
interface DatabaseBinding {
  readonly owner: HeldRuntimeOwnerToken;
  readonly path: string;
  readonly identity: Identity;
}
const heldOwners = new WeakMap<object, HeldState>();
const databaseOwners = new WeakMap<object, DatabaseBinding>();
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
): HeldRuntimeOwnerToken {
  if (connection.isTransaction) refuse("unexpected_existing_owner_transaction");
  // This is the original ownership transaction, not another lease or connection.
  // Keep native busy errors intact for ServerAlreadyRunningError classification.
  connection.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;");
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
  if (!state.active || !state.connection.isOpen || !state.connection.isTransaction)
    return refuse("owner_scope_closed");
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
    if (isCheckedRuntimeOwnerError(cause)) throw cause;
    return refuse("owner_identity_unavailable", cause);
  }
  return state;
};

const databaseIdentity = (path: string, stateDir: string): Identity => {
  if (path !== NodePath.join(stateDir, "statev2.sqlite"))
    return refuse("database_outside_owned_state_directory");
  try {
    const entry = NodeFS.lstatSync(path, { bigint: true });
    if (!entry.isFile() || entry.nlink !== 1n || NodeFS.realpathSync(path) !== path)
      return refuse("unsupported_database_alias");
    return entry;
  } catch (cause) {
    if (isCheckedRuntimeOwnerError(cause)) throw cause;
    return refuse("database_identity_unavailable", cause);
  }
};

/** Bind only after the actual persistence layer has opened its application DB. */
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
  const capture = () => {
    const state = currentOwner(owner);
    if (
      paths.stateDir !== state.stateDir ||
      paths.serverRuntimeStatePath !== NodePath.join(state.stateDir, "server-runtime.json")
    )
      return refuse("configured_owner_binding_mismatch");
    return databaseIdentity(paths.dbPath, state.stateDir);
  };
  const mapError = (cause: unknown) =>
    isCheckedRuntimeOwnerError(cause)
      ? cause
      : new CheckedRuntimeOwnerError({ reason: "database_owner_binding_failed", cause });
  const identity = yield* Effect.try({ try: capture, catch: mapError });
  // Verify the actual owning SqlClient, rather than trusting configuration paths
  // alongside a raw in-memory or differently bound persistence layer.
  const databases = yield* sql<{ name: string; file: string }>`PRAGMA database_list`.pipe(
    Effect.mapError(mapError),
  );
  const main = databases.filter((entry) => entry.name === "main");
  if (main.length !== 1 || main[0]?.file !== paths.dbPath)
    return yield* new CheckedRuntimeOwnerError({
      reason: "application_sql_client_binding_mismatch",
    });
  return yield* Effect.try({
    try: () => {
      if (!matches(capture(), identity)) return refuse("database_identity_changed_during_binding");
      const token: CheckedDatabaseOwnerToken = Object.freeze({ [databaseBrand]: true });
      databaseOwners.set(token, { owner, path: paths.dbPath, identity });
      return token;
    },
    catch: mapError,
  });
});

/** Must run outside SQL, at admission and immediately before the guarded effect. */
export function assertCheckedDatabaseOwner(owner: unknown): void {
  const key = objectKey(owner);
  const binding = key === undefined ? undefined : databaseOwners.get(key);
  if (binding === undefined) return refuse("unknown_database_owner_capability");
  const state = currentOwner(binding.owner);
  if (!matches(databaseIdentity(binding.path, state.stateDir), binding.identity))
    return refuse("database_identity_changed");
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
