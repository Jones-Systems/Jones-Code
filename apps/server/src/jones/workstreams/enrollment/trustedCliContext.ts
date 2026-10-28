// @effect-diagnostics nodeBuiltinImport:off -- Standalone preflight reads only explicit, preexisting owner-controlled metadata.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import packageJson from "../../../../package.json" with { type: "json" };
import { WorkstreamsNativeBuild } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { JonesNativeWriterAdmission } from "@t3tools/shared/jones/nativeWriterFence";
import {
  currentQualifiedRuntimeBinding,
  decodeQualifiedRuntimeReceipt,
  readQualifiedRuntimeReceipt,
  QUALIFIED_RUNTIME_RECEIPT,
} from "../../cloud/qualifiedRuntime.ts";
import {
  parseServiceState,
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_RESTART_PENDING_FILE,
  SERVICE_STATE_FILE,
} from "../../../cloud/serviceProtocol.ts";
import type { NativeStoreAuthority } from "../../../environment/NativeStoreAuthority.ts";
import {
  NativeStoreAuthorityPersistenceError,
  readExistingNativeStoreAuthorityState,
  requireNativeStoreAuthorityLauncherProtocolForBaseDir,
} from "../../../environment/nativeStoreAuthorityPersistence.ts";
import { validateNativeStoreAuthorityPath } from "../../../environment/nativeStoreAuthorityPath.ts";
import {
  makeNativeEnrollmentSqliteConfig,
  NativeEnrollmentSqliteQualificationError,
  type NativeEnrollmentSqliteAccess,
  type NativeEnrollmentSqliteConfig,
} from "./sqliteConnection.ts";

export interface NativeEnrollmentCliLocation {
  readonly baseDir: string;
  readonly authorityDir: string;
}
export class NativeEnrollmentCliContextError extends Schema.TaggedError<NativeEnrollmentCliContextError>()(
  "NativeEnrollmentCliContextError",
  {
    code: Schema.Literals([
      "invalid_location",
      "identity_unavailable",
      "launcher_unavailable",
      "authority_unavailable",
      "runtime_unqualified",
      "source_mismatch",
      "context_changed",
      "database_unavailable",
      "database_wal_unqualified",
    ]),
  },
) {}
export interface TrustedNativeEnrollmentCliContext {
  readonly baseDir: string;
  readonly dbPath: string;
  readonly secretsDir: string;
  readonly sqlite: NativeEnrollmentSqliteConfig;
  readonly build: WorkstreamsNativeBuild;
  readonly authority: Pick<NativeStoreAuthority["Service"], "readCurrent">;
  readonly bindAuthorityToSqlClient: (
    client: SqlClient.SqlClient,
  ) => Effect.Effect<
    TrustedNativeEnrollmentCliContext["authority"],
    NativeEnrollmentCliContextError
  >;
  readonly verifyUnchanged: Effect.Effect<void, NativeEnrollmentCliContextError>;
}
const isContextError = Schema.is(NativeEnrollmentCliContextError);
const decodeBuild = Schema.decodeUnknownSync(WorkstreamsNativeBuild);
const isJournalMode = Schema.is(
  Schema.Array(
    Schema.Struct({ journal_mode: Schema.Literals(["delete", "truncate", "persist", "wal"]) }),
  ),
);
const isSchemaVersion = Schema.is(Schema.Array(Schema.Struct({ schema_version: Schema.Number })));
const fail = (code: NativeEnrollmentCliContextError["code"]): never => {
  throw new NativeEnrollmentCliContextError({ code });
};
const uid = () =>
  typeof process.getuid === "function" ? process.getuid() : fail("invalid_location");
const safeDirectory = (directory: string) => {
  const stat = NodeFS.lstatSync(directory);
  if (
    stat.isSymbolicLink() ||
    !stat.isDirectory() ||
    stat.uid !== uid() ||
    (stat.mode & 0o022) !== 0
  )
    fail("invalid_location");
};
const ownedFile = (path: string, maxBytes: number): string => {
  const fd = NodeFS.openSync(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const stat = NodeFS.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== uid() || (stat.mode & 0o022) !== 0 || stat.size > maxBytes)
      fail("invalid_location");
    const bytes = Buffer.alloc(maxBytes + 1);
    const count = NodeFS.readSync(fd, bytes, 0, bytes.length, 0);
    if (count > maxBytes) fail("invalid_location");
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, count));
  } finally {
    NodeFS.closeSync(fd);
  }
};
const absence = (path: string) => {
  try {
    NodeFS.lstatSync(path);
    return false;
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT")
      return true;
    throw error;
  }
};
type DatabaseReadPhase =
  | "outside_transaction"
  | "owned_transaction"
  | "refresh_selected"
  | "selected_connection";
const databaseIdentity = (
  dbPath: string,
  access: NativeEnrollmentSqliteAccess,
  phase: DatabaseReadPhase,
) => {
  if (phase === "refresh_selected" || phase === "selected_connection")
    return databasePathIdentity(dbPath);
  // Refuse an unowned rollback journal before opening anything: even a failed
  // qualification must preserve locks held by another client in this process.
  if (!absence(`${dbPath}-journal`) && !(access === "apply" && phase === "owned_transaction"))
    fail("database_unavailable");
  // The bound SQLite connection qualifies format/journal state in its transaction.
  // Closing a raw descriptor here would discard that connection's POSIX locks.
  if (phase === "owned_transaction") return databasePathIdentity(dbPath);
  const identity = databasePathIdentity(dbPath);
  const fd = NodeFS.openSync(dbPath, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const stat = NodeFS.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== uid() || (stat.mode & 0o022) !== 0)
      fail("database_unavailable");
    const header = Buffer.alloc(100);
    if (
      NodeFS.readSync(fd, header, 0, header.length, 0) !== 100 ||
      header.subarray(0, 16).toString("ascii") !== "SQLite format 3\0"
    )
      fail("database_unavailable");
    if ((header[18] !== 1 && header[18] !== 2) || (header[19] !== 1 && header[19] !== 2))
      fail("database_unavailable");
    // Strict inspection is limited to a separately quiesced rollback store. These metadata
    // checks do not prove quiescence; readonly WAL connections can write WAL/SHM sidecars.
    if (
      access === "strict_inspection" &&
      (header[18] !== 1 ||
        header[19] !== 1 ||
        !absence(`${dbPath}-wal`) ||
        !absence(`${dbPath}-shm`))
    )
      fail("database_wal_unqualified");
    if (!absence(`${dbPath}-journal`)) fail("database_unavailable");
    if (stat.dev !== identity.device || stat.ino !== identity.inode) fail("database_unavailable");
    return identity;
  } finally {
    NodeFS.closeSync(fd);
  }
};

// Closing an independently opened main-file descriptor can release this process's
// SQLite POSIX locks. Desktop and a bound standalone transaction use SQLite for
// format/journal qualification. Capture comparison pins this pathname's inode.
const databasePathIdentity = (dbPath: string) => {
  const stat = NodeFS.lstatSync(dbPath);
  if (
    stat.isSymbolicLink() ||
    !stat.isFile() ||
    stat.uid !== uid() ||
    (stat.mode & 0o022) !== 0 ||
    NodeFS.realpathSync(dbPath) !== dbPath
  )
    fail("database_unavailable");
  return { device: stat.dev, inode: stat.ino, uid: stat.uid, mode: stat.mode };
};

const capture = async (
  location: NativeEnrollmentCliLocation,
  access: NativeEnrollmentSqliteAccess,
  phase: DatabaseReadPhase,
) => {
  const { baseDir, authorityDir } = location;
  if (
    !NodePath.isAbsolute(baseDir) ||
    !NodePath.isAbsolute(authorityDir) ||
    NodePath.resolve(baseDir) !== baseDir ||
    NodePath.resolve(authorityDir) !== authorityDir ||
    /[\r\n\0]/.test(baseDir + authorityDir)
  )
    fail("invalid_location");
  safeDirectory(baseDir);
  safeDirectory(NodePath.join(baseDir, "userdata"));
  safeDirectory(NodePath.join(baseDir, "runtime"));
  validateNativeStoreAuthorityPath(baseDir, authorityDir);
  const environmentText = ownedFile(NodePath.join(baseDir, "userdata", "environment-id"), 1024);
  const environmentId = environmentText.trim();
  if (environmentId.length === 0 || environmentId.length > 512 || /[\r\n\0]/.test(environmentId))
    fail("identity_unavailable");
  const stateText = ownedFile(NodePath.join(baseDir, "runtime", SERVICE_STATE_FILE), 32768);
  const launcher = parseServiceState(stateText);
  if (
    !launcher ||
    launcher.protocol !== SERVICE_LAUNCHER_PROTOCOL ||
    launcher.update?.status === "pending" ||
    !absence(NodePath.join(baseDir, "runtime", SERVICE_RESTART_PENDING_FILE))
  )
    return fail("launcher_unavailable");
  requireNativeStoreAuthorityLauncherProtocolForBaseDir(
    baseDir,
    SERVICE_LAUNCHER_PROTOCOL,
    packageJson.version,
  );
  const authority = readExistingNativeStoreAuthorityState(authorityDir);
  if (
    authority.state !== "active" ||
    authority.transition_id !== null ||
    authority.environment_id !== environmentId
  )
    fail("authority_unavailable");
  const build = decodeBuild((packageJson as { readonly jonesSource?: unknown }).jonesSource);
  // A selected runtime never opens main outside its selected SQLite connection. Full
  // payload qualification finishes before the operation; writer checks use fixed metadata.
  const boundedReceipt =
    phase === "selected_connection"
      ? decodeQualifiedRuntimeReceipt(
          JSON.parse(
            ownedFile(
              NodePath.join(
                baseDir,
                "runtime",
                "versions",
                launcher.activeVersion,
                QUALIFIED_RUNTIME_RECEIPT,
              ),
              8192,
            ),
          ),
        )
      : undefined;
  if (phase === "selected_connection" && !boundedReceipt) fail("runtime_unqualified");
  const installed =
    phase === "selected_connection"
      ? {
          activeVersion: launcher.activeVersion,
          activeSourceSha: boundedReceipt!.sourceSha,
          environmentId,
          baseDir,
          dbPath: NodePath.join(baseDir, "userdata", "statev2.sqlite"),
        }
      : await currentQualifiedRuntimeBinding(baseDir, launcher.activeVersion);
  const receipt =
    boundedReceipt ?? (await readQualifiedRuntimeReceipt(baseDir, launcher.activeVersion));
  if (
    installed.activeVersion !== packageJson.version ||
    installed.activeSourceSha !== build.sha ||
    receipt.repository !== build.repository ||
    receipt.sourceSha !== build.sha ||
    receipt.sourceTree !== build.tree ||
    installed.environmentId !== environmentId ||
    installed.baseDir !== baseDir
  )
    fail("source_mismatch");
  const receiptText = ownedFile(
    NodePath.join(
      baseDir,
      "runtime",
      "versions",
      launcher.activeVersion,
      QUALIFIED_RUNTIME_RECEIPT,
    ),
    8192,
  );
  const database = databaseIdentity(installed.dbPath, access, phase);
  const sqlite = makeNativeEnrollmentSqliteConfig(installed.dbPath, access);
  return {
    environmentText,
    stateText,
    authority,
    receiptText,
    database,
    build,
    dbPath: installed.dbPath,
    sqlite,
  };
};

const makeTrustedNativeEnrollmentContext = Effect.fn("makeTrustedNativeEnrollmentContext")(
  function* (
    location: NativeEnrollmentCliLocation,
    access: NativeEnrollmentSqliteAccess,
    captureContext: typeof capture,
    beforeRead: Effect.Effect<void, NativeEnrollmentCliContextError> = Effect.void,
    selectedRuntime = false,
  ) {
    const read = (phase: DatabaseReadPhase) =>
      beforeRead.pipe(
        Effect.andThen(
          Effect.tryPromise({
            try: () => captureContext(location, access, phase),
            catch: (error) =>
              isContextError(error)
                ? error
                : new NativeEnrollmentCliContextError({
                    code:
                      error instanceof NativeEnrollmentSqliteQualificationError
                        ? error.code
                        : "runtime_unqualified",
                  }),
          }),
        ),
      );
    const initial = yield* read(selectedRuntime ? "refresh_selected" : "outside_transaction");
    const verify = (phase: DatabaseReadPhase) =>
      read(phase).pipe(
        Effect.flatMap((current) =>
          JSON.stringify(current) === JSON.stringify(initial)
            ? Effect.void
            : Effect.fail(new NativeEnrollmentCliContextError({ code: "context_changed" })),
        ),
      );
    const verifyUnchanged = verify(selectedRuntime ? "selected_connection" : "outside_transaction");
    const authorityFor = (qualified: Effect.Effect<void, NativeEnrollmentCliContextError>) => ({
      readCurrent: qualified.pipe(
        Effect.as({
          environmentId: initial.authority.environment_id,
          authorityNamespace: initial.authority.authority_namespace,
          storeGeneration: initial.authority.store_generation,
        }),
        Effect.mapError(
          () =>
            new NativeStoreAuthorityPersistenceError(
              "source_unavailable",
              "Native enrollment persisted qualification changed.",
            ),
        ),
      ),
    });
    const bindAuthorityToSqlClient: TrustedNativeEnrollmentCliContext["bindAuthorityToSqlClient"] =
      Effect.fn("TrustedNativeEnrollmentCliContext.bindAuthorityToSqlClient")(function* (client) {
        if (
          access !== "apply" ||
          Option.isSome(yield* Effect.serviceOption(client.transactionService))
        )
          return yield* new NativeEnrollmentCliContextError({ code: "database_unavailable" });
        yield* verifyUnchanged;
        const connection = yield* Effect.gen(function* () {
          const connection = yield* client.reserve;
          const databases = yield* connection.execute("PRAGMA database_list", [], undefined).pipe(
            Effect.flatMap(
              Schema.decodeUnknownEffect(
                Schema.Array(
                  Schema.Struct({
                    seq: Schema.Number,
                    name: Schema.String,
                    file: Schema.String,
                  }),
                ),
              ),
            ),
          );
          const main = databases.filter((database) => database.name === "main");
          if (main.length !== 1 || main[0]?.seq !== 0 || main[0]?.file !== initial.dbPath)
            return yield* new NativeEnrollmentCliContextError({ code: "database_unavailable" });
          yield* verifyUnchanged;
          return connection;
        }).pipe(
          Effect.scoped,
          Effect.mapError((error) =>
            isContextError(error)
              ? error
              : new NativeEnrollmentCliContextError({ code: "database_unavailable" }),
          ),
        );
        // Only this verified client's active transaction may account for its journal.
        // SQLite reads the format and journal mode; raw filesystem checks never open main.
        return authorityFor(
          Effect.gen(function* () {
            const selected = yield* Effect.serviceOption(SqlClient.SqlClient);
            if (Option.isSome(selected) && selected.value !== client)
              return yield* new NativeEnrollmentCliContextError({ code: "database_unavailable" });
            const transaction = yield* Effect.serviceOption(client.transactionService);
            if (Option.isNone(transaction)) {
              if (!selectedRuntime) return yield* verifyUnchanged;
              return yield* Effect.gen(function* () {
                const current = yield* client.reserve;
                if (current !== connection)
                  return yield* new NativeEnrollmentCliContextError({
                    code: "database_unavailable",
                  });
                yield* verifyUnchanged;
                const databases = yield* current.execute("PRAGMA database_list", [], undefined);
                const modes = yield* current.execute("PRAGMA main.journal_mode", [], undefined);
                const versions = yield* current.execute(
                  "PRAGMA main.schema_version",
                  [],
                  undefined,
                );
                if (
                  !Array.isArray(databases) ||
                  databases.filter(
                    (d) => d.name === "main" && d.seq === 0 && d.file === initial.dbPath,
                  ).length !== 1 ||
                  !isJournalMode(modes) ||
                  modes.length !== 1 ||
                  !isSchemaVersion(versions) ||
                  versions.length !== 1
                )
                  return yield* new NativeEnrollmentCliContextError({
                    code: "database_unavailable",
                  });
              }).pipe(Effect.scoped);
            }
            if (transaction.value[0] !== connection)
              return yield* new NativeEnrollmentCliContextError({ code: "database_unavailable" });
            yield* verify(selectedRuntime ? "selected_connection" : "owned_transaction");
            const modes = yield* connection.execute("PRAGMA main.journal_mode", [], undefined);
            const versions = yield* connection.execute("PRAGMA main.schema_version", [], undefined);
            if (
              !isJournalMode(modes) ||
              modes.length !== 1 ||
              !isSchemaVersion(versions) ||
              versions.length !== 1
            )
              return yield* new NativeEnrollmentCliContextError({ code: "database_unavailable" });
          }).pipe(
            Effect.mapError(
              () => new NativeEnrollmentCliContextError({ code: "database_unavailable" }),
            ),
          ),
        );
      });
    return {
      baseDir: location.baseDir,
      dbPath: initial.dbPath,
      secretsDir: NodePath.join(location.baseDir, "userdata", "secrets"),
      sqlite: initial.sqlite,
      build: initial.build,
      authority: authorityFor(verifyUnchanged),
      bindAuthorityToSqlClient,
      verifyUnchanged,
    } satisfies TrustedNativeEnrollmentCliContext;
  },
);

// Standalone CLI qualification remains the persisted protocol-4/runtime-receipt path.
export const makeTrustedNativeEnrollmentCliContext = (
  location: NativeEnrollmentCliLocation,
  access: NativeEnrollmentSqliteAccess = "strict_inspection",
) => makeTrustedNativeEnrollmentContext(location, access, capture);

export type NativeEnrollmentContextResolver = typeof makeTrustedNativeEnrollmentCliContext;

/** Reuses the same database/transaction guards with the actual live desktop owner's source binding. */
export const makeTrustedNativeEnrollmentDesktopContext = (
  location: NativeEnrollmentCliLocation,
  access: NativeEnrollmentSqliteAccess,
  owner: JonesNativeWriterAdmission & {
    readonly refresh: Effect.Effect<void, NativeStoreAuthorityPersistenceError>;
  },
  selectedRuntime = false,
) => {
  const captureDesktop: typeof capture = async (currentLocation, currentAccess) => {
    owner.assertCurrent();
    const { baseDir, authorityDir } = currentLocation;
    if (
      baseDir !== owner.binding.home ||
      authorityDir !== NodePath.join(baseDir, "native-store-authority")
    )
      return fail("invalid_location");
    safeDirectory(baseDir);
    safeDirectory(NodePath.join(baseDir, "userdata"));
    validateNativeStoreAuthorityPath(baseDir, authorityDir);
    const environmentText = ownedFile(NodePath.join(baseDir, "userdata", "environment-id"), 1024);
    if (environmentText.trim() !== owner.binding.environmentId) return fail("identity_unavailable");
    const authority = readExistingNativeStoreAuthorityState(authorityDir);
    if (
      authority.state !== "active" ||
      authority.transition_id !== null ||
      authority.environment_id !== owner.binding.environmentId
    )
      return fail("authority_unavailable");
    const build = decodeBuild((packageJson as { readonly jonesSource?: unknown }).jonesSource);
    if (
      build.sha !== owner.binding.sourceSha ||
      build.tree !== owner.binding.sourceTree ||
      packageJson.version !== owner.binding.version
    )
      return fail("source_mismatch");
    const dbPath = owner.binding.databasePath;
    const database = databasePathIdentity(dbPath);
    const sqlite = makeNativeEnrollmentSqliteConfig(dbPath, currentAccess);
    return {
      environmentText,
      stateText: owner.binding.manifestSha256,
      authority,
      receiptText: JSON.stringify(owner.binding),
      database,
      build,
      dbPath,
      sqlite,
    };
  };
  return makeTrustedNativeEnrollmentContext(
    location,
    access,
    captureDesktop,
    selectedRuntime
      ? Effect.void
      : owner.refresh.pipe(
          Effect.mapError(() => new NativeEnrollmentCliContextError({ code: "context_changed" })),
        ),
    selectedRuntime,
  );
};

/** Selected-runtime qualification. All waiting refresh/payload work precedes its local checker. */
export const makeTrustedNativeEnrollmentRuntimeContext = Effect.fn(
  "makeTrustedNativeEnrollmentRuntimeContext",
)(function* (
  location: NativeEnrollmentCliLocation,
  client: SqlClient.SqlClient,
  owner?: JonesNativeWriterAdmission & {
    readonly refresh: Effect.Effect<void, NativeStoreAuthorityPersistenceError>;
  },
) {
  if (Option.isSome(yield* Effect.serviceOption(client.transactionService)))
    return yield* new NativeEnrollmentCliContextError({ code: "database_unavailable" });
  if (owner)
    yield* owner.refresh.pipe(
      Effect.mapError(() => new NativeEnrollmentCliContextError({ code: "context_changed" })),
    );
  const trusted = yield* owner
    ? makeTrustedNativeEnrollmentDesktopContext(location, "apply", owner, true)
    : makeTrustedNativeEnrollmentContext(location, "apply", capture, Effect.void, true);
  const authority = yield* trusted.bindAuthorityToSqlClient(client);
  return {
    ...trusted,
    authority,
    verifyUnchanged: authority.readCurrent.pipe(
      Effect.asVoid,
      Effect.mapError(() => new NativeEnrollmentCliContextError({ code: "context_changed" })),
    ),
  };
});
