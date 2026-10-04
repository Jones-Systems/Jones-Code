// @effect-diagnostics nodeBuiltinImport:off -- Standalone preflight reads only explicit, preexisting owner-controlled metadata.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import packageJson from "../../../package.json" with { type: "json" };
import { WorkstreamsNativeBuild } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  currentQualifiedRuntimeBinding,
  readQualifiedRuntimeReceipt,
  QUALIFIED_RUNTIME_RECEIPT,
} from "../../cloud/qualifiedRuntime.ts";
import {
  parseServiceState,
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_RESTART_PENDING_FILE,
  SERVICE_STATE_FILE,
} from "../../cloud/serviceProtocol.ts";
import type { NativeStoreAuthority } from "../../environment/NativeStoreAuthority.ts";
import {
  NativeStoreAuthorityPersistenceError,
  readExistingNativeStoreAuthorityState,
  requireNativeStoreAuthorityLauncherProtocolForBaseDir,
} from "../../environment/nativeStoreAuthorityPersistence.ts";
import { validateNativeStoreAuthorityPath } from "../../environment/nativeStoreAuthorityPath.ts";
import {
  makeNativeEnrollmentSqliteConfig,
  NativeEnrollmentSqliteQualificationError,
  type NativeEnrollmentSqliteAccess,
  type NativeEnrollmentSqliteConfig,
} from "./sqliteConnection.ts";

export interface NativeEnrollmentCliLocation {
  readonly baseDir: string;
  readonly authorityDir: string;
  readonly dbPath: string;
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
type DatabaseReadPhase = "outside_transaction" | "owned_transaction";
const databaseIdentity = (
  dbPath: string,
  access: NativeEnrollmentSqliteAccess,
  phase: DatabaseReadPhase,
) => {
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
    if (!absence(`${dbPath}-journal`) && !(access === "apply" && phase === "owned_transaction"))
      fail("database_unavailable");
    return { device: stat.dev, inode: stat.ino };
  } finally {
    NodeFS.closeSync(fd);
  }
};

const capture = async (
  location: NativeEnrollmentCliLocation,
  access: NativeEnrollmentSqliteAccess,
  phase: DatabaseReadPhase,
) => {
  const { baseDir, authorityDir, dbPath } = location;
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
    dbPath,
  );
  const authority = readExistingNativeStoreAuthorityState(authorityDir);
  if (
    authority.state !== "active" ||
    authority.transition_id !== null ||
    authority.environment_id !== environmentId
  )
    fail("authority_unavailable");
  const build = Schema.decodeUnknownSync(WorkstreamsNativeBuild)(
    (packageJson as { readonly jonesSource?: unknown }).jonesSource,
  );
  const installed = await currentQualifiedRuntimeBinding(
    baseDir,
    launcher.activeVersion,
    undefined,
    dbPath,
  );
  const receipt = await readQualifiedRuntimeReceipt(baseDir, launcher.activeVersion);
  if (
    installed.activeVersion !== packageJson.version ||
    installed.activeSourceSha !== build.sha ||
    receipt.repository !== build.repository ||
    receipt.sourceSha !== build.sha ||
    receipt.sourceTree !== build.tree ||
    installed.environmentId !== environmentId ||
    installed.baseDir !== baseDir ||
    installed.dbPath !== dbPath
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

// This is persisted preflight, not live IPC proof. Native provider requests still use NativeStoreAuthority's runtime checks.
export const makeTrustedNativeEnrollmentCliContext = Effect.fn(
  "makeTrustedNativeEnrollmentCliContext",
)(function* (
  location: NativeEnrollmentCliLocation,
  access: NativeEnrollmentSqliteAccess = "strict_inspection",
) {
  const read = (phase: DatabaseReadPhase) =>
    Effect.tryPromise({
      try: () => capture(location, access, phase),
      catch: (error) =>
        Schema.is(NativeEnrollmentCliContextError)(error)
          ? error
          : new NativeEnrollmentCliContextError({
              code:
                error instanceof NativeEnrollmentSqliteQualificationError
                  ? error.code
                  : "runtime_unqualified",
            }),
    });
  const initial = yield* read("outside_transaction");
  const verify = (phase: DatabaseReadPhase) =>
    read(phase).pipe(
      Effect.flatMap((current) =>
        JSON.stringify(current) === JSON.stringify(initial)
          ? Effect.void
          : Effect.fail(new NativeEnrollmentCliContextError({ code: "context_changed" })),
      ),
    );
  const verifyUnchanged = verify("outside_transaction");
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
  const bindAuthorityToSqlClient: TrustedNativeEnrollmentCliContext["bindAuthorityToSqlClient"] = (
    client,
  ) =>
    Effect.gen(function* () {
      yield* verifyUnchanged;
      if (
        access !== "apply" ||
        Option.isSome(yield* Effect.serviceOption(client.transactionService))
      )
        return yield* new NativeEnrollmentCliContextError({ code: "database_unavailable" });
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
          Schema.is(NativeEnrollmentCliContextError)(error)
            ? error
            : new NativeEnrollmentCliContextError({ code: "database_unavailable" }),
        ),
      );
      // Only this verified client's active transaction may account for its rollback journal.
      // The same inode and all persisted authority/build metadata are still compared each time.
      return authorityFor(
        Effect.serviceOption(client.transactionService).pipe(
          Effect.flatMap((transaction) =>
            Option.isSome(transaction) && transaction.value[0] === connection
              ? verify("owned_transaction")
              : verifyUnchanged,
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
});
