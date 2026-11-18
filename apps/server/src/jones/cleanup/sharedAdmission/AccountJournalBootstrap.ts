// The synchronous Effect UUID constructor keeps the existing factory context and cryptographic UUIDv4 contract.
import * as SecureRandom from "effect/crypto/SecureRandom";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Bootstrap retains bigint nanosecond identities and synchronous exclusive-create/link/descriptor/parent-fsync ordering; FileSystem.Info exposes Date birthtime.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The native bootstrap uses synchronous host-coordinate helpers alongside its physical adapter without widening the existing factory context.
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Option from "effect/Option";
import * as Journal from "./Journal.ts";
import * as JournalSchema from "./JournalSchema.ts";
import * as NativeJournal from "./NativeJournal.ts";
import { NativeJournalHost } from "./NativeAccount.ts";
import * as Physical from "./PhysicalScope.ts";
import * as EnrollmentJournal from "./EnrollmentJournal.ts";
import { AdmissionError, canonical, failure, ScopeObservation, storageError } from "./Protocol.ts";

export const Anchor = Schema.Struct({
  format: Schema.Literal(1),
  binding: Journal.JournalBinding,
  schemaContractDigest: Schema.NonEmptyString,
  stagingBasename: Schema.NonEmptyString,
});
const decodeAnchor = Schema.decodeUnknownSync(Schema.fromJsonString(Anchor));
const stagePattern = /^\.workspace-admission-v1\.[0-9a-f-]{36}\.sqlite$/;
const exists = (path: string) => {
  try {
    NodeFS.lstatSync(path);
    return true;
  } catch (cause) {
    if (hasCode(cause, "ENOENT")) return false;
    throw cause;
  }
};
const hasCode = (cause: unknown, code: string) =>
  typeof cause === "object" && cause !== null && "code" in cause && cause.code === code;
const syncPath = (path: string) => {
  const fd = NodeFS.openSync(path, "r");
  try {
    NodeFS.fsyncSync(fd);
  } finally {
    NodeFS.closeSync(fd);
  }
};
const physicalFile = (path: string) => {
  const stat = NodeFS.lstatSync(path, { bigint: true });
  return { device: String(stat.dev), inode: String(stat.ino), birth: String(stat.birthtimeNs) };
};
const checkedFile = (path: string, uid: string, links: readonly bigint[]) => {
  const stat = NodeFS.lstatSync(path, { bigint: true });
  if (
    !stat.isFile() ||
    String(stat.uid) !== uid ||
    (stat.mode & 0o077n) !== 0n ||
    !links.includes(stat.nlink) ||
    NodeFS.realpathSync(path) !== path
  )
    throw new AdmissionError({ reason: "bootstrap_file_unsupported" });
};
const fixedAnchorPath = (path: string) => path.replace(/\.sqlite$/, ".enrollment.json");

/** Instrumentation is synthetic-only. Production supplies neither hooks nor account overrides. */
export class BootstrapBoundary extends Context.Service<
  BootstrapBoundary,
  {
    readonly reached: (stage: string) => Effect.Effect<void, AdmissionError>;
  }
>()("t3/jones/cleanup/sharedAdmission/AccountJournalBootstrap/BootstrapBoundary") {}

export const makeForHost = Effect.fn("AccountJournalBootstrap.makeForHost")(function* () {
  const ambient = yield* Effect.serviceOption(SqlClient.SqlClient);
  if (
    Option.isSome(ambient) &&
    Option.isSome(yield* Effect.serviceOption(ambient.value.transactionService))
  )
    return yield* failure("bootstrap_inside_local_transaction");
  const host = yield* NativeJournalHost;
  const hook = yield* Effect.serviceOption(BootstrapBoundary);
  const reached = (stage: string) =>
    hook._tag === "Some" ? hook.value.reached(stage) : Effect.void;
  const account = yield* host.account;
  const path = yield* Effect.try({
    try: () => Journal.fixedAccountJournalPath(account),
    catch: storageError,
  });
  const directory = NodePath.dirname(path);
  const anchorPath = fixedAnchorPath(path);
  yield* Effect.try({
    try: () => {
      let current = account.home;
      for (const component of ["", ".local", "state", "jones-code"]) {
        if (component !== "") {
          const parent = current;
          current = NodePath.join(current, component);
          if (!exists(current)) {
            try {
              NodeFS.mkdirSync(current, { mode: 0o700 });
            } catch (cause) {
              if (!hasCode(cause, "EEXIST")) throw cause;
            }
            syncPath(parent);
          }
        }
        const stat = NodeFS.lstatSync(current, { bigint: true });
        if (
          !stat.isDirectory() ||
          String(stat.uid) !== account.uid ||
          (stat.mode & 0o022n) !== 0n ||
          NodeFS.realpathSync(current) !== current
        )
          throw new AdmissionError({ reason: "bootstrap_directory_unsupported" });
        syncPath(current);
      }
    },
    catch: storageError,
  });
  const observations = Context.get(yield* Layer.build(Physical.layer), ScopeObservation);
  const parent = yield* observations.capture(directory);
  const readAnchor = Effect.try({
    try: () => {
      checkedFile(anchorPath, account.uid, [1n, 2n]);
      const raw = NodeFS.readFileSync(anchorPath, "utf8");
      const anchor = decodeAnchor(raw);
      if (
        raw !== canonical(anchor) ||
        anchor.schemaContractDigest !== JournalSchema.contractDigest ||
        !stagePattern.test(anchor.stagingBasename) ||
        anchor.binding.canonicalPath !== path ||
        anchor.binding.uid !== account.uid ||
        canonical(anchor.binding.parent) !== canonical(parent)
      )
        throw new AdmissionError({ reason: "bootstrap_anchor_mismatch" });
      return anchor;
    },
    catch: storageError,
  });
  if (!(yield* Effect.try({ try: () => exists(anchorPath), catch: storageError }))) {
    if (yield* Effect.try({ try: () => exists(path) && !exists(anchorPath), catch: storageError }))
      return yield* failure("bootstrap_unanchored_journal");
    const stagingBasename = `.workspace-admission-v1.${SecureRandom.uuidV4Unsafe()}.sqlite`;
    const stagingPath = NodePath.join(directory, stagingBasename);
    yield* Effect.try({
      try: () => {
        const fd = NodeFS.openSync(stagingPath, "wx", 0o600);
        NodeFS.closeSync(fd);
      },
      catch: storageError,
    });
    yield* reached("stage_created");
    const binding: Journal.JournalBinding = {
      journalId: SecureRandom.uuidV4Unsafe(),
      schema: 1,
      uid: account.uid,
      canonicalPath: path,
      file: yield* Effect.try({ try: () => physicalFile(stagingPath), catch: storageError }),
      parent,
    };
    yield* Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* host.open(stagingPath);
        yield* sql`PRAGMA journal_mode=DELETE`;
        yield* sql`PRAGMA synchronous=FULL`;
        yield* sql`PRAGMA fullfsync=ON`;
        yield* sql`PRAGMA checkpoint_fullfsync=ON`;
        const mode = yield* sql<{ journal_mode: string }>`PRAGMA journal_mode`;
        const sync = yield* sql<{ synchronous: number }>`PRAGMA synchronous`;
        const full = yield* sql<{ fullfsync: number }>`PRAGMA fullfsync`;
        if (
          mode[0]?.journal_mode !== "delete" ||
          (sync[0]?.synchronous ?? 0) < 2 ||
          full[0]?.fullfsync !== 1
        )
          return yield* failure("bootstrap_durability_unavailable");
        yield* sql.withTransaction(JournalSchema.create(sql, canonical(binding)));
      }),
    ).pipe(Effect.mapError(storageError));
    yield* Effect.try({
      try: () => {
        checkedFile(stagingPath, account.uid, [1n]);
        if (
          canonical(physicalFile(stagingPath)) !== canonical(binding.file) ||
          ["-wal", "-shm", "-journal"].some((suffix) => exists(stagingPath + suffix))
        )
          throw new AdmissionError({ reason: "bootstrap_stage_not_closed" });
        syncPath(stagingPath);
        syncPath(directory);
      },
      catch: storageError,
    });
    yield* reached("stage_durable");
    const candidate = NodePath.join(
      directory,
      `.workspace-admission-v1.${SecureRandom.uuidV4Unsafe()}.anchor`,
    );
    yield* Effect.try({
      try: () => {
        const fd = NodeFS.openSync(candidate, "wx", 0o600);
        try {
          NodeFS.writeFileSync(
            fd,
            canonical({
              format: 1,
              binding,
              schemaContractDigest: JournalSchema.contractDigest,
              stagingBasename,
            }),
          );
          NodeFS.fsyncSync(fd);
        } finally {
          NodeFS.closeSync(fd);
        }
        syncPath(directory);
      },
      catch: storageError,
    });
    yield* reached("candidate_durable");
    // Errors retain exact candidates. Never choose a second UUID after an unknown publication.
    yield* Effect.try({
      try: () => {
        try {
          NodeFS.linkSync(candidate, anchorPath);
        } catch (cause) {
          if (!hasCode(cause, "EEXIST")) throw cause;
        }
      },
      catch: storageError,
    });
    yield* reached("anchor_linked");
    yield* Effect.try({ try: () => syncPath(directory), catch: storageError });
    yield* reached("anchor_durable");
    // Retain candidate links and losing private stages; no orphan sweep is part of bootstrap.
  }
  const anchor = yield* readAnchor;
  const stagingPath = NodePath.join(directory, anchor.stagingBasename);
  yield* observations.revalidate(parent);
  yield* Effect.try({
    try: () => {
      if (!exists(path)) {
        try {
          checkedFile(stagingPath, account.uid, [1n, 2n]);
          if (canonical(physicalFile(stagingPath)) !== canonical(anchor.binding.file))
            throw new AdmissionError({ reason: "bootstrap_stage_binding_changed" });
          try {
            NodeFS.linkSync(stagingPath, path);
          } catch (cause) {
            if (!hasCode(cause, "EEXIST")) throw cause;
          }
        } catch (cause) {
          // Another joiner may have completed this same publication and removed
          // the original stage. Only the exact final binding below can resolve it.
          if (!hasCode(cause, "ENOENT") || !exists(path)) throw cause;
        }
      }
      checkedFile(path, account.uid, [1n, 2n]);
      if (canonical(physicalFile(path)) !== canonical(anchor.binding.file))
        throw new AdmissionError({ reason: "bootstrap_final_binding_changed" });
    },
    catch: storageError,
  });
  yield* reached("journal_linked");
  yield* Effect.try({ try: () => syncPath(directory), catch: storageError });
  yield* reached("journal_durable");
  yield* Effect.try({
    try: () => {
      if (exists(stagingPath)) {
        try {
          checkedFile(stagingPath, account.uid, [2n]);
          if (canonical(physicalFile(stagingPath)) !== canonical(anchor.binding.file))
            throw new AdmissionError({ reason: "bootstrap_stage_binding_changed" });
          NodeFS.unlinkSync(stagingPath);
        } catch (cause) {
          if (!hasCode(cause, "ENOENT")) throw cause;
        }
      }
      syncPath(directory);
      checkedFile(path, account.uid, [1n]);
    },
    catch: storageError,
  });
  yield* reached("stage_unlinked");
  const adapter = yield* NativeJournal.makeForHost(anchor.binding);
  const connection = yield* adapter.connectFixedAccountJournal;
  const infrastructure = yield* EnrollmentJournal.makeInfrastructure.pipe(
    Effect.provideService(Journal.JournalConnection, connection),
  );
  yield* infrastructure.beginInfrastructure(parent);
  return {
    binding: connection.binding,
    connection,
    beginInfrastructure: infrastructure.beginInfrastructure,
    observeCreatedInfrastructure: infrastructure.observeCreatedInfrastructure,
    bindStoreInfrastructure: infrastructure.bindStoreInfrastructure,
  };
});
