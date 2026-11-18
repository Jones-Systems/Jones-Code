// @effect-diagnostics-next-line nodeBuiltinImport:off -- The retained pin compares bigint birthtimeNs, dev/inode, links and canonical no-follow metadata; FileSystem.Info exposes Date birthtime.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native pin validation retains synchronous host-coordinate semantics without adding a new requirement to the existing adapter facade.
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Journal from "./Journal.ts";
import * as JournalSchema from "./JournalSchema.ts";
import * as Physical from "./PhysicalScope.ts";
import { AdmissionError, canonical, failure, ScopeObservation, storageError } from "./Protocol.ts";

import { NativeJournalHost, nativeHost } from "./NativeAccount.ts";
export { NativeJournalHost } from "./NativeAccount.ts";

const decodeAnchorJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodePin = Schema.decodeUnknownEffect(Schema.fromJsonString(Journal.JournalBinding));
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const freezePin = (pin: Journal.JournalBinding): Journal.JournalBinding =>
  Object.freeze({
    ...pin,
    file: Object.freeze({ ...pin.file }),
    parent: Object.freeze({
      ...pin.parent,
      ancestors: Object.freeze(
        pin.parent.ancestors.map((ancestor) =>
          Object.freeze({
            ...ancestor,
            identity: Object.freeze({ ...ancestor.identity }),
          }),
        ),
      ) as Journal.JournalBinding["parent"]["ancestors"],
      unresolved: Object.freeze([...pin.parent.unresolved]),
    }),
  });

/** Test seam only. A pin is required: first enrollment and journal creation belong
 * to the startup owner, never a missing-file recovery path in this adapter. */
export const makeForHost = Effect.fn("NativeJournal.makeForHost")(function* (
  localPin: Journal.JournalBinding,
): Effect.fn.Return<
  Journal.ProductionJournalAdapter["Service"],
  AdmissionError,
  NativeJournalHost | Scope.Scope
> {
  const host = yield* NativeJournalHost;
  const lifetime = yield* Effect.scope;
  // Copy before the first host operation; caller mutation cannot replace enrollment identity.
  const pin = freezePin(yield* decodePin(canonical(localPin)).pipe(Effect.mapError(storageError)));
  if (!uuid.test(pin.journalId)) return yield* failure("native_journal_uuid_invalid");
  const account = Object.freeze({ ...(yield* host.account) });
  const path = yield* Effect.try({
    try: () => Journal.fixedAccountJournalPath(account),
    catch: storageError,
  });
  if (pin.uid !== account.uid || pin.canonicalPath !== path)
    return yield* failure("native_journal_local_pin_mismatch");
  const observations = Context.get(yield* Layer.build(Physical.layer), ScopeObservation);
  let invalidated = false;
  const checkFiles = Effect.gen(function* () {
    if (invalidated || lifetime.state._tag === "Closed")
      return yield* failure("native_journal_scope_closed");
    const currentAccount = yield* host.account;
    if (canonical(currentAccount) !== canonical(account))
      return yield* failure("native_journal_account_changed");
    yield* Effect.try({
      try: () => {
        // Every component from the native home must be a canonical account-owned
        // directory without group/other write access. Never mkdir/chmod to repair it.
        let directory = account.home;
        for (const component of ["", ".local", "state", "jones-code"]) {
          if (component !== "") directory = NodePath.join(directory, component);
          const entry = NodeFS.lstatSync(directory, { bigint: true });
          if (
            !entry.isDirectory() ||
            String(entry.uid) !== account.uid ||
            (entry.mode & 0o022n) !== 0n ||
            NodeFS.realpathSync(directory) !== directory
          )
            throw new AdmissionError({ reason: "native_journal_directory_binding_unsupported" });
        }
        const anchorPath = path.replace(/\.sqlite$/, ".enrollment.json");
        const anchorStat = NodeFS.lstatSync(anchorPath, { bigint: true });
        if (
          !anchorStat.isFile() ||
          String(anchorStat.uid) !== account.uid ||
          (anchorStat.mode & 0o022n) !== 0n ||
          NodeFS.realpathSync(anchorPath) !== anchorPath
        )
          throw new AdmissionError({ reason: "native_journal_anchor_unsupported" });
        const anchorRaw = NodeFS.readFileSync(anchorPath, "utf8");
        const anchor: unknown = decodeAnchorJson(anchorRaw);
        if (
          typeof anchor !== "object" ||
          anchor === null ||
          !("format" in anchor) ||
          anchor.format !== 1 ||
          !("binding" in anchor) ||
          canonical(anchor.binding) !== canonical(pin) ||
          !("schemaContractDigest" in anchor) ||
          anchor.schemaContractDigest !== JournalSchema.contractDigest ||
          !("stagingBasename" in anchor) ||
          typeof anchor.stagingBasename !== "string" ||
          !/^\.workspace-admission-v1\.[0-9a-f-]{36}\.sqlite$/.test(anchor.stagingBasename) ||
          canonical(anchor) !== anchorRaw
        )
          throw new AdmissionError({ reason: "native_journal_anchor_mismatch" });
        const entry = NodeFS.lstatSync(path, { bigint: true });
        if (
          !entry.isFile() ||
          entry.nlink !== 1n ||
          String(entry.uid) !== account.uid ||
          (entry.mode & 0o022n) !== 0n ||
          NodeFS.realpathSync(path) !== path ||
          String(entry.dev) !== pin.file.device ||
          String(entry.ino) !== pin.file.inode ||
          String(entry.birthtimeNs) !== pin.file.birth
        )
          throw new AdmissionError({ reason: "native_journal_pinned_file_changed" });
      },
      catch: storageError,
    });
    yield* observations.revalidate(pin.parent);
    if (pin.parent.canonicalPath !== NodePath.dirname(path) || pin.parent.unresolved.length !== 0)
      return yield* failure("native_journal_parent_pin_mismatch");
  }).pipe(
    Effect.tapError(() =>
      Effect.sync(() => {
        invalidated = true;
      }),
    ),
  );
  // A missing/replaced pinned file refuses before the runtime can open or initialize it.
  yield* checkFiles;
  const sql = yield* host.open(path);
  // Bound simultaneous same-account writer admission before the first lock-taking read.
  yield* sql`PRAGMA busy_timeout = 250`.pipe(Effect.mapError(storageError));
  const outside = Effect.gen(function* () {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* failure("native_journal_check_inside_transaction");
    yield* checkFiles;
  });
  const checkSql = Effect.gen(function* () {
    yield* outside;
    const databases = yield* sql<{ name: string; file: string }>`PRAGMA database_list`;
    const main = databases.filter((database) => database.name === "main");
    if (main.length !== 1 || main[0]?.file !== path)
      return yield* failure("native_journal_sql_path_mismatch");
    const rows = yield* sql<{ version: number; binding_json: string }>`SELECT version,binding_json
      FROM jones_common_admission_schema WHERE singleton=1`;
    if (rows.length !== 1 || rows[0]?.version !== 1 || rows[0]?.binding_json !== canonical(pin))
      return yield* failure("native_journal_durable_pin_mismatch");
    yield* checkFiles;
  }).pipe(Effect.mapError(storageError));
  // Validate the existing durable UUID before changing any connection pragmas.
  yield* checkSql;
  yield* sql`PRAGMA synchronous = FULL`.pipe(Effect.mapError(storageError));
  yield* sql`PRAGMA fullfsync = ON`.pipe(Effect.mapError(storageError));
  yield* sql`PRAGMA checkpoint_fullfsync = ON`.pipe(Effect.mapError(storageError));
  const revalidate = Effect.gen(function* () {
    yield* checkSql;
    const sync = yield* sql<{ synchronous: number }>`PRAGMA synchronous`;
    const full = yield* sql<{ fullfsync: number }>`PRAGMA fullfsync`;
    const checkpoint = yield* sql<{ checkpoint_fullfsync: number }>`PRAGMA checkpoint_fullfsync`;
    const mode = yield* sql<{ journal_mode: string }>`PRAGMA journal_mode`;
    const busy = yield* sql<{ timeout: number }>`PRAGMA busy_timeout`;
    if (
      busy[0]?.timeout !== 250 ||
      (sync[0]?.synchronous ?? 0) < 2 ||
      full[0]?.fullfsync !== 1 ||
      checkpoint[0]?.checkpoint_fullfsync !== 1 ||
      !["delete", "truncate", "persist", "wal"].includes(mode[0]?.journal_mode ?? "")
    )
      return yield* failure("native_journal_durability_unavailable");
  }).pipe(
    Effect.mapError(storageError),
    Effect.tapError(() =>
      Effect.sync(() => {
        invalidated = true;
      }),
    ),
  );
  yield* revalidate;
  const connection = Journal.JournalConnection.of({ sql, binding: pin, revalidate });
  return Journal.ProductionJournalAdapter.of({
    connectFixedAccountJournal: Effect.as(revalidate, connection),
  });
});

/** Uses native account coordinates and an immutable enrollment pin; never initializes replacements.
 * Path checks do not prove native fd identity against hostile replace-and-restore.
 * Unlink racing open can create an empty file; post-open checks refuse it without repair. */
export const make = Effect.fn("NativeJournal.make")(function* (localPin: Journal.JournalBinding) {
  return yield* makeForHost(localPin).pipe(Effect.provideService(NativeJournalHost, nativeHost));
});
