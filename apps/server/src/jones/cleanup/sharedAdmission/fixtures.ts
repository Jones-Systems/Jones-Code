// @effect-diagnostics-next-line nodeBuiltinImport:off -- Shared fixtures capture bigint birthtimeNs/dev/inode and synchronously own exact-root setup and teardown.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native fixture setup/teardown shares synchronous host paths with its retained file/process handles and keeps the existing fixture context.
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Journal from "./Journal.ts";
import * as Bridge from "./LocalBridge.ts";
import * as Physical from "./PhysicalScope.ts";
import * as Protocol from "./Protocol.ts";
import { enrollSyntheticFoundation } from "./enrollment.fixture.ts";

export const fileIdentity = (path: string) => {
  const stat = NodeFS.statSync(path, { bigint: true });
  return {
    version: 1 as const,
    device: String(stat.dev),
    inode: String(stat.ino),
    birth: String(stat.birthtimeNs),
  };
};
export const openSql = Effect.fn("SharedAdmissionFixture.openSql")(function* (filename: string) {
  const context = yield* Layer.build(
    NodeSqliteClient.layer({
      filename,
      readerWorker: false,
      writerWorker: false,
      checkpointWorker: false,
    }),
  );
  return Context.get(context, SqlClient.SqlClient);
});
export const makeGraph = Effect.fn("SharedAdmissionFixture.makeGraph")(function* (input: {
  readonly local: SqlClient.SqlClient;
  readonly common: SqlClient.SqlClient;
  readonly identity: Protocol.ParticipantIdentity;
  readonly binding: Journal.JournalBinding;
  readonly initialization?: "existing";
}) {
  let ownerActive = true;
  const terminalEvidence = new Set<string>();
  const recreationEvidence = new Set<string>();
  const participant = Protocol.Participant.of({
    identity: input.identity,
    sql: input.local,
    checkOutside: Effect.gen(function* () {
      if (!ownerActive) return yield* Protocol.failure("synthetic_owner_closed");
      for (const sql of [input.local, input.common])
        if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
          return yield* Protocol.failure("synthetic_physical_check_inside_sql");
    }),
    validateLocal: Effect.gen(function* () {
      if (
        !ownerActive ||
        Option.isNone(yield* Effect.serviceOption(input.local.transactionService)) ||
        Option.isSome(yield* Effect.serviceOption(input.common.transactionService))
      )
        return yield* Protocol.failure("synthetic_local_owner_or_nested_transaction");
    }),
  });
  const connection = Journal.JournalConnection.of({
    sql: input.common,
    binding: input.binding,
    revalidate: Effect.try({
      try: () => {
        const actual = fileIdentity(input.binding.canonicalPath);
        if (
          actual.device !== input.binding.file.device ||
          actual.inode !== input.binding.file.inode ||
          actual.birth !== input.binding.file.birth
        )
          throw new Protocol.AdmissionError({ reason: "synthetic_journal_replaced" });
      },
      catch: Protocol.storageError,
    }),
  });
  const ports = Context.make(Protocol.Participant, participant).pipe(
    Context.add(Journal.JournalConnection, connection),
  );
  if (input.initialization !== "existing") {
    yield* Journal.initializeJournal.pipe(Effect.provide(ports));
    yield* Bridge.createLocalBridgeSchema.pipe(
      Effect.provideService(SqlClient.SqlClient, input.local),
    );
    yield* Bridge.pinLocalInstallation.pipe(Effect.provide(ports));
    yield* input.local`CREATE TABLE IF NOT EXISTS jones_fixture_acceptances (id TEXT PRIMARY KEY)`;
  }
  const journal = Context.get(
    yield* Layer.build(Journal.layer).pipe(Effect.provide(ports)),
    Protocol.CommonJournal,
  );
  if (input.initialization !== "existing") yield* journal.enroll(input.identity);
  const custody = Bridge.LocalCustody.of({
    validateTerminal: Effect.fn("Bridge.LocalCustody.validateTerminal")(function* (evidence) {
      if (!terminalEvidence.has(Protocol.digest(evidence)))
        return yield* Protocol.failure("synthetic_terminal_evidence_not_proved");
    }),
    validateRecreation: Effect.fn("Bridge.LocalCustody.validateRecreation")(
      function* (certificate) {
        if (!recreationEvidence.has(Protocol.digest(certificate)))
          return yield* Protocol.failure("synthetic_recreation_not_proved");
      },
    ),
  });
  const bridge = Context.get(
    yield* Layer.build(Bridge.layer).pipe(
      Effect.provide(ports),
      Effect.provideService(Bridge.LocalCustody, custody),
    ),
    Protocol.LocalBridge,
  );
  const observations = Context.get(yield* Layer.build(Physical.layer), Protocol.ScopeObservation);
  if (input.initialization !== "existing")
    yield* enrollSyntheticFoundation.pipe(
      Effect.provide(ports),
      Effect.provideService(Protocol.ScopeObservation, observations),
    );
  const context = ports.pipe(
    Context.add(Protocol.CommonJournal, journal),
    Context.add(Protocol.LocalBridge, bridge),
    Context.add(Protocol.ScopeObservation, observations),
    Context.add(SqlClient.SqlClient, input.local),
  );
  return {
    ...input,
    participant,
    connection,
    journal,
    bridge,
    observations,
    context,
    closeOwner: () => {
      ownerActive = false;
    },
    terminalEvidence,
    recreationEvidence,
  };
});

export const fixture = Effect.fn("SharedAdmissionFixture.fixture")(function* () {
  // Exactly this fresh root belongs to this fixture; consumers close first,
  // before its registered release removes synthetic databases and workspaces.
  const directory = yield* Effect.acquireRelease(
    Effect.sync(() =>
      NodeFS.realpathSync(
        NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "jones-shared-admission-")),
      ),
    ),
    (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
  );
  const rootPath = NodePath.join(directory, "scratch");
  const aPath = NodePath.join(rootPath, "a");
  const bPath = NodePath.join(rootPath, "b");
  yield* Effect.sync(() => {
    NodeFS.mkdirSync(aPath, { recursive: true });
    NodeFS.mkdirSync(bPath);
    NodeFS.mkdirSync(NodePath.join(aPath, "nested"));
    NodeFS.symlinkSync(rootPath, NodePath.join(directory, "alias"), "dir");
  });
  const commonPath = NodePath.join(directory, "common.sqlite");
  NodeFS.mkdirSync(NodePath.join(directory, "state-a"));
  NodeFS.mkdirSync(NodePath.join(directory, "state-b"));
  const aDb = NodePath.join(directory, "state-a", "a.sqlite");
  const bDb = NodePath.join(directory, "state-b", "b.sqlite");
  const commonA = yield* openSql(commonPath);
  const commonB = yield* openSql(commonPath);
  const localA = yield* openSql(aDb);
  const localB = yield* openSql(bDb);
  const observations = Context.get(yield* Layer.build(Physical.layer), Protocol.ScopeObservation);
  const physicalFile = fileIdentity(commonPath);
  const binding: Journal.JournalBinding = {
    journalId: "synthetic-journal",
    schema: 1,
    uid: String(NodeOS.userInfo().uid),
    canonicalPath: commonPath,
    file: { device: physicalFile.device, inode: physicalFile.inode, birth: physicalFile.birth },
    parent: yield* observations.capture(directory),
  };
  const a = yield* makeGraph({
    local: localA,
    common: commonA,
    binding,
    identity: {
      storeId: "store-a",
      database: fileIdentity(aDb),
      nonce: "a-incarnation-1",
      coverage: "synthetic-hooks/v1",
    },
  });
  const b = yield* makeGraph({
    local: localB,
    common: commonB,
    binding,
    identity: {
      storeId: "store-b",
      database: fileIdentity(bDb),
      nonce: "b-incarnation-1",
      coverage: "synthetic-hooks/v1",
    },
  });
  const root = yield* observations.capture(rootPath);
  const scope = yield* observations.capture(aPath);
  const unrelated = yield* observations.capture(bPath);
  const owner: Protocol.OwnerReference = {
    storeId: "store-a",
    threadId: "thread",
    threadBirth: "birth-1",
    projectId: "scratch-project",
    ownerEpoch: "owner-1",
    targetRevision: 1,
  };
  const rootId = yield* a.journal.enrollRoot(root);
  const slot: Protocol.Slot = {
    slotId: "slot-a",
    rootId,
    root,
    scope,
    owner,
    revision: 0,
    phase: "owned",
    operation: null,
    removalReceipt: null,
    successor: null,
    bindingCertificate: null,
  };
  yield* a.journal.enrollSlot(slot);
  return {
    directory,
    rootPath,
    aPath,
    bPath,
    aDb,
    bDb,
    commonPath,
    a,
    b,
    slot,
    scope,
    root,
    unrelated,
    binding,
  };
});
export type Fixture = Effect.Success<ReturnType<typeof fixture>>;
export const bindingFor = (
  storeId: string,
  scopes: readonly [Protocol.PhysicalScope, ...Protocol.PhysicalScope[]],
  originalOperationId = "same-command",
): Omit<Protocol.ReservationBinding, "participant"> => ({
  operation: { storeId, originalOperationId, role: "writer", generation: 0 },
  scopes,
  kind: "runtime",
  localReference: "original-receipt",
  owner: null,
  slot: null,
});
export const removalFor = (f: Fixture, originalOperationId = "remove-a") => ({
  ...bindingFor(f.a.identity.storeId, [f.scope], originalOperationId),
  kind: "removal" as const,
  owner: f.slot.owner,
  slot: {
    action: "remove" as const,
    slotId: f.slot.slotId,
    revision: f.slot.revision,
    owner: f.slot.owner,
    beforeImageSha256: Protocol.digest({ owner: f.slot.owner, scope: f.scope }),
  },
});
