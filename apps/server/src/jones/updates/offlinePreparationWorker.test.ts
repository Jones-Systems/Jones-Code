import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Tests own every acquired file descriptor and close it before exact scratch cleanup.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Tests allocate one unique root per resource scope.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- All paths remain within the test's captured temporary root.
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeSqlite from "node:sqlite";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import {
  createContinuationPlan,
  offlinePreparationLegacy,
  preparationLedgerDefinition,
} from "./offlinePreparationCore.ts";
import {
  encodeOfflinePreparationPlan,
  decodeOfflinePreparationPlan,
} from "./offlinePreparationPlan.ts";
import {
  decodePreparationFrame,
  encodePreparationFrame,
  preparationWorkerArguments,
  preparationWorkerSql,
  requirePreparationCorrelation,
  runInheritedPreparationWorker,
} from "./offlinePreparationWorker.ts";

const sessionId = "a".repeat(64),
  scopeId = "b".repeat(64);
const frame = (type: string, fields: Record<string, unknown> = {}) => ({
  schema: 1,
  type,
  sessionId,
  sequence: 1,
  payloadBytes: 0,
  ...fields,
});
const creation = {
  id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  timestamp: "2020-02-03T04:05:06.007Z",
  snapshotSha256: "c".repeat(64),
  settingsSha256: "d".repeat(64),
  preparationIds: [],
};

it("round-trips canonical correlated frames and exact binary plan bytes", () => {
  const plan = encodeOfflinePreparationPlan(
    createContinuationPlan({ rows: [], selections: [] }, creation),
  );
  const header = frame("PLAN_READY", {
    planSha256: decodeOfflinePreparationPlan(plan).sha256,
    payloadBytes: plan.length,
  });
  const decoded = decodePreparationFrame(encodePreparationFrame(header, plan));
  assert.deepEqual(decoded.header, header);
  assert.deepEqual(decoded.payload, plan);
});

it.each([
  frame("CONTINUE", { scopeId, extra: true }),
  frame("CONTINUE", { scopeId: "x" }),
  frame("CONTINUE", { scopeId, sequence: -1 }),
  frame("CONTINUE", { scopeId, sessionId: "x" }),
  frame("SQL_SCOPE_OPENED", { scopeId, purpose: "reconcile", access: "read-write" }),
  frame("CLOSED_RESULT", { scopeId, planSha256: null, outcome: "applied", errorCode: null }),
  frame("CLOSED_RESULT", {
    scopeId,
    planSha256: null,
    outcome: "query-failed",
    errorCode: "private error text",
  }),
  frame("PLAN", {
    pairSha256: sessionId,
    snapshotSha256: sessionId,
    settingsSha256: sessionId,
    planId: creation.id,
    timestamp: "2020-02-30T04:05:06.007Z",
  }),
])("rejects malformed closed frame %#", (header) => {
  assert.throws(() => encodePreparationFrame(header));
});

it.each(["truncated", "trailing", "oversized-header", "noncanonical", "duplicate-key"] as const)(
  "rejects %s framing before dispatch",
  (kind) => {
    const valid = Buffer.from(encodePreparationFrame(frame("CONTINUE", { scopeId })));
    let malformed: Buffer;
    if (kind === "truncated") malformed = valid.subarray(0, valid.length - 1);
    else if (kind === "trailing") malformed = Buffer.concat([valid, Buffer.from([0])]);
    else if (kind === "oversized-header") {
      malformed = Buffer.alloc(4);
      malformed.writeUInt32BE(16385);
    } else {
      const body =
        kind === "noncanonical"
          ? JSON.stringify(frame("CONTINUE", { scopeId }), null, 1)
          : valid.subarray(4).toString().replace('"schema":1', '"schema":1,"schema":1');
      const prefix = Buffer.alloc(4);
      prefix.writeUInt32BE(Buffer.byteLength(body));
      malformed = Buffer.concat([prefix, Buffer.from(body)]);
    }
    assert.throws(() => decodePreparationFrame(malformed));
  },
);

it.each([
  { sessionId: "c".repeat(64) },
  { sequence: 2 },
  { scopeId: "c".repeat(64) },
  { type: "EXIT" },
])("refuses wrong native response correlation %#", (change) => {
  const header = { ...frame("CONTINUE", { scopeId }), ...change };
  assert.throws(() =>
    requirePreparationCorrelation(
      { header, payload: new Uint8Array() },
      sessionId,
      1,
      "CONTINUE",
      scopeId,
    ),
  );
});
it("accepts only the exact outstanding scope response", () => {
  requirePreparationCorrelation(
    decodePreparationFrame(encodePreparationFrame(frame("CONTINUE", { scopeId }))),
    sessionId,
    1,
    "CONTINUE",
    scopeId,
  );
});
it.each(
  [
    [],
    ["--database", "/tmp/state.sqlite"],
    ["--admit"],
    ["--jones-preparation-worker-v1", "--lock-fd", "2", "--owner-directory-fd", "4"],
    ["--jones-preparation-worker-v1", "--lock-fd", "03", "--owner-directory-fd", "4"],
    ["--jones-preparation-worker-v1", "--lock-fd", "4", "--owner-directory-fd", "4"],
    [
      "--jones-preparation-worker-v1",
      "--lock-fd",
      "3",
      "--owner-directory-fd",
      "4",
      "--database",
      "/tmp/state.sqlite",
    ],
  ].map((args) => ({ args })),
)("refuses non-closed descriptor syntax %#", ({ args }) => {
  assert.isNull(preparationWorkerArguments(args));
});
it("parses descriptor roles without claiming custody", () => {
  assert.deepEqual(
    preparationWorkerArguments([
      "--jones-preparation-worker-v1",
      "--lock-fd",
      "13",
      "--owner-directory-fd",
      "14",
    ]),
    { lock: 13, directory: 14 },
  );
});

const scratch = Effect.acquireRelease(
  Effect.promise(() =>
    NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-preparation-worker-")),
  ),
  (root) => Effect.promise(() => NodeFSP.rm(root, { recursive: true })),
);
function initialize(path: string, mode: "wal" | "delete" = "wal") {
  const database = new NodeSqlite.DatabaseSync(path);
  try {
    database.exec("PRAGMA journal_mode=" + mode);
    for (const ddl of Object.values(offlinePreparationLegacy.tables)) database.exec(ddl);
    for (const [table, ledger] of [
      ["effect_sql_migrations", offlinePreparationLegacy.ledgers.upstream],
      ["jones_sql_migrations", offlinePreparationLegacy.ledgers.jones],
    ] as const) {
      database.exec(preparationLedgerDefinition(table));
      for (const [id, name] of ledger)
        database
          .prepare("INSERT INTO " + table + " (migration_id,name) VALUES (?,?)")
          .run(id, name);
    }
  } finally {
    database.close();
  }
}
function sqlLayer(path: string, readonly = false) {
  const uri = NodeURL.pathToFileURL(path);
  uri.searchParams.set("mode", readonly ? "ro" : "rw");
  return Layer.mergeAll(ProjectionStore.layer, EffectOutbox.layer).pipe(
    Layer.provideMerge(NodeSqliteClient.layer({ filename: uri.href, readonly })),
  );
}

it.scoped(
  "rejects non-SEA dispatch and independently closes both owned inherited descriptors",
  () =>
    Effect.gen(function* () {
      const root = yield* scratch,
        original = process.argv;
      const descriptors: number[] = [];
      try {
        descriptors.push(NodeFS.openSync(root, NodeFS.constants.O_RDONLY));
        descriptors.push(
          NodeFS.openSync(
            NodePath.join(root, "lock"),
            NodeFS.constants.O_CREAT | NodeFS.constants.O_EXCL | NodeFS.constants.O_RDWR,
            0o600,
          ),
        );
        process.argv = [
          original[0]!,
          original[1]!,
          "--jones-preparation-worker-v1",
          "--lock-fd",
          String(descriptors[1]),
          "--owner-directory-fd",
          String(descriptors[0]),
        ];
        const outcome = yield* Effect.tryPromise(() => runInheritedPreparationWorker()).pipe(
          Effect.exit,
        );
        assert.isTrue(Exit.isFailure(outcome));
        for (const fd of descriptors) {
          let failure: unknown;
          try {
            NodeFS.fstatSync(fd);
          } catch (error) {
            failure = error;
          }
          assert.equal((failure as NodeJS.ErrnoException | undefined)?.code, "EBADF");
        }
        assert.deepEqual(yield* Effect.promise(() => NodeFSP.readdir(root)), ["lock"]);
      } finally {
        process.argv = original;
        const failures: unknown[] = [];
        for (const fd of descriptors) {
          try {
            NodeFS.closeSync(fd);
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "EBADF") failures.push(error);
          }
        }
        if (failures.length > 0)
          throw new AggregateError(failures, "Owned descriptor cleanup failed");
      }
    }),
);

it.scoped("uses actual WAL SQL to plan, apply and read-only reconcile the exact durable plan", () =>
  Effect.gen(function* () {
    const root = yield* scratch,
      path = NodePath.join(root, "state.sqlite");
    initialize(path);
    const planned = yield* preparationWorkerSql({ kind: "plan", settings: undefined }).pipe(
      Effect.provide(sqlLayer(path)),
    );
    assert.equal(planned.kind, "plan");
    if (planned.kind !== "plan") throw new Error("Expected basis");
    const bytes = encodeOfflinePreparationPlan(createContinuationPlan(planned.basis, creation));
    const planPath = NodePath.join(root, "plan.json");
    yield* Effect.promise(() => NodeFSP.writeFile(planPath, bytes, { flag: "wx", mode: 0o400 }));
    const plan = decodeOfflinePreparationPlan(
      yield* Effect.promise(() => NodeFSP.readFile(planPath)),
    ).plan;
    assert.deepEqual(
      yield* preparationWorkerSql({ kind: "apply", settings: undefined, plan }).pipe(
        Effect.provide(sqlLayer(path)),
      ),
      { kind: "result", outcome: "applied" },
    );
    assert.deepEqual(
      yield* preparationWorkerSql({ kind: "reconcile", settings: undefined, plan }).pipe(
        Effect.provide(sqlLayer(path, true)),
      ),
      { kind: "result", outcome: "applied" },
    );
  }),
);

it.scoped("does not create an absent SQLite family with mode=rw", () =>
  Effect.gen(function* () {
    const root = yield* scratch,
      path = NodePath.join(root, "absent.sqlite");
    const result = yield* preparationWorkerSql({ kind: "plan", settings: undefined }).pipe(
      Effect.provide(sqlLayer(path)),
      Effect.exit,
    );
    assert.isTrue(Exit.isFailure(result));
    assert.deepEqual(yield* Effect.promise(() => NodeFSP.readdir(root)), []);
  }),
);

it.scoped("settles a query rejection only after the SQL layer is closed", () =>
  Effect.gen(function* () {
    const root = yield* scratch,
      path = NodePath.join(root, "state.sqlite");
    initialize(path, "delete");
    const originalClose = NodeSqlite.DatabaseSync.prototype.close;
    let closed = 0;
    const close = vi.spyOn(NodeSqlite.DatabaseSync.prototype, "close").mockImplementation(function (
      this: NodeSqlite.DatabaseSync,
    ) {
      originalClose.call(this);
      closed += 1;
    });
    try {
      const result = yield* preparationWorkerSql({ kind: "plan", settings: undefined }).pipe(
        Effect.exit,
        Effect.provide(sqlLayer(path)),
      );
      assert.isTrue(Exit.isFailure(result));
      assert.equal(closed, 1);
    } finally {
      close.mockRestore();
    }
    const reopened = new NodeSqlite.DatabaseSync(path);
    try {
      reopened.exec("BEGIN EXCLUSIVE");
      reopened.exec("ROLLBACK");
    } finally {
      reopened.close();
    }
  }),
);

it.scoped("refuses drifted historic ledgers without running migrations", () =>
  Effect.gen(function* () {
    const root = yield* scratch,
      path = NodePath.join(root, "state.sqlite");
    initialize(path);
    const sql = NodeSqliteClient.layer({ filename: path });
    yield* Effect.gen(function* () {
      const client = yield* SqlClient.SqlClient;
      yield* client.unsafe("UPDATE effect_sql_migrations SET name='changed' WHERE migration_id=1");
    }).pipe(Effect.provide(sql));
    const result = yield* preparationWorkerSql({ kind: "plan", settings: undefined }).pipe(
      Effect.exit,
      Effect.provide(sqlLayer(path)),
    );
    assert.isTrue(Exit.isFailure(result));
    const reopened = new NodeSqlite.DatabaseSync(path, { readOnly: true });
    try {
      assert.equal(
        reopened.prepare("SELECT name FROM effect_sql_migrations WHERE migration_id=1").get()!.name,
        "changed",
      );
    } finally {
      reopened.close();
    }
  }),
);

it.scoped("does not turn a SQL finalizer failure into a settled query result", () =>
  Effect.gen(function* () {
    const root = yield* scratch,
      path = NodePath.join(root, "state.sqlite");
    initialize(path, "delete");
    const originalClose = NodeSqlite.DatabaseSync.prototype.close;
    let actuallyClosed = false;
    const close = vi.spyOn(NodeSqlite.DatabaseSync.prototype, "close").mockImplementation(function (
      this: NodeSqlite.DatabaseSync,
    ) {
      originalClose.call(this);
      actuallyClosed = true;
      throw new Error("Synthetic post-close failure");
    });
    try {
      const result = yield* preparationWorkerSql({ kind: "plan", settings: undefined }).pipe(
        Effect.exit,
        Effect.provide(sqlLayer(path)),
        Effect.exit,
      );
      assert.isTrue(actuallyClosed);
      assert.isTrue(Exit.isFailure(result));
    } finally {
      close.mockRestore();
    }
  }),
);
