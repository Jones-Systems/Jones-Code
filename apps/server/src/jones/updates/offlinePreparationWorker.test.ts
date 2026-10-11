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
import type { OrchestrationV2ProviderCapabilities } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import {
  createContinuationPlan,
  classifyPreparation,
  readPreparationRows,
  offlinePreparationLegacy,
  preparationLedgerDefinition,
} from "./offlinePreparationCore.ts";
import {
  encodeOfflinePreparationPlan,
  decodeOfflinePreparationPlan,
  type OfflinePreparationPlan,
} from "./offlinePreparationPlan.ts";
import {
  decodePreparationFrame,
  encodePreparationFrame,
  preparationWorkerArguments,
  preparationWorkerSql,
  requirePreparationCorrelation,
  runInheritedPreparationWorker,
  readBoundedPreparationMetadata,
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
// Seed the same historical projection shapes as the owned adapter, without starting any provider.
const historicalCapabilities = {
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: false,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: false,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: false,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: false,
    emitsTurnCompleted: false,
    supportsInterrupt: false,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: false,
    terminalStatusQuality: "none",
  },
  streaming: {
    streamsAssistantText: false,
    streamsReasoning: false,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: false,
  },
  tools: {
    exposesToolItemIds: false,
    emitsToolStarted: false,
    emitsToolCompleted: false,
    emitsToolOutput: false,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: false,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: false,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: false,
    supportsDeltaHandoff: false,
    supportsFullThreadHandoff: false,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: false,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "none",
    nativeTurnIds: "none",
    nativeItemIds: "none",
    nativeRequestIds: "none",
  },
  runtimePolicy: {
    enforcement: "client-boundary",
  },
} as const satisfies OrchestrationV2ProviderCapabilities;

const preservedRow = {
  effect_id: "effect:preserved",
  command_id: "command:ordinary",
  thread_id: "thread:other",
  effect_type: "provider-turn.start",
  payload_json: "opaque-unrelated-payload",
  status: "succeeded" as const,
  attempt_count: 1,
  available_at: "2019-01-01T00:00:00.000Z",
  created_at: "2019-01-01T00:00:00.000Z",
  updated_at: "2019-01-02T00:00:00.000Z",
  lease_owner: null,
  lease_expires_at: null,
  completed_at: "2019-01-02T00:00:00.000Z",
  last_error: null,
};
function seedEligibleHistoricalThread(path: string) {
  const database = new NodeSqlite.DatabaseSync(path);
  try {
    const insert = (table: string, values: Record<string, string | number | null>) => {
      const columns = Object.keys(values);
      database
        .prepare(
          "INSERT INTO " +
            table +
            " (" +
            columns.join(",") +
            ") VALUES (" +
            columns.map(() => "?").join(",") +
            ")",
        )
        .run(...Object.values(values));
    };
    const now = "2026-10-11T12:00:00.000Z";
    database.exec("BEGIN IMMEDIATE");
    const id = "thread:worker";
    const run = "run:" + id,
      session = "session:" + id,
      providerThread = "provider-thread:" + id;
    const attempt = "attempt:" + id,
      turn = "turn:" + id,
      node = "node:" + id;
    const status = "running";
    const payload = (value: Record<string, unknown>) => JSON.stringify(value);
    const modelSelection = { instanceId: "codex", model: "synthetic" };
    insert("orchestration_v2_projection_threads", {
      thread_id: id,
      project_id: "project:synthetic",
      title: id,
      default_provider: "codex",
      runtime_mode: "full-access",
      interaction_mode: "default",
      active_provider_thread_id: providerThread,
      created_at: now,
      updated_at: now,
      archived_at: null,
      deleted_at: null,
      payload_json: payload({
        id,
        projectId: "project:synthetic",
        title: id,
        providerInstanceId: "codex",
        modelSelection,
        createdBy: "user",
        creationSource: "web",
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: providerThread,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        deletedAt: null,
      }),
    });
    insert("orchestration_v2_projection_runs", {
      run_id: run,
      thread_id: id,
      ordinal: 1,
      provider: "codex",
      provider_thread_id: providerThread,
      status,
      requested_at: now,
      completed_at: null,
      payload_json: payload({
        id: run,
        threadId: id,
        ordinal: 1,
        providerInstanceId: "codex",
        modelSelection,
        providerThreadId: providerThread,
        userMessageId: "message:" + id,
        rootNodeId: node,
        activeAttemptId: attempt,
        status,
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      }),
    });
    insert("orchestration_v2_projection_run_attempts", {
      attempt_id: attempt,
      thread_id: id,
      run_id: run,
      attempt_ordinal: 1,
      root_node_id: node,
      provider: "codex",
      provider_thread_id: providerThread,
      provider_turn_id: turn,
      status: "running",
      payload_json: payload({
        id: attempt,
        runId: run,
        attemptOrdinal: 1,
        rootNodeId: node,
        providerInstanceId: "codex",
        providerThreadId: providerThread,
        providerTurnId: turn,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      }),
    });
    insert("orchestration_v2_projection_provider_sessions", {
      provider_session_id: session,
      thread_id: id,
      provider: "codex",
      status: "ready",
      model: "synthetic",
      updated_at: now,
      payload_json: payload({
        id: session,
        driver: "codex",
        providerInstanceId: "codex",
        status: "ready",
        cwd: "/synthetic",
        model: "synthetic",
        capabilities: historicalCapabilities,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      }),
    });
    insert("orchestration_v2_projection_provider_session_bindings", {
      provider_session_id: session,
      thread_id: id,
    });
    insert("orchestration_v2_projection_provider_threads", {
      provider_thread_id: providerThread,
      thread_id: id,
      owner_node_id: null,
      provider: "codex",
      provider_session_id: session,
      status: "active",
      first_run_ordinal: 1,
      last_run_ordinal: 1,
      updated_at: now,
      payload_json: payload({
        id: providerThread,
        driver: "codex",
        providerInstanceId: "codex",
        providerSessionId: session,
        appThreadId: id,
        ownerNodeId: null,
        nativeThreadRef: { driver: "codex", nativeId: "synthetic:" + id, strength: "strong" },
        nativeConversationHeadRef: null,
        status: "active",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      }),
    });
    insert("orchestration_v2_projection_provider_turns", {
      provider_turn_id: turn,
      thread_id: id,
      provider_thread_id: providerThread,
      node_id: node,
      run_attempt_id: attempt,
      ordinal: 1,
      status: "running",
      started_at: now,
      completed_at: null,
      payload_json: payload({
        id: turn,
        providerThreadId: providerThread,
        nodeId: node,
        runAttemptId: attempt,
        nativeTurnRef: null,
        ordinal: 1,
        status: "running",
        startedAt: now,
        completedAt: null,
      }),
    });
    insert("orchestration_v2_effect_outbox", preservedRow);
    database.exec("COMMIT");
  } finally {
    database.close();
  }
}

function sqlLayer(path: string, readonly = false) {
  const uri = NodeURL.pathToFileURL(path);
  uri.searchParams.set("mode", readonly ? "ro" : "rw");
  return ProjectionStore.layer.pipe(
    Layer.provideMerge(NodeSqliteClient.layer({ filename: uri.href, readonly })),
  );
}

it.effect(
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
    }).pipe(Effect.scoped),
);

it.effect("uses actual WAL SQL to plan, apply and read-only reconcile the exact durable plan", () =>
  Effect.gen(function* () {
    const root = yield* scratch,
      path = NodePath.join(root, "state.sqlite");
    initialize(path);
    seedEligibleHistoricalThread(path);
    const settings = '{"continueThreadsAfterServerUpdate":true}';
    const planned = yield* preparationWorkerSql({ kind: "plan", settings }).pipe(
      Effect.provide(sqlLayer(path)),
    );
    assert.equal(planned.kind, "plan");
    if (planned.kind !== "plan") return yield* Effect.die("Expected basis");
    assert.equal(planned.basis.selections.length, 1);
    assert.equal(planned.basis.selections[0]!.threadId, "thread:worker");
    assert.equal(planned.basis.selections[0]!.runId, "run:thread:worker");
    assert.isNull(planned.basis.selections[0]!.existingId);
    assert.deepEqual(planned.basis.rows, [preservedRow]);
    const preparationId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const bytes = encodeOfflinePreparationPlan(
      createContinuationPlan(planned.basis, {
        ...creation,
        preparationIds: [preparationId],
      }),
    );
    const planPath = NodePath.join(root, "plan.json");
    yield* Effect.promise(() => NodeFSP.writeFile(planPath, bytes, { flag: "wx", mode: 0o400 }));
    const plan = decodeOfflinePreparationPlan(
      yield* Effect.promise(() => NodeFSP.readFile(planPath)),
    ).plan;
    assert.equal(plan.expectedRows.length, 1);
    assert.deepEqual(plan.preservedRows, [preservedRow]);
    assert.equal(classifyPreparation(plan, planned.basis), "not-applied");
    assert.deepEqual(
      yield* preparationWorkerSql({ kind: "reconcile", settings, plan }).pipe(
        Effect.provide(sqlLayer(path, true)),
      ),
      { kind: "result", outcome: "not-applied" },
    );
    assert.deepEqual(
      yield* preparationWorkerSql({ kind: "apply", settings, plan }).pipe(
        Effect.provide(sqlLayer(path)),
      ),
      { kind: "result", outcome: "applied" },
    );
    const expectedRow: OfflinePreparationPlan["expectedRows"][number] = {
      effect_id: "effect:server-update-continuation:" + preparationId,
      command_id: "command:server-update-prepare:" + preparationId,
      thread_id: "thread:worker",
      effect_type: "provider-runtime.continue",
      payload_json:
        '{"type":"provider-runtime.continue","sourceRunId":"run:thread:worker","preparedForRestart":true,"continueWithoutPreference":false}',
      status: "pending",
      attempt_count: 0,
      available_at: creation.timestamp,
      created_at: creation.timestamp,
      updated_at: creation.timestamp,
      lease_owner: null,
      lease_expires_at: null,
      completed_at: null,
      last_error: null,
    };
    const after = yield* readPreparationRows.pipe(Effect.provide(sqlLayer(path, true)));
    assert.equal(after.length, 2);
    assert.deepEqual(
      after.find((row) => row.effect_id === expectedRow.effect_id),
      expectedRow,
    );
    assert.deepEqual(
      after.find((row) => row.effect_id === preservedRow.effect_id),
      preservedRow,
    );
    assert.deepEqual(
      yield* preparationWorkerSql({ kind: "reconcile", settings, plan }).pipe(
        Effect.provide(sqlLayer(path, true)),
      ),
      { kind: "result", outcome: "applied" },
    );
    assert.deepEqual(
      yield* preparationWorkerSql({ kind: "apply", settings, plan }).pipe(
        Effect.provide(sqlLayer(path)),
      ),
      { kind: "result", outcome: "applied" },
    );
    assert.deepEqual(yield* readPreparationRows.pipe(Effect.provide(sqlLayer(path, true))), after);
    assert.deepEqual(yield* Effect.promise(() => NodeFSP.readFile(planPath)), Buffer.from(bytes));
  }).pipe(Effect.scoped),
);

it.effect("does not create an absent SQLite family with mode=rw", () =>
  Effect.gen(function* () {
    const root = yield* scratch,
      path = NodePath.join(root, "absent.sqlite");
    const result = yield* preparationWorkerSql({ kind: "plan", settings: undefined }).pipe(
      Effect.provide(sqlLayer(path)),
      Effect.exit,
    );
    assert.isTrue(Exit.isFailure(result));
    assert.deepEqual(yield* Effect.promise(() => NodeFSP.readdir(root)), []);
  }).pipe(Effect.scoped),
);

it.effect("settles a query rejection only after the SQL layer is closed", () =>
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
  }).pipe(Effect.scoped),
);

it.effect("refuses drifted historic ledgers without running migrations", () =>
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
  }).pipe(Effect.scoped),
);

it.effect("does not turn a SQL finalizer failure into a settled query result", () =>
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
  }).pipe(Effect.scoped),
);

for (const scenario of ["short-read", "exact-limit", "growth", "read-failure"] as const) {
  it.effect(`bounds metadata ${scenario} and closes the owned descriptor`, () =>
    Effect.gen(function* () {
      const root = yield* scratch;
      yield* Effect.promise(async () => {
        const path = NodePath.join(root, "metadata");
        const initial = Buffer.from(scenario === "exact-limit" ? "12345678" : "12");
        await NodeFSP.writeFile(path, initial, { flag: "wx", mode: 0o600 });
        const handle = await NodeFSP.open(path, "r");
        const fd = handle.fd;
        let delivered = 0;
        let eof = false;
        let failure: unknown;
        let result: Buffer | undefined;
        try {
          assert.equal((await handle.stat()).size, initial.length);
          if (scenario === "growth") await NodeFSP.appendFile(path, "345678901234567890");
          result = await readBoundedPreparationMetadata(
            {
              async read(buffer, offset, length, position) {
                assert.equal(buffer.length, 9);
                assert.equal(position, delivered);
                assert.isAtMost(length, 9 - delivered);
                if (scenario === "read-failure" && delivered !== 0)
                  throw new Error("Synthetic metadata read failure");
                const read = await handle.read(buffer, offset, Math.min(length, 2), position);
                delivered += read.bytesRead;
                if (read.bytesRead === 0) eof = true;
                return read;
              },
            },
            8,
          );
        } catch (cause) {
          failure = cause;
        } finally {
          await handle.close();
        }
        assert.throws(() => NodeFS.fstatSync(fd), /EBADF/);
        if (scenario === "growth" || scenario === "read-failure") {
          assert.isDefined(failure);
          assert.isUndefined(result);
          assert.isFalse(eof);
          assert.equal(delivered, scenario === "growth" ? 9 : 2);
          if (scenario === "growth")
            assert.equal((failure as { readonly _tag?: string })._tag, "WorkerRefused");
          else assert.equal((failure as Error).message, "Synthetic metadata read failure");
        } else {
          assert.isUndefined(failure);
          assert.deepEqual(result, initial);
          assert.isTrue(eof);
          assert.equal(delivered, initial.length);
        }
      });
    }).pipe(Effect.scoped),
  );
}
