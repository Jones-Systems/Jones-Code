// @effect-diagnostics-next-line nodeBuiltinImport:off -- This standalone adapter owns only its internally allocated synthetic scratch.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Synchronous native file and projection digests preserve the exact persisted SHA-256 binding bytes.
import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Paths are confined to the captured synthetic scratch root.
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import {
  RunId,
  ThreadId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import { serverUpdateContinuationEffect } from "../../orchestration-v2/ServerUpdateContinuationPolicy.ts";
import {
  OfflinePreparationError,
  applyContinuationPlan,
  classifyPreparation,
  createContinuationPlan,
  offlinePreparationLegacy,
  preparationLedgerDefinition,
  preparationOutboxRow,
  readPreparationBasis,
  readPreparationRows,
  verifyPreparationSchema,
  type PreparationOutboxRow as OutboxRow,
} from "./offlinePreparationCore.ts";
import {
  decodeOfflinePreparationPlan,
  encodeOfflinePreparationPlan,
  type OfflinePreparationPlan as ContinuationPlan,
} from "./offlinePreparationPlan.ts";

export { OfflinePreparationError } from "./offlinePreparationCore.ts";
const {
  source: legacySource,
  provenance: legacyProvenance,
  ledgers: legacyLedgers,
  tables: legacyTables,
} = offlinePreparationLegacy;

type FailurePoint =
  | "plan-file-sync"
  | "plan-directory-sync"
  | "before-commit"
  | "after-commit"
  | "evidence-file-sync"
  | "evidence-directory-sync"
  | "cancel-before-write"
  | "cancel-after-commit"
  | "initializer-close-after-close"
  | "mutation-close-after-close"
  | "read-scope-after-close"
  | "write-scope-after-close"
  | "cleanup";
type FixtureMutation =
  | "missing-database"
  | "incomplete-wal"
  | "unsupported-ledger"
  | "unsupported-schema"
  | "marker-collision"
  | "partial-rows"
  | "advanced-row"
  | "projection-drift";
interface SyntheticThread {
  readonly id: string;
  readonly status?: OrchestrationV2Run["status"];
  readonly existingMarker?: boolean;
  readonly malformedMessage?: boolean;
}
interface SyntheticSeed {
  readonly threads: ReadonlyArray<SyntheticThread>;
  readonly capabilities: OrchestrationV2ProviderCapabilities;
  readonly settings?: string;
  readonly schema?: "unsupported-ledger" | "unsupported-schema";
  readonly failure?: "initializer-close-after-close";
}
interface PreparationEvidence {
  readonly status: "applied" | "not-applied" | "held";
  readonly planId: string;
  readonly productionInvocation: false;
}
interface SyntheticFixture {
  plan(): Promise<ContinuationPlan>;
  apply(plan: ContinuationPlan): Promise<PreparationEvidence>;
  reconcile(plan: ContinuationPlan): Promise<PreparationEvidence>;
  cancel(): Promise<"not-applied" | "reconciliation-required">;
  restoreSnapshot(): Promise<void>;
  injectFailure(point: FailurePoint): void;
  mutate(point: FixtureMutation, plan?: ContinuationPlan): Promise<void>;
  inspect(): Promise<{
    scratchRoot: string;
    rows: ReadonlyArray<OutboxRow>;
    audit: ReadonlyArray<string>;
    planOnDisk: string | null;
  }>;
}

export { offlinePreparationStatus } from "./offlinePreparationEntry.ts";

const digest = (value: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
const settingsDigest = (raw: string | undefined) =>
  digest(JSON.stringify(raw === undefined ? { present: false } : { present: true, raw }));
async function syncFile(path: string, content: string | Uint8Array) {
  const handle = await NodeFSP.open(path, "wx", 0o600);
  try {
    await handle.writeFile(content);
    await handle.sync();
  } finally {
    await handle.close();
  }
}
async function syncDirectory(path: string) {
  const handle = await NodeFSP.open(path, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

// Only this fixture initializer creates a database. The maintenance operations
// below require an existing, bound file and never execute these DDL statements.
function initializeFixture(database: NodeSqlite.DatabaseSync, seed: SyntheticSeed) {
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
  for (const ddl of Object.values(legacyTables)) database.exec(ddl);
  for (const [table, ledger] of [
    ["effect_sql_migrations", legacyLedgers.upstream],
    ["jones_sql_migrations", legacyLedgers.jones],
  ] as const) {
    database.exec(preparationLedgerDefinition(table));
    for (const [id, name] of ledger)
      database
        .prepare("INSERT INTO " + table + " (migration_id, name) VALUES (?, ?)")
        .run(id, name);
  }
  for (const item of seed.threads) {
    const id = item.id;
    const run = "run:" + id,
      session = "session:" + id,
      providerThread = "provider-thread:" + id;
    const attempt = "attempt:" + id,
      turn = "turn:" + id,
      node = "node:" + id;
    const status = item.status ?? "running";
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
        capabilities: seed.capabilities,
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
    if (item.malformedMessage)
      insert("orchestration_v2_projection_messages", {
        message_id: "bad:" + id,
        thread_id: id,
        run_id: run,
        node_id: null,
        role: "assistant",
        streaming: 1,
        created_at: now,
        updated_at: now,
        payload_json: "{}",
      });
    if (item.existingMarker) {
      const effect = serverUpdateContinuationEffect({
        preparationId: "existing:" + id,
        threadId: ThreadId.make(id),
        sourceRunId: RunId.make(run),
        optedInOnly: true,
      });
      insert("orchestration_v2_effect_outbox", preparationOutboxRow(effect, now));
    }
  }
  if (seed.schema === "unsupported-ledger")
    database.exec("UPDATE jones_sql_migrations SET name = 'unrecognized' WHERE migration_id = 105");
  if (seed.schema === "unsupported-schema")
    database.exec("ALTER TABLE orchestration_v2_effect_outbox ADD COLUMN unexpected TEXT");
  database.exec("COMMIT");
}

/**
 * Qualification access is confined to a freshly allocated synthetic database.
 * There is deliberately no constructor accepting an installed path or custody
 * assertion. The production entry cannot turn this scope into an admission grant.
 */
export async function withSyntheticPreparationFixture<A>(
  seed: SyntheticSeed,
  use: (fixture: SyntheticFixture) => Promise<A>,
): Promise<A> {
  if (
    seed.threads.length > 32 ||
    (seed.settings?.length ?? 0) > 262_144 ||
    seed.threads.some((thread) => !/^[a-zA-Z0-9:_-]{1,96}$/.test(thread.id))
  )
    throw new OfflinePreparationError({ reason: "invalid-synthetic-seed" });
  const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-offline-preparation-"));
  const databasePath = NodePath.join(root, "state.sqlite");
  const snapshotPath = NodePath.join(root, "previous.sqlite");
  const settingsPath = NodePath.join(root, "settings.json");
  const savedSettingsPath = NodePath.join(root, "previous-settings.json");
  const planPath = NodePath.join(root, "continuation-plan.json");
  const evidencePath = NodePath.join(root, "preparation-evidence.json");
  const audit: string[] = [];
  const plans = new WeakSet<ContinuationPlan>();
  let currentPlan: ContinuationPlan | undefined;
  let accepting = true;
  let active = true;
  let inFlight: Promise<unknown> | undefined;
  let unresolvedEffect = false;
  let writeStarted = false;
  let cancelled = false;
  let failurePoint: FailurePoint | undefined = seed.failure;
  // Row reconciliation can resolve effects, never a lost handle-finalization proof.
  const custodyFailures: Array<{ source: string; failure: OfflinePreparationError }> = [];
  let initialHash = "";
  let originalIdentity: { dev: number; ino: number } | undefined;

  const assertActive = () => {
    if (!active) throw new OfflinePreparationError({ reason: "expired-fixture" });
  };
  const assertAdmission = () => {
    if (!accepting) throw new OfflinePreparationError({ reason: "expired-fixture" });
  };
  const assertCustody = () => {
    if (custodyFailures.length !== 0)
      throw new OfflinePreparationError({
        reason: "sqlite-custody-unresolved",
        cause: custodyFailures[0]?.failure,
      });
  };
  const loseCustody = (source: string, cause: unknown) => {
    const failure = new OfflinePreparationError({ reason: "sqlite-scope-unresolved", cause });
    custodyFailures.push({ source, failure });
    audit.push(source + ":close-unresolved");
    return failure;
  };
  const checkPlan = (plan: ContinuationPlan) => {
    assertActive();
    if (!plans.has(plan) || plan !== currentPlan)
      throw new OfflinePreparationError({ reason: "unowned-plan" });
  };
  const failAt = (point: FailurePoint) => {
    if (failurePoint === point) {
      failurePoint = undefined;
      if (point === "cancel-before-write" || point === "cancel-after-commit") cancelled = true;
      throw new OfflinePreparationError({ reason: point });
    }
  };
  const operation = async <T>(run: () => Promise<T>): Promise<T> => {
    assertAdmission();
    if (inFlight !== undefined)
      return Promise.reject(new OfflinePreparationError({ reason: "fixture-busy" }));
    const pending = run();
    inFlight = pending;
    void pending
      .finally(() => {
        if (inFlight === pending) inFlight = undefined;
      })
      .catch(() => {});
    return pending;
  };
  const withCapturedDatabase = <T>(
    source: "initializer" | "mutation",
    run: (database: NodeSqlite.DatabaseSync) => T,
  ): T => {
    assertCustody();
    const database = new NodeSqlite.DatabaseSync(databasePath);
    let operationFailure: unknown;
    try {
      return run(database);
    } catch (cause) {
      operationFailure = cause;
      throw cause;
    } finally {
      try {
        database.close();
        audit.push(source + ":native-close-completed");
        // The closed fault set loses confirmation only after the real captured
        // handle closes, allowing tests to consume evidence and remove their root.
        failAt(
          source === "initializer" ? "initializer-close-after-close" : "mutation-close-after-close",
        );
      } catch (cause) {
        throw loseCustody(
          source,
          operationFailure === undefined
            ? cause
            : new AggregateError(
                [operationFailure, cause],
                "Synthetic operation and SQLite close failed.",
              ),
        );
      }
    }
  };
  const existingFile = async (path: string) => {
    const stat = await NodeFSP.lstat(path);
    if (!stat.isFile() || stat.nlink !== 1)
      throw new OfflinePreparationError({ reason: "invalid-database-object" });
    for (const suffix of ["-wal", "-shm", "-journal"]) {
      const sidecar = await NodeFSP.lstat(path + suffix).catch((cause: unknown) => {
        if (
          typeof cause === "object" &&
          cause !== null &&
          "code" in cause &&
          cause.code === "ENOENT"
        )
          return undefined;
        throw cause;
      });
      if (sidecar !== undefined)
        throw new OfflinePreparationError({ reason: "incomplete-snapshot" });
    }
    return stat;
  };
  const withDatabase = async <T, E>(
    path: string,
    readonly: boolean,
    effect: Effect.Effect<T, E, SqlClient.SqlClient | ProjectionStore.ProjectionStoreV2>,
  ): Promise<T> => {
    assertCustody();
    const stat = await existingFile(path);
    if (
      path === databasePath &&
      originalIdentity !== undefined &&
      (stat.dev !== originalIdentity.dev || stat.ino !== originalIdentity.ino)
    )
      throw new OfflinePreparationError({ reason: "database-identity-changed" });
    audit.push(readonly ? "database:read-only-open" : "database:write-open");
    const layer = ProjectionStore.layer.pipe(
      Layer.provideMerge(NodeSqliteClient.layer({ filename: path, readonly })),
    );
    const program = readonly
      ? Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          return yield* sql.withTransaction(effect);
        })
      : effect;
    // Capture operation failure as a value inside the layer so a failed scope
    // finalizer remains distinguishable from a query rejection. Never remove a
    // root whose SQLite close did not complete successfully.
    const source = readonly ? "read-scope" : "write-scope";
    let operationFailure: unknown;
    const completed = await Effect.runPromise(
      program.pipe(
        Effect.exit,
        Effect.map((exit) => {
          if (exit._tag === "Failure") operationFailure = Cause.squash(exit.cause);
          return exit;
        }),
        Effect.provide(layer),
      ),
    )
      .then((exit) => {
        audit.push("database:closed", source + ":native-close-completed");
        failAt(readonly ? "read-scope-after-close" : "write-scope-after-close");
        return exit;
      })
      .catch((cause: unknown) => {
        throw loseCustody(
          source,
          operationFailure === undefined
            ? cause
            : new AggregateError(
                [operationFailure, cause],
                "Synthetic operation and SQLite scope close failed.",
              ),
        );
      });
    if (completed._tag === "Failure") throw Cause.squash(completed.cause);
    return completed.value;
  };
  const publish = async (kind: "plan" | "evidence", path: string, content: string | Uint8Array) => {
    const handle = await NodeFSP.open(path, "wx", 0o400);
    try {
      await handle.writeFile(content);
      failAt(kind === "plan" ? "plan-file-sync" : "evidence-file-sync");
      await handle.sync();
      audit.push(kind + ":file-synced");
    } finally {
      await handle.close();
    }
    failAt(kind === "plan" ? "plan-directory-sync" : "evidence-directory-sync");
    await syncDirectory(root);
    audit.push(kind + ":directory-synced");
    const expected = typeof content === "string" ? new TextEncoder().encode(content) : content;
    if (!(await NodeFSP.readFile(path)).equals(expected))
      throw new OfflinePreparationError({ reason: "durable-evidence-changed" });
  };
  const readSettings = async () => {
    return NodeFSP.readFile(settingsPath, "utf8").catch((cause: unknown) => {
      if (typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ENOENT")
        return undefined;
      throw cause;
    });
  };
  const recordReconciliation = async (result: PreparationEvidence) => {
    const content = JSON.stringify(result);
    const existing = await NodeFSP.readFile(evidencePath, "utf8").catch((cause: unknown) => {
      if (typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ENOENT")
        return undefined;
      throw cause;
    });
    if (existing === undefined) await publish("evidence", evidencePath, content);
    else {
      if (existing !== content) throw new OfflinePreparationError({ reason: "evidence-mismatch" });
      // A prior write may have reached disk without completing either sync.
      // Exact durable rows permit finishing those syncs, never replaying SQL.
      const handle = await NodeFSP.open(evidencePath, "r");
      try {
        await handle.sync();
      } finally {
        await handle.close();
      }
      await syncDirectory(root);
      audit.push("evidence:reconciled-and-synced");
    }
  };
  const validatePlanFiles = async (plan: ContinuationPlan) => {
    const raw = await NodeFSP.readFile(planPath);
    const expected = encodeOfflinePreparationPlan(plan);
    if (!raw.equals(expected)) throw new OfflinePreparationError({ reason: "plan-file-changed" });
    decodeOfflinePreparationPlan(raw);
    if (
      digest(await NodeFSP.readFile(snapshotPath)) !== plan.snapshotSha256 ||
      settingsDigest(await readSettings()) !== plan.settingsSha256
    )
      throw new OfflinePreparationError({ reason: "baseline-changed" });
  };
  const evidence = (
    plan: ContinuationPlan,
    status: PreparationEvidence["status"],
  ): PreparationEvidence => ({ status, planId: plan.id, productionInvocation: false });
  const reconcile = async (plan: ContinuationPlan): Promise<PreparationEvidence> => {
    checkPlan(plan);
    await validatePlanFiles(plan);
    const raw = await readSettings();
    const basis = await withDatabase(databasePath, true, readPreparationBasis(raw));
    const status = classifyPreparation(plan, basis);
    if (status === "applied") return evidence(plan, status);
    // This fixture excludes WAL and still requires the exact raw main-file baseline.
    // Logical comparison alone does not replace the future production owner's family proof.
    if (status === "not-applied" && digest(await NodeFSP.readFile(databasePath)) === initialHash) {
      unresolvedEffect = false;
      return evidence(plan, status);
    }
    unresolvedEffect = true;
    return evidence(plan, "held");
  };

  let result: A | undefined;
  let failure: unknown;
  try {
    withCapturedDatabase("initializer", (database) => initializeFixture(database, seed));
    const stat = await existingFile(databasePath);
    originalIdentity = { dev: stat.dev, ino: stat.ino };
    const original = await NodeFSP.readFile(databasePath);
    initialHash = digest(original);
    await syncFile(snapshotPath, original);
    if (seed.settings !== undefined) {
      await syncFile(settingsPath, seed.settings);
      await syncFile(savedSettingsPath, seed.settings);
    }
    await syncDirectory(root);
    audit.push("snapshot-pair:synced");
    const fixture: SyntheticFixture = {
      plan: () =>
        operation(async () => {
          if (currentPlan !== undefined || writeStarted)
            throw new OfflinePreparationError({ reason: "plan-already-created" });
          const raw = await readSettings();
          const basis = await withDatabase(snapshotPath, true, readPreparationBasis(raw));
          const plan = createContinuationPlan(basis, {
            id: NodeCrypto.randomUUID(),
            timestamp: DateTime.formatIso(await Effect.runPromise(DateTime.now)),
            snapshotSha256: initialHash,
            settingsSha256: settingsDigest(raw),
            preparationIds: basis.selections
              .filter((selection) => selection.existingId === null)
              .map(() => NodeCrypto.randomUUID()),
          });
          // A failed fsync never publishes the in-memory authority token.
          await publish("plan", planPath, encodeOfflinePreparationPlan(plan));
          plans.add(plan);
          currentPlan = plan;
          return plan;
        }),
      apply: (plan) =>
        operation(async () => {
          assertCustody();
          checkPlan(plan);
          if (cancelled) throw new OfflinePreparationError({ reason: "preparation-cancelled" });
          if (writeStarted)
            throw new OfflinePreparationError({ reason: "reconciliation-required" });
          await validatePlanFiles(plan);
          const raw = await readSettings();
          if (digest(await NodeFSP.readFile(databasePath)) !== plan.snapshotSha256)
            throw new OfflinePreparationError({ reason: "baseline-changed" });
          await withDatabase(databasePath, true, verifyPreparationSchema);
          failAt("cancel-before-write");
          writeStarted = true;
          unresolvedEffect = true;
          await withDatabase(
            databasePath,
            false,
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              // Only the synthetic adapter adds this checkpoint, inside the core's real transaction.
              const checkpoint = <T, E, R>(effect: Effect.Effect<T, E, R>) =>
                sql.withTransaction(
                  Effect.tap(effect, () => Effect.sync(() => failAt("before-commit"))),
                );
              const fixtureSql = new Proxy(sql, {
                get(target, property, receiver) {
                  return property === "withTransaction"
                    ? checkpoint
                    : Reflect.get(target, property, receiver);
                },
              });
              yield* applyContinuationPlan(plan, raw).pipe(
                Effect.provideService(SqlClient.SqlClient, fixtureSql),
              );
            }),
          );
          audit.push("transaction:committed");
          failAt("after-commit");
          failAt("cancel-after-commit");
          const applied = await reconcile(plan);
          if (applied.status !== "applied") return applied;
          unresolvedEffect = true;
          await publish("evidence", evidencePath, JSON.stringify(applied));
          unresolvedEffect = false;
          return applied;
        }),
      reconcile: (plan) =>
        operation(async () => {
          const result = await reconcile(plan);
          if (result.status === "applied") {
            unresolvedEffect = true;
            await recordReconciliation(result);
            unresolvedEffect = false;
          }
          return result;
        }),
      cancel: () =>
        operation(async () => {
          cancelled = true;
          return writeStarted ? "reconciliation-required" : "not-applied";
        }),
      restoreSnapshot: () =>
        operation(async () => {
          assertCustody();
          if (digest(await NodeFSP.readFile(snapshotPath)) !== initialHash)
            throw new OfflinePreparationError({ reason: "snapshot-changed" });
          // Only the owned fixture performs restoration. Production rollback
          // remains the future maintenance owner's paired-state operation.
          await existingFile(databasePath);
          const handle = await NodeFSP.open(databasePath, "r+");
          try {
            await handle.truncate(0);
            await handle.writeFile(await NodeFSP.readFile(snapshotPath));
            await handle.sync();
          } finally {
            await handle.close();
          }
          if (seed.settings !== undefined) {
            const settings = await NodeFSP.readFile(savedSettingsPath);
            await NodeFSP.writeFile(settingsPath, settings);
            const handle = await NodeFSP.open(settingsPath, "r");
            try {
              await handle.sync();
            } finally {
              await handle.close();
            }
          }
          await syncDirectory(root);
          if (digest(await NodeFSP.readFile(databasePath)) !== initialHash)
            throw new OfflinePreparationError({ reason: "restore-mismatch" });
          unresolvedEffect = false;
          audit.push("snapshot-pair:restored");
        }),
      injectFailure: (point) => {
        assertAdmission();
        failurePoint = point;
      },
      mutate: (point, plan) =>
        operation(async () => {
          assertCustody();
          if (point === "missing-database") {
            await NodeFSP.unlink(databasePath);
            return;
          }
          if (point === "incomplete-wal") {
            await syncFile(databasePath + "-wal", "incomplete");
            return;
          }
          withCapturedDatabase("mutation", (database) => {
            if (point === "unsupported-ledger")
              database.exec(
                "UPDATE jones_sql_migrations SET name = 'unrecognized' WHERE migration_id = 105",
              );
            else if (point === "unsupported-schema")
              database.exec(
                "ALTER TABLE orchestration_v2_effect_outbox ADD COLUMN unexpected TEXT",
              );
            else {
              if (plan === undefined)
                throw new OfflinePreparationError({ reason: "missing-fixture-plan" });
              checkPlan(plan);
              const row = plan.expectedRows[0];
              if (row === undefined)
                throw new OfflinePreparationError({ reason: "empty-fixture-plan" });
              if (point === "marker-collision")
                database
                  .prepare(
                    "INSERT INTO orchestration_v2_effect_outbox (effect_id,command_id,thread_id,effect_type,payload_json,status,available_at,created_at,updated_at) VALUES (?,?,?,?,?,'pending',?,?,?)",
                  )
                  .run(
                    row.effect_id,
                    "command:collision",
                    row.thread_id,
                    row.effect_type,
                    "{}",
                    row.available_at,
                    row.created_at,
                    row.updated_at,
                  );
              if (point === "partial-rows")
                database
                  .prepare("DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = ?")
                  .run(row.effect_id);
              if (point === "advanced-row")
                database
                  .prepare(
                    "UPDATE orchestration_v2_effect_outbox SET status = 'running', attempt_count = 1 WHERE effect_id = ?",
                  )
                  .run(row.effect_id);
              if (point === "projection-drift")
                database
                  .prepare(
                    "UPDATE orchestration_v2_projection_provider_threads SET payload_json = json_set(payload_json, '$.nativeThreadRef.nativeId', 'changed-native-thread') WHERE thread_id = ?",
                  )
                  .run(row.thread_id);
              unresolvedEffect = true;
            }
          });
        }),
      inspect: () =>
        operation(async () => ({
          scratchRoot: root,
          rows: await withDatabase(databasePath, true, readPreparationRows),
          audit: [...audit],
          planOnDisk: await NodeFSP.readFile(planPath, "utf8").catch((cause: unknown) => {
            if (
              typeof cause === "object" &&
              cause !== null &&
              "code" in cause &&
              cause.code === "ENOENT"
            )
              return null;
            throw cause;
          }),
        })),
    };
    result = await use(fixture);
  } catch (cause) {
    failure = cause;
  } finally {
    accepting = false;
    try {
      await inFlight;
    } catch (cause) {
      failure ??= cause;
    }
    active = false;
    if (unresolvedEffect || custodyFailures.length !== 0) {
      const causes: unknown[] = custodyFailures.map((entry) => entry.failure);
      if (failure !== undefined && !causes.includes(failure)) causes.unshift(failure);
      const retained = new OfflinePreparationError({
        reason:
          custodyFailures.length !== 0
            ? "unresolved-sqlite-custody"
            : "unresolved-synthetic-effect",
        retainedRoot: root,
        cause:
          causes.length > 1
            ? new AggregateError(causes, "Synthetic scope has unresolved custody.")
            : causes[0],
      });
      try {
        await syncFile(
          NodePath.join(root, "retained-evidence.json"),
          JSON.stringify({
            reason: retained.reason,
            planId: currentPlan?.id ?? null,
            source: legacySource,
            provenance: legacyProvenance,
            custody: custodyFailures.map(({ source }) => ({ source, status: "unresolved" })),
            audit,
            productionInvocation: false,
          }),
        );
        await syncDirectory(root);
      } catch (cause) {
        throw new AggregateError(
          [retained, cause],
          "Could not preserve synthetic recovery evidence.",
        );
      }
      throw retained;
    }
    try {
      failAt("cleanup");
      await NodeFSP.rm(root, { recursive: true, force: true });
    } catch (cause) {
      const cleanup = new OfflinePreparationError({
        reason: "synthetic-cleanup-failed",
        retainedRoot: root,
        cause,
      });
      if (failure !== undefined)
        throw new AggregateError([failure, cleanup], "Synthetic operation and cleanup failed.");
      throw cleanup;
    }
  }
  if (failure !== undefined) throw failure;
  return result as A;
}
