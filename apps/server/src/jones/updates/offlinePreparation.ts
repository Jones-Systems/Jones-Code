// @effect-diagnostics-next-line nodeBuiltinImport:off -- This standalone adapter owns only its internally allocated synthetic scratch.
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Paths are confined to the captured synthetic scratch root.
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import {
  CommandId,
  RunId,
  ThreadId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { decodePersistedServerSettings } from "../../ServerSettingsPersistence.ts";
import { isEntrypoint } from "../../entrypoint.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutbox.ts";
import { findPendingServerUpdateContinuation } from "../../orchestration-v2/ServerUpdateContinuationPreparation.ts";
import {
  serverUpdateContinuationEffect,
  serverUpdateContinuationRun,
} from "../../orchestration-v2/ServerUpdateContinuationPolicy.ts";

const legacySource = "bd20d2d2fc0ba89797698192b877a7f1312f8fe3";
// These public tracked blobs fix the source of the ledger names and relevant
// table definitions. This is a projection/outbox compatibility surface, not a
// claim about an installed database or a migration/rebuild recipe.
const legacyProvenance = {
  "apps/server/src/persistence/Migrations.ts": {
    blob: "fe43ee4a8fd25bcd4829bf3a13b49cda4860e309",
    sha256: "39f4767d10b1ec5a5e37f9165e09f476c1e83cf522f4a16cf361f188a470c79e",
  },
  "apps/server/src/jones/persistence/JonesMigrations.ts": {
    blob: "4de6155626f3263e6526a3f6fc1269dcdfef1678",
    sha256: "b7127e3be693171525f23a38d12b51944f0e7d47d8efd5fae2c405c01ec93752",
  },
  "apps/server/src/persistence/Migrations/055_OrchestrationV2.ts": {
    blob: "7b658c296a4495578bc91d47f79129e057b0b9e1",
    sha256: "9b152eaa769029e1f5fdaaf3bd4696ca3c3803aff29ba38e161c586ef11393df",
  },
  "apps/server/src/persistence/Migrations/OrchestrationV2/Foundation.ts": {
    blob: "ebc6ebbc346d041c7ab9471ce977c8611f41c2f1",
    sha256: "8562015010a88b421c25fcafc0db7d88b044eef139cea88fabff2ad90ee8f6a6",
  },
  "apps/server/src/persistence/Migrations/OrchestrationV2/ProviderSessionBindings.ts": {
    blob: "79d9e655e3d4542340cb42344169ab70a33d8162",
    sha256: "fce5eed1f774d0a7213e438cf7b459bbfd03f5e5276bd92a6b2b5197d0784279",
  },
  "apps/server/src/persistence/Migrations/OrchestrationV2/Subagents.ts": {
    blob: "d63dc6a112a8795841012b68d5b6a4d3ab3ca7e3",
    sha256: "b4ef157a71bf21bbaaee2032f18c8259a2bbf8fa9a99acaa3e11c20e542dfd36",
  },
  "apps/server/src/persistence/Migrations/OrchestrationV2/EffectCancellation.ts": {
    blob: "12c54a6e8b98ae97107effbb3ceb7a5bc2c6b5e1",
    sha256: "f5de552e19987e6e7174c26a9a4bafdd082febea6e328cc1160e9f72fd83336b",
  },
  "apps/server/src/orchestration-v2/ProjectionStore.ts": {
    blob: "508ed888185cf1a5756fc795e1994865b2528563",
    sha256: "892ba1247870b8ea7448dfb83904ad5a69293e3194e847987710073fb55baf8b",
  },
} as const;
const legacyLedgers = {
  upstream: [
    [1, "OrchestrationEvents"],
    [2, "OrchestrationCommandReceipts"],
    [3, "CheckpointDiffBlobs"],
    [4, "ProviderSessionRuntime"],
    [5, "Projections"],
    [6, "ProjectionThreadSessionRuntimeModeColumns"],
    [7, "ProjectionThreadMessageAttachments"],
    [8, "ProjectionThreadActivitySequence"],
    [9, "ProviderSessionRuntimeMode"],
    [10, "ProjectionThreadsRuntimeMode"],
    [11, "OrchestrationThreadCreatedRuntimeMode"],
    [12, "ProjectionThreadsInteractionMode"],
    [13, "ProjectionThreadProposedPlans"],
    [14, "ProjectionThreadProposedPlanImplementation"],
    [15, "ProjectionTurnsSourceProposedPlan"],
    [16, "CanonicalizeModelSelections"],
    [17, "ProjectionThreadsArchivedAt"],
    [18, "ProjectionThreadsArchivedAtIndex"],
    [19, "ProjectionSnapshotLookupIndexes"],
    [20, "AuthAccessManagement"],
    [21, "AuthSessionClientMetadata"],
    [22, "AuthSessionLastConnectedAt"],
    [23, "ProjectionThreadShellSummary"],
    [24, "BackfillProjectionThreadShellSummary"],
    [25, "CleanupInvalidProjectionPendingApprovals"],
    [26, "CanonicalizeModelSelectionOptions"],
    [27, "ProviderSessionRuntimeInstanceId"],
    [28, "ProjectionThreadSessionInstanceId"],
    [29, "ProjectionThreadDetailOrderingIndexes"],
    [30, "ProjectionThreadShellArchiveIndexes"],
    [31, "AuthAuthorizationScopes"],
    [32, "AuthPairingProofKeyThumbprint"],
    [33, "ProjectionThreadsSettled"],
    [34, "ProjectionThreadsSnoozed"],
    [35, "ProjectionThreadTitleRegeneration"],
    [36, "ProjectionThreadsPinned"],
    [37, "ProjectionTurnsKeysetIndex"],
    [38, "ProjectionThreadsPinOrderKey"],
    [39, "ProjectionProjectsDefaultThreadEnvMode"],
    [40, "ProjectionProjectFaviconPath"],
    [41, "AuthSessionClientConnection"],
    [42, "ProjectionThreadLinkedPullRequest"],
    [43, "ProjectionThreadsUnsettledAt"],
    [44, "ClearAutomaticProjectModelDefaults"],
    [45, "ProjectionProjectsAutoPull"],
    [46, "RepairAutomaticSettlementTimestamps"],
    [47, "ProjectionProjectIcon"],
    [48, "ProjectionThreadBranchPullRequest"],
    [49, "ProjectionThreadsActiveOrderKey"],
    [50, "ProjectionThreadPullRequests"],
    [51, "ProjectionThreadMessageContext"],
    [52, "ProjectionThreadTitleState"],
    [53, "PullRequestFilesViewed"],
    [54, "ProjectionThreadsAutoSettleDisabledAt"],
    [55, "OrchestrationV2"],
    [56, "RemoveRedundantProjectionIndexes"],
    [57, "ScheduledTaskWebhooks"],
    [58, "WebhookRelayDeliveries"],
    [59, "McpAppModelContext"],
  ],
  jones: [
    [1, "WorktreeOwnershipLeases"],
    [2, "ProjectionThreadRuntimeIdentity"],
    [3, "NativeCreationIntents"],
    [4, "NativeCreationCommandIdentities"],
    [5, "WorkstreamsNativeAttempts"],
    [6, "WorkstreamsProviderEnrollments"],
    [100, "NativeCreationExecution"],
    [101, "NativeWorkspacePreparation"],
    [102, "ImportedHistoryChoices"],
    [103, "RuntimeStop"],
    [104, "DeletionAdmission"],
    [105, "UpstreamReferenceIsolation"],
  ],
} as const;
const legacyTables: Readonly<Record<string, string>> = {
  orchestration_v2_projection_threads:
    "CREATE TABLE orchestration_v2_projection_threads (\n      thread_id TEXT PRIMARY KEY,\n      project_id TEXT NOT NULL,\n      title TEXT NOT NULL,\n      default_provider TEXT NOT NULL,\n      runtime_mode TEXT NOT NULL,\n      interaction_mode TEXT NOT NULL,\n      active_provider_thread_id TEXT,\n      created_at TEXT NOT NULL,\n      updated_at TEXT NOT NULL,\n      archived_at TEXT,\n      deleted_at TEXT,\n      payload_json TEXT NOT NULL\n    , provider_instance_id TEXT\n    )",
  orchestration_v2_projection_runs:
    "CREATE TABLE orchestration_v2_projection_runs (\n      run_id TEXT PRIMARY KEY,\n      thread_id TEXT NOT NULL,\n      ordinal INTEGER NOT NULL,\n      provider TEXT NOT NULL,\n      provider_thread_id TEXT,\n      status TEXT NOT NULL,\n      requested_at TEXT NOT NULL,\n      completed_at TEXT,\n      payload_json TEXT NOT NULL\n    , provider_instance_id TEXT\n    )",
  orchestration_v2_projection_run_attempts:
    "CREATE TABLE orchestration_v2_projection_run_attempts (\n      attempt_id TEXT PRIMARY KEY,\n      thread_id TEXT NOT NULL,\n      run_id TEXT NOT NULL,\n      attempt_ordinal INTEGER NOT NULL,\n      root_node_id TEXT NOT NULL,\n      provider TEXT NOT NULL,\n      provider_thread_id TEXT NOT NULL,\n      provider_turn_id TEXT,\n      status TEXT NOT NULL,\n      payload_json TEXT NOT NULL\n    , provider_instance_id TEXT\n    )",
  orchestration_v2_projection_nodes:
    "CREATE TABLE orchestration_v2_projection_nodes (\n      node_id TEXT PRIMARY KEY,\n      thread_id TEXT NOT NULL,\n      run_id TEXT,\n      parent_node_id TEXT,\n      root_node_id TEXT NOT NULL,\n      kind TEXT NOT NULL,\n      status TEXT NOT NULL,\n      provider_thread_id TEXT,\n      provider_turn_id TEXT,\n      runtime_request_id TEXT,\n      checkpoint_scope_id TEXT,\n      started_at TEXT,\n      completed_at TEXT,\n      payload_json TEXT NOT NULL\n    )",
  orchestration_v2_projection_provider_sessions:
    "CREATE TABLE orchestration_v2_projection_provider_sessions (\n      provider_session_id TEXT PRIMARY KEY,\n      thread_id TEXT,\n      provider TEXT NOT NULL,\n      status TEXT NOT NULL,\n      model TEXT,\n      updated_at TEXT NOT NULL,\n      payload_json TEXT NOT NULL\n    , driver TEXT\n    , provider_instance_id TEXT\n    )",
  orchestration_v2_projection_provider_threads:
    "CREATE TABLE orchestration_v2_projection_provider_threads (\n      provider_thread_id TEXT PRIMARY KEY,\n      thread_id TEXT,\n      owner_node_id TEXT,\n      provider TEXT NOT NULL,\n      provider_session_id TEXT,\n      status TEXT NOT NULL,\n      first_run_ordinal INTEGER,\n      last_run_ordinal INTEGER,\n      updated_at TEXT NOT NULL,\n      payload_json TEXT NOT NULL\n    , driver TEXT\n    , provider_instance_id TEXT\n    )",
  orchestration_v2_projection_provider_turns:
    "CREATE TABLE orchestration_v2_projection_provider_turns (\n      provider_turn_id TEXT PRIMARY KEY,\n      thread_id TEXT NOT NULL,\n      provider_thread_id TEXT NOT NULL,\n      node_id TEXT NOT NULL,\n      run_attempt_id TEXT,\n      ordinal INTEGER NOT NULL,\n      status TEXT NOT NULL,\n      started_at TEXT,\n      completed_at TEXT,\n      payload_json TEXT NOT NULL\n    )",
  orchestration_v2_projection_runtime_requests:
    "CREATE TABLE orchestration_v2_projection_runtime_requests (\n      runtime_request_id TEXT PRIMARY KEY,\n      thread_id TEXT NOT NULL,\n      node_id TEXT NOT NULL,\n      provider_turn_id TEXT,\n      kind TEXT NOT NULL,\n      status TEXT NOT NULL,\n      created_at TEXT NOT NULL,\n      resolved_at TEXT,\n      payload_json TEXT NOT NULL\n    )",
  orchestration_v2_projection_messages:
    "CREATE TABLE orchestration_v2_projection_messages (\n      message_id TEXT PRIMARY KEY,\n      thread_id TEXT NOT NULL,\n      run_id TEXT,\n      node_id TEXT,\n      role TEXT NOT NULL,\n      streaming INTEGER NOT NULL,\n      created_at TEXT NOT NULL,\n      updated_at TEXT NOT NULL,\n      payload_json TEXT NOT NULL\n    )",
  orchestration_v2_projection_turn_items:
    "CREATE TABLE orchestration_v2_projection_turn_items (\n      turn_item_id TEXT PRIMARY KEY,\n      thread_id TEXT NOT NULL,\n      run_id TEXT,\n      node_id TEXT,\n      provider_thread_id TEXT,\n      provider_turn_id TEXT,\n      parent_item_id TEXT,\n      ordinal INTEGER NOT NULL,\n      type TEXT NOT NULL,\n      status TEXT NOT NULL,\n      updated_at TEXT NOT NULL,\n      payload_json TEXT NOT NULL\n    )",
  orchestration_v2_projection_provider_session_bindings:
    "CREATE TABLE orchestration_v2_projection_provider_session_bindings (\n      provider_session_id TEXT NOT NULL,\n      thread_id TEXT NOT NULL,\n      PRIMARY KEY (provider_session_id, thread_id)\n    )",
  orchestration_v2_projection_subagents:
    "CREATE TABLE orchestration_v2_projection_subagents (\n      subagent_id TEXT PRIMARY KEY,\n      thread_id TEXT NOT NULL,\n      run_id TEXT,\n      parent_node_id TEXT NOT NULL,\n      provider TEXT NOT NULL,\n      provider_thread_id TEXT,\n      child_thread_id TEXT,\n      origin TEXT NOT NULL,\n      status TEXT NOT NULL,\n      started_at TEXT,\n      completed_at TEXT,\n      updated_at TEXT NOT NULL,\n      payload_json TEXT NOT NULL\n    , driver TEXT\n    , provider_instance_id TEXT\n    )",
  orchestration_v2_effect_outbox:
    "CREATE TABLE orchestration_v2_effect_outbox (\n      effect_id TEXT PRIMARY KEY,\n      command_id TEXT NOT NULL,\n      thread_id TEXT NOT NULL,\n      effect_type TEXT NOT NULL,\n      payload_json TEXT NOT NULL,\n      status TEXT NOT NULL CHECK (\n        status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')\n      ),\n      attempt_count INTEGER NOT NULL DEFAULT 0,\n      available_at TEXT NOT NULL,\n      lease_owner TEXT,\n      lease_expires_at TEXT,\n      created_at TEXT NOT NULL,\n      updated_at TEXT NOT NULL,\n      completed_at TEXT,\n      last_error TEXT\n    )",
};

export class OfflinePreparationError extends Schema.TaggedError<OfflinePreparationError>()(
  "OfflinePreparationError",
  {
    reason: Schema.String,
    retainedRoot: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return "Offline preparation is held: " + this.reason;
  }
}

type FailurePoint =
  | "plan-file-sync"
  | "plan-directory-sync"
  | "before-commit"
  | "after-commit"
  | "evidence-file-sync"
  | "evidence-directory-sync"
  | "cancel-before-write"
  | "cancel-after-commit"
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
}
interface Selection {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly existingId: string | null;
  readonly projectionSha256: string;
}
type OutboxRow = {
  readonly effect_id: string;
  readonly command_id: string;
  readonly thread_id: string;
  readonly effect_type: string;
  readonly payload_json: string;
  readonly status: string;
  readonly attempt_count: number;
  readonly available_at: string;
  readonly created_at: string;
  readonly updated_at: string;
  readonly lease_owner: string | null;
  readonly lease_expires_at: string | null;
  readonly completed_at: string | null;
  readonly last_error: string | null;
};
interface ContinuationPlan {
  readonly id: string;
  readonly timestamp: string;
  readonly source: typeof legacySource;
  readonly snapshotSha256: string;
  readonly settingsSha256: string;
  readonly selections: ReadonlyArray<Selection>;
  readonly expectedRows: ReadonlyArray<OutboxRow>;
  readonly preservedRows: ReadonlyArray<OutboxRow>;
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

export function offlinePreparationStatus() {
  return {
    status: "held",
    reason: "maintenance-owner-binding-unqualified",
    databaseOpened: false,
    productionInvocation: false,
    activation: false,
  } as const;
}

const digest = (value: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
const settingsDigest = (raw: string | undefined) =>
  digest(JSON.stringify(raw === undefined ? { present: false } : { present: true, raw }));
const normalizeSql = (value: string) => value.replaceAll('"', "").replace(/\s+/g, "");
const ledgerDefinition = (table: "effect_sql_migrations" | "jones_sql_migrations") =>
  "CREATE TABLE " +
  table +
  " (migration_id integer PRIMARY KEY NOT NULL, created_at datetime NOT NULL DEFAULT current_timestamp, name VARCHAR(255) NOT NULL)";
const encodeRequest = Schema.encodeSync(
  Schema.fromJsonString(EffectOutbox.OrchestrationEffectRequestV2),
);
const decodeRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(EffectOutbox.OrchestrationEffectRequestV2),
);
const readRows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<OutboxRow>`SELECT effect_id, command_id, thread_id, effect_type,
    payload_json, status, attempt_count, available_at, created_at, updated_at,
    lease_owner, lease_expires_at, completed_at, last_error
    FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
});
const readSelection = (rawSettings: string | undefined) =>
  Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const preferences = decodePersistedServerSettings(rawSettings).settings;
    const selections: Selection[] = [];
    for (const threadId of yield* store.getRecoveryThreadIds("runtime")) {
      const projection = yield* store.getRuntimeRecoveryProjection(threadId);
      const run = serverUpdateContinuationRun(projection, preferences);
      if (run === undefined) continue;
      const existing = yield* findPendingServerUpdateContinuation(threadId, run.id);
      selections.push({
        threadId,
        runId: run.id,
        existingId: existing[0]?.effect_id ?? null,
        projectionSha256: digest(JSON.stringify(projection)),
      });
    }
    return selections;
  });
const verifySchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [table, expected] of [
    ["effect_sql_migrations", legacyLedgers.upstream],
    ["jones_sql_migrations", legacyLedgers.jones],
  ] as const) {
    const definition = yield* sql<{
      sql: string;
    }>`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ${table}`;
    if (
      definition[0] === undefined ||
      normalizeSql(definition[0].sql) !== normalizeSql(ledgerDefinition(table))
    )
      return yield* Effect.fail(
        new OfflinePreparationError({ reason: "unsupported-ledger-schema" }),
      );
    const rows = yield* sql<{
      migration_id: number;
      name: string;
    }>`SELECT migration_id, name FROM ${sql(table)} ORDER BY migration_id`;
    if (
      JSON.stringify(rows.map((row) => [row.migration_id, row.name])) !== JSON.stringify(expected)
    )
      return yield* Effect.fail(new OfflinePreparationError({ reason: "unsupported-ledger" }));
  }
  const tables = yield* sql<{
    name: string;
    sql: string;
  }>`SELECT name, sql FROM sqlite_master WHERE type = 'table'`;
  for (const [name, ddl] of Object.entries(legacyTables)) {
    const actual = tables.find((table) => table.name === name);
    if (actual === undefined || normalizeSql(actual.sql) !== normalizeSql(ddl))
      return yield* Effect.fail(new OfflinePreparationError({ reason: "unsupported-schema" }));
  }
  const triggers = yield* sql`SELECT name FROM sqlite_master WHERE type = 'trigger'`;
  if (triggers.length !== 0)
    return yield* Effect.fail(new OfflinePreparationError({ reason: "unsupported-trigger" }));
});

function fixedClock(timestamp: string): Clock.Clock {
  const millis = Date.parse(timestamp);
  const nanos = BigInt(millis) * 1_000_000n;
  return {
    currentTimeMillisUnsafe: () => millis,
    currentTimeMillis: Effect.succeed(millis),
    currentTimeNanosUnsafe: () => nanos,
    currentTimeNanos: Effect.succeed(nanos),
    monotonicTimeNanosUnsafe: () => nanos,
    monotonicTimeNanos: Effect.succeed(nanos),
    sleep: () =>
      Effect.die(new OfflinePreparationError({ reason: "unexpected-maintenance-sleep" })),
  };
}

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
function initializeFixture(path: string, seed: SyntheticSeed) {
  const database = new NodeSqlite.DatabaseSync(path);
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
  try {
    database.exec("BEGIN IMMEDIATE");
    for (const ddl of Object.values(legacyTables)) database.exec(ddl);
    for (const [table, ledger] of [
      ["effect_sql_migrations", legacyLedgers.upstream],
      ["jones_sql_migrations", legacyLedgers.jones],
    ] as const) {
      database.exec(ledgerDefinition(table));
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
        insert("orchestration_v2_effect_outbox", expectedRow(effect, now));
      }
    }
    if (seed.schema === "unsupported-ledger")
      database.exec(
        "UPDATE jones_sql_migrations SET name = 'unrecognized' WHERE migration_id = 105",
      );
    if (seed.schema === "unsupported-schema")
      database.exec("ALTER TABLE orchestration_v2_effect_outbox ADD COLUMN unexpected TEXT");
    database.exec("COMMIT");
  } finally {
    database.close();
  }
}

function expectedRow(
  effect: EffectOutbox.PendingOrchestrationEffectV2,
  timestamp: string,
): OutboxRow {
  return {
    effect_id: effect.id,
    command_id: effect.commandId,
    thread_id: effect.threadId,
    effect_type: effect.request.type,
    payload_json: encodeRequest(effect.request),
    status: "pending",
    attempt_count: 0,
    available_at: timestamp,
    created_at: timestamp,
    updated_at: timestamp,
    lease_owner: null,
    lease_expires_at: null,
    completed_at: null,
    last_error: null,
  };
}
const sameRow = (actual: OutboxRow | undefined, expected: OutboxRow) =>
  actual !== undefined &&
  Object.keys(expected).every(
    (key) => actual[key as keyof OutboxRow] === expected[key as keyof OutboxRow],
  );

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
  let active = true;
  let inFlight: Promise<unknown> | undefined;
  let unresolved = false;
  let writeStarted = false;
  let cancelled = false;
  let failurePoint: FailurePoint | undefined;
  let initialHash = "";
  let originalIdentity: { dev: number; ino: number } | undefined;

  const assertActive = () => {
    if (!active) throw new OfflinePreparationError({ reason: "expired-fixture" });
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
    assertActive();
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
    effect: Effect.Effect<
      T,
      E,
      SqlClient.SqlClient | ProjectionStore.ProjectionStoreV2 | EffectOutbox.EffectOutboxV2
    >,
  ): Promise<T> => {
    const stat = await existingFile(path);
    if (
      path === databasePath &&
      originalIdentity !== undefined &&
      (stat.dev !== originalIdentity.dev || stat.ino !== originalIdentity.ino)
    )
      throw new OfflinePreparationError({ reason: "database-identity-changed" });
    audit.push(readonly ? "database:read-only-open" : "database:write-open");
    const layer = Layer.mergeAll(ProjectionStore.layer, EffectOutbox.layer).pipe(
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
    const completed = await Effect.runPromise(
      program.pipe(Effect.exit, Effect.provide(layer)),
    ).catch((cause: unknown) => {
      unresolved = true;
      throw new OfflinePreparationError({ reason: "sqlite-scope-unresolved", cause });
    });
    audit.push("database:closed");
    if (completed._tag === "Failure") throw Cause.squash(completed.cause);
    return completed.value;
  };
  const publish = async (kind: "plan" | "evidence", path: string, content: string) => {
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
    if ((await NodeFSP.readFile(path, "utf8")) !== content)
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
    if ((await NodeFSP.readFile(planPath, "utf8")) !== JSON.stringify(plan))
      throw new OfflinePreparationError({ reason: "plan-file-changed" });
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
    const { rows, selections } = await withDatabase(
      databasePath,
      true,
      Effect.gen(function* () {
        yield* verifySchema;
        return { rows: yield* readRows, selections: yield* readSelection(raw) };
      }),
    );
    const preserved = plan.preservedRows.every((row) =>
      sameRow(
        rows.find((candidate) => candidate.effect_id === row.effect_id),
        row,
      ),
    );
    const all = plan.expectedRows.every((row) =>
      sameRow(
        rows.find((candidate) => candidate.effect_id === row.effect_id),
        row,
      ),
    );
    const absent = plan.expectedRows.every(
      (row) => !rows.some((candidate) => candidate.effect_id === row.effect_id),
    );
    const appliedSelections = plan.selections.map((selection) => ({
      ...selection,
      existingId:
        selection.existingId ??
        plan.expectedRows.find((row) => row.thread_id === selection.threadId)?.effect_id ??
        null,
    }));
    if (
      preserved &&
      all &&
      rows.length === plan.preservedRows.length + plan.expectedRows.length &&
      JSON.stringify(selections) === JSON.stringify(appliedSelections)
    ) {
      return evidence(plan, "applied");
    }
    if (preserved && absent && digest(await NodeFSP.readFile(databasePath)) === initialHash) {
      unresolved = false;
      return evidence(plan, "not-applied");
    }
    unresolved = true;
    return evidence(plan, "held");
  };

  let result: A | undefined;
  let failure: unknown;
  try {
    initializeFixture(databasePath, seed);
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
          const { selections, rows } = await withDatabase(
            snapshotPath,
            true,
            Effect.gen(function* () {
              yield* verifySchema;
              return { selections: yield* readSelection(raw), rows: yield* readRows };
            }),
          );
          const timestamp = new Date().toISOString();
          const expectedRows = selections
            .filter((selection) => selection.existingId === null)
            .map((selection) =>
              expectedRow(
                serverUpdateContinuationEffect({
                  preparationId: NodeCrypto.randomUUID(),
                  threadId: selection.threadId,
                  sourceRunId: selection.runId,
                  optedInOnly: true,
                }),
                timestamp,
              ),
            );
          const plan: ContinuationPlan = {
            id: NodeCrypto.randomUUID(),
            timestamp,
            source: legacySource,
            snapshotSha256: initialHash,
            settingsSha256: settingsDigest(raw),
            selections,
            expectedRows,
            preservedRows: rows,
          };
          for (const row of [...plan.selections, ...plan.expectedRows, ...plan.preservedRows])
            Object.freeze(row);
          Object.freeze(plan.selections);
          Object.freeze(plan.expectedRows);
          Object.freeze(plan.preservedRows);
          Object.freeze(plan);
          // A failed fsync never publishes the in-memory authority token.
          await publish("plan", planPath, JSON.stringify(plan));
          plans.add(plan);
          currentPlan = plan;
          return plan;
        }),
      apply: (plan) =>
        operation(async () => {
          checkPlan(plan);
          if (cancelled) throw new OfflinePreparationError({ reason: "preparation-cancelled" });
          if (writeStarted)
            throw new OfflinePreparationError({ reason: "reconciliation-required" });
          await validatePlanFiles(plan);
          const raw = await readSettings();
          if (digest(await NodeFSP.readFile(databasePath)) !== plan.snapshotSha256)
            throw new OfflinePreparationError({ reason: "baseline-changed" });
          await withDatabase(databasePath, true, verifySchema);
          failAt("cancel-before-write");
          writeStarted = true;
          unresolved = true;
          await withDatabase(
            databasePath,
            false,
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              const outbox = yield* EffectOutbox.EffectOutboxV2;
              yield* sql.withTransaction(
                Effect.gen(function* () {
                  yield* verifySchema;
                  const selected = yield* readSelection(raw);
                  if (JSON.stringify(selected) !== JSON.stringify(plan.selections))
                    return yield* Effect.fail(
                      new OfflinePreparationError({ reason: "selection-changed" }),
                    );
                  const rowsBefore = yield* readRows;
                  if (
                    rowsBefore.length !== plan.preservedRows.length ||
                    !plan.preservedRows.every((row) =>
                      sameRow(
                        rowsBefore.find((candidate) => candidate.effect_id === row.effect_id),
                        row,
                      ),
                    )
                  )
                    return yield* Effect.fail(
                      new OfflinePreparationError({ reason: "baseline-rows-changed" }),
                    );
                  for (const row of plan.expectedRows) {
                    const request = decodeRequest(row.payload_json);
                    yield* outbox
                      .enqueue([
                        {
                          id: row.effect_id,
                          commandId: CommandId.make(row.command_id),
                          threadId: ThreadId.make(row.thread_id),
                          request,
                          availableAt: DateTime.makeUnsafe(plan.timestamp),
                        },
                      ])
                      .pipe(Effect.provideService(Clock.Clock, fixedClock(plan.timestamp)));
                  }
                  const rows = yield* readRows;
                  if (
                    !plan.expectedRows.every((row) =>
                      sameRow(
                        rows.find((candidate) => candidate.effect_id === row.effect_id),
                        row,
                      ),
                    )
                  )
                    return yield* Effect.fail(
                      new OfflinePreparationError({ reason: "enqueue-mismatch" }),
                    );
                  yield* Effect.sync(() => failAt("before-commit"));
                }),
              );
            }),
          );
          audit.push("transaction:committed");
          failAt("after-commit");
          failAt("cancel-after-commit");
          const applied = await reconcile(plan);
          if (applied.status !== "applied") return applied;
          unresolved = true;
          await publish("evidence", evidencePath, JSON.stringify(applied));
          unresolved = false;
          return applied;
        }),
      reconcile: (plan) =>
        operation(async () => {
          const result = await reconcile(plan);
          if (result.status === "applied") {
            unresolved = true;
            await recordReconciliation(result);
            unresolved = false;
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
          unresolved = false;
          audit.push("snapshot-pair:restored");
        }),
      injectFailure: (point) => {
        assertActive();
        failurePoint = point;
      },
      mutate: (point, plan) =>
        operation(async () => {
          if (point === "missing-database") {
            await NodeFSP.unlink(databasePath);
            return;
          }
          if (point === "incomplete-wal") {
            await syncFile(databasePath + "-wal", "incomplete");
            return;
          }
          const database = new NodeSqlite.DatabaseSync(databasePath);
          try {
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
              unresolved = true;
            }
          } finally {
            database.close();
          }
        }),
      inspect: () =>
        operation(async () => ({
          scratchRoot: root,
          rows: await withDatabase(databasePath, true, readRows),
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
    active = false;
    try {
      await inFlight;
    } catch (cause) {
      failure ??= cause;
    }
    if (unresolved) {
      const retained = new OfflinePreparationError({
        reason: "unresolved-synthetic-effect",
        retainedRoot: root,
        cause: failure,
      });
      try {
        await syncFile(
          NodePath.join(root, "retained-evidence.json"),
          JSON.stringify({
            reason: retained.reason,
            planId: currentPlan?.id ?? null,
            source: legacySource,
            provenance: legacyProvenance,
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

// The packaged entry never parses a state path or opens a database. Owner
// currentness and cold-custody verification require a future reviewed binding.
if (
  isEntrypoint({
    moduleUrl: import.meta.url,
    entryPath: process.argv[1],
    runtimeMain: import.meta.main,
  })
)
  console.log(JSON.stringify(offlinePreparationStatus()));
