// @effect-diagnostics-next-line nodeBuiltinImport:off -- Hash only supplied projection bytes; this algorithm module never opens application state.
import * as NodeCrypto from "node:crypto";
import { CommandId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { decodePersistedServerSettings } from "../../ServerSettingsPersistence.ts";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import * as EffectOutbox from "../../orchestration-v2/EffectOutboxEnqueue.ts";
import { findPendingServerUpdateContinuation } from "../../orchestration-v2/ServerUpdateContinuationPreparation.ts";
import {
  serverUpdateContinuationEffect,
  serverUpdateContinuationRun,
} from "../../orchestration-v2/ServerUpdateContinuationPolicy.ts";
import {
  decodeOfflinePreparationPlan,
  encodeOfflinePreparationPlan,
  offlinePreparationPlanLimits,
  type OfflinePreparationPlan,
} from "./offlinePreparationPlan.ts";

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

// These values also seed the synthetic adapter; the core never executes their DDL.
export const offlinePreparationLegacy = {
  source: legacySource,
  provenance: legacyProvenance,
  ledgers: legacyLedgers,
  tables: legacyTables,
} as const;

type PreparationSelection = OfflinePreparationPlan["selections"][number];
export type PreparationOutboxRow = OfflinePreparationPlan["preservedRows"][number];
type Selection = PreparationSelection;
type OutboxRow = PreparationOutboxRow;
export interface PreparationBasis {
  readonly selections: ReadonlyArray<PreparationSelection>;
  readonly rows: ReadonlyArray<PreparationOutboxRow>;
}
const digest = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");

const normalizeSql = (value: string) => value.replaceAll('"', "").replace(/\s+/g, "");
export const preparationLedgerDefinition = (
  table: "effect_sql_migrations" | "jones_sql_migrations",
) =>
  "CREATE TABLE " +
  table +
  " (migration_id integer PRIMARY KEY NOT NULL, created_at datetime NOT NULL DEFAULT current_timestamp, name VARCHAR(255) NOT NULL)";
const encodeRequest = Schema.encodeSync(
  Schema.fromJsonString(EffectOutbox.OrchestrationEffectRequestV2),
);
const decodeRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(EffectOutbox.OrchestrationEffectRequestV2),
);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
interface ReadSize {
  readonly row_count: number;
  readonly byte_count: number;
}
function checkReadSize(value: ReadSize | undefined, rows: number, bytes: number, reason: string) {
  if (
    value === undefined ||
    !Number.isSafeInteger(value.row_count) ||
    value.row_count < 0 ||
    !Number.isSafeInteger(value.byte_count) ||
    value.byte_count < 0 ||
    value.row_count > rows ||
    value.byte_count > bytes
  )
    return Effect.fail(new OfflinePreparationError({ reason }));
  return Effect.void;
}

export const readPreparationRows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Reconciliation may contain both the preserved rows and every planned insertion.
  const maximum =
    offlinePreparationPlanLimits.preservedRows + offlinePreparationPlanLimits.selections;
  const sizes = yield* sql<ReadSize>`SELECT COUNT(*) AS row_count, COALESCE(SUM(
    COALESCE(length(CAST(effect_id AS BLOB)), 0) + length(CAST(command_id AS BLOB)) +
    length(CAST(thread_id AS BLOB)) + length(CAST(effect_type AS BLOB)) +
    length(CAST(payload_json AS BLOB)) + length(CAST(status AS BLOB)) +
    length(CAST(attempt_count AS BLOB)) + length(CAST(available_at AS BLOB)) +
    length(CAST(created_at AS BLOB)) + length(CAST(updated_at AS BLOB)) +
    COALESCE(length(CAST(lease_owner AS BLOB)), 0) +
    COALESCE(length(CAST(lease_expires_at AS BLOB)), 0) +
    COALESCE(length(CAST(completed_at AS BLOB)), 0) +
    COALESCE(length(CAST(last_error AS BLOB)), 0)
  ), 0) AS byte_count FROM orchestration_v2_effect_outbox`;
  yield* checkReadSize(sizes[0], maximum, offlinePreparationPlanLimits.bytes, "outbox-read-limit");
  const rows = yield* sql<OutboxRow>`SELECT effect_id, command_id, thread_id, effect_type,
    payload_json, status, attempt_count, available_at, created_at, updated_at,
    lease_owner, lease_expires_at, completed_at, last_error
    FROM orchestration_v2_effect_outbox ORDER BY effect_id LIMIT ${maximum + 1}`;
  if (rows.length !== sizes[0]!.row_count || rows.length > maximum)
    return yield* Effect.fail(new OfflinePreparationError({ reason: "outbox-read-limit" }));
  return rows;
});

const checkProjectionReadSize = (threadId: Selection["threadId"]) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    let rows = 0;
    let bytes = 0;
    // Bound conservative supersets before the unchanged eleven-field reader. This
    // may refuse a large history; it never silently drops rows from an accepted read.
    for (const table of Object.keys(legacyTables)) {
      if (!table.startsWith("orchestration_v2_projection_") || table.endsWith("_bindings"))
        continue;
      let condition = "thread_id = ?";
      let parameters: ReadonlyArray<string> = [threadId];
      if (table === "orchestration_v2_projection_provider_sessions") {
        condition =
          "provider_session_id IN (SELECT provider_session_id FROM orchestration_v2_projection_provider_session_bindings WHERE thread_id = ?)";
      } else if (table === "orchestration_v2_projection_provider_threads") {
        condition =
          "thread_id = ? OR owner_node_id IN (SELECT node_id FROM orchestration_v2_projection_nodes WHERE thread_id = ?) OR provider_thread_id IN (SELECT provider_thread_id FROM orchestration_v2_projection_subagents WHERE thread_id = ?)";
        parameters = [threadId, threadId, threadId];
      }
      const sizes = yield* sql.unsafe<ReadSize>(
        "SELECT COUNT(*) AS row_count, COALESCE(SUM(length(CAST(payload_json AS BLOB))), 0) AS byte_count FROM " +
          table +
          " WHERE " +
          condition,
        parameters,
      );
      yield* checkReadSize(
        sizes[0],
        offlinePreparationPlanLimits.preservedRows - rows,
        offlinePreparationPlanLimits.bytes - bytes,
        "projection-read-limit",
      );
      rows += sizes[0]!.row_count;
      bytes += sizes[0]!.byte_count;
    }
  });

const readSelection = (rawSettings: string | undefined) =>
  Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    // Every candidate comes from this bounded source set; the canonical selector
    // still decides eligibility and ordering without a copied SQL predicate.
    const size = yield* sql<ReadSize>`SELECT COUNT(*) AS row_count,
      COALESCE(SUM(length(CAST(thread_id AS BLOB))), 0) AS byte_count
      FROM orchestration_v2_projection_threads WHERE deleted_at IS NULL`;
    yield* checkReadSize(
      size[0],
      offlinePreparationPlanLimits.selections,
      offlinePreparationPlanLimits.selections * offlinePreparationPlanLimits.idBytes,
      "selection-read-limit",
    );
    const preferences = decodePersistedServerSettings(rawSettings).settings;
    // The receiving server rechecks eligibility when it delivers the continuation.
    const now = yield* DateTime.now;
    const selections: Selection[] = [];
    const candidates = yield* store.getRecoveryThreadIds("runtime");
    if (
      candidates.length > offlinePreparationPlanLimits.selections ||
      candidates.length > size[0]!.row_count
    )
      return yield* Effect.fail(new OfflinePreparationError({ reason: "selection-read-limit" }));
    for (const threadId of candidates) {
      yield* checkProjectionReadSize(threadId);
      const projection = yield* store.getRuntimeRecoveryProjection(threadId);
      const run = serverUpdateContinuationRun(projection, preferences, now);
      if (run === undefined) continue;
      const existing = yield* findPendingServerUpdateContinuation(threadId, run.id);
      selections.push({
        threadId,
        runId: run.id,
        existingId: existing[0]?.effect_id ?? null,
        projectionSha256: digest(yield* encodeJson(projection)),
      });
    }
    return selections;
  });
export const verifyPreparationSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const size = yield* sql<ReadSize>`SELECT COUNT(*) AS row_count,
    COALESCE(SUM(length(CAST(name AS BLOB)) + COALESCE(length(CAST(sql AS BLOB)), 0)), 0) AS byte_count
    FROM sqlite_master WHERE type = 'table'`;
  yield* checkReadSize(size[0], 1024, offlinePreparationPlanLimits.bytes, "schema-read-limit");
  for (const [table, expected] of [
    ["effect_sql_migrations", legacyLedgers.upstream],
    ["jones_sql_migrations", legacyLedgers.jones],
  ] as const) {
    const definition = yield* sql<{
      sql: string;
    }>`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ${table}`;
    if (
      definition[0] === undefined ||
      normalizeSql(definition[0].sql) !== normalizeSql(preparationLedgerDefinition(table))
    )
      return yield* Effect.fail(
        new OfflinePreparationError({ reason: "unsupported-ledger-schema" }),
      );
    const size = yield* sql<ReadSize>`SELECT COUNT(*) AS row_count,
      COALESCE(SUM(length(CAST(name AS BLOB))), 0) AS byte_count FROM ${sql(table)}`;
    yield* checkReadSize(
      size[0],
      expected.length,
      offlinePreparationPlanLimits.bytes,
      "unsupported-ledger",
    );
    const rows = yield* sql<{
      migration_id: number;
      name: string;
    }>`SELECT migration_id, name FROM ${sql(table)} ORDER BY migration_id LIMIT ${expected.length + 1}`;
    if (
      (yield* encodeJson(rows.map((row) => [row.migration_id, row.name]))) !==
      (yield* encodeJson(expected))
    )
      return yield* Effect.fail(new OfflinePreparationError({ reason: "unsupported-ledger" }));
  }
  const tables = yield* sql<{
    name: string;
    sql: string;
  }>`SELECT name, sql FROM sqlite_master WHERE type = 'table' LIMIT 1025`;
  for (const [name, ddl] of Object.entries(legacyTables)) {
    const actual = tables.find((table) => table.name === name);
    if (actual === undefined || normalizeSql(actual.sql) !== normalizeSql(ddl))
      return yield* Effect.fail(new OfflinePreparationError({ reason: "unsupported-schema" }));
  }
  const triggers = yield* sql`SELECT name FROM sqlite_master WHERE type = 'trigger' LIMIT 1`;
  if (triggers.length !== 0)
    return yield* Effect.fail(new OfflinePreparationError({ reason: "unsupported-trigger" }));
});

export function preparationClock(timestamp: string): Clock.Clock {
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

export function preparationOutboxRow(
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

/** SQL and data algorithms only; callers own database acquisition, exclusion and finalization. */
export const readPreparationBasis = (rawSettings: string | undefined) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* verifyPreparationSchema;
        // Bound marker data before the historical selector can return an existing ID.
        const rows = yield* readPreparationRows;
        return { selections: yield* readSelection(rawSettings), rows } satisfies PreparationBasis;
      }),
    );
  });

export function createContinuationPlan(
  basis: PreparationBasis,
  creation: Readonly<{
    id: string;
    timestamp: string;
    snapshotSha256: string;
    settingsSha256: string;
    preparationIds: ReadonlyArray<string>;
  }>,
): OfflinePreparationPlan {
  if (
    basis.selections.length > offlinePreparationPlanLimits.selections ||
    basis.rows.length > offlinePreparationPlanLimits.preservedRows ||
    creation.preparationIds.length > offlinePreparationPlanLimits.selections
  )
    throw new OfflinePreparationError({ reason: "plan-input-limit" });
  const selected = basis.selections.filter((selection) => selection.existingId === null);
  if (selected.length !== creation.preparationIds.length)
    throw new OfflinePreparationError({ reason: "preparation-id-count" });
  const plan = {
    id: creation.id,
    timestamp: creation.timestamp,
    source: legacySource,
    snapshotSha256: creation.snapshotSha256,
    settingsSha256: creation.settingsSha256,
    selections: basis.selections,
    expectedRows: selected.map((selection, index) =>
      preparationOutboxRow(
        serverUpdateContinuationEffect({
          preparationId: creation.preparationIds[index]!,
          threadId: selection.threadId,
          sourceRunId: selection.runId,
          optedInOnly: true,
        }),
        creation.timestamp,
      ),
    ),
    preservedRows: basis.rows,
  };
  // The codec copies and freezes data. Neither these bytes nor this object grants a state scope.
  return decodeOfflinePreparationPlan(encodeOfflinePreparationPlan(plan)).plan;
}

export const applyContinuationPlan = (
  plan: OfflinePreparationPlan,
  rawSettings: string | undefined,
) =>
  Effect.gen(function* () {
    const captured = yield* Effect.try({
      try: () => decodeOfflinePreparationPlan(encodeOfflinePreparationPlan(plan)).plan,
      catch: (cause) => new OfflinePreparationError({ reason: "invalid-plan", cause }),
    });
    const sql = yield* SqlClient.SqlClient;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* verifyPreparationSchema;
        const rowsBefore = yield* readPreparationRows;
        const selected = yield* readSelection(rawSettings);
        if ((yield* encodeJson(selected)) !== (yield* encodeJson(captured.selections)))
          return yield* Effect.fail(new OfflinePreparationError({ reason: "selection-changed" }));
        if (
          rowsBefore.length !== captured.preservedRows.length ||
          !captured.preservedRows.every((row) =>
            sameRow(
              rowsBefore.find((candidate) => candidate.effect_id === row.effect_id),
              row,
            ),
          )
        )
          return yield* Effect.fail(
            new OfflinePreparationError({ reason: "baseline-rows-changed" }),
          );
        for (const row of captured.expectedRows) {
          const request = decodeRequest(row.payload_json);
          yield* EffectOutbox.enqueue([
            {
              id: row.effect_id,
              commandId: CommandId.make(row.command_id),
              threadId: ThreadId.make(row.thread_id),
              request,
              availableAt: DateTime.makeUnsafe(captured.timestamp),
            },
          ]).pipe(Effect.provideService(Clock.Clock, preparationClock(captured.timestamp)));
        }
        const rows = yield* readPreparationRows;
        if (
          !captured.expectedRows.every((row) =>
            sameRow(
              rows.find((candidate) => candidate.effect_id === row.effect_id),
              row,
            ),
          )
        )
          return yield* Effect.fail(new OfflinePreparationError({ reason: "enqueue-mismatch" }));
      }),
    );
  });

/** A logical comparison only; the adapter must also establish its physical baseline and custody. */
export function classifyPreparation(
  plan: OfflinePreparationPlan,
  current: PreparationBasis,
): "applied" | "not-applied" | "held" {
  const preserved = plan.preservedRows.every((row) =>
    sameRow(
      current.rows.find((candidate) => candidate.effect_id === row.effect_id),
      row,
    ),
  );
  const all = plan.expectedRows.every((row) =>
    sameRow(
      current.rows.find((candidate) => candidate.effect_id === row.effect_id),
      row,
    ),
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
    current.rows.length === plan.preservedRows.length + plan.expectedRows.length &&
    JSON.stringify(current.selections) === JSON.stringify(appliedSelections)
  )
    return "applied";
  const absent = plan.expectedRows.every(
    (row) => !current.rows.some((candidate) => candidate.effect_id === row.effect_id),
  );
  if (
    preserved &&
    absent &&
    current.rows.length === plan.preservedRows.length &&
    JSON.stringify(current.selections) === JSON.stringify(plan.selections)
  )
    return "not-applied";
  return "held";
}
