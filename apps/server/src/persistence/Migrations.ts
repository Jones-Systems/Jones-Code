/**
 * Migration runner with independent inline upstream and Jones loaders.
 *
 * Uses Migrator.make with fromRecord to define migrations inline.
 * All migrations are statically imported - no dynamic file system loading.
 *
 * `runMigrations` is called by the SQLite persistence layer at startup, so the
 * applicable schema is current before the application starts.
 */

import * as Migrator from "effect/unstable/sql/Migrator";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { reconcileV2PreviewMigration } from "./reconcileV2PreviewMigration.ts";
import JonesMigration0138 from "./Migrations/138_JonesThreadCreationLookupIndex.ts";
import JonesMigration0139 from "./Migrations/139_JonesDeletionWorktreeAdmission.ts";
import JonesMigration0140 from "./Migrations/140_JonesOrdinaryCheckoutOwnership.ts";
import JonesMigration0141 from "./Migrations/141_JonesOrdinaryCheckoutExecutionLifetime.ts";
import JonesMigration0142 from "./Migrations/142_JonesV2NativeAcceptance.ts";
import JonesMigration0143 from "./Migrations/143_JonesAttachmentCleanup.ts";
import JonesMigration0144 from "./Migrations/144_JonesImportedApplicationAttachments.ts";
import { runJonesMigrationsDetailed, type JonesMigrationEntry } from "./JonesMigrationGuard.ts";

// Import all migrations statically
import Migration0001 from "./Migrations/001_OrchestrationEvents.ts";
import Migration0002 from "./Migrations/002_OrchestrationCommandReceipts.ts";
import Migration0003 from "./Migrations/003_CheckpointDiffBlobs.ts";
import Migration0004 from "./Migrations/004_ProviderSessionRuntime.ts";
import Migration0005 from "./Migrations/005_Projections.ts";
import Migration0006 from "./Migrations/006_ProjectionThreadSessionRuntimeModeColumns.ts";
import Migration0007 from "./Migrations/007_ProjectionThreadMessageAttachments.ts";
import Migration0008 from "./Migrations/008_ProjectionThreadActivitySequence.ts";
import Migration0009 from "./Migrations/009_ProviderSessionRuntimeMode.ts";
import Migration0010 from "./Migrations/010_ProjectionThreadsRuntimeMode.ts";
import Migration0011 from "./Migrations/011_OrchestrationThreadCreatedRuntimeMode.ts";
import Migration0012 from "./Migrations/012_ProjectionThreadsInteractionMode.ts";
import Migration0013 from "./Migrations/013_ProjectionThreadProposedPlans.ts";
import Migration0014 from "./Migrations/014_ProjectionThreadProposedPlanImplementation.ts";
import Migration0015 from "./Migrations/015_ProjectionTurnsSourceProposedPlan.ts";
import Migration0016 from "./Migrations/016_CanonicalizeModelSelections.ts";
import Migration0017 from "./Migrations/017_ProjectionThreadsArchivedAt.ts";
import Migration0018 from "./Migrations/018_ProjectionThreadsArchivedAtIndex.ts";
import Migration0019 from "./Migrations/019_ProjectionSnapshotLookupIndexes.ts";
import Migration0020 from "./Migrations/020_AuthAccessManagement.ts";
import Migration0021 from "./Migrations/021_AuthSessionClientMetadata.ts";
import Migration0022 from "./Migrations/022_AuthSessionLastConnectedAt.ts";
import Migration0023 from "./Migrations/023_ProjectionThreadShellSummary.ts";
import Migration0024 from "./Migrations/024_BackfillProjectionThreadShellSummary.ts";
import Migration0025 from "./Migrations/025_CleanupInvalidProjectionPendingApprovals.ts";
import Migration0026 from "./Migrations/026_CanonicalizeModelSelectionOptions.ts";
import Migration0027 from "./Migrations/027_ProviderSessionRuntimeInstanceId.ts";
import Migration0028 from "./Migrations/028_ProjectionThreadSessionInstanceId.ts";
import Migration0029 from "./Migrations/029_ProjectionThreadDetailOrderingIndexes.ts";
import Migration0030 from "./Migrations/030_ProjectionThreadShellArchiveIndexes.ts";
import Migration0031 from "./Migrations/031_AuthAuthorizationScopes.ts";
import Migration0032 from "./Migrations/032_AuthPairingProofKeyThumbprint.ts";
import Migration0033 from "./Migrations/033_ProjectionThreadsSettled.ts";
import Migration0034 from "./Migrations/034_ProjectionThreadsSnoozed.ts";
import Migration0035 from "./Migrations/035_ProjectionThreadTitleRegeneration.ts";
import Migration0036 from "./Migrations/036_ProjectionThreadsPinned.ts";
import Migration0037 from "./Migrations/037_ProjectionTurnsKeysetIndex.ts";
import Migration0038 from "./Migrations/038_ProjectionThreadsPinOrderKey.ts";
import Migration0039 from "./Migrations/039_ProjectionProjectsDefaultThreadEnvMode.ts";
import Migration0040 from "./Migrations/040_ProjectionProjectFaviconPath.ts";
import Migration0041 from "./Migrations/041_AuthSessionClientConnection.ts";
import Migration0042 from "./Migrations/042_ProjectionThreadLinkedPullRequest.ts";
import Migration0043 from "./Migrations/043_ProjectionThreadsUnsettledAt.ts";
import Migration0044 from "./Migrations/044_ClearAutomaticProjectModelDefaults.ts";
import Migration0045 from "./Migrations/045_ProjectionProjectsAutoPull.ts";
import Migration0046 from "./Migrations/046_RepairAutomaticSettlementTimestamps.ts";
import Migration0047 from "./Migrations/047_ProjectionProjectIcon.ts";
import Migration0048 from "./Migrations/048_ProjectionThreadBranchPullRequest.ts";
import Migration0049 from "./Migrations/049_ProjectionThreadsActiveOrderKey.ts";
import Migration0050 from "./Migrations/050_ProjectionThreadPullRequests.ts";
import Migration0051 from "./Migrations/051_ProjectionThreadMessageContext.ts";
import Migration0052 from "./Migrations/052_ProjectionThreadTitleState.ts";
import Migration0053 from "./Migrations/053_PullRequestFilesViewed.ts";
import Migration0054 from "./Migrations/054_ProjectionThreadsAutoSettleDisabledAt.ts";
import Migration0055 from "./Migrations/055_OrchestrationV2.ts";
import Migration0056 from "./Migrations/056_RemoveRedundantProjectionIndexes.ts";
import JonesMigration0001 from "./Migrations/001_JonesWorktreeOwnershipLeases.ts";
import JonesMigration0002 from "./Migrations/002_JonesProjectionThreadRuntimeIdentity.ts";
import JonesMigration0003 from "./Migrations/003_JonesNativeCreationIntents.ts";
import JonesMigration0004 from "./Migrations/004_JonesNativeCreationCommandIdentities.ts";
import JonesMigration0005 from "./Migrations/005_JonesWorkstreamsNativeAttempts.ts";
import JonesMigration0006 from "./Migrations/006_JonesWorkstreamsProviderEnrollments.ts";

/**
 * Migration loader with all migrations defined inline.
 *
 * Key format: "{id}_{name}" where:
 * - id: numeric migration ID (determines execution order)
 * - name: descriptive name for the migration
 *
 * Uses Migrator.fromRecord which parses the key format and
 * returns migrations sorted by ID.
 */
export const migrationEntries = [
  [1, "OrchestrationEvents", Migration0001],
  [2, "OrchestrationCommandReceipts", Migration0002],
  [3, "CheckpointDiffBlobs", Migration0003],
  [4, "ProviderSessionRuntime", Migration0004],
  [5, "Projections", Migration0005],
  [6, "ProjectionThreadSessionRuntimeModeColumns", Migration0006],
  [7, "ProjectionThreadMessageAttachments", Migration0007],
  [8, "ProjectionThreadActivitySequence", Migration0008],
  [9, "ProviderSessionRuntimeMode", Migration0009],
  [10, "ProjectionThreadsRuntimeMode", Migration0010],
  [11, "OrchestrationThreadCreatedRuntimeMode", Migration0011],
  [12, "ProjectionThreadsInteractionMode", Migration0012],
  [13, "ProjectionThreadProposedPlans", Migration0013],
  [14, "ProjectionThreadProposedPlanImplementation", Migration0014],
  [15, "ProjectionTurnsSourceProposedPlan", Migration0015],
  [16, "CanonicalizeModelSelections", Migration0016],
  [17, "ProjectionThreadsArchivedAt", Migration0017],
  [18, "ProjectionThreadsArchivedAtIndex", Migration0018],
  [19, "ProjectionSnapshotLookupIndexes", Migration0019],
  [20, "AuthAccessManagement", Migration0020],
  [21, "AuthSessionClientMetadata", Migration0021],
  [22, "AuthSessionLastConnectedAt", Migration0022],
  [23, "ProjectionThreadShellSummary", Migration0023],
  [24, "BackfillProjectionThreadShellSummary", Migration0024],
  [25, "CleanupInvalidProjectionPendingApprovals", Migration0025],
  [26, "CanonicalizeModelSelectionOptions", Migration0026],
  [27, "ProviderSessionRuntimeInstanceId", Migration0027],
  [28, "ProjectionThreadSessionInstanceId", Migration0028],
  [29, "ProjectionThreadDetailOrderingIndexes", Migration0029],
  [30, "ProjectionThreadShellArchiveIndexes", Migration0030],
  [31, "AuthAuthorizationScopes", Migration0031],
  [32, "AuthPairingProofKeyThumbprint", Migration0032],
  [33, "ProjectionThreadsSettled", Migration0033],
  [34, "ProjectionThreadsSnoozed", Migration0034],
  [35, "ProjectionThreadTitleRegeneration", Migration0035],
  [36, "ProjectionThreadsPinned", Migration0036],
  [37, "ProjectionTurnsKeysetIndex", Migration0037],
  [38, "ProjectionThreadsPinOrderKey", Migration0038],
  [39, "ProjectionProjectsDefaultThreadEnvMode", Migration0039],
  [40, "ProjectionProjectFaviconPath", Migration0040],
  [41, "AuthSessionClientConnection", Migration0041],
  [42, "ProjectionThreadLinkedPullRequest", Migration0042],
  [43, "ProjectionThreadsUnsettledAt", Migration0043],
  [44, "ClearAutomaticProjectModelDefaults", Migration0044],
  [45, "ProjectionProjectsAutoPull", Migration0045],
  [46, "RepairAutomaticSettlementTimestamps", Migration0046],
  [47, "ProjectionProjectIcon", Migration0047],
  [48, "ProjectionThreadBranchPullRequest", Migration0048],
  [49, "ProjectionThreadsActiveOrderKey", Migration0049],
  [50, "ProjectionThreadPullRequests", Migration0050],
  [51, "ProjectionThreadMessageContext", Migration0051],
  [52, "ProjectionThreadTitleState", Migration0052],
  [53, "PullRequestFilesViewed", Migration0053],
  [54, "ProjectionThreadsAutoSettleDisabledAt", Migration0054],
  // Released as 53 and 54 in V2 previews; reconcileV2PreviewMigration preserves their ledger.
  // Preserve this migration's schema. Future V2 schema changes need new migrations.
  [55, "OrchestrationV2", Migration0055],
  [56, "RemoveRedundantProjectionIndexes", Migration0056],
] as const;

export const migrationManifest = migrationEntries.map(([id, name]) => [id, name] as const);

// Preserve the released Jones identities. New Jones migrations start at 100;
// IDs 7–99 belong to known foreign histories, never to this rebuild's loader.
export const jonesMigrationEntries = [
  [1, "WorktreeOwnershipLeases", JonesMigration0001],
  [2, "ProjectionThreadRuntimeIdentity", JonesMigration0002],
  [3, "NativeCreationIntents", JonesMigration0003],
  [4, "NativeCreationCommandIdentities", JonesMigration0004],
  [5, "WorkstreamsNativeAttempts", JonesMigration0005],
  [6, "WorkstreamsProviderEnrollments", JonesMigration0006],
  [
    138,
    "ThreadCreationLookupIndex",
    JonesMigration0138,
    {
      foreignV2: "independent",
      requiresOwn: [],
      sourceBasis: "138 creates covering indexes on upstream orchestration_events only.",
    },
  ],
  [
    139,
    "DeletionWorktreeAdmission",
    JonesMigration0139,
    {
      foreignV2: "inert-excluded",
      requiresOwn: [],
      sourceBasis: "139 creates the foreign deletion and worktree admission family.",
    },
  ],
  [
    140,
    "OrdinaryCheckoutOwnership",
    JonesMigration0140,
    {
      foreignV2: "inert-excluded",
      requiresOwn: [[139, "DeletionWorktreeAdmission"]],
      sourceBasis: "140 creates the foreign ordinary checkout ownership family.",
    },
  ],
  [
    141,
    "OrdinaryCheckoutExecutionLifetime",
    JonesMigration0141,
    {
      foreignV2: "inert-excluded",
      requiresOwn: [
        [139, "DeletionWorktreeAdmission"],
        [140, "OrdinaryCheckoutOwnership"],
      ],
      sourceBasis: "141 creates the foreign execution lifetime family.",
    },
  ],
  [
    142,
    "V2NativeAcceptance",
    JonesMigration0142,
    {
      foreignV2: "inert-excluded",
      requiresOwn: [],
      sourceBasis: "142 creates the indivisible foreign V2 native acceptance family.",
    },
  ],
  [
    143,
    "AttachmentCleanup",
    JonesMigration0143,
    {
      foreignV2: "inert-excluded",
      requiresOwn: [],
      sourceBasis: "143 creates the foreign attachment cleanup observation family.",
    },
  ],
  [
    144,
    "ImportedApplicationAttachments",
    JonesMigration0144,
    {
      foreignV2: "inert-excluded",
      requiresOwn: [[142, "V2NativeAcceptance"]],
      sourceBasis:
        "144 creates immutable imported application attachment inventories tied to own native acceptance.",
    },
  ],
] as const satisfies ReadonlyArray<JonesMigrationEntry>;

const makeMigrationLoader = (throughId?: number) =>
  Migrator.fromRecord(
    Object.fromEntries(
      migrationEntries
        .filter(([id]) => throughId === undefined || id <= throughId)
        .map(([id, name, migration]) => [`${id}_${name}`, migration]),
    ),
  );

/**
 * Migrator run function - no schema dumping needed
 * Uses the base Migrator.make without platform dependencies
 */
const run = Migrator.make({});

export interface RunMigrationsOptions {
  /** Replay upstream history only; leave the Jones ledger and schema untouched. */
  readonly toMigrationInclusive?: number | undefined;
}

/**
 * Run all pending migrations.
 *
 * Runs upstream history in effect_sql_migrations, then validates and runs Jones
 * history in jones_sql_migrations. An upstream limit skips the Jones track.
 *
 * Returns array of [id, name] tuples for upstream migrations that were run.
 *
 * @returns Effect containing array of executed upstream migrations
 */
export const runMigrations = Effect.fn("runMigrations")(function* ({
  toMigrationInclusive,
}: RunMigrationsOptions = {}) {
  const previewMigrations =
    toMigrationInclusive === undefined || toMigrationInclusive >= 55
      ? yield* reconcileV2PreviewMigration()
      : [];
  const executedMigrations = [
    ...previewMigrations,
    ...(yield* run({ loader: makeMigrationLoader(toMigrationInclusive) })),
  ];
  const migrations = executedMigrations.map(([id, name]) => `${id}_${name}`);
  yield* migrations.length === 0
    ? Effect.logDebug("Database schema is current")
    : Effect.log("Migrations ran successfully").pipe(Effect.annotateLogs({ migrations }));

  // The migrator keys on migration_id: a database that recorded a different
  // migration under a shared id (local or fork builds) keeps that id and
  // silently skips this build's migration at it. Surface the divergence so the
  // skipped schema change is diagnosable.
  const sql = yield* SqlClient.SqlClient;
  const recorded = yield* sql<{
    readonly migration_id: number;
    readonly name: string;
  }>`SELECT migration_id, name FROM effect_sql_migrations`;
  const manifestNames = new Map<number, string>(migrationEntries.map(([id, name]) => [id, name]));
  const divergent = recorded.flatMap((row) => {
    const expected = manifestNames.get(row.migration_id);
    if (expected === undefined) {
      return [`${row.migration_id}:${row.name} (unknown to this build)`];
    }
    return expected === row.name
      ? []
      : [`${row.migration_id}:${row.name} (this build: ${expected})`];
  });
  if (divergent.length > 0) {
    yield* Effect.logWarning(
      "Database migration history diverges from this build; recorded migration ids are skipped, not reconciled by name.",
    ).pipe(Effect.annotateLogs({ divergent }));
  }
  if (toMigrationInclusive === undefined) {
    const jonesResult = yield* runJonesMigrationsDetailed(jonesMigrationEntries);
    const jonesMigrations = jonesResult.applied;
    if (jonesResult.excluded.length > 0) {
      yield* Effect.logWarning(
        "Jones feature migrations are inapplicable to preserved foreign V2 history",
      ).pipe(
        Effect.annotateLogs({
          historyMode: jonesResult.historyMode,
          excluded: jonesResult.excluded,
        }),
      );
    }
    if (jonesMigrations.length > 0) {
      yield* Effect.log("Jones migrations ran successfully").pipe(
        Effect.annotateLogs({ migrations: jonesMigrations.map(([id, name]) => `${id}_${name}`) }),
      );
    }
  }
  return executedMigrations;
});
