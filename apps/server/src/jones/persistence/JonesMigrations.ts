import * as Effect from "effect/Effect";
import { runJonesMigrations } from "./JonesMigrationGuard.ts";

import JonesMigration0001 from "./Migrations/001_JonesWorktreeOwnershipLeases.ts";
import JonesMigration0002 from "./Migrations/002_JonesProjectionThreadRuntimeIdentity.ts";
import JonesMigration0003 from "./Migrations/003_JonesNativeCreationIntents.ts";
import JonesMigration0004 from "./Migrations/004_JonesNativeCreationCommandIdentities.ts";
import JonesMigration0005 from "./Migrations/005_JonesWorkstreamsNativeAttempts.ts";
import JonesMigration0006 from "./Migrations/006_JonesWorkstreamsProviderEnrollments.ts";

import JonesMigration0100 from "./Migrations/100_JonesNativeCreationExecution.ts";
import JonesMigration0101 from "./Migrations/101_JonesNativeWorkspacePreparation.ts";
import JonesMigration0104 from "./Migrations/104_JonesDeletionAdmission.ts";

import JonesMigration0103 from "./Migrations/103_JonesRuntimeStop.ts";

// Preserve the released Jones identities. New Jones migrations start at 100;
// IDs 7–99 belong to known foreign histories, never to this rebuild's loader.
import JonesMigration0102 from "./Migrations/102_JonesImportedHistoryChoices.ts";
import JonesMigration0105 from "./Migrations/105_JonesUpstreamReferenceIsolation.ts";

import JonesMigration0106 from "./Migrations/106_JonesPlannedUpdateContinuity.ts";

import JonesMigration0107 from "./Migrations/107_JonesWorkstreamsSettlementProofs.ts";

import JonesMigration0108 from "./Migrations/108_JonesPhysicalProcessEvidence.ts";

import JonesMigration0109 from "./Migrations/109_JonesOrdinaryCheckoutLifecycle.ts";

import JonesMigration0110 from "./Migrations/110_JonesThreadLookupIndexes.ts";
import JonesMigration0111 from "./Migrations/111_JonesClientSettlementCommands.ts";

import JonesMigration0112 from "./Migrations/112_JonesClientLaunchRequests.ts";

import JonesMigration0114 from "./Migrations/114_JonesSelfSettlementProjectItems.ts";
import JonesMigration0115 from "./Migrations/115_JonesScheduledWebhookPendingRuns.ts";

import JonesMigration0120 from "./Migrations/120_JonesSettlementRestores.ts";

export const jonesMigrationEntries = [
  [1, "WorktreeOwnershipLeases", JonesMigration0001],
  [2, "ProjectionThreadRuntimeIdentity", JonesMigration0002],
  [3, "NativeCreationIntents", JonesMigration0003],
  [4, "NativeCreationCommandIdentities", JonesMigration0004],
  [5, "WorkstreamsNativeAttempts", JonesMigration0005],
  [6, "WorkstreamsProviderEnrollments", JonesMigration0006],
  [100, "NativeCreationExecution", JonesMigration0100],
  [101, "NativeWorkspacePreparation", JonesMigration0101],
  [102, "ImportedHistoryChoices", JonesMigration0102],
  [103, "RuntimeStop", JonesMigration0103],
  [104, "DeletionAdmission", JonesMigration0104],
  [105, "UpstreamReferenceIsolation", JonesMigration0105],
  [106, "PlannedUpdateContinuity", JonesMigration0106],
  [107, "WorkstreamsSettlementProofs", JonesMigration0107],
  [108, "PhysicalProcessEvidence", JonesMigration0108],
  [109, "OrdinaryCheckoutLifecycle", JonesMigration0109],
  [110, "ThreadLookupIndexes", JonesMigration0110],
  [111, "ClientSettlementCommands", JonesMigration0111],
  [112, "ClientLaunchRequests", JonesMigration0112],
  [114, "SelfSettlementProjectItems", JonesMigration0114],
  [115, "ScheduledWebhookPendingRuns", JonesMigration0115],
  [120, "SettlementRestores", JonesMigration0120],
] as const;

export const runMigrations = () =>
  Effect.gen(function* () {
    const jonesMigrations = yield* runJonesMigrations(jonesMigrationEntries);
    if (jonesMigrations.length > 0) {
      yield* Effect.log("Jones migrations ran successfully").pipe(
        Effect.annotateLogs({ migrations: jonesMigrations.map(([id, name]) => `${id}_${name}`) }),
      );
    }
  });
