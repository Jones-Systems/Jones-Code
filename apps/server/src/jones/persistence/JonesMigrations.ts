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

// Preserve the released Jones identities. New Jones migrations start at 100;
// IDs 7–99 belong to known foreign histories, never to this rebuild's loader.
const jonesMigrationEntries = [
  [1, "WorktreeOwnershipLeases", JonesMigration0001],
  [2, "ProjectionThreadRuntimeIdentity", JonesMigration0002],
  [3, "NativeCreationIntents", JonesMigration0003],
  [4, "NativeCreationCommandIdentities", JonesMigration0004],
  [5, "WorkstreamsNativeAttempts", JonesMigration0005],
  [6, "WorkstreamsProviderEnrollments", JonesMigration0006],
  [100, "NativeCreationExecution", JonesMigration0100],
  [101, "NativeWorkspacePreparation", JonesMigration0101],
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
