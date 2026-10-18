import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import { Command, Flag } from "effect/cli";

import { readJonesStartupGateProtocol } from "../jones/updates/trialStartup.ts";
import { runServicePreflight } from "../cloud/servicePreflight.ts";
import { migrationManifest } from "../persistence/Migrations.ts";
import { jonesMigrationEntries } from "../jones/persistence/JonesMigrations.ts";
import { readMigrationPlan } from "../jones/updates/migrationPlan.ts";

export const servicePreflightCommand = Command.make("__service-preflight", {
  databasePath: Flag.String("database-path"),
  launcherProtocol: Flag.Int("launcher-protocol"),
}).pipe(
  Command.unlisted,
  Command.withHandler(({ databasePath, launcherProtocol }) =>
    Console.log(
      JSON.stringify(
        runServicePreflight({
          databasePath,
          launcherProtocol,
          startupGateProtocol: readJonesStartupGateProtocol(),
          migrationPlanResult: readMigrationPlan({
            databasePath,
            upstream: migrationManifest,
            jones: jonesMigrationEntries.map(([id, name]) => [id, name] as const),
          }),
        }),
      ),
    ).pipe(Effect.asVoid),
  ),
);
