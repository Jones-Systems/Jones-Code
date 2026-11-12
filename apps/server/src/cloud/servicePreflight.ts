import packageJson from "../../package.json" with { type: "json" };
import { BUILD_MIGRATION_HEAD, recordedMigrations } from "../persistence/databaseMigrationHead.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "./serviceProtocol.ts";
import type { MigrationPlanResult } from "../jones/updates/migrationPlan.ts";
import type { ServicePreflightResult } from "../jones/cloud/servicePreflightResult.ts";

// The service launcher decodes preflight output without importing this module's migration graph.
export {
  decodeServicePreflightResult,
  qualifiedServicePreflightFailure,
  type ServicePreflightResult,
} from "../jones/cloud/servicePreflightResult.ts";

export function runServicePreflight(input: {
  readonly databasePath: string;
  readonly launcherProtocol: number;
  readonly version?: string;
  readonly startupGateProtocol?: 1;
  readonly qualifiedRecoveryProtocol?: 1;
  readonly migrationPlanResult?: MigrationPlanResult;
}): ServicePreflightResult {
  const version = input.version ?? packageJson.version;
  if (input.launcherProtocol !== SERVICE_LAUNCHER_PROTOCOL) {
    return {
      status: "blocked",
      version,
      // A stale `t3` earlier on PATH (an old npm/nvm global) makes `t3 service install`
      // reinstall itself and report success, so name the exact install and the path check.
      reason: [
        `The installed service launcher speaks protocol ${input.launcherProtocol}, but t3@${version} requires protocol ${SERVICE_LAUNCHER_PROTOCOL}.`,
        `On the server machine, install this exact CLI: \`curl -fsSL https://t3.codes/install.sh | T3CODE_VERSION=${version} sh\`.`,
        `Check that \`t3 --version\` prints ${version} and that \`which -a t3\` lists no older t3 (such as an npm or nvm install) first, then run \`~/.local/bin/t3 service install\` (the installer's default path).`,
        "Updating the desktop app does not update this CLI, and `t3 service update` only repairs the service with whichever CLI runs it.",
      ].join(" "),
    };
  }

  const newer = newerRecordedMigrations(input.databasePath);
  if (newer === null) {
    return {
      status: "blocked",
      version,
      reason: `Could not read this database to confirm t3@${version} supports it.`,
    };
  }
  if (newer.length > 0) {
    return {
      status: "blocked",
      version,
      reason: `This database has migrations that t3@${version} does not know: ${newer.join(", ")}. A newer build wrote them, and running t3@${version} on this database is not safe. Stay on the newer release, or wait for a release that includes these migrations.`,
    };
  }

  if (input.migrationPlanResult?.status === "blocked") {
    return { status: "blocked", version, reason: input.migrationPlanResult.reason };
  }
  return {
    status: "ready",
    version,
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
    ...(input.migrationPlanResult?.status === "ready"
      ? { migrationPlan: input.migrationPlanResult.migrationPlan }
      : {}),
    ...(input.startupGateProtocol === undefined
      ? {}
      : { startupGateProtocol: input.startupGateProtocol }),
    ...(input.qualifiedRecoveryProtocol === undefined
      ? {}
      : { qualifiedRecoveryProtocol: input.qualifiedRecoveryProtocol }),
  };
}

/** Migrations a newer build recorded, or null when the database exists but cannot be read.
 * Startup already handles renamed ids this build knows. */
function newerRecordedMigrations(databasePath: string): ReadonlyArray<string> | null {
  const recorded = recordedMigrations(databasePath);
  return recorded === null
    ? null
    : recorded.filter(({ id }) => id > BUILD_MIGRATION_HEAD).map(({ id, name }) => `${id}_${name}`);
}
