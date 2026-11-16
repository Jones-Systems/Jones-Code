import { decodeMigrationPlan, type MigrationPlan, type MigrationPlanResult, readMigrationPlan } from "../jones/updates/migrationPlan.ts";
import { jonesMigrationEntries } from "../jones/persistence/JonesMigrations.ts";
import { migrationManifest } from "../persistence/Migrations.ts";
import packageJson from "../../package.json" with { type: "json" };
import { BUILD_MIGRATION_HEAD, recordedMigrations } from "../persistence/databaseMigrationHead.ts";
import { skippedV2Migrations } from "../persistence/Migrations.ts";
import { reconciledV2PreviewHistory } from "../persistence/reconcileV2PreviewMigration.ts";
import {
  isSupportedLauncherProtocol,
  SERVICE_LAUNCHER_PROTOCOL,
  type SupportedLauncherProtocol,
} from "./serviceProtocol.ts";

export { qualifiedServicePreflightFailure } from "../jones/cloud/servicePreflightResult.ts";

export type ServicePreflightResult =
  | {
      readonly status: "ready";
      readonly version: string;
      /** The launcher protocol the server named; it accepts no other in reply. */
      readonly launcherProtocol: SupportedLauncherProtocol;
      readonly startupGateProtocol?: 1;
      readonly qualifiedRecoveryProtocol?: 1;
      readonly migrationPlan?: MigrationPlan;
    }
  | {
      readonly status: "blocked";
      readonly version: string;
      readonly reason: string;
    };

export function runServicePreflight(input: {
  readonly databasePath: string;
  readonly launcherProtocol: number;
  readonly version?: string;
  readonly startupGateProtocol?: 1;
  readonly qualifiedRecoveryProtocol?: 1;
  readonly migrationPlanResult?: MigrationPlanResult;
}): ServicePreflightResult {
  const version = input.version ?? packageJson.version;
  const { launcherProtocol } = input;
  if (!isSupportedLauncherProtocol(launcherProtocol)) {
    return {
      status: "blocked",
      version,
      // A stale `t3` earlier on PATH (an old npm/nvm global) makes `t3 service install`
      // reinstall itself and report success, so name the exact install and the path check.
      reason: [
        `The installed service launcher speaks protocol ${launcherProtocol}, but t3@${version} requires protocol ${SERVICE_LAUNCHER_PROTOCOL}.`,
        `On the server machine, install this exact CLI: \`curl -fsSL https://t3.codes/install.sh | T3CODE_VERSION=${version} sh\`.`,
        `Check that \`t3 --version\` prints ${version} and that \`which -a t3\` lists no older t3 (such as an npm or nvm install) first, then run \`~/.local/bin/t3 service install\` (the installer's default path).`,
        "Updating the desktop app does not update this CLI, and `t3 service update` only repairs the service with whichever CLI runs it.",
      ].join(" "),
    };
  }

  const recorded = recordedMigrations(input.databasePath);
  if (recorded === null) {
    return {
      status: "blocked",
      version,
      reason: `Could not read this database to confirm t3@${version} supports it.`,
    };
  }
  const newer = recorded
    .filter(({ id }) => id > BUILD_MIGRATION_HEAD)
    .map(({ id, name }) => `${id}_${name}`);
  if (newer.length > 0) {
    return {
      status: "blocked",
      version,
      reason: `This database has migrations that t3@${version} does not know: ${newer.join(", ")}. A newer build wrote them, and running t3@${version} on this database is not safe. Stay on the newer release, or wait for a release that includes these migrations.`,
    };
  }

  // Startup reconciles V2 preview ids before it checks the history, so check what it will see.
  const reconciled = reconciledV2PreviewHistory(recorded);
  const incompatible =
    reconciled === null ? ["an unexpected V2 preview history"] : skippedV2Migrations(reconciled);
  if (incompatible.length > 0) {
    return {
      status: "blocked",
      version,
      reason: `This database has incompatible migration history: ${incompatible.join(", ")}. Start the build that wrote it, or report this migration history so an upgrade path can be added.`,
    };
  }

  const plan = readMigrationPlan({ databasePath: input.databasePath,
    upstream: migrationManifest, jones: jonesMigrationEntries.map(([id, name]) => [id, name]) });
  if (plan.status === "blocked") return { status: "blocked", version, reason: plan.reason };
  if (input.migrationPlanResult?.status === "blocked") return { status: "blocked", version, reason: input.migrationPlanResult.reason };
  return { status: "ready", version, launcherProtocol,
    ...(input.migrationPlanResult?.status === "ready" ? { migrationPlan: input.migrationPlanResult.migrationPlan } : {}),
    ...(input.startupGateProtocol === undefined ? {} : { startupGateProtocol: input.startupGateProtocol }),
    ...(input.qualifiedRecoveryProtocol === undefined ? {} : { qualifiedRecoveryProtocol: input.qualifiedRecoveryProtocol }),
  };
}

export function decodeServicePreflightResult(
  value: unknown,
  launcherProtocol: SupportedLauncherProtocol = SERVICE_LAUNCHER_PROTOCOL,
): ServicePreflightResult | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  const record = value as Record<string, unknown>;
  const migrationPlan = "migrationPlan" in record ? decodeMigrationPlan(record.migrationPlan) : undefined;
  if (("migrationPlan" in record && migrationPlan === undefined) ||
      ("startupGateProtocol" in record && record.startupGateProtocol !== 1) ||
      ("qualifiedRecoveryProtocol" in record && record.qualifiedRecoveryProtocol !== 1)) return undefined;
  if (
    record.status === "ready" &&
    record.launcherProtocol === launcherProtocol &&
    typeof record.version === "string"
  ) {
    return { status: "ready", version: record.version, launcherProtocol,
      ...(migrationPlan === undefined ? {} : { migrationPlan }),
      ...(record.startupGateProtocol === undefined ? {} : { startupGateProtocol: 1 as const }),
      ...(record.qualifiedRecoveryProtocol === undefined ? {} : { qualifiedRecoveryProtocol: 1 as const }),
    };
  }
  if (
    record.status === "blocked" &&
    typeof record.version === "string" &&
    typeof record.reason === "string"
  ) {
    return { status: "blocked", version: record.version, reason: record.reason };
  }
  return undefined;
}
