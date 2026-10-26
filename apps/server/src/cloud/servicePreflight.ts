import packageJson from "../../package.json" with { type: "json" };
import { SERVICE_LAUNCHER_PROTOCOL } from "./serviceProtocol.ts";
import {
  decodeMigrationPlan,
  type MigrationPlan,
  type MigrationPlanResult,
} from "../jones/updates/migrationPlan.ts";

export type ServicePreflightResult =
  | {
      readonly status: "ready";
      readonly version: string;
      readonly launcherProtocol: typeof SERVICE_LAUNCHER_PROTOCOL;
      readonly startupGateProtocol?: 1;
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
  };
}

export function decodeServicePreflightResult(value: unknown): ServicePreflightResult | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }
  const record = value as Record<string, unknown>;
  const migrationPlan =
    "migrationPlan" in record ? decodeMigrationPlan(record.migrationPlan) : undefined;
  if ("migrationPlan" in record && migrationPlan === undefined) return undefined;
  if ("startupGateProtocol" in record && record.startupGateProtocol !== 1) return undefined;
  if (
    record.status === "ready" &&
    record.launcherProtocol === SERVICE_LAUNCHER_PROTOCOL &&
    typeof record.version === "string"
  ) {
    return {
      status: "ready",
      version: record.version,
      launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
      ...(migrationPlan === undefined ? {} : { migrationPlan }),
      ...(record.startupGateProtocol === undefined ? {} : { startupGateProtocol: 1 as const }),
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

export function qualifiedServicePreflightFailure(input: {
  readonly code: number | null;
  readonly stdout: string;
  readonly version: string;
}): string | undefined {
  let result: ServicePreflightResult | undefined;
  try {
    result = decodeServicePreflightResult(JSON.parse(input.stdout.trim()) as unknown);
  } catch {
    return "startup-gate-unavailable: The candidate returned an invalid service preflight.";
  }
  if (result?.status === "blocked" && result.version === input.version) return result.reason;
  if (input.code !== 0 || result?.status !== "ready" || result.version !== input.version)
    return "startup-gate-unavailable: The candidate did not pass its bound service preflight.";
  if (result.startupGateProtocol !== 1)
    return "startup-gate-unavailable: The candidate does not affirm the symmetric qualified startup gate.";
  return undefined;
}
