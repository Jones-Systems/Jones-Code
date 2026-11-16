// Decodes a candidate's `service-preflight` output. The service launcher reads this, so it must not
// import the preflight itself: that pulls the migration manifest, Effect and the contracts into the
// executable bootstrap, which `scripts/cli.ts` holds under 1 MiB.
import { SERVICE_LAUNCHER_PROTOCOL } from "../../cloud/serviceProtocol.ts";
import { decodeMigrationPlan, type MigrationPlan } from "../updates/migrationPlan.ts";

export type ServicePreflightResult =
  | {
      readonly status: "ready";
      readonly version: string;
      readonly launcherProtocol: typeof SERVICE_LAUNCHER_PROTOCOL;
      readonly startupGateProtocol?: 1;
      readonly qualifiedRecoveryProtocol?: 1;
      readonly migrationPlan?: MigrationPlan;
    }
  | {
      readonly status: "blocked";
      readonly version: string;
      readonly reason: string;
    };

export function decodeServicePreflightResult(value: unknown): ServicePreflightResult | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }
  const record = value as Record<string, unknown>;
  const migrationPlan =
    "migrationPlan" in record ? decodeMigrationPlan(record.migrationPlan) : undefined;
  if ("migrationPlan" in record && migrationPlan === undefined) return undefined;
  if ("startupGateProtocol" in record && record.startupGateProtocol !== 1) return undefined;
  if ("qualifiedRecoveryProtocol" in record && record.qualifiedRecoveryProtocol !== 1)
    return undefined;
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
      ...(record.qualifiedRecoveryProtocol === undefined
        ? {}
        : { qualifiedRecoveryProtocol: 1 as const }),
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
  readonly requiredQualifiedRecoveryProtocol?: 1;
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
  if (
    input.requiredQualifiedRecoveryProtocol !== undefined &&
    result.qualifiedRecoveryProtocol !== input.requiredQualifiedRecoveryProtocol
  )
    return "qualified-recovery-unsupported: The candidate cannot read historical enrollment receipts.";
  return undefined;
}
