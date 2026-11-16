import type { ShellDestinationFence } from "@t3tools/contracts/jones/shellDestinationFence";

export interface DestinationEnvironment {
  readonly environmentId: string;
  readonly label: string;
  readonly serverLabel?: string | undefined;
  readonly phase: string;
  readonly cwd: string | null;
  readonly canOperate: boolean;
}

export type DestinationStatus =
  | "invalid"
  | "self"
  | "ready"
  | "connecting"
  | "offline"
  | "no-permission"
  | "unknown"
  | "label-mismatch";

export interface ShellDestination {
  readonly status: DestinationStatus;
  readonly environmentId: string | null;
  readonly label: string;
  readonly claimedLabel?: string | undefined;
  readonly reason: string | null;
  readonly cwd: string | null;
}

function normalizeLabel(label: string): string {
  return label
    .normalize("NFKC")
    .replace(/[‘’ʼ]/gu, "'")
    .toLowerCase()
    .trim()
    .replace(/\s+/gu, " ");
}

export function resolveShellDestination(
  fence: ShellDestinationFence,
  sourceEnvironmentId: string,
  environments: readonly DestinationEnvironment[],
): ShellDestination {
  if (fence._tag === "Invalid") {
    return {
      status: "invalid",
      environmentId: null,
      label: "Invalid destination",
      reason: fence.reason,
      cwd: null,
    };
  }
  const environment = environments.find((entry) => entry.environmentId === fence.environmentId);
  const base = {
    environmentId: fence.environmentId,
    label: environment?.label ?? "Unknown computer",
    claimedLabel: fence.label,
    cwd: environment?.cwd ?? null,
  };
  if (!environment)
    return {
      ...base,
      status: "unknown",
      reason:
        "This exact environment ID is not in this client's connections. The supplied label is an agent claim.",
    };
  if (
    fence.label &&
    ![environment.label, environment.serverLabel].some(
      (label) => label && normalizeLabel(label) === normalizeLabel(fence.label!),
    )
  ) {
    return {
      ...base,
      status: "label-mismatch",
      reason: `Destination label mismatch: connected computer “${environment.label}”; agent claimed “${fence.label}”.`,
    };
  }
  if (
    ["connecting", "reconnecting"].includes(environment.phase) ||
    (environment.phase === "connected" && !environment.cwd)
  ) {
    return {
      ...base,
      status: "connecting",
      reason: "Waiting for this computer's connection and server configuration.",
    };
  }
  if (environment.phase !== "connected")
    return {
      ...base,
      status: "offline",
      reason: "This computer is offline or unavailable. Connect it before running.",
    };
  if (!environment.canOperate)
    return {
      ...base,
      status: "no-permission",
      reason: "This connection does not grant terminal control.",
    };
  return {
    ...base,
    status: fence.environmentId === sourceEnvironmentId ? "self" : "ready",
    reason: null,
  };
}

export function shellCommandEligibility(
  code: string,
  complete: boolean,
): { command: string; reason: string | null } {
  const command = code.trim();
  const reason = !complete
    ? "Wait for the completed, closed code block."
    : !code.endsWith("\n")
      ? "The command must end with a newline."
      : !command
        ? "The command is empty."
        : command.endsWith("\\")
          ? "A trailing shell continuation cannot be run."
          : /[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u.test(code.slice(0, -1))
            ? "Run supports one command line without control or hidden format characters."
            : command.length + 1 > 65_535
              ? "The command exceeds the terminal write limit."
              : null;
  return { command, reason };
}

export function hasLegacyDestinationMetadata(
  language: string,
  meta: string | null | undefined,
): boolean {
  return (
    /^(?:sh|bash|zsh|fish|shell|powershell|pwsh)$/.test(language) &&
    /(?:^|\s)environment\s*=/u.test(meta ?? "")
  );
}
