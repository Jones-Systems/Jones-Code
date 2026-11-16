import type {
  OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  ProviderSessionId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { threadShellFromProjection } from "../../orchestration-v2/ProjectionStore.ts";
import { workModeCandidate, workModeContext } from "./Policy.ts";

export const WORK_MODE_RETENTION_RECHECK_MS = 60_000;

export function workModeRetainsSession(input: {
  readonly projection: OrchestrationV2ThreadProjection;
  readonly providerSessionId: ProviderSessionId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly nowMs: number;
}): boolean {
  const { projection, nowMs } = input;
  const shell = threadShellFromProjection(projection);
  if (
    !Number.isFinite(nowMs) ||
    shell.forkedFrom != null ||
    shell.lastErrorClass != null ||
    (shell.snoozedUntil != null && DateTime.toEpochMillis(shell.snoozedUntil) > nowMs)
  )
    return false;

  // Keep eligible sessions resident before dispatch is due, using the real clock
  // so the initial age guard and snooze cannot be advanced by retention.
  if (workModeCandidate(shell, nowMs) === null) return false;
  const owner = workModeContext(projection, nowMs, { ignoreInterval: true });
  return (
    owner !== null &&
    owner.providerSessionId === input.providerSessionId &&
    owner.providerInstanceId === input.providerInstanceId
  );
}
