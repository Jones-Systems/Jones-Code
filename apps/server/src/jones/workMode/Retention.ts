import type {
  OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  ProviderSessionId,
} from "@t3tools/contracts";
import { WORK_MODE_INTERVAL_MS } from "@t3tools/shared/jones/workMode";
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

  // Residency must survive the default 30-minute eviction before dispatch is
  // due at 55 minutes. Only the inactivity threshold uses this advanced clock;
  // snooze remains governed by the real clock above.
  const eligibilityNowMs = nowMs + WORK_MODE_INTERVAL_MS;
  if (workModeCandidate(shell, eligibilityNowMs) === null) return false;
  const owner = workModeContext(projection, eligibilityNowMs);
  return (
    owner !== null &&
    owner.providerSessionId === input.providerSessionId &&
    owner.providerInstanceId === input.providerInstanceId
  );
}
