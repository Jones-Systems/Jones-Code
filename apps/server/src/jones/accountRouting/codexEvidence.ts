import type { QualifiedQuota, QualifiedQuotaMap } from "@t3tools/contracts";

import { readAccountRoutingQuotaProof } from "../../provider/qualifiedQuota.ts";
import {
  blockingReset,
  currentObservation,
  validPercent,
  type AccountAllowance,
  type AllowanceWindow,
} from "./policy.ts";

const WEEK_IN_MINUTES = 7 * 24 * 60;

function resetMillis(seconds: number | null | undefined): number | null {
  // Bound the conversion to the range supported by Date.
  return seconds != null && seconds > 0 && seconds <= 8.64e12 ? seconds * 1_000 : null;
}

function bucketApplies(id: string, bucket: QualifiedQuotaMap[string], model: string): boolean {
  const limitId = bucket.limitId ?? id;
  return (
    limitId === "codex" ||
    bucket.normalModelSlug === model ||
    (bucket.normalModelSlug == null && limitId === model)
  );
}

export function codexAllowance(
  quota: QualifiedQuota,
  model: string,
  now: number,
): AccountAllowance | undefined {
  const proof = readAccountRoutingQuotaProof(quota);
  const observedAt = Date.parse(quota.quotaReceivedAt ?? "");
  if (!proof || !currentObservation(observedAt, now)) return undefined;

  const blockers: Array<number | null> = [];
  const weekly: Array<AllowanceWindow> = [];
  let hasGeneralBucket = false;
  let complete = true;
  for (const [id, bucket] of Object.entries(quota.rateLimitsByLimitId ?? {})) {
    if (!bucketApplies(id, bucket, model)) continue;
    hasGeneralBucket ||= (bucket.limitId ?? id) === "codex";
    const windows = [bucket.primary, bucket.secondary].filter((window) => window != null);
    if (windows.length === 0) complete = false;
    const bucketBlockers: Array<number | null> = [];
    for (const window of windows) {
      const resetsAt = resetMillis(window.resetsAt);
      const used = window.usedPercent;
      const mins = window.windowDurationMins;
      const active = resetsAt !== null && resetsAt > now;
      if (used === 100 && (resetsAt === null || active)) bucketBlockers.push(resetsAt);
      if (!validPercent(used) || !active || mins == null || !Number.isFinite(mins) || mins <= 0) {
        complete = false;
        continue;
      }
      if (mins === WEEK_IN_MINUTES)
        weekly.push({ usedPercent: used, resetsAt, durationMs: mins * 60_000 });
    }
    let individualBlocked = false;
    if (bucket.individualLimit != null) {
      const reset = resetMillis(bucket.individualLimit.resetsAt);
      if (bucket.individualLimit.remainingPercent === 0 && (reset === null || reset > now)) {
        bucketBlockers.push(reset);
        individualBlocked = true;
      }
      complete = false;
    }
    if (bucket.spendControlReached && !individualBlocked) bucketBlockers.push(null);
    if (
      bucket.rateLimitReachedType != null &&
      (bucket.rateLimitReachedType !== "rate_limit_reached" || bucketBlockers.length === 0)
    )
      bucketBlockers.push(null);
    blockers.push(...bucketBlockers);
  }

  if (blockers.length > 0 || !proof.ordinaryUsageAllowed) {
    return { status: "blocked", resetsAt: blockingReset(blockers) };
  }
  // A bucket for another model can make the complete map unqualified. Here
  // the same private observation must supply every applicable active window.
  if (
    !hasGeneralBucket ||
    !complete ||
    (quota.status !== "qualified" && quota.failureCode !== "incomplete_windows")
  )
    return { status: "unknown" };
  return { status: "eligible", weekly };
}
