import type { QualifiedQuota, QualifiedQuotaMap } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Schema from "effect/Schema";

import {
  makeAccountRoutingQuota,
  readAccountRoutingQuotaProof,
} from "../../provider/qualifiedQuota.ts";
import {
  blockingReset,
  currentObservation,
  validPercent,
  type AccountAllowance,
  type AllowanceWindow,
} from "./policy.ts";

const WEEK_IN_MINUTES = 7 * 24 * 60;
type QuotaBucket = QualifiedQuotaMap[string];
const legacyBuckets = new WeakMap<QualifiedQuota, QuotaBucket>();

/** Positive proof of the selected account with no ordinary usage available. */
export class CodexRoutedAccountUsageLimit extends Schema.TaggedError<CodexRoutedAccountUsageLimit>()(
  "CodexRoutedAccountUsageLimit",
  {},
) {
  override get message(): string {
    return "The verified Codex subscription has reached its usage limit.";
  }
}

const isCodexRoutedAccountUsageLimit = Schema.is(CodexRoutedAccountUsageLimit);

export function routedCodexUsageFailure(cause: unknown): CodexRoutedAccountUsageLimit | undefined {
  for (let depth = 0; depth < 16; depth++) {
    if (isCodexRoutedAccountUsageLimit(cause)) return new CodexRoutedAccountUsageLimit();
    if (Cause.isCause(cause)) cause = Cause.squash(cause);
    else if (typeof cause === "object" && cause !== null && "cause" in cause) cause = cause.cause;
    else break;
  }
  return undefined;
}

export function makeCodexRoutingQuota(
  input: Parameters<typeof makeAccountRoutingQuota>[0] & { readonly legacyRateLimit: QuotaBucket },
): QualifiedQuota {
  const quota = makeAccountRoutingQuota(input);
  legacyBuckets.set(quota, input.legacyRateLimit);
  return quota;
}

function resetMillis(seconds: number | null | undefined): number | null {
  // Bound the conversion to the range supported by Date.
  return seconds != null && seconds > 0 && seconds <= 8.64e12 ? seconds * 1_000 : null;
}

function bucketApplies(id: string, bucket: QuotaBucket, model: string): boolean {
  return (
    id === "codex" ||
    bucket.limitId === "codex" ||
    id === model ||
    bucket.limitId === model ||
    bucket.normalModelSlug === model
  );
}

function bucketBlockers(bucket: QuotaBucket, now: number): ReadonlyArray<number | null> {
  const blockers: Array<number | null> = [];
  for (const window of [bucket.primary, bucket.secondary]) {
    if (window?.usedPercent !== 100) continue;
    const reset = resetMillis(window.resetsAt);
    if (reset === null || reset > now) blockers.push(reset);
  }
  const individualReset = resetMillis(bucket.individualLimit?.resetsAt);
  const individualBlocked =
    bucket.individualLimit?.remainingPercent === 0 &&
    (individualReset === null || individualReset > now);
  if (individualBlocked) blockers.push(individualReset);
  if (bucket.spendControlReached && !individualBlocked) blockers.push(null);
  if (
    bucket.rateLimitReachedType != null &&
    (bucket.rateLimitReachedType !== "rate_limit_reached" || blockers.length === 0)
  ) {
    blockers.push(null);
  }
  return blockers;
}

function conflictingBucketEvidence(a: QuotaBucket, b: QuotaBucket): boolean {
  const disagree = (left: unknown, right: unknown) =>
    left != null && right != null && left !== right;
  return (
    disagree(a.normalModelSlug, b.normalModelSlug) ||
    disagree(a.planType, b.planType) ||
    disagree(a.spendControlReached, b.spendControlReached) ||
    disagree(a.rateLimitReachedType, b.rateLimitReachedType) ||
    disagree(a.individualLimit?.remainingPercent, b.individualLimit?.remainingPercent) ||
    disagree(a.individualLimit?.resetsAt, b.individualLimit?.resetsAt) ||
    (["primary", "secondary"] as const).some(
      (key) =>
        disagree(a[key]?.usedPercent, b[key]?.usedPercent) ||
        disagree(a[key]?.resetsAt, b[key]?.resetsAt) ||
        disagree(a[key]?.windowDurationMins, b[key]?.windowDurationMins),
    )
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

  const legacy = legacyBuckets.get(quota);
  const legacyId = legacy?.limitId ?? "codex";
  const legacyApplies = legacy !== undefined && bucketApplies(legacyId, legacy, model);
  const blockers = legacyApplies ? [...bucketBlockers(legacy, now)] : [];
  const weekly: Array<AllowanceWindow> = [];
  let hasGeneralBucket = false;
  let complete = true;
  let conflicting = false;
  for (const [id, bucket] of Object.entries(quota.rateLimitsByLimitId ?? {})) {
    const duplicatedLegacy =
      legacy !== undefined && (id === legacyId || bucket.limitId === legacyId);
    if (!bucketApplies(id, bucket, model) && !(duplicatedLegacy && legacyApplies)) continue;
    conflicting ||=
      (bucket.limitId != null && bucket.limitId !== id) ||
      (duplicatedLegacy && conflictingBucketEvidence(legacy, bucket));
    hasGeneralBucket ||= id === "codex";
    const windows = [bucket.primary, bucket.secondary].filter((window) => window != null);
    if (windows.length === 0) complete = false;
    for (const window of windows) {
      const resetsAt = resetMillis(window.resetsAt);
      const used = window.usedPercent;
      const mins = window.windowDurationMins;
      const active = resetsAt !== null && resetsAt > now;
      if (!validPercent(used) || !active || mins == null || !Number.isFinite(mins) || mins <= 0) {
        complete = false;
        continue;
      }
      if (mins === WEEK_IN_MINUTES)
        weekly.push({ usedPercent: used, resetsAt, durationMs: mins * 60_000 });
    }
    if (bucket.individualLimit != null) complete = false;
    blockers.push(...bucketBlockers(bucket, now));
  }

  if (blockers.length > 0 || !proof.ordinaryUsageAllowed) {
    return { status: "blocked", resetsAt: blockingReset(blockers) };
  }
  if (conflicting) return undefined;
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
