import { nativeBlockReceiptPredicate } from "./native-block-proof.mjs";

// One gate for CLI reports. A truthy string, absent predicate or empty set never qualifies.
export function qualificationDecision(predicates, required = []) {
  const values = {
    ...Object.fromEntries(required.map((name) => [name, "unknown"])),
    ...predicates,
  };
  const entries = Object.entries(values);
  if (entries.length === 0)
    return { outcome: "unknown", passed: false, failed: [], unknown: ["predicates"] };
  const failed = entries.filter(([, value]) => value === false).map(([name]) => name);
  const unknown = entries
    .filter(([, value]) => value !== true && value !== false)
    .map(([name]) => name);
  return {
    outcome: failed.length ? "failed" : unknown.length ? "unknown" : "passed",
    passed: failed.length === 0 && unknown.length === 0,
    failed,
    unknown,
  };
}

export const requiredNativePredicates = [
  "productiveOverlap",
  "serializedNegativeControl",
  "normalTargets",
  "writerTarget",
  "writerP95Target",
  "loopComparison",
  "shutdownProved",
  "noSteadyChurn",
  "churnFailuresAbsent",
  "resourcesComplete",
  "observationComplete",
  "lifecycleBounds",
  "guardianResourcesObserved",
  "aggregateResourceBudget",
  "childRssWithinBudget",
  "churnThroughput",
];

/** Exact receipt fan-in, independently of subprocess success. Per-OS CI requires all eight cases. */
export function nativeBlockMatrixDecision(receipts, expected) {
  if (
    !Array.isArray(receipts) ||
    !Array.isArray(expected) ||
    ![8, 16].includes(expected.length) ||
    expected.some(
      (value) =>
        !value ||
        !["linux-x64", "macos-arm64"].includes(value.os) ||
        !["source", "bundle", "electron", "sea"].includes(value.form) ||
        !["laptop", "vps"].includes(value.profile) ||
        !/^[a-f0-9]{64}$/.test(value.build ?? ""),
    )
  )
    return { predicate: "unknown", cases: [] };
  const oses = [...new Set(expected.map((value) => value.os))];
  if (oses.some((os) => expected.filter((value) => value.os === os).length !== 8))
    return { predicate: "unknown", cases: [] };
  const key = (value) => JSON.stringify([value.os, value.form, value.profile]);
  if (new Set(expected.map(key)).size !== expected.length)
    return { predicate: "unknown", cases: [] };
  const cases = expected.map((binding) => {
    const matches = receipts.filter(
      (receipt) => receipt?.proof && key(receipt.proof) === key(binding),
    );
    const predicate =
      matches.length === 1 &&
      matches[0].artifactsVerified === true &&
      matches[0].outcome === "passed" &&
      matches[0].predicate === true &&
      matches[0].skipped !== true
        ? nativeBlockReceiptPredicate(matches[0].proof, binding)
        : "unknown";
    return { ...binding, predicate };
  });
  const unexpected = receipts.some(
    (receipt) =>
      !receipt?.proof || !expected.some((binding) => key(binding) === key(receipt.proof)),
  );
  return {
    predicate: unexpected
      ? "unknown"
      : cases.some((value) => value.predicate === false)
        ? false
        : cases.every((value) => value.predicate === true)
          ? true
          : "unknown",
    cases,
  };
}
