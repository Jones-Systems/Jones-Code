// One gate for CLI reports. A truthy string, absent predicate or empty set never qualifies.
export function qualificationDecision(predicates, required = []) {
  const values = { ...Object.fromEntries(required.map((name) => [name, "unknown"])), ...predicates };
  const entries = Object.entries(values);
  if (entries.length === 0) return { outcome: "unknown", passed: false, failed: [], unknown: ["predicates"] };
  const failed = entries.filter(([, value]) => value === false).map(([name]) => name);
  const unknown = entries.filter(([, value]) => value !== true && value !== false).map(([name]) => name);
  return { outcome: failed.length ? "failed" : unknown.length ? "unknown" : "passed",
    passed: failed.length === 0 && unknown.length === 0, failed, unknown };
}

export const requiredNativePredicates = ["productiveOverlap", "serializedNegativeControl", "normalTargets", "writerTarget",
  "writerP95Target", "loopComparison", "shutdownProved", "noSteadyChurn", "churnFailuresAbsent", "resourcesComplete",
  "observationComplete", "lifecycleBounds", "guardianResourcesObserved", "aggregateResourceBudget", "childRssWithinBudget", "churnThroughput"];
