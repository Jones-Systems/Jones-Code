import * as NodeAssert from "node:assert";

export function classifyNativeFailure(phase, error, assertions = [], steps = []) {
  if (phase !== "scenario") return "unknown";
  const failed = assertions.filter((item) => item.status === "failed");
  if (
    error?.constructor === Error &&
    failed.length === 1 &&
    error.message === `Scenario assertion failed: ${failed[0].name}`
  )
    return "assertion-failed";
  return error instanceof NodeAssert.AssertionError &&
    error.code === "ERR_ASSERTION" &&
    failed.length === 0 &&
    steps.some((step) => step.status === "failed")
    ? "assertion-failed"
    : "unknown";
}
