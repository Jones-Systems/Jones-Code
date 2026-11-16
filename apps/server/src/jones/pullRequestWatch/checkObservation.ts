import type { PullRequestCheck, PullRequestCheckObservation } from "@t3tools/contracts";

const failedCheck = (check: PullRequestCheck) =>
  check.status === "failure" || check.status === "cancelled" || check.status === "action-required";

export function observationInFlight(observation: PullRequestCheckObservation | undefined): boolean {
  return (
    observation !== undefined &&
    (!observation.enumerationComplete ||
      observation.workflowState !== "complete" ||
      (observation.requiredSetKnown &&
        ["unknown", "missing", "pending"].includes(observation.requiredState)))
  );
}

export interface ObservedPass {
  readonly kind: "checks-passed";
  readonly count: number;
  readonly required: boolean;
  readonly observed: {
    readonly successful: number;
    readonly skipped: number;
    readonly neutral: number;
  };
}

export function evaluateObservedChecks(input: {
  readonly headSha: string | null;
  readonly checks: ReadonlyArray<PullRequestCheck>;
  readonly observation: PullRequestCheckObservation;
  readonly failedChecks: ReadonlyArray<string>;
  readonly passed: boolean;
  readonly passedChecks: ReadonlyArray<string>;
  readonly owedChecks: ReadonlyArray<string>;
}) {
  const state = classifyCheckObservation(input.headSha, input.checks, input.observation);
  const failures = state.sameHead ? input.checks.filter(failedCheck) : [];
  const newlyFailed = failures.filter((check) => !input.failedChecks.includes(check.name));
  // Absence in a partial read says nothing about a previously observed result.
  const failedChecks = state.complete
    ? failures.map((check) => check.name)
    : [...new Set([...input.failedChecks, ...failures.map((check) => check.name)])];
  const names = input.checks
    .filter((check) => check.status !== "pending")
    .map((check) => check.name);
  const grew = names.some((name) => !input.passedChecks.includes(name));
  const pass: ObservedPass | undefined =
    state.ready && (!input.passed || grew)
      ? {
          kind: "checks-passed",
          count: state.required ? state.gate.length : names.length,
          required: state.required,
          observed: {
            successful: state.successful,
            skipped: state.skipped,
            neutral: state.neutral,
          },
        }
      : undefined;
  const settled = state.complete && input.observation.workflowState === "complete";
  const pending =
    state.complete &&
    (input.observation.workflowState === "pending" ||
      input.checks.some((check) => check.status === "pending"));
  return {
    newlyFailed,
    pass,
    failedChecks,
    passed: state.ready ? true : settled || pending ? false : input.passed,
    passedChecks: state.ready ? names : settled || pending ? [] : input.passedChecks,
    owedChecks: settled ? [] : input.owedChecks,
  };
}

export function classifyCheckObservation(
  headSha: string | null,
  checks: ReadonlyArray<PullRequestCheck>,
  observation: PullRequestCheckObservation,
) {
  const sameHead = headSha !== null && observation.headSha === headSha;
  const complete = sameHead && observation.enumerationComplete;
  const required = observation.requiredSetKnown;
  const gate = required ? observation.requiredChecks : checks.map((check) => check.name);
  const successful = checks.filter((check) => check.status === "success").length;
  return {
    sameHead,
    complete,
    required,
    gate,
    successful,
    skipped: checks.filter((check) => check.status === "skipped").length,
    neutral: checks.filter((check) => check.status === "neutral").length,
    ready:
      complete &&
      (required
        ? observation.requiredState === "success" && gate.length > 0
        : observation.workflowState === "complete" &&
          successful > 0 &&
          checks.every((check) => check.status !== "pending" && !failedCheck(check))),
  };
}
