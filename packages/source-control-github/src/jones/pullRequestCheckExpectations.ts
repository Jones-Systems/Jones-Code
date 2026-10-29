import type { PullRequestCheckObservation } from "@t3tools/contracts";

export interface ObservedWorkflow {
  readonly id: number;
  readonly workflow_id: number;
  readonly check_suite_id: number;
  readonly run_number: number;
  readonly run_attempt: number;
  readonly head_sha: string;
  readonly head_branch: string;
  readonly path: string;
  readonly name: string;
  readonly event: string;
  readonly status: string;
  readonly conclusion: string | null;
}

export interface ObservedCheckRun {
  readonly id: number;
  readonly name: string;
  readonly head_sha: string;
  readonly status: string;
  readonly conclusion: string | null;
  readonly details_url: string | null;
  readonly check_suite: { readonly id: number };
  readonly app?: { readonly id: number; readonly slug: string } | null | undefined;
}

export function expectedRequiredChecks(input: {
  readonly host: string;
  readonly repository: string;
  readonly baseBranch: string;
}): ReadonlyArray<string> | null {
  // Existing Jones-Code policy: contributor-guidance.md and ci.yml's required Check job.
  // This binding supplies no expectations for other repositories, hosts or target refs.
  return input.host.toLowerCase() === "github.com" &&
    input.repository.toLowerCase() === "jones-systems/jones-code" &&
    input.baseBranch === "main"
    ? ["Check"]
    : null;
}

export function requiredCheckState(input: {
  readonly expected: ReadonlyArray<string> | null;
  readonly headSha: string;
  readonly headBranch: string;
  readonly runs: ReadonlyArray<ObservedWorkflow>;
  readonly checks: ReadonlyArray<ObservedCheckRun>;
  readonly complete: boolean;
}): PullRequestCheckObservation["requiredState"] {
  if (input.expected === null || !input.complete) return "unknown";
  const ci = input.runs.filter(
    (run) =>
      run.head_sha === input.headSha &&
      run.head_branch === input.headBranch &&
      run.event === "pull_request" &&
      run.name === "CI" &&
      run.path.split("@")[0] === ".github/workflows/ci.yml",
  );
  if (ci.length === 0) return "missing";
  if (ci.length !== 1) return "unknown";
  const run = ci[0]!;
  const gates = input.expected.map((name) =>
    input.checks.filter(
      (check) =>
        check.head_sha === input.headSha &&
        check.check_suite.id === run.check_suite_id &&
        check.name === name &&
        check.app?.slug === "github-actions",
    ),
  );
  if (gates.some((matches) => matches.length !== 1)) return "missing";
  if (gates.some(([check]) => check!.status !== "completed")) return "pending";
  if (gates.some(([check]) => check!.conclusion !== "success")) return "failure";
  // A completed Check cannot vouch for a still-running or rerun CI workflow.
  return run.status === "completed" ? "success" : "pending";
}
