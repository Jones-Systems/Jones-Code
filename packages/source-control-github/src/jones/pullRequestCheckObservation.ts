import type { PullRequestCheck, PullRequestCheckObservation } from "@t3tools/contracts";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { GitHubRest } from "../server/GitHubRest.ts";
import { expectedRequiredChecks, requiredCheckState } from "./pullRequestCheckExpectations.ts";

const CheckRun = Schema.Struct({
  id: Schema.Number,
  name: Schema.String,
  head_sha: Schema.String,
  status: Schema.String,
  conclusion: Schema.NullOr(Schema.String),
  details_url: Schema.NullOr(Schema.String),
  check_suite: Schema.Struct({ id: Schema.Number }),
  app: Schema.optional(Schema.NullOr(Schema.Struct({ id: Schema.Number, slug: Schema.String }))),
});
const Status = Schema.Struct({
  id: Schema.Number,
  context: Schema.String,
  state: Schema.String,
  description: Schema.NullOr(Schema.String),
  target_url: Schema.NullOr(Schema.String),
});
const Workflow = Schema.Struct({
  id: Schema.Number,
  workflow_id: Schema.Number,
  check_suite_id: Schema.Number,
  run_number: Schema.Number,
  run_attempt: Schema.Number,
  head_sha: Schema.String,
  head_branch: Schema.String,
  path: Schema.String,
  name: Schema.String,
  event: Schema.String,
  status: Schema.String,
  conclusion: Schema.NullOr(Schema.String),
});
const Head = Schema.Struct({
  head: Schema.Struct({ sha: Schema.String, ref: Schema.String }),
  base: Schema.Struct({ ref: Schema.String, repo: Schema.Struct({ full_name: Schema.String }) }),
});
const CheckPage = Schema.Struct({ total_count: Schema.Number, check_runs: Schema.Array(CheckRun) });
const WorkflowPage = Schema.Struct({
  total_count: Schema.Number,
  workflow_runs: Schema.Array(Workflow),
});

class CheckObservationChanged extends Data.TaggedError("CheckObservationChanged")<{
  readonly message: string;
}> {}

export interface CheckObservationInput {
  readonly host: string;
  readonly repository: string;
  readonly number: number;
  readonly headSha?: string | null | undefined;
  readonly baseBranch?: string | undefined;
  readonly headBranch?: string | undefined;
  readonly checks: ReadonlyArray<PullRequestCheck>;
}

export interface CheckObservationResult {
  readonly headSha: string;
  readonly checks: ReadonlyArray<PullRequestCheck>;
  readonly jonesCheckObservation: PullRequestCheckObservation;
}

function latest<T>(
  rows: ReadonlyArray<T>,
  key: (row: T) => string,
  id: (row: T) => number,
): Array<T> {
  const selected = new Map<string, T>();
  for (const row of rows) {
    const previous = selected.get(key(row));
    if (previous === undefined || id(row) > id(previous)) selected.set(key(row), row);
  }
  return [...selected.values()];
}

function checkStatus(status: string, conclusion: string | null): PullRequestCheck["status"] {
  if (status !== "completed") return "pending";
  switch (conclusion) {
    case "success":
    case "skipped":
    case "neutral":
    case "cancelled":
      return conclusion;
    case "action_required":
      return "action-required";
    case "failure":
    case "timed_out":
    case "startup_failure":
      return "failure";
    default:
      return "pending";
  }
}

/** Uses the API's existing REST policy; partial pages can prove failure, never completion. */
export const makeCheckObservationReader = Effect.gen(function* () {
  const api = yield* GitHubRest;
  return (input: CheckObservationInput) =>
    Effect.gen(function* () {
      const root = `repos/${input.repository}`;
      const read = <A>(path: string, schema: Schema.Decoder<A>) =>
        api.rest({ host: input.host, operation: "getPullRequestCheckObservation", path }).pipe(
          Effect.map((response) =>
            response.status === 200 && !response.truncated && !response.invalidUtf8
              ? Schema.decodeOption(Schema.fromJsonString(schema))(response.body)
              : Option.none<A>(),
          ),
          Effect.catchIf(
            (error) =>
              error._tag !== "GitHubApiRateLimitError" &&
              error._tag !== "SourceControlRateLimitPausedError",
            () => Effect.succeed(Option.none<A>()),
          ),
        );
      const head = yield* read(`${root}/pulls/${input.number}`, Head);
      if (
        Option.isNone(head) ||
        head.value.base.repo.full_name.toLowerCase() !== input.repository.toLowerCase() ||
        (input.headSha != null && head.value.head.sha !== input.headSha) ||
        (input.baseBranch !== undefined && head.value.base.ref !== input.baseBranch) ||
        (input.headBranch !== undefined && head.value.head.ref !== input.headBranch)
      ) {
        return yield* new CheckObservationChanged({
          message: "Pull request head could not be pinned.",
        });
      }
      const pinned = head.value;
      const sha = pinned.head.sha;
      const collect = Effect.fn("GitHubCheckObservation.collect")(function* <A, B>(
        path: string,
        schema: Schema.Decoder<B>,
        rows: (page: B) => ReadonlyArray<A>,
        total: (page: B) => number | null,
        cap: number,
      ) {
        const all: Array<A> = [];
        for (let page = 1; page <= cap; page++) {
          const result = yield* read(
            `${path}${path.includes("?") ? "&" : "?"}per_page=100&page=${page}`,
            schema,
          );
          if (Option.isNone(result)) return { rows: all, complete: false };
          const items = rows(result.value);
          all.push(...items);
          const count = total(result.value);
          if (items.length < 100 || (count !== null && all.length >= count))
            return {
              rows: all,
              complete: count === null || (count < cap * 100 && all.length === count),
            };
        }
        return { rows: all, complete: false };
      });
      const checks = yield* collect(
        `${root}/commits/${sha}/check-runs?filter=all`,
        CheckPage,
        (p) => p.check_runs,
        (p) => p.total_count,
        20,
      );
      const statuses = yield* collect(
        `${root}/commits/${sha}/statuses`,
        Schema.Array(Status),
        (p) => p,
        () => null,
        20,
      );
      const workflows = yield* collect(
        `${root}/actions/runs?head_sha=${encodeURIComponent(sha)}`,
        WorkflowPage,
        (p) => p.workflow_runs,
        (p) => p.total_count,
        10,
      );
      const after = yield* read(`${root}/pulls/${input.number}`, Head);
      if (
        Option.isNone(after) ||
        after.value.head.sha !== sha ||
        after.value.head.ref !== pinned.head.ref ||
        after.value.base.ref !== pinned.base.ref ||
        after.value.base.repo.full_name !== pinned.base.repo.full_name
      ) {
        return yield* new CheckObservationChanged({
          message: "Pull request changed during check enumeration.",
        });
      }
      const valid =
        checks.rows.every((check) => check.head_sha === sha) &&
        workflows.rows.every((run) => run.head_sha === sha);
      const runs = latest(
        workflows.rows.filter((run) => run.head_sha === sha),
        (run) => `${run.workflow_id}:${run.event}:${run.head_branch}`,
        (run) => run.id,
      );
      const allSuites = new Set(workflows.rows.map((run) => run.check_suite_id));
      const currentSuites = new Map(runs.map((run) => [run.check_suite_id, run]));
      const currentChecks = latest(
        checks.rows.filter(
          (check) =>
            check.head_sha === sha &&
            (!allSuites.has(check.check_suite.id) || currentSuites.has(check.check_suite.id)),
        ),
        (check) => `${check.check_suite.id}:${check.app?.id ?? 0}:${check.name}`,
        (check) => check.id,
      );
      const ambiguousWorkflow = currentChecks.some(
        (check) => check.app?.slug === "github-actions" && !currentSuites.has(check.check_suite.id),
      );
      const complete =
        valid && checks.complete && statuses.complete && workflows.complete && !ambiguousWorkflow;
      const counts = new Map<string, number>();
      for (const check of currentChecks) counts.set(check.name, (counts.get(check.name) ?? 0) + 1);
      const checkRows: Array<PullRequestCheck> = currentChecks.map((check) => {
        const run = currentSuites.get(check.check_suite.id);
        const name =
          (counts.get(check.name) ?? 0) > 1
            ? `${check.name} (${run?.path ?? `suite ${check.check_suite.id}`})`
            : check.name;
        const gql = input.checks.find((candidate) => candidate.name === check.name);
        return {
          name,
          status: checkStatus(check.status, check.conclusion),
          description: null,
          url: check.details_url,
          ...(gql?.required === undefined ? {} : { required: gql.required }),
        };
      });
      for (const status of latest(
        statuses.rows,
        (row) => row.context,
        (row) => row.id,
      )) {
        checkRows.push({
          name: counts.has(status.context) ? `${status.context} (commit status)` : status.context,
          status:
            status.state === "success"
              ? "success"
              : status.state === "error" || status.state === "failure"
                ? "failure"
                : "pending",
          description: status.description,
          url: status.target_url,
        });
      }
      if (!complete) {
        for (const check of input.checks) {
          if (!checkRows.some((row) => row.name === check.name)) checkRows.push(check);
        }
      }
      const expected = expectedRequiredChecks({ ...input, baseBranch: pinned.base.ref });
      const jonesCheckObservation: PullRequestCheckObservation = {
        headSha: sha,
        enumerationComplete: complete,
        workflowState:
          !workflows.complete || !valid || ambiguousWorkflow
            ? "unknown"
            : runs.some((run) => run.status !== "completed")
              ? "pending"
              : "complete",
        requiredSetKnown: expected !== null,
        requiredChecks: expected ?? [],
        requiredState: requiredCheckState({
          expected,
          headSha: sha,
          headBranch: pinned.head.ref,
          runs,
          checks: currentChecks,
          complete,
        }),
      };
      const result: CheckObservationResult = {
        headSha: sha,
        checks: checkRows,
        jonesCheckObservation,
      };
      return result;
    });
});
