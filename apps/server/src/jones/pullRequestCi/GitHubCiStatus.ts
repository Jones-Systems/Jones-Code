import { PositiveInt, type PullRequestCiStatusResult } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import * as GitHubApi from "../../sourceControl/GitHubApi.ts";
import * as SourceControlRateLimit from "../../sourceControl/SourceControlRateLimit.ts";

const CI_RUN_STATUSES = ["in_progress", "queued", "waiting", "pending", "requested"] as const;
const CI_PAGE_SIZE = 100;
const CI_JOB_RUN_LIMIT = 20;
const CI_TOTAL_CALL_LIMIT = 80;
const CiRun = Schema.Struct({
  id: PositiveInt,
  name: Schema.NullOr(Schema.String),
  status: Schema.String,
  html_url: Schema.NullOr(Schema.String),
});
const CiJob = Schema.Struct({
  id: PositiveInt,
  name: Schema.String,
  status: Schema.String,
  html_url: Schema.NullOr(Schema.String),
  runner_id: Schema.NullOr(Schema.Number),
  runner_name: Schema.NullOr(Schema.String),
  started_at: Schema.NullOr(Schema.String),
});
const CiRunner = Schema.Struct({
  id: PositiveInt,
  name: Schema.String,
  status: Schema.String,
  busy: Schema.Boolean,
  labels: Schema.Array(Schema.Struct({ name: Schema.String })),
});

function ciReadReason(error: unknown): string {
  const tag = typeof error === "object" && error !== null && "_tag" in error ? error._tag : "";
  if (tag === "GitHubApiRateLimitError" || tag === "SourceControlRateLimitPausedError")
    return "GitHub API rate limit reached; reads are paused.";
  if (tag === "GitHubApiAuthenticationError") return "GitHub authentication is unavailable.";
  if (tag === "GitHubCliMissingError") return "GitHub CLI is unavailable.";
  return "GitHub data could not be read; access may be restricted.";
}

/** Read only under the caller's verified credential; error payloads never cross the wire. */
export const readGitHubCiStatus = (
  github: Pick<GitHubApi.GitHubApi["Service"], "rest">,
  input: {
    readonly cwd: string;
    readonly host: string;
    readonly organization: string;
    readonly accountId: string;
    readonly repositories: ReadonlyArray<string>;
    readonly scopeTruncated: boolean;
  },
): Effect.Effect<PullRequestCiStatusResult> =>
  Effect.gen(function* () {
    const started = yield* Clock.currentTimeMillis;
    const rateLimits = yield* Effect.serviceOption(SourceControlRateLimit.SourceControlRateLimit);
    const rateLimitKey = { provider: "github" as const, host: input.host };
    let lastLease: number | undefined;
    const request: GitHubApi.GitHubApi["Service"]["rest"] = (requestInput) => {
      if (Option.isNone(rateLimits)) return github.rest(requestInput);
      return rateLimits.value.check(rateLimitKey).pipe(
        Effect.flatMap((lease) => {
          lastLease = lease;
          return github.rest(requestInput).pipe(
            Effect.tapError((error) =>
              error._tag === "GitHubApiRateLimitError" ||
              (error._tag === "GitHubApiResponseError" && error.status === 429)
                ? rateLimits.value.recordRateLimit({
                    ...rateLimitKey,
                    lease,
                    retryAt: error._tag === "GitHubApiRateLimitError" ? error.retryAt : undefined,
                  })
                : Effect.void,
            ),
          );
        }),
        Effect.catchTag("SourceControlRateLimitPausedError", (cause) =>
          Effect.fail(
            new GitHubApi.GitHubApiRateLimitError({
              host: input.host,
              operation: "PullRequestCiStatus.read",
              retryAt: cause.retryAt,
            }),
          ),
        ),
      );
    };
    const result: PullRequestCiStatusResult = {
      host: input.host,
      organization: input.organization,
      accountId: input.accountId,
      observedAt: DateTime.formatIso(DateTime.makeUnsafe(started)),
      repositories: input.repositories,
      scopeTruncated: input.scopeTruncated,
      jobs: { state: "unavailable", reasons: [], items: [] },
      workflows: { state: "unavailable", reasons: [], items: [] },
      runners: { state: "unavailable", reasons: [], items: [] },
    };
    const queueReasons = new Set<string>();
    if (input.scopeTruncated)
      queueReasons.add("Only the first 10 configured repositories were scanned.");
    const jobs: Array<PullRequestCiStatusResult["jobs"]["items"][number]> = [];
    const workflows: Array<PullRequestCiStatusResult["workflows"]["items"][number]> = [];
    let successfulRunReads = 0;
    let successfulJobReads = 0;
    let jobReads = 0;
    let totalCalls = 0;
    let repositoryCallLimit = CI_TOTAL_CALL_LIMIT;
    const callsByRepository = new Map<string, number>();
    const page = <A>(
      endpoint: string,
      field: string,
      item: Schema.Codec<A, unknown>,
      maxPages: number,
      repository?: string,
    ) =>
      Effect.gen(function* () {
        const items: Array<A> = [];
        const reasons: string[] = [];
        let succeeded = false;
        const decode = Schema.decodeUnknownEffect(
          Schema.fromJsonString(
            Schema.Struct({
              total_count: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
              [field]: Schema.Array(item),
            }),
          ),
        );
        for (let number = 1; number <= maxPages; number++) {
          if ((yield* Clock.currentTimeMillis) - started >= 30_000) {
            reasons.push("The bounded observation time was reached.");
            break;
          }
          const repositoryCalls =
            repository === undefined ? 0 : (callsByRepository.get(repository) ?? 0);
          if (
            totalCalls >= CI_TOTAL_CALL_LIMIT ||
            (repository !== undefined && repositoryCalls >= repositoryCallLimit)
          ) {
            reasons.push("The bounded GitHub request budget was reached.");
            break;
          }
          totalCalls++;
          if (repository !== undefined) callsByRepository.set(repository, repositoryCalls + 1);
          const read = yield* request({
            host: input.host,
            operation: "PullRequestCiStatus.read",
            method: "GET",
            path: `${endpoint}${endpoint.includes("?") ? "&" : "?"}per_page=${CI_PAGE_SIZE}&page=${number}`,
            timeout: 8_000,
            maxResponseBytes: 2 * 1024 * 1024,
          }).pipe(
            Effect.flatMap((output) =>
              output.truncated || output.invalidUtf8
                ? Effect.fail("incomplete-output")
                : decode(output.body).pipe(Effect.mapError(() => "invalid-response")),
            ),
            Effect.result,
          );
          if (Result.isFailure(read)) {
            reasons.push(ciReadReason(read.failure));
            break;
          }
          succeeded = true;
          const rows = read.success[field] as ReadonlyArray<A>;
          items.push(...rows);
          if (items.length >= (read.success.total_count as number)) break;
          if (number === maxPages || rows.length === 0) {
            reasons.push("GitHub has more results than this bounded scan can include.");
            break;
          }
        }
        return { items, reasons, succeeded };
      });
    const runnerRead = yield* page(
      `orgs/${encodeURIComponent(input.organization)}/actions/runners`,
      "runners",
      CiRunner,
      2,
    );
    // Reserve an equal share for each configured repository so one busy repository cannot consume the scan.
    repositoryCallLimit = Math.floor(
      (CI_TOTAL_CALL_LIMIT - totalCalls) / Math.max(1, input.repositories.length),
    );
    const runners: Array<PullRequestCiStatusResult["runners"]["items"][number]> =
      runnerRead.items.map((runner) => ({
        id: runner.id,
        name: runner.name,
        status:
          runner.status === "online" || runner.status === "offline"
            ? runner.status
            : ("unknown" as const),
        busy: runner.busy,
        labels: runner.labels.map((label) => label.name),
      }));
    yield* Effect.forEach(
      input.repositories,
      (repository) =>
        Effect.gen(function* () {
          const path = `repos/${repository.split("/").map(encodeURIComponent).join("/")}/actions/runs`;
          const runs = new Map<number, typeof CiRun.Type>();
          const inspected = new Set<number>();
          for (const status of CI_RUN_STATUSES) {
            const read = yield* page(
              `${path}?status=${status}`,
              "workflow_runs",
              CiRun,
              1,
              repository,
            );
            if (read.succeeded) successfulRunReads++;
            for (const reason of read.reasons) queueReasons.add(`${repository}: ${reason}`);
            for (const run of read.items) if (run.status !== "completed") runs.set(run.id, run);
            for (const run of runs.values()) {
              if (inspected.has(run.id)) continue;
              inspected.add(run.id);
              if (jobReads >= CI_JOB_RUN_LIMIT) {
                queueReasons.add(
                  "Only the first 20 active workflow runs could be inspected for jobs.",
                );
                break;
              }
              jobReads++;
              const read = yield* page(
                `${path}/${run.id}/jobs?filter=latest`,
                "jobs",
                CiJob,
                2,
                repository,
              );
              if (read.succeeded) successfulJobReads++;
              for (const reason of read.reasons) queueReasons.add(`${repository}: ${reason}`);
              let active = 0;
              for (const job of read.items) {
                if (job.status !== "queued" && job.status !== "in_progress") continue;
                active++;
                jobs.push({
                  id: job.id,
                  runId: run.id,
                  repository,
                  name: job.name,
                  status: job.status,
                  url: ciWebUrl(job.html_url, input.host),
                  runnerId:
                    job.runner_id !== null &&
                    Number.isSafeInteger(job.runner_id) &&
                    job.runner_id > 0
                      ? job.runner_id
                      : null,
                  runnerName: job.runner_name || null,
                  startedAt:
                    job.started_at !== null && Number.isFinite(Date.parse(job.started_at))
                      ? DateTime.formatIso(DateTime.makeUnsafe(Date.parse(job.started_at)))
                      : null,
                });
              }
              if (
                active === 0 &&
                read.succeeded &&
                read.reasons.length === 0 &&
                run.status !== "in_progress"
              ) {
                workflows.push({
                  id: run.id,
                  repository,
                  name: run.name ?? "Workflow",
                  status: run.status,
                  url: ciWebUrl(run.html_url, input.host),
                });
              }
            }
          }
        }),
      { concurrency: 2, discard: true },
    );
    const reasons = [...queueReasons].sort();
    const queueState =
      successfulRunReads === 0 || (jobReads > 0 && successfulJobReads === 0)
        ? "unavailable"
        : reasons.length > 0
          ? "partial"
          : "available";
    if (
      Option.isSome(rateLimits) &&
      lastLease !== undefined &&
      queueState === "available" &&
      runnerRead.reasons.length === 0
    )
      yield* rateLimits.value.recordSuccess({ ...rateLimitKey, lease: lastLease });
    return {
      ...result,
      jobs: {
        state: queueState,
        reasons,
        items: [...new Map(jobs.map((job) => [job.id, job])).values()].sort((a, b) => a.id - b.id),
      },
      workflows: { state: queueState, reasons, items: workflows.sort((a, b) => a.id - b.id) },
      runners: {
        state: !runnerRead.succeeded
          ? "unavailable"
          : runnerRead.reasons.length > 0
            ? "partial"
            : "available",
        reasons: runnerRead.reasons,
        items: [...new Map(runners.map((runner) => [runner.id, runner])).values()],
      },
    } satisfies PullRequestCiStatusResult;
  });

function ciWebUrl(value: string | null, host: string): string | null {
  if (value === null) return null;
  try {
    const url = new URL(value);
    return url.protocol === "https:" &&
      url.host.toLowerCase() === host &&
      !url.username &&
      !url.password
      ? url.toString()
      : null;
  } catch {
    return null;
  }
}
