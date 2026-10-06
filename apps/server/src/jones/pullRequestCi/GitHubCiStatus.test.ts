import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as TestClock from "effect/testing/TestClock";
import * as SourceControlRateLimit from "../../sourceControl/SourceControlRateLimit.ts";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";
import { PullRequestCiStatusResult } from "@t3tools/contracts";
import * as GitHubCli from "../../sourceControl/GitHubCli.ts";
import { readGitHubCiStatus } from "./GitHubCiStatus.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const input = {
  cwd: "/workspace",
  host: "github.com",
  organization: "acme",
  accountId: "101",
  repositories: ["acme/web"],
  scopeTruncated: false,
};
function output(value: unknown, stdoutTruncated = false) {
  return {
    exitCode: ChildProcessSpawner.ExitCode(0),
    stdout: encodeJson(value),
    stderr: "",
    stdoutTruncated,
    stderrTruncated: false,
    stdoutInvalidUtf8: false,
  };
}
const run = (id: number, status = "queued") => ({
  id,
  name: "CI",
  status,
  html_url: `https://github.com/acme/web/actions/runs/${id}`,
});
const job = (id: number, status = "queued") => ({
  id,
  name: "test",
  status,
  html_url: `https://github.com/acme/web/actions/runs/1/job/${id}`,
  runner_id: 0,
  runner_name: null,
  started_at: null,
});
const runner = (id: number) => ({
  id,
  name: `runner-${id}`,
  status: "online",
  busy: false,
  labels: [{ name: "self-hosted" }],
});
function cli(read: (endpoint: string) => unknown): Pick<GitHubCli.GitHubCli["Service"], "execute"> {
  return {
    execute: (request) => {
      assert.deepEqual(request.args.slice(0, 5), [
        "api",
        "--method",
        "GET",
        "--hostname",
        "github.com",
      ]);
      assert.equal(request.env, undefined);
      return Effect.succeed(output(read(request.args[5]!)));
    },
  };
}
function empty(endpoint: string) {
  return endpoint.includes("/runners")
    ? { total_count: 0, runners: [] }
    : endpoint.includes("/jobs?")
      ? { total_count: 0, jobs: [] }
      : { total_count: 0, workflow_runs: [] };
}

it.effect("normalizes active jobs and runners and separates unmaterialized waiting runs", () =>
  Effect.gen(function* () {
    const result = yield* readGitHubCiStatus(
      cli((endpoint) => {
        if (endpoint.includes("/runners"))
          return {
            total_count: 2,
            runners: [runner(1), { ...runner(2), status: "offline", busy: true }],
          };
        if (endpoint.includes("status=in_progress"))
          return { total_count: 1, workflow_runs: [run(1, "in_progress")] };
        if (endpoint.includes("status=waiting"))
          return { total_count: 1, workflow_runs: [run(2, "waiting")] };
        if (endpoint.includes("/runs/1/jobs"))
          return {
            total_count: 3,
            jobs: [
              job(1),
              {
                ...job(2, "in_progress"),
                runner_id: 1,
                runner_name: "runner-1",
                started_at: "2026-10-04T12:00:00Z",
              },
              job(3, "completed"),
            ],
          };
        return empty(endpoint);
      }),
      input,
    );
    assert.deepEqual(
      result.jobs.items.map((item) => [item.id, item.runnerId, item.status]),
      [
        [1, null, "queued"],
        [2, 1, "in_progress"],
      ],
    );
    assert.equal(result.jobs.items[1]!.startedAt, "2026-10-04T12:00:00.000Z");
    assert.deepEqual(
      result.workflows.items.map((item) => [item.id, item.status]),
      [[2, "waiting"]],
    );
    assert.equal(result.runners.items[1]!.busy, true);
    assert.equal(result.runners.items[1]!.status, "offline");
    assert.equal(result.jobs.state, "available");
    assert.equal(result.runners.state, "available");
    yield* Schema.decodeEffect(PullRequestCiStatusResult)(result);
  }),
);

it.effect("reads a second jobs and runners page and retains partial observations at the cap", () =>
  Effect.gen(function* () {
    const endpoints: string[] = [];
    const result = yield* readGitHubCiStatus(
      cli((endpoint) => {
        endpoints.push(endpoint);
        const second = endpoint.includes("page=2");
        if (endpoint.includes("/runners"))
          return {
            total_count: 201,
            runners: Array.from({ length: 100 }, (_, i) => runner(i + (second ? 101 : 1))),
          };
        if (endpoint.includes("status=queued")) return { total_count: 1, workflow_runs: [run(1)] };
        if (endpoint.includes("/jobs?"))
          return {
            total_count: 101,
            jobs: second ? [job(101)] : Array.from({ length: 100 }, (_, i) => job(i + 1)),
          };
        return empty(endpoint);
      }),
      input,
    );
    assert.equal(result.jobs.items.length, 101);
    assert.equal(result.jobs.state, "available");
    assert.equal(result.runners.items.length, 200);
    assert.equal(result.runners.state, "partial");
    assert.isTrue(
      endpoints.some((endpoint) => endpoint.includes("/jobs?") && endpoint.includes("page=2")),
    );
    assert.isFalse(endpoints.some((endpoint) => endpoint.includes("page=3")));
  }),
);

it.effect("reports run and scope truncation instead of an authoritative empty queue", () =>
  Effect.gen(function* () {
    const result = yield* readGitHubCiStatus(
      cli((endpoint) =>
        endpoint.includes("status=queued")
          ? { total_count: 101, workflow_runs: [] }
          : empty(endpoint),
      ),
      { ...input, scopeTruncated: true },
    );
    assert.equal(result.jobs.state, "partial");
    assert.equal(result.jobs.items.length, 0);
    assert.isTrue(result.jobs.reasons.some((reason) => reason.includes("more results")));
    assert.isTrue(result.jobs.reasons.some((reason) => reason.includes("10 configured")));
  }),
);

it.effect("keeps runner permission errors independent and never returns error payloads", () =>
  Effect.gen(function* () {
    const secret = "fixture-credential-must-not-leak";
    const result = yield* readGitHubCiStatus(
      {
        execute: (request) =>
          request.args[5]!.includes("/runners")
            ? Effect.fail(
                new GitHubCli.GitHubCliCommandError({
                  command: "gh",
                  cwd: "/workspace",
                  httpStatus: 403,
                  cause: secret,
                }),
              )
            : Effect.succeed(output(empty(request.args[5]!))),
      },
      input,
    );
    assert.equal(result.jobs.state, "available");
    assert.equal(result.runners.state, "unavailable");
    assert.isFalse(encodeJson(result).includes(secret));
    assert.isFalse(encodeJson(result).includes("credentialFingerprint"));
  }),
);

it.effect("treats malformed and truncated responses as unavailable", () =>
  Effect.gen(function* () {
    for (const response of [
      output({ total_count: 0 }),
      output({ total_count: 0, workflow_runs: [], runners: [] }, true),
    ]) {
      const result = yield* readGitHubCiStatus({ execute: () => Effect.succeed(response) }, input);
      assert.equal(result.jobs.state, "unavailable");
      assert.equal(result.runners.state, "unavailable");
    }
  }),
);

it.effect("bounds active run job lookups and strips links to foreign origins", () =>
  Effect.gen(function* () {
    let jobReads = 0;
    const result = yield* readGitHubCiStatus(
      cli((endpoint) => {
        if (endpoint.includes("status=queued"))
          return {
            total_count: 21,
            workflow_runs: Array.from({ length: 21 }, (_, i) => run(i + 1)),
          };
        if (endpoint.includes("/jobs?")) {
          jobReads++;
          return {
            total_count: 1,
            jobs: [
              { ...job(jobReads), html_url: "https://foreign.test/private", started_at: "invalid" },
            ],
          };
        }
        return empty(endpoint);
      }),
      input,
    );
    assert.equal(jobReads, 20);
    assert.equal(result.jobs.state, "partial");
    assert.isTrue(result.jobs.items.every((item) => item.url === null && item.startedAt === null));
  }),
);

it.effect("retains all waiting workflow states without counting them as queued jobs", () =>
  Effect.gen(function* () {
    const statuses = ["queued", "waiting", "pending", "requested"];
    const result = yield* readGitHubCiStatus(
      cli((endpoint) => {
        const status = statuses.find((status) => endpoint.includes(`status=${status}`));
        return status === undefined
          ? empty(endpoint)
          : { total_count: 1, workflow_runs: [run(statuses.indexOf(status) + 1, status)] };
      }),
      input,
    );
    assert.equal(result.jobs.items.length, 0);
    assert.deepEqual(
      result.workflows.items.map((item) => item.status),
      statuses,
    );
  }),
);

it.effect(
  "pauses rate-limited credentials across refreshes while admitting a different credential",
  () =>
    Effect.gen(function* () {
      const limits = yield* SourceControlRateLimit.make;
      let reads = 0;
      const now = yield* Clock.currentTimeMillis;
      const failing: Pick<GitHubCli.GitHubCli["Service"], "execute"> = {
        execute: () => {
          reads++;
          return Effect.fail(
            new GitHubCli.GitHubCliRateLimitError({
              command: "gh",
              cwd: "/workspace",
              retryAt: now + 300_000,
              cause: "fixture-secret",
            }),
          );
        },
      };
      const read = (credential: string) =>
        readGitHubCiStatus(failing, input).pipe(
          Effect.provideService(SourceControlRateLimit.SourceControlRateLimit, limits),
          Effect.provideService(SourceControlRateLimit.CredentialScope, credential),
        );
      const initial = yield* read("credential-a");
      assert.equal(reads, 1);
      assert.equal(initial.jobs.state, "unavailable");
      yield* TestClock.adjust("61 seconds");
      const paused = yield* read("credential-a");
      assert.equal(reads, 1);
      assert.isTrue(paused.jobs.reasons.some((reason) => reason.includes("paused")));
      assert.isFalse(encodeJson(paused).includes("fixture-secret"));
      yield* read("credential-b");
      assert.equal(reads, 2);
    }),
);

it.effect(
  "observes all ten repositories with one active job each within the total request budget",
  () =>
    Effect.gen(function* () {
      let calls = 0;
      const repositories = Array.from({ length: 10 }, (_, i) => `acme/repo-${i}`);
      const result = yield* readGitHubCiStatus(
        cli((endpoint) => {
          calls++;
          const id =
            repositories.findIndex((repository) => endpoint.startsWith(`repos/${repository}/`)) + 1;
          if (endpoint.includes("status=in_progress"))
            return { total_count: 1, workflow_runs: [run(id, "in_progress")] };
          if (endpoint.includes("/jobs?"))
            return { total_count: 1, jobs: [job(id, "in_progress")] };
          return empty(endpoint);
        }),
        { ...input, repositories },
      );
      assert.equal(result.jobs.items.length, 10);
      assert.equal(result.jobs.state, "available");
      assert.equal(calls, 61);
      assert.isAtMost(calls, 80);
    }),
);

it.effect(
  "keeps busy repository scans within the shared budget and marks incomplete queue data",
  () =>
    Effect.gen(function* () {
      let calls = 0;
      const repositories = Array.from({ length: 10 }, (_, i) => `acme/repo-${i}`);
      const observedRepositories = new Set<string>();
      const result = yield* readGitHubCiStatus(
        cli((endpoint) => {
          calls++;
          const repository = repositories.find((repository) =>
            endpoint.startsWith(`repos/${repository}/`),
          );
          if (repository !== undefined) observedRepositories.add(repository);
          if (endpoint.includes("status=in_progress"))
            return {
              total_count: 10,
              workflow_runs: Array.from({ length: 10 }, (_, i) => run(i + 1, "in_progress")),
            };
          if (endpoint.includes("/jobs?"))
            return { total_count: 1, jobs: [job(calls, "in_progress")] };
          return empty(endpoint);
        }),
        { ...input, repositories },
      );
      assert.equal(observedRepositories.size, 10);
      assert.equal(result.jobs.state, "partial");
      assert.isAbove(result.jobs.items.length, 0);
      assert.isAtMost(calls, 80);
    }),
);
