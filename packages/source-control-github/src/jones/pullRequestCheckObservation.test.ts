import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { GitHubRest } from "../server/GitHubRest.ts";
import { makeCheckObservationReader } from "./pullRequestCheckObservation.ts";

const sha = "a".repeat(40);
const input = {
  host: "github.com",
  repository: "Jones-Systems/Jones-Code",
  number: 610,
  headSha: sha,
  checks: [],
};
const head = (value = sha) => ({
  head: { sha: value, ref: "topic" },
  base: { ref: "main", repo: { full_name: input.repository } },
});
const run = (overrides = {}) => ({
  id: 1,
  workflow_id: 1,
  check_suite_id: 10,
  run_number: 1,
  run_attempt: 1,
  head_sha: sha,
  head_branch: "topic",
  path: ".github/workflows/ci.yml",
  name: "CI",
  event: "pull_request",
  status: "completed",
  conclusion: "success",
  ...overrides,
});
const check = (overrides = {}) => ({
  id: 1,
  name: "Check",
  head_sha: sha,
  status: "completed",
  conclusion: "success",
  details_url: "https://example.test/check",
  check_suite: { id: 10 },
  app: { id: 1, slug: "github-actions" },
  ...overrides,
});
const fixture = (answer: (path: string) => unknown) =>
  Layer.mock(GitHubRest)({
    rest: ({ path }) =>
      Effect.sync(() => ({
        status: 200,
        headers: {},
        body: JSON.stringify(answer(path)),
        truncated: false,
        invalidUtf8: false,
      })),
  });
const defaultAnswer = (path: string) =>
  path.includes("/pulls/")
    ? head()
    : path.includes("/check-runs")
      ? { total_count: 1, check_runs: [check()] }
      : path.includes("/statuses")
        ? []
        : { total_count: 1, workflow_runs: [run()] };

it.effect("paginates all workflow runs before proving the bound gate complete", () =>
  Effect.gen(function* () {
    const reader = yield* makeCheckObservationReader.pipe(
      Effect.provide(
        fixture((path) => {
          if (!path.includes("/actions/runs")) return defaultAnswer(path);
          return path.endsWith("page=1")
            ? {
                total_count: 101,
                workflow_runs: Array.from({ length: 100 }, (_, index) =>
                  run({
                    id: index + 1000,
                    workflow_id: index + 1000,
                    check_suite_id: index + 1000,
                    path: ".github/workflows/advisory.yml",
                    name: "Advisory",
                  }),
                ),
              }
            : { total_count: 101, workflow_runs: [run()] };
        }),
      ),
    );
    const result = yield* reader(input);
    assert.isTrue(result.jonesCheckObservation.enumerationComplete);
    assert.equal(result.jonesCheckObservation.requiredState, "success");
  }),
);

it.effect("retains confirmed same-head GraphQL failures when a REST page is truncated", () =>
  Effect.gen(function* () {
    const reader = yield* makeCheckObservationReader.pipe(
      Effect.provide(
        Layer.mock(GitHubRest)({
          rest: ({ path }) =>
            Effect.succeed({
              status: 200,
              headers: {},
              body: JSON.stringify(defaultAnswer(path)),
              truncated: path.includes("/check-runs"),
              invalidUtf8: false,
            }),
        }),
      ),
    );
    const result = yield* reader({
      ...input,
      checks: [{ name: "known failure", status: "failure", description: null, url: null }],
    });
    assert.isFalse(result.jonesCheckObservation.enumerationComplete);
    assert.equal(result.checks.find((row) => row.name === "known failure")?.status, "failure");
  }),
);

it.effect("keeps the workflow search ceiling explicitly incomplete", () =>
  Effect.gen(function* () {
    const reader = yield* makeCheckObservationReader.pipe(
      Effect.provide(
        fixture((path) => {
          if (!path.includes("/actions/runs")) return defaultAnswer(path);
          const page = Number(new URL(`https://example.test/${path}`).searchParams.get("page"));
          return {
            total_count: 1000,
            workflow_runs: Array.from({ length: 100 }, (_, index) =>
              run({
                id: (page - 1) * 100 + index + 1,
                workflow_id: (page - 1) * 100 + index + 1,
                check_suite_id: (page - 1) * 100 + index + 1,
              }),
            ),
          };
        }),
      ),
    );
    const result = yield* reader(input);
    assert.isFalse(result.jonesCheckObservation.enumerationComplete);
    assert.equal(result.jonesCheckObservation.requiredState, "unknown");
  }),
);

it.effect(
  "pins REST failures despite an empty GraphQL rollup and paginates checks and statuses",
  () =>
    Effect.gen(function* () {
      const paths: Array<string> = [];
      const reader = yield* makeCheckObservationReader.pipe(
        Effect.provide(
          fixture((path) => {
            paths.push(path);
            if (path.includes("/check-runs"))
              return path.endsWith("page=1")
                ? {
                    total_count: 101,
                    check_runs: Array.from({ length: 100 }, (_, id) =>
                      check({ id, name: `job-${id}` }),
                    ),
                  }
                : {
                    total_count: 101,
                    check_runs: [check({ id: 101, name: "late-failure", conclusion: "failure" })],
                  };
            if (path.includes("/statuses"))
              return path.endsWith("page=1")
                ? Array.from({ length: 100 }, (_, id) => ({
                    id,
                    context: `status-${id}`,
                    state: "success",
                    description: null,
                    target_url: null,
                  }))
                : [
                    {
                      id: 101,
                      context: "late-status",
                      state: "failure",
                      description: null,
                      target_url: null,
                    },
                  ];
            return defaultAnswer(path);
          }),
        ),
      );
      const result = yield* reader(input);
      assert.equal(result.checks.find((row) => row.name === "late-failure")?.status, "failure");
      assert.equal(result.checks.find((row) => row.name === "late-status")?.status, "failure");
      assert.isTrue(result.jonesCheckObservation.enumerationComplete);
      assert.isTrue(paths.some((path) => path.includes("check-runs") && path.endsWith("page=2")));
      assert.isTrue(paths.some((path) => path.includes("statuses") && path.endsWith("page=2")));
    }),
);

it.effect("rejects a head that moves during enumeration", () =>
  Effect.gen(function* () {
    let reads = 0;
    const reader = yield* makeCheckObservationReader.pipe(
      Effect.provide(
        fixture((path) =>
          path.includes("/pulls/")
            ? head(++reads === 1 ? sha : "b".repeat(40))
            : defaultAnswer(path),
        ),
      ),
    );
    assert.equal((yield* reader(input).pipe(Effect.flip))._tag, "CheckObservationChanged");
  }),
);

it.effect("rejects a target that changed since the GraphQL detail was read", () =>
  Effect.gen(function* () {
    const reader = yield* makeCheckObservationReader.pipe(Effect.provide(fixture(defaultAnswer)));
    assert.equal(
      (yield* reader({ ...input, baseBranch: "release" }).pipe(Effect.flip))._tag,
      "CheckObservationChanged",
    );
  }),
);

it.effect("never qualifies truncated, malformed, stale-head or capped observations", () =>
  Effect.gen(function* () {
    for (const broken of ["malformed", "stale", "capped"]) {
      const reader = yield* makeCheckObservationReader.pipe(
        Effect.provide(
          fixture((path) => {
            if (!path.includes("/check-runs")) return defaultAnswer(path);
            return broken === "malformed"
              ? {}
              : {
                  total_count: broken === "capped" ? 2001 : 1,
                  check_runs: [check(broken === "stale" ? { head_sha: "b".repeat(40) } : {})],
                };
          }),
        ),
      );
      const result = yield* reader(input);
      assert.isFalse(result.jonesCheckObservation.enumerationComplete, broken);
      assert.equal(result.jonesCheckObservation.requiredState, "unknown");
    }
  }),
);

it.effect(
  "ignores old workflow runs and retains identically named jobs in distinct workflows",
  () =>
    Effect.gen(function* () {
      const reader = yield* makeCheckObservationReader.pipe(
        Effect.provide(
          fixture((path) => {
            if (path.includes("/actions/runs"))
              return {
                total_count: 3,
                workflow_runs: [
                  run(),
                  run({ id: 2, check_suite_id: 20, run_number: 2 }),
                  run({
                    id: 3,
                    workflow_id: 2,
                    check_suite_id: 30,
                    path: ".github/workflows/advisory.yml",
                    name: "Advisory",
                  }),
                ],
              };
            if (path.includes("/check-runs"))
              return {
                total_count: 3,
                check_runs: [
                  check({ conclusion: "failure" }),
                  check({ id: 2, check_suite: { id: 20 } }),
                  check({ id: 3, check_suite: { id: 30 }, conclusion: "neutral" }),
                ],
              };
            return defaultAnswer(path);
          }),
        ),
      );
      const result = yield* reader(input);
      assert.lengthOf(result.checks, 2);
      assert.equal(new Set(result.checks.map((row) => row.name)).size, 2);
      assert.notInclude(
        result.checks.map((row) => row.status),
        "failure",
      );
      assert.equal(result.jonesCheckObservation.requiredState, "success");
    }),
);

it.effect("waits for a missing aggregator and active same-repository workflows", () =>
  Effect.gen(function* () {
    const reader = yield* makeCheckObservationReader.pipe(
      Effect.provide(
        fixture((path) => {
          if (path.includes("/actions/runs"))
            return {
              total_count: 1,
              workflow_runs: [run({ status: "in_progress", conclusion: null })],
            };
          if (path.includes("/check-runs"))
            return {
              total_count: 3,
              check_runs: [1, 2, 3].map((id) =>
                check({ id, name: `advisory-${id}`, conclusion: "skipped" }),
              ),
            };
          return defaultAnswer(path);
        }),
      ),
    );
    const result = yield* reader(input);
    assert.equal(result.jonesCheckObservation.workflowState, "pending");
    assert.equal(result.jonesCheckObservation.requiredState, "missing");
  }),
);
