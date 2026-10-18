import { SourceControlProviderKind } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as SourceControlRateLimit from "@t3tools/source-control-core/server/SourceControlRateLimit";
import * as GitHubRest from "../server/GitHubRest.ts";
import * as GitHubRequestPolicy from "../server/GitHubRequestPolicy.ts";
import { listQualifiedHeadPullRequests } from "./qualifiedHeadDiscovery.ts";

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const credential = { token: Redacted.make("synthetic"), fingerprint: "synthetic" };

const row = (number: number, merged = false) => ({
  number,
  title: `PR ${number}`,
  html_url: `https://github.com/acme/web/pull/${number}`,
  state: "closed",
  draft: true,
  closed_at: "2026-01-02T00:00:00Z",
  merged_at: merged ? "2026-01-02T00:00:00Z" : null,
  updated_at: "2026-01-02T00:00:00Z",
  base: { ref: "main", repo: { full_name: "acme/web", owner: { login: "acme" } } },
  head: {
    ref: "feature/a&b",
    sha: "abc123",
    repo: { full_name: "fork/web", owner: { login: "fork" } },
    user: { login: "fork" },
  },
});
const input = {
  locator: { host: "github.example.com", owner: "acme", name: "web" },
  headSelector: "fork:feature/a&b",
  state: "closed" as const,
  limit: 1,
};
function run(
  state: "open" | "closed" | "merged" | "all",
  pages: ReadonlyArray<unknown>,
  limit = 1,
) {
  const calls: GitHubRest.GitHubRestInput[] = [];
  const effect = listQualifiedHeadPullRequests({ ...input, state, limit }).pipe(
    Effect.provideService(
      GitHubRest.GitHubRest,
      GitHubRest.GitHubRest.of({
        credential: () => Effect.succeed(credential),
        rest: (request) =>
          Effect.gen(function* () {
            const pinned = yield* GitHubRequestPolicy.PinnedGitHubCredential;
            assert.strictEqual(pinned?.token, credential.token);
            assert.strictEqual(pinned?.credentialFingerprint, credential.fingerprint);
            assert.strictEqual(pinned?.host, input.locator.host);
            calls.push(request);
            return {
              status: 200,
              headers: {},
              truncated: false,
              invalidUtf8: false,
              body: encodeJson(pages[Math.min(calls.length - 1, pages.length - 1)]),
            };
          }),
      }),
    ),
  );
  return { effect, calls };
}

describe("qualified head REST discovery", () => {
  it.effect("paginates by raw count past malformed and merged rows, retaining fork and SHA", () => {
    const { effect, calls } = run("closed", [
      [...Array.from({ length: 99 }, () => row(1, true)), {}],
      [row(42)],
    ]);
    return Effect.gen(function* () {
      const rows = yield* effect;
      assert.strictEqual(rows[0]?.number, 42);
      assert.strictEqual(rows[0]?.headSha, "abc123");
      assert.strictEqual(rows[0]?.isCrossRepository, true);
      assert.strictEqual(rows[0]?.headRepositoryNameWithOwner, "fork/web");
      assert.strictEqual(rows[0]?.isDraft, true);
      assert.strictEqual(calls.length, 2);
      for (const call of calls) {
        const url = new URL(`https://${call.host}/${call.path}`);
        assert.strictEqual(call.host, "github.example.com");
        assert.strictEqual(call.allowReserve, false);
        assert.strictEqual(url.searchParams.get("head"), input.headSelector);
        assert.strictEqual(url.searchParams.get("state"), "closed");
        assert.strictEqual(url.searchParams.get("sort"), "created");
        assert.strictEqual(url.searchParams.get("direction"), "desc");
        assert.strictEqual(url.searchParams.get("per_page"), "100");
      }
      assert.strictEqual(
        new URL(`https://example.com/${calls[1]?.path}`).searchParams.get("page"),
        "2",
      );
    });
  });
  it.effect("filters merged results and preserves deleted-head owner identity", () => {
    const deleted = { ...row(2, true), head: { ...row(2).head, repo: null } };
    const { effect, calls } = run("merged", [[row(1), deleted]]);
    return Effect.gen(function* () {
      const rows = yield* effect;
      assert.strictEqual(rows[0]?.number, 2);
      assert.strictEqual(rows[0]?.state, "merged");
      assert.strictEqual(rows[0]?.headRepositoryOwnerLogin, "fork");
      assert.strictEqual(rows[0]?.headRepositoryNameWithOwner, undefined);
      assert.strictEqual(
        new URL(`https://example.com/${calls[0]?.path}`).searchParams.get("state"),
        "closed",
      );
    });
  });
  it.effect("terminates an empty result without another request", () => {
    const { effect, calls } = run("all", [[]]);
    return Effect.gen(function* () {
      assert.deepStrictEqual(yield* effect, []);
      assert.strictEqual(calls.length, 1);
    });
  });
  it.effect("fails explicitly after ten full pages without enough matching rows", () => {
    const { effect, calls } = run("closed", [Array.from({ length: 100 }, () => row(1, true))]);
    return Effect.gen(function* () {
      const result = yield* Effect.result(effect);
      assert(Result.isFailure(result));
      assert.strictEqual(result.failure._tag, "QualifiedHeadPageLimitError");
      assert.strictEqual(calls.length, 10);
    });
  });
  it.effect("fails malformed page JSON shape without fallback", () => {
    const { effect, calls } = run("all", [{}]);
    return Effect.gen(function* () {
      const result = yield* Effect.result(effect);
      assert(Result.isFailure(result));
      assert.strictEqual(result.failure._tag, "QualifiedHeadDecodeError");
      assert.strictEqual(calls.length, 1);
    });
  });
  it.effect("propagates a REST failure without retry or protocol fallback", () => {
    let calls = 0;
    const error = new GitHubRequestPolicy.GitHubApiRequestError({
      host: input.locator.host,
      operation: "listPullRequestsByHead",
      cause: new Error("offline"),
    });
    return Effect.gen(function* () {
      const result = yield* Effect.result(
        listQualifiedHeadPullRequests(input).pipe(
          Effect.provideService(
            GitHubRest.GitHubRest,
            GitHubRest.GitHubRest.of({
              credential: () => Effect.succeed(credential),
              rest: () => {
                calls++;
                return Effect.fail(error);
              },
            }),
          ),
        ),
      );
      assert(Result.isFailure(result));
      assert.strictEqual(result.failure, error);
      assert.strictEqual(calls, 1);
    });
  });

  for (const state of ["open", "all"] as const) {
    it.effect(`preserves ${state} selection and the requested result limit`, () => {
      const { effect, calls } = run(
        state,
        [[{ ...row(3), state: "open" }, { ...row(2), state: "open" }, row(1, true)]],
        2,
      );
      return Effect.gen(function* () {
        assert.deepStrictEqual(
          (yield* effect).map((entry) => entry.number),
          [3, 2],
        );
        assert.strictEqual(calls.length, 1);
        assert.strictEqual(
          new URL(`https://example.com/${calls[0]?.path}`).searchParams.get("state"),
          state,
        );
      });
    });
  }
  for (const error of [
    new GitHubRequestPolicy.GitHubApiAuthenticationError({
      host: input.locator.host,
      operation: "listPullRequestsByHead",
    }),
    new SourceControlRateLimit.SourceControlRateLimitPausedError({
      provider: SourceControlProviderKind.make("github"),
      host: input.locator.host,
      retryAt: 123,
    }),
  ]) {
    it.effect(`keeps ${error._tag} terminal under ambient interactive context`, () => {
      let calls = 0;
      return Effect.gen(function* () {
        const result = yield* Effect.result(
          listQualifiedHeadPullRequests(input).pipe(
            Effect.provideService(
              GitHubRest.GitHubRest,
              GitHubRest.GitHubRest.of({
                credential: () => Effect.succeed(credential),
                rest: (request) => {
                  calls++;
                  assert.strictEqual(request.allowReserve, false);
                  return Effect.fail(error);
                },
              }),
            ),
            Effect.provideService(SourceControlRateLimit.Interactive, true),
          ),
        );
        assert(Result.isFailure(result));
        assert.strictEqual(result.failure, error);
        assert.strictEqual(calls, 1);
      });
    });
  }
});
