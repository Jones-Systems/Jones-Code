import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as ServerSettings from "../../serverSettings.ts";
import * as TestClock from "effect/testing/TestClock";
import type { OrchestrationProjectShell, ProjectId } from "@t3tools/contracts";
import * as GitHubApi from "../../sourceControl/GitHubApi.ts";
import * as SourceControlRateLimit from "../../sourceControl/SourceControlRateLimit.ts";
import * as SourceControlProviderRegistry from "../../sourceControl/SourceControlProviderRegistry.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import * as PullRequestFilesViewed from "../../persistence/PullRequestFilesViewed.ts";
import * as GitHubPullRequestApi from "../../pullRequest/GitHubPullRequestApi.ts";
import * as GitHubPullRequestProvider from "../../pullRequest/GitHubPullRequestProvider.ts";
import * as PullRequestProviderRegistry from "../../pullRequest/PullRequestProviderRegistry.ts";
import * as PullRequestReadCache from "../../pullRequest/PullRequestReadCache.ts";
import * as PullRequestService from "../../pullRequest/PullRequestService.ts";

// The fixture supplies project reads; its write-side boot dependencies are outside this service test.
vi.mock("../../project/ProjectService.ts", async () => {
  const Context = await import("effect/Context");
  class ProjectService extends Context.Service<
    ProjectService,
    {
      readonly listShells: () => Effect.Effect<ReadonlyArray<OrchestrationProjectShell>>;
    }
  >()("t3/jones/pullRequestCi/PullRequestCiStatus.test/ProjectService") {}
  return { ProjectService };
});

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function project(
  repository: string,
  host = "github.com",
  provider = "github",
): OrchestrationProjectShell {
  return {
    id: `${host}/${repository}` as ProjectId,
    title: repository,
    workspaceRoot: `/workspace/${repository}`,
    repositoryIdentity: {
      canonicalKey: `${host}/${repository}`,
      provider,
      displayName: repository,
      locator: {
        source: "git-remote",
        remoteName: "origin",
        remoteUrl: `https://${host}/${repository}.git`,
      },
    },
    defaultModelSelection: null,
    scripts: [],
    createdAt: "2026-10-04T00:00:00Z",
    updatedAt: "2026-10-04T00:00:00Z",
  };
}
const input = { host: "github.com", organization: "acme" };
function fixture(projects: ReadonlyArray<OrchestrationProjectShell>) {
  return Effect.gen(function* () {
    let credential = "credential-a";
    const calls: { endpoint: string; credential: string | undefined }[] = [];
    const provider = yield* GitHubPullRequestProvider.make.pipe(
      Effect.provide(
        Layer.mock(GitHubPullRequestApi.GitHubPullRequestApi)({
          withVerifiedCredential: (request, use) =>
            Effect.suspend(() =>
              use({
                accountId: "101",
                viewer: "test-user",
                credentialFingerprint: credential,
              }).pipe(
                Effect.provideService(GitHubApi.PinnedGitHubCredential, {
                  host: request.host,
                  token: Redacted.make("fixture-secret"),
                  credentialFingerprint: credential,
                }),
                Effect.provideService(SourceControlRateLimit.CredentialScope, credential),
              ),
            ),
        }),
      ),
    );
    const service = yield* PullRequestService.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(
            PullRequestProviderRegistry.PullRequestProviderRegistry,
            PullRequestProviderRegistry.fromProviders([provider]),
          ),
          Layer.mock(ProjectService.ProjectService)({ listShells: () => Effect.succeed(projects) }),
          Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({}),
          Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
            resolveLink: () => undefined,
          }),
          Layer.mock(PullRequestFilesViewed.PullRequestFilesViewedRepository)({}),
          Layer.mock(PullRequestReadCache.PullRequestReadCache)({}),
          SourceControlRateLimit.layer,
          ServerSettings.layerTest(),
          Layer.mock(GitHubApi.GitHubApi)({
            rest: (request) =>
              Effect.gen(function* () {
                const pinned = yield* GitHubApi.PinnedGitHubCredential;
                const endpoint = request.path;
                calls.push({ endpoint, credential: pinned?.credentialFingerprint });
                const value = endpoint.includes("/runners")
                  ? { total_count: 0, runners: [] }
                  : { total_count: 0, workflow_runs: [] };
                return {
                  status: 200,
                  headers: {},
                  body: encodeJson(value),
                  truncated: false,
                  invalidUtf8: false,
                };
              }),
          }),
        ),
      ),
    );
    return {
      service,
      calls,
      changeCredential: () => {
        credential = "credential-b";
      },
    };
  });
}

it.effect("restricts scope to ten configured GitHub repositories and normalizes host/org", () =>
  Effect.gen(function* () {
    const { service, calls } = yield* fixture([
      ...Array.from({ length: 11 }, (_, i) => project(`acme/repo-${i}`)),
      project("other/repo"),
      project("acme/gitlab", "gitlab.com", "gitlab"),
      project("acme/enterprise", "github.example.test"),
      project("acme/repo-1"),
    ]);
    const result = yield* service.ciStatus({ host: "GITHUB.COM", organization: "ACME" });
    assert.equal(result.repositories.length, 10);
    assert.isTrue(result.scopeTruncated);
    assert.equal(result.jobs.state, "partial");
    assert.isTrue(
      calls.every(
        (call) =>
          !call.endpoint.includes("other/") &&
          !call.endpoint.includes("enterprise") &&
          call.credential === "credential-a",
      ),
    );
    assert.isFalse(encodeJson(result).includes("fixture-secret"));
    assert.isFalse(encodeJson(result).includes("credential-a"));
  }),
);

it.effect(
  "does not use GitHub credentials or APIs for an unconfigured scope or mismatched account",
  () =>
    Effect.gen(function* () {
      const { service, calls } = yield* fixture([project("acme/web")]);
      const absent = yield* service.ciStatus({ ...input, organization: "other" });
      const changed = yield* service.ciStatus({ ...input, expectedAccountId: "202" });
      assert.equal(absent.jobs.state, "unavailable");
      assert.equal(changed.jobs.state, "unavailable");
      assert.equal(calls.length, 0);
      assert.equal(changed.accountId, null);
    }),
);

it.effect("coalesces concurrent reads for two minutes and separates credential changes", () =>
  Effect.gen(function* () {
    const { service, calls, changeCredential } = yield* fixture([project("acme/web")]);
    const results = yield* Effect.all([service.ciStatus(input), service.ciStatus(input)], {
      concurrency: 2,
    });
    assert.equal(calls.length, 6);
    assert.deepEqual(results[0], results[1]);
    yield* TestClock.adjust("119 seconds");
    yield* service.ciStatus(input);
    assert.equal(calls.length, 6);
    yield* TestClock.adjust("1 second");
    yield* service.ciStatus(input);
    assert.equal(calls.length, 12);
    changeCredential();
    yield* service.ciStatus(input);
    assert.equal(calls.length, 18);
    assert.equal(calls[17]!.credential, "credential-b");
  }),
);
