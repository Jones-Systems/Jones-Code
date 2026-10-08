import type { PullRequestCiStatusResult } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";

import type * as GitHubApi from "../../sourceControl/GitHubApi.ts";
import type * as PullRequestProviderRegistry from "../../pullRequest/PullRequestProviderRegistry.ts";
import type {
  PullRequestError,
  PullRequestService,
  SupportedProject,
} from "../../pullRequest/PullRequestService.ts";
import * as SourceControlRateLimit from "../../sourceControl/SourceControlRateLimit.ts";
import { readGitHubCiStatus } from "./GitHubCiStatus.ts";

interface CiStatusDependencies {
  readonly listWorkspaceProjects: (filter: {
    readonly host: string;
  }) => Effect.Effect<{ readonly supported: ReadonlyArray<SupportedProject> }, PullRequestError>;
  readonly registry: Pick<
    PullRequestProviderRegistry.PullRequestProviderRegistry["Service"],
    "get"
  >;
  readonly githubCi: Option.Option<GitHubApi.GitHubApi["Service"]>;
  readonly rateLimits: SourceControlRateLimit.SourceControlRateLimit["Service"];
}

export const createCiStatus = ({
  listWorkspaceProjects,
  registry,
  githubCi,
  rateLimits,
}: CiStatusDependencies) =>
  Effect.gen(function* () {
    const ciCache = new Map<string, { at: number; value: PullRequestCiStatusResult }>();
    const ciGate = yield* Semaphore.make(1);
    const ciStatus: PullRequestService["Service"]["ciStatus"] = Effect.fn(
      "PullRequestService.ciStatus",
    )(function* (input) {
      const host = input.host.toLowerCase();
      const organization = input.organization.toLowerCase();
      const { supported } = yield* listWorkspaceProjects({ host });
      const matching = supported
        .filter(
          (candidate) =>
            candidate.api.kind === "github" &&
            candidate.repository.split("/").length === 2 &&
            candidate.repository.split("/")[0]!.toLowerCase() === organization,
        )
        .sort((a, b) => a.repository.localeCompare(b.repository));
      const selected = matching.slice(0, 10);
      const repositories = selected.map((candidate) => candidate.repository);
      const unavailable = (reason: string): Effect.Effect<PullRequestCiStatusResult> =>
        Effect.map(Clock.currentTimeMillis, (now) => ({
          host,
          organization: input.organization,
          accountId: null,
          observedAt: DateTime.formatIso(DateTime.makeUnsafe(now)),
          repositories,
          scopeTruncated: matching.length > selected.length,
          jobs: { state: "unavailable", reasons: [reason], items: [] },
          workflows: { state: "unavailable", reasons: [reason], items: [] },
          runners: { state: "unavailable", reasons: [reason], items: [] },
        }));
      const project = selected[0];
      const api = registry.get("github");
      if (project === undefined)
        return yield* unavailable(
          "No configured GitHub repositories match this organization and host.",
        );
      if (api?.withVerifiedCredential === undefined || Option.isNone(githubCi))
        return yield* unavailable("This environment cannot read GitHub CI status.");
      return yield* api
        .withVerifiedCredential({ cwd: project.project.workspaceRoot, host }, (identity) => {
          if (
            input.expectedAccountId !== undefined &&
            identity.accountId !== input.expectedAccountId
          )
            return unavailable("The GitHub account changed. Refresh the selected environment.");
          const key = JSON.stringify([
            host,
            organization,
            identity.credentialFingerprint,
            selected.map((candidate) => [candidate.repository, candidate.project.workspaceRoot]),
            matching.length,
          ]);
          // Keep only normalized responses; the verified credential stays in the request context.
          return ciGate.withPermit(
            Effect.gen(function* () {
              const now = yield* Clock.currentTimeMillis;
              const cached = ciCache.get(key);
              if (cached !== undefined && now - cached.at < 120_000) return cached.value;
              const value = yield* readGitHubCiStatus(githubCi.value, {
                cwd: project.project.workspaceRoot,
                host,
                organization: input.organization,
                accountId: identity.accountId,
                repositories,
                scopeTruncated: matching.length > selected.length,
              }).pipe(
                Effect.provideService(SourceControlRateLimit.SourceControlRateLimit, rateLimits),
              );
              if (ciCache.size >= 32) ciCache.delete(ciCache.keys().next().value!);
              ciCache.set(key, { at: yield* Clock.currentTimeMillis, value });
              return value;
            }),
          );
        })
        .pipe(Effect.catch(() => unavailable("The GitHub account could not be verified.")));
    });

    return ciStatus;
  });
