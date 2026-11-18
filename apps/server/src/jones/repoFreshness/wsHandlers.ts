import { WS_METHODS, type ProjectId } from "@t3tools/contracts";
import { RepoFreshnessUnavailableError } from "@t3tools/contracts/jones/repoFreshness";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as RepoFreshness from "../git/RepoFreshness.ts";

const unavailable = Effect.fail(
  new RepoFreshnessUnavailableError({ message: "Repository freshness is unavailable." }),
);

const sampleStatus = (status: RepoFreshness.RepoFreshnessStatus) =>
  Effect.map(Clock.currentTimeMillis, (sampledAtMs) => ({ ...status, sampledAtMs }));

export const make = Effect.fn("RepoFreshnessWs.make")(function* () {
  const service = yield* Effect.serviceOption(RepoFreshness.RepoFreshness);
  return {
    [WS_METHODS.vcsGetRepoFreshness]: ({ projectId }: { readonly projectId: ProjectId }) =>
      Option.isNone(service)
        ? unavailable
        : service.value.status(projectId).pipe(Effect.flatMap(sampleStatus)),
    [WS_METHODS.vcsRefreshRepoFreshness]: ({ projectId }: { readonly projectId: ProjectId }) =>
      Option.isNone(service)
        ? unavailable
        : service.value
            .refresh(projectId)
            .pipe(
              Effect.flatMap((result) =>
                Effect.map(sampleStatus(result.status), (status) => ({ ...result, status })),
              ),
            ),
  };
});
