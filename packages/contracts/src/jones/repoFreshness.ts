import * as Schema from "effect/Schema";

import { NonNegativeInt, ProjectId } from "../baseSchemas.ts";

const gitName = Schema.String.check(Schema.isMinLength(1));
const RepoFreshnessVerification = Schema.Struct({
  remoteName: gitName,
  branchName: gitName,
  commitSha: Schema.String.check(Schema.isPattern(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/)),
  checkedAtMs: NonNegativeInt,
});

const RepoFreshnessAttempt = Schema.Union([
  Schema.Struct({ status: Schema.Literal("succeeded"), verification: RepoFreshnessVerification }),
  Schema.Struct({ status: Schema.Literal("failed"), checkedAtMs: NonNegativeInt }),
]);

export const RepoFreshnessInput = Schema.Struct({ projectId: ProjectId });

export const RepoFreshnessStatus = Schema.Struct({
  projectId: ProjectId,
  cadenceSeconds: Schema.Literals([0, 120, 300]),
  target: Schema.Union([
    Schema.Struct({ status: Schema.Literal("resolved"), remoteName: gitName }),
    Schema.Struct({
      status: Schema.Literal("unavailable"),
      reason: Schema.Literals([
        "project-missing",
        "project-state-unavailable",
        "remote-unresolved",
        "target-changed",
      ]),
    }),
  ]),
  latestVerified: Schema.NullOr(RepoFreshnessVerification),
  latestAttempt: Schema.NullOr(RepoFreshnessAttempt),
  freshness: Schema.Literals(["unknown", "fresh", "stale", "cadence-off"]),
  sampledAtMs: NonNegativeInt,
});
export type RepoFreshnessStatus = typeof RepoFreshnessStatus.Type;

export const RepoFreshnessRefreshResult = Schema.Struct({
  status: RepoFreshnessStatus,
  checkout: Schema.Union([
    Schema.Struct({ status: Schema.Literal("updated") }),
    Schema.Struct({ status: Schema.Literal("already-current") }),
    Schema.Struct({
      status: Schema.Literal("skipped"),
      reason: Schema.optionalKey(Schema.String),
    }),
  ]),
});
export type RepoFreshnessRefreshResult = typeof RepoFreshnessRefreshResult.Type;

export class RepoFreshnessUnavailableError extends Schema.TaggedError<
  RepoFreshnessUnavailableError,
  { readonly brand: unique symbol }
>()("RepoFreshnessUnavailableError", { message: Schema.String }) {}
