import * as Schema from "effect/Schema";
import * as Rpc from "effect/rpc/Rpc";

import { EnvironmentAuthorizationError } from "../auth.ts";
import {
  RepoFreshnessInput,
  RepoFreshnessRefreshResult,
  RepoFreshnessStatus,
  RepoFreshnessUnavailableError,
} from "./repoFreshness.ts";

const error = Schema.Union([RepoFreshnessUnavailableError, EnvironmentAuthorizationError]);

export const GetRepoFreshnessRpc = Rpc.make("vcs.getRepoFreshness", {
  payload: RepoFreshnessInput,
  success: RepoFreshnessStatus,
  error,
});

export const RefreshRepoFreshnessRpc = Rpc.make("vcs.refreshRepoFreshness", {
  payload: RepoFreshnessInput,
  success: RepoFreshnessRefreshResult,
  error,
});
