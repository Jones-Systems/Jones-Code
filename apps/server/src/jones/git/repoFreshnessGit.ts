import { GitCommandError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { invalidRemoteFetch, type makeRemoteFetchBinding } from "./remoteFetchObservation.ts";
import type {
  RepoFreshnessCheckout,
  RepoFreshnessCheckoutSkipReason,
} from "./repoFreshnessCheckout.ts";

type BoundRemote = NonNullable<
  Effect.Success<ReturnType<ReturnType<typeof makeRemoteFetchBinding>>>
>;

export interface VerifiedRepoDefault {
  readonly remoteName: string;
  readonly branchName: string;
  readonly commitSha: string;
}

export interface RepoFreshnessRemote {
  /** Opaque, process-local key. Never serialize it into a status or an error. */
  readonly identity: string;
  readonly remoteName: string;
  readonly checkoutIdentity: string | null;
  readonly check: Effect.Effect<VerifiedRepoDefault, GitCommandError>;
  readonly updateCheckout: (
    verified: VerifiedRepoDefault,
    authorize: Effect.Effect<RepoFreshnessCheckoutSkipReason | null>,
  ) => Effect.Effect<RepoFreshnessCheckout>;
}

export const fetchRepoDefault = Effect.fn("GitVcsDriver.fetchRepoDefault")(function* (
  bound: BoundRemote,
) {
  const operation = "GitVcsDriver.fetchRepoDefault";
  const { remoteName } = bound;
  const advertised = yield* bound.run({
    operation,
    args: ["ls-remote", "--symref", "--exit-code", "--", remoteName, "HEAD"],
    timeoutMs: 15_000,
    maxOutputBytes: 8_192,
  });
  const lines = advertised.stdout.split("\n");
  const branch = lines.flatMap((line) => {
    const match = /^ref: refs\/heads\/([^\t]+)\tHEAD$/.exec(line);
    return match?.[1] === undefined ? [] : [match[1]];
  });
  const commits = lines.flatMap((line) => {
    const match = /^([a-f0-9]{40}|[a-f0-9]{64})\tHEAD$/.exec(line);
    return match?.[1] === undefined ? [] : [match[1]];
  });
  const [branchName] = branch;
  const [commitSha] = commits;
  if (
    advertised.stdoutTruncated ||
    branch.length !== 1 ||
    commits.length !== 1 ||
    branchName === undefined ||
    commitSha === undefined ||
    /^0+$/.test(commitSha)
  ) {
    return yield* new GitCommandError({
      operation,
      command: "git",
      cwd: bound.cwd,
      detail: "The configured remote did not advertise a default branch and commit.",
    });
  }
  yield* bound.read({ operation, args: ["check-ref-format", `refs/heads/${branchName}`] });
  const readObjectType = bound.read({
    operation,
    args: ["--no-replace-objects", "cat-file", "-t", commitSha],
    timeoutMs: 5_000,
    allowNonZeroExit: true,
  });
  const local = yield* readObjectType;
  if (local.exitCode === 0 && !local.stdoutTruncated && local.stdout === "commit\n") {
    return { remoteName, branchName, commitSha } satisfies VerifiedRepoDefault;
  }
  // The remote's live advertisement selects the exact SHA. Materialize that SHA
  // without writing any checkout or tracking ref, even for custom refspecs.
  yield* bound.run({
    operation,
    args: [
      "fetch",
      "--quiet",
      "--no-tags",
      "--no-prune",
      "--no-prune-tags",
      "--no-auto-gc",
      "--no-recurse-submodules",
      "--no-write-fetch-head",
      "--refmap=",
      "--",
      remoteName,
      commitSha,
    ],
    timeoutMs: 30_000,
  });
  const object = yield* readObjectType;
  if (object.exitCode !== 0 || object.stdoutTruncated || object.stdout !== "commit\n")
    return yield* invalidRemoteFetch(bound.cwd);
  return { remoteName, branchName, commitSha } satisfies VerifiedRepoDefault;
});
