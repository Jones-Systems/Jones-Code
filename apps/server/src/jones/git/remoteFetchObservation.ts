import type { GitCommandError, GitCommandFailureReason } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

export interface RemoteFetchTarget {
  readonly remoteName: string;
  readonly coverage:
    | { readonly kind: "configured-refspecs" }
    | { readonly kind: "branch"; readonly branchName: string };
}

type FetchResult =
  | { readonly status: "succeeded" }
  | { readonly status: "failed"; readonly reason?: GitCommandFailureReason };

export interface RemoteFetchObservation {
  readonly latestVerifiedAtMs: number | null;
  readonly latestOutcome: (FetchResult & { readonly completedAtMs: number }) | null;
}

const unknown: RemoteFetchObservation = {
  latestVerifiedAtMs: null,
  latestOutcome: null,
};

const observationKey = (gitCommonDir: string, { remoteName, coverage }: RemoteFetchTarget) =>
  JSON.stringify([
    gitCommonDir,
    remoteName,
    coverage.kind,
    "branchName" in coverage ? coverage.branchName : null,
  ]);

/** Driver-lifetime evidence. A fresh server starts unknown, even if tracking refs already exist. */
export function makeRemoteFetchObservation() {
  let sequence = 0;
  const entries = new Map<
    string,
    {
      readonly latest: RemoteFetchObservation;
      readonly outcomeSequence: number;
      readonly successSequence: number;
    }
  >();

  // Called within the existing fetch spans. The sequence orders overlapping attempts independently
  // of wall-clock time, including an older success that finishes after a newer failure.
  const observe = Effect.fnUntraced(function* <A, R>(
    gitCommonDir: string | null,
    target: RemoteFetchTarget,
    run: Effect.Effect<A, GitCommandError, R>,
    classify: (value: A) => FetchResult,
  ) {
    if (gitCommonDir === null || target.remoteName === "." || target.remoteName === "") {
      return yield* run;
    }
    const key = observationKey(gitCommonDir, target);
    const attempt = ++sequence;
    const record = (result: FetchResult) =>
      Clock.currentTimeMillis.pipe(
        Effect.map((completedAtMs) => {
          const prior = entries.get(key);
          const outcomeSequence = prior?.outcomeSequence ?? 0;
          const successSequence = prior?.successSequence ?? 0;
          const isLatestOutcome = attempt > outcomeSequence;
          const isLatestSuccess = result.status === "succeeded" && attempt > successSequence;
          if (!isLatestOutcome && !isLatestSuccess) return;
          entries.set(key, {
            latest: {
              latestVerifiedAtMs: isLatestSuccess
                ? completedAtMs
                : (prior?.latest.latestVerifiedAtMs ?? null),
              latestOutcome: isLatestOutcome
                ? { ...result, completedAtMs }
                : (prior?.latest.latestOutcome ?? null),
            },
            outcomeSequence: isLatestOutcome ? attempt : outcomeSequence,
            successSequence: isLatestSuccess ? attempt : successSequence,
          });
        }),
      );

    return yield* run.pipe(
      Effect.tap((value) => record(classify(value))),
      Effect.tapError((error) =>
        record(error.reason ? { status: "failed", reason: error.reason } : { status: "failed" }),
      ),
    );
  });

  return {
    observe,
    read: (gitCommonDir: string | null, target: RemoteFetchTarget): RemoteFetchObservation =>
      (gitCommonDir === null
        ? undefined
        : entries.get(observationKey(gitCommonDir, target))?.latest) ?? unknown,
  };
}
