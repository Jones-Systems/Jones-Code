import * as Cache from "effect/Cache";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { type GitProcessTotals, provideGitProcessTotals } from "./gitTiming.ts";

// "joined" waited on a read another caller started (or its cached failure), so its
// Git totals stay zero: that read's subprocess time belongs to the starting call.
type ListRefsSnapshotSource = "hit" | "joined" | "miss" | "none";

interface ListRefsObservation {
  readonly git: GitProcessTotals;
  snapshotReads: number;
  snapshot: ListRefsSnapshotSource;
  repositoryPathsMs: number;
  snapshotMs: number;
}

const CurrentListRefsObservation = Context.Reference<ListRefsObservation | undefined>(
  "jones/git/ListRefsObservation",
  { defaultValue: () => undefined },
);

/** Annotates the enclosing listRefs span with snapshot source, phase times and Git admission. */
export const observeListRefs =
  (refresh: boolean) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.suspend(() => {
      const observation: ListRefsObservation = {
        git: { commands: 0, executionMs: 0, sharedPermitWaitMsMax: 0, backgroundGateWaitMsMax: 0 },
        snapshotReads: 0,
        snapshot: "none",
        repositoryPathsMs: 0,
        snapshotMs: 0,
      };
      return effect.pipe(
        provideGitProcessTotals(observation.git),
        Effect.provideService(CurrentListRefsObservation, observation),
        Effect.ensuring(
          Effect.suspend(() =>
            Effect.annotateCurrentSpan({
              "vcs.list_refs.refresh": refresh,
              "vcs.list_refs.snapshot": observation.snapshot,
              "vcs.list_refs.repository_paths_ms": observation.repositoryPathsMs,
              "vcs.list_refs.snapshot_ms": observation.snapshotMs,
              "vcs.list_refs.git_commands": observation.git.commands,
              "vcs.list_refs.git_execution_ms": observation.git.executionMs,
              "vcs.list_refs.git_shared_permit_wait_ms_max": observation.git.sharedPermitWaitMsMax,
              "vcs.list_refs.git_background_gate_wait_ms_max":
                observation.git.backgroundGateWaitMsMax,
            }),
          ),
        ),
      );
    });

export const timeListRefsPhase =
  (phase: "repositoryPathsMs" | "snapshotMs") =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const observation = yield* CurrentListRefsObservation;
      if (observation === undefined) return yield* effect;
      const startedAt = yield* Clock.currentTimeMillis;
      return yield* effect.pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;
            observation[phase] += Math.max(0, now - startedAt);
          }),
        ),
      );
    });

// Cache lookups fork from the starting caller, so a read marks that caller's observation.
export const markListRefsSnapshotRead = <A, E, R>(read: Effect.Effect<A, E, R>) =>
  Effect.flatMap(CurrentListRefsObservation, (observation) => {
    if (observation !== undefined) observation.snapshotReads += 1;
    return read;
  });

// No extra span: the classification lands on the enclosing listRefs span.
export const getObservedListRefsSnapshot = Effect.fnUntraced(function* <K, A, E, R>(
  cache: Cache.Cache<K, A, E, R>,
  key: K,
) {
  const observation = yield* CurrentListRefsObservation;
  if (observation === undefined) return yield* Cache.get(cache, key);
  const completed = Option.isSome(yield* Cache.getSuccess(cache, key));
  const readsBefore = observation.snapshotReads;
  const snapshot = yield* Cache.get(cache, key);
  observation.snapshot = completed
    ? "hit"
    : observation.snapshotReads > readsBefore
      ? "miss"
      : "joined";
  return snapshot;
});
