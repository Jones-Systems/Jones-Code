import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";

const BackgroundGitWork = Context.Reference<boolean>("jones/git/BackgroundGitWork", {
  defaultValue: () => false,
});
const backgroundGitProcesses = Semaphore.makeUnsafe(4);

export const asBackgroundGitWork = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.provideService(effect, BackgroundGitWork, true);

// Admission covers one subprocess, never provider or network waits. A joined
// computation keeps the classification of the fiber that started it.
// No extra span: the Git operation already covers both admission waits.
export const admitGitProcess = Effect.fnUntraced(function* <A, E, R>(run: Effect.Effect<A, E, R>) {
  const background = yield* BackgroundGitWork;
  return yield* background ? backgroundGitProcesses.withPermits(1)(run) : run;
});
