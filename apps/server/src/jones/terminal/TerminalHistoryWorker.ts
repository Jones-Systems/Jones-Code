import * as Effect from "effect/Effect";
import * as TxRef from "effect/TxRef";

interface HistoryRequest<History> {
  readonly history: History;
  readonly immediate: boolean;
}

/**
 * One writer keeps terminal history ordered. Background writes retain the
 * existing global debounce; explicit flushes bypass queued debounce waits.
 */
export const makeTerminalHistoryWorker = Effect.fn("TerminalHistoryWorker.make")(function* <
  History,
>(options: {
  readonly debounceMs: number;
  readonly process: (key: string, history: History) => Effect.Effect<void>;
}) {
  const stateRef = yield* TxRef.make({
    pending: new Map<string, HistoryRequest<History>>(),
    activeKey: null as string | null,
  });
  const awaitPending = TxRef.get(stateRef).pipe(
    Effect.flatMap((state) => (state.pending.size > 0 ? Effect.void : Effect.txRetry)),
    Effect.tx,
  );
  const awaitImmediate = TxRef.get(stateRef).pipe(
    Effect.flatMap((state) =>
      [...state.pending.values()].some((request) => request.immediate)
        ? Effect.void
        : Effect.txRetry,
    ),
    Effect.tx,
  );

  yield* Effect.gen(function* () {
    yield* awaitPending;
    // This wait holds no write or queue item. A flush wakes it and is selected
    // before background keys; an already running write is never interrupted.
    yield* Effect.raceFirst(Effect.sleep(options.debounceMs), awaitImmediate);
    const [key, request] = yield* TxRef.modify(stateRef, (state) => {
      const entries = [...state.pending.entries()];
      const selected = entries.find(([, request]) => request.immediate) ?? entries[0]!;
      const pending = new Map(state.pending);
      pending.delete(selected[0]);
      return [selected, { pending, activeKey: selected[0] }] as const;
    }).pipe(Effect.tx);

    yield* options
      .process(key, request.history)
      .pipe(
        Effect.ignoreCause,
        Effect.ensuring(
          TxRef.update(stateRef, (state) => ({ ...state, activeKey: null })).pipe(Effect.tx),
        ),
      );
  }).pipe(Effect.forever, Effect.forkScoped);

  const enqueue = (key: string, request: HistoryRequest<History>): Effect.Effect<void> =>
    TxRef.update(stateRef, (state) => {
      const pending = new Map(state.pending);
      pending.set(key, {
        history: request.history,
        immediate: request.immediate || (pending.get(key)?.immediate ?? false),
      });
      return { ...state, pending };
    }).pipe(Effect.tx);

  const drainKey = (key: string): Effect.Effect<void> =>
    TxRef.get(stateRef).pipe(
      Effect.flatMap((state) =>
        state.pending.has(key) || state.activeKey === key ? Effect.txRetry : Effect.void,
      ),
      Effect.tx,
    );

  return { enqueue, drainKey };
});
