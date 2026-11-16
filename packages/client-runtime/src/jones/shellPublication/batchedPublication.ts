import { Atom } from "effect/reactivity";

/**
 * Stream publications otherwise rebuild an aggregate once per changed member.
 * Forward each received notification synchronously in one batch, without adding
 * a timer. Listener-triggered writes are additional mutations. Inside an outer
 * batch the wrapper updates when the source notifies, rather than reading through.
 */
export const publishInBatches = <A>(self: Atom.Atom<A>): Atom.Atom<A> =>
  Atom.transform(
    self,
    (get) => {
      get.subscribe(self, (value) => Atom.batch(() => get.setSelf(value)));
      return get.once(self);
    },
    { initialValueTarget: self },
  );

/** Retain quiet list values without mounting one reactive point-read for every row. */
export function createQuietShellList<A>(
  keyOf: (value: A) => string,
  sameStreaming: (previous: A, next: A) => boolean,
) {
  let retained = new Map<string, A>();
  let previous: ReadonlyArray<A> = [];
  return (rows: ReadonlyArray<A>): ReadonlyArray<A> => {
    const current = new Map<string, A>();
    const next: A[] = [];
    for (const row of rows) {
      const key = keyOf(row);
      const before = retained.get(key);
      const value =
        before !== undefined && (before === row || sameStreaming(before, row)) ? before : row;
      current.set(key, value);
      next.push(value);
    }
    retained = current;
    if (previous.length === next.length && previous.every((value, index) => value === next[index]))
      return previous;
    previous = next;
    return next;
  };
}
