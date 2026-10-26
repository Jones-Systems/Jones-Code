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
