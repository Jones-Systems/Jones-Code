import * as Effect from "effect/Effect";

/** One holder at a time, served in the order callers asked. */
export interface FifoPermit {
  readonly withPermit: <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
}

interface Ticket {
  notify: (() => void) | undefined;
}

/**
 * The webhook runs of one task go in arrival order. Effect's semaphore wakes its waiters only
 * after a release, so a caller arriving in between takes the permit ahead of them; here a
 * caller waits behind everyone already in line. As in that semaphore, a woken waiter takes the
 * permit itself in an uninterruptible step, so an interrupt never strands a handed-over permit.
 */
export const makeFifoPermit = Effect.sync((): FifoPermit => {
  let held = false;
  const line: Array<Ticket> = [];

  const wakeHead = () => {
    if (!held) line[0]?.notify?.();
  };

  const turn = (ticket: Ticket) =>
    Effect.callback<void>((resume) => {
      if (!held && line[0] === ticket) return resume(Effect.void);
      ticket.notify = () => resume(Effect.void);
      return Effect.sync(() => {
        ticket.notify = undefined;
      });
    });

  const leave = (ticket: Ticket) =>
    Effect.sync(() => {
      const index = line.indexOf(ticket);
      if (index !== -1) line.splice(index, 1);
      // The next caller may now be at the head of an idle line.
      wakeHead();
    });

  const release = Effect.sync(() => {
    held = false;
    wakeHead();
  });

  return {
    withPermit: (self) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.suspend(() => {
          const ticket: Ticket = { notify: undefined };
          line.push(ticket);
          return Effect.onInterrupt(restore(turn(ticket)), () => leave(ticket)).pipe(
            Effect.andThen(
              Effect.suspend(() => {
                line.shift();
                held = true;
                return Effect.ensuring(restore(self), release);
              }),
            ),
          );
        }),
      ),
  };
});
