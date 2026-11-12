/** Thirty minutes of positively observed idle shell time, independent of clients. */
export const SHELL_IDLE_EXPIRY_MS = 30 * 60 * 1_000;

interface IdleObservation {
  readonly process: object;
  readonly mark: number;
  readonly since: number;
}

/** Uncertain inspection, activity, or process replacement starts a new interval. */
export class ShellIdleExpiry {
  private observations = new WeakMap<object, IdleObservation>();

  reset(session?: object): void {
    if (session) this.observations.delete(session);
    else this.observations = new WeakMap();
  }

  isDue(session: object, process: object | null, mark: number, now: number): boolean {
    const previous = this.observations.get(session);
    return (
      previous !== undefined &&
      previous.process === process &&
      previous.mark === mark &&
      now - previous.since >= SHELL_IDLE_EXPIRY_MS
    );
  }

  observe(
    session: object,
    process: object | null,
    mark: number,
    idle: boolean,
    now: number,
  ): boolean {
    if (!idle || process === null) {
      this.reset(session);
      return false;
    }
    const previous = this.observations.get(session);
    if (
      !previous ||
      previous.process !== process ||
      previous.mark !== mark ||
      now < previous.since
    ) {
      this.observations.set(session, { process, mark, since: now });
      return false;
    }
    return this.isDue(session, process, mark, now);
  }
}
