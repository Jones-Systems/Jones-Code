// @effect-diagnostics globalDate:off globalTimers:off - Recorder ownership follows native Promise callbacks.

type RecordingState = {
  readonly state:
    | "idle"
    | "starting"
    | "active"
    | "stopping"
    | "abandoning"
    | "unknown"
    | "unsupported";
  readonly startedAt?: string;
};

export class RecordingAttemptUnresolved extends Error {
  readonly outcome = "unknown";
  constructor() {
    super(
      "The recorder's native outcome is unknown. Reading status or taking a snapshot cannot reconcile it; await the exact original reply or use another tab.",
    );
  }
}

export class RecordingCaptureRefused extends Error {
  readonly outcome = "not_started";
  constructor() {
    super(
      "This capture action was not started because an earlier recorder on this tab is unresolved.",
    );
  }
}

/** Submitted commands remain owned until an answer, even after frame authority is revoked. */
export class RecordingCommandLog {
  private readonly pending = new Set<object>();
  private readonly waiters = new Set<() => void>();
  track<A>(send: () => Promise<A>, answeredError: () => boolean): Promise<A> {
    const entry = {};
    this.pending.add(entry);
    const answer = () => {
      this.pending.delete(entry);
      if (this.pending.size === 0) {
        for (const resolve of this.waiters) resolve();
        this.waiters.clear();
      }
    };
    let submitted: Promise<A>;
    try {
      submitted = send();
    } catch (cause) {
      submitted = Promise.reject(cause);
    }
    return submitted.then(
      (value) => {
        answer();
        return value;
      },
      (cause: unknown) => {
        if (answeredError()) answer();
        throw cause;
      },
    );
  }
  drain(): Promise<void> {
    return this.pending.size === 0
      ? Promise.resolve()
      : new Promise((resolve) => this.waiters.add(resolve));
  }
}

export class RecordingAttempt<T extends object> {
  readonly commands = new RecordingCommandLog();
  phase: "preparing" | "queued" | "capturing" | "active" | "stopping" | "abandoning" | "unknown" =
    "preparing";
  abandoned = false;
  frames = true;
  startSubmitted = false;
  stopConfirmed = false;
  startedAt: string | undefined;
  readonly owner: T;
  constructor(owner: T) {
    this.owner = owner;
  }
  revoke() {
    this.frames = false;
  }
}

/** Routing retirement does not remove a native recorder's barrier. */
export class RecordingAttempts<T extends object> {
  private readonly byKey = new Map<string, RecordingAttempt<T>>();
  private readonly byInstance = new WeakMap<T, RecordingAttempt<T>>();
  create(key: string, owner: T) {
    const attempt = new RecordingAttempt(owner);
    this.byKey.set(key, attempt);
    this.byInstance.set(owner, attempt);
    return attempt;
  }
  restore(key: string, expected: RecordingAttempt<T>, active: RecordingAttempt<T>) {
    if (this.byKey.get(key) === expected) this.byKey.set(key, active);
    if (this.byInstance.get(expected.owner) === expected)
      this.byInstance.set(expected.owner, active);
  }
  clear(key: string, attempt: RecordingAttempt<T>) {
    if (this.byKey.get(key) === attempt) this.byKey.delete(key);
    if (this.byInstance.get(attempt.owner) === attempt) this.byInstance.delete(attempt.owner);
  }
  assertCapture(key: string, owner: T, own?: RecordingAttempt<T>) {
    const attempt = this.byKey.get(key);
    if (
      attempt &&
      attempt !== own &&
      (attempt.owner !== owner || attempt.phase === "unknown" || attempt.phase === "abandoning")
    ) {
      throw new RecordingCaptureRefused();
    }
  }
  drop(owner: T) {
    const attempt = this.byInstance.get(owner);
    if (!attempt) return;
    attempt.revoke();
    attempt.abandoned = true;
    attempt.phase = "unknown";
  }
  project(key: string, owner: T, current: boolean, unsupported: boolean): RecordingState {
    if (unsupported) return { state: "unsupported" };
    const attempt = this.byKey.get(key);
    if (!current || (attempt && attempt.owner !== owner)) return { state: "unknown" };
    if (!attempt) return { state: "idle" };
    const state =
      attempt.phase === "preparing" || attempt.phase === "queued" || attempt.phase === "capturing"
        ? "starting"
        : attempt.phase;
    return {
      state,
      ...((state === "active" || state === "stopping") && attempt.startedAt
        ? { startedAt: attempt.startedAt }
        : {}),
    };
  }
}

/** Only preparation/queueing can return while a provably inert continuation remains. */
export const recordingDeadline = <T extends object, A>(input: {
  attempt: RecordingAttempt<T>;
  deadlineMs: number;
  notStarted: () => Error;
  abandon: () => void;
  run: () => Promise<A>;
}): Promise<A> => {
  const early = Promise.withResolvers<never>();
  const timer = setTimeout(
    () => {
      if (input.attempt.phase === "active") return;
      input.attempt.abandoned = true;
      input.attempt.revoke();
      const inert = input.attempt.phase === "preparing" || input.attempt.phase === "queued";
      if (!inert) input.attempt.phase = "abandoning";
      input.abandon();
      if (inert) early.reject(input.notStarted());
    },
    Math.max(0, input.deadlineMs - Date.now()),
  );
  return Promise.race([input.run(), early.promise]).finally(() => clearTimeout(timer));
};

/** Old resources are independent; neither a delayed detach nor scratch close gates proof. */
export const fireRecordingCleanup = (
  encoder: { close(): Promise<unknown> } | null,
  session: { detach(): Promise<unknown> } | null,
) => {
  if (session)
    void Promise.resolve()
      .then(() => session.detach())
      .catch(() => undefined);
  if (encoder)
    void Promise.resolve()
      .then(() => encoder.close())
      .catch(() => undefined);
};
