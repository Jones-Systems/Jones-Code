// @effect-diagnostics globalDate:off globalTimers:off - Native Promise queue deadlines share Playwright callback time and retain active work until settlement.
import { PreviewAutomationControlReason } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class BrowserControlInterrupted extends Schema.TaggedError<BrowserControlInterrupted>()(
  "BrowserControlInterrupted",
  {
    message: Schema.String.pipe(
      Schema.withConstructorDefault(
        Effect.succeed("Browser control changed. Refresh the snapshot before trying again."),
      ),
    ),
    reason: PreviewAutomationControlReason.pipe(
      Schema.withConstructorDefault(Effect.succeed("interrupted" as const)),
    ),
  },
) {}

export class BrowserActionNotStarted extends Error {
  constructor() {
    super("The preview request deadline elapsed before its action started.");
  }
}

export const assertBrowserActionDeadline = (deadlineMs?: number) => {
  if (deadlineMs !== undefined && Date.now() >= deadlineMs) throw new BrowserActionNotStarted();
};

export class BrowserRequestDeadline {
  readonly deadlineMs: number;
  private started = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  constructor(timeoutMs: number) {
    this.deadlineMs = Date.now() + timeoutMs;
  }
  start = () => {
    assertBrowserActionDeadline(this.deadlineMs);
    this.started = true;
    if (this.timer !== undefined) clearTimeout(this.timer);
  };
  response<A>(work: Promise<A>): Promise<A> {
    const waiting = new Promise<never>((_, reject) => {
      this.timer = setTimeout(
        () => {
          if (!this.started) reject(new BrowserActionNotStarted());
        },
        Math.max(0, this.deadlineMs - Date.now()),
      );
    });
    return Promise.race([work, waiting]).finally(() => {
      if (this.timer !== undefined) clearTimeout(this.timer);
    });
  }
}
const settle = (work: Promise<unknown>) =>
  work.then(
    () => undefined,
    () => undefined,
  );

/** Reserves control immediately, but drains running work before the new owner can act. */
export class SessionControl {
  readonly agentId: string | null;
  private readonly onGenerationChange: () => void;
  private tail: Promise<unknown> = Promise.resolve();
  private latestHuman: {
    entry: { viewerId: string; key: string; input: unknown; run: (input: never) => Promise<void> };
    result: Promise<void>;
  } | null = null;
  private pending: Promise<void> = Promise.resolve();
  private readonly handoff = new Set<Promise<void>>();
  private owner: string | null = null;
  private epoch = 0;
  private closed = false;

  constructor(agentId: string | null, onGenerationChange: () => void = () => {}) {
    this.agentId = agentId;
    this.onGenerationChange = onGenerationChange;
  }

  get controller() {
    return this.owner;
  }

  get generation() {
    return this.epoch;
  }

  private enqueue<A>(run: () => Promise<A>): Promise<A> {
    this.latestHuman = null;
    const result = this.tail.then(run);
    this.tail = result.then(
      () => this.pending,
      () => this.pending,
    );
    return result;
  }

  /** An action can respond before its navigation commits, while later actions still wait. */
  track(work: Promise<unknown>) {
    this.pending = Promise.all([this.pending, settle(work)]).then(() => undefined);
  }

  /** Viewer input stays responsive, but control handoffs still drain its navigations. */
  trackUntilHandoff(work: Promise<unknown>) {
    const settled = settle(work).finally(() => {
      this.handoff.delete(settled);
    });
    this.handoff.add(settled);
  }

  private assertOpen() {
    if (this.closed)
      throw new BrowserControlInterrupted({
        message: "This browser tab is closed.",
        reason: "closed",
      });
  }

  private changeOwner(owner: string | null) {
    this.latestHuman = null;
    this.owner = owner;
    this.epoch++;
    this.onGenerationChange();
  }

  private async action<A>(allowed: () => boolean, run: () => Promise<A>, deadlineMs?: number) {
    // The receive budget and queue deadline share Date.now(); do not compare to performance.now().
    assertBrowserActionDeadline(deadlineMs);
    this.assertOpen();
    if (!allowed())
      throw new BrowserControlInterrupted({ message: "You do not control this browser tab." });
    const epoch = this.epoch;
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
    const queued = this.enqueue(async () => {
      if (deadlineTimer !== undefined) clearTimeout(deadlineTimer);
      assertBrowserActionDeadline(deadlineMs);
      this.assertOpen();
      if (epoch !== this.epoch || !allowed()) throw new BrowserControlInterrupted();
      return run();
    });
    if (deadlineMs === undefined) return queued;
    const waiting = new Promise<never>((_, reject) => {
      deadlineTimer = setTimeout(
        () => reject(new BrowserActionNotStarted()),
        Math.max(0, deadlineMs - Date.now()),
      );
    });
    // Expiring a waiter does not release the running action's queue permit.
    return Promise.race([queued, waiting]).finally(() => {
      if (deadlineTimer !== undefined) clearTimeout(deadlineTimer);
    });
  }

  agentMayAct(agentId: string) {
    return this.agentId === null || this.agentId === agentId;
  }

  /**
   * An agent action. It may act on its own tab or on one a human opened, but
   * never while a human controls the tab; taking control interrupts it.
   */
  agent<A>(agentId: string, run: () => Promise<A>, deadlineMs?: number) {
    if (!this.agentMayAct(agentId))
      return Promise.reject(
        new BrowserControlInterrupted({
          message: "This tab belongs to another agent.",
          reason: "agentMismatch",
        }),
      );
    if (this.owner !== null)
      return Promise.reject(
        new BrowserControlInterrupted({
          message: "A human controls this tab.",
          reason: "humanControl",
        }),
      );
    return this.action(() => this.agentMayAct(agentId) && this.owner === null, run, deadlineMs);
  }

  agentCanActNow(agentId: string) {
    return this.agentMayAct(agentId) && this.owner === null;
  }

  /** Read without control or queueing so agents can observe without delaying human input. */
  observe<A>(run: () => Promise<A>) {
    this.assertOpen();
    return run();
  }

  system<A>(run: () => Promise<A>) {
    this.assertOpen();
    return this.enqueue(async () => {
      this.assertOpen();
      return run();
    });
  }

  human<A>(viewerId: string, run: () => Promise<A>) {
    return this.action(() => this.owner === viewerId, run);
  }

  /**
   * Merge into only adjacent waiting work with the same key; the running action and
   * intervening actions keep their order. Work sharing a key must share its input type.
   */
  humanLatest<A>(
    viewerId: string,
    key: string,
    input: A,
    run: (input: A) => Promise<void>,
    merge: (waiting: A, next: A) => A = (_waiting, next) => next,
  ): Promise<void> {
    if (this.closed || this.owner !== viewerId) return this.human(viewerId, () => run(input));
    const latest = this.latestHuman;
    if (latest?.entry.viewerId === viewerId && latest.entry.key === key) {
      latest.entry.input = merge(latest.entry.input as A, input);
      latest.entry.run = run;
      return latest.result;
    }
    const entry = { viewerId, key, input, run };
    const result = this.human(viewerId, async () => {
      if (this.latestHuman?.entry === entry) this.latestHuman = null;
      await entry.run(entry.input);
    });
    this.latestHuman = { entry, result };
    return result;
  }

  async take(viewerId: string) {
    this.assertOpen();
    if (this.owner !== null && this.owner !== viewerId) {
      throw new BrowserControlInterrupted({ message: "Another viewer controls this browser tab." });
    }
    if (this.owner !== viewerId) this.changeOwner(viewerId);
    return this.human(viewerId, async () => {});
  }

  async release(viewerId: string, afterDrain: () => Promise<void> = async () => {}) {
    this.assertOpen();
    if (this.owner !== viewerId) {
      throw new BrowserControlInterrupted({
        message: "Only the controlling viewer can release this browser tab.",
      });
    }
    this.changeOwner(null);
    await this.enqueue(async () => {
      await Promise.all(this.handoff);
      await afterDrain();
    });
  }

  async disconnect(viewerId: string, afterDrain?: () => Promise<void>) {
    if (!this.closed && this.owner === viewerId) await this.release(viewerId, afterDrain);
  }

  async close() {
    if (!this.closed) {
      this.closed = true;
      this.changeOwner(null);
    }
    await this.tail;
  }
}
