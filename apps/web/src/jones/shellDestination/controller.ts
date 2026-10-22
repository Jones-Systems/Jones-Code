import type { ShellDestination } from "./model";
import { shellCommandEligibility } from "./model";
import { ShellRunStore, legacyShellRunKey, type ShellRun } from "./runStore";

export const SHELL_RUN_THREAD_ID = "jones-shell-run";
type Target = { environmentId: string; input: { threadId: string; terminalId: string } };
export interface ShellTerminalPort {
  readonly open: (target: Target & { input: { cwd: string } }) => Promise<boolean>;
  readonly write: (target: Target & { input: { data: string } }) => Promise<boolean>;
  readonly close: (target: Target & { input: { deleteHistory: true } }) => Promise<boolean>;
}

export class ShellRunController {
  private pending = new Set<string>();
  constructor(
    readonly store: ShellRunStore,
    private readonly uuid: () => string,
    private readonly now = Date.now,
  ) {}

  async run(input: {
    key: string;
    code: string;
    complete: boolean;
    destination: () => ShellDestination;
    terminal: ShellTerminalPort;
    again?: boolean;
    expectedTerminalId?: string;
  }): Promise<void> {
    const legacyKey = legacyShellRunKey(input.key);
    if (!legacyKey || this.pending.has(input.key)) return;
    const observedTerminalId = input.expectedTerminalId ?? this.store.get(input.key)?.terminalId;
    this.pending.add(input.key);
    try {
      await this.store.withRunLock(input.key, async () => {
        const existing = this.store.get(input.key);
        // Recheck after lock acquisition: a stale tab may not have observed old evidence.
        if ((existing || this.store.get(legacyKey)) && !input.again) return;
        // A stale Run again action must not replace a newer run from another tab.
        if (input.again && existing?.terminalId !== observedTerminalId) return;
        const eligibility = shellCommandEligibility(input.code, input.complete);
        const destination = input.destination();
        if (
          eligibility.reason ||
          destination.status !== "ready" ||
          !destination.environmentId ||
          !destination.cwd
        )
          return;
        const run: ShellRun = {
          key: input.key,
          environmentId: destination.environmentId,
          cwd: destination.cwd,
          terminalId: `run-${this.uuid()}`,
          phase: "opening",
          updatedAt: this.now(),
        };
        const target = {
          environmentId: run.environmentId,
          input: { threadId: SHELL_RUN_THREAD_ID, terminalId: run.terminalId },
        };
        try {
          await this.store.save(run);
          const opened = await input.terminal.open({
            ...target,
            input: { ...target.input, cwd: run.cwd },
          });
          // Even an interrupted open may have reached the server. Never auto-retry
          // or infer that an unacknowledged operation had no effect.
          if (!opened || input.destination().status !== "ready") {
            await this.store.mark(run.key, "writeUnknown");
            return;
          }
          await this.store.save({ ...run, phase: "writing", updatedAt: this.now() });
          const wrote = await input.terminal.write({
            ...target,
            input: { ...target.input, data: `${eligibility.command}\r` },
          });
          await this.store.mark(run.key, wrote ? "running" : "writeUnknown");
        } catch (error) {
          if (this.store.get(run.key)?.terminalId === run.terminalId)
            await this.store.mark(run.key, "writeUnknown");
          else throw error;
        }
      });
    } finally {
      this.pending.delete(input.key);
    }
  }

  async close(
    key: string,
    terminal: ShellTerminalPort,
    canOperate: () => boolean,
    observedTerminalId = this.store.get(key)?.terminalId,
  ): Promise<boolean> {
    if (this.pending.has(key)) return false;
    this.pending.add(key);
    try {
      return (
        (await this.store.withRunLock(key, async () => {
          const run = this.store.get(key);
          if (!run || run.terminalId !== observedTerminalId || !canOperate()) return false;
          const closed = await terminal.close({
            environmentId: run.environmentId,
            input: {
              threadId: SHELL_RUN_THREAD_ID,
              terminalId: run.terminalId,
              deleteHistory: true,
            },
          });
          if (closed) await this.store.mark(key, "closed", run.terminalId);
          return closed;
        })) ?? false
      );
    } finally {
      this.pending.delete(key);
    }
  }

  async reconcile(
    key: string,
    sessions: readonly { terminalId: string; status: string }[] | null,
  ): Promise<void> {
    const observed = this.store.get(key);
    if (!observed || !sessions || this.pending.has(key)) return;
    await this.store.withRunLock(
      key,
      async () => {
        const run = this.store.get(key);
        if (
          !run ||
          run.terminalId !== observed.terminalId ||
          run.phase !== observed.phase ||
          run.phase === "closed"
        )
          return;
        const session = sessions.find((entry) => entry.terminalId === run.terminalId);
        if (!session && run.phase !== "unavailable")
          await this.store.mark(key, "unavailable", run.terminalId);
        else if (session && run.phase === "unavailable")
          await this.store.mark(key, "writeUnknown", run.terminalId);
        else if (
          session?.status === "exited" &&
          run.phase !== "exited" &&
          run.phase !== "writeUnknown"
        )
          await this.store.mark(key, "exited", run.terminalId);
      },
      false,
    );
  }
}
