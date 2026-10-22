import type { ShellDestination } from "./model";
import { shellCommandEligibility } from "./model";
import { ShellRunStore, type ShellRun } from "./runStore";

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
  }): Promise<void> {
    if (this.pending.has(input.key)) return;
    const existing = this.store.get(input.key);
    if (existing && !input.again) return;
    const eligibility = shellCommandEligibility(input.code, input.complete);
    const destination = input.destination();
    if (
      eligibility.reason ||
      destination.status !== "ready" ||
      !destination.environmentId ||
      !destination.cwd
    )
      return;
    this.pending.add(input.key);
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
      this.store.save(run);
      const opened = await input.terminal.open({
        ...target,
        input: { ...target.input, cwd: run.cwd },
      });
      // Even an interrupted open may have reached the server. Never auto-retry
      // or infer that an unacknowledged operation had no effect.
      if (!opened || input.destination().status !== "ready") {
        this.store.mark(run.key, "writeUnknown");
        return;
      }
      this.store.save({ ...run, phase: "writing", updatedAt: this.now() });
      const wrote = await input.terminal.write({
        ...target,
        input: { ...target.input, data: `${eligibility.command}\r` },
      });
      this.store.mark(run.key, wrote ? "running" : "writeUnknown");
    } catch (error) {
      if (this.store.get(run.key)?.terminalId === run.terminalId)
        this.store.mark(run.key, "writeUnknown");
      else throw error;
    } finally {
      this.pending.delete(input.key);
    }
  }

  async close(
    key: string,
    terminal: ShellTerminalPort,
    canOperate: () => boolean,
  ): Promise<boolean> {
    const run = this.store.get(key);
    if (!run || this.pending.has(key) || !canOperate()) return false;
    this.pending.add(key);
    try {
      const closed = await terminal.close({
        environmentId: run.environmentId,
        input: { threadId: SHELL_RUN_THREAD_ID, terminalId: run.terminalId, deleteHistory: true },
      });
      if (closed) this.store.mark(key, "closed");
      return closed;
    } finally {
      this.pending.delete(key);
    }
  }

  reconcile(key: string, sessions: readonly { terminalId: string; status: string }[] | null): void {
    const run = this.store.get(key);
    if (!run || !sessions || this.pending.has(key) || run.phase === "closed") return;
    const session = sessions.find((entry) => entry.terminalId === run.terminalId);
    if (!session && run.phase !== "unavailable") this.store.mark(key, "unavailable");
    else if (session && run.phase === "unavailable") this.store.mark(key, "writeUnknown");
    else if (session?.status === "exited" && run.phase !== "exited" && run.phase !== "writeUnknown")
      this.store.mark(key, "exited");
  }
}
