import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex } from "@noble/hashes/utils.js";

export const SHELL_RUN_STORAGE_KEY = "jones.shell-destination-runs.v1";
export const SHELL_RUN_TTL = 7 * 24 * 60 * 60 * 1000;
const MAX_RUNS = 32;
export type RunPhase =
  | "opening"
  | "writing"
  | "running"
  | "exited"
  | "writeUnknown"
  | "unavailable"
  | "closed";
export interface ShellRun {
  readonly key: string;
  readonly environmentId: string;
  readonly terminalId: string;
  readonly cwd: string;
  readonly phase: RunPhase;
  readonly updatedAt: number;
}
type Storage = Pick<globalThis.Storage, "getItem" | "setItem">;
const phases = new Set<string>([
  "opening",
  "writing",
  "running",
  "exited",
  "writeUnknown",
  "unavailable",
  "closed",
]);

export function shellRunKey(
  sourceEnvironmentId: string,
  threadId: string,
  blockPosition: number,
  destinationId: string,
  code: string,
): string {
  return bytesToHex(
    sha256(
      new TextEncoder().encode(
        JSON.stringify([sourceEnvironmentId, threadId, blockPosition, destinationId, code]),
      ),
    ),
  );
}

function isRun(value: unknown, now: number): value is ShellRun {
  if (!value || typeof value !== "object") return false;
  const run = value as Record<string, unknown>;
  return (
    typeof run.key === "string" &&
    /^[a-f0-9]{64}$/.test(run.key) &&
    typeof run.environmentId === "string" &&
    run.environmentId.length > 0 &&
    run.environmentId.length <= 1024 &&
    typeof run.terminalId === "string" &&
    /^run-[a-f0-9-]{36}$/.test(run.terminalId) &&
    typeof run.cwd === "string" &&
    run.cwd.length > 0 &&
    run.cwd.length <= 32_768 &&
    typeof run.phase === "string" &&
    phases.has(run.phase) &&
    typeof run.updatedAt === "number" &&
    Number.isFinite(run.updatedAt) &&
    run.updatedAt <= now &&
    now - run.updatedAt < SHELL_RUN_TTL
  );
}

export class ShellRunStore {
  private runs = new Map<string, ShellRun>();
  private listeners = new Set<() => void>();
  constructor(
    private readonly storage: Storage | null,
    private readonly now = Date.now,
  ) {
    try {
      const raw = storage?.getItem(SHELL_RUN_STORAGE_KEY);
      const values: unknown = raw && raw.length <= 1_200_000 ? JSON.parse(raw) : [];
      if (Array.isArray(values))
        for (const value of values.slice(-MAX_RUNS)) {
          if (!isRun(value, now())) continue;
          const { key, environmentId, terminalId, cwd, phase, updatedAt } = value;
          this.runs.set(key, {
            key,
            environmentId,
            terminalId,
            cwd,
            updatedAt,
            phase: phase === "opening" || phase === "writing" ? "writeUnknown" : phase,
          });
        }
    } catch {
      /* Invalid or unavailable client storage cannot establish a prior run. */
    }
  }
  get = (key: string): ShellRun | null => {
    const run = this.runs.get(key);
    return run && this.now() - run.updatedAt < SHELL_RUN_TTL ? run : null;
  };
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };
  save(run: ShellRun): void {
    const next = new Map(this.runs);
    for (const [key, value] of next)
      if (this.now() - value.updatedAt >= SHELL_RUN_TTL) next.delete(key);
    next.delete(run.key);
    next.set(run.key, run);
    while (next.size > MAX_RUNS) next.delete(next.keys().next().value!);
    if (!this.storage) throw new Error("Run tracking storage is unavailable; no command was sent.");
    this.storage.setItem(SHELL_RUN_STORAGE_KEY, JSON.stringify([...next.values()]));
    this.runs = next;
    this.emit();
  }
  mark(key: string, phase: RunPhase): void {
    const previous = this.get(key);
    if (!previous) return;
    const next = { ...previous, phase, updatedAt: this.now() };
    try {
      this.save(next);
    } catch {
      // Keep the UI conservative even if storage fails after an RPC. Its last
      // durable opening/writing record also recovers as unknown on reload.
      this.runs.set(key, next);
      this.emit();
    }
  }
  private emit(): void {
    for (const listener of this.listeners) listener();
  }
}

let clientStore: ShellRunStore | undefined;
export function shellRunStore(): ShellRunStore {
  if (!clientStore) {
    let storage: Storage | null = null;
    try {
      storage = globalThis.localStorage;
    } catch {
      /* Fail closed at save. */
    }
    clientStore = new ShellRunStore(storage);
  }
  return clientStore;
}
