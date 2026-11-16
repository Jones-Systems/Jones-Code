import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex } from "@noble/hashes/utils.js";

export const SHELL_RUN_STORAGE_KEY = "jones.shell-destination-runs.v1";
export const SHELL_RUN_TTL = 7 * 24 * 60 * 60 * 1000;
const MAX_RUNS = 32;
const MAX_STORAGE_LENGTH = 1_200_000;
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

const messageRunKeyPattern = /^m1:([a-f0-9]{64}):[a-f0-9]{64}$/;

function digest(value: unknown): string {
  return bytesToHex(sha256(new TextEncoder().encode(JSON.stringify(value))));
}

export function shellRunKey(
  sourceEnvironmentId: string,
  threadId: string,
  blockPosition: number,
  destinationId: string,
  code: string,
  messageId: string,
): string {
  if (!messageId?.trim()) return "";
  // Retain the old lookup without assigning its ambiguous terminal to this message.
  const legacyKey = digest([sourceEnvironmentId, threadId, blockPosition, destinationId, code]);
  return `m1:${legacyKey}:${digest(messageId)}`;
}

export function legacyShellRunKey(key: string): string | null {
  return messageRunKeyPattern.exec(key)?.[1] ?? null;
}

function isRun(value: unknown, now: number): value is ShellRun {
  if (!value || typeof value !== "object") return false;
  const run = value as Record<string, unknown>;
  return (
    typeof run.key === "string" &&
    (/^[a-f0-9]{64}$/.test(run.key) || messageRunKeyPattern.test(run.key)) &&
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

// The browser owns lock lifetime, including release when a tab disappears.
// Per-run locks span terminal effects; the storage lock spans only read/modify/write.
export interface ShellRunCoordination {
  exclusive<T>(name: string, work: () => Promise<T>): Promise<T>;
  tryExclusive<T>(name: string, work: () => Promise<T>): Promise<T | undefined>;
}
function browserCoordination(): ShellRunCoordination | null {
  const locks = globalThis.navigator?.locks;
  return locks
    ? {
        exclusive: (name, work) => locks.request(name, work),
        tryExclusive: (name, work) =>
          locks.request(name, { ifAvailable: true }, (lock) => (lock ? work() : undefined)),
      }
    : null;
}

export class ShellRunStore {
  private runs = new Map<string, ShellRun>();
  private listeners = new Set<() => void>();
  private active = new Set<string>();
  constructor(
    private readonly storage: Storage | null,
    private readonly now = Date.now,
    private readonly coordination: ShellRunCoordination | null = browserCoordination(),
  ) {
    this.observe();
  }
  private read(): Map<string, ShellRun> {
    if (!this.storage) throw new Error("Run tracking storage is unavailable; no command was sent.");
    const raw = this.storage.getItem(SHELL_RUN_STORAGE_KEY);
    if (raw && raw.length > MAX_STORAGE_LENGTH) throw new Error("Run tracking storage is invalid.");
    const values: unknown = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(values)) throw new Error("Run tracking storage is invalid.");
    const runs = new Map<string, ShellRun>();
    for (const value of values) {
      if (!isRun(value, this.now())) continue;
      const { key, environmentId, terminalId, cwd, phase, updatedAt } = value;
      runs.set(key, { key, environmentId, terminalId, cwd, phase, updatedAt });
    }
    if (runs.size > MAX_RUNS) throw new Error("Run tracking storage is full.");
    return runs;
  }
  private adopt(runs: Map<string, ShellRun>): void {
    let changed = runs.size !== this.runs.size;
    for (const [key, value] of runs) {
      const run =
        !this.active.has(key) && (value.phase === "opening" || value.phase === "writing")
          ? { ...value, phase: "writeUnknown" as const }
          : value;
      const old = this.runs.get(key);
      if (old && JSON.stringify(old) === JSON.stringify(run)) runs.set(key, old);
      else {
        runs.set(key, run);
        changed = true;
      }
    }
    this.runs = runs;
    if (changed) this.emit();
  }
  // Observers never replace shared state with their private snapshot.
  observe = (): void => {
    try {
      this.adopt(this.read());
    } catch {
      // Retain conservative local evidence. Execution rereads strictly under a lock.
    }
  };
  async withRunLock<T>(key: string, work: () => Promise<T>, wait = true): Promise<T | undefined> {
    if (!this.coordination) throw new Error("Cross-tab run coordination is unavailable.");
    const acquire = wait ? this.coordination.exclusive : this.coordination.tryExclusive;
    return acquire(`${SHELL_RUN_STORAGE_KEY}:run:${key}`, async () => {
      this.adopt(this.read());
      this.active.add(key);
      try {
        return await work();
      } finally {
        this.active.delete(key);
      }
    });
  }
  get = (key: string): ShellRun | null => {
    const run = this.runs.get(key);
    return run && this.now() - run.updatedAt < SHELL_RUN_TTL ? run : null;
  };
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };
  private async update(change: (runs: Map<string, ShellRun>) => void): Promise<void> {
    if (!this.coordination) throw new Error("Cross-tab run coordination is unavailable.");
    await this.coordination.exclusive(`${SHELL_RUN_STORAGE_KEY}:storage`, async () => {
      const next = this.read();
      change(next);
      const serialized = JSON.stringify([...next.values()]);
      if (serialized.length > MAX_STORAGE_LENGTH) throw new Error("Run tracking storage is full.");
      this.storage!.setItem(SHELL_RUN_STORAGE_KEY, serialized);
      this.adopt(next);
    });
  }
  async save(run: ShellRun): Promise<void> {
    await this.update((next) => {
      if (!next.has(run.key) && next.size >= MAX_RUNS) {
        // Evicting an unclosed association could turn an unknown delivery into a new Play.
        // Legacy records also guard other messages; a new run must not remove that evidence.
        const closed = [...next.values()].find(
          (value) => value.phase === "closed" && legacyShellRunKey(value.key) !== null,
        );
        if (!closed) throw new Error("Run tracking storage is full; close a previous run first.");
        next.delete(closed.key);
      }
      next.set(run.key, run);
    });
  }
  async mark(key: string, phase: RunPhase, terminalId = this.get(key)?.terminalId): Promise<void> {
    try {
      await this.update((next) => {
        const previous = next.get(key);
        if (previous?.phase === "writeUnknown" && phase === "exited") return;
        if (previous && previous.terminalId === terminalId)
          next.set(key, { ...previous, phase, updatedAt: this.now() });
      });
    } catch {
      // A durable opening/writing record recovers as unknown after storage failure.
      const previous = this.get(key);
      if (previous && previous.terminalId === terminalId) {
        this.runs.set(key, { ...previous, phase: "writeUnknown" });
        this.emit();
      }
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
    const store = clientStore;
    globalThis.addEventListener?.("storage", (event) => {
      if (event.key === SHELL_RUN_STORAGE_KEY || event.key === null) store.observe();
    });
    globalThis.addEventListener?.("focus", store.observe);
  }
  return clientStore;
}
