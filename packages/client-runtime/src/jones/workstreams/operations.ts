// @effect-diagnostics globalTimers:off globalDate:off - Abortable Promise polling is shared by React clients outside an Effect runtime.
import {
  WORKSTREAM_OPERATIONS_CONTRACT,
  parseWorkstreamOperationRequest,
  parseWorkstreamOperationResult,
  type WorkstreamOperationRequest,
  type WorkstreamOperationResult,
  type WorkstreamOperationReadRequest,
  type WorkstreamOperationReadResponse,
  type WorkstreamOperationSubmitRequest,
  type WorkstreamSetupStatus,
} from "@t3tools/contracts/jones/workstreamOperations";

export type OperationResult = WorkstreamOperationResult;
export interface WorkstreamOperationsTransport {
  submit: (
    payload: WorkstreamOperationSubmitRequest,
    signal?: AbortSignal,
  ) => Promise<OperationResult>;
  read: (
    payload: WorkstreamOperationReadRequest,
    signal?: AbortSignal,
  ) => Promise<WorkstreamOperationReadResponse>;
  setup: (signal?: AbortSignal) => Promise<WorkstreamSetupStatus>;
}
export interface OperationStorage {
  load: () => Promise<string | null>;
  save: (value: string) => Promise<void>;
}
export interface RetainedOperation {
  readonly key: string;
  readonly request: WorkstreamOperationRequest | null;
  readonly operationId: string;
  readonly result: OperationResult | null;
}
export const supportsWorkstreamOperations = (
  capabilities:
    | {
        readonly workstreamOperations?: { readonly contract: string };
      }
    | null
    | undefined,
) => capabilities?.workstreamOperations?.contract === WORKSTREAM_OPERATIONS_CONTRACT;
/** Operations use the signed instance ID from setup, never the legacy gateway origin. */
export function operationRegistryIdentity(status: WorkstreamSetupStatus, ownerId: string) {
  if (status.registry.state !== "ready")
    throw new Error(`Workstreams ${status.registry.state}. Check Workstreams setup in Settings.`);
  if (!status.registry.registry_id)
    throw new Error(
      "Workstream registry identity unavailable. Check Workstreams setup in Settings.",
    );
  if (status.registry.owner_id !== ownerId)
    throw new Error("Workstream change rejected: registry_mismatch.");
  return { registry_id: status.registry.registry_id, owner_id: status.registry.owner_id };
}
export const operationOpen = (result: OperationResult) =>
  result.state === "in_progress" || result.state === "reconciling";
export const operationSucceeded = (result: OperationResult) =>
  result.state === "committed" || result.state === "already_satisfied";
export const operationConfigFailure = (result: OperationResult) =>
  (result.state === "rejected" &&
    ["foreign_thread", "registry_mismatch", "permission_denied"].includes(result.reason)) ||
  (result.state === "unavailable" &&
    ["offline", "not_configured", "registration_unavailable"].includes(result.reason));
export const operationMayReplace = (result: OperationResult | null) =>
  result !== null && !operationOpen(result) && !operationConfigFailure(result);
export function operationMessage(result: OperationResult): string {
  if (result.state === "conflict")
    return `Assignment changed. Currently ${result.observed.kind === "primary" ? `in ${result.observed.name}` : "Unassigned"}. Retry to use this placement.`;
  if (result.state === "reconciling") return "Reconciling Workstream change…";
  if (result.state === "in_progress") return "Saving Workstream change…";
  if (operationSucceeded(result)) return "Workstream change saved.";
  return `Workstream change ${result.state}: ${result.reason}${result.detail && "availability" in result.detail ? ` (${result.detail.availability})` : ""}. Retry after resolving this condition.`;
}
export class WorkstreamOperationError extends Error {
  readonly result: OperationResult;
  constructor(result: OperationResult) {
    super(operationMessage(result));
    this.result = result;
  }
}
export class WorkstreamOperationJoined extends Error {
  readonly operationId: string;
  constructor(operationId: string) {
    super(
      `Another assignment is in progress (${operationId}). Waiting for its result; retry your choice afterwards.`,
    );
    this.operationId = operationId;
  }
}

const pause = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    signal?.throwIfAborted();
    const abort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", abort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", abort, { once: true });
  });
const intent = (request: WorkstreamOperationRequest) =>
  JSON.stringify(
    request.kind === "set_primary"
      ? { ...request, operation_id: "", expected: null }
      : { ...request, operation_id: "" },
  );

/** Retains exact request bytes before dispatch. A missing memory read never creates a new request. */
export class WorkstreamOperationsClient {
  private entries = new Map<string, RetainedOperation>();
  private runs = new Map<string, { intent: string; promise: Promise<OperationResult> }>();
  private listeners = new Set<() => void>();
  private ready: Promise<void> | undefined;
  private saves: Promise<void> = Promise.resolve();
  private watcher:
    | { controller: AbortController; promise: Promise<void>; users: number }
    | undefined;
  readonly transport: WorkstreamOperationsTransport;
  private readonly storage: OperationStorage;
  private readonly newId: () => Promise<string>;
  constructor(
    transport: WorkstreamOperationsTransport,
    storage: OperationStorage,
    newId: () => Promise<string>,
  ) {
    this.transport = transport;
    this.storage = storage;
    this.newId = newId;
  }
  snapshot = (): readonly RetainedOperation[] => [...this.entries.values()];
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  private emit() {
    for (const listener of this.listeners) listener();
  }
  private initialize() {
    return (this.ready ??= this.storage.load().then((raw) => {
      if (!raw) return;
      const rows: unknown = JSON.parse(raw);
      if (!Array.isArray(rows)) throw new Error("Workstream recovery data is invalid.");
      for (const row of rows) {
        if (
          typeof row !== "object" ||
          row === null ||
          typeof row.key !== "string" ||
          typeof row.operationId !== "string"
        )
          throw new Error("Workstream recovery data is invalid.");
        const request = row.request === null ? null : parseWorkstreamOperationRequest(row.request);
        const result = row.result === null ? null : parseWorkstreamOperationResult(row.result);
        if (
          (request && request.operation_id !== row.operationId) ||
          (result && result.operation_id !== row.operationId)
        )
          throw new Error("Workstream recovery identity changed.");
        this.entries.set(row.key, { key: row.key, operationId: row.operationId, request, result });
      }
      this.emit();
    }));
  }
  private async retain(entry: RetainedOperation) {
    if (entry.result && entry.result.operation_id !== entry.operationId)
      throw new Error("Workstream response operation ID does not match the retained request.");
    const previous = this.entries.get(entry.key);
    if (previous?.operationId === entry.operationId && previous.result?.state === "committed")
      entry = previous;
    this.entries.set(entry.key, entry);
    const finished = this.snapshot().filter((item) => operationMayReplace(item.result));
    for (const old of finished.slice(0, Math.max(0, finished.length - 1000)))
      this.entries.delete(old.key);
    const bytes = JSON.stringify(this.snapshot());
    this.saves = this.saves.catch(() => undefined).then(() => this.storage.save(bytes));
    await this.saves;
    this.emit();
  }
  run(
    key: string,
    make: (id: string) => WorkstreamOperationRequest,
    options: {
      signal?: AbortSignal | undefined;
      retry?: boolean | undefined;
      waitBudgetMs?: number | undefined;
    } = {},
  ): Promise<OperationResult> {
    const requested = intent(make("client-intent-preview"));
    const pending = this.runs.get(key);
    if (pending)
      return pending.intent === requested
        ? pending.promise
        : Promise.reject(new Error("Another Workstream choice is still being submitted."));
    const run = this.execute(key, make, options).finally(() => {
      this.runs.delete(key);
    });
    this.runs.set(key, { intent: requested, promise: run });
    return run;
  }
  private async execute(
    key: string,
    make: (id: string) => WorkstreamOperationRequest,
    options: {
      signal?: AbortSignal | undefined;
      retry?: boolean | undefined;
      waitBudgetMs?: number | undefined;
    },
  ): Promise<OperationResult> {
    await this.initialize();
    options.signal?.throwIfAborted();
    let entry = this.entries.get(key);
    if (entry?.request) {
      const same = intent(entry.request) === intent(make(entry.operationId));
      if (!same && !operationMayReplace(entry.result))
        throw new Error(
          "An earlier Workstream change must be reconciled before changing this choice. Retry the original choice.",
        );
      if (
        same &&
        entry.result &&
        operationSucceeded(entry.result) &&
        entry.request.kind === "create_workstream"
      )
        return entry.result;
      const succeeded = entry.result !== null && operationSucceeded(entry.result);
      const contention =
        entry.result?.state === "unavailable" && entry.result.reason === "contention";
      if (!same || succeeded || contention || (options.retry && entry.result?.state === "conflict"))
        entry = undefined;
      else if (entry.result?.state === "conflict") return entry.result;
    }
    if (!entry?.request) {
      const request = parseWorkstreamOperationRequest(make(await this.newId()));
      entry = { key, request, operationId: request.operation_id, result: null };
    }
    this.entries.delete(`joined:${key}`);
    await this.retain(entry);
    options.signal?.throwIfAborted();
    let result = await this.submitEntry(entry, 3000, options.signal);
    await this.retain({ ...entry, result });
    const deadline = Date.now() + (options.waitBudgetMs ?? 3000);
    while (operationOpen(result) && Date.now() + result.poll_after_ms! <= deadline) {
      await pause(result.poll_after_ms!, options.signal);
      result = (await this.refresh(key, options.signal)) ?? result;
    }
    return result;
  }
  private async submitEntry(
    entry: RetainedOperation,
    waitMs: number,
    signal?: AbortSignal,
  ): Promise<OperationResult> {
    const request = entry.request!;
    try {
      return await this.transport.submit({ request, wait_ms: waitMs }, signal);
    } catch (cause) {
      if (typeof cause === "object" && cause !== null && "code" in cause) {
        if (cause.code === "operation_id_conflict")
          throw new Error(
            "Workstream operation ID conflict. The retained request cannot be replaced safely.",
          );
        if (
          cause.code === "thread_operation_in_progress" &&
          "open_operation_id" in cause &&
          typeof cause.open_operation_id === "string"
        ) {
          const open = await this.transport.read({ operation_id: cause.open_operation_id }, signal);
          await this.retain({
            key: `joined:${entry.key}`,
            operationId: cause.open_operation_id,
            request: null,
            result: "operation" in open ? open.operation : null,
          });
          throw new WorkstreamOperationJoined(cause.open_operation_id);
        }
      }
      throw cause;
    }
  }
  async refresh(key: string, signal?: AbortSignal): Promise<OperationResult | null> {
    await this.initialize();
    const entry = this.entries.get(key);
    if (!entry) return null;
    const response = await this.transport.read({ operation_id: entry.operationId }, signal);
    if (!("operation" in response)) throw new Error("Unexpected Workstream operation read.");
    let result = response.operation;
    if (result === null && entry.request) {
      await this.retain(entry);
      result = await this.submitEntry(entry, 0, signal);
    }
    signal?.throwIfAborted();
    const current = this.entries.get(key);
    if (current?.operationId !== entry.operationId) return current?.result ?? null;
    await this.retain({ ...entry, result });
    return this.entries.get(key)?.result ?? null;
  }
  watch(signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    let watcher = this.watcher;
    if (!watcher || watcher.controller.signal.aborted) {
      const controller = new AbortController();
      const next = { controller, promise: Promise.resolve(), users: 0 };
      next.promise = this.watchLoop(controller.signal).finally(() => {
        if (this.watcher === next) this.watcher = undefined;
      });
      watcher = next;
      this.watcher = next;
    }
    const current = watcher;
    current.users += 1;
    return new Promise<void>((resolve, reject) => {
      let done = false;
      const finish = (cause?: unknown) => {
        if (done) return;
        done = true;
        signal.removeEventListener("abort", abort);
        current.users -= 1;
        if (current.users === 0) current.controller.abort();
        if (cause !== undefined) reject(cause);
        else resolve();
      };
      const abort = () => finish(signal.reason);
      signal.addEventListener("abort", abort, { once: true });
      current.promise.then(() => finish(), finish);
    });
  }
  private async watchLoop(signal: AbortSignal): Promise<void> {
    await this.initialize();
    const open = await this.transport.read({ open: true }, signal);
    if ("operations" in open) {
      for (const result of open.operations) {
        if (!this.snapshot().some((entry) => entry.operationId === result.operation_id))
          await this.retain({
            key: `discovered:${result.operation_id}`,
            operationId: result.operation_id,
            request: null,
            result,
          });
      }
    }
    const due = new Map<string, { at: number; result: OperationResult | null }>();
    while (!signal.aborted) {
      const active = this.snapshot().filter(
        (entry) =>
          !this.entries.has(`joined:${entry.key}`) &&
          ((entry.result === null && entry.request !== null) ||
            (entry.result !== null && operationOpen(entry.result))),
      );
      const activeKeys = new Set(active.map((entry) => entry.key));
      for (const key of due.keys()) if (!activeKeys.has(key)) due.delete(key);
      for (const entry of active) {
        if (this.runs.has(entry.key)) {
          due.delete(entry.key);
          continue;
        }
        const scheduled = due.get(entry.key);
        if (!scheduled || scheduled.result !== entry.result)
          due.set(entry.key, {
            at: Date.now() + (entry.result?.poll_after_ms ?? 1000),
            result: entry.result,
          });
        if (due.get(entry.key)!.at > Date.now()) continue;
        try {
          await this.refresh(entry.key, signal);
        } catch (cause) {
          if (!(cause instanceof WorkstreamOperationJoined)) throw cause;
        }
        due.delete(entry.key);
      }
      await pause(
        Math.max(1, Math.min(1000, ...[...due.values()].map(({ at }) => at - Date.now()))),
        signal,
      );
    }
  }
}
