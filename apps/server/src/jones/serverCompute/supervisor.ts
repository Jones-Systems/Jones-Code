// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off cryptoRandomUUID:off preferSchemaOverJson:off -- Imperative bounded supervisor owns Node callbacks and immutable input snapshots; Effect owns its scope.
import { randomUUID } from "node:crypto";
import type { LogicalFinderHandle, FinderToken, WorkspaceKey, WorkspaceJobs } from "../serverParallelism/computeContract.ts";
import { observeSafely, type ComputeObservation } from "../serverParallelism/observations.ts";
import { computeFailure, isComputeFailure } from "./failure.ts";
import { isKey, LIMITS, sameToken, validInput, type Bootstrap, type Operation, type Request, type ScanStatus } from "./protocol.ts";
import type { ComputeChild, SlotResult } from "./processSlot.ts";

type OperationName = keyof WorkspaceJobs | "open";
type Physical = { token: FinderToken; state: "opening" | "ready" | "closing" | "lost";
  epoch: number; lastAccess: number; lastObserved: number; status?: ScanStatus | undefined };
type Key = { readonly id: string; readonly key: WorkspaceKey; readonly handles: Set<string>;
  pending: number; running: boolean; incarnation: number; physical?: Physical | undefined };
type Job = { readonly key: Key; readonly operation: OperationName; readonly input: unknown;
  readonly admission: number; readonly enqueued: number; readonly deadline: number; readonly controller: AbortController;
  readonly resolve: (value: unknown) => void; readonly reject: (cause: unknown) => void;
  readonly timer: NodeJS.Timeout; readonly disposeSignal: () => void;
  binding?: FinderToken; residencyEpoch?: number; group?: Job[]; leader?: Job; dispatched: boolean; settled: boolean };
type Slot = { readonly id: number; generation: number; state: "empty" | "starting" | "ready" | "draining" | "retiring" | "unresolved";
  child?: ComputeChild | undefined; boot?: Bootstrap; busy: boolean; retiring?: Promise<void> | undefined;
  cooldownUntil: number; failures: number; allocationsUsed: number;
  foreground?: "open" | "refresh" | undefined; planned: boolean; readonly keys: Set<Key> };
type Drain = { readonly slot: Slot; readonly generation: number; readonly cut: number; demand: Job | undefined };
type Reservation = { readonly slot: Slot; readonly demand: Job };
export interface JournalPort {
  readonly claim: { readonly authorityId: string };
  reserve(slot: number, generation: number, nonce: string): Promise<void>;
  registered(slot: number, generation: number, nonce: string, pid: number): Promise<void>;
  retired(slot: number, generation: number, nonce: string, pid: number): Promise<void>;
  release(): Promise<void>;
}
interface SupervisorOptions {
  readonly count: 2 | 3;
  readonly build: string;
  readonly journal: JournalPort;
  readonly start: (bootstrap: Bootstrap, register: (pid: number) => Promise<void>, onExit: (cause: unknown) => void) => Promise<ComputeChild>;
  readonly observe?: (event: ComputeObservation) => void;
}
const outcome = (failure: unknown): ComputeObservation["outcome"] => {
  if (failure === undefined) return "ok";
  if (!isComputeFailure(failure)) return "failed";
  if (failure.reason === "cancelled") return "cancelled";
  if (failure.reason === "queue-timeout" || failure.reason === "deadline") return "timeout";
  if (["unavailable", "worker-exit", "retirement-unknown"].includes(failure.reason)) return "unavailable";
  return "failed";
};
const keyString = (key: WorkspaceKey) => JSON.stringify([key.variant, key.cwd]);
const tick = (value: number) => { if (!Number.isSafeInteger(value + 1)) throw computeFailure("unavailable"); return value + 1; };

/** The only physical allocation authority. Child maps never evict independently. */
export class ComputeSupervisor {
  private readonly handles = new Map<string, { readonly handle: LogicalFinderHandle; readonly key: Key }>();
  private readonly keys = new Map<string, Key>();
  private readonly queue: Job[] = [];
  private readonly slots: Slot[];
  private readonly active = new Set<Job>();
  private readonly tasks = new Set<Promise<void>>();
  private management = false;
  private pumping = false;
  private admissionSequence = 0;
  private drain: Drain | undefined;
  private reservation: Reservation | undefined;
  private stopped = false;
  private shutdownTask: Promise<void> | undefined;
  private blocked: unknown;
  private requestId = 0;
  private accessEpoch = 0;
  private wake: NodeJS.Timeout | undefined;
  private readonly maintenance: NodeJS.Timeout;
  private readonly observe: (event: ComputeObservation) => void;
  private readonly options: SupervisorOptions;
  constructor(options: SupervisorOptions) {
    this.options = options;
    if (options.count !== 2 && options.count !== 3) throw computeFailure("unavailable");
    this.slots = Array.from({ length: options.count }, (_, id) => ({ id, generation: 0, state: "empty", busy: false,
      cooldownUntil: 0, failures: 0, allocationsUsed: 0, planned: false, keys: new Set<Key>() }));
    this.observe = observeSafely(options.observe);
    this.maintenance = setInterval(() => this.maintain(), 250); this.maintenance.unref();
  }
  register(key: WorkspaceKey): LogicalFinderHandle {
    this.admission();
    if (!isKey(key)) throw computeFailure("protocol");
    if (this.handles.size >= LIMITS.handles) throw computeFailure("capacity-held");
    const id = keyString(key);
    let entry = this.keys.get(id);
    if (!entry) {
      entry = { id: randomUUID(), key: Object.freeze({ ...key }), handles: new Set(), pending: 0, running: false, incarnation: 0 };
      this.keys.set(id, entry);
    }
    const handle: LogicalFinderHandle = Object.freeze({ handleId: randomUUID(), key: entry.key });
    entry.handles.add(handle.handleId); this.handles.set(handle.handleId, { handle, key: entry });
    this.counts(); return handle;
  }
  release(handleId: string): void {
    const entry = this.handles.get(handleId); if (!entry) return;
    this.handles.delete(handleId); entry.key.handles.delete(handleId); this.forget(entry.key); this.counts();
    // Scope release is logical only. It cannot close a cached/still-leased physical token.
  }
  private admission(): void {
    if (this.blocked !== undefined) throw computeFailure("retirement-unknown", this.blocked);
    if (this.stopped) throw computeFailure("unavailable");
  }
  run<K extends keyof WorkspaceJobs>(handle: LogicalFinderHandle, operation: K, input: WorkspaceJobs[K]["input"], signal?: AbortSignal): Promise<WorkspaceJobs[K]["output"]> {
    return this.enqueue(handle, operation, input, signal) as Promise<WorkspaceJobs[K]["output"]>;
  }
  ensure(handle: LogicalFinderHandle, signal?: AbortSignal): Promise<void> {
    return this.enqueue(handle, "open", null, signal) as Promise<void>;
  }
  private enqueue(handle: LogicalFinderHandle, operation: OperationName, input: unknown, signal?: AbortSignal): Promise<unknown> {
    try {
      this.admission();
      const registration = this.handles.get(handle.handleId);
      if (!registration || registration.handle.key.cwd !== handle.key.cwd || registration.handle.key.variant !== handle.key.variant)
        throw computeFailure("unavailable");
      if (signal?.aborted) throw computeFailure("cancelled");
      const key = registration.key, wireInput = operation === "open" ? key.key : input ?? null;
      if (!validInput(operation, wireInput)) throw computeFailure("protocol");
      // Snapshot inputs at admission; later caller mutation cannot bypass bounds or change an admitted operation.
      const encoded = JSON.stringify(wireInput);
      if (Buffer.byteLength(encoded) + 2048 > LIMITS.requestBytes) throw computeFailure("oversize");
      if (this.queue.length >= LIMITS.queue || key.pending >= LIMITS.perKeyQueue) throw computeFailure("queue-full");
      return new Promise((resolve, reject) => {
        const enqueued = performance.now();
        const cancel = () => {
          if (job.settled) return;
          this.settle(job, undefined, computeFailure("cancelled"));
          if (job.dispatched) {
            const leader = job.leader ?? job;
            if ([leader, ...(leader.group ?? [])].every((member) => member.settled)) leader.controller.abort();
          }
          else this.removeQueued(job);
        };
        this.admissionSequence = tick(this.admissionSequence);
        const job: Job = { admission: this.admissionSequence, key, operation, input: JSON.parse(encoded), enqueued, deadline: enqueued + LIMITS.queueMs,
          controller: new AbortController(), resolve, reject, dispatched: false, settled: false,
          ...(key.physical && this.slots[key.physical.token.slotId]!.state === "ready" && (key.physical.state === "ready" || key.physical.state === "opening") ? { binding: key.physical.token, residencyEpoch: key.physical.epoch } : {}),
          timer: setTimeout(() => {
            if (!job.dispatched) { this.settle(job, undefined, computeFailure("queue-timeout")); this.removeQueued(job); }
          }, LIMITS.queueMs),
          disposeSignal: () => signal?.removeEventListener("abort", cancel),
        };
        // Protection is taken synchronously, before any cohort admission cut.
        key.pending++; this.queue.push(job); signal?.addEventListener("abort", cancel, { once: true });
        this.pump();
      });
    } catch (cause) { return Promise.reject(cause); }
  }
  private settle(job: Job, result?: unknown, failure?: unknown): void {
    if (job.settled) return; job.settled = true; clearTimeout(job.timer); job.disposeSignal();
    const token = job.binding;
    this.observe({ phase: "job", operation: job.operation, slotId: token?.slotId ?? -1,
      generation: token?.slotGeneration ?? 0, durationMs: performance.now() - job.enqueued,
      queueDepth: this.queue.length, outcome: outcome(failure) });
    if (failure !== undefined) job.reject(failure); else job.resolve(result);
  }
  private finish(job: Job, result?: unknown, failure?: unknown): void {
    this.settle(job, result, failure);
    for (const member of job.group ?? []) this.settle(member, result, failure);
  }
  private removeQueued(job: Job): void {
    const index = this.queue.indexOf(job);
    if (index < 0) return;
    this.queue.splice(index, 1); job.key.pending--; this.forget(job.key); this.pump();
  }
  private forget(key: Key): void {
    if (!key.handles.size && !key.pending && !key.running && !key.physical) this.keys.delete(keyString(key.key));
  }
  private bound(job: Job, slot: Slot): boolean {
    return job.binding?.slotId === slot.id && job.binding.slotGeneration === slot.generation;
  }
  private boundWork(slot: Slot): number {
    return this.queue.filter((job) => this.bound(job, slot)).length +
      [...this.active].filter((job) => this.bound(job, slot)).reduce((n, job) => n + 1 + (job.group?.length ?? 0), 0);
  }
  private newestAccess(slot: Slot): number {
    return Math.max(0, ...[...slot.keys].map((key) => key.physical!.lastAccess));
  }
  private idleExpired(slot: Slot): boolean {
    // A logical facade or an idle sibling does not expire a warm, actively used cohort.
    return slot.keys.size > 0 && !slot.busy && this.boundWork(slot) === 0 &&
      performance.now() - this.newestAccess(slot) >= LIMITS.idleMs;
  }
  private absentDemand(): Job | undefined {
    return this.queue.find((job) => !job.settled && !job.binding && !job.key.physical);
  }
  private planDrain(): void {
    if (this.drain || this.reservation) return;
    const demand = this.absentDemand();
    if (!demand) return;
    // Busy children with spare allocation capacity will become eligible without destroying warm siblings.
    if (this.slots.some((slot) => slot.state !== "ready" || slot.allocationsUsed < LIMITS.residents)) return;
    const slot = this.slots.filter((slot) => slot.state === "ready").sort((a, b) =>
      Number(this.boundWork(a) > 0) - Number(this.boundWork(b) > 0) ||
      this.newestAccess(a) - this.newestAccess(b) || this.boundWork(a) - this.boundWork(b) || a.id - b.id)[0];
    if (slot) this.markDrain(slot, demand);
  }
  private markDrain(slot: Slot, demand?: Job): void {
    // Synchronous admission barrier: no await, native transition, or management-gate lease here.
    slot.state = "draining";
    this.drain = { slot, generation: slot.generation, cut: this.admissionSequence, demand };
    this.counts();
  }
  private advanceDrain(): void {
    const drain = this.drain;
    if (!drain) return;
    const slot = drain.slot;
    if (slot.generation !== drain.generation || slot.state !== "draining") { this.drain = undefined; return; }
    if (slot.busy || this.boundWork(slot) > 0) return;
    drain.demand = this.absentDemand();
    if (!drain.demand && !this.idleExpired(slot)) {
      // Pressure vanished before teardown. Future-unbound requests may now bind at dispatch.
      slot.state = "ready"; this.drain = undefined; this.counts(); return;
    }
    if (this.management) return;
    slot.planned = true; slot.state = "retiring"; this.drain = undefined;
    this.scheduleRetirement(slot);
  }
  private slotFor(job: Job): Slot | undefined {
    const physical = job.key.physical;
    if (physical) {
      const slot = this.slots[physical.token.slotId]!;
      if (slot.busy || physical.state !== "ready" || (slot.state !== "ready" && slot.state !== "draining")) return undefined;
      if (slot.state === "draining" && (!this.bound(job, slot) ||
          job.admission > this.drain!.cut || this.drain!.generation !== slot.generation)) return undefined;
      if (job.binding && !sameToken(job.binding, physical.token)) return undefined;
      return slot;
    }
    if (job.binding || this.management) return undefined;
    return this.slots.filter((slot) => !slot.busy && (slot.state === "ready" || slot.state === "empty") &&
      slot.cooldownUntil <= performance.now() && slot.allocationsUsed < LIMITS.residents &&
      (!this.reservation || this.reservation.slot !== slot || this.reservation.demand.key === job.key))
      .sort((a, b) => a.allocationsUsed - b.allocationsUsed || a.id - b.id)[0];
  }
  private pump(): void {
    if (this.stopped || this.blocked !== undefined || this.pumping) return;
    this.pumping = true;
    try {
      for (const job of [...this.queue]) if (performance.now() >= job.deadline) {
        this.settle(job, undefined, computeFailure("queue-timeout")); this.removeQueued(job);
      }
      if (this.reservation && !this.queue.includes(this.reservation.demand)) {
        const demand = this.absentDemand();
        this.reservation = demand ? { slot: this.reservation.slot, demand } : undefined;
      }
      this.advanceDrain();
      for (const slot of this.slots) if (slot.state === "retiring") this.scheduleRetirement(slot);
      this.planDrain(); this.advanceDrain();
      for (let i = 0; i < this.queue.length; i++) {
        const job = this.queue[i]!;
        if (job.key.running) continue;
        // FIFO per key, oldest dispatchable globally; busy keys do not block unrelated slots.
        if (this.queue.slice(0, i).some((prior) => prior.key === job.key)) continue;
        const slot = this.slotFor(job); if (!slot) continue;
        if (job.key.physical && !job.binding) {
          job.binding = job.key.physical.token; job.residencyEpoch = job.key.physical.epoch;
        }
        if (this.reservation?.demand.key === job.key) this.reservation = undefined;
        this.queue.splice(i--, 1); job.key.pending--; job.key.running = true; job.dispatched = true;
        clearTimeout(job.timer); slot.busy = true; this.active.add(job);
        // Coalesce only queued refreshes of the same current token AND access epoch.
        // Each caller keeps its own cancellation/deadline and still counts against queue admission.
        if (job.operation === "refresh" && job.binding && job.residencyEpoch !== undefined) {
          job.group = [];
          for (const member of this.queue) {
            if (member.key !== job.key) continue;
            if (member.operation !== "refresh" || !member.binding || !sameToken(member.binding, job.binding) ||
                member.residencyEpoch !== job.residencyEpoch || (slot.state === "draining" && member.admission > this.drain!.cut)) break;
            job.group.push(member);
          }
          for (const member of job.group) {
            this.queue.splice(this.queue.indexOf(member), 1); member.key.pending--;
            member.dispatched = true; member.leader = job; clearTimeout(member.timer);
          }
        }
        let lifecycleHeld = !job.key.physical;
        if (lifecycleHeld) this.management = true;
        const releaseManagement = () => {
          if (!lifecycleHeld) return;
          lifecycleHeld = false; this.management = false; this.pump();
        };
        this.observe({ phase: "queue", operation: job.operation, slotId: slot.id, generation: slot.generation,
          durationMs: performance.now() - job.enqueued, queueDepth: this.queue.length, outcome: "ok" });
        const task = this.execute(slot, job, releaseManagement).catch((cause) => { this.blocked = cause; this.failQueue(cause); }).finally(() => {
          this.active.delete(job); job.key.running = false; slot.busy = false;
          this.tasks.delete(task); this.forget(job.key); releaseManagement(); this.counts(); this.pump();
        });
        this.tasks.add(task);
      }
      this.counts();
      if (this.queue.length && !this.wake) {
        // Demand-driven cooldown wake, never a speculative process restart.
        const delay = Math.min(...this.slots.filter((s) => s.state === "empty" && s.cooldownUntil > performance.now()).map((s) => s.cooldownUntil - performance.now()));
        if (Number.isFinite(delay)) this.wake = setTimeout(() => { this.wake = undefined; this.pump(); }, Math.max(1, delay));
      }
    } finally { this.pumping = false; }
  }
  private async start(slot: Slot): Promise<void> {
    const started = performance.now();
    slot.state = "starting"; slot.planned = false; slot.generation = tick(slot.generation);
    const boot: Bootstrap = { version: 1, build: this.options.build, authorityId: this.options.journal.claim.authorityId,
      slotId: slot.id, slotGeneration: slot.generation, nonce: randomUUID() };
    slot.boot = boot;
    try {
      await this.options.journal.reserve(slot.id, slot.generation, boot.nonce);
      slot.child = await this.options.start(boot, async (pid) => {
        await this.options.journal.registered(slot.id, slot.generation, boot.nonce, pid);
        if (this.stopped) throw computeFailure("unavailable");
      }, (cause) => {
        if (slot.boot === boot && (slot.state === "ready" || slot.state === "draining") && !this.stopped) {
          // Mark immediately; deferred retirement does not admit new work into this generation.
          this.faultSlot(slot, cause); this.scheduleRetirement(slot);
        }
      });
      slot.state = "ready"; this.counts();
      this.observe({ phase: "startup", operation: "open", slotId: slot.id, generation: slot.generation,
        durationMs: performance.now() - started, queueDepth: this.queue.length, outcome: "ok" });
    } catch (cause) {
      this.observe({ phase: "startup", operation: "open", slotId: slot.id, generation: slot.generation,
        durationMs: performance.now() - started, queueDepth: this.queue.length, outcome: "unavailable" });
      // ProcessSlot.start only returns unavailable after it has observed captured exit.
      const knownRetired = isComputeFailure(cause) && cause.reason === "unavailable" &&
        typeof cause.cause === "object" && cause.cause !== null && "retiredPid" in cause.cause &&
        typeof cause.cause.retiredPid === "number";
      if (knownRetired) {
        const pid = (cause.cause as { retiredPid: number }).retiredPid;
        try {
          await this.options.journal.retired(slot.id, slot.generation, boot.nonce, pid);
          slot.state = "empty"; this.cooldown(slot);
        } catch (retirementCause) { slot.state = "unresolved"; this.blocked = retirementCause; }
      } else {
        // Missing registration or unknown spawn effect must hold the entire generation closed.
        slot.state = "unresolved"; this.blocked = cause;
      }
      if (this.blocked !== undefined) throw computeFailure("retirement-unknown", this.blocked);
      throw cause;
    }
  }
  private async request(slot: Slot, key: Key, operation: Operation, input: unknown, signal?: AbortSignal): Promise<SlotResult> {
    const physical = key.physical, child = slot.child;
    if (!physical || !child || (slot.state !== "ready" && slot.state !== "draining") ||
        physical.token.slotGeneration !== slot.generation) throw computeFailure("unavailable");
    this.requestId = tick(this.requestId);
    const request: Request = { ...child.bootstrap, requestId: this.requestId, operation, token: physical.token, input };
    const started = performance.now();
    if (operation === "open" || operation === "refresh") slot.foreground = operation;
    this.counts();
    try {
      const result = await child.request(request, signal);
      if (key.physical !== physical || slot.child !== child || !sameToken(physical.token, request.token) ||
          (slot.state !== "ready" && slot.state !== "draining")) throw computeFailure("worker-exit");
      // Search readiness is incomplete diagnostics. It cannot release charges or certify a native tail ended.
      if (result.final.status) { physical.status = result.final.status; physical.lastObserved = performance.now(); }
      this.observe({ phase: "execute", operation: operation === "status" ? "list" : operation,
        slotId: slot.id, generation: slot.generation, durationMs: performance.now() - started, queueDepth: this.queue.length, outcome: "ok",
        estimated: true, uncertaintyMs: performance.now() - started });
      return result;
    } finally {
      if (slot.child === child) slot.foreground = undefined;
      this.counts();
    }
  }
  private async execute(slot: Slot, job: Job, releaseManagement: () => void): Promise<void> {
    try {
      if (slot.state === "empty") await this.start(slot);
      if (this.stopped || (slot.state !== "ready" && slot.state !== "draining")) throw computeFailure("unavailable");
      const key = job.key;
      if (!key.physical) {
        if (slot.state !== "ready" || slot.allocationsUsed >= LIMITS.residents) throw computeFailure("capacity-held");
        // Reserve before open; failure, readiness, scope close and JS-map loss never refund this generation.
        slot.allocationsUsed++;
        key.incarnation = tick(key.incarnation); this.accessEpoch = tick(this.accessEpoch);
        key.physical = { token: { authorityId: this.options.journal.claim.authorityId, slotId: slot.id,
          slotGeneration: slot.generation, keyId: key.id, incarnation: key.incarnation }, state: "opening",
          epoch: this.accessEpoch, lastAccess: performance.now(), lastObserved: 0 };
        slot.keys.add(key);
        // All admitted same-key work now belongs to this generation, including coalesced logical opens.
        for (const queued of this.queue) if (queued.key === key) {
          queued.binding = key.physical.token; queued.residencyEpoch = key.physical.epoch;
        }
        job.binding = key.physical.token;
        await this.request(slot, key, "open", key.key, job.controller.signal);
        key.physical.state = "ready";
        // Creation ends the management transition. Productive work keeps only its key/child data lease.
        releaseManagement();
      }
      job.binding = key.physical.token;
      this.accessEpoch = tick(this.accessEpoch); key.physical.epoch = this.accessEpoch; key.physical.lastAccess = performance.now();
      if (job.operation === "open") { this.finish(job); return; }
      const result = await this.request(slot, key, job.operation, job.input, job.controller.signal);
      const output = job.operation === "refresh" ? undefined : job.operation === "searchContents" ? {
        matches: result.items, truncated: result.final.truncated!,
        ...(result.final.regexFallbackError === undefined ? {} : { regexFallbackError: result.final.regexFallbackError }),
      } : { entries: result.items, truncated: result.final.truncated! };
      this.finish(job, output);
    } catch (cause) {
      this.finish(job, undefined, cause);
      // A typed search/refresh error leaves the finder alive. A failed open/transport/unknown teardown cannot.
      const reusable = isComputeFailure(cause) && ["finder-search", "finder-refresh", "oversize", "cancelled"].includes(cause.reason) && job.key.physical?.state === "ready";
      if (reusable && job.key.physical) job.key.physical.status = undefined;
      if (!reusable && slot.child && slot.state !== "empty" && slot.state !== "unresolved") {
        this.faultSlot(slot, cause);
        // Retirement runs after this data lease leaves, under the pool management gate.
      }
      if (this.blocked !== undefined) this.failQueue(this.blocked);
    }
  }
  private faultSlot(slot: Slot, cause: unknown): void {
    slot.planned = false; slot.state = "retiring";
    if (this.drain?.slot === slot) this.drain = undefined;
    this.invalidate(slot, cause);
  }
  private invalidate(slot: Slot, cause: unknown): void {
    for (const job of [...this.queue]) if (this.bound(job, slot)) {
      this.settle(job, undefined, cause); this.removeQueued(job);
    }
    for (const job of this.active) if (this.bound(job, slot)) {
      this.finish(job, undefined, cause); job.controller.abort();
    }
    for (const key of slot.keys) if (key.physical) key.physical.state = "lost";
    this.counts();
  }
  private cooldown(slot: Slot): void {
    slot.failures = Math.min(slot.failures + 1, 6);
    slot.cooldownUntil = performance.now() + Math.min(30_000, 1000 * 2 ** (slot.failures - 1));
  }
  private async retire(slot: Slot): Promise<void> {
    if (slot.retiring) return slot.retiring;
    slot.state = "retiring"; this.counts();
    const child = slot.child, boot = slot.boot;
    if (!child || !boot) { slot.state = "unresolved"; this.blocked = computeFailure("retirement-unknown"); throw this.blocked; }
    slot.retiring = (async () => {
      try {
        await child.retire();
        await this.options.journal.retired(slot.id, slot.generation, boot.nonce, child.pid);
        // Select pressure while old sibling ownership still exists: later hot facade requests
        // cannot become "absent" first and steal the capacity this rotation was meant to supply.
        const pressure = slot.planned ? this.absentDemand() : undefined;
        for (const key of slot.keys) { key.physical = undefined; this.forget(key); }
        slot.keys.clear(); slot.child = undefined; slot.allocationsUsed = 0; slot.state = "empty";
        if (slot.planned) {
          // Reserve the freshly freed capacity before any hot facade can steal it. No eager rebuild.
          slot.cooldownUntil = 0;
          const demand = pressure ?? this.absentDemand();
          this.reservation = demand ? { slot, demand } : undefined;
        } else this.cooldown(slot);
        this.counts();
      } catch (cause) {
        slot.state = "unresolved"; this.blocked = cause; this.failQueue(cause); throw computeFailure("retirement-unknown", cause);
      } finally { slot.retiring = undefined; }
    })();
    return slot.retiring;
  }
  private scheduleRetirement(slot: Slot): void {
    // The management gate is shared by startup/eviction/retirement, not by productive work.
    if (this.management || slot.busy) return;
    this.management = true; slot.busy = true;
    void this.retire(slot).catch(() => {}).finally(() => { this.management = false; slot.busy = false; this.counts(); this.pump(); });
  }
  private failQueue(cause: unknown): void {
    for (const job of this.queue.splice(0)) { job.key.pending--; this.settle(job, undefined, cause); this.forget(job.key); }
  }
  private maintain(): void {
    if (this.stopped || this.blocked !== undefined) return;
    for (const slot of this.slots) {
      if (slot.state !== "ready" || slot.busy) continue;
      if (!this.drain && !this.reservation && !this.slots.some((s) => s.planned && s.state === "retiring") && this.idleExpired(slot)) { this.markDrain(slot); continue; }
      // Samples inform diagnostics/search readiness only. Unknown native background state is never a lease.
      const key = [...slot.keys].filter((key) => key.physical?.state === "ready")
        .sort((a, b) => a.physical!.lastObserved - b.physical!.lastObserved)[0];
      if (!key || performance.now() - key.physical!.lastObserved < 1000) continue;
      slot.busy = true;
      void this.request(slot, key, "status", null).catch((cause) => {
        this.faultSlot(slot, cause);
      }).finally(() => { slot.busy = false; this.counts(); this.pump(); });
    }
    this.pump();
  }
  private counts(): void {
    const entries = [...this.keys.values()].flatMap((key) => key.physical ? [key.physical] : []);
    this.observe({ phase: "lifecycle", operation: "pool", slotId: -1, generation: 0, durationMs: 0,
      queueDepth: this.queue.length, outcome: "ok", counts: { logicalHandles: this.handles.size,
        physicalEntries: entries.length,
        activeForegroundOpenRefresh: this.slots.filter((slot) => slot.foreground !== undefined).length,
        chargedAllocations: this.slots.reduce((n, slot) => n + slot.allocationsUsed, 0),
        retiringGenerations: this.slots.filter((slot) => slot.state === "retiring" || slot.state === "unresolved").length,
        observedNativeScanning: entries.filter((p) => p.status?.scanning).length,
        observedNativeWarmup: entries.filter((p) => p.status?.warmupComplete === false).length } });
  }
  shutdown(): Promise<void> {
    this.shutdownTask ??= this.shutdownOwned();
    return this.shutdownTask;
  }
  private async shutdownOwned(): Promise<void> {
    this.stopped = true; this.drain = undefined; this.reservation = undefined; clearInterval(this.maintenance); if (this.wake) clearTimeout(this.wake);
    this.failQueue(computeFailure("unavailable"));
    for (const job of this.active) { this.finish(job, undefined, computeFailure("cancelled")); job.controller.abort(); }
    // Give cancellation/native teardown one total 2-second grace, then use the independent captured-child path.
    const grace = async (work: Promise<unknown>, ms: number): Promise<void> => {
      let timer: NodeJS.Timeout | undefined;
      try { await Promise.race([work, new Promise<void>((resolve) => { timer = setTimeout(resolve, ms); })]); }
      finally { if (timer) clearTimeout(timer); }
    };
    const deadline = performance.now() + LIMITS.destroyMs;
    await grace(Promise.allSettled([...this.tasks]), LIMITS.destroyMs);
    // Shutdown shares the same one-transition budget. If foreground/native work did not drain,
    // skip optional scope closes and contain it through the captured-child retirement path below.
    if (!this.management && this.tasks.size === 0) {
      this.management = true;
      const cleanup = (async () => {
        for (const slot of this.slots) {
          if ((slot.state !== "ready" && slot.state !== "draining") || slot.busy || performance.now() >= deadline) continue;
          slot.busy = true;
          try {
            for (const key of slot.keys) {
              if (performance.now() >= deadline) break;
              const p = key.physical;
              if (!p || p.state !== "ready") continue;
              p.state = "closing";
              await this.request(slot, key, "close", null);
              // A close ACK is not exit proof. Keep every mapping and charge through journal retirement.
              this.counts();
            }
          } catch { /* Both acknowledged and unacknowledged closes remain charged. */ }
          finally { slot.busy = false; }
        }
      })().finally(() => { this.management = false; });
      await grace(cleanup, Math.max(1, deadline - performance.now()));
    }
    let unknown: unknown;
    for (const slot of this.slots) {
      if (slot.state === "unresolved") { unknown = computeFailure("retirement-unknown"); continue; }
      if (slot.child) {
        try { slot.state = "retiring"; this.invalidate(slot, computeFailure("cancelled")); await this.retire(slot); }
        catch (cause) { unknown = cause; }
      } else if (slot.state === "starting") unknown = computeFailure("retirement-unknown");
    }
    if (unknown !== undefined) throw computeFailure("retirement-unknown", unknown);
    await this.options.journal.release();
  }
}
