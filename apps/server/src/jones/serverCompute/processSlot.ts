// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off -- Captured child callbacks and independent deadline/control transport require Node primitives.
import { spawn, type ChildProcess } from "node:child_process";
import type { Duplex, Readable, Writable } from "node:stream";
import { computeFailure, ComputeFailure } from "./failure.ts";
import { bootstrapArgument, type WorkerCommand } from "./workerCommand.ts";
import {
  observeSafely,
  type ComputeObservation,
  type ComputeRequestTiming,
} from "../serverParallelism/observations.ts";
import {
  controls,
  decodeReply,
  estimateReplyDelivery,
  frames,
  keys,
  LIMITS,
  replyByteTotal,
  sameBootstrap,
  sameToken,
  writeControl,
  writeFrame,
  type Bootstrap,
  type Request,
  type Reply,
  type ReplyTiming,
} from "./protocol.ts";

export interface SlotResult {
  readonly items: readonly unknown[];
  readonly final: Extract<Reply["body"], { type: "done" }>;
}
export interface ComputeRuntimeMetadata {
  readonly heapLimitBytes: number;
  readonly availableParallelism: number;
  readonly parallelismSource: "node:os.availableParallelism";
}
export interface ComputeChild {
  readonly pid: number;
  readonly bootstrap: Bootstrap;
  readonly runtimeMetadata?: ComputeRuntimeMetadata | undefined;
  request(
    request: Request,
    signal?: AbortSignal,
    observeTiming?: (timing: ComputeRequestTiming) => void,
  ): Promise<SlotResult>;
  retire(): Promise<void>;
}
/** Harness operation wrappers retain the captured identity and live audit getter. */
export function withComputeChildOperations(
  child: ComputeChild,
  operations: Pick<ComputeChild, "request" | "retire">,
): ComputeChild {
  return {
    pid: child.pid,
    bootstrap: child.bootstrap,
    get runtimeMetadata() {
      return child.runtimeMetadata;
    },
    request: operations.request,
    retire: operations.retire,
  };
}
function bounded<T>(
  promise: Promise<T>,
  ms: number,
  reason: "deadline" | "retirement-unknown" | "unavailable",
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(computeFailure(reason)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (cause) => {
        clearTimeout(timer);
        reject(cause);
      },
    );
  });
}
interface Pending {
  readonly request: Request;
  sequence: number;
  bytes: number;
  items: unknown[];
  readonly resolve: (value: SlotResult) => void;
  readonly reject: (cause: unknown) => void;
  terminal: boolean;
  sentMs?: number;
  receivedMs?: number;
  timing?: ReplyTiming;
}

/** This object retains the captured ChildProcess for every signal. Journal PIDs are never kill targets. */
export class ProcessSlot implements ComputeChild {
  readonly pid: number;
  private metadata: ComputeRuntimeMetadata | undefined;
  // Audit readback only: retained after exit, never used to infer liveness or completion.
  get runtimeMetadata(): ComputeRuntimeMetadata | undefined {
    return this.metadata;
  }
  private readonly input: Writable;
  private readonly output: Readable;
  private readonly control: Duplex;
  private readonly exited: Promise<void>;
  private exitObserved = false;
  private fault: unknown;
  private pending: Pending | undefined;
  private readonly acknowledgements = new Map<
    string,
    { resolve: () => void; reject: (cause: unknown) => void }
  >();
  private readonly received = new Set<string>();
  private readonly readiness = new Set<string>();
  private diagnostics = Buffer.alloc(0);
  private controlWrites = 0;
  private controlTail: Promise<void> = Promise.resolve();
  private readonly child: ChildProcess;
  readonly bootstrap: Bootstrap;
  private readonly onExit: (cause: unknown) => void;
  private readonly observe: (event: ComputeObservation) => void;
  private constructor(
    child: ChildProcess,
    bootstrap: Bootstrap,
    onExit: (cause: unknown) => void,
    observe?: (event: ComputeObservation) => void,
  ) {
    this.child = child;
    this.bootstrap = Object.freeze({ ...bootstrap });
    this.onExit = onExit;
    this.observe = observeSafely(observe);
    if (!child.pid || !child.stdin || !child.stdout || !child.stderr || !child.stdio[3])
      throw computeFailure("unavailable");
    this.pid = child.pid;
    this.input = child.stdin;
    this.output = child.stdout;
    this.control = child.stdio[3] as Duplex;
    this.exited = new Promise<void>((resolve) => {
      child.once("exit", () => {
        this.exitObserved = true;
        this.fail(computeFailure("worker-exit"));
        resolve();
      });
      child.once("error", (cause) => this.fail(computeFailure("worker-exit", cause)));
    });
    this.input.on("error", (cause) => this.fail(cause));
    child.stderr.on("data", (chunk: Buffer) => {
      // Ring is never emitted as a metric label/public error, nor allowed to accumulate without bound.
      this.diagnostics = Buffer.concat([
        this.diagnostics,
        chunk.subarray(-LIMITS.diagnosticsBytes),
      ]).subarray(-LIMITS.diagnosticsBytes);
    });
    void this.readControl().catch((cause) => this.fail(cause));
    void this.readData().catch((cause) => this.fail(cause));
  }
  static async start(
    command: WorkerCommand,
    bootstrap: Bootstrap,
    register: (pid: number) => Promise<void>,
    onExit: (cause: unknown) => void,
    observe?: (event: ComputeObservation) => void,
    capture?: (child: ComputeChild) => void,
  ): Promise<ProcessSlot> {
    const child = spawn(command.file, [...command.args, bootstrapArgument(bootstrap)], {
      stdio: ["pipe", "pipe", "pipe", "pipe"],
      env: command.env,
      windowsHide: true,
    });
    if (!child.pid) {
      // Observe spawn errors without leaking an unhandled event. Reservation remains unresolved.
      child.once("error", () => {});
      throw computeFailure("retirement-unknown");
    }
    let slot: ProcessSlot;
    try {
      slot = new ProcessSlot(child, bootstrap, onExit, observe);
    } catch (cause) {
      // Even an unsupported stdio route retains and reaps the exact spawned handle.
      const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
      child.once("error", () => {});
      try {
        child.kill("SIGKILL");
        await bounded(exited, LIMITS.reapMs, "retirement-unknown");
      } catch {
        throw computeFailure("retirement-unknown", cause);
      }
      throw new ComputeFailure({ reason: "unavailable", cause: { cause, retiredPid: child.pid } });
    }
    try {
      capture?.(slot); // Qualification can bind starting PIDs before readiness; no admission authority.
      await slot.wait("ready");
      await register(slot.pid); // durable before GO; startup guardian timer covers a lost registration.
      await slot.command("go");
      await slot.wait("nativeReady");
      await slot.command("loaded");
      return slot;
    } catch (cause) {
      // Return exit proof to the caller even for startup failure; do not lose the captured child handle.
      try {
        await slot.retire();
      } catch {
        throw computeFailure("retirement-unknown", cause);
      }
      throw new ComputeFailure({ reason: "unavailable", cause: { cause, retiredPid: slot.pid } });
    }
  }
  private fail(cause: unknown): void {
    if (this.fault !== undefined) return;
    this.fault = cause;
    this.pending?.reject(cause);
    for (const ack of this.acknowledgements.values()) ack.reject(cause);
    this.acknowledgements.clear();
    try {
      this.onExit(cause);
    } catch {
      /* The captured-child safety path must survive a callback defect. */
    }
    if (!this.exitObserved) {
      try {
        this.child.kill("SIGKILL");
      } catch {
        /* Reaping still needs proof; retire will hold unresolved on timeout. */
      }
    }
  }
  private wait(key: string): Promise<void> {
    if (this.fault !== undefined) return Promise.reject(this.fault);
    if (this.received.delete(key)) return Promise.resolve();
    if (this.acknowledgements.has(key) || this.acknowledgements.size >= 4)
      return Promise.reject(computeFailure("protocol"));
    return bounded(
      new Promise<void>((resolve, reject) => {
        this.acknowledgements.set(key, { resolve, reject });
      }),
      LIMITS.startupMs,
      "unavailable",
    ).finally(() => this.acknowledgements.delete(key));
  }
  private acknowledge(key: string): void {
    if (key === "ready" || key === "nativeReady") {
      if (this.readiness.has(key)) throw computeFailure("protocol");
      this.readiness.add(key);
    }
    const ack = this.acknowledgements.get(key);
    if (ack) {
      this.acknowledgements.delete(key);
      ack.resolve();
    } else if ((key === "ready" || key === "nativeReady") && !this.received.has(key))
      this.received.add(key);
    else throw computeFailure("protocol");
  }
  private send(type: string, fields: Record<string, unknown> = {}): Promise<void> {
    if (this.fault !== undefined) return Promise.reject(this.fault);
    if (++this.controlWrites > 8) {
      this.controlWrites--;
      return Promise.reject(computeFailure("protocol"));
    }
    const write = this.controlTail.then(() =>
      writeControl(this.control, { ...this.bootstrap, type, ...fields }),
    );
    this.controlTail = write.catch(() => {});
    return write.finally(() => {
      this.controlWrites--;
    });
  }
  private async command(
    type: string,
    fields: Record<string, unknown> = {},
    key = type,
  ): Promise<void> {
    const ack = this.wait(key);
    // Always observe the ack if write fails; this prevents an orphan rejected promise.
    const written = this.send(type, fields);
    await Promise.all([written, ack]);
  }
  private async readControl(): Promise<void> {
    const base = Object.keys(this.bootstrap);
    for await (const msg of controls(this.control)) {
      if (
        !sameBootstrap(msg as unknown as Bootstrap, this.bootstrap) ||
        typeof msg.type !== "string"
      )
        throw computeFailure("protocol");
      if (msg.type === "ready") {
        if (!keys(msg, [...base, "type", "pid"]) || msg.pid !== this.pid)
          throw computeFailure("protocol");
        this.acknowledge("ready");
      } else if (["go", "loaded"].includes(msg.type)) {
        if (!keys(msg, [...base, "type"])) throw computeFailure("protocol");
        this.acknowledge(msg.type);
      } else if (msg.type === "nativeReady") {
        if (
          this.metadata !== undefined ||
          !this.readiness.has("ready") ||
          !keys(msg, [...base, "type", "heapLimit", "availableParallelism"]) ||
          !Number.isSafeInteger(msg.heapLimit) ||
          (msg.heapLimit as number) < 256 * 1024 * 1024 ||
          (msg.heapLimit as number) > 400 * 1024 * 1024 ||
          !Number.isSafeInteger(msg.availableParallelism) ||
          (msg.availableParallelism as number) < 1
        )
          throw computeFailure("protocol");
        this.metadata = Object.freeze({
          heapLimitBytes: msg.heapLimit as number,
          availableParallelism: msg.availableParallelism as number,
          parallelismSource: "node:os.availableParallelism",
        });
        this.acknowledge("nativeReady");
      } else if (msg.type === "armed" || msg.type === "disarmed") {
        if (
          !keys(msg, [...base, "type", "requestId"]) ||
          msg.requestId !== this.pending?.request.requestId
        )
          throw computeFailure("protocol");
        this.acknowledge(`${msg.type}:${msg.requestId}`);
      } else if (msg.type === "resources") {
        if (
          !keys(msg, [...base, "type", "rss"]) ||
          !Number.isSafeInteger(msg.rss) ||
          (msg.rss as number) < 0
        )
          throw computeFailure("protocol");
        this.observe({
          phase: "resources",
          operation: "pool",
          slotId: this.bootstrap.slotId,
          generation: this.bootstrap.slotGeneration,
          outcome: "ok",
          rssBytes: msg.rss as number,
        });
      } else if (msg.type === "overBudget") {
        if (!keys(msg, [...base, "type"])) throw computeFailure("protocol");
        throw computeFailure("unavailable", { category: "rss-watermark" });
      } else throw computeFailure("protocol");
    }
    throw computeFailure("worker-exit");
  }
  private async readData(): Promise<void> {
    let frameBytes = 0;
    for await (const raw of frames(this.output, LIMITS.batchBytes, (bytes) => {
      frameBytes = bytes;
    })) {
      const reply = decodeReply(raw, this.bootstrap),
        pending = this.pending;
      if (
        !pending ||
        pending.terminal ||
        reply.requestId !== pending.request.requestId ||
        reply.operation !== pending.request.operation ||
        !sameToken(reply.token, pending.request.token) ||
        reply.sequence !== pending.sequence++
      )
        throw computeFailure("protocol");
      const body = reply.body;
      pending.bytes = replyByteTotal(pending.bytes, frameBytes, body.type !== "chunk");
      if (reply.timing && reply.timing.parentSentMs !== pending.sentMs)
        throw computeFailure("protocol");
      if (body.type === "chunk") pending.items.push(...body.items);
      else {
        pending.receivedMs = performance.now();
        if (reply.timing) pending.timing = reply.timing;
      }
      // Acknowledged only after strict schema/token validation and bounded assembly.
      await this.send("credit", { requestId: reply.requestId });
      if (body.type !== "chunk") {
        pending.terminal = true;
        if (body.type === "failure")
          pending.reject(
            new ComputeFailure({ reason: body.reason, scanTimedOut: body.scanTimedOut }),
          );
        else pending.resolve({ items: pending.items, final: body });
      }
    }
    throw computeFailure("worker-exit");
  }
  async request(
    request: Request,
    signal?: AbortSignal,
    observeTiming?: (timing: ComputeRequestTiming) => void,
  ): Promise<SlotResult> {
    if (this.pending || this.fault !== undefined || signal?.aborted)
      throw computeFailure(signal?.aborted ? "cancelled" : "unavailable");
    let resolve!: (value: SlotResult) => void, reject!: (cause: unknown) => void;
    const reply = new Promise<SlotResult>((ok, fail) => {
      resolve = ok;
      reject = fail;
    });
    // Attach before any I/O can fail; the result is awaited below even when arm/write fails.
    void reply.catch(() => {});
    this.pending = { request, sequence: 0, bytes: 0, items: [], resolve, reject, terminal: false };
    const cancel = () => {
      if (this.pending?.request.requestId !== request.requestId || this.pending.terminal) return;
      void this.send("cancel", { requestId: request.requestId }).catch((cause) => this.fail(cause));
    };
    try {
      await this.command(
        "arm",
        {
          requestId: request.requestId,
          deadline: Date.now() + (request.operation === "close" ? LIMITS.destroyMs : LIMITS.jobMs),
        },
        `armed:${request.requestId}`,
      );
      signal?.addEventListener("abort", cancel, { once: true });
      const sentMs = performance.now();
      this.pending.sentMs = sentMs;
      await writeFrame(
        this.input,
        { ...request, clock: { parentSentMs: sentMs } },
        LIMITS.requestBytes,
      );
      if (signal?.aborted) cancel();
      let failure: unknown, result: SlotResult | undefined;
      try {
        result = await bounded(reply, LIMITS.jobMs + 1000, "deadline");
      } catch (cause) {
        failure = cause;
      }
      if (this.pending?.terminal)
        await this.command(
          "disarm",
          { requestId: request.requestId },
          `disarmed:${request.requestId}`,
        );
      else {
        this.fail(failure ?? computeFailure("protocol"));
      }
      if (failure !== undefined) throw failure;
      if (!result) throw computeFailure("protocol");
      if (signal?.aborted) throw computeFailure("cancelled");
      return result;
    } catch (cause) {
      if (!this.pending?.terminal) this.fail(cause);
      throw cause;
    } finally {
      const pending = this.pending;
      signal?.removeEventListener("abort", cancel);
      this.pending = undefined;
      // Runs after terminal handling/disarm or failure, never in place of actual request settlement.
      observeSafely(observeTiming)({
        ...(pending?.timing
          ? {
              worker: pending.timing,
              delivery: estimateReplyDelivery(pending.timing, pending.receivedMs!),
            }
          : {}),
        ...(pending?.receivedMs === undefined
          ? {}
          : { parentContinuationMs: performance.now() - pending.receivedMs }),
      });
    }
  }
  async retire(): Promise<void> {
    // Do not infer exit from signal-send success or child.killed. Wait for this captured child's event.
    if (!this.exitObserved) {
      try {
        await bounded(this.send("terminate"), 100, "unavailable");
      } catch {
        this.child.kill("SIGKILL");
      }
    }
    await bounded(this.exited, LIMITS.reapMs, "retirement-unknown");
    this.control.destroy();
    this.input.destroy();
    this.output.destroy();
  }
}
