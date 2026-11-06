// @effect-diagnostics nodeBuiltinImport:off globalTimers:off preferSchemaOverJson:off -- Hidden entry is the native callback/runtime boundary with a closed bootstrap decoder.
import type * as EffectTypes from "effect/Effect";
import type * as ScopeTypes from "effect/Scope";
import * as NodeWorkerThreads from "node:worker_threads";
import * as NodePath from "node:path";
import * as NodeV8 from "node:v8";
import * as NodeOS from "node:os";
import * as NodeTimersPromises from "node:timers/promises";
import type * as NodeStream from "node:stream";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { guardianSource } from "./guardian.ts";
import { currentBuildIdentity } from "./workerCommand.ts";
import { computeFailure } from "./failure.ts";
import {
  decodeBootstrap,
  record,
  keys,
  decodeRequest,
  encodeFrame,
  frames,
  LIMITS,
  replyByteTotal,
  sameToken,
  writeEncodedFrame,
  type Reply,
  type ReplyTiming,
  type Request,
  type ScanStatus,
} from "./protocol.ts";
import type { WorkspaceJobs, WorkspaceKey } from "../serverParallelism/computeContract.ts";

type Output = WorkspaceJobs["list" | "search" | "searchContents"]["output"];

/** A failed result remains a typed terminal failure; the same child can serve the next request. */
export class WorkerReplies {
  private sequence = 0;
  private total = 0;
  private credits: number = LIMITS.credits;
  private wake: (() => void) | undefined;
  private readonly request: Request;
  private readonly output: NodeStream.Writable;
  private readonly now: () => number;
  private readonly receivedMs: number;
  private execution:
    | Pick<ReplyTiming, "executionStartedMs" | "executionCompletedMs" | "executionOutcome">
    | undefined;
  private replyStartedMs: number | undefined;
  constructor(request: Request, output: NodeStream.Writable, now = () => performance.now()) {
    this.request = request;
    this.output = output;
    this.now = now;
    this.receivedMs = now();
  }
  /** Await native/Effect cleanup before ending this interval; telemetry never settles the data lease. */
  async execute<A>(work: () => Promise<A>, signal: AbortSignal): Promise<A> {
    const executionStartedMs = this.now();
    let executionOutcome: ReplyTiming["executionOutcome"] = "ok";
    try {
      return await work();
    } catch (cause) {
      executionOutcome = signal.aborted ? "cancelled" : "failed";
      throw cause;
    } finally {
      this.execution = { executionStartedMs, executionCompletedMs: this.now(), executionOutcome };
    }
  }
  acknowledge(): void {
    if (this.credits >= LIMITS.credits) throw computeFailure("protocol");
    this.credits++;
    this.wake?.();
    this.wake = undefined;
  }
  async send(body: Reply["body"]): Promise<void> {
    const { input: _input, clock, ...envelope } = this.request;
    const timing =
      body.type !== "chunk" && clock && this.execution
        ? {
            parentSentMs: clock.parentSentMs,
            receivedMs: this.receivedMs,
            ...this.execution,
            replyStartedMs: this.replyStartedMs ?? this.now(),
            handoffMs: this.now(),
          }
        : undefined;
    const frame: Reply = {
      ...envelope,
      sequence: this.sequence,
      body,
      ...(timing ? { timing } : {}),
    };
    const encoded = encodeFrame(frame, LIMITS.batchBytes);
    const next = replyByteTotal(this.total, encoded.length, body.type !== "chunk");
    // Encoding and every byte limit precede sequence/credit consumption. Rejected frames are never owed an ack.
    while (this.credits === 0)
      await new Promise<void>((resolve) => {
        this.wake = resolve;
      });
    this.credits--;
    this.sequence++;
    this.total = next;
    await writeEncodedFrame(this.output, encoded);
  }
  async result(
    output: Output | undefined,
    status: ScanStatus | undefined,
    signal: AbortSignal,
  ): Promise<void> {
    this.replyStartedMs ??= this.now();
    if (output !== undefined) {
      const items = "matches" in output ? output.matches : output.entries;
      let batch: unknown[] = [],
        bytes = 2048;
      for (const item of items) {
        const size = Buffer.byteLength(JSON.stringify(item)) + 1;
        if (size + 2048 > LIMITS.batchBytes) throw computeFailure("oversize");
        if (batch.length === LIMITS.batchEntries || bytes + size > LIMITS.batchBytes) {
          await this.send({ type: "chunk", items: batch });
          batch = [];
          bytes = 2048;
          await NodeTimersPromises.setImmediate();
        }
        batch.push(item);
        bytes += size;
        if (signal.aborted) throw computeFailure("cancelled");
      }
      if (batch.length) await this.send({ type: "chunk", items: batch });
    }
    await this.send({
      type: "done",
      ...(status ? { status } : {}),
      ...(output === undefined
        ? {}
        : {
            truncated: output.truncated,
            ...("regexFallbackError" in output && output.regexFallbackError !== undefined
              ? { regexFallbackError: output.regexFallbackError }
              : {}),
          }),
    });
  }
  async failure(cause: unknown, signal: AbortSignal): Promise<void> {
    this.replyStartedMs ??= this.now();
    const tag =
      typeof cause === "object" && cause !== null && "_tag" in cause ? cause._tag : undefined;
    // Protocol/ownership uncertainty is fatal: the parent must prove process exit before reuse.
    if (tag === "ComputeFailure" && (cause as { reason?: string }).reason === "protocol")
      throw cause;
    const oversize =
      tag === "ComputeFailure" && (cause as { reason?: string }).reason === "oversize";
    // Keep the same aggregate counter; reserved terminal bytes make a partial result an explicit failure.
    await this.send({
      type: "failure",
      reason: signal.aborted
        ? "cancelled"
        : oversize
          ? "oversize"
          : this.request.operation === "open"
            ? "finder-create"
            : this.request.operation === "refresh"
              ? "finder-refresh"
              : "finder-search",
      scanTimedOut: tag === "WorkspaceSearchIndexScanTimedOut",
    });
  }
  async settle(): Promise<void> {
    while (this.credits < LIMITS.credits)
      await new Promise<void>((resolve) => {
        this.wake = resolve;
      });
  }
}

/** Generation-lifetime tombstones: scope close and failed creation never refund native ownership. */
export class WorkerAllocations {
  private readonly attempts = new Map<
    string,
    { readonly token: Request["token"]; readonly key: WorkspaceKey }
  >();
  reserve(token: Request["token"], key: WorkspaceKey): void {
    if (
      this.attempts.size >= LIMITS.residents ||
      this.attempts.has(token.keyId) ||
      [...this.attempts.values()].some(
        (attempt) => attempt.key.cwd === key.cwd && attempt.key.variant === key.variant,
      )
    )
      throw computeFailure("protocol");
    // Runs before Scope.make / native creation. Records survive failure, close and loss of the JS resident.
    this.attempts.set(token.keyId, { token, key });
  }
}

/** Qualification supplies one path-shaped fff option, never executable code or process controls. */
export function decodeNativeBlockQualification(argument: string): {
  readonly targetWorkspace: string;
  readonly logFileHint: string;
} {
  const prefix = "--qualification-native-block=";
  if (!argument.startsWith(prefix) || Buffer.byteLength(argument) > 4096)
    throw computeFailure("protocol");
  const value: unknown = JSON.parse(argument.slice(prefix.length));
  const path = (input: unknown): input is string =>
    typeof input === "string" &&
    input.length <= 1800 &&
    NodePath.isAbsolute(input) &&
    NodePath.normalize(input) === input &&
    !/[\x00-\x1f\x7f]/.test(input);
  if (
    !record(value) ||
    !keys(value, ["targetWorkspace", "logFileHint"]) ||
    !path(value.targetWorkspace) ||
    !path(value.logFileHint) ||
    NodePath.basename(value.targetWorkspace) !== "blocked-workspace" ||
    NodePath.basename(value.logFileHint) !== "native-block.log" ||
    NodePath.basename(NodePath.dirname(value.logFileHint)) !== "barrier" ||
    NodePath.dirname(NodePath.dirname(value.logFileHint)) !==
      NodePath.dirname(value.targetWorkspace)
  )
    throw computeFailure("protocol");
  return Object.freeze({ targetWorkspace: value.targetWorkspace, logFileHint: value.logFileHint });
}

/** Closed hidden command. Root dispatches here before loading the normal server graph. */
export async function runServerComputeWorkerCommand(args: ReadonlyArray<string>): Promise<void> {
  const platform = HostProcess.Platform.defaultValue();
  if (
    (args.length !== 1 && args.length !== 2) ||
    args.some((argument) => Buffer.byteLength(argument) > 4096) ||
    (platform !== "linux" && platform !== "darwin")
  )
    throw computeFailure("protocol");
  const qualification = args.length === 2 ? decodeNativeBlockQualification(args[0]!) : undefined;
  const boot = decodeBootstrap(JSON.parse(args.at(-1)!));
  let qualificationUsed = false;
  if (boot.build !== (await currentBuildIdentity())) throw computeFailure("protocol");
  const heapLimit = NodeV8.getHeapStatistics().heap_size_limit;
  const heapSelected =
    process.execArgv.includes("--max-old-space-size=256") ||
    process.env.NODE_OPTIONS === "--max-old-space-size=256";
  if (!heapSelected || heapLimit < 256 * 1024 * 1024 || heapLimit > 400 * 1024 * 1024)
    throw computeFailure("unavailable");
  const guardian = new NodeWorkerThreads.Worker(guardianSource, { eval: true, workerData: boot });
  // In a Worker, process.exit would exit only that thread. This callback is on child main.
  guardian.on("error", () => process.kill(process.pid, "SIGKILL"));
  guardian.on("exit", () => process.kill(process.pid, "SIGKILL"));
  await new Promise<void>((resolve) =>
    guardian.once("message", (msg) => {
      if (msg?.type !== "go") process.kill(process.pid, "SIGKILL");
      resolve();
    }),
  );
  // GO was issued only after the parent durably registered this captured child PID.
  const [{ make }, Effect, Scope, Exit] = await Promise.all([
    import("../workspace/nativeSearchRuntime.ts"),
    import("effect/Effect"),
    import("effect/Scope"),
    import("effect/Exit"),
  ]);
  guardian.postMessage({
    type: "nativeReady",
    heapLimit,
    availableParallelism: NodeOS.availableParallelism(),
  });
  // Preserve typed native errors across the Promise boundary (runPromise rejection can wrap a Cause).
  const execute = async <A, E>(
    effect: EffectTypes.Effect<A, E>,
    signal?: AbortSignal,
  ): Promise<A> => {
    const result = await Effect.runPromise(
      effect.pipe(Effect.result),
      signal ? { signal } : undefined,
    );
    if (result._tag === "Failure") throw result.failure;
    return result.success;
  };
  type Native = EffectTypes.Success<ReturnType<typeof make>>;
  // Scope carries native destruction. No parent-side native instance exists.
  const residents = new Map<
    string,
    { token: Request["token"]; key: WorkspaceKey; scope: ScopeTypes.Closeable; runtime: Native }
  >();
  const allocations = new WorkerAllocations();
  let active: { id: number; controller: AbortController; replies: WorkerReplies } | undefined;
  let lastRequestId = 0,
    lastCompletedId = 0;
  let pendingCancellation: number | undefined;
  guardian.on("message", (msg) => {
    if (msg.type === "cancel" && Number.isSafeInteger(msg.requestId)) {
      if (msg.requestId <= lastCompletedId) return;
      if (!active && msg.requestId > lastRequestId && pendingCancellation === undefined) {
        pendingCancellation = msg.requestId;
        return;
      }
    }
    if (!active || msg.requestId !== active.id) return process.kill(process.pid, "SIGKILL");
    if (msg.type === "cancel") active.controller.abort();
    else if (msg.type === "credit") {
      try {
        active.replies.acknowledge();
      } catch {
        process.kill(process.pid, "SIGKILL");
      }
    } else process.kill(process.pid, "SIGKILL");
  });
  for await (const raw of frames(process.stdin, LIMITS.requestBytes)) {
    const request = decodeRequest(raw, boot);
    if (request.requestId <= lastRequestId || active) throw computeFailure("protocol");
    lastRequestId = request.requestId;
    active = {
      id: request.requestId,
      controller: new AbortController(),
      replies: new WorkerReplies(request, process.stdout),
    };
    const job = active;
    if (pendingCancellation !== undefined) {
      if (pendingCancellation !== request.requestId) throw computeFailure("protocol");
      job.controller.abort();
      pendingCancellation = undefined;
    }
    try {
      const { output, status } = await job.replies.execute(async () => {
        job.controller.signal.throwIfAborted();
        let resident = residents.get(request.token.keyId);
        if (request.operation === "open") {
          const key = request.input as WorkspaceKey;
          allocations.reserve(request.token, key);
          let createOptions: { readonly logFilePath: string } | undefined;
          if (qualification?.targetWorkspace === key.cwd) {
            if (qualificationUsed) throw computeFailure("protocol");
            qualificationUsed = true;
            createOptions = { logFilePath: qualification.logFileHint };
          }
          const scope = await Effect.runPromise(Scope.make());
          try {
            const runtime = await execute(
              make(key.cwd, key.variant, createOptions).pipe(
                Effect.provideService(Scope.Scope, scope),
              ),
              job.controller.signal,
            );
            resident = { token: request.token, key, scope, runtime };
            residents.set(request.token.keyId, resident);
          } catch (cause) {
            // Best-effort cleanup is not native exit proof. The parent retires this failed generation.
            await Effect.runPromise(Scope.close(scope, Exit.void));
            throw cause;
          }
        } else if (!resident || !sameToken(resident.token, request.token))
          throw computeFailure("protocol");
        if (!resident) throw computeFailure("protocol");
        let output: Output | undefined;
        switch (request.operation) {
          case "list":
            output = await execute(resident.runtime.list(), job.controller.signal);
            break;
          case "search": {
            const input = request.input as WorkspaceJobs["search"]["input"];
            output = await execute(
              resident.runtime.search(input.query, input.limit, input.kind, input.imageOnly),
              job.controller.signal,
            );
            break;
          }
          case "searchContents":
            output = await execute(
              resident.runtime.searchContents(
                request.input as WorkspaceJobs["searchContents"]["input"],
              ),
              job.controller.signal,
            );
            break;
          case "refresh":
            await execute(resident.runtime.refresh(), job.controller.signal);
            break;
          case "close":
            // Close is diagnostic teardown only; the allocation tombstone survives until process exit.
            await Effect.runPromise(Scope.close(resident.scope, Exit.void));
            residents.delete(request.token.keyId);
            break;
          case "open":
          case "status":
            break;
        }
        let status: ScanStatus | undefined;
        if (request.operation !== "close") {
          try {
            status = resident.runtime.status();
          } catch {
            throw computeFailure("protocol");
          }
        }
        return { output, status };
      }, job.controller.signal);
      await job.replies.result(output, status, job.controller.signal);
    } catch (cause) {
      await job.replies.failure(cause, job.controller.signal);
    } finally {
      // Drain written-frame credits before this main loop admits the next data request.
      await job.replies.settle();
      lastCompletedId = request.requestId;
      active = undefined;
    }
  }
  process.kill(process.pid, "SIGKILL");
}
