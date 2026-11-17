// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Named private Promise/Effect boundary; no process creation.
import type * as NodeStream from "node:stream";
import * as Effect from "effect/Effect";
import type { SearchGitProbeService, ForegroundProbeOwner } from "../workspace/SearchGitProbe.ts";
import type { FinderToken } from "../serverParallelism/computeContract.ts";
import { sameToken, type Bootstrap, type Request } from "./protocol.ts";
import {
  PROBE_LIMITS as L,
  ProbeWriter,
  decodeProbeChunk,
  decodeProbeFrame,
  probeFailure,
  isProbeFailure,
  ownerKey,
  probeFrames,
  tokenKey,
  validatePaths,
  type ParentFrame,
  type ProbeOutput,
  type SearchGitProbeFailure,
  type SearchGitProbeInput,
  type WorkerFrame,
} from "./searchProbeProtocol.ts";

/** Uses the one captured service. No per-worker runtime or VCS pool is created. */
export function searchProbePort(service: SearchGitProbeService) {
  const run = async <A>(
    effect: Effect.Effect<A, SearchGitProbeFailure>,
    signal?: AbortSignal,
  ): Promise<A> => {
    const result = await Effect.runPromise(
      effect.pipe(Effect.result),
      signal ? { signal } : undefined,
    );
    if (result._tag === "Failure") throw result.failure;
    return result.success;
  };
  return { service, run };
}
export type SearchProbePort = ReturnType<typeof searchProbePort>;
type Begin = Extract<WorkerFrame, { type: "begin" }>;
interface Transfer {
  readonly begin: Begin;
  readonly chunks: Buffer[];
  uploadSequence: number;
  uploadBytes: number;
  admitted: boolean;
  inputReady: boolean;
  cancelled: boolean;
  terminal: boolean;
  downloadSequence: number;
  acknowledgedSequence: number;
  wake?: () => void;
}
const callKey = (f: { token: FinderToken; owner: Begin["owner"]; probeId: number }) =>
  `${tokenKey(f.token)}:${ownerKey(f.owner)}:${f.probeId}`;

/** FD4 is read independently of the blocked foreground compute request. */
export class SearchProbeBinding {
  private readonly boot: Bootstrap;
  private readonly pipe: NodeStream.Duplex;
  private readonly port: SearchProbePort;
  private readonly fault: (cause: unknown) => void;
  private readonly writer: ProbeWriter;
  private readonly transfers = new Map<string, Transfer>();
  private stopped = false;
  private lastId = 0;
  constructor(
    boot: Bootstrap,
    pipe: NodeStream.Duplex,
    port: SearchProbePort,
    fault: (cause: unknown) => void,
  ) {
    this.boot = boot;
    this.pipe = pipe;
    this.port = port;
    this.fault = fault;
    this.writer = new ProbeWriter(pipe);
    pipe.on("error", (cause) => this.fail(cause));
    void this.read().catch((cause) => this.fail(cause));
  }
  private fail(cause: unknown) {
    if (this.stopped) return;
    this.revoke();
    this.fault(cause);
  }
  async foregroundBegin(request: Request, deadlineMonotonicMs: number) {
    if (this.stopped) throw probeFailure("binding-closed");
    if (request.operation === "open")
      await this.port.run(
        this.port.service.bind({
          bootstrap: this.boot,
          token: request.token,
          key: request.input as { cwd: string; variant: "paths" | "content" },
        }),
      );
    const input: ForegroundProbeOwner = {
      token: request.token,
      requestId: request.requestId,
      operation: request.operation,
      input: request.input,
      deadlineMonotonicMs,
    };
    await this.port.run(this.port.service.foregroundBegin(input));
  }
  async foregroundEnd(request: Request, succeeded: boolean) {
    if (this.stopped) return;
    if (request.operation === "close" || (request.operation === "open" && !succeeded)) {
      await this.port.run(this.port.service.revoke(request.token));
    } else {
      if (request.operation === "open")
        await this.port.run(this.port.service.opened(request.token));
      await this.port.run(this.port.service.foregroundEnd(request.token, request.requestId));
    }
  }
  /** Revokes publication synchronously; auxiliary finalization remains service-owned. */
  revoke() {
    if (this.stopped) return;
    this.stopped = true;
    for (const transfer of this.transfers.values()) {
      transfer.cancelled = true;
      transfer.wake?.();
    }
    void this.port.run(this.port.service.revokeSlot(this.boot)).catch(() => {});
    this.pipe.destroy();
  }
  private envelope(begin: Begin) {
    return {
      ...this.boot,
      probeVersion: 1 as const,
      token: begin.token,
      owner: begin.owner,
      probeId: begin.probeId,
    };
  }
  private async terminal(t: Transfer, reason: ParentFrame & { type: "failure" }) {
    if (t.terminal || this.stopped) return;
    t.terminal = true;
    t.cancelled = true;
    t.wake?.();
    t.chunks.length = 0;
    await this.writer.send(reason);
  }
  private async start(t: Transfer) {
    const { token, owner, probeId, input } = t.begin;
    try {
      await this.port.run(this.port.service.prepare(token, owner, probeId, input));
      if (t.cancelled || this.stopped) {
        // Cancellation can race the Effect admission boundary. The exact late
        // record is cancelled/observed, never launched or abandoned.
        await this.port.run(this.port.service.cancel(token, owner, probeId));
        await this.port.run(this.port.service.acknowledge(token, owner, probeId));
        return;
      }
      t.admitted = true;
      await this.writer.send({ ...this.envelope(t.begin), type: "admitted" });
      if (input.kind !== "ignoredCandidates") {
        t.inputReady = true;
        await this.execute(t, input);
      }
    } catch (cause) {
      await this.terminal(t, {
        ...this.envelope(t.begin),
        type: "failure",
        reason: isProbeFailure(cause) ? cause.reason : "process-failed",
      });
    }
  }
  private async execute(t: Transfer, input: SearchGitProbeInput) {
    const { token, owner, probeId } = t.begin;
    try {
      const result = await this.port.run(this.port.service.run(token, owner, probeId, input));
      if (t.cancelled || this.stopped || t.terminal) return;
      await this.deliver(t, result);
    } catch (cause) {
      await this.terminal(t, {
        ...this.envelope(t.begin),
        type: "failure",
        reason: isProbeFailure(cause) ? cause.reason : "process-failed",
      });
    }
  }
  private async deliver(t: Transfer, result: ProbeOutput) {
    const check = () =>
      this.port.run(this.port.service.checkDelivery(t.begin.token, t.begin.owner, t.begin.probeId));
    const stdout = Buffer.from(result.stdout),
      stderr = Buffer.from(result.stderr);
    if (stdout.length + stderr.length > L.resultBytes) throw probeFailure("oversize");
    for (const [stream, bytes] of [
      ["stdout", stdout],
      ["stderr", stderr],
    ] as const) {
      for (let offset = 0; offset < bytes.length; offset += L.chunkBytes) {
        while (t.downloadSequence - t.acknowledgedSequence >= L.credits) {
          if (t.cancelled || this.stopped) return;
          await new Promise<void>((resolve) => {
            t.wake = resolve;
          });
        }
        if (t.cancelled || this.stopped) return;
        await check();
        await this.writer.send({
          ...this.envelope(t.begin),
          type: "download",
          sequence: t.downloadSequence++,
          stream,
          base64: bytes.subarray(offset, offset + L.chunkBytes).toString("base64"),
        });
      }
    }
    while (t.acknowledgedSequence !== t.downloadSequence) {
      if (t.cancelled || this.stopped) return;
      await new Promise<void>((resolve) => {
        t.wake = resolve;
      });
    }
    if (t.cancelled || this.stopped) return;
    await check();
    const { stdout: _stdout, stderr: _stderr, ...output } = result;
    t.terminal = true;
    await this.writer.send({
      ...this.envelope(t.begin),
      type: "done",
      output,
      stdoutBytes: stdout.length,
      stderrBytes: stderr.length,
    });
  }
  private async read() {
    for await (const raw of probeFrames(this.pipe)) {
      if (this.stopped) return;
      const frame = decodeProbeFrame(raw, this.boot, "worker");
      if (frame.type === "scanBegin" || frame.type === "scanEnd") {
        const base = {
          ...this.boot,
          probeVersion: 1 as const,
          token: frame.token,
          epoch: frame.epoch,
        };
        try {
          if (frame.type === "scanEnd")
            await this.port.run(this.port.service.scanEnd(frame.token, frame.epoch));
          else
            await this.port.run(
              this.port.service.scanBegin(frame.token, frame.epoch, frame.birthRequestId),
            );
          await this.writer.send({ ...base, type: "scanGranted" });
        } catch (cause) {
          await this.writer.send({
            ...base,
            type: "scanRevoked",
            reason: isProbeFailure(cause) ? cause.reason : "protocol",
          });
        }
        continue;
      }
      const key = callKey(frame);
      if (frame.type === "begin") {
        if (
          frame.probeId <= this.lastId ||
          this.transfers.size >= L.perSlot ||
          this.transfers.has(key)
        )
          throw probeFailure("protocol");
        this.lastId = frame.probeId;
        const t: Transfer = {
          begin: frame,
          chunks: [],
          uploadSequence: 0,
          uploadBytes: 0,
          admitted: false,
          inputReady: false,
          cancelled: false,
          terminal: false,
          downloadSequence: 0,
          acknowledgedSequence: 0,
        };
        this.transfers.set(key, t);
        void this.start(t).catch((cause) => this.fail(cause));
        continue;
      }
      const t = this.transfers.get(key);
      if (!t || !sameToken(t.begin.token, frame.token)) throw probeFailure("protocol");
      if (frame.type === "terminalAck") {
        if (!t.terminal) throw probeFailure("protocol");
        await this.port.run(this.port.service.acknowledge(frame.token, frame.owner, frame.probeId));
        this.transfers.delete(key);
      } else if (frame.type === "cancel") {
        if (t.cancelled) throw probeFailure("protocol");
        t.cancelled = true;
        t.wake?.();
        await this.port.run(this.port.service.cancel(frame.token, frame.owner, frame.probeId));
        await this.terminal(t, { ...this.envelope(t.begin), type: "failure", reason: "cancelled" });
      } else if (frame.type === "downloadCredit") {
        if (frame.sequence !== t.acknowledgedSequence || frame.sequence >= t.downloadSequence)
          throw probeFailure("protocol");
        t.acknowledgedSequence++;
        t.wake?.();
        delete t.wake;
      } else if (frame.type === "upload") {
        const descriptor = t.begin.input;
        if (
          !t.admitted ||
          t.inputReady ||
          descriptor.kind !== "ignoredCandidates" ||
          frame.sequence !== t.uploadSequence++
        )
          throw probeFailure("protocol");
        const bytes = decodeProbeChunk(frame.base64);
        if (!frame.last && bytes.length !== L.chunkBytes) throw probeFailure("protocol");
        if ((t.uploadBytes += bytes.length) > descriptor.uploadBytes)
          throw probeFailure("oversize");
        // Consume bounded in-flight chunks after cancellation; never dispatch them.
        if (!t.cancelled) t.chunks.push(bytes);
        await this.writer.send({
          ...this.envelope(t.begin),
          type: "uploadCredit",
          sequence: frame.sequence,
        });
        if (frame.last) {
          t.inputReady = true;
          if (t.uploadBytes !== descriptor.uploadBytes) throw probeFailure("protocol");
          if (!t.cancelled) {
            const bytes = Buffer.concat(t.chunks);
            t.chunks.length = 0;
            const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
            const paths: unknown = JSON.parse(text);
            validatePaths(paths);
            if (
              paths.length !== descriptor.pathCount ||
              paths.reduce((n, p) => n + Buffer.byteLength(p) + 1, 0) !== descriptor.stdinBytes
            )
              throw probeFailure("protocol");
            void this.execute(t, {
              kind: "ignoredCandidates",
              repository: descriptor.repository,
              paths,
            }).catch((cause) => this.fail(cause));
          }
        }
      }
    }
    if (!this.stopped) throw probeFailure("transport");
  }
}
