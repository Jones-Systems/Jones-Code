// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Worker-only bounded FD4 capability, no spawner or VCS implementation.
import type * as NodeStream from "node:stream";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import { computeFailure, isComputeFailure } from "./failure.ts";
import type { Bootstrap } from "./protocol.ts";
import type { FinderToken } from "../serverParallelism/computeContract.ts";
import {
  PROBE_LIMITS as L,
  ProbeWriter,
  decodeProbeChunk,
  decodeProbeFrame,
  isProbeFailure,
  ownerKey,
  probeFailure,
  probeFrames,
  tokenKey,
  validateInput,
  type BeginInput,
  type ProbeOwner,
  type ProbeOutput,
  type SearchGitProbeFailure,
  type SearchGitProbeInput,
  type WorkerFrame,
} from "./searchProbeProtocol.ts";

export const CurrentProbeOwner = Context.Reference<ProbeOwner | undefined>(
  "t3/jones/SearchProbe/owner",
  {
    defaultValue: () => undefined,
  },
);
export interface ScanLease {
  readonly owner: ProbeOwner;
  readonly end: Effect.Effect<void, SearchGitProbeFailure>;
}
export class WorkspaceSearchProbe extends Context.Service<
  WorkspaceSearchProbe,
  {
    readonly run: (input: SearchGitProbeInput) => Effect.Effect<ProbeOutput, SearchGitProbeFailure>;
    readonly beginScan: Effect.Effect<ScanLease, SearchGitProbeFailure>;
  }
>()("t3/jones/serverCompute/searchProbeClient/WorkspaceSearchProbe") {}

interface Pending {
  readonly token: FinderToken;
  readonly owner: ProbeOwner;
  readonly probeId: number;
  readonly input: SearchGitProbeInput;
  readonly resolve: (output: ProbeOutput) => void;
  readonly reject: (cause: unknown) => void;
  readonly stdout: Buffer[];
  readonly stderr: Buffer[];
  stdoutBytes: number;
  stderrBytes: number;
  admitted: boolean;
  cancelled: boolean;
  downloadSequence: number;
  uploadSequence: number;
  uploadAcknowledged: number;
  wake?: () => void;
}
const key = (f: { token: FinderToken; owner: ProbeOwner; probeId: number }) =>
  `${tokenKey(f.token)}:${ownerKey(f.owner)}:${f.probeId}`;
const failure = (cause: unknown) =>
  isProbeFailure(cause)
    ? cause
    : isComputeFailure(cause) && (cause.reason === "protocol" || cause.reason === "oversize")
      ? probeFailure(cause.reason, cause)
      : probeFailure("transport", cause);

export class SearchProbeClient {
  private readonly boot: Bootstrap;
  private readonly pipe: NodeStream.Duplex;
  private readonly onFault: (cause: unknown) => void;
  private readonly writer: ProbeWriter;
  private nextProbe = 0;
  private readonly pending = new Map<string, Pending>();
  private readonly scans = new Map<
    string,
    { resolve: () => void; reject: (cause: unknown) => void }
  >();
  private readonly epochs = new Map<string, number>();
  private fault: unknown;
  constructor(boot: Bootstrap, pipe: NodeStream.Duplex, onFault: (cause: unknown) => void) {
    this.boot = boot;
    this.pipe = pipe;
    this.onFault = onFault;
    this.writer = new ProbeWriter(pipe);
    pipe.on("error", (cause) => this.fail(cause));
    void this.read().catch((cause) => this.fail(cause));
  }
  private fail(cause: unknown) {
    if (this.fault !== undefined) return;
    this.fault = cause;
    for (const p of this.pending.values()) {
      p.cancelled = true;
      p.wake?.();
      p.reject(cause);
    }
    for (const scan of this.scans.values()) scan.reject(cause);
    this.onFault(computeFailure("protocol", cause));
  }
  private envelope(p: Pick<Pending, "token" | "owner" | "probeId">) {
    return {
      ...this.boot,
      probeVersion: 1 as const,
      token: p.token,
      owner: p.owner,
      probeId: p.probeId,
    };
  }
  private async upload(p: Pending) {
    if (p.input.kind !== "ignoredCandidates") return;
    const bytes = Buffer.from(JSON.stringify(p.input.paths));
    for (let offset = 0; offset < bytes.length; offset += L.chunkBytes) {
      while (p.uploadSequence - p.uploadAcknowledged >= L.credits) {
        if (p.cancelled || this.fault) return;
        await new Promise<void>((resolve) => {
          p.wake = resolve;
        });
      }
      if (p.cancelled || this.fault) return;
      const chunk = bytes.subarray(offset, offset + L.chunkBytes);
      await this.writer.send({
        ...this.envelope(p),
        type: "upload",
        sequence: p.uploadSequence++,
        base64: chunk.toString("base64"),
        last: offset + chunk.length === bytes.length,
      });
    }
  }
  private async read() {
    for await (const raw of probeFrames(this.pipe)) {
      const f = decodeProbeFrame(raw, this.boot, "parent");
      if (f.type === "scanGranted" || f.type === "scanRevoked") {
        const scanKey = `${tokenKey(f.token)}:${f.epoch}`;
        const scan = this.scans.get(scanKey);
        if (!scan) throw probeFailure("protocol");
        this.scans.delete(scanKey);
        if (f.type === "scanGranted") scan.resolve();
        else scan.reject(probeFailure(f.reason));
        continue;
      }
      const p = this.pending.get(key(f));
      if (!p) throw probeFailure("protocol");
      if (f.type === "admitted") {
        if (p.admitted) throw probeFailure("protocol");
        p.admitted = true;
        if (!p.cancelled) void this.upload(p).catch((cause) => this.fail(cause));
      } else if (f.type === "uploadCredit") {
        if (!p.admitted || f.sequence !== p.uploadAcknowledged || f.sequence >= p.uploadSequence)
          throw probeFailure("protocol");
        p.uploadAcknowledged++;
        p.wake?.();
        delete p.wake;
      } else if (f.type === "download") {
        if (!p.admitted || f.sequence !== p.downloadSequence++) throw probeFailure("protocol");
        const bytes = decodeProbeChunk(f.base64);
        const limit =
          p.input.kind === "grep"
            ? L.grepBytes
            : p.input.kind === "repositoryPresence"
              ? L.repositoryBytes
              : L.ignoreBytes;
        const total =
          f.stream === "stdout" ? (p.stdoutBytes += bytes.length) : (p.stderrBytes += bytes.length);
        if (total > limit) throw probeFailure("oversize");
        if (!p.cancelled) p[f.stream].push(bytes);
        await this.writer.send({
          ...this.envelope(p),
          type: "downloadCredit",
          sequence: f.sequence,
        });
      } else {
        if (
          f.type === "done" &&
          (p.stdoutBytes !== f.stdoutBytes ||
            p.stderrBytes !== f.stderrBytes ||
            p.uploadAcknowledged !== p.uploadSequence)
        )
          throw probeFailure("protocol");
        const cancelled = p.cancelled;
        // Stop any upload continuation before queuing the terminal receipt.
        p.cancelled = true;
        p.wake?.();
        await this.writer.send({ ...this.envelope(p), type: "terminalAck" });
        this.pending.delete(key(f));
        if (f.type === "failure" && f.reason === "protocol") {
          p.reject(probeFailure("protocol"));
          this.fail(probeFailure("protocol"));
          return;
        }
        if (!cancelled) {
          if (f.type === "failure")
            p.reject(
              f.reason === "protocol" || f.reason === "oversize"
                ? computeFailure(f.reason)
                : probeFailure(f.reason),
            );
          else {
            try {
              const decoder = new TextDecoder("utf-8", { fatal: true });
              p.resolve({
                ...f.output,
                stdout: decoder.decode(Buffer.concat(p.stdout)),
                stderr: decoder.decode(Buffer.concat(p.stderr)),
              });
            } catch (cause) {
              p.reject(probeFailure("protocol", cause));
              throw cause;
            }
          }
        }
      }
    }
    throw probeFailure("transport");
  }
  private run(
    token: FinderToken,
    owner: ProbeOwner,
    input: SearchGitProbeInput,
    signal: AbortSignal,
  ): Promise<ProbeOutput> {
    validateInput(input);
    if (this.fault) return Promise.reject(this.fault);
    if (signal.aborted) return Promise.reject(probeFailure("cancelled"));
    if (!Number.isSafeInteger(++this.nextProbe) || this.pending.size >= L.perSlot)
      return Promise.reject(probeFailure("queue-full"));
    let resolve!: (output: ProbeOutput) => void, reject!: (cause: unknown) => void;
    const result = new Promise<ProbeOutput>((ok, fail) => {
      resolve = ok;
      reject = fail;
    });
    const p: Pending = {
      token,
      owner,
      input,
      probeId: this.nextProbe,
      resolve,
      reject,
      stdout: [],
      stderr: [],
      stdoutBytes: 0,
      stderrBytes: 0,
      admitted: false,
      cancelled: false,
      downloadSequence: 0,
      uploadSequence: 0,
      uploadAcknowledged: 0,
    };
    const cancel = () => {
      if (!this.pending.has(key(p)) || p.cancelled) return;
      p.cancelled = true;
      p.stdout.length = p.stderr.length = 0;
      p.wake?.();
      p.reject(probeFailure("cancelled"));
      void this.writer
        .send({ ...this.envelope(p), type: "cancel" })
        .catch((cause) => this.fail(cause));
    };
    const descriptor: BeginInput =
      input.kind === "ignoredCandidates"
        ? {
            kind: input.kind,
            repository: input.repository,
            pathCount: input.paths.length,
            stdinBytes: input.paths.reduce((n, path) => n + Buffer.byteLength(path) + 1, 0),
            uploadBytes: Buffer.byteLength(JSON.stringify(input.paths)),
          }
        : input;
    const begin: WorkerFrame = { ...this.envelope(p), type: "begin", input: descriptor };
    // Validate encoded size before retaining a caller or sending any identity.
    // A local oversize query keeps the ordinary compute oversize failure ABI.
    decodeProbeFrame(begin, this.boot, "worker");
    this.pending.set(key(p), p);
    // Begin is queued first so cancellation cannot precede its declared identity.
    void this.writer.send(begin).catch((cause) => this.fail(cause));
    signal.addEventListener("abort", cancel, { once: true });
    if (signal.aborted) cancel();
    return result.finally(() => signal.removeEventListener("abort", cancel));
  }
  forToken(token: FinderToken): WorkspaceSearchProbe["Service"] {
    const client = this;
    return WorkspaceSearchProbe.of({
      run: Effect.fn("WorkspaceSearchProbe.run")(function* (input) {
        const owner = yield* CurrentProbeOwner;
        if (!owner) return yield* Effect.fail(probeFailure("protocol"));
        return yield* Effect.tryPromise({
          try: (signal) => client.run(token, owner, input, signal),
          catch: failure,
        });
      }),
      beginScan: Effect.gen(function* () {
        const birth = yield* CurrentProbeOwner;
        if (birth?.kind !== "foreground") return yield* Effect.fail(probeFailure("protocol"));
        const tokenId = tokenKey(token),
          epoch = (client.epochs.get(tokenId) ?? 0) + 1;
        if (!Number.isSafeInteger(epoch)) return yield* Effect.fail(probeFailure("protocol"));
        client.epochs.set(tokenId, epoch);
        const scanKey = `${tokenId}:${epoch}`;
        yield* Effect.tryPromise({
          try: () =>
            new Promise<void>((resolve, reject) => {
              client.scans.set(scanKey, { resolve, reject });
              void client.writer
                .send({
                  ...client.boot,
                  probeVersion: 1,
                  token,
                  type: "scanBegin",
                  epoch,
                  birthRequestId: birth.requestId,
                })
                .catch(reject);
            }),
          catch: failure,
        });
        let ending: Promise<void> | undefined;
        return {
          owner: { kind: "scan" as const, epoch },
          end: Effect.tryPromise({
            // The close acknowledgement also validates the original parent's lease
            // before a native scan can publish its final result.
            try: () =>
              (ending ??= new Promise<void>((resolve, reject) => {
                client.scans.set(scanKey, { resolve, reject });
                void client.writer
                  .send({ ...client.boot, probeVersion: 1, token, type: "scanEnd", epoch })
                  .catch(reject);
              })),
            catch: failure,
          }),
        };
      }),
    });
  }
}
