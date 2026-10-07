// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - This boundary owns an exact POSIX process group and absolute monotonic cleanup deadlines.
import * as NodeChildProcess from "node:child_process";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

const MAX_DECISION_SNAPSHOT_BYTES = 65_536;
export const monotonicSeconds = () => Number(process.hrtime.bigint()) / 1e9;
import {
  verifyRelease,
  RELEASE_CUSTODY,
  type CollectorBinding,
  type VerifiedRelease,
} from "./release.ts";
export type { CollectorBinding } from "./release.ts";
export class CollectorFailure extends Error {
  readonly reason:
    | "runtime_unavailable"
    | "release_mismatch"
    | "collector_failed"
    | "output_too_large"
    | "timeout"
    | "cleanup_unknown";
  constructor(reason: CollectorFailure["reason"]) {
    super(reason);
    this.reason = reason;
  }
}

export async function runBoundCollector(
  binding: CollectorBinding | null,
  purpose: "admission" | "display",
  nativeCounts: string | Promise<string>,
  deadlineMonotonic: number,
  signal: AbortSignal,
): Promise<string> {
  const packet = Promise.resolve(nativeCounts);
  void packet.catch(() => {});
  if (binding === null) throw new CollectorFailure("runtime_unavailable");
  const composerDeadline = deadlineMonotonic - 5;
  if (
    signal.aborted ||
    !Number.isFinite(deadlineMonotonic) ||
    composerDeadline <= monotonicSeconds()
  )
    throw new CollectorFailure("timeout");
  const root = binding.releaseDirectory;
  let verified: VerifiedRelease;
  let setupTimer: ReturnType<typeof setTimeout> | undefined;
  let setupAbort: (() => void) | undefined;
  try {
    await Promise.race([
      (async () => {
        verified = await verifyRelease(binding);
        const beforeLaunch = await verifyRelease(binding);
        if (beforeLaunch.fingerprint !== verified.fingerprint)
          throw new CollectorFailure("release_mismatch");
      })(),
      new Promise<never>((_resolve, reject) => {
        setupAbort = () => reject(new CollectorFailure("timeout"));
        signal.addEventListener("abort", setupAbort, { once: true });
        setupTimer = setTimeout(
          setupAbort,
          Math.max(1, Math.min(1_000, (composerDeadline - monotonicSeconds()) * 1_000)),
        );
      }),
    ]);
  } catch (cause) {
    throw cause instanceof CollectorFailure ? cause : new CollectorFailure("release_mismatch");
  } finally {
    if (setupTimer !== undefined) clearTimeout(setupTimer);
    if (setupAbort !== undefined) signal.removeEventListener("abort", setupAbort);
  }
  if (signal.aborted || composerDeadline <= monotonicSeconds())
    throw new CollectorFailure("timeout");
  const output = await new Promise<string>((resolve, reject) => {
    const child = NodeChildProcess.spawn(
      "/usr/bin/python3",
      [
        "-I",
        "-S",
        "-B",
        verified!.entrypoint,
        "--purpose",
        purpose,
        "--native-counts-stdin",
        "--deadline-monotonic",
        String(composerDeadline),
      ],
      {
        cwd: root,
        detached: true,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const chunks: Array<Buffer> = [];
    let bytes = 0;
    let failure: CollectorFailure | null = null;
    let grace: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    const killGroup = (mode: NodeJS.Signals) => {
      if (child.pid === undefined) return;
      try {
        process.kill(-child.pid, mode);
      } catch (cause) {
        if (!(cause instanceof Error && "code" in cause && cause.code === "ESRCH"))
          failure = new CollectorFailure("cleanup_unknown");
      }
    };
    const stop = (reason: CollectorFailure["reason"]) => {
      if (settled || failure !== null) return;
      failure = new CollectorFailure(reason);
      killGroup("SIGTERM");
      grace = setTimeout(
        () => {
          failure = new CollectorFailure("cleanup_unknown");
          killGroup("SIGKILL");
        },
        Math.max(1, Math.min(4_500, (deadlineMonotonic - monotonicSeconds()) * 1_000 - 500)),
      );
    };
    const abort = () => stop("timeout");
    signal.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(
      () => stop("timeout"),
      Math.max(1, (composerDeadline - monotonicSeconds()) * 1_000),
    );
    const closureDeadline = setTimeout(
      () => {
        if (settled) return;
        settled = true;
        killGroup("SIGKILL");
        clearTimeout(timer);
        if (grace !== undefined) clearTimeout(grace);
        signal.removeEventListener("abort", abort);
        child.stdin.destroy();
        child.stdout.destroy();
        child.stderr.destroy();
        reject(new CollectorFailure("cleanup_unknown"));
      },
      Math.max(1, (deadlineMonotonic - monotonicSeconds()) * 1_000 - 500),
    );
    child.stdout.on("data", (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > MAX_DECISION_SNAPSHOT_BYTES) stop("output_too_large");
      else chunks.push(chunk);
    });
    child.stderr.on("data", () => {});
    child.stdin.on("error", () => stop("collector_failed"));
    child.on("error", () => stop("collector_failed"));
    child.on("close", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(closureDeadline);
      clearTimeout(timer);
      if (grace !== undefined) clearTimeout(grace);
      signal.removeEventListener("abort", abort);
      killGroup("SIGKILL");
      if (
        failure !== null &&
        !(failure.reason === "timeout" && (code === 0 || code === 1) && bytes > 0)
      )
        reject(failure);
      else if ((code !== 0 && code !== 1) || bytes === 0)
        reject(new CollectorFailure("collector_failed"));
      else {
        try {
          resolve(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks)));
        } catch {
          reject(new CollectorFailure("collector_failed"));
        }
      }
    });
    void packet.then(
      (packet) => {
        if (settled || failure !== null) return;
        if (Buffer.byteLength(packet) > MAX_DECISION_SNAPSHOT_BYTES) {
          stop("collector_failed");
          return;
        }
        child.stdin.end(packet);
      },
      () => {
        if (!settled && failure === null) child.stdin.end();
      },
    );
  });
  let completionTimer: ReturnType<typeof setTimeout> | undefined;
  const after = await Promise.race([
    verifyRelease(binding).catch(() => {
      throw new CollectorFailure("release_mismatch");
    }),
    new Promise<never>((_resolve, reject) => {
      completionTimer = setTimeout(
        () => reject(new CollectorFailure("timeout")),
        Math.max(1, (deadlineMonotonic - monotonicSeconds()) * 1_000 - 500),
      );
    }),
  ]).finally(() => {
    if (completionTimer !== undefined) clearTimeout(completionTimer);
  });
  if (after.fingerprint !== verified!.fingerprint) throw new CollectorFailure("release_mismatch");
  return output;
}

export class DecisionSnapshotCollector extends Context.Service<
  DecisionSnapshotCollector,
  {
    readonly collect: (
      purpose: "admission" | "display",
      nativeCounts: Effect.Effect<string, CollectorFailure>,
      deadlineMonotonic: number,
    ) => Effect.Effect<string, CollectorFailure>;
  }
>()("t3/jones/mcp/decisionSnapshot/collector/DecisionSnapshotCollector") {}

export function makeCollector(
  binding: CollectorBinding | null,
): DecisionSnapshotCollector["Service"] {
  return {
    collect: (purpose, nativeCounts, deadlineMonotonic) =>
      Effect.callback<string, CollectorFailure>((resume, signal) => {
        const inputCancellation = new AbortController();
        const cancelInput = () => inputCancellation.abort();
        signal.addEventListener("abort", cancelInput, { once: true });
        const packet = Effect.runPromise(nativeCounts, { signal: inputCancellation.signal });
        void packet.catch(() => {});
        const running = runBoundCollector(
          binding,
          purpose,
          packet,
          deadlineMonotonic,
          signal,
        ).finally(() => {
          cancelInput();
          signal.removeEventListener("abort", cancelInput);
        });
        running.then(
          (output) => resume(Effect.succeed(output)),
          (cause) =>
            resume(
              Effect.fail(
                cause instanceof CollectorFailure
                  ? cause
                  : new CollectorFailure("collector_failed"),
              ),
            ),
        );
        return Effect.promise(() => {
          cancelInput();
          return Promise.allSettled([running, packet]).then(() => undefined);
        });
      }),
  };
}
export const DecisionSnapshotCollectorLive = Layer.sync(DecisionSnapshotCollector, () => {
  const releaseDirectory = process.env.T3_DECISION_SNAPSHOT_RELEASE_DIR;
  const releaseId = process.env.T3_DECISION_SNAPSHOT_RELEASE_ID;
  const manifestSha256 = process.env.T3_DECISION_SNAPSHOT_MANIFEST_SHA256;
  const custody = process.env.T3_DECISION_SNAPSHOT_RELEASE_CUSTODY;
  return makeCollector(
    releaseDirectory && releaseId && manifestSha256 && custody === RELEASE_CUSTODY
      ? { releaseDirectory, releaseId, manifestSha256, custody }
      : null,
  );
});
