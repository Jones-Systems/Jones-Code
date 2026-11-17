// @effect-diagnostics globalTimers:off globalDate:off -- Independent monotonic auxiliary leases; no process signaling here.
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import type { FinderToken } from "../serverParallelism/computeContract.ts";
import { isKey, type Bootstrap, type Operation } from "../serverCompute/protocol.ts";
import {
  PROBE_LIMITS as L,
  isProbeFailure,
  ownerKey,
  probeFailure,
  slotKey,
  tokenKey,
  validateInput,
  type BeginInput,
  type ProbeOwner,
  type ProbeOutput,
  type SearchGitProbeFailure,
  type SearchGitProbeInput,
  type TrustedBinding,
} from "../serverCompute/searchProbeProtocol.ts";

export interface ProbeClock {
  readonly now: () => number;
  readonly schedule: (milliseconds: number, callback: () => void) => () => void;
}
export const SearchProbeClock = Context.Reference<ProbeClock>("t3/jones/SearchGitProbe/clock", {
  defaultValue: () => ({
    now: () => performance.now(),
    schedule: (milliseconds, callback) => {
      const handle = setTimeout(callback, milliseconds);
      return () => clearTimeout(handle);
    },
  }),
});

export interface ForegroundProbeOwner {
  readonly token: FinderToken;
  readonly requestId: number;
  readonly operation: Operation;
  readonly input: unknown;
  readonly deadlineMonotonicMs: number;
}
interface Lease {
  readonly owner: ProbeOwner;
  readonly deadline: number;
  readonly operation: Operation;
  readonly input: unknown;
  probes: number;
  stdin: number;
  stdout: number;
  stderr: number;
  live: boolean;
  timer: () => void;
}
interface Binding extends TrustedBinding {
  state: "opening" | "live" | "revoked";
  lastRequestId: number;
  lastEpoch: number;
  readonly leases: Map<string, Lease>;
}
interface Call {
  readonly binding: Binding;
  readonly lease: Lease;
  readonly probeId: number;
  readonly descriptor: BeginInput;
  readonly begun: number;
  readonly descriptorBytes: number;
  readonly admitted: Promise<void>;
  readonly resolveAdmission: () => void;
  readonly rejectAdmission: (cause: SearchGitProbeFailure) => void;
  active: boolean;
  started: boolean;
  settled: boolean;
  acknowledged: boolean;
  cancelled: SearchGitProbeFailure | undefined;
  fiber?: Fiber.Fiber<ProbeOutput, SearchGitProbeFailure>;
  timer: () => void;
  cleanupTimer?: () => void;
  finalizerDefect: boolean;
}
export interface SearchGitProbeService {
  readonly bind: (binding: TrustedBinding) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly opened: (token: FinderToken) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly foregroundBegin: (
    input: ForegroundProbeOwner,
  ) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly foregroundEnd: (
    token: FinderToken,
    requestId: number,
  ) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly scanBegin: (
    token: FinderToken,
    epoch: number,
    birthRequestId: number,
  ) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly scanEnd: (
    token: FinderToken,
    epoch: number,
  ) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly run: (
    token: FinderToken,
    owner: ProbeOwner,
    probeId: number,
    input: SearchGitProbeInput,
  ) => Effect.Effect<ProbeOutput, SearchGitProbeFailure>;
  readonly cancel: (
    token: FinderToken,
    owner: ProbeOwner,
    probeId: number,
  ) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly revoke: (token: FinderToken) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly revokeSlot: (boot: Bootstrap) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly shutdown: () => Effect.Effect<void, SearchGitProbeFailure>;
  // Trusted FD4 transfer accounting. These never expand the worker command surface.
  readonly prepare: (
    token: FinderToken,
    owner: ProbeOwner,
    probeId: number,
    input: BeginInput,
  ) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly acknowledge: (
    token: FinderToken,
    owner: ProbeOwner,
    probeId: number,
  ) => Effect.Effect<void, SearchGitProbeFailure>;
  readonly checkDelivery: (
    token: FinderToken,
    owner: ProbeOwner,
    probeId: number,
  ) => Effect.Effect<void, SearchGitProbeFailure>;
}
export class SearchGitProbe extends Context.Service<SearchGitProbe, SearchGitProbeService>()(
  "t3/jones/workspace/SearchGitProbe",
) {}

/** Closed command builder. No transport-provided argv, environment, timeout or executable. */
export function probeCommand(
  input: SearchGitProbeInput,
  cwd: string,
  remainingMs: number,
): VcsProcess.VcsProcessInput {
  validateInput(input);
  const prefix = [
    "--no-pager",
    "--no-lazy-fetch",
    "--no-optional-locks",
    "-c",
    "core.fsmonitor=false",
  ];
  const grep = input.kind === "grep";
  const args =
    input.kind === "ignoredPaths"
      ? ["ls-files", "--others", "--ignored", "--exclude-standard", "--directory", "-z"]
      : input.kind === "ignoredCandidates"
        ? ["check-ignore", "--no-index", "-z", "--stdin"]
        : input.kind === "repositoryPresence"
          ? ["rev-parse", "--git-dir"]
          : [
              "-c",
              "core.quotePath=false",
              "-c",
              "grep.column=false",
              "-c",
              "grep.fullName=false",
              "-c",
              "submodule.recurse=false",
              "grep",
              "--untracked",
              "--no-textconv",
              "--no-recurse-submodules",
              "--threads=1",
              "--no-color",
              "-I",
              "-n",
              "-z",
              "-m",
              "100",
              ...(input.onlyMatching ? ["-o", "--column"] : []),
              ...(input.ignoreCase ? ["-i"] : []),
              input.regexMode ? "-P" : "-F",
              "-e",
              input.pattern,
            ];
  if (!Number.isFinite(remainingMs) || remainingMs <= 0) throw probeFailure("deadline");
  return {
    command: "git",
    operation:
      input.kind === "grep"
        ? "WorkspaceSearchIndex.gitGrep"
        : input.kind === "ignoredPaths"
          ? "WorkspaceSearchIndex.listGitIgnoredPaths"
          : "WorkspaceSearchIndex.listGitIgnored",
    cwd,
    args: [...prefix, ...args],
    ...(input.kind === "ignoredCandidates" ? { stdin: `${input.paths.join("\0")}\0` } : {}),
    env: { LC_ALL: "C", LANG: "C", GIT_TERMINAL_PROMPT: "0" },
    allowNonZeroExit: true,
    timeoutMs: Math.max(1, Math.min(grep ? 30_000 : 10_000, Math.floor(remainingMs))),
    maxOutputBytes: grep
      ? L.grepBytes
      : input.kind === "repositoryPresence"
        ? L.repositoryBytes
        : L.ignoreBytes,
    outputMode: "truncate",
    appendTruncationMarker: false,
  };
}
const failure = (cause: unknown) =>
  isProbeFailure(cause) ? cause : probeFailure("process-failed", cause);
const encodePaths = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
export const make = Effect.gen(function* () {
  const vcs = yield* VcsProcess.VcsProcess;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const scope = yield* Effect.scope;
  const context = yield* Effect.context<never>();
  const runFork = Effect.runForkWith(context);
  const bindings = new Map<string, Binding>();
  const calls = new Map<string, Call>();
  const lastProbe = new Map<string, number>();
  let closed = false;
  const clock = yield* SearchProbeClock;
  const now = clock.now;
  const id = (token: FinderToken, owner: ProbeOwner, probeId: number) =>
    `${tokenKey(token)}:${ownerKey(owner)}:${probeId}`;
  const getBinding = (token: FinderToken) => {
    const b = bindings.get(tokenKey(token));
    if (!b || b.state === "revoked") throw probeFailure("binding-closed");
    return b;
  };
  const cleanupUnknown = () =>
    [...calls.values()].some(
      (c) =>
        c.finalizerDefect ||
        (c.cancelled !== undefined &&
          !c.settled &&
          now() - c.begun > L.cleanupMs &&
          c.cleanupTimer === undefined),
    );
  const release = (call: Call) => {
    if (!call.acknowledged || !call.settled || call.finalizerDefect) return;
    call.timer();
    call.cleanupTimer?.();
    calls.delete(id(call.binding.token, call.lease.owner, call.probeId));
    pump();
  };
  const pump = () => {
    if (closed || cleanupUnknown()) return;
    let active = [...calls.values()].filter((c) => c.active).length;
    for (const c of calls.values()) {
      if (active >= L.active) return;
      if (c.active || c.cancelled) continue;
      if (
        [...calls.values()].some(
          (a) =>
            a !== c && a.active && slotKey(a.binding.bootstrap) === slotKey(c.binding.bootstrap),
        )
      )
        continue;
      c.active = true;
      active++;
      c.resolveAdmission();
    }
  };
  const interruptFiber = (call: Call) => {
    if (call.fiber && !call.settled) {
      // Scope exit is observed separately; sent interruption never refunds a charge.
      runFork(Fiber.interrupt(call.fiber).pipe(Effect.forkIn(scope)));
      call.cleanupTimer ??= clock.schedule(L.cleanupMs, () => {
        delete call.cleanupTimer;
        if (!call.settled) call.cancelled = probeFailure("cleanup-unknown", call.cancelled);
      });
    }
  };
  const interrupt = (call: Call, why: SearchGitProbeFailure) => {
    if (call.cancelled) return;
    call.cancelled = why;
    call.timer();
    call.rejectAdmission(why);
    interruptFiber(call);
    if (!call.fiber && !call.started) call.settled = true;
    release(call);
  };
  const endLease = (binding: Binding, key: string) => {
    const lease = binding.leases.get(key);
    if (!lease) return;
    lease.live = false;
    lease.timer();
    for (const c of calls.values())
      if (c.binding === binding && c.lease === lease) interrupt(c, probeFailure("cancelled"));
    // Retain until ordinary calls settle; later epochs cannot relabel them.
    binding.leases.delete(key);
  };
  const revokeBinding = (b: Binding) => {
    b.state = "revoked";
    for (const key of b.leases.keys()) endLease(b, key);
    // Revocation discards publication, but ordinary scopes still own their charges.
    for (const c of calls.values())
      if (c.binding === b) {
        c.acknowledged = true;
        release(c);
      }
    bindings.delete(tokenKey(b.token));
  };
  const trySync = <A>(run: () => A) => Effect.try({ try: run, catch: failure });
  const makeLease = (
    b: Binding,
    owner: ProbeOwner,
    operation: Operation,
    input: unknown,
    deadline: number,
  ) => {
    const lease: Lease = {
      owner,
      deadline,
      operation,
      input,
      probes: 0,
      stdin: 0,
      stdout: 0,
      stderr: 0,
      live: true,
      timer: clock.schedule(Math.max(0, deadline - now() - L.cancelReserveMs), () =>
        endLease(b, ownerKey(owner)),
      ),
    };
    b.leases.set(ownerKey(owner), lease);
  };
  const prepare = Effect.fn("SearchGitProbe.prepare")(function* (
    token: FinderToken,
    owner: ProbeOwner,
    probeId: number,
    descriptor: BeginInput,
  ) {
    const call = yield* trySync(() => {
      if (closed) throw probeFailure("binding-closed");
      if (cleanupUnknown()) throw probeFailure("cleanup-unknown");
      const b = getBinding(token),
        lease = b.leases.get(ownerKey(owner));
      if (!lease?.live || lease.deadline - L.cancelReserveMs <= now())
        throw probeFailure("deadline");
      const slot = slotKey(b.bootstrap);
      if (!Number.isSafeInteger(probeId) || probeId <= (lastProbe.get(slot) ?? 0))
        throw probeFailure("protocol");
      lastProbe.set(slot, probeId);
      const all = [...calls.values()];
      if (
        all.length >= L.outstanding ||
        all.filter((c) => c.binding === b).length >= L.perToken ||
        all.filter((c) => slotKey(c.binding.bootstrap) === slot).length >= L.perSlot
      )
        throw probeFailure("queue-full");
      const descriptorBytes = Buffer.byteLength(JSON.stringify(descriptor));
      if (all.reduce((n, c) => n + c.descriptorBytes, 0) + descriptorBytes > L.descriptorBytes)
        throw probeFailure("oversize");
      if (descriptor.kind === "grep") {
        const input = lease.input as {
          query?: string;
          caseSensitive?: boolean;
          useRegex?: boolean;
        };
        if (
          owner.kind !== "foreground" ||
          lease.operation !== "searchContents" ||
          descriptor.pattern !== input.query ||
          descriptor.ignoreCase !== !input.caseSensitive ||
          (descriptor.regexMode && !input.useRegex) ||
          (descriptor.onlyMatching && !descriptor.regexMode)
        )
          throw probeFailure("protocol");
      } else if (descriptor.kind === "ignoredPaths") {
        if (
          owner.kind !== "foreground" ||
          !["open", "refresh", "refreshContent"].includes(lease.operation)
        )
          throw probeFailure("protocol");
      } else if (owner.kind !== "scan") throw probeFailure("protocol");
      const stdin = descriptor.kind === "ignoredCandidates" ? descriptor.stdinBytes : 0;
      if (++lease.probes > L.ownerProbes || (lease.stdin += stdin) > L.ownerStdinBytes)
        throw probeFailure("oversize");
      let resolveAdmission!: () => void, rejectAdmission!: (cause: SearchGitProbeFailure) => void;
      const admitted = new Promise<void>((resolve, reject) => {
        resolveAdmission = resolve;
        rejectAdmission = reject;
      });
      void admitted.catch(() => {});
      const c: Call = {
        binding: b,
        lease,
        probeId,
        descriptor,
        begun: now(),
        descriptorBytes,
        admitted,
        resolveAdmission,
        rejectAdmission,
        active: false,
        started: false,
        settled: false,
        acknowledged: false,
        cancelled: undefined,
        finalizerDefect: false,
        timer: clock.schedule(
          Math.min(L.admissionMs, Math.max(0, lease.deadline - now() - L.cancelReserveMs)),
          () => interrupt(c, probeFailure("queue-timeout")),
        ),
      };
      calls.set(id(token, owner, probeId), c);
      pump();
      return c;
    });
    yield* Effect.tryPromise({
      try: (signal) => {
        signal.addEventListener("abort", () => interrupt(call, probeFailure("cancelled")), {
          once: true,
        });
        return call.admitted;
      },
      catch: failure,
    });
    if (call.cancelled) return yield* Effect.fail(call.cancelled);
  });
  const service: SearchGitProbeService = {
    bind: (input) =>
      trySync(() => {
        if (
          closed ||
          bindings.size >= 3 * 4 ||
          !isKey(input.key) ||
          input.token.authorityId !== input.bootstrap.authorityId ||
          input.token.slotId !== input.bootstrap.slotId ||
          input.token.slotGeneration !== input.bootstrap.slotGeneration ||
          bindings.has(tokenKey(input.token))
        )
          throw probeFailure("protocol");
        bindings.set(tokenKey(input.token), {
          ...input,
          bootstrap: Object.freeze({ ...input.bootstrap }),
          token: Object.freeze({ ...input.token }),
          key: Object.freeze({ ...input.key }),
          state: "opening",
          lastRequestId: 0,
          lastEpoch: 0,
          leases: new Map(),
        });
      }),
    opened: (token) =>
      trySync(() => {
        const b = getBinding(token);
        if (b.state !== "opening") throw probeFailure("protocol");
        b.state = "live";
      }),
    foregroundBegin: (input) =>
      trySync(() => {
        const b = getBinding(input.token);
        if (
          input.requestId <= b.lastRequestId ||
          !Number.isSafeInteger(input.requestId) ||
          !Number.isFinite(input.deadlineMonotonicMs) ||
          input.deadlineMonotonicMs <= now() ||
          [...b.leases.values()].some((l) => l.owner.kind === "foreground") ||
          (b.state === "opening" && input.operation !== "open")
        )
          throw probeFailure("protocol");
        b.lastRequestId = input.requestId;
        makeLease(
          b,
          { kind: "foreground", requestId: input.requestId },
          input.operation,
          input.input,
          input.deadlineMonotonicMs,
        );
      }),
    foregroundEnd: (token, requestId) =>
      trySync(() => endLease(getBinding(token), ownerKey({ kind: "foreground", requestId }))),
    scanBegin: (token, epoch, birthRequestId) =>
      trySync(() => {
        const b = getBinding(token),
          birth = b.leases.get(ownerKey({ kind: "foreground", requestId: birthRequestId }));
        if (
          b.state !== "live" ||
          b.key.variant !== "paths" ||
          !birth?.live ||
          !["list", "search"].includes(birth.operation) ||
          !Number.isSafeInteger(epoch) ||
          epoch <= b.lastEpoch ||
          [...b.leases.values()].some((l) => l.owner.kind === "scan") ||
          [...calls.values()].some((c) => c.binding === b && c.lease.owner.kind === "scan")
        )
          throw probeFailure("protocol");
        b.lastEpoch = epoch;
        makeLease(b, { kind: "scan", epoch }, birth.operation, null, now() + L.residentMs);
      }),
    scanEnd: (token, epoch) =>
      trySync(() => {
        const b = getBinding(token),
          key = ownerKey({ kind: "scan", epoch });
        const lease = b.leases.get(key);
        if (!lease?.live || lease.deadline - L.cancelReserveMs <= now())
          throw probeFailure("deadline");
        endLease(b, key);
      }),
    prepare,
    run: Effect.fn("SearchGitProbe.run")(function* (
      token: FinderToken,
      owner: ProbeOwner,
      probeId: number,
      input: SearchGitProbeInput,
    ) {
      yield* trySync(() => validateInput(input));
      const key = id(token, owner, probeId);
      let call = calls.get(key);
      if (!call) {
        const descriptor: BeginInput =
          input.kind === "ignoredCandidates"
            ? {
                kind: input.kind,
                repository: input.repository,
                pathCount: input.paths.length,
                stdinBytes: input.paths.reduce((n, p) => n + Buffer.byteLength(p) + 1, 0),
                uploadBytes: Buffer.byteLength(encodePaths(input.paths)),
              }
            : input;
        yield* prepare(token, owner, probeId, descriptor);
        call = calls.get(key);
      }
      if (!call || !call.active || call.started || call.cancelled)
        return yield* Effect.fail(call?.cancelled ?? probeFailure("protocol"));
      const c = call;
      yield* trySync(() => {
        const descriptor = c.descriptor;
        if (
          input.kind !== descriptor.kind ||
          (input.kind === "ignoredCandidates" &&
            (descriptor.kind !== "ignoredCandidates" ||
              input.repository !== descriptor.repository ||
              input.paths.length !== descriptor.pathCount ||
              Buffer.byteLength(encodePaths(input.paths)) !== descriptor.uploadBytes ||
              input.paths.reduce((n, p) => n + Buffer.byteLength(p) + 1, 0) !==
                descriptor.stdinBytes)) ||
          (input.kind !== "ignoredCandidates" &&
            JSON.stringify(input) !== JSON.stringify(descriptor))
        )
          throw probeFailure("protocol");
      });
      c.started = true;
      const work = Effect.gen(function* () {
        const root = yield* fs.realPath(c.binding.key.cwd);
        const relative =
          input.kind === "ignoredCandidates" || input.kind === "repositoryPresence"
            ? input.repository
            : "";
        const cwd = path.resolve(root, relative);
        const actual = yield* fs.realPath(cwd);
        if (actual !== cwd || (actual !== root && !actual.startsWith(`${root}${path.sep}`)))
          return yield* Effect.fail(probeFailure("protocol"));
        const command = yield* trySync(() =>
          probeCommand(input, actual, c.lease.deadline - now() - L.cancelReserveMs),
        );
        const result = yield* vcs.run({
          ...command,
          onSpawn: () => {
            // VCS admission (including uninterruptible spawn acquisition) cannot reset the owner deadline.
            c.timer();
            c.timer = clock.schedule(
              Math.max(0, c.lease.deadline - now() - L.cancelReserveMs),
              () => interrupt(c, probeFailure("deadline")),
            );
            if (c.cancelled) interruptFiber(c);
          },
        });
        const stdout = Buffer.byteLength(result.stdout),
          stderr = Buffer.byteLength(result.stderr);
        if (
          stdout > command.maxOutputBytes! ||
          stderr > command.maxOutputBytes! ||
          (c.lease.stdout += stdout) > L.ownerOutputBytes ||
          (c.lease.stderr += stderr) > L.ownerOutputBytes
        )
          return yield* Effect.fail(probeFailure("oversize"));
        if (!Number.isSafeInteger(result.exitCode) || result.exitCode < 0)
          return yield* Effect.fail(probeFailure("process-failed"));
        if (c.cancelled || !c.lease.live)
          return yield* Effect.fail(c.cancelled ?? probeFailure("cancelled"));
        return {
          ...result,
          stdoutInvalidUtf8: result.stdoutInvalidUtf8 ?? false,
          stderrInvalidUtf8: result.stderrInvalidUtf8 ?? false,
        } satisfies ProbeOutput;
      }).pipe(Effect.mapError(failure));
      c.fiber = yield* Effect.scoped(work.pipe(Effect.result)).pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            c.settled = true;
            if (Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause))
              c.finalizerDefect = true;
            if (c.cleanupTimer) {
              c.cleanupTimer();
              delete c.cleanupTimer;
            }
            release(c);
          }),
        ),
        Effect.flatMap((result) =>
          result._tag === "Failure" ? Effect.fail(result.failure) : Effect.succeed(result.success),
        ),
        Effect.forkIn(scope),
      );
      // Cancellation may arrive between marking the work started and capturing
      // its fork. Deliver that original cancellation to this exact fiber too.
      if (c.cancelled) interruptFiber(c);
      const exit = yield* Fiber.await(c.fiber).pipe(
        Effect.onInterrupt(() => Effect.sync(() => interrupt(c, probeFailure("cancelled")))),
      );
      if (Exit.isFailure(exit)) {
        const error = Cause.findErrorOption(exit.cause);
        return yield* Effect.fail(
          c.cancelled ??
            (Option.isSome(error) ? error.value : probeFailure("process-failed", exit.cause)),
        );
      }
      if (c.cancelled || !c.lease.live)
        return yield* Effect.fail(c.cancelled ?? probeFailure("cancelled"));
      return exit.value;
    }),
    cancel: (token, owner, probeId) =>
      trySync(() => {
        const c = calls.get(id(token, owner, probeId));
        if (!c) return;
        interrupt(c, probeFailure("cancelled"));
      }),
    acknowledge: (token, owner, probeId) =>
      trySync(() => {
        const c = calls.get(id(token, owner, probeId));
        if (!c) return; // Transport independently validates the exact terminal and duplicates.
        if (c.acknowledged) throw probeFailure("protocol");
        c.acknowledged = true;
        release(c);
      }),
    checkDelivery: (token, owner, probeId) =>
      trySync(() => {
        const c = calls.get(id(token, owner, probeId));
        if (!c || c.acknowledged || c.binding.state === "revoked")
          throw probeFailure("binding-closed");
        if (c.cancelled) throw c.cancelled;
        if (!c.lease.live || c.lease.deadline - L.cancelReserveMs <= now())
          throw probeFailure("deadline");
      }),
    revoke: (token) =>
      trySync(() => {
        const b = bindings.get(tokenKey(token));
        if (b) revokeBinding(b);
      }),
    revokeSlot: (boot) =>
      trySync(() => {
        for (const b of bindings.values())
          if (slotKey(b.bootstrap) === slotKey(boot)) revokeBinding(b);
        lastProbe.delete(slotKey(boot));
      }),
    shutdown: Effect.fn("SearchGitProbe.shutdown")(function* () {
      closed = true;
      for (const b of bindings.values()) revokeBinding(b);
      const joined = Effect.forEach(
        [...calls.values()],
        (c) => (c.fiber ? Fiber.await(c.fiber) : Effect.void),
        { concurrency: "unbounded" },
      );
      const result = yield* joined.pipe(Effect.timeoutOption(L.cleanupMs));
      if (result._tag === "None" || cleanupUnknown())
        return yield* Effect.fail(probeFailure("cleanup-unknown"));
    }),
  };
  yield* Effect.addFinalizer(() => service.shutdown().pipe(Effect.orDie));
  return SearchGitProbe.of(service);
});
export const layer = Layer.effect(SearchGitProbe, make);
