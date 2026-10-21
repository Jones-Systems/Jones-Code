import type { PhysicalProcessLease } from "@t3tools/provider-core/server/jones/runtimeTypes";
import type { PhysicalProcessIdentity, PhysicalProcessObservation } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

// Only these fields cross the protocol boundary. Gauges and arbitrary provider data are discarded.
const Diagnostics = Schema.Struct({
  process: Schema.Struct({
    id: Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 4294967295 })),
    residentMemoryBytes: Schema.NullOr(
      Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
    ),
    physicalFootprintBytes: Schema.NullOr(
      Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
    ),
  }),
});

const decodeDiagnostics = Schema.decodeUnknownEffect(Diagnostics);

type LocalProcessRead =
  | { readonly status: "observed"; readonly identity: PhysicalProcessIdentity }
  | { readonly status: "absent" | "unknown" };

export const readLocalProcess: (pid: number) => Effect.Effect<LocalProcessRead, never, FileSystem.FileSystem> = Effect.fn("PhysicalProcessEvidence.readLocalProcess")(function* (pid: number) {
    if (!Number.isSafeInteger(pid) || pid <= 0) return { status: "unknown" } as const;
    const fs = yield* FileSystem.FileSystem;
    const bootId = (yield* fs.readFileString("/proc/sys/kernel/random/boot_id")).trim();
    const localNamespace = yield* fs.readLink("/proc/self/ns/pid");
    // Only an absent process stat after validating procfs is absence evidence.
    // Missing executable/namespace metadata and permission errors stay unknown.
    if (!/^[0-9a-f-]{36}$/.test(bootId) || !/^pid:\[\d+\]$/.test(localNamespace))
      return { status: "unknown" } as const;
    const stat = yield* fs.readFileString(`/proc/${pid}/stat`).pipe(
      Effect.catchIf(
        (error) => error.reason._tag === "NotFound",
        () => Effect.succeed(null),
      ),
    );
    if (stat === null) return { status: "absent" } as const;
    const pidNamespace = yield* fs.readLink(`/proc/${pid}/ns/pid`);
    if (
      !/^[0-9a-f-]{36}$/.test(bootId) ||
      !/^pid:\[\d+\]$/.test(pidNamespace) ||
      pidNamespace !== localNamespace
    )
      return { status: "unknown" } as const;
    // comm may contain spaces and parentheses; field 22 follows its final closing parenthesis.
    const end = stat.lastIndexOf(")");
    const startTime = stat.slice(end + 2).split(" ")[19];
    if (
      end < 0 ||
      stat.slice(0, stat.indexOf(" ")) !== String(pid) ||
      !startTime ||
      !/^\d+$/.test(startTime)
    )
      return { status: "unknown" } as const;
    const executable = yield* fs.stat(`/proc/${pid}/exe`);
    if (Option.isNone(executable.ino) || Option.isNone(executable.mtime))
      return { status: "unknown" } as const;
    return {
      status: "observed",
      identity: {
        platform: "linux",
        bootId,
        pidNamespace,
        pid,
        startTime,
        startTimeUnit: "linux-clock-ticks-since-boot",
        executableIdentity: `${executable.dev}:${executable.ino.value}:${executable.size}:${executable.mtime.value.getTime()}`,
        build: "unknown",
      },
    } as const;
  }, Effect.timeoutOption("250 millis"), Effect.map((result): LocalProcessRead =>
      Option.isSome(result) ? result.value : { status: "unknown" },
    ), Effect.catchCause(() => Effect.succeed({ status: "unknown" } as const)));

const samePhysicalProcess = (a: PhysicalProcessIdentity, b: PhysicalProcessIdentity) =>
  a.platform === b.platform &&
  a.bootId === b.bootId &&
  a.pidNamespace === b.pidNamespace &&
  a.pid === b.pid &&
  a.startTime === b.startTime &&
  a.startTimeUnit === b.startTimeUnit &&
  a.executableIdentity === b.executableIdentity &&
  a.build === b.build;

export const makePhysicalProcessLease = (input: {
  readonly runtimeGeneration: string;
  readonly launcher: PhysicalProcessIdentity | null;
  readonly enabled: boolean;
  // PID coincidence cannot prove locality through an unqualified forwarding executable.
  // Only a separately qualified producer boundary may supply this witness.
  readonly localResponderProven?: boolean;
  readonly isCurrent: () => boolean;
  readonly request: () => Effect.Effect<unknown, Error | { readonly code: number }>;
  readonly readProcess: (pid: number) => Effect.Effect<LocalProcessRead>;
}): PhysicalProcessLease => {
  let retired = false;
  const unknown = (
    reason: Extract<PhysicalProcessObservation, { status: "unknown" }>["reason"],
  ): PhysicalProcessObservation => ({
    status: "unknown",
    runtimeGeneration: input.runtimeGeneration,
    reason,
  });
  const current = () => !retired && input.isCurrent();
  const diagnostics = Effect.suspend(input.request).pipe(
    Effect.flatMap((value) =>
      decodeDiagnostics(value).pipe(
        Effect.map((decoded) => ({ status: "supported" as const, pid: decoded.process.id })),
        Effect.catch(() => Effect.succeed(unknown("malformed"))),
      ),
    ),
    Effect.catch((error) => {
      const code =
        typeof error === "object" && error !== null ? Reflect.get(error, "code") : undefined;
      return Effect.succeed(unknown(code === -32601 ? "unsupported" : "transport"));
    }),
  );
  const capture = Effect.gen(function* () {
    if (!current()) return unknown("retired");
    if (!input.enabled) return unknown("capability_disabled");
    const first = yield* diagnostics;
    if (!current()) return unknown("retired");
    if (first.status !== "supported") return first;
    if (input.launcher === null) return unknown("launcher_unavailable");
    if (first.pid !== input.launcher.pid) return unknown("responder_mismatch");
    const before = yield* input.readProcess(first.pid);
    if (before.status !== "observed")
      return unknown(before.status === "absent" ? "process_absent" : "os_unavailable");
    if (!samePhysicalProcess(before.identity, input.launcher)) {
      retired = true;
      return unknown("incarnation_changed");
    }
    // A second correlated reply on this exact client fences stale PID data and producer replacement.
    const second = yield* diagnostics;
    const after = yield* input.readProcess(first.pid);
    if (!current()) return unknown("retired");
    if (second.status !== "supported") return second;
    if (after.status !== "observed")
      return unknown(after.status === "absent" ? "process_absent" : "os_unavailable");
    if (second.pid !== first.pid || !samePhysicalProcess(before.identity, after.identity)) {
      retired = true;
      return unknown("incarnation_changed");
    }
    if (input.localResponderProven !== true)
      return {
        ...unknown("provenance_unavailable"),
        diagnostics: { status: "supported" as const, reportedPid: first.pid },
        localLauncher: after.identity,
      };
    return {
      status: "observed",
      runtimeGeneration: input.runtimeGeneration,
      launcherPid: input.launcher.pid,
      responder: after.identity,
      source: "codex.server/diagnostics+local-os",
    } as const;
  }).pipe(
    Effect.timeoutOption("2 seconds"),
    Effect.map((result) => (Option.isSome(result) ? result.value : unknown("timeout"))),
    Effect.catchCause(() => Effect.succeed(unknown(current() ? "transport" : "retired"))),
  );
  return { runtimeGeneration: input.runtimeGeneration, capture };
};
