// @effect-diagnostics nodeBuiltinImport:off -- Bounded same-user native probes; argv is transient and never returned.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeChildProcess from "node:child_process";
import * as DateTime from "effect/DateTime";
import {
  infrastructureBasis,
  INFRASTRUCTURE_POLICY,
  type InfrastructureResult,
  type LinuxProcessAnchor,
  type InfrastructureReason,
} from "./LinuxInfrastructureEvidence.ts";
import { inside, nativePlatform, readBounded } from "./Filesystem.ts";

export const CONSUMER_LIMITS = [
  "other-user/root processes",
  "remote consumers",
  "unmanaged writers and tools without claims or reservations",
  "general open-file handles are not scanned",
  "lifecycle bindings to other Git directories are outside the participant guard",
] as const;
export interface ProcessPolicy {
  policy: typeof INFRASTRUCTURE_POLICY;
  provenance: "local-system-manager-and-proc-metadata";
  assurance: "accepted-exclusion-not-non-use";
  rawStatus: Probe["status"];
  effectiveStatus: Probe["status"];
  deniedInstances: number;
  verifiedExclusions: number;
  unclassifiedInstances: number;
  nonExemptErrors: number;
  classes: { "system-user-manager-main": number };
  reasons: Partial<Record<InfrastructureReason, number>>;
  basisDigest: string;
}
// Only this observation can confer effective coverage. Decoded/copied summaries remain diagnostics.
const freshPolicies = new WeakMap<Probe, ProcessPolicy>();
export interface Probe {
  observedAt: number;
  status: "complete" | "partial" | "unavailable";
  paths: readonly string[];
  limits: readonly string[];
  processPolicy?: ProcessPolicy;
}
export interface ConsumerEvidence {
  native: Probe;
  services: Probe;
  registry: Probe;
  workloads: Probe;
  obligations: Probe;
  process: Probe;
}
export function consumerDecision(
  target: string,
  evidence: ConsumerEvidence,
  now = DateTime.toEpochMillis(DateTime.nowUnsafe()),
) {
  const holds: string[] = [],
    unknown: string[] = [],
    coverage: Record<
      string,
      {
        status: string;
        ageMs: number;
        limits: readonly string[];
        reason: string | null;
        processPolicy?: ProcessPolicy;
      }
    > = {};
  for (const [name, probe] of Object.entries(evidence)) {
    const ageMs = now - probe.observedAt;
    const policy = name === "process" ? freshPolicies.get(probe) : undefined;
    const effectiveStatus =
      policy && policy === probe.processPolicy ? policy.effectiveStatus : probe.status;
    coverage[name] = {
      status: probe.status,
      ageMs,
      limits: probe.limits,
      ...(probe.processPolicy ? { processPolicy: probe.processPolicy } : {}),
      reason:
        !Number.isFinite(ageMs) || ageMs < 0 || ageMs > 120000
          ? "stale_or_invalid_observation"
          : effectiveStatus !== "complete"
            ? "incomplete_" + name + "_coverage"
            : null,
    };
    if (probe.paths.some((p: string) => inside(p, target)))
      holds.push(name === "obligations" ? "obligation" : "active_consumer");
    if (effectiveStatus !== "complete" || !Number.isFinite(ageMs) || ageMs < 0 || ageMs > 120000)
      unknown.push("consumer_evidence_unavailable");
  }
  return {
    holds: [...new Set(holds)],
    unknown: [...new Set(unknown)],
    coverage,
    limits: CONSUMER_LIMITS,
  };
}
function command(file: string, args: string[], timeout = 1500): Promise<string> {
  return new Promise((resolve, reject) =>
    NodeChildProcess.execFile(
      file,
      args,
      { timeout, maxBuffer: 2 * 1024 * 1024, encoding: "utf8" },
      (error, stdout) => (error ? reject(error) : resolve(stdout)),
    ),
  );
}
function argvUses(bytes: string, target: string) {
  return bytes
    .split(/[\0\s"']+/)
    .some(
      (arg) =>
        arg === target ||
        arg.startsWith(target + NodePath.sep) ||
        arg.includes("=" + target + NodePath.sep) ||
        arg.endsWith("=" + target),
    );
}
async function procText(path: string, limit: number) {
  const file = await NodeFSP.open(path, "r");
  try {
    const bytes = Buffer.alloc(limit + 1);
    let offset = 0;
    while (offset < bytes.length) {
      const read = await file.read(bytes, offset, bytes.length - offset, null);
      if (!read.bytesRead) break;
      offset += read.bytesRead;
    }
    if (offset > limit) throw new Error("bound_exceeded");
    return bytes.subarray(0, offset).toString();
  } finally {
    await file.close();
  }
}
export interface ProcessProbeIO {
  now?: () => number;
  capture?: (pid: string) => Promise<LinuxProcessAnchor>;
  verifyInfrastructure?: (
    anchor: LinuxProcessAnchor,
    pid: string,
    uid: number,
    remainingMs: number,
  ) => Promise<InfrastructureResult>;
  command: (file: string, args: string[]) => Promise<string>;
  list: (path: string) => Promise<string[]>;
  uid: () => number | undefined;
  statUid: (path: string) => Promise<number>;
  readlink: (path: string) => Promise<string>;
  read: (path: string, limit: number) => Promise<string>;
}
export const nativeProcessIO: ProcessProbeIO = {
  command,
  list: (path) => NodeFSP.readdir(path),
  uid: () => process.getuid?.(),
  statUid: async (path) => (await NodeFSP.stat(path)).uid,
  readlink: (path) => NodeFSP.readlink(path),
  read: procText,
};
type ProcessStage =
  | "scan"
  | "linux_enumerate"
  | "linux_owner"
  | "linux_cwd"
  | "linux_exe"
  | "linux_cmdline"
  | "linux_maps"
  | "darwin_enumerate"
  | "darwin_lsof"
  | "darwin_argv";
type ProcessCategory =
  | "permission"
  | "disappeared"
  | "data_bound"
  | "timeout"
  | "unavailable"
  | "time_budget"
  | "pid_bound";
function processErrorCategory(cause: unknown): ProcessCategory {
  const error = cause as
    | (NodeJS.ErrnoException & { killed?: boolean; signal?: string })
    | null
    | undefined;
  if (error?.code === "EACCES" || error?.code === "EPERM") return "permission";
  if (error?.code === "ENOENT" || error?.code === "ESRCH") return "disappeared";
  if (error?.code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER" || error?.message === "bound_exceeded")
    return "data_bound";
  if (error?.code === "ETIMEDOUT" || (error?.killed === true && error.signal === "SIGTERM"))
    return "timeout";
  return "unavailable";
}
export async function processProbe(
  target: string,
  platform: NodeJS.Platform = nativePlatform,
  io: ProcessProbeIO = nativeProcessIO,
): Promise<Probe> {
  const now = io.now ?? (() => performance.now());
  const started = now();
  const observedAt = DateTime.toEpochMillis(DateTime.nowUnsafe()),
    paths = new Set<string>();
  let complete = true;
  let listed = 0,
    attempted = 0,
    sameUser = 0,
    otherUser = 0;
  let stage: ProcessStage = "scan";
  const observations = new Map<string, number>();
  const record = (at: ProcessStage, category: ProcessCategory) => {
    const key = `stage=${at},category=${category}`;
    observations.set(key, (observations.get(key) ?? 0) + 1);
  };
  let deniedInstances = 0,
    verifiedExclusions = 0,
    nonExemptErrors = 0;
  const basis: string[] = [];
  const policyReasons: Partial<Record<InfrastructureReason, number>> = {};
  const finish = (status: Probe["status"]): Probe => {
    const probe: Probe = { observedAt, status, paths: [...paths], limits: limits() };
    if (platform === "linux" && io.verifyInfrastructure) {
      const effectiveStatus =
        status === "partial" &&
        deniedInstances > 0 &&
        deniedInstances === verifiedExclusions &&
        nonExemptErrors === 0
          ? "complete"
          : status;
      const summary: ProcessPolicy = Object.freeze({
        policy: INFRASTRUCTURE_POLICY,
        provenance: "local-system-manager-and-proc-metadata",
        assurance: "accepted-exclusion-not-non-use",
        rawStatus: status,
        effectiveStatus,
        deniedInstances,
        verifiedExclusions,
        unclassifiedInstances: deniedInstances - verifiedExclusions,
        nonExemptErrors,
        classes: Object.freeze({ "system-user-manager-main": verifiedExclusions }),
        reasons: Object.freeze(policyReasons),
        basisDigest: infrastructureBasis(basis),
      });
      probe.processPolicy = summary;
      freshPolicies.set(probe, summary);
    }
    return probe;
  };
  const limits = () => [
    ...CONSUMER_LIMITS,
    `process_probe_coverage:listed=${listed},attempted=${attempted},same_user=${sameUser},other_user=${otherUser}`,
    ...[...observations]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, count]) => `process_probe_observation:${key},count=${count}`),
  ];
  try {
    if (platform === "linux") {
      stage = "linux_enumerate";
      const pids = (await io.list("/proc")).filter((p) => /^\d+$/.test(p));
      listed = pids.length;
      if (pids.length > 4096) {
        record("scan", "pid_bound");
        nonExemptErrors++;
        return finish("unavailable");
      }
      for (const pid of pids) {
        if (now() - started > 5000) {
          record("scan", "time_budget");
          nonExemptErrors++;
          complete = false;
          break;
        }
        attempted++;
        let anchor: LinuxProcessAnchor | undefined;
        let denied = false;
        const gap = (at: ProcessStage, cause: unknown, link = false) => {
          const category = processErrorCategory(cause);
          record(at, category);
          if (category === "permission") denied = true;
          if (
            (cause as NodeJS.ErrnoException).code === "ENOENT" ||
            (!link && (cause as NodeJS.ErrnoException).code === "ESRCH")
          )
            return;
          complete = false;
          if (category !== "permission") nonExemptErrors++;
        };
        try {
          stage = "linux_owner";
          if ((await io.statUid("/proc/" + pid)) !== io.uid()) {
            otherUser++;
            continue;
          }
          sameUser++;
          // The directory remains open across normal same-user surface reads and the helper bracket.
          try {
            anchor = await io.capture?.(pid);
          } catch {
            /* No anchor means no exception authority. */
          }
          const base = anchor?.base ?? "/proc/" + pid;
          for (const name of ["cwd", "exe"] as const) {
            stage = name === "cwd" ? "linux_cwd" : "linux_exe";
            try {
              const p = await io.readlink(base + "/" + name);
              if (inside(p, target)) paths.add(p);
            } catch (cause) {
              gap(stage, cause, true);
            }
          }
          for (const name of ["cmdline", "maps"] as const) {
            stage = name === "cmdline" ? "linux_cmdline" : "linux_maps";
            try {
              const cap = name === "cmdline" ? 1024 * 1024 : 2 * 1024 * 1024;
              const text = await io.read(base + "/" + name, cap);
              if (text.length > cap) throw Error("bound_exceeded");
              if (name === "cmdline") {
                if (argvUses(text, target)) paths.add(target);
              } else
                for (const line of text.split("\n"))
                  if (/^[\da-f]+-[\da-f]+\s+\S*x\S*\s/.test(line)) {
                    const p = line.match(/\s(\/.*)$/)?.[1];
                    if (p && inside(p, target)) paths.add(p);
                  }
            } catch (cause) {
              gap(stage, cause);
            }
          }
        } catch (cause) {
          gap(stage, cause);
        } finally {
          try {
            if (denied) {
              deniedInstances++;
              let result: InfrastructureResult = { kind: "unclassified", reason: "unavailable" };
              const uid = io.uid();
              if (deniedInstances > 16) result = { kind: "unclassified", reason: "bounds" };
              else if (
                anchor &&
                uid !== undefined &&
                io.verifyInfrastructure &&
                now() - started < 5000
              ) {
                try {
                  result = await io.verifyInfrastructure(
                    anchor,
                    pid,
                    uid,
                    5000 - (now() - started),
                  );
                } catch {
                  /* A failed verifier cannot suppress a permission hold. */
                }
              }
              if (result.kind === "verified" && /^[a-f0-9]{64}$/.test(result.basisDigest)) {
                verifiedExclusions++;
                basis.push(result.basisDigest);
              } else {
                const reason = result.kind === "unclassified" ? result.reason : "unavailable";
                policyReasons[reason] = (policyReasons[reason] ?? 0) + 1;
              }
            }
          } finally {
            if (anchor) {
              try {
                await anchor.close();
              } catch {
                nonExemptErrors++;
                complete = false;
              }
            }
          }
        }
      }
    } else if (platform === "darwin") {
      stage = "darwin_enumerate";
      const rows = (await io.command("/bin/ps", ["-axo", "pid=,uid="])).trim().split("\n");
      const pids = rows
        .map((r) => r.trim().split(/\s+/))
        .filter((r) => Number(r[1]) === io.uid())
        .map((r) => r[0]!);
      listed = pids.length;
      if (pids.length > 4096) {
        record("scan", "pid_bound");
        nonExemptErrors++;
        return finish("unavailable");
      }
      for (let i = 0; i < pids.length; i += 64) {
        if (now() - started > 5000) {
          record("scan", "time_budget");
          nonExemptErrors++;
          complete = false;
          break;
        }
        const selected = pids.slice(i, i + 64);
        attempted += selected.length;
        sameUser += selected.length;
        const batch = selected.join(",");
        stage = "darwin_lsof";
        try {
          const output = await io.command("/usr/sbin/lsof", [
            "-a",
            "-p",
            batch,
            "-d",
            "cwd,txt",
            "-Fn",
          ]);
          for (const line of output.split("\n"))
            if (line.startsWith("n") && inside(line.slice(1), target)) paths.add(line.slice(1));
        } catch (cause) {
          record(stage, processErrorCategory(cause));
          complete = false;
        }
        stage = "darwin_argv";
        try {
          if (argvUses(await io.command("/bin/ps", ["-ww", "-p", batch, "-o", "command="]), target))
            paths.add(target);
        } catch (cause) {
          record(stage, processErrorCategory(cause));
          complete = false;
        }
      }
    } else
      return {
        observedAt,
        status: "unavailable",
        paths: [],
        limits: [...CONSUMER_LIMITS, "unsupported platform"],
      };
    if (
      platform === "linux" &&
      now() - started > 5000 &&
      !observations.has("stage=scan,category=time_budget")
    ) {
      complete = false;
      nonExemptErrors++;
      record("scan", "time_budget");
    }
    return finish(complete ? "complete" : "partial");
  } catch (cause) {
    record(stage, processErrorCategory(cause));
    nonExemptErrors++;
    return finish("unavailable");
  }
}
export async function serviceProbe(target: string, files: readonly string[]): Promise<Probe> {
  const observedAt = DateTime.toEpochMillis(DateTime.nowUnsafe());
  const paths: string[] = [];
  try {
    if (files.length > 2000) throw new Error("bound");
    for (const file of files)
      if (argvUses((await readBounded(file, 1024 * 1024)).toString(), target)) paths.push(target);
    // Exact supplied files are only partial coverage of loaded user/system services.
    return {
      observedAt,
      status: "partial",
      paths,
      limits: ["supplied service definitions only; loaded units require an owning adapter"],
    };
  } catch {
    return {
      observedAt,
      status: "unavailable",
      paths,
      limits: ["service definitions unreadable or bounded"],
    };
  }
}
