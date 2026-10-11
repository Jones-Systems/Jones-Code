// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
// Linux exact-inode writer proof through kernel read leases. The kernel grants
// F_RDLCK only while no open file description on the host has the inode open
// for writing; writable shared mappings keep their write-open description.
// That covers root and other-user actors plus non-dumpable same-user processes
// (sshd sessions, the systemd user manager, ssh-agent) whose /proc descriptors
// the service user cannot read. A lease cannot exclude an allowed writer, so
// it proves only states where no process may own writable state.
//
// Node exposes no fcntl, so a fixed program runs under the distribution perl
// against descriptors this process opened and identity-checked. It receives
// only a descriptor count: no path, environment or caller-selected command.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";

interface LeasedStateFile {
  readonly path: string;
  readonly device: bigint;
  readonly inode: bigint;
}

const MAXIMUM_FILES = 8;
const OUTPUT_LIMIT = 4 * 1024;
// Admission runs this probe inside the server's 30 second launcher exchange.
const DEADLINE_MS = 5_000;
const TERMINATE_GRACE_MS = 2_000;
const REAP_GRACE_MS = 2_000;

export interface CapturedProcessResult {
  /** "unreaped": the captured child outlived SIGKILL plus its reap grace. */
  readonly outcome: "exited" | "deadline" | "output-limit" | "spawn-failed" | "unreaped";
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * Runs one fixed command with an empty environment and bounded output. On its
 * deadline or output limit only the captured child is signalled: SIGTERM, then
 * SIGKILL after a grace period. The result settles once that child is reaped,
 * or reports "unreaped" after a final bound; timers and output listeners end with it.
 */
export function runCapturedProcess(input: {
  readonly command: string;
  readonly args: readonly string[];
  readonly descriptors?: readonly number[];
  readonly deadlineMs: number;
  readonly outputLimit: number;
  /** sudo relays SIGTERM only from a sender outside the command's process group. */
  readonly newProcessGroup?: boolean;
}): Promise<CapturedProcessResult> {
  return new Promise((resolve) => {
    const output = { stdout: "", stderr: "" };
    let stopped: "deadline" | "output-limit" | undefined;
    let settled = false;
    const timers: NodeJS.Timeout[] = [];
    const child = NodeChildProcess.spawn(input.command, [...input.args], {
      cwd: "/",
      env: {},
      detached: input.newProcessGroup === true,
      stdio: ["ignore", "pipe", "pipe", ...(input.descriptors ?? [])],
    });
    const streams = [child.stdout, child.stderr];
    const detachOutput = () => {
      for (const stream of streams) {
        stream?.removeAllListeners("data");
        stream?.destroy();
      }
    };
    const settle = (
      outcome: CapturedProcessResult["outcome"],
      code: number | null = null,
      signal: NodeJS.Signals | null = null,
    ) => {
      if (settled) return;
      settled = true;
      for (const timer of timers) clearTimeout(timer);
      detachOutput();
      resolve({ outcome, code, signal, ...output });
    };
    const stop = (reason: "deadline" | "output-limit") => {
      if (stopped !== undefined || settled) return;
      stopped = reason;
      detachOutput();
      child.kill("SIGTERM");
      timers.push(setTimeout(() => child.kill("SIGKILL"), TERMINATE_GRACE_MS));
      timers.push(setTimeout(() => settle("unreaped"), TERMINATE_GRACE_MS + REAP_GRACE_MS));
    };
    timers.push(setTimeout(() => stop("deadline"), input.deadlineMs));
    for (const [index, name] of (["stdout", "stderr"] as const).entries()) {
      streams[index]?.on("data", (data: Buffer) => {
        output[name] += data.toString("utf8");
        if (Buffer.byteLength(output[name]) > input.outputLimit) stop("output-limit");
      });
    }
    // A failed signal also reports here; only a failed spawn ends the run.
    child.on("error", () => {
      if (child.pid === undefined) settle("spawn-failed");
    });
    child.once("close", (code, signal) => settle(stopped ?? "exited", code, signal));
  });
}

// F_SETLEASE/F_GETLEASE are F_LINUX_SPECIFIC_BASE + 0/1 on every architecture;
// lock types come from Fcntl because their values are architecture-specific.
// Every lease is held before any is read back, so an unbroken F_RDLCK on each
// file proves no write-open description existed once the last was granted.
// SIGIO is ignored because a concurrent write-open delivers it on lease break.
const LEASE_PROGRAM = `use strict;
use warnings;
use Errno qw(EAGAIN);
use Fcntl qw(F_RDLCK F_UNLCK);
$SIG{IO} = "IGNORE";
my ($setlease, $getlease) = (1024, 1025);
my $count = shift @ARGV;
exit 2 unless defined $count && $count =~ /\\A[1-${MAXIMUM_FILES}]\\z/ && !@ARGV;
my (@handles, @leased, @busy);
for my $fd (3 .. 2 + $count) {
  open(my $handle, "<&=", $fd) or exit 3;
  push @handles, $handle;
}
for my $index (0 .. $#handles) {
  if (defined fcntl($handles[$index], $setlease, F_RDLCK)) {
    push @leased, $index;
  } elsif ($! == EAGAIN) {
    push @busy, $index;
  } else {
    exit 4;
  }
}
for my $index (@leased) {
  my $type = fcntl($handles[$index], $getlease, 0);
  exit 4 unless defined $type;
  push @busy, $index if $type != F_RDLCK;
}
for my $index (@leased) {
  exit 4 unless defined fcntl($handles[$index], $setlease, F_UNLCK);
}
print "busy $_\\n" for sort { $a <=> $b } @busy;
print "complete\\n";
exit 0;
`;

export type LinuxLeaseObservation =
  | { readonly status: "complete"; readonly busy: readonly number[] }
  | { readonly status: "unavailable" | "proof-limit" | "identity-changed" };

/** Returns write-open file indexes, or undefined unless the probe completed exactly. */
export function parseLinuxLeaseResult(
  result: {
    readonly code: number | null;
    readonly signal: NodeJS.Signals | null;
    readonly stdout: string;
    readonly stderr: string;
  },
  count: number,
): readonly number[] | undefined {
  if (result.code !== 0 || result.signal !== null || result.stderr !== "") return undefined;
  const lines = result.stdout.split("\n");
  if (lines.pop() !== "" || lines.pop() !== "complete") return undefined;
  const busy: number[] = [];
  for (const line of lines) {
    const index = /^busy (\d)$/.exec(line)?.[1];
    if (index === undefined) return undefined;
    const value = Number(index);
    if (value >= count || value <= (busy[busy.length - 1] ?? -1)) return undefined;
    busy.push(value);
  }
  return busy;
}

async function runLeaseProbe(
  perl: string,
  descriptors: readonly number[],
  deadlineMs: number,
): Promise<LinuxLeaseObservation> {
  // A stopped probe settles only after it is reaped, so the caller's close is
  // the last reference and releases any lease it still held. An unreaped probe
  // is never reported as a proof.
  const result = await runCapturedProcess({
    command: perl,
    args: ["-e", LEASE_PROGRAM, "--", String(descriptors.length)],
    descriptors,
    deadlineMs,
    outputLimit: OUTPUT_LIMIT,
  });
  if (result.outcome === "output-limit") return { status: "proof-limit" };
  if (result.outcome !== "exited") return { status: "unavailable" };
  const busy = parseLinuxLeaseResult(result, descriptors.length);
  return busy === undefined ? { status: "unavailable" } : { status: "complete", busy };
}

export async function observeLinuxStateLeases(
  files: readonly LeasedStateFile[],
  options: { readonly perl?: string; readonly deadlineMs?: number } = {},
): Promise<LinuxLeaseObservation> {
  if (files.length === 0) return { status: "complete", busy: [] };
  if (files.length > MAXIMUM_FILES) return { status: "proof-limit" };
  const { O_RDONLY, O_NOFOLLOW, O_NONBLOCK, O_NOCTTY, O_NOATIME } = NodeFS.constants;
  // O_NONBLOCK refuses instead of waiting on another holder's lease; O_NOATIME
  // keeps the read-only open from touching inode times.
  const flags = O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY | O_NOATIME;
  const handles: NodeFSP.FileHandle[] = [];
  try {
    for (const file of files) {
      let handle: NodeFSP.FileHandle;
      try {
        handle = await NodeFSP.open(file.path, flags);
      } catch (cause) {
        const code = (cause as NodeJS.ErrnoException)?.code;
        if (code === "ENOENT" || code === "ELOOP") return { status: "identity-changed" };
        return { status: "unavailable" };
      }
      handles.push(handle);
      // The lease binds the opened description, so its inode must be the snapshot's.
      const stat = await handle.stat({ bigint: true });
      if (!stat.isFile() || stat.dev !== file.device || stat.ino !== file.inode)
        return { status: "identity-changed" };
    }
    return await runLeaseProbe(
      options.perl ?? "/usr/bin/perl",
      handles.map((handle) => handle.fd),
      options.deadlineMs ?? DEADLINE_MS,
    );
  } catch {
    return { status: "unavailable" };
  } finally {
    await Promise.all(handles.map((handle) => handle.close().catch(() => undefined)));
  }
}
