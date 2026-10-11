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
const DEADLINE_MS = 10_000;

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

function runLeaseProbe(
  perl: string,
  descriptors: readonly number[],
  deadlineMs: number,
): Promise<LinuxLeaseObservation> {
  return new Promise((resolve) => {
    let settled = false;
    const output = { stdout: "", stderr: "" };
    const child = NodeChildProcess.spawn(
      perl,
      ["-e", LEASE_PROGRAM, "--", String(descriptors.length)],
      { cwd: "/", env: {}, stdio: ["ignore", "pipe", "pipe", ...descriptors] },
    );
    // Killing the captured probe drops its references; the caller's close then
    // releases any lease it still held.
    const deadline = setTimeout(() => finish({ status: "unavailable" }), deadlineMs);
    function finish(observation: LinuxLeaseObservation) {
      if (settled) return;
      settled = true;
      clearTimeout(deadline);
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      resolve(observation);
    }
    for (const stream of ["stdout", "stderr"] as const) {
      const source = child[stream];
      if (source === null) return finish({ status: "unavailable" });
      source.on("data", (data: Buffer) => {
        output[stream] += data.toString("utf8");
        if (Buffer.byteLength(output[stream]) > OUTPUT_LIMIT) finish({ status: "proof-limit" });
      });
    }
    child.once("error", () => finish({ status: "unavailable" }));
    child.once("close", (code, signal) => {
      const busy = parseLinuxLeaseResult({ code, signal, ...output }, descriptors.length);
      finish(busy === undefined ? { status: "unavailable" } : { status: "complete", busy });
    });
  });
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
