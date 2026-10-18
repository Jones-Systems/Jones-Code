# Process placement boundary

The source integration decorates the shared AgentScope service and Effect
spawner. With binding v2, systemd creates the final scope and the helper verifies
membership from inside it. Terminals use their existing manager/AgentScope path;
the native PTY adapter remains unchanged. Unconfigured launches retain the
existing services. The server checks its control membership before database
ownership; the cold launcher must establish that binding beforehand.

This is not installed-host acceptance. Kernel-backed placement, full launch-route
coverage, memory safeguards and short-lived command responsiveness still require
qualification. Keep the PR held until the source composition and lifecycle
checks are complete; do not enable a binding merely because it parses.

Placement belongs to the environment that owns execution, regardless of the
client's machine. The versioned [binding](../../apps/server/src/jones/processPlacement/processPlacement.ts)
selects separate control and workload siblings in an already delegated Linux
cgroup v2 hierarchy. A policy owner must establish that hierarchy and its CPU
limits. The v1 helper moves itself; v2 workload mode only verifies membership below
the final systemd scope. It then replaces itself with the payload. It neither
creates cgroups nor migrates running sessions.

The helper's canonical path, hash, device and inode must identify the executed
file, not merely the file inspected before spawn. The native helper repeats
these checks against `/proc/self/exe` to reject a replacement between validation
and execution. A required placement failure must never retry the payload
without placement. The unset configuration preserves the original command.
Successful exec preserves the PID and inherited descriptors; the integrating
adapter must preserve its existing cancellation and process-group ownership.

An external cold bootstrap must validate its own immutable artifacts, establish
and read back the delegated hierarchy, remove the bootstrap variable and emit
a ready binding before replacing itself with the launcher. The
[bootstrap contract](../../apps/server/src/jones/processPlacement/processPlacementBootstrap.ts)
validates inputs; it does not install a service or implement that bootstrap.
Optional negative nice with reset-on-fork applies only to an explicitly selected
server generation. It requires separately supplied host permissions and does
not promise priority for every runtime thread.

The Effect spawner wrapper scopes unmarked workload commands and permits explicitly
marked control commands. AgentScope management uses a separate raw runner to
avoid recursion; telemetry is explicitly control. VCS and other application
runners remain workload. It is not a complete process interception mechanism:
SDK-owned children and launcher replacement/rollback still need qualification
at their owning boundaries. Provider tools normally inherit their
parent's cgroup; independently launched agent roots need their own wrapper.

The explicit `build:process-placement` server script compiles a host Linux helper
and records source and binary hashes. It is separate from bundle and executable
packaging: those pipelines clean their output and can target another platform
or architecture. Release integration must select the target helper, retain its
manifest and bind the installed artifact before enabling placement. A compiled
helper or passing rejection test does not prove delegation, positive placement,
live responsiveness, or installed release provenance.

## Systemd descendant binding (v2 source contract)

Version 2 adds `workloadMode: "systemd-descendant"`. Control and workload remain
separate sibling cgroups with pinned identities. Workload validation requires a
readable `cpu.max`, but no writable root `cgroup.procs`. The native helper's
`--verify-descendant` mode accepts workload only, verifies strict descendant
membership through directory descriptors without following symlinks, resets
workload scheduling and execs the payload. It never writes `cgroup.procs` in this
mode. Version 1 retains its exact move behavior.

This mode must run inside the final AgentScope systemd scope. The superseded
outer PTY hook has been removed. Until AgentScope, ACP session termination and
the shared spawner are qualified together, v2 is source
preparation, not an enabled production path.
Keep memory limits, OOM reporting, restart cleanup and session descendant
termination load-bearing. Short-lived command overhead remains unqualified.

## Captured scope termination

The shared AgentScope command may carry a stop effect bound to its exact unit,
independent of the mutable thread-to-scope map. It performs one bounded systemd
stop and checks that the unit is gone or inactive and empty. An unavailable or
still-active postcondition fails as unknown; timeout is not retirement proof.
The result is cached so repeated cleanup does not blindly repeat an unknown
operation. Existing OOM reporting, memory policy and restart cleanup remain in
the authoritative AgentScope implementation.

For placed ACP launches, a context reference disables the direct non-systemd
cgroup lease. Explicit controller options still take precedence. Descendant
cleanup uses the captured scope stop rather than losing hard cleanup to the
process ledger fallback; the existing conservative containment label remains.
The spawner registers the captured stop with its owning Effect scope as well.
No second lifetime registry is introduced.

This stop effect is not PR91 operation-bound retirement evidence: it does not
publish not-invoked/active/currentness/restart receipts for an old workload.
Those consumer states remain unknown until an exact producer contract is bound.

## Synthetic unit qualification

Ordinary ACP unit tests inject a controller that creates no kernel lease. The
shell-wrapper tests use task-owned regular files for `cgroup.procs`; their only
membership read is the current process's `/proc/self/cgroup`. The detached-fork
test injects a lease whose kill callback targets only its captured child PID,
while retaining the unrelated sentinel and nested-cleanup assertions. These
checks exercise argv, environment handling and termination orchestration, not
kernel membership or cgroup-wide descendant coverage. They never discover or
sweep inherited cgroup siblings. Real cgroup qualification requires a separate,
explicitly bounded fixture protocol; ordinary unit commands do not opt into it.

## Retained command and operation permits

The Codex app-server launch records its exact command object's originating
AgentScope and captured stop effect. The placing spawner claims that annotation
once and registers the original stop, without creating a second, empty outer
scope. Owner substitution and repeated claims fail closed. Copied argv or JSON
has no annotation. This is command-local provenance, not a lifetime registry.

`jones/runtime/RuntimeOperationPermit.ts` defines a source-only operation port.
Production providers and the ordinary-checkout consumer are not wired to it.
The existing executor must supply its captured binding, issuer, generation,
evidence revision/reference, exact participant writer coverage, serialized
current-runtime invocation, and retained old-scope observation. A successful
stop alone cannot supply those missing contracts. No database migration or
restart history reconciliation is installed by this port.

The permit captures one native invocation and linearizes its claim against
revocation. The consumer's durable-start effect runs before invocation. The
runtime owner must hold its existing lifecycle guard while rechecking the exact
binding and performing invocation; an earlier currentness check is insufficient.
Coverage is frozen at issuance and covers only the declared participant writers.
The consumer still accounts for every other association and derived effect.

| Situation | Source-port result |
| --- | --- |
| Revoke before any durable-start attempt | Participant `not_invoked` |
| Start attempted, committed, or response lost; no confirmed invocation | `unknown` |
| Confirmed invocation, complete declared coverage, current owner | Participant `active`; not operation completion |
| Partial coverage or escaped/unknown descendants | `unknown` |
| Interrupted or uncertain invocation | `unknown` until independently complete retirement evidence |
| Complete old-scope quiescence, no pending/future permit invocation | Exact participant `quiescent`, including after invocation-permit expiry |
| Scope observation overlaps a permit transition | `unknown`; stale emptiness cannot retire later work |
| Warm reuse, unsupported provider, or restarted issuer | Typed `unavailable` |

There is no post-restart `not_invoked` inference from a dead issuer, lock,
timeout, or new placement. Warm-provider progression needs a separate qualified
operation-specific coverage contract; this port never kills a warm runtime or
silently holds all ordinary operations until it exits. These synthetic contracts
are neither runtime adoption nor workload/performance qualification.
