# Launcher handoff

A bootstrapped host's service manager keeps one immutable launcher shim as its program.
The launcher and server can change independently. Hosts with an older service definition
keep the explicit planned-refresh notice.

After a qualified application update commits, the new server announces readiness only
after the HTTP listener, parked auxiliary roots, activation, command admission, ready
event and compile-cache flush have completed. A spawn, capability file or prepared
trial alone cannot promote the launcher.

The new optional launcherHandoffProtocol capability adds health and handoff IPC to
service protocol 4. Old peers do not participate. Enrollment receipts, qualified stages,
service state and update operation outcomes keep their existing formats.

The server waits for quiet work through UpdateAdmissionFence, the durable host-work
oracle and terminal metadata. Admission remains closed from the final check through
the launcher's answer. A definitive deferral releases it and resumes held queued work.
A lost answer, cancellation or timeout after sending preserves the fence until exit;
it does not permit retrying a possibly accepted handoff.

At that point the launcher checks native manager membership. Linux requires a known
non-root unified cgroup, no nested groups and exactly the launcher and captured child.
Darwin requires a known job process group and no additional members or escaped
descendants. Only the captured already-exited inventory subprocess is excluded.
The manager must also report that this exact launcher PID uses the stable shim program.
Unavailable, malformed, oversized or timed-out inventory defers. These reads happen
at handoff, not as prerequisites for ordinary server startup.

The launcher's existing lifetime lock serializes pointer changes; handoff never takes
that lock again. It hashes the retained and selected executable artifacts with bounded
reads and stable metadata. Byte-identical executables need no handoff. Otherwise it
rechecks manager membership after hashing, resets the attempt budget, retains an immutable
requested receipt and publishes pending.
Only then does it acknowledge the fenced request, gracefully stop its child and return
reserved exit code 75 to the service manager.

Each shim start consumes one of three pending attempts. A pending launcher whose child
never announces readiness fails within the existing prepared-startup deadline, so a
hung child cannot prevent fallback. The replacement launcher
promotes pending to current only after its capability file is bound to the exact
live child and that child announces actual readiness. It resets attempts and retains
the committed receipt without replacing requested history. Durable prefixes replay
against the same operation UUID, versions and executable hashes.

Three failed starts move pending to retained rejected evidence. The next start runs
current; its healthy child settles the request as rejected. The same target is not
automatically retried. With the existing systemd five-start budget, the initial
launcher, three pending attempts and fallback fit within StartLimitBurst=5/300 seconds.
Launchd's existing five-second throttle spaces those starts.

Pointer and last-handoff evidence are optional, strictly decoded fields of the existing
launcher capability receipt. They distinguish waiting, requested and exhausted
handoff from the manual-refresh notice on hosts without a pointer.

This source does not bootstrap or modify a running host, rewrite its service definition,
or authorize installation, restart, signing or activation. Native acceptance and
supported-platform evidence are separate from synthetic lifecycle tests.
