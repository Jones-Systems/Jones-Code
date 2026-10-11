# Server updates

A [stable launcher](../../apps/server/src/serviceLauncher.ts) owns the runtime
selected by systemd or launchd. It is the only runtime writer of durable service
state. Server children request updates over inherited IPC; they never rewrite
their service definition or select their own replacement. Local service commands
may replace the launcher and state while the service is stopped. Foreground CLI
processes do not self-update.

Jones update checks require a qualified runtime receipt. Download additionally
requires a managed launcher advertising qualified staging and its current version;
having a receipt or a systemd/launchd unit alone is insufficient. Install requires
qualified activation support, a matching environment identity, and reconciled staging.

Exact-version installs keep restarts independent of npm cache eviction or a moving
release tag. Installation and preflight happen in staging before publishing an
immutable runtime. Preflight checks the launcher protocol because a target that
needs new rollback guarantees cannot safely run under an older launcher. Upgrading
that launcher requires a local service update.

## Commit boundary

The launcher durably records the pending update before acknowledging it, then
stops the old child and starts the target as a trial. Service-state writes use
same-directory replacement with file and directory fsync. Invalid state stops
startup rather than guessing which runtime to boot.

The trial must finish migrations, acquire dependencies, bind HTTP, and park every
long-running root at the activation gate before reporting `prepared`. The launcher
then commits the target version durably and replies `committed`. Only then may the
child release its gates, accept commands, and publish ready. Keep fallible startup
acquisitions before this boundary. A listener alone does not prove the runtime is
ready to commit.

A failed or timed-out trial returns to the old version. After commit, the target
is authoritative and the service manager's ordinary restart policy applies.

## Database rollback

After the old child exits, the launcher snapshots SQLite's main file, WAL, and
shared-memory file. This makes trial migrations reversible without down
migrations. The snapshot is made once per update and survives launcher restarts;
replacing it during a retry could capture changes from the failed trial.

Rollback stops the trial before restoring. A durable restore marker makes an
interrupted restore finish before either version boots. Keep the snapshot until
commit, or until both restoration and the terminal rollback state are durable.
Attachments and other files outside SQLite are outside this rollback boundary.

## Client acknowledgement

An accepted update is still pending. Clients correlate the launcher's update ID
with the ready event after reconnecting, then check the outcome and target version.
A reconnect alone cannot distinguish successful replacement from rollback. Older
servers without an update ID retain version-only correlation.

Fleet installs use a caller-supplied UUID and an exact source selection. A launcher
must advertise `updateOperationsProtocol: 1` before accepting that ID. It durably
reserves the ID with the staged handle, source pair, environment, database, and
version pair before acknowledging native acceptance. Repeating the same binding
reads its retained operation; reusing the ID for another binding is rejected.
The launcher archives a terminal receipt before a later operation replaces its
service-state entry. These Jones receipts live under `runtime/jones-update-operations`,
outside the database rollback pair.

An absent reservation and absent matching service state permit submission with the
same ID. A reservation without matching native state or a retained terminal outcome
requires reconciliation; it does not authorize another replacement with any ID. The
launcher inspects at most 4,096 receipt entries and fails closed on malformed,
unreadable, or larger receipt stores. Receipt preparation failures preserve the old
child and any partial reservation. Only an exact operation proven absent can clear
its uncertain in-memory handoff fence. Readback of an operation and observation of the currently installed source are separate checks.

Desktop updates have a separate two-phase handoff because installing the app stops
its bundled backend. Preparation returns a token while the connection is alive;
the client commits that token only after receiving it. Otherwise backend shutdown
could lose the only successful RPC result. The client must then observe the
prepared version after reconnecting. If installation fails, desktop restarts the
stopped backends and replays the failure for the same token.

## Recovering interrupted threads

Restart continuation is an environment-owned preference, off by default. The
[v2 recovery service](../../apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.ts)
requires matching durable run, provider thread, session, and native resume identity.
Queued runs never started, so recovery holds them and continues the run they wait
behind. Ordinary restart recovery leaves those queues held. Completed roots and
roots waiting for approval or user input do not start a restart continuation.
If a completed root lost background work, the cancellation note is retained for its
next ordinary turn.

Recovery retires effects tied to the lost process and records continuation intent
in the durable outbox. That intent survives another restart before provider startup.
Continuation effects wait for activation; a slow provider must not delay the server's
readiness or the launcher's commit boundary. Graceful shutdown captures intent before
closing providers, then reconciles after ingestion has stopped so a late completion
cannot be overwritten by a stale cancellation.

The [continuation handler](../../apps/server/src/orchestration-v2/RestartContinuation.ts)
rechecks the preference, archive state, provider selection, newer user work, a stop
the user requested, and maintenance turns such as `/compact` before dispatching. Stable
command and message IDs prevent duplicate submissions after an outbox retry. Codex
resumes without adding provider prompt text only when resuming the original native
thread without an inline handoff or background-work note. A replacement native
thread receives a prompt, including history when native injection is unsupported.
Other adapters receive the continuation message through their normal turn path.

Delegated tasks (`delegate_task` child threads) are reconciled as their own threads,
never as the parent's background work. The orchestrator settles child results and
completion deliveries in a startup pass after reconciliation, because the terminal-run
listener ignores reconciliation's cancellations. A cancelled child whose restart
continuation is still pending in the outbox is not a result yet; the continuation's run
settles it, or the handler settles it when it declines to continue. Schedulers wait for
activation so they cannot start runs that reconciliation would then cancel.

Qualified planned updates with caller-bound operation support capture eligible queues
and live Work Mode conversations before handoff. Jones migration 106 stores this
eligibility separately from upstream projections. Saved continuation preferences or
the explicit one-install continuation option authorize capture; enrollment does not
change those preferences. Startup verifies the exact native operation outcome and
the actual selected source, environment, home, and database before consuming a
one-shot activation claim. Rollback must prove the restored previous source as well.
A later ordinary restart cannot grant the same captured eligibility again.

A captured queue is released through the existing deterministic `queue.resume`
command only after its root or exact restart-continuation chain completes
successfully. Its messages, ordering, provider selection, and control receipts must
remain unchanged. Preexisting holds, later Stop or hold commands (including no-op
commands), edits, new user work, approvals, failures, and identity variance keep it
held. Existing continuation and queue outbox identities provide retry idempotence.

Work Mode captures only eligible completed roots whose provider conversation was
still live before this update. One cold admission may reopen that same native
conversation at its original due time; it adds no immediate prompt or interval
reset. Current Work Mode and continuation controls are checked again. A failed
native resume or a changed native identity sends no keep-warm prompt and does not
fall back to a fresh conversation. Previously evicted sessions and generic crashes
do not acquire this planned-update permission. Both the capturing runtime and the
runtime performing recovery need this implementation; older installed binaries do
not gain it from source publication alone.
