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

The prepared deadline is long because migrations can scale with the database
and block the trial's main thread, so it cannot report progress. A missed
deadline restores the snapshot, and the retry pays the same cost again. A
crashed trial is caught when it exits; the deadline bounds only a stuck one.
Client update waits must outlast the deadline, or a slow update shows as failed.

## Database rollback

Before stopping the old child, the launcher refuses an update whose snapshot will
not fit on disk, so a full disk does not cause an outage. After the captured old child exits, the launcher holds the same exclusive transaction
on `userdata/server-owner.sqlite` that servers hold for their lifetime. A busy
lock, or a known live legacy owner, refuses recovery before changing state. The
launcher snapshots SQLite's main file, WAL, and shared-memory file, plus the five
startup identity/settings files. It releases ownership before spawning the trial.
This makes trial migrations reversible without down migrations. The snapshot is made once per update and survives launcher restarts;
replacing it during a retry could capture changes from the failed trial.

Rollback awaits the captured trial's exit and acquires state ownership before
restoring. A durable restore marker makes an interrupted restore finish before
either version boots. After durable commit and its IPC acknowledgement, or after
restoration and the terminal rollback state are durable, the launcher removes that
attempt's recovery payload and transient advanced state, keeping the directory and
small receipts used by startup and update status. Cleanup failures preserve the
durable result and keep the remaining files for later reconciliation. An uncertain pointer write, failed
restore or failed terminal write preserves recovery bytes. It leaves older retained
snapshots alone. The shared ownership file is never unlinked.
Attachments and other files outside SQLite and the paired startup files are outside
this rollback boundary. Qualified at-most-once resume reservations remain at
`runtime/db-backup/<update-id>/resume-dispatched.json`. Keeping that directory lets
existing candidates reserve resume after commit; the guard still blocks replay
after recovery payload cleanup. Launcher, qualified-update and startup-gate protocols remain 4, 1 and 1.

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

Restart continuation is an environment-owned preference, on by default. A
`settings.json` that fails to decode turns it off, since the file may hold an
opt-out. The
[v2 recovery service](../../apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.ts)
requires matching durable run, provider thread, session, and native resume identity.
Queued runs never started, so recovery holds them and continues the run they wait
behind. Ordinary restart recovery leaves those queues held. A settled run qualifies
only when the restart cancels work the agent waits on (a monitor, subagent, or
background task) and its session was live, so stale records from an older crash
never wake it. Its continuation prompt is the note of what will not report back. A
shell alone does not wake a settled run: most are dev servers nobody waits on, so
that note waits for the next user message.

Recovery retires effects tied to the lost process and records continuation intent
in the durable outbox. That intent survives another restart before provider startup.
Continuation effects wait for activation; a slow provider must not delay the server's
readiness or the launcher's commit boundary. Graceful shutdown captures intent before
closing providers, then reconciles after ingestion has stopped so a late completion
cannot be overwritten by a stale cancellation.

Shutdown releases captured provider sessions concurrently, retaining the existing
30-second bound for each session scope. The launcher gives the child 75 seconds
for both session and parent-scope cleanup plus reconciliation; generated systemd
and launchd definitions explicitly allow 90 seconds. After timeout escalation the launcher still waits for its captured child to exit
and holds state ownership throughout backup or restore.
These source defaults do not rewrite existing installed service definitions.

The [continuation handler](../../apps/server/src/orchestration-v2/RestartContinuation.ts)
rechecks the preference, provider selection, newer user work, a stop the user requested,
and maintenance turns such as `/compact` before dispatching. Intent and delivery both
refuse a thread that is archived, settled, or still snoozed, a cut turn whose provider
turn already ended, and a run waiting on the user: an approval, question, or secret that was pending,
or that recovery closed on the cut turn. Message-capable questions stay answerable, so
they do not block. Delivery also stops after three consecutive continuations in one
`restartContinuationOfRunId` chain, so a turn that crashes the server cannot loop. Stable
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

Watchers keep only what is stored. Timer tasks keep their next run: an interval
due during the outage runs once, and a fixed time missed by more than ten minutes
is skipped. Pull request watches keep their cursor on the thread's link, so the
first sweep after startup reports outage changes once. Neither the relay nor a
sender resends a webhook delivery the server acknowledged, so the server
acknowledges one only after its relay claim, its log row and a pending record
commit in one transaction. A failed write answers 503 with nothing claimed, so
a retry runs it. The record stays until the run settles; a run that shutdown
interrupts is not marked failed, and after activation the server runs records
it does not own under the delivery's command id, which keeps a dispatch that
landed from repeating
([recovery](../../apps/server/src/jones/scheduledTasks/webhookRunRecovery.ts)).
Requests that arrive while the server is down are lost unless T3 Connect holds
them or the sender redelivers. Approvals bound to the provider process expire,
and background shells, monitors and agent polling loops end with it.

Qualified planned updates with caller-bound operation support capture eligible queues
and live Work Mode conversations before handoff. Jones migration 106 stores this
eligibility separately from upstream projections. Saved continuation preferences or
the explicit one-install continuation option authorize capture; enrollment does not
change those preferences. Startup verifies the exact native operation outcome and
the actual selected source, environment, home, and database before consuming a
one-shot activation claim. Rollback must prove the restored previous source as well.
A later ordinary restart cannot grant the same captured eligibility again.

A dedicated indexed selector captures queues behind active roots as well as idle
roots; ordinary recovery's idle-only queue selector keeps its existing behavior.
A captured queue is released through the existing deterministic `queue.resume`
command only after its root or exact restart-continuation chain completes
successfully. Its messages, ordering, provider selection, and control receipts must
remain unchanged. Preexisting holds, later Stop or hold commands (including no-op
commands), edits, new user work, approvals, failures, and identity variance keep it
held. Recovery's own accepted `provider-runtime.reconcile` receipt is allowed only
when its internal command identity matches the captured thread, startup or shutdown
trigger, and canonical acceptance timestamp. Every intervening user control receipt
is still checked. The exact checkpoint-completion receipt for a captured root or
its continuation is also allowed; a successful current run is still required.
Existing continuation and queue outbox identities provide retry idempotence.

Work Mode captures only eligible completed roots whose provider conversation was
still live before this update. One cold admission may reopen that same native
conversation at its original due time; it adds no immediate prompt or interval
reset. Current Work Mode and continuation controls are checked again. A failed
native resume or a changed native identity sends no keep-warm prompt and does not
fall back to a fresh conversation. Previously evicted sessions and generic crashes
do not acquire this planned-update permission. Both the capturing runtime and the
runtime performing recovery need this implementation; older installed binaries do
not gain it from source publication alone.
