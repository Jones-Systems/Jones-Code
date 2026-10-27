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
attempt's exact recovery payload files, keeping every directory, restore journal,
receipt and unknown entry. Cleanup failures preserve the durable result. Before
the next qualified Install's capacity checks, the launcher retries only the
immediately previous committed or rolled-back generation. An unsafe binding,
file identity or incomplete restore journal refuses that Install while the current
server remains active. Completed rollback journals remain valid after live WAL
or settings files rotate. Startup does not clean terminal qualified generations,
and older retained snapshots remain operator-owned.

An uncertain pointer write, failed restore or failed terminal write preserves
recovery bytes. Failed generations are never retried; the sole qualified failure
cleanup is an accepted-phase launcher restart, before the candidate could run,
with no restore evidence. State-ownership and post-restore failures stay held.
The shared ownership file is never unlinked.
Attachments and other files outside SQLite and the paired startup files are outside
this rollback boundary. Qualified at-most-once resume reservations remain at
`runtime/db-backup/<update-id>/resume-dispatched.json`. Keeping that directory lets
existing candidates reserve resume after commit; the guard still blocks replay
after recovery payload cleanup. Launcher, qualified-update and startup-gate protocols remain 4, 1 and 1.

Recovery policy is negotiated independently of staging and operation support.
The target measures its own database, WAL, SHM and required startup settings,
with distinct copy and strict-clone capacity budgets. A short-lived preflight
binds the environment, installed source, current version and staged handle;
capacity is a snapshot, not a reservation. Both the install adapter and native
launcher/helper revalidate before handoff. Clone proof must come from the native
primitive on the target volume, never from APFS naming or `cp -c` success. A
`clone-required` request cannot fall back to copying if that proof or syscall
later fails. First installation with no existing state claims no backup. The
requested policy remains separate from the method observed in the bound recovery
receipt; neither the policy nor a platform label proves that a clone occurred.

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
same ID. Only the request that reserved an operation can accept it, and the launcher
runs requests one at a time, so a reservation that service state does not name and
that has no outcome belongs to a request that ended, or a launcher that is gone,
without accepting it. The launcher records such an operation as `rejected`: before
it answers the refusing request, before another request or stage retirement, and at
startup. A rejected operation never starts and does not block the next one. Receipts
are published whole, through an exclusive hard link from a temporary file. A failed
outcome without a valid no-effect settlement blocks further replacement. A
malformed or unreadable receipt store also refuses replacement. The launcher validates
the complete retained operation history with bounded per-entry memory rather than
limiting the number of terminal receipts.
Readback of an operation and observation of the currently installed source are
separate checks.

A timeout or missing reply proves nothing about a handoff: the launcher can still
be finishing the reservation's durability writes when the server stops waiting.
After a failed handoff the server therefore asks the launcher. A launcher that
advertises `updateOutcomeProtocol: 1` answers `reconcile-update` in IPC order, after
every earlier request from that server, so an operation it never reserved can no
longer be accepted by any of them. It answers `accepted` when its state names the
operation or holds its terminal outcome, `rejected` for a recorded or newly
recorded rejection or an unreserved operation, and `unknown` otherwise. The server
asks again on `unknown`, backing off to every 30 seconds, until the process exits.
Under an older launcher only acceptance or a refusal it sent can be proven, so an
unanswered handoff stays held until the process exits. A proven rejection clears
the attempt's continuation markers and allows another Install.

Desktop updates have a separate two-phase handoff because installing the app stops
its bundled backend. Preparation returns a token while the connection is alive;
the client commits that token only after receiving it. Otherwise backend shutdown
could lose the only successful RPC result. The client must then observe the
prepared version after reconnecting. If installation fails, desktop restarts the
stopped backends and replays the failure for the same token.

## Settling failed attempts

A failed attempt remains failed. A digest-bound `launcher-recorded` or
`legacy-reconciled` no-effect settlement can permit a **new** attempt; it never
reuses the old ID or rewrites its outcome. Settlement and bounded, redacted cause
evidence live beside the [operation ledger](../../apps/server/src/jones/updates/launcherOperation.ts),
so older receipt readers and historical `reconciliation.state` retain their
meaning. A rolled-back trial and an unknown handoff are not no-effect evidence.

For a proved pre-trial failure, launcher recovery records the failed outcome,
removes only its owned pre-trial recovery payload, publishes settlement, then
records native failure. Interrupted recovery repeats these steps idempotently
under the same ownership checks. Restore evidence or uncertain state keeps the
attempt held; settlement is not a shortcut around rollback.

The client offers **Reconcile** only for an unsettled attempt with an ID and a
launcher advertising `updateSettlementProtocol: 1`. The
[reconciliation endpoint](../../apps/server/src/jones/updates/http.ts),
`/api/jones-updates/reconcile`, requires `orchestration:operate`; its typed refusals distinguish missing records,
changed active state, trial or restore evidence, and failure to save settlement.
Without launcher support, the client recommends a launcher refresh rather than
performing one. Retired attempts use the separate operator-only
`/api/jones-updates/record-retired` endpoint requiring `environment:maintain`, not the normal UI. Its
`retired-exception` tombstone binds the retained retirement evidence and reserves
the old ID permanently. Readback preserves the retired operation's accepted-failure
identity after the current update changes, without authorizing a retry. Deleting a
receipt would destroy that replay protection.

## Admission fence

A process that hands off to an update must not start provider work that the
stopped process then loses. The event sink checks the
[fence](../../apps/server/src/jones/updates/UpdateAdmissionFence.ts) inside the
transaction that records a process-bound effect, or a run that prepares a
workspace, so every origin passes it; an open fence reads one reference and never
waits. An installation raises it just before its final quiet check, which runs on
the SQLite writer: a transaction admitted before the fence rose holds the writer,
so the check sees its work. Until the launcher accepts, new work withdraws an
automatic activation and leaves its stage ready; a manual Install refuses it
instead. From the handoff on, new work is refused with a message to send it again,
and nothing is recorded, so the same command id is accepted later. A failure or
cancellation before the handoff, or a refusal the launcher sent, reopens admission
and restarts the queued runs it held. After any other failed or cancelled handoff,
admission stays closed until the launcher proves the outcome: rejection reopens it,
and acceptance keeps it closed until the process exits.

A scheduled task's message is held rather than refused, because the scheduler
records any dispatch failure as a failed run and drops an accepted webhook delivery
with it. A held dispatch records nothing, and stays held while a handoff's outcome is
unknown. It continues under its command id when admission reopens, and the restart
after an accepted handoff interrupts it from outside, which leaves the delivery's
pending record for the next server.

## Following hosts

An enrolled service host follows the laptop's committed source. The renderer
relays the repository, commit and tree only from native committed publication;
the host verifies the tree against the repository's commit. Its report is bound
to the enrollment ID under `runtime/jones-fleet`, so re-enrollment discards it
and an ordinary follower can finish while the laptop sleeps. Missing tree or
native commit evidence holds publication. `JonesUpdater` enforces the follower
ceiling behind HTTP check, download, exact download, Install, fleet staging and
activation; hiding a button is not the control. A host ahead of the laptop holds
rather than downgrading.

The host retires an unrelated unaccepted stage before preparing the committed
source. Each retirement adds a permanent launcher revocation receipt; validating
that history uses bounded per-entry memory but still visits the retained history.
Both the campaign driver and native fleet store now require a committed Laptop
campaign with its committed generation before staging **or** activation, including
on non-followers. A prepared campaign is insufficient. Install plans additionally
require the native health proof described below before any selected host download.

Plan targets are excluded from legacy campaigns and leader publication by durable
native dispositions, retained after Done and restart. Enrollment is unchanged.
Before a selected enrolled host downloads, a target-owned, enrollment-bound
manual-review directive must be confirmed. Legacy leader writes preserve it;
it suppresses ordinary following of the leader at issue and of the plan target.
A later different published leader source restores ordinary following. An
unselected enrolled host is not implicitly a plan member. Builds outside a plan
retain the ordinary follower path.

Ordinary automatic activation waits for 30 minutes without client input, at least
15 minutes since the previous automatic activation, and no work a restart would
cut. Its time is recorded before dispatch and its final check runs under the
admission fence. A message-answerable request survives restart; a live or
non-resumable approval holds activation until its provider adapter can establish
that it rebinds to the resumed session. These automatic rules do not authorize a
plan-target Install, which is separately reviewed.

## Install plans and durable operations

The [native coordinator](../../apps/desktop/src/jones/installPlan/coordinator.ts)
owns plan persistence, CAS edits, leases, grants and target dispositions. The
renderer relays requests and target evidence; it cannot set a Laptop outcome or
open the gate. Before each grant, native reconciliation must bind the active
resumed transaction to the exact source, tree, artifact, generation and runtime,
with a committed healthy result and no activation block. Source equality,
renderer observations, a descriptor match or an adopted installation do not
establish that proof. The pure shared model validates transitions and projects
status without creating native authority.

A target's [operation ledger](../../apps/server/src/jones/updates/operations/ledger.ts)
records acceptance before effects and retains the original environment, action,
installed-source and plan correlation. A relayed receipt must match that original
intent and the previous receipt's legal transition; it can establish that host's
outcome, never Laptop health. Launcher handoff acceptance, target receipt
completion and a fresh host observation are distinct evidence. Current capability
and freshness govern dispatch even when an older receipt remains visible. Each
Install reviews the exact current stage and revalidates it before dispatch;
Download and the automatic runner never install.

An expired grant with no receipt is fenced with the full original start intent.
If never accepted, its tombstone preserves the real action, source and correlation,
has no `acceptedAt`, and authorizes no effect. A fence cannot overwrite an accepted
operation. A timeout, missing receipt, missing payload or absent job does not
prove no effect; unknown and held work stays unresolved until exact native or
target evidence settles it. Check again reads evidence and never turns absence
into Retry or completion. Directive expiry likewise requires matching target
proof; it is not a synthetic operation fence.

Stop records a request separately from actual worker closure. The preparation
outcome promise can settle before the captured worker and process group have
joined; only positive exit/reap evidence releases admission. A completed download
can win a late Stop, and an admitted copy without a validated stage remains held.
Completion covers Laptop and every chosen device with no outstanding operation;
staging, supersession, handoff and unknown results do not settle a plan. Dismissing
it retains a read-only last result and the dispositions that prevent legacy work
from restarting the target.

## Off-process preparation

Download, archive and tree hashes, extraction and candidate validation run in a
[worker](../../apps/server/src/jones/updates/preparation/worker.ts) started from
the server's own build as `__jones-preparation-worker`, which `bin.ts` dispatches
before the CLI loads. Because the worker is the same build, its protocol always
matches its parent's, and it needs no separate packaging. The serving process only
[supervises](../../apps/server/src/jones/updates/preparation/supervisor.ts) it.
Jobs run one at a time: the supervisor queues them, and the worker holds a
per-home lock, so a second server on the same home is refused. The worker runs at
the lowest scheduling priority; that is not a CPU cap.

The parent never trusts the worker's report on its own. It
[rereads](../../apps/server/src/jones/updates/preparation/rebind.ts) the bounded
durable stage receipt, requires it to equal that report, and checks the receipt's
home, database, active runtime and candidate provenance. A matching qualified
verification receipt, metadata fingerprint and ownership binding allow receipt-mode
verification without repeating the full payload hash. A miss or changed fingerprint
requires full verification; a serving caller delegates that work to its same-build
worker. This is not a general cache bypass: staging still detects copy or tamper
changes, and the launcher fully verifies the candidate at activation. Staging
refuses a Linux candidate that cannot answer
`__jones-preparation-worker --protocol`, since an installed host could not prepare
its next update without it. Darwin candidates are never launched while staging, so
only the desktop artifact smoke proves their packaged worker.

Each job lives in `runtime/jones-preparation/jobs/<id>` on the home's own disk.
That directory holds the worker's `TMPDIR` and extraction scratch, and the parent
holds the job's owner lock while the worker runs. Cancellation, a deadline, a
missed heartbeat or an output bound requests termination of the captured worker's
process group. The supervisor exposes outcome and settled/join separately; an
unproved exit retains the busy/admission hold. A worker whose parent exits sees
end-of-file on stdin and stops itself. A sweep job
at startup and after every failed job removes what a killed worker left. It takes
a job directory only when its owner lock is free, and it also removes unpublished
Actions attempts and abandoned `.jones-stage-*` runtime copies. Only exact removed
job IDs provide sweep release evidence; retained IDs preserve the hold and a
missing job alone releases nothing. It keeps only the two newest
completed downloads, so failing candidates do not accumulate.

### Verification cost evidence

[Verification receipts](../../apps/server/src/jones/cloud/qualifiedRuntime.ts)
at `runtime/jones-verification/<version>.json` bind the matching fingerprint and
receipt to staging timing. Timing distinguishes the executing role, file and byte
counts, wall time, process CPU, result and event-loop delay; dedicated workers
report no event-loop delay (`null`). These observations explain where work ran;
they do not establish a fixed speedup or number of saved hashes.

The in-process timing ring holds 32 records. The launcher also emits JSON timing
to stderr and exclusively creates `runtime/jones-update-timing/<operationId>.json`,
retaining the newest 64 files. A timing-write failure must not replace the original
error or turn an already committed update into a failure. These diagnostics do not
change database backup receipts.

## Desktop background preparation

The desktop can prepare its own stage while the user works. The opt-in lives in a
Jones-owned file beside the desktop settings, whose writer drops unknown keys, and
is off by default. Preparation only stages: Install remains a renderer action, so a
background download starts no fleet campaign, and a current plan pins its Laptop
stage against replacement. Durable stopped, interrupted and held target
dispositions prevent automatic restart; an explicit Download clears a stop only
through native admission. A claimed install pins its stage until commit or rollback.
Downloads, startup artifact validation and payload removal run one at a time in a
forked low-priority process group. Main trusts its reply only after binding it to
the on-disk receipts. Niceness lowers scheduling priority; it is
not a CPU cap.

A newer build replaces an unaccepted stage only through the helper's validated
discard. Unlike a host retirement, a desktop discard writes no launcher revocation
receipt. Disk capacity still bounds retained payloads. The replaced
handle is recorded before the discard. After a proven discard, the worker removes
that handle's payload unless any selection, attempt, transaction or retired
preparation names it, and it removes the completion index before the tree. Two
unremoved handles pause replacement with a visible reason while the prepared build
stays installable. Cleanup authority is durably pinned outside the payload tree before
the completion index is removed, so interrupted deletion can resume even after the
inner receipt is gone. This evidence authorizes cleanup only; it cannot make an
unindexed attempt installable. Background cleanup retains its handle until both app
and artifact roots are proved reclaimed or absent. Unknown or conflicting evidence
still holds replacement.

## Desktop activation and startup refusal

Desktop preparation uses an admission handshake after transfer and hashing but
before mount/copy. Main records admission durably before permitting the worker to
continue. User Stop is limited to transfer and signals only that disposable
captured group, never the app, backend or installer. A result, IPC error or PID
alone does not establish exit. On restart, a validated retained stage or positive
captured-worker absence can reconcile transfer; admitted copy work without a
validated stage remains held even after worker exit. Recovery review is bound to
native target-owned preflight and negotiated helper support before quiescing.
A cold staged Laptop can obtain this bound preflight without creating a plan or
reservation; only a later reviewed start authorizes installation.

A helper failure proved to precede state pairing and app swap can return
`rolled-back` with `preEffect` evidence: the original app and state are unchanged.
This is distinct from rollback after a trial. Relaunching the previous app is safe
only after every recorded process is proved exited within the 30-second bound;
otherwise relaunch is skipped. An uncertain exit cannot authorize a second writer.

[Native startup](../../apps/desktop/src/jones/updates/jonesNativeStartup.ts)
checks the [writer fence](../../packages/shared/src/jones/nativeWriterFence.ts)
before opening application state. Refusals carry a code and, when known, a
transaction ID. For an exactly settleable interrupted activation, startup
synchronously materializes the **current app's** embedded helper using its
hash-derived filename, exclusive creation and file/directory durability. It invokes
`--settle-interrupted-activation` once; the helper owns the nonblocking activation
lock and compare-and-swap checks. Only a closed protocol-1 `settled` result permits
one fresh fence check.

This journal-only settlement requires matching manifest and transaction evidence
in intent/preparing/quiescent state, or a matching intent alone, before any state
pairing or app swap. Trial, cold, paired, swapped, blocked, legacy and unknown states
are not eligible. A valid candidate trial or later paired rollback still follows
its existing path. No automatic broad repair or artifact deletion is implied.

If the fence still refuses, startup reports **Jones Code could not start safely**
with the code, optional transaction ID and the
[user recovery reference](../user/updating.md#if-an-update-fails), preserves the
bounded stderr diagnostic and exits with status 1 before opening state, even if
dialog reporting fails.

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
readiness or the launcher's commit boundary. Before quitting, the desktop asks its
ready local backends to write the same passive, opted-in continuation markers as
the native installer. Preparing before signalling the backend process group avoids
racing provider exit; the markers remain passive until process-loss recovery.
Graceful shutdown also captures intent before closing providers, then reconciles
after ingestion has stopped so a late completion cannot be overwritten by a stale
cancellation. Older or unavailable backends fall back to that shutdown and startup
recovery. Desktop Quit and updater handoff allow the same server shutdown grace
and await the captured backend exit, including after forced escalation; expiration
of the grace alone does not establish closure. Ordinary restart recovery keeps
queued runs held.

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
