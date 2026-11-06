# Updating Jones Code

The app you use and the server running your agents can be on different machines.
When a server is behind your web or desktop app, an update notice appears in the
conversation and **Settings → Connections**. Update the machine named in that
notice.

## Install status

In the desktop app, open **Install status** in the sidebar footer, just above the
machine filters. The update button also opens it while work is in progress.
A new plan includes **Laptop** only. Choose **Edit** to include other available
machines, then the small **Done** to apply your selection. Escape or collapsing
the panel discards an unfinished edit. Selection applies to this plan and does
not change host enrollment.

Choose **Download on Laptop**, then **Install on Laptop** when ready. Downloading
never installs. Other selected machines wait until the native Laptop installation
has committed and passed its health verification before they download, stage or
install. Their downloads can then begin; every installation still needs its own
review and confirmation. A machine that cannot participate shows the reason.
Enrolled hosts receive this plan's build only through plan selection; enrollment
alone does not include them.

Use **Remove from queue** before a machine starts. This removes planned work;
it does not uninstall anything or roll back a completed update. Started work
cannot be removed through Edit. **Stop update** on a staged host ends its plan
work without installing the downloaded build.

On Laptop, **Stop download** is available only during transfer. Once checking
has entered mount or copy work, or installation has begun, it cannot be stopped
from the panel. A Stop request on a supported host remains pending until its
worker and any native cleanup are proved finished; a late Stop can leave a completed download ready to
install. **Held** or **Not confirmed** means the result is unresolved. Keep the
operation ID and use **Check again** to read fresh evidence; it does not retry
the operation. Closing the panel or restarting the app does not clear a hold.

The header's completion **Done** appears only when Laptop and every chosen device
have settled and no operation is outstanding. A downloaded stage or unresolved
result keeps it open. Done dismisses the plan. Reopening **Last update** is
read-only. The next plan starts with Laptop only.

### Recovery space

The installation review uses storage measurements and recovery support from the
machine being updated. If a full backup copy will not fit but a native clone is
proved and fits, choose **Clone** or **Stop**. Clone requires that method and
never falls back to a full copy. If the required method is unsupported or cannot
be proved, only **Stop** is offered with the reason. A filesystem name or a
successful copy command alone does not prove shared storage. Available-space
measurements are not reservations; installation checks again. An initial install
with no existing state has no existing-data backup to claim.

## Update everything at once

The sidebar update action lists the connected machines and providers it can
update, with a reason for anything skipped. Jones machines are handed to
**Install status**, where Laptop verification and each install review still
apply. They remain visible in the confirmation and results. Provider updates
and other supported server updates keep their existing flow; a Jones plan does
not install Laptop from a bulk restart countdown.

Server updates can interrupt running threads. The confirmation counts the
threads a restart will interrupt and how many can resume afterwards. See
**Before you update** below.

### Prepare Jones desktop builds in the background

Jones desktop builds can download and verify the newest main build while you work.
Turn on **Settings → General → Prepare updates in the background**; it is off by
default. The app never installs a build on its own. Choose **Install** when you are
ready.

A build is prepared after it has been the newest for about ten minutes, so a quick
series of builds downloads only the last one. A newer build can replace an
unclaimed download, but an active plan keeps its Laptop download pinned. Stopped, interrupted or held work does not restart in the
background; use an explicit Download when it becomes safe to proceed. A build you
discard is not prepared again; the next newer build is.

## Before you update

Server updates restart the connection and can interrupt active agents and
terminal commands. Saved threads, settings, and project files remain.

**Settings → General → Continue threads after restarts** is on by default.
It resumes supported active threads after an update, crash, machine restart,
or after you quit and reopen the desktop app. Changes are saved to connected
environments that support this setting; update older servers first. If a supported
environment was offline or has a
different value, use **Apply to all** in Settings after it connects.
T3 Code must start again on that machine;
the setting does not enable automatic startup. Terminal commands may still be
interrupted, and threads without saved provider resume state need a new message.
A thread that finished its turn to wait on a subagent, a background task, or a
command or monitor the agent said it was waiting on is woken and told that this work
stopped. Other background shells and monitors, such as a dev server and its
watchdog, do not wake it; the thread learns about them with your next message.
A thread is not resumed while it waits for your approval, answer, or secret, or
while it is archived, snoozed, or settled. After three automatic resumes in a row,
the thread waits for your next message.

Updates from the previous orchestration system preserve conversation transcripts but cannot carry
every kind of runtime history forward. Read [Threads from older T3 Code versions](./thread-migration.md)
before continuing an important older thread.

## When versions don't match

A client and server must speak the same orchestration protocol. If they do not, the connection is
refused rather than running half-upgraded:

- An app newer than the server is blocked before connecting, with a notice telling you to update
  T3 Code on the machine named in the notice.
- A server newer than your app refuses the connection with an update message.

Update the side the notice names, then reconnect.

For Jones preview builds, **Different preview sources** means the app and server
were built from different source revisions. Its update action targets the build
running in your app, even if a newer desktop update has already downloaded.
Mac and Linux build version numbers can differ while their source matches.
Download the matching server build, then confirm installation when you are ready
for that server to restart. If another source is already downloaded, supported
hosts offer **Replace with** and ask you to confirm replacement of that download.
Replacement downloads the matching source; it does not restart the server.
Install remains a separate confirmed action.

In the desktop app, **Settings → Connections → Match laptop** targets this
laptop's verified installed source. It can also replace a different download
after confirmation on supported hosts. The host's ordinary Download action
follows its update channel and may offer a different source. Match laptop stays
visible with an explanation when it cannot run, and disappears once the matching
source is installed or downloaded. Older hosts may need an update before they
can replace a download. If replacement is interrupted, check the host's current
status and explicitly download again if the earlier download is gone.
Offline, unsupported, or newer servers show why the action is unavailable; the
notice does not substitute the latest build.

When a download is retained, **Check** in Connections or **Check for builds** on
mobile can show a separate **Alternative**. **Replace download** asks you to
confirm that exact pair, then retires the old download before downloading the
alternative. Checking never replaces the retained download, and **Install**
remains separate. If the offer changes or expires, check again before replacing.

## Update a connected server

The offered action depends on how the server runs:

| Action                     | What to do                                                                                                                                                                                      |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Update server**          | Keep the client open while it installs and reconnects. Supported background services update remotely. For a desktop-hosted server, this also closes and relaunches the desktop app on the host. |
| **Update the desktop app** | Update the desktop app on the machine running the server, then reopen it if needed.                                                                                                             |
| **Copy update command**    | Run the command on the named host to update the detected global npm install, then restart the server with your usual options.                                                                   |
| **Copy relaunch command**  | Stop the command-line server on its host and relaunch with the copied command, keeping your usual subcommand and options. This does not update an installed `t3` command.                       |

On the host, run:

```sh
t3 update <client-version>
```

Replace `<client-version>` with the version shown in the notice. The command
asks before restarting the background service; if you decline, run
`t3 service restart` when you are ready. For a server you started by hand,
stop it and start it again afterwards with your usual options such as `--host`
or `--tailscale-serve`.

If you run the server with `npx` rather than an installed `t3`, there is
nothing to update on the host: stop the server and relaunch it as
`npx t3@<client-version>` with the same subcommand and options.

## If an update fails

Keep the client open until it reconnects or reports a failure. A failed service
update can roll back to the previous version. For Jones updates, read the result
before trying another update:

- **Update did not start / Nothing changed** means the attempt was proved to have
  stopped before changing the app or state. Its failure record remains, but a
  settled attempt permits a new update attempt.
- A **rollback** can mean a candidate ran and the previous version was restored;
  it does not always mean nothing changed.
- An unresolved or blocked result needs recovery evidence. Repeated **Install**
  clicks or deleting update records cannot resolve it safely.

### Reconcile a previous attempt

If the status says **Previous attempt needs reconciliation**, use **Reconcile**
only when offered. It appears for an unsettled attempt with an ID when the
launcher supports reconciliation. It checks retained evidence; it does not retry
installation. Read the fresh status afterwards and keep the copyable attempt ID.
If the request's result is unknown, check status before sending it again.

A refusal explains why the attempt remains held:

| Code                                                                                             | What to do                                                                                                             |
| ------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| `not-failed`, `not-found`                                                                        | Check the current status and attempt ID; the referenced record is not a failed attempt or could not be found.          |
| `insufficient-evidence`, `active-changed`, `trial-evidence-present`, `retired-evidence-mismatch` | Keep the ID and message for recovery investigation. The evidence cannot safely establish that nothing changed.         |
| `restore-pending`                                                                                | Recovery is still pending. Have that recovery investigated before another installation.                                |
| `settlement-write-failed`                                                                        | The launcher could not save reconciliation. Check fresh status and retain the ID and error for diagnosis.              |
| `capability-unsupported`                                                                         | **Launcher refresh recommended** means this launcher lacks compatible support. Reconcile does not update the launcher. |

Do not delete receipts or state to bypass a refusal. Retained attempt diagnostics
help explain the failure; maintainers can use the
[update recovery internals](../internals/server-updates.md#settling-failed-attempts).
The normal supported **Install** flow needs no extra reconciliation or manual
state operation.

### If Jones Code refuses to start

**Jones Code could not start safely** shows `Startup refused (<code>)`, a
transaction ID when available, and this recovery reference. Startup can make one
automatic attempt to settle an exactly safe interrupted activation, then check
again once. If it still refuses, it exits before opening your state.

Keep the displayed code and transaction ID for diagnosis:

| Code                                                                      | Meaning                                                                                                      |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `transaction-unsettled`, `transaction-settleable`, `unstarted-activation` | An interrupted activation needs a proven safe settlement; automatic handling did not establish a safe start. |
| `trial-permit-mismatch`, `generation-changed`                             | The trial permission or recorded generation does not match.                                                  |
| `ownership-unknown`, `paths-differ`, `lease-lost`                         | Startup cannot establish ownership of the expected state.                                                    |
| `writer-mismatch`, `foreign-state`                                        | The selected writer or state belongs to a different binding.                                                 |

Do not repeatedly relaunch or manually edit activation journals, locks or leases.
Have the displayed evidence investigated before retrying.

### Other server update failures

Check that you updated the server's machine, not only the device you are using.
For a command-line server, use the exact version shown in the notice when
relaunching. Only one server can use a T3 home directory at a time. If startup
reports another owner, resolve that ownership before starting another server;
use a separate home directory when a separate server is intended.

## Update providers

**Settings → Providers** shows provider updates for the selected environment.
**Update all** updates every outdated provider on every connected environment
at once. Hover it to see which providers it will update. Providers that only
offer a manual update command are not included.

After a successful provider CLI update, T3 Code refreshes model discovery for
every enabled instance of that provider using its own configuration. Connected
clients receive the refreshed catalogs automatically. Provider update checks
must be enabled to download the latest T3 Code model catalog.

Provider updates do not restart running chats. An existing session can keep
using the previous CLI until it ends. If discovery fails, check the provider
status and refresh it again.

## Mobile updates

To update an environment from your phone, open **Settings → Environments** and
select it. **Check for updates** finds the latest release on that environment's
current release channel. Keep the app open while the environment updates and
reconnects. Hosts that cannot update remotely show instructions for updating on
the machine instead.

The same page lets you refresh provider status and update supported providers.
These controls update the selected host and require a fresh connection, its
advertised update capabilities, and permission to operate it. They do not install
a phone binary.
Provider update checks and restart continuation preferences are in
**Settings → Maintenance**. If provider update checks are disabled, enable them
there before refreshing to find newer versions. The same setting controls whether
T3 Code downloads model catalog updates, so with it off new models may not appear
until you enable it and refresh provider status.

Phone binaries remain managed by the App Store or Google Play. The mobile app
can also download updates in the background and apply them when you next leave the app.
It saves drafts and queued messages before restarting. If you keep the app open
for a long time, it may ask to install immediately; choosing **Later** leaves the
update queued for the next suitable moment.
