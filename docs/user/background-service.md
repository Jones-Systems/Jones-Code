# Running Jones Code in the background

On Linux and macOS, Jones Code can run as a service for your user. A laptop
connection can close while the host keeps running submitted provider work;
reconnect through the saved environment to see its current state. The host must
stay available: sleep, logout without the required service support, reboot, or
a service restart can interrupt work.

## Set up a persistent private host

Use a verified Jones CLI artifact for the host's platform and architecture.
Choose a separate absolute base directory, an unused loopback port, and a
Tailscale HTTPS port that differs from upstream T3 Code and development routes.
Specify the HTTPS port explicitly; use a separate port rather than 443.
The [persistent-host rollout guide](../operations/persistent-hosts.md) covers
artifact selection, host approval, backups, and platform prerequisites.

Run setup on the host from the artifact's CLI. These examples use a base directory
under your home, loopback port 47991, and Tailscale HTTPS port 8443:

```sh
t3 jones host setup --base-dir "$HOME/.jones-code" --port 47991 \
  --artifact-dir /absolute/path/to/jones-artifact --expect-source-commit EXACT_COMMIT \
  --tailscale-serve-port 8443 --dry-run
```

Replace the artifact path and commit with the verified download. The running CLI
must have the artifact's version. Review the printed effect plan, then run the
same command without `--dry-run` under the approved host rollout. On Linux,
`--allow-linger-enable` permits an attempt to enable lingering; include it only
when that effect is approved.

Setup starts the Jones service on its fixed loopback port and prints the next
pairing command. It does not create a Tailscale route or pair a client:

```sh
t3 pair --base-dir "$HOME/.jones-code" --tailscale --tailscale-serve-port 8443
```

Use the pairing link in your client. Treat it as a secret. This creates or reuses
a guarded, persistent Tailscale Serve mapping. The mapping belongs to the host's
Tailscale service, so closing the client or restarting Jones Code does not remove
it. A conflicting mapping is refused; an uncertain result needs inspection before
retrying. Setup keeps the recorded HTTPS port when you omit the flag later.

## Manage the service

Use the Jones CLI and the same base directory for every command:

| Task                             | Command                                                               |
| -------------------------------- | --------------------------------------------------------------------- |
| Inspect host, runtime, and route | `t3 jones host status --base-dir "$HOME/.jones-code"`                 |
| Inspect service and log location | `t3 service status --base-dir "$HOME/.jones-code"`                    |
| Restart                          | `t3 service restart --base-dir "$HOME/.jones-code"`                   |
| Stop and remove from startup     | `t3 service uninstall --base-dir "$HOME/.jones-code"`                 |
| Preview route removal            | `t3 jones host route-remove --base-dir "$HOME/.jones-code" --dry-run` |
| Remove the claimed route         | `t3 jones host route-remove --base-dir "$HOME/.jones-code"`           |

Restarting or uninstalling the service interrupts active turns, terminals, and
remote clients. Service uninstall leaves projects, threads, and settings intact;
it does not remove the persistent route. Route removal disconnects private-route
clients and runs only when the mapping exactly matches this host's recorded claim.

For upgrades, use another verified local artifact and its matching CLI, then
repeat setup with the same ports and base directory. Back up state first as
specified in the rollout guide. Jones runtime installation, `t3 update`, and
remote host update refuse the default upstream download source. Setting
`T3CODE_RELEASE_BASE_URL` explicitly permits a network mirror download, but that
path checks checksums only and does not establish Jones provenance. Prefer local
staging; an existing cache without matching Jones provenance is refused and
preserved. An update prompt in a client does not prove a Jones artifact is available.

## Adopt an existing Mac direct-serve installation

For an existing user LaunchAgent named `com.t3tools.t3code.service` that runs
an owned executable under your home as `t3 serve`, adoption can retain that
label, active version, port and environment while adding the qualified Jones
launcher. It preserves the dated installation and archives the exact plist for
recovery. This interrupts active work and requires an approved host operation.

First obtain the exact active Mac CLI artifact and the launcher artifact, each
with its retained `github-artifact.zip` and source commit. Adoption verifies
both against their successful Jones Actions generation and main-source CI.
The native `runtime/service-state.json` must have no update block and its active
version must already match the running server and active artifact. A stale
state file requires a separately approved, backed-up reconciliation; adoption
refuses it and does not rewrite it.

Run the qualified CLI on the host, replacing every placeholder with the reviewed
absolute path or full digest:

```sh
t3 jones host adopt --base-dir /absolute/service/home \
  --active-artifact-dir /absolute/active-artifact --active-source-commit ACTIVE_COMMIT \
  --launcher-artifact-dir /absolute/launcher-artifact --launcher-source-commit LAUNCHER_COMMIT \
  --launchd-label com.t3tools.t3code.service --service-unit-sha256 PLIST_SHA256 \
  --retained-executable /absolute/user/home/dated-install/t3 --dry-run
```

Review the effect plan and recovery instructions. The plist hash binds its exact
bytes; the executable must match the authenticated active archive, the loaded
LaunchAgent and the running process. Only the two arguments `t3 serve` are
supported for this legacy shape. The existing plist's other settings remain in
place. Apply the same command without `--dry-run` only under the approved
operation, with a current state backup and enough space for both qualified
runtimes. Do not add the Linux `--legacy-direct-serve` flags or accept unattested
child capability for this Mac path.

Success requires a new service-managed child with the same version, environment
and port, a matching `jones-update-capability.json` reporting qualified launcher
and Install support, and an applied `runtime/jones-host-adoption.json` receipt.
Use `launchctl print gui/USER_ID/com.t3tools.t3code.service` for this retained
legacy label; the ordinary Jones service commands above target the Jones label.
A pending receipt, missing readback or lost command response requires inspection
before another attempt; adoption never retries service effects automatically.

Recovery is a separate approved operation: verify the current plist against the
adoption receipt, stop that exact legacy LaunchAgent, restore the archived
`launch-agent.plist` only after checking its recorded SHA-256 and original mode,
then run `launchctl bootstrap gui/USER_ID /absolute/path/to/original.plist`.
Confirm the old executable, port, version and environment. Keep the adoption
receipt and enrolled runtimes for reconciliation. Any earlier state-file repair
has its own compare-and-swap recovery; restoring the plist does not undo it.

## Follow laptop updates automatically

In the Jones desktop app, open **Settings → Connections** and choose **Enroll
this host** for each server you want to follow laptop updates. Enrollment is
separate from connecting to a server. A host must first have a verified Jones
installation and the supported background launcher; **Setup required** means
that initial host setup is still needed. Enrollment does not replace an existing
system service or take over an upstream T3 Code installation.

Download the offered laptop update, then choose Install. Enrolled hosts follow
that exact source only after the laptop successfully switches to it, and never
move past the laptop. A current host learns the laptop's installed build while
the desktop app is open, downloads it in the background, and installs it by
itself once no client has had input for 30 minutes and nothing a restart would
interrupt is running: turns and delegated tasks, queued messages about to start,
terminal commands, provider background tasks, and approvals waiting in a live
provider session. Questions that can still be answered by message after a
restart, held queues and future scheduled tasks do not hold an update. Automatic
installs are at least 15 minutes apart, and an install stops before it starts if
someone begins using the host. Because the host remembers the laptop's build,
it can finish while the laptop sleeps. Older hosts wait for the desktop app to
coordinate their rollout, and an offline host updates after it reconnects.

On an enrolled host, Check, Download and Install offer only the laptop's
installed build, from any client. Install stays off for any other downloaded
build, including an older one, and a host waits until the laptop has reported
its build.

The connection row shows each host's progress and any action needed. If the
connection drops during installation, Jones checks what the host accepted before
sending another installation request. An uncertain outcome stays on hold;
repeatedly clicking Install does not resolve missing evidence.

Choose **Continue eligible active threads after this host updates** when enrolling
if you want that behavior. Explicit user stops and queue holds remain in force;
provider approval or unavailable native session state can still require attention.
Disabling automatic updates stops new dispatch from the laptop; an installation
already accepted by a host must still finish or reconcile.

## Platform support

Linux uses the systemd user unit `jones-code.service`. Lingering is needed to
start at boot and survive logout. If it is disabled, setup stops unless you
explicitly allow the enable attempt. Administrator recovery needs separate
approval; run Jones Code as the normal service user.

macOS uses `~/Library/LaunchAgents/com.jones-systems.jones-code.service.plist`.
It starts at the user's GUI login and stops at logout. Keep the Mac logged in and
awake for unattended access. Setup over SSH with nobody logged in at the screen
can report `installed-awaiting-gui-login`; installed does not mean running.
Starting before GUI login requires a separate operational decision and is not
provided by this LaunchAgent. The manual
[Mac CLI artifact workflow](../operations/build-artifacts.md#mac-cli-artifact)
provides an Apple Silicon archive for initial setup. A successful native build
and the approved host setup are separate requirements. After qualified launcher
adoption, routine Mac updates use the verified desktop DMG through the headless
runtime wrapper; the DMG itself is not an initial CLI setup command.

Windows background services are not supported. T3 Connect and the background
service are managed separately; signing out of Connect does not stop the service.

## Troubleshooting

Start with `t3 jones host status --base-dir "$HOME/.jones-code"`. It reports
runtime provenance, service state, the configured port, and Tailscale mapping
ownership. A host-side endpoint check does not prove access from your laptop;
check the saved connection there too.

| Status or problem                       | Next step                                                                                                    |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `linger-disabled`                       | Obtain approval to enable lingering, then follow setup's recovery instructions.                              |
| `linger-unavailable`                    | Check `loginctl show-user "$(id -un)" --property=Linger` and systemd-logind availability.                    |
| `user-manager-unavailable`              | Check `systemctl --user status` in the service user's login session.                                         |
| `service-disabled` or `service-stopped` | Read the reported log and `systemctl --user status jones-code.service`; use the printed repair instructions. |
| `restart-pending`                       | A newer runtime is installed; restart when active work can be interrupted.                                   |
| `conflicting` or `unknown` route        | Inspect the host and Serve configuration; do not force overwrite or use raw Serve removal.                   |

On macOS, check **System Settings → General → Login Items** if startup fails.
Access to Desktop, Documents, or Downloads may need Full Disk Access for the
`t3` executable listed in the Jones LaunchAgent's `ProgramArguments`.
For T3 Connect failures, see
[connection troubleshooting](./remote-access.md#t3-connect-troubleshooting).

## Reclaim database space

T3 Code removes superseded internal history on its own while it runs; your
conversations are kept. Databases created before this shrank automatically
still hold the freed space on disk until a one-time offline repair returns it
and turns on automatic shrinking. Settings > Storage (web and desktop) or
Settings > Maintenance (mobile) suggests the repair when at least 1 GiB can be
reclaimed.

Run `t3 doctor` on the machine that hosts the environment. It reports the
database size and the space it can reclaim, then asks which repairs to run;
pass `--fix` to run the recommended ones without a prompt (for scripts), or
`--dry-run` to only report. An installed background service for the same data
directory is stopped for the repair and started again afterwards, also when
the repair fails. Quit a server you started by hand, or the desktop app, first:
the repair refuses to touch a database that is in use. Use `--base-dir` for a
different data directory.

The repair can take several minutes on a large database. It rebuilds the
database through a temporary copy, so it needs free space of about the
database's size twice over: once in the system temporary directory (`TMPDIR`
or `/tmp` on Linux and macOS) and once next to the database for its
write-ahead log.

If you upgraded from a version before 0.1.0, its database (`state.sqlite`) is
still kept next to the current one so you can roll back. `t3 doctor` offers to
delete it, but never selects that repair for you and `--fix` skips it: once it
is gone, a version before 0.1.0 starts with no threads or settings.
