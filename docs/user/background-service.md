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
provided by this LaunchAgent. The current artifact workflows do not provide a
Mac CLI archive for this setup; the desktop DMG is not a substitute.

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
