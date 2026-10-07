# Persistent private Jones hosts

This runbook prepares and validates a host-owned Jones service with a persistent
private Tailscale route. Source support does not install or adopt it on any host.
Approve rollout separately for each host, including its service user, absolute
base directory, exact artifact commit/version, loopback and HTTPS ports, state
backup, interruption window, and recovery. Keep upstream T3 units and state
outside that scope.

## Before rollout

Use the [build artifact procedure](./build-artifacts.md) to select a successful
CLI build for the target platform and verify its exact built commit. PR artifacts can contain a
merge-ref commit rather than the PR head. Retain the artifact directory, including
`ARTIFACT.json`, `SOURCE_COMMIT`, `SHA256SUMS`, and the archive. Select the host's
platform and architecture. These unsigned checksums and descriptors are not
independent signatures; choose a source and build run you trust.

The manual Mac CLI artifact workflow source is available alongside the Linux CLI
and Mac desktop workflows. Mac service rollout still requires a successful native
Mac job supplying a verified darwin-arm64 CLI archive and native execution evidence,
or an explicitly approved local build with equivalent evidence. A desktop DMG is
not that artifact.

For each host, record these read-only observations before deployment:

- Current Jones/upstream service identities and their base directories; the
  Jones service cannot be reassigned from another base implicitly.
- The intended base's live server and service state, if any. Do not introduce a
  second server against the same base.
- Listener ownership for the chosen loopback port, and the chosen HTTPS port's
  route ownership. Choose a distinct HTTPS port rather than 443; also avoid the
  upstream desktop's Serve port and development sharing ports.
- A bounded `tailscale serve status --json` capture. Source classifier fixtures
  are inferred from Tailscale's ServeConfig shape, so validate the actual shape
  on every host. Keep the capture in the approved operational record; inspect
  only the relevant mapping and do not publish unrelated routing details.
- Linux linger/user-manager availability, or macOS GUI-login and sleep state.

Arrange a consistent, approved backup of server state before migration or update,
with no concurrent writer during the snapshot. Retain the prior compatible
binary, its provenance, host configuration, service definition, and state
snapshot. Preserve the desktop's profile and saved connections separately.
Backup access is a separate private-state effect; this runbook grants none.

## Operator effects and approval

| Phase                                 | Effects covered by the per-host approval                                                                                                                                                                     |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Artifact placement/staging            | Copy the selected artifact to the approved host path; publish a verified runtime under the chosen base.                                                                                                      |
| Setup                                 | Write host config and the Jones user service; enable/start/reconcile it, potentially interrupting current work.                                                                                              |
| Linux linger                          | Permit `--allow-linger-enable` only when enabling persistent user startup is approved. If administrator recovery is needed, obtain explicit sudo coverage before `sudo loginctl enable-linger SERVICE_USER`. |
| Linux Tailscale permission, if needed | If pairing reports `permission-denied`, obtain explicit sudo coverage before `sudo tailscale set --operator=SERVICE_USER`. Source never runs this command automatically.                                     |
| Pairing                               | Create/reuse the persistent Serve mapping and mint a short-lived one-time pairing secret; expose the secret only to the owner.                                                                               |
| Update/recovery                       | Stop/restart service work, change the runtime, and restore approved compatible state when necessary.                                                                                                         |
| Route removal                         | Remove only the exact recorded claim using the guarded Jones command.                                                                                                                                        |
| Mac pre-login startup                 | A separate owner choice; no LaunchDaemon or automatic-login change is part of this setup. Include any credential, keychain/TCC, admin, or sleep-setting effects in its own approval.                         |

Use the normal service user. Do not run Jones as root, modify
`t3code.service` or `com.t3tools.t3code.service`, reuse upstream `~/.t3`, kill by
process-name pattern, or use raw `tailscale serve ... off` against a Jones route.
Do not pair against that port with a pre-guard binary. Existing upstream binaries
and `dev-share` remain unguarded and can change it; separate ports reduce overlap
but do not establish ownership.

## Stage and set up

Examples below use shell variables only to keep the exact paths and ports
consistent. Set them to the approved host-local values; `JONES_CLI` is the
extracted artifact's `t3`, with its adjacent runtime files intact.

```sh
JONES_BASE="$HOME/.jones-code"
JONES_ARTIFACT=/absolute/path/to/downloaded-artifact
JONES_COMMIT=EXACT_BUILT_COMMIT
JONES_CLI=/absolute/path/to/extracted-artifact/t3
JONES_PORT=47991
JONES_HTTPS_PORT=8443

"$JONES_CLI" jones host stage-runtime --base-dir "$JONES_BASE" \
  --artifact-dir "$JONES_ARTIFACT" --expect-source-commit "$JONES_COMMIT"

"$JONES_CLI" jones host setup --base-dir "$JONES_BASE" --port "$JONES_PORT" \
  --artifact-dir "$JONES_ARTIFACT" --expect-source-commit "$JONES_COMMIT" \
  --tailscale-serve-port "$JONES_HTTPS_PORT" --dry-run
```

Staging checks repository `Jones-Systems/Jones-Code`, the expected source commit,
platform/architecture, archive checksum, and executable version. It records an
extracted-entry checksum for subsequent cached-runtime validation. Existing
version directories without matching provenance are refused and preserved;
removal or replacement needs a separate exact operational decision.

The running setup CLI must match the artifact version because service reconcile
pins that CLI's package version. `stage-runtime` can stage a different version,
but staging alone does not switch the active service. Execute the matching
artifact CLI for setup; do not use an arbitrary source CLI to provision it.

Review the printed plan. Under the approved rollout, rerun setup without
`--dry-run`. Add `--allow-linger-enable` only with coverage for that effect.
Setup writes the fixed loopback configuration, explicitly disables server-owned
Serve, and starts/reconciles the Jones service. It records the HTTPS port and
prints a pairing command without executing it. Later setup preserves the
recorded HTTPS port when omitted; a first setup otherwise defaults to 443, so
always pass the separate approved port explicitly.

Linux uses `jones-code.service` and needs lingering to start at boot and survive
logout. macOS uses `com.jones-systems.jones-code.service` in the GUI user domain.
`installed-awaiting-gui-login` means installed but not running; validate again
at GUI login. This does not provide pre-login boot execution.

## Pair and validate

Use the guarded Jones artifact CLI under the route/pairing approval:

```sh
"$JONES_CLI" pair --base-dir "$JONES_BASE" --tailscale \
  --tailscale-serve-port "$JONES_HTTPS_PORT"
"$JONES_CLI" jones host status --base-dir "$JONES_BASE" --json
```

Pairing uses the existing direct HTTP/WebSocket authentication protocol. Open
the link on the intended desktop, web, or mobile client and retain the saved
connection. The route persists in tailscaled independently of server restarts
and client disconnects; setup and the host service do not own its teardown.

Serve ownership is `absent`, `exact`, `conflicting`, or `unknown` for the chosen
port and loopback target. Jones pairing writes only to an absent mapping or the
existing same-environment development repoint case proven by an environment-ID
probe. It reuses an exact mapping. Conflicts and unknown configuration stop
writes, even when an endpoint is down or responds with 502. After a write,
pairing requires exact configuration readback before minting the secret.
There is no CLI compare-and-swap: another writer can race inspection and mutation.
Readback detects variance after the effect, so an uncertain result is a hold for
inspection, not permission to retry or clear the route.

Validate the verified runtime, Jones unit, fixed loopback port, and live
environment ID in status. Check that the route is exact and the HTTPS descriptor
matches that ID. These checks prove the host side only. From the actual laptop,
verify the saved connection reaches that same environment and can submit work.
Then close the laptop connection for the intended interval (for example, four
hours), confirm host work progressed, and reconnect to current state. Record
Linux boot-without-login coverage separately from Mac GUI-login coverage.
Host sleep, logout, reboot, updates, and service restart can still interrupt
provider work. No source test establishes these operational postconditions.

## Update and recovery

For an approved update, retain the consistent pre-change backup and prior binary,
stage the new exact artifact, and run its matching CLI's setup with the existing
base and ports. Expect interruption. Jones runtime installation, `t3 update`, and
remote self-update refuse the default upstream origin. An explicitly configured
`T3CODE_RELEASE_BASE_URL` permits checksum-checked network installation without
Jones provenance; prefer local staging. Cached reuse without matching provenance
is refused. A client update offer is not a verified Jones release.

Inspect first after an interrupted setup or pairing operation:

```sh
"$JONES_CLI" jones host status --base-dir "$JONES_BASE" --json
tailscale serve status --json
"$JONES_CLI" service status --base-dir "$JONES_BASE"
```

Do not blindly reinstall, uninstall, delete a cache, or retry an uncertain route
write. Reconcile the observed service/config/runtime against the approved plan.
For a known installed service needing repair, use its printed instructions;
`service install --base-dir "$JONES_BASE"` repairs and
`service restart --base-dir "$JONES_BASE"` interrupts work. Only use the verified
matching Jones CLI and include those effects in approval.

A state-compatible rollback needs both the retained prior Jones artifact and the
pre-change state snapshot. Stop the service under the recovery approval with
`service uninstall --base-dir "$JONES_BASE"`, confirm it is stopped, restore the
exact approved state/configuration snapshot, and run the prior artifact's CLI
setup with the retained ports. There is no `setup --allow-downgrade` option.
Do not infer that selecting an older binary alone can roll back migrated state.

For approved route removal, preview and then execute the guarded command:

```sh
"$JONES_CLI" jones host route-remove --base-dir "$JONES_BASE" --dry-run
"$JONES_CLI" jones host route-remove --base-dir "$JONES_BASE"
```

Removal requires the exact recorded HTTPS port and loopback target. It reads
back absence after the effect. A timeout, remaining mapping, conflict, or unknown
readback needs status inspection before any further operation. Service uninstall
leaves the route; route removal leaves the service. Validate each separately.

## SSH fallback limit

SSH persistence still releases only the local forward on disconnect. Its default
server discovery is `~/.t3`, and its Node-free helper download still resolves
upstream `pingdotgg/t3code`. It can install a helper before reusing a host server;
it does not provision a verified Jones runtime or discover the custom base here.
Use direct private pairing for these hosts. Clients and DeviceHub SSH media keep
their existing protocols; this setup adds no new connection type.
