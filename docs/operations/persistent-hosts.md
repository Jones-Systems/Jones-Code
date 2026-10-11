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

Qualified host adoption accepts the main-push Linux CLI and Mac desktop artifacts,
and the manual main Mac CLI artifact. A Mac desktop DMG is inspected and extracted
into the existing headless wrapper; its native execution still requires Mac evidence.
The older private-artifact setup below accepts CLI archives only.

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

Use the native updater's consistent recovery generation for routine updates, with
no concurrent writer during its snapshot. Do not add a second full backup or
exhaustive database scan. For initial rollout or operator recovery, include the
chosen recovery method in the approval. Retain the prior compatible binary, its
provenance, host configuration, service definition, and recovery generation.
Preserve the desktop's profile and saved connections. Private-state access is
a separate effect; this runbook grants none.

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

Use the normal service user. The explicit, bound legacy adoption below can preserve
a Jones-owned `t3code.service` user unit. Ordinary setup must not modify that unit
or `com.t3tools.t3code.service`, take over upstream state, kill by
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

`jones host setup` uses a verified private-artifact cache, including preview
artifacts. It validates the cached executable before service changes and never
downloads a replacement. Ordinary `service install` retains the source-qualified
preview restrictions. A desktop-owned home or a runtime with qualified update
receipts must use its qualified activation procedure; private setup does not
replace those receipts or take over that home.

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

## Adopt a qualified launcher

This source command is a separate, per-host approved operation. It preserves the
selected home (including a Jones-owned `~/.t3`), environment ID, database and active
server version. The launcher artifact can be newer than the active server. It does
not upgrade the database or switch the active server to the launcher version.

Retain each exact Actions ZIP as `github-artifact.zip` beside its extracted
`ARTIFACT.json` and archive. Adoption checks the run ID and attempt, canonical
repository, main source/tree, accepted workflow/event, successful main CI, artifact
name and ZIP digest through read-only `gh api` calls. It compares the metadata
inside the authenticated ZIP with the local metadata, then verifies the archive
hash. A local `.jones-provenance.json` or checksum alone cannot enroll a runtime.
The CLI uses gh's ordinary authentication; it does not read an application token.

```sh
"$JONES_CLI" jones host adopt --base-dir "$JONES_BASE" \
  --active-artifact-dir /approved/current-artifact --active-source-commit CURRENT_SOURCE_SHA \
  --launcher-artifact-dir /approved/launcher-artifact --launcher-source-commit LAUNCHER_SOURCE_SHA \
  --dry-run
```

Dry-run verifies and prints exact paths, artifact generations, service contents,
stop/restart commands and readback effects without creating files. Review that
plan as the host operation envelope before repeating the command without
`--dry-run`. Linux appends `zzzz-jones-qualified-launcher.conf`, preserving the
existing unit and environment. It refuses unowned ExecStart overrides unless
`--supersede-execstart` explicitly authorizes the higher-priority drop-in. It
preserves every existing drop-in and refuses unknown or later-sorting precedence.
Mac replaces only the existing launch agent's ProgramArguments, preserving its
environment and other plist settings.

Both verified runtimes receive qualified receipts. An occupied cache is preserved
and must exactly match the extracted artifact before a receipt is appended. Service
state must already decode and contain no pending update. The closed legacy mode
below handles one explicitly bound task handoff; other unknown schemas are refused. A
`jones-host-adoption.json` receipt records pending intent before runtime or service
changes. If any effect or final readback is uncertain, it remains pending and
blocks a blind retry. Inspect and reconcile the exact failed effect under the
operational approval; the command neither deletes nor rolls back native state.

Success requires the restarted service's observed launcher, retained server version
and environment ID, plus `runtime/jones-update-capability.json` bound to the new
server PID with `qualifiedLauncher` and `capability.install` both true. No token
store is accessed for that readback. Source tests use synthetic state and command
adapters; they do not prove an installed host's adoption.

### Bound legacy direct-serve bootstrap

For a Jones-owned Linux user unit running `t3 serve` directly, the additional mode
accepts only `jones.task-tunnel-handoff/1` with the exact seven fields, the retained
tunnel purpose, `nativeProtocol3State:false`, matching top-level and update
operation IDs, and task `update.status:"pending"`. That marker is distinct from a
native pending update. Bind its exact SHA-256 and operation ID, the original unit
SHA-256, and the task-owned drop-in 50 SHA-256. Genuine native pending updates,
unknown fields, staged selections, interrupted restores and native authority
state block bootstrap. The empty, owner-owned 0700 authority directory created by
ordinary server startup is preserved; other contents, permissions or ownership
require reconciliation. The separate `runtime/native-store-authority` path
remains a bootstrap blocker whenever it exists.

```sh
"$JONES_CLI" jones host adopt --base-dir "$JONES_BASE" \
  --active-artifact-dir /approved/current-artifact --active-source-commit CURRENT_SOURCE_SHA \
  --launcher-artifact-dir /approved/launcher-artifact --launcher-source-commit LAUNCHER_SOURCE_SHA \
  --legacy-direct-serve --service-unit t3code.service \
  --service-unit-sha256 APPROVED_UNIT_SHA256 \
  --task-operation-id APPROVED_OPERATION_ID --task-handoff-sha256 APPROVED_HANDOFF_SHA256 \
  --task-dropin 50-jones-code-transition.conf=APPROVED_TASK_DROPIN_SHA256 \
  --preserve-dropin 20-openrouter.conf --preserve-dropin 30-github-cli-path.conf \
  --preserve-dropin 40-agent-mail-token.conf --accept-unattested-child-capability \
  --dry-run
```

Use only the approved host's actual names and hashes. Earlier preserved drop-ins
are observed by metadata without opening credential contents. The task drop-in
parser accepts only its public service directives and maps its explicit loopback
host, port and home into launcher environment values. The effective user fragment,
live direct child PID, executable path, authenticated archive executable bytes,
descriptor environment/version and runtime port must agree. A private provenance
file in the active cache blocks the older PR239 child's qualified layout; adoption
preserves it and refuses instead of deleting it.

After the reviewed dry-run and separate host approval, the applied command records
pending intent, enrolls both runtimes, compares the new launcher's read-only pending
migration IDs, stops the selected user unit, and exclusively archives exact handoff
and task drop-in bytes under `runtime/jones-adoption`. It syncs and reads back those
files before comparing and replacing service state with protocol 4 and the retained
active version. The original unit and all original drop-ins remain intact. Migration
IDs must agree after startup; adoption does not execute migrations or restore state.

The owner-only `runtime/jones-launcher-capability.json` must bind the systemd
MainPID, new child PID, launcher version, retained child version and supported
protocols. With explicit unattested-child acceptance, an absent child capability
receipt permits `attestation:"launcher-only"`; it leaves child Install unattested.
A present stale or invalid child receipt blocks completion. Only a valid child
receipt permits observed `capability.install:true`. Any uncertain effect keeps the
pending adoption receipt and recovery archive and refuses another attempt.

Dry-run prints recovery instructions for separate review: compare the adopted
state and owned launcher drop-in, overwrite only that owned drop-in with the
archived direct-serve ExecStart, compare and restore the archived task state, reload
and restart the same user unit, then verify retained version, home, port and
environment. No recovery is performed automatically. A later launcher-only
continuation must retain the same unit and task preimage bindings and explicit
unattested acceptance, while omitting `--legacy-direct-serve` because service state
is then native.

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

After native updater adoption, use the connected server's Update button. The
launcher retains the prior binary and its consistent recovery generation, tries
a filesystem clone first, and records fallback copy cost. Routine updates trust
upstream migrations; added checks target Jones migrations and their interactions.
No full integrity or historical-content scan is required. Preserve old recovery
generations. Operator setup remains a separate approved recovery or initial-rollout
path using the exact artifact, existing base and ports. Expect interruption.
Jones runtime installation, `t3 update`, and
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
rerun the matching artifact CLI's `jones host setup` with the retained base and
ports to repair a private preview service.
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
