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
state ownership held during its snapshot and restore. Completed current-attempt
recovery payloads are removed after a durable terminal outcome, while small
startup and timing receipts remain; older retained snapshots
are left alone. Do not add a second full backup or
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

### Exact pre-publisher private setup compatibility

Ordinary adoption has one closed exception for the authenticated Linux x64 artifact
at source `d3e6f8e843a0477542380a4ec0d9ae02a1084053`, tree
`250db17dd136cef960e9bb8e054acadb41fba03d`. It requires
`--accept-unattested-child-capability` during the reviewed dry-run and apply.
Other sources, trees and platforms retain the ordinary child capability requirement.
A version string alone never selects this exception.

Run both the old matching artifact's genuine private setup and the qualified
adoption commands with `umask 0077`. Payload qualification compares executable
permission bits, so both extractions must use the same mask. Adoption requires an
owner-only `.jones-provenance.json` and checks its actual metadata. It authenticates
all nine provenance fields against
the Actions artifact and executable bytes, and requires the exact old root layout.
The native pending receipt records the file's exact preimage and a new exclusive
evidence archive under `runtime/jones-adoption`, outside the selected version root.
After comparing the occupied payload with the authenticated archive, adoption
preserves provenance with a same-filesystem rename into the owned 0700 archive,
syncs both directories, and checks the preserved bytes and inode before adding the
canonical qualified receipt. It rechecks layout, evidence and running child identity
before stopping. Unexpected entries, metadata, changed evidence or an occupied
archive fail closed. No database content is hashed, copied or migrated by this step.

The old startup preflight proves protocol and version compatibility only. The
explicit layout check precedes effects and stop; real runtime readiness is observed
after restart. Success requires the actual launcher receipt bound to its live PID,
new child PID and both artifact versions. Any child capability receipt on this
pre-publisher path blocks completion. The adoption receipt reports
`attestation:"launcher-only"`, `attestationBasis:"pre-publisher-child"` and an
unattested child capability, never `capability.install:true`. The normal API update
gate remains independent.

`jones host status` may print private provenance as `unverified` after the archive
move. That display does not authenticate qualified adoption. Use the qualified
runtime receipt, preserved archive, and native launcher/runtime readback as evidence.
An interrupted move leaves the pending receipt and available evidence intact and
blocks replay. Reconciliation or any manual restoration needs a separate exact
approval; adoption never deletes evidence or restores it automatically.

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

### Exact system-unit adoption

`jones host adopt-system` adopts one existing system unit that runs the exact
authenticated pre-publisher private setup build directly as its service user.
It keeps the unit's owner, service user, `NoNewPrivileges=yes`, `KillMode`,
slice, resource limits and every existing unit file. It adds only the owned
`zzzz-jones-qualified-launcher.conf` drop-in, which selects a qualified launcher
that supervises the same retained version. It does not move the service to a
user unit, change the database, run migrations or claim child Install. Other
active builds, homes with native service state or an adoption record, and
already-owned drop-ins are refused.

The unit's effective `ExecStart` must run the retained executable as `serve`
with only `--host`, `--port`, `--base-dir` and `--mode`. The one other accepted
form is the historical `start` with those flags, `--mode web`, and
`--no-browser DIR` last, where `DIR` is exactly the unit's effective
`WorkingDirectory`. The dry-run and the worker both refuse any other flag, a
repeated flag, a boolean value instead of DIR after `--no-browser`, and a missing, extra, non-canonical
or different directory. Adoption does not keep running `start`: the launcher
runs the retained version as `serve` from the unchanged `WorkingDirectory`. That
startup is headless and does not bootstrap a project from that directory
automatically. Existing projects and state are not changed, and the plan states
this transition.

Root effects go through the closed worker
[`systemServiceControl.pl`](../../apps/server/src/jones/hostService/systemServiceControl.pl),
installed root-owned at `/usr/local/libexec/jones-code/jones-system-service-control`
with the root-owned policy `/etc/jones-code/system-service.policy`. The policy
fixes the unit, service user, UID, GID, home, slice and retained executable. The
worker accepts only `inspect`, `stop`, `install-launcher`, `start` and
`archive-launcher`, each with the expected unit digest and owned drop-in state.
Its unit digest covers the fragment and every other drop-in by owner, mode and
SHA-256. It reports metadata and digests, never unit contents, Environment values
or a command line. It renders the owned drop-in itself from the policy home, the
launcher version and the host, port and mode of the retained command.
No sudoers rule grants it: the owner authenticates sudo for the session that
runs the apply, and the CLI reaches the worker only with `sudo -n`. Normal
Download and Install never use it.

The dry-run is read-only: it creates no lock, record, scratch, permission change
or cache, and needs no sudo. It prints the plan, its SHA-256, the owned drop-in
text, the observed state of the root bootstrap and the bootstrap manifest.
Installing that bootstrap is a separate, one-time, approved root operation. It
covers the worker bytes from the qualified source, bound by
`SYSTEM_SERVICE_CONTROL_SHA256`, and the rendered policy. Adoption installs
neither and refuses to apply until both are present exactly. Every unit file must be readable by the service user, so that
the dry-run can bind it.

```sh
"$JONES_CLI" jones host adopt-system --base-dir "$JONES_BASE" \
  --system-unit APPROVED_UNIT --port APPROVED_PORT \
  --retained-executable /approved/retained/t3 \
  --active-artifact-dir /approved/current-artifact --active-source-commit CURRENT_SOURCE_SHA \
  --launcher-artifact-dir /approved/launcher-artifact --launcher-source-commit LAUNCHER_SOURCE_SHA \
  --accept-unattested-child-capability --dry-run
```

Apply repeats the command with `--approved-plan-sha256 REVIEWED_PLAN_SHA256`
instead of `--dry-run`, after the owner has authenticated sudo in that terminal.
It derives the plan again and stops unless the digest matches. It then records
each phase in `runtime/jones-system-adoption.json` before acting:

1. Compares the worker's inspection with the plan.
2. Changes an owned 0775 `runtime` directory to 0755 when the plan says so. The
   launcher requires that nothing else can write it; `versions` and the rest of
   the home are unchanged. Every later record carries `runtimeMode`: `applied`
   is `false` before the attempt, missing while the change or its readback is
   unsettled, and `true` once read back on the same directory.
3. Enrolls both authenticated runtimes. The full retained payload must equal the
   extracted archive.
4. Reads the launcher's pending migration IDs without writing them.
5. Rechecks the unit, process birth, executable, cgroup, descriptor, database
   inode and environment.
6. Stops the unit through the worker. Before stopping, the worker refuses a
   unit with a queued job, or whose cgroup it could not read afterwards: a
   path outside the policy slice and unit, no unified cgroup hierarchy, or a
   missing, unreadable or malformed `cgroup.events`. That check shows only
   that the later proof is possible; the running cgroup may still be
   populated. The stop completes only when systemd reports exactly
   `inactive`/`dead` or `failed`/`failed`, with no main process, queued job
   or cgroup, twice with the same state, and the cgroup the unit ran in, with
   every descendant, is unpopulated or gone. A retained server that exits
   with a nonzero status during the stop may leave `failed`/`failed`; that
   state is admitted only with this proof. The worker never resets a failed
   unit or changes its exit-status settings, so systemd's recorded result
   stays until the later start replaces it. Any other state leaves the stop
   `unknown`, and an inspection alone never counts as proof that the unit
   stopped.
7. Acquires the server's lifetime exclusive transaction on
   `userdata/server-owner.sqlite`. A busy lock or a known live legacy owner keeps
   the unit stopped. The lock file is never unlinked.
8. Creates `service-state.json` exclusively with protocol 4 and the retained
   version, then installs the owned drop-in while holding state ownership.
9. Releases state ownership before starting the unit. Each worker operation
   requires the same exact stopped state again, as systemd reports it at
   that moment; installation must read its terminal pair back unchanged after
   the reload. Only the stop examines the cgroup the unit ran in.

The record becomes `applied` only after the launcher and the retained child are
observed under the unit's cgroup, together with the launcher capability receipt,
the same environment, database inode, unit digest and migration IDs. The child is expected to
publish no capability receipt, so the record says `attestation:"launcher-only"`
and `childCapability:"unattested"`. Whether the retained child offers Download
and Install is observed afterwards through its own update capability; a
`bootstrap-required` result is a gap to report, not a reason to retry.

A failure whose effects are all known leaves the record `partial` at its
recorded phase. That does not mean nothing changed: the permission correction
and runtimes enrolled before that phase remain, and a failure while `enrolling`
can leave only some of them. An uncertain permission change, stop, state
creation, launcher installation, start or readback leaves it `unknown`.
Neither is retried, reversed or restarted automatically, and a record of either
kind blocks a second adoption, including one written by an earlier release:
adoption refuses any existing record, and no newer release resumes, rewrites
or clears it. The root worker never writes these records. If state ownership
cannot be acquired after the stop, the unit stays stopped. The retained executable
directory, every original unit file and the database are preserved. Recovery,
including archiving the owned drop-in with `archive-launcher`, needs its own
approved plan.

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

## Workstreams control-plane activation (Linux user service)

The server enables Workstreams only when all six variables are present and
canonical: `T3_WORKSTREAM_CONTROL_PLANE_URL`, `T3_WORKSTREAM_OWNER_ID`,
`T3_WORKSTREAM_PRINCIPAL_ID`, `T3_WORKSTREAM_KEY_ID`,
`T3_WORKSTREAM_SIGNING_SECRET` and `T3_WORKSTREAM_AUTHORIZATION_REVISION`.
The control plane's provisioning tool, `ops/bin/provision-workstreams-principal.mjs`
in chatgpt-control-plane, produces their values. Never put them in a drop-in,
command line or log.

On a Linux user service, store them in the private EnvironmentFile
`%h/.config/t3code/secrets/jones-workstreams.env`, owned by the service user with
mode `0600`. Reference it from
`%h/.config/systemd/user/t3code.service.d/45-jones-workstreams.conf`, which
contains only:

```ini
[Service]
EnvironmentFile=%h/.config/t3code/secrets/jones-workstreams.env
```

The `45-` prefix sorts before the `50-` task drop-in and the owned
`zzzz-jones-qualified-launcher.conf`. Adoption refuses drop-ins that sort after
the owned launcher, and bound legacy or continuation adoption accepts an extra
drop-in only through `--preserve-dropin` when it sorts before the task drop-in.
Preservation observes metadata without opening the file. The qualified launcher
passes its environment to the server child, so these variables reach the server.

Choose the URL by where the control plane runs:

- **Same host.** Use `http://127.0.0.1:8787/`. The server accepts plain HTTP only
  for the IP literals `127.0.0.1` and `[::1]`, with no path, query or
  credentials; `localhost` is refused. Request signing is unchanged.
- **Different host.** Use the control plane's HTTPS tailnet origin, from a tagged
  node only. Tailscale Serve adds Tailscale identity headers to requests from
  untagged nodes, and the control plane answers `401` to a request that carries
  those headers alongside a signature. That is also why a same-host server
  cannot use the tailnet route.

Writing either file and running `systemctl --user restart t3code.service` to
apply them are operator effects that need host approval. Source changes do not
configure or restart any service.

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

Before a snapshot, restart recovery or rollback, the launcher acquires an
exclusive SQLite transaction on `userdata/server-owner.sqlite`, matching the
server's lifetime ownership. It waits for the captured old or trial child to exit
before acquiring that lock, and releases it before starting the next server.
A busy lock or a known live pre-lock server reports `state-dir-owned`: stop that
server and retry Install. Exit 78 leaves the launcher idle without restoring over
another owner. The ownership file is never unlinked, including after a crash.

Install still validates the exact staged source, candidate startup gate, database
migration compatibility and snapshot capacity before accepting or stopping the
active child. Clone is preferred; unsupported cloning falls back to a checked
copy. Recovery restores the database and paired settings before durably recording
the terminal rollback result. A completed attempt's recovery payload and transient migrated
state are then removed. Its directory and small startup/timing receipts remain,
so existing candidates can reserve resume and duplicate dispatch stays blocked.
Cleanup failure keeps the durable outcome and any remaining files; unknown commit
or restore effects preserve recovery bytes.
Older retained snapshots are not removed by this source change.

The server abandons a launcher exchange after 30 seconds. The launcher refuses
an Install with `request-expired` once 25 seconds have passed since the request
arrived; at readiness it rolls back with `prepared-exchange-expired` before
writing the pointer. Captured child termination still escalates from SIGTERM to
SIGKILL after the grace period and waits for exit.

Normal Download and Install require no observer helper, root socket, lease probe,
sudo, password or terminal. This source change does not remove any previously
installed host files or alter running services. Exact system-unit adoption still
uses its approved worker and policy, described
[above](#exact-system-unit-adoption).

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
