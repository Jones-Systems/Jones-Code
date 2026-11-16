# Switch servers between Jones Code and T3 Code

The server-swap CLI switches an explicitly selected server runtime and service
configuration while preserving its data directory and environment identity. It
supports Jones → T3, T3 → Jones, and Jones → Jones. Different payloads with the
same version string require direct executable configurations in distinct runtime
directories; the native version-keyed launcher cannot represent that pair.
Use normal T3 updates for ordinary T3 → T3 updates.

The CLI accepts pre-staged, exact artifacts. It does not download `latest`, infer
database compatibility, or switch the laptop application. A version can be
selected only when the service layout and an exact-pair compatibility
qualification are available. Unknown compatibility fails before service stop.

## Run it

Source execution requires Node 24.13.1 or newer and the workspace dependencies
installed with the repository-pinned package manager. The CLI reads actual host
platform and architecture through the canonical shared HostProcess references
and Effect; callers cannot substitute plan, environment or build-host values.
From this source checkout:

```sh
node apps/server-swap-cli/cli.cjs --help
node apps/server-swap-cli/cli.cjs plan /absolute/host-plan.json
node apps/server-swap-cli/cli.cjs apply /absolute/host-plan.json
node apps/server-swap-cli/cli.cjs status /absolute/host-plan.json
node apps/server-swap-cli/cli.cjs reconcile /absolute/host-plan.json
node apps/server-swap-cli/cli.cjs resume /absolute/host-plan.json
```

Use an independent terminal or SSH session, outside the server being stopped.
The CLI rejects running inside that service's process ownership. Pause active
work before applying a plan. Source and synthetic tests do not authorize a live
switchover; the operator must approve the exact services, artifacts and private
backup inputs. No credentials are enrolled. Elevation is never inferred: an
approved system-unit plan may explicitly select `service.elevation:
"sudo-noninteractive"` for bounded configuration and systemctl operations.

## Prepare one host

Preparation is a two-stage file workflow. Supply explicit host/service metadata
and staged paths, then obtain compatibility evidence for the returned binding:

```sh
server-swap prepare /absolute/request.json /absolute/preparation.json
# Obtain external qualification for the reported bindingDigest.
server-swap finalize /absolute/preparation.json /absolute/qualification.json /absolute/host-plan.json
server-swap plan /absolute/host-plan.json
```

Both output paths must be new. `prepare` hashes and inventories staged inputs;
its output is not an executable plan. `finalize` checks the external qualification
and unchanged staged artifacts, then returns the final plan's SHA-256 `digest`
for the fleet manifest's `planDigest`. Neither command stops services or reads
the live database. Native preflight remains the separate `plan` command.

A request uses the plan metadata described below with these simplifications:
`source.config`, `source.executable`, `target.config`, and `target.executable`
are absolute path strings; each `payload` contains only `root`. Optional
`runtimeState.source`, `runtimeState.target`, `service.fragment`, and entries in
`service.dropIns` are also absolute path strings. Omit `qualification`; optional
`stateFiles` adds to the standard paired state-file list. No host identity,
service configuration or compatibility conclusion is guessed.

For example, after staging the files (replace all host-specific values):

```json
{
  "schema": "server-swap-plan/1",
  "id": "jones-to-t3-001",
  "host": "exact-hostname",
  "uid": 1000,
  "platform": "linux",
  "arch": "x64",
  "home": "/home/operator/.t3",
  "stateDir": "/home/operator/swap-state",
  "environmentId": "exact-existing-environment-id",
  "descriptorUrl": "http://127.0.0.1:3773/.well-known/t3/environment",
  "service": {
    "manager": "systemd-user",
    "name": "t3-code.service",
    "configPath": "/home/operator/.config/systemd/user/t3-code.service"
  },
  "source": {
    "distribution": "jones",
    "version": "1.2.3",
    "config": "/home/operator/staged/source.service",
    "executable": "/home/operator/staged/jones/t3",
    "argv": ["/home/operator/staged/jones/t3", "--no-browser"],
    "payload": { "root": "/home/operator/staged/jones" }
  },
  "target": {
    "distribution": "t3",
    "version": "1.3.0",
    "config": "/home/operator/staged/target.service",
    "executable": "/home/operator/staged/t3/t3",
    "argv": ["/home/operator/staged/t3/t3", "--no-browser"],
    "payload": { "root": "/home/operator/staged/t3" }
  }
}
```

Stage complete old and new runtime payloads in distinct directories. Keep the
old runtime, native configuration and current data available. Include required
web assets and dependencies, not just an executable. Artifact digests and exact
configuration bytes are part of the plan; changed inputs invalidate it.

The plan uses schema `server-swap-plan/1` and binds:

- A unique `id`, exact `host` hostname, `uid`, `platform` and `arch`.
- Canonical absolute `home`, separate private `stateDir`, `environmentId`, and
  loopback `descriptorUrl` ending in `/.well-known/t3/environment`.
- `service`: `manager` (`systemd-user`, `systemd-system`, or `launchd`), `name`,
  and exact `configPath`. launchd also requires `domain`, such as `gui/501`.
  For a selected systemd override, bind the base unit as `fragment: {path,
  sha256}` and list every loaded override in `dropIns: [{path, sha256}]` in
  effective order. `configPath` identifies the one override being replaced;
  its source hash must match the source configuration artifact.
- `source` and `target`: `distribution` (`jones` or `t3`), exact `version`,
  `config: {path, sha256}`, `executable: {path, sha256}`, exact service `argv`,
  and `payload: {root, files: [{path, sha256}]}`. Payload file paths are relative
  to the payload root; the complete inventory is checked.
- `qualification: {path, sha256}` and explicit `stateFiles` relative to `home`.
  Include all applicable databases, WAL/SHM companions, settings, identity and
  runtime files. A state file's absence is recorded rather than invented.
- Optional bounded `timeoutMs`.

For native `__service-launcher` configurations, the plan also binds the exact
old and new `runtime/service-state.json` artifacts through `runtimeState`.
This prevents a replacement launcher from selecting the old active version.
A pending native update is a blocker; the CLI does not erase its transaction.
Complete source and target version directories must already be staged.

The qualification schema is `server-swap-qualification/1`. Its `bindingDigest`
binds the exact pair and host inputs. Each of `migrations`, `events`, `settings`
and `clients` must contain `status: "passed"`, a substantive `summary`, and a
digest-bound `evidence: {path, sha256}`. These are externally produced qualification
results, not a claim the CLI established compatibility merely by hashing files.
Never manufacture passing evidence to bypass an unknown version pair.

The `prepare` response provides the canonical `bindingDigest` needed by the
qualification producer. Qualification paths/digests are excluded from that binding to
avoid a circular digest. The raw final plan's SHA-256 is the operation identity
reported by the CLI and pinned by the fleet manifest.

Native configuration validation is deliberately strict. Unsupported launch
wrappers or service directives are rejected, not silently rewritten. Existing
privileged system services require separately authorized noninteractive sudo
when that option is selected. The worker itself stays under the data owner's
account; running the whole CLI as another user will not satisfy the plan's
user identity. Unlisted or unsupported drop-ins fail closed.

## Run the remote fleet

Install the standalone worker outside each managed runtime and stage each plan
on its own host. Use existing trusted SSH aliases. The worker keeps its journal,
launch receipt, backups and output on that host; SSH carries bounded status only.

Create a `server-swap-fleet/v1` manifest in the intended order, normally test
server, VPS, then Mac Mini:

```json
{
  "schema": "server-swap-fleet/v1",
  "targets": [
    {
      "id": "test",
      "ssh": "existing-test-alias",
      "worker": "/absolute/tools/server-swap",
      "plan": "/absolute/plans/test.json",
      "planDigest": "<64 lowercase hexadecimal characters from plan>"
    }
  ]
}
```

```sh
server-swap fleet plan /absolute/fleet.json
server-swap fleet apply /absolute/fleet.json
server-swap fleet status /absolute/fleet.json
server-swap fleet resume /absolute/fleet.json
server-swap fleet run /absolute/fleet.json 900000 2000
```

`run` performs the ordered fleet in one foreground command: launch one host,
observe its journal, verify its commit, then advance. The example sets a
15-minute overall deadline and two-second polling interval. The deadline must
be 1 second–30 minutes; polling must be 100 milliseconds–30 seconds and within
the deadline. Every SSH call is capped by the remaining deadline. Unknown
outcomes, failed-before-start, recovery-required, changed plan digests and
deadline expiry stop progression and return exit code 2. Invalid input returns
exit code 1. Output names the stopping host; the laptop handoff stays pending.
All hosts are revalidated after the final host finishes.

The controller deadline or interruption does not stop detached host workers.
Rerun the same `fleet run` command to observe existing journals and launch claims;
it never deletes a claim to retry an uncertain launch. An interrupted local
transaction may require explicit `reconcile` and `resume` on its host before
the fleet can continue. Do not run recovery concurrently with its worker.

`apply` starts at most one unfinished host. `resume` observes the host journal
and advances to the next host only after the preceding host is committed. Run
it again at the next checkpoint, or use foreground `run`; there is no hidden
background fleet scheduler.
The detached host worker continues independently of SSH. Its launch is claimed
once before spawning, so a lost response does not cause a second swap. A crash
between the durable launch claim and a proved spawn remains unknown and needs
operator reconciliation; the CLI does not delete that claim and guess.

When every remote target is committed, the CLI gives the manual laptop handoff.
Quit the current laptop application, launch the selected matching application,
and verify its local environment and remote Connections. A server commit does
not prove provider sessions resumed or the laptop connected successfully.

## Interruption and recovery

Every consequential phase records durable intent before execution. Status and
reconciliation distinguish successful commit, incomplete work, unknown outcome,
and recovery required. Do not start a new plan to bypass an unresolved operation.
Persistent service/home ownership prevents another plan ID or state directory
from bypassing that rule after a process exits. Local status marks a saved commit
as `committed-unverified`; reconciliation establishes current runtime identity.
Fleet status revalidates that identity before offering the laptop handoff.

After target startup may have written data, the CLI does not automatically put
back an old database or start an old binary against advanced state. It preserves
the backup and current state for a separately qualified recovery. Backups are
retained; this CLI has no cleanup or delete command.

## Package and check

Run the focused package tests serially:

```sh
node --test --test-concurrency=1 apps/server-swap-cli/*.test.cjs
```

Build on the target OS/architecture using Node 26 or newer:

```sh
node apps/server-swap-cli/build.cjs /absolute/new-artifacts/server-swap
```

The build uses Node SEA and ad hoc signs the macOS arm64 artifact. Build-only
esbuild 0.28.2 compiles the HostProcess bridge and its complete shared/Effect
dependency graph into one factory inside the existing CommonJS loader. Only
Node builtins may remain external; sidecar outputs and unresolved dependencies
fail packaging. The resulting single executable needs no external Node,
node_modules, source checkout or bridge sidecar. Existing output is never
overwritten. Build natively for Linux x86_64 and macOS arm64.

Linux arm64 source remains available. A Node SEA regression introduced in
26.9.0 affects kernels with 16 KiB or 64 KiB pages, depending on the Node version
([Node issue 67048](https://github.com/nodejs/node/issues/67048)). Node 26.8.2
predates the reported regression; this package has not qualified Linux arm64.
It does not apply the application server's separate arm64 workaround. Packaging
and synthetic manager tests are distinct from live fleet qualification and
exact-version data compatibility evidence.
