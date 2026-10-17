# Native Workstreams provider enrollment

Workstreams thread registration needs the control plane to call this server's
native provider API (`/api/workstreams/native/v1/{context,attestations,settlements,settlements/lookup}`)
with a dedicated bearer. `t3 workstream provider` reserves that bearer's session
and publishes it as a private file. Control-plane activation alone (the six
`T3_WORKSTREAM_*` variables) shows Workstreams but cannot register native
threads.

These commands change protected server state. Running them against a real
installation, reading its stores, or restarting services needs the owner's
approval of that exact operation. A command or flag grants no authority.

## Standalone CLI prerequisites

- The installed server is a qualified Jones artifact: the launcher state under
  `<home>/runtime/` names the active version, its release receipt matches the
  invoked package's stamped Jones source, and no update or restart is pending.
  A development build has no stamped source; enrollment and the native API
  refuse it.
- Native store authority was enrolled (`t3 workstream authority enroll`) and is
  active. No command here initializes a T3 home, authority, schema or signing
  key; missing or changed qualification fails closed.
- The server has started once on this home, so `statev2.sqlite` holds the
  migrated schema and `userdata/secrets/server-signing-key.bin` exists.
- The control plane has provisioned the owner and principal IDs used below.

## Commands

All commands take the explicit `--base-dir` (T3 home) and `--authority-dir`.
`plan`, `apply` and `readback` also take `--credential-path`, which must be a
path under the T3 home, and the request file. Each prints one JSON receipt with
no bearer, signing material or database URL.

| Command    | Effect                                                                                                                                                                                                                                                                             |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `prepare`  | Builds the closed request from persisted authority, environment and build identity plus `--owner-id`, `--principal-id`, `--enrollment-id`, `--registry-origin` and `--lifetime-days` (1–30, default 30). Writes it no-clobber, mode `0600`. Opens no database and reads no secret. |
| `plan`     | Inspects records and the credential file hash; never reserves or issues.                                                                                                                                                                                                           |
| `apply`    | Reserves the session and enrollment records, signs the bearer for exactly that session, and publishes the credential. `--expected-prior-sha256 absent\|<sha256>` guards the file.                                                                                                  |
| `readback` | Verifies the records and that the file hash equals `--expected-credential-sha256`.                                                                                                                                                                                                 |

`--registry-origin` is the control plane's exact origin: HTTPS, or HTTP on the
literal `127.0.0.1` or `[::1]` for a same-host control plane. It is bound into
the enrollment record; the server does not contact it.

Run `prepare` once and keep its file. Retries reuse the same request, so a lost
`apply` reply resumes the same session. Preparing again creates a different
session, which conflicts with any reservation already made for that enrollment
ID. A receipt in state `unknown`
needs `readback` before any further effect. Existing different credential bytes
are preserved as a `conflict`, never replaced.

### SQLite sidecars

`plan` and `readback` default to `--sqlite-sidecar-effects deny`, which only
accepts a quiescent rollback-journal store and refuses a WAL database before
opening it. A running server keeps its store in WAL mode, so inspecting it needs
`allow`. That opens the store read-only but may create or update its `-wal` and
`-shm` files. `apply` always opens the store read-write and may write the same
sidecars. Approval for these commands must cover those effects.

The store is opened through a fixed `file:` URI with extensions disabled, and
only on these source-qualified runtime pairs: Node 24.13.1 with SQLite 3.51.2,
24.19.0 with 3.53.3, and 24.21.0, 24.22.0 or 26.8.2 with 3.53.4. The Node 26
pair serves the standalone CLI artifacts; the desktop's Electron backend uses
its own embedded Node runtime. Qualification follows the process executing the
enrollment command, not the version of a different running server.

The native-enrollment CI job exercises the exact Node 26 pair on Linux and
macOS arm64, records the test and subprocess executable identities, and checks
escaped file URIs, missing-file non-creation, read-only writes, disabled
extension loading, WAL visibility and transaction-bound authority. Existing
Node 24 CI remains in place. A newer runtime needs these checks before its exact
Node/SQLite pair is added; a release label or successful server startup alone
is insufficient. Unlisted pairs continue to fail closed.

## Desktop owner setup

A packaged desktop uses its live owner channel instead of the standalone
launcher qualification. The established primary app and its captured backend
must retain native admission and the exclusive desktop owner lease. Missing
admission, an unfinished update, changed source or paths, or a lost parent
channel refuses setup. Ordinary startup never enrolls authority.

Send POST requests to /api/workstreams/native/owner/v1/operations on the actual
local backend socket with an existing access:write bearer. The socket peer must
be loopback. Browser cookies, Origin, forwarded identity and Tailscale identity
headers are rejected; a reverse proxy is not an owner transport. Responses never
contain a bearer or signing material. The server fixes the home, database,
default authority directory and build identity; callers cannot select them.

Perform the closed operations in this order:

1. authority-enroll has only its operation field. It initializes absent authority
   through the existing persistence lock, or reads the same active namespace and
   generation. It never resets an existing authority.
2. provider-prepare takes enrollmentId, ownerId, principalId, registryOrigin and
   lifetimeDays (1–30). Use the provisioned control-plane identities. The server
   writes a private, no-clobber request under the home at
   workstreams/native-enrollment/<enrollmentId>.request.json.
3. provider-plan takes enrollmentId and returns the existing non-secret plan.
4. provider-apply takes enrollmentId and expectedPriorSha256 (null for absent,
   otherwise the exact prior file hash). The existing reservation transaction
   publishes workstreams/native-enrollment/<enrollmentId>.credential.json with
   mode 0600 in a private directory.
5. provider-readback takes enrollmentId and expectedCredentialSha256 from the
   apply receipt. Verify the same request, native context and credential hash.

Each owner operation qualifies the existing live backend SQLite connection with
database-list and journal-mode queries, releasing only that reservation before
the operation proceeds. Desktop identity checks use filesystem metadata without
opening the main file, since closing a raw descriptor can release SQLite's
process locks. Plan, apply and readback retain their existing SQLite-managed
per-operation connections; the outer backend connection remains live throughout.

Plan and readback inspect the running WAL store with the existing operational
sidecar allowance. Apply retains the same reservation, conflict and unknown
receipts as the standalone command. Reuse the prepared request after a lost
reply; reconcile unknown with readback before further effects. A new prepare
never replaces an existing request. Desktop setup supports only the default
native-store-authority directory and the qualified embedded Node/SQLite pair.

## Control-plane configuration

The supplied Control Plane contract at published revision
`3861a93be609ec08a67a3b5fec71d48125395179` configures
`CONTROL_PLANE_WORKSTREAM_PROVIDERS_JSON.native` with exactly `origin`,
`credential_locator` and `context`. These external configuration and staging
details were supplied for this procedure, not independently read from Control
Plane source; resolve the installed revision's contract before operating it.

`origin` is this server's reachable HTTPS origin, or its loopback origin for a
same-host control plane, for example `http://127.0.0.1:3774`.
`credential_locator` names a private mode `0600` JSON file on the control-plane
host, owned by the same UID as its service. Its only field is
`{"authorization_header":"Bearer …"}`. For separate hosts, transfer the exact
credential through an authorized private channel; never print it or include it
in an API receipt. No directory on the credential path may be a symbolic link.

Set `context` to the complete `native_context` from the Jones enrollment
receipt: `owner_id`, `principal_id`, `source_instance_id`, `authority_namespace`,
`store_generation`, `enrollment_id`, `protocol`, and
`build` (`repository`, `sha`, `tree`). The
[local native contract](../../packages/contracts/src/jones/workstreamsNativeProvider.ts)
defines these fields. `source_instance_id` binds the persisted environment ID.
Keep the request hash as enrollment evidence; it is not an extra descriptor
field.

The Control Plane enrollment CLI emits `registry_context`, `owner_grants` and
`source_bindings`; that output is not the native descriptor. Its `stage-runtime`
step stages signing keys, and `stage-jones` stages the six activation values.
Neither updates the native descriptor. Do not rewrite unchanged signing or
activation files solely because the Jones build or native bearer changed.

## Lifetime and revocation

The bearer expires with its session: 1–30 days from `prepare`, default and
maximum 30. Use the request's `session.expires_at` as the deadline; `apply` does
not restart that clock. Enrollment records are permanent. A replacement bearer
or a changed source build needs a new `--enrollment-id` and native context.
The enrollment binds repository, commit SHA and tree as well as environment,
authority namespace and store generation; an unexpired old bearer does not
qualify a changed build.

Assign an operator and a due trigger before `expires_at`, allowing time for
replacement and proof, and make this procedure part of every source-changing
update. This runbook does not install a renewal scheduler. With approval for
the exact installation, private configuration and restart effects:

1. Capture the current descriptor's exact bytes and hash privately, the old
   session ID, and the existing reference's registration, memberships and
   workstream versions. Qualify the intended running Jones build and authority.
2. Prepare, plan, apply and read back a replacement enrollment using a new ID
   and credential path. Enroll its context in Control Plane through its
   qualified enrollment route. Reuse that request on retry and reconcile any
   unknown result before proceeding.
3. Separately update the private native descriptor with a compare-and-swap
   guard on the observed old configuration bytes/hash. Retain the old bytes
   and credential privately for recovery. A mismatched guard stops the change;
   the enrollment and staging commands do not perform this switch.
4. Restart Control Plane through its controlled service route only after
   verifying there are no active jobs. Prove that the actual running adapter
   loaded the replacement descriptor and bearer using the checks below.
5. Only after the new adapter and same-reference verification succeed, revoke
   the old session with `t3 auth session revoke <session_id>` and confirm its
   rejection. On failed proof, retain the old session and private recovery
   material. Reconcile unknown effects before another change; restoring old
   descriptor bytes alone cannot qualify a source build that has changed.

### Prove the running adapter and renew an existing reference

Through the actual running Control Plane adapter and configured credential,
require `GET /api/workstreams/native/v1/context` to return `ready` with full
context equality, including the build and enrollment ID. Then require
`POST /api/workstreams/native/v1/attestations` for the intended native identity
and expected namespace/generation to return `attested` with fresh timestamps
and `expires_at - attested_at = 300000` milliseconds. A standalone request with
a manually substituted bearer does not prove what the running adapter loaded.

Native attestation lasts five minutes, independently of the session lifetime.
Renew it on the same reference with a signed `POST /workstreams/v1/commands`,
using a fresh `command_id`, current `expected_server_generation` and
`expected_registry_version` guards, and
`action.operation = verify_reference`, the existing `native_reference_id` and
its current `expected_attestation_version`. Reusing an earlier command ID can
replay its receipt instead of performing a fresh verification. Require a
terminal committed receipt, then GET the reference and confirm
`registration.state = attested`, `attestation_version` increased by one, fresh
`attested_at`/`expires_at`, and the expected authority namespace and generation
in its evidence. Read back memberships and workstream versions and confirm
they are unchanged. The action and registration fields are defined in the
[local registry contract](../../packages/contracts/src/workstreams.ts).

Do not reattach or move a reference to renew its attestation. Initial attachment
and movement have separate smoke checks. Registry list/create/edit health is
also separate from native attestation: successful registry calls do not prove
native freshness, and an expired attestation does not necessarily produce a
global unavailable banner. Completing this source procedure does not prove a
live renewal, configuration switch or service restart occurred.

## Interrupted desktop restore

The native helper records rollback-intent before restoring the paired database
and profile. It fences existing authority before replacement and advances the
same namespace exactly once after durable restoration. If the advance reply
is lost, authority may already be active at generation G+1 with a null
transition. The helper stops without launching or replaying; rollback-intent or
blocked in the enclosing update journal prevents a conforming native package
from obtaining ordinary admission. This does not intercept legacy binaries or
make the standalone transition command a launch gate by itself.

Reconciliation starts read-only: bind the same home, database, profile,
authority, transaction and retained private advanced-state-authority-guard.json;
inspect all six canonical fields (record_version, environment_id,
authority_namespace, store_generation, state and transition_id), the durable
retained.json and pair proof, captured child custody and terminal
outcome, and bundle disposition separately. Fenced generation G with its
matching transition is still fenced. The same identity at active generation
G+1 with a null transition is the expected authority postcondition and must
never be advanced again. That record alone cannot attribute completion to the
restore. Missing or mismatched evidence keeps the installation held; do not
refence, decrement, restore an old authority record or retry automatically.
