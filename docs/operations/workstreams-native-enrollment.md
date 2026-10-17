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

Plan and readback inspect the running WAL store with the existing operational
sidecar allowance. Apply retains the same reservation, conflict and unknown
receipts as the standalone command. Reuse the prepared request after a lost
reply; reconcile unknown with readback before further effects. A new prepare
never replaces an existing request. Desktop setup supports only the default
native-store-authority directory and the qualified embedded Node/SQLite pair.

## Control-plane configuration

The credential file contains only `{"authorization_header":"Bearer …"}` and is
the control plane's `native.credential_locator`. The locator names a private file on the control-plane host, owned by its
service user. For separate hosts, transfer the exact credential through an
authorized private channel; never print it or include it in an API receipt. No
directory on the credential path may be a symbolic link.

From the `prepare` or `apply` receipt, the control plane's qualified
dependencies use `native.native_request_sha256` = `request_sha256` and
`native.context` = `native_context`. Its provider configuration uses the same
context and this server's reachable HTTPS origin, or its loopback origin for a
same-host control plane, for example `http://127.0.0.1:3774`.

## Lifetime and revocation

The bearer expires with its session, at most 30 days after `prepare`. Enrollment
records are permanent, and a new bearer needs a new `--enrollment-id`, which
changes the native context the control plane must enroll. Revoke a bearer with
`t3 auth session revoke <session_id>`; the native API then rejects it.

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
