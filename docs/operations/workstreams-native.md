# Native Workstreams home

A native registry has one writable Jones home for an owner. Thread environments
remain responsible for thread identity, caller admission and settlement. The home
holds membership; it never writes a membership copy into the thread source.

Select the home and key provider explicitly. The non-secret configuration contract
is `<baseDir>/workstreams-native.json`:

```json
{
  "mode": "native-home",
  "registry": {
    "registry_instance_id": "11111111-1111-4111-8111-111111111111",
    "owner_id": "owner-example",
    "canonical_origin": "https://11111111-1111-4111-8111-111111111111.workstreams.invalid",
    "home_environment_id": "environment-example",
    "server_generation": 1
  },
  "bootstrap": {
    "home": "/absolute/selected-home",
    "database": "/absolute/selected-home/userdata/statev2.sqlite",
    "key_provider": "server-secret-store",
    "source_session_id": "existing-source-issued-session-id"
  }
}
```

The canonical origin is an identity, not a network destination. Configuration alone
does not initialize or activate a home. Startup validates this selection against the application's already selected SQL
resource and environment-owned secret store. Both paths must be absolute and
match the server configuration; the native feature opens no second database. Native and legacy activation together
fence registry access unless the selected mode is explicitly `legacy`. Missing
keys, incomplete imports and fenced authority refuse access; none selects the
legacy transport as a fallback. A selected member requires its genuine bound enrollment,
original SQL resource, existing keys and current owner qualification. Missing composition
stays fenced; configuration alone never grants home access.

Initialization verifies explicitly provisioned home and local content keys and
the registry index key before creating registry metadata, its initial grant and
singleton binding. Reads never create keys, a home or source enrollment. Preserve
registry UUID, canonical origin, owner, principal and generation when importing
existing history. A new empty registry is not a migration of an existing registry.
Do not select live keys, initialize live state or change authority as part of a
source smoke test.

The optional source session ID is the AuthSession ID in the source-setup request's
`enrollment.session.session_id`; it identifies an existing source-issued enrollment
and is not a token. Missing, expired or revoked enrollment stays unconfigured.
Ordinary startup never reserves an enrollment or creates a source binding. New
homes need an explicit source-binding/enrollment procedure before thread
verification and settlement are available. Existing imported bindings must still
match the current source identity and generation.

A source must supply actual admission and current store-authority evidence for
this registry, home and generation. Missing source or GitHub providers remain
unconfigured. A successful registry read does not prove that a thread can be
verified or settled. Native setup does not request Control Plane enrollment.

Keep the original operation ID and request after an unknown result. Receipt
lookup distinguishes a retained result, proven absence in a complete lineage,
and unknown. Missing keys, an incomplete archive or a generation mismatch cannot
prove absence. Open operations and external reservations must be reconciled at
their original destination; never retarget them or dispatch settlement blindly.
Retained terminal records have no automatic deletion path.

Ordinary local `workstream create`, `assign` and `unassign` commands retain one
bounded Workstreams CLI client session in the selected server. Later `status`
and identical retries use that same current client; the token is held only in
memory. Its original expiry is not extended. Revoking that client in Access or
letting it expire also removes its permission to inspect or retry its accepted
operations. A new client does not inherit them. Read-only commands alone create
only a temporary read session. Keep the same base directory, operation ID and
exact request when recovering an unknown outcome.

Qualified member forwarding also accepts raw linked-PR refresh commands. The home
uses its configured GitHub observer for the reference's original source and
locator; it does not relay the request through another source. Missing GitHub
access leaves the accepted intent pending without a provider fallback. Recovery
uses an encrypted retained observation before another idempotent PR read, and
fresh caller, session, grant and source-binding checks precede atomic receipt and
parent completion. A PR observation does not declare workstream completion.
Raw supplied-proof forwarding remains unsupported.

The placement POST retains its existing body. Omit the placement contract header
for v1. The exact header
`x-t3-workstreams-placement-contract: workstreams-t3-placement/2.0.0` requests
native provenance, including home generation and source authority namespace and
store generation. Unsupported versions are rejected. Historical references
without verified native attestation must be verified before producing a native
placement proof; historical CP evidence is not relabeled native.

A qualified selected member serves this same explicit v2 HTTP route through its
original authenticated caller and bound home. It completes coherent bounded pages
and preserves native proof fields; local trust still comes independently from the
member server's authority provider. Changed home access, inconsistent pages, expired
proofs or changed trust refuse the whole load. The default v1 route is unchanged
and does not strip native proof to force compatibility.

The explicit admin command is:

```sh
t3 workstream registry init --home /absolute/selected-home --key-provider server-secret-store --request '<registry-init JSON>'
```

The selected home must already exist. Init uses its standard
`userdata/statev2.sqlite` and `userdata/secrets` paths and performs normal database
migrations only as part of this explicit command. Provision both content keys and
the registry index key before init. It does not provision keys or enroll a source.
Ordinary startup reads never initialize a registry.

If setup reports `explicit_selection_required` or `database_mismatch`, correct the
explicit paths and provider. For `key_unavailable`, restore the selected existing
keys; do not initialize a replacement registry. For `binding_mismatch` or
`home_unavailable`, reconcile the selected home, binding, import state and
generation. `authority_fenced` requires resolving the deliberate fence or
conflicting legacy activation. None of these repairs authorizes a live transition.

Native callers use current HTTP session scopes, MCP authentication and caller
limits, or an application-owned trusted CLI context. Request bodies cannot select
another origin or actor. A qualified member can read the home through its
request-authenticated public Gateway. Its session and appearance views expose only
the intersection of current home permission and the original source caller's ceiling. The public operation service forwards create and primary-placement
requests through the selected member owner, preserving the original caller, ID,
request digest and bound home. Its reads expose only that caller's accepted
operations after fresh home access checks. A null operation is a local unknown,
never proof of home absence or permission to redispatch. Setup reports current
home access; thread assignments remain unverified until native placement proof
consumers are qualified. The frozen operation codec uses the bound native registry UUID; the Gateway
session uses its canonical-origin identity. Neither selects a transport destination. Public
registration context observes the actual current home grant and qualifies the member's
source-issued enrollment against the source provider's current authority and build.
A historical enrollment is an association, not home permission. Missing support
refuses without falling back to a different registry or protocol. Public
Gateway commands and appearance writes use durable source forwarding. Receipt reads
and retained resubmissions require the original caller's accepted raw intent and
look up the original home only. Missing intent, unavailable lookup and unknown
outcomes do not prove absence or authorize dispatch; retained request bytes stay
unchanged. Native placement v2 keeps its separate negotiated
route; the frozen v1 Gateway cannot erase native proof fields to accept it. Linked
metadata requests remain refused. Scheduled MCP calls retain their authenticated agent origin; a queue origin
requires separate trusted queue-request evidence and is not accepted from a body.

The six public Workstreams MCP tools accept the standard optional
`environmentId`: omit it for the current server or name an explicitly linked
server. The existing MCP router checks the original caller and sends one call with
its narrowed limits. The selected server still owns thread qualification and
native admission; a body's thread environment never selects a route. A denied or
unavailable peer never falls back locally. Retain the same operation ID and
request at the original selected destination after an unknown result.
A public `set_primary` call from the bound home's authenticated native link may
place a thread owned by the selected member. The member retains that incoming
link session and the original narrowed limits, qualifies its own target thread,
and attaches fresh source evidence. The home admits only that exact high-level
placement through its private atomic-operation proof and current SQL grant.
Lost replies recover by looking up the same accepted intent; they never authorize
redispatch. A local agent whose calling thread inherits that same bound-home
session may place only that calling thread. Its original run, provider session
and credential remain mandatory through replay and recovery; replacement runs
cannot inherit an accepted intent. Other linked sessions, metadata, appearance
and raw commands remain refused. The generic hop guard is unchanged.
Scheduled provider runs use the same authenticated agent origin and caller limits;
a tool body cannot select trusted queue origin.

Authenticated clients can explicitly query the selected home or member at
`POST /api/workstreams/native/v1/read` using the existing closed
`workstreams-native-home-read-result/1.0.0` request. It returns the route-specific
value, native envelope and authorization revision; continuation pages retain one
home observation and require current permission on every request. Placement v2
requires the explicit `placement_contract` field. Missing native support refuses
without a legacy fallback. Responses are private and not cacheable by HTTP caches.

A selected home or member publishes the optional native capability after its
authorized read and placement-v2 services are composed. Configuration alone does
not advertise support. Clients select legacy grouping when the native capability
is absent or explicitly legacy. A native home preserves the legacy read shapes;
valid current-generation headerless commands remain supported, stale-generation
commands refuse without writes, and command response negotiation is request-local.
A member's home-link status reflects fresh local link and
bound-enrollment validation; missing, expired or unknown links block native client
reads. An available link does not prove peer reachability or current home
permission. Every request still requires current admission. Rollups are not
advertised. Setup uses the existing closed status shape without CP enrollment
instructions. Recovery requires the owning Effect worker and current
retained-caller admission; constructing a service is not recovery authorization.

Use isolated synthetic state for qualification. Verify both command entry paths,
restart replay and placement negotiation with Control Plane access denied before
claiming a native runtime. Source changes alone do not install, switch or adopt
that runtime. Existing legacy installations continue to use
[legacy activation](./workstreams-activation.md) until a separately authorized
transition changes their authority.

A selected home or member exposes its authenticated read export at
`GET /api/workstreams/native/v1/export`, with optional `page_size=1..100`.
It requires the current orchestration read scope and streams the existing JSONL
format as `application/x-ndjson`. Retain the original UTF-8 bytes through EOF:
only the final complete trailer and its matching digest establish a completed
export. Revocation, drift, interruption or cancellation leaves the stream incomplete.
A mode without a qualified selected exporter refuses with `workstreams_native_unavailable`; it never falls back to a local
registry or legacy export. Capability publication belongs to the selected runtime;
composing an exporter alone does not advertise support.

Use `t3 workstream export --home /absolute/existing/server-home` for a selected
local home or member, optionally with `--page-size 1..100`. It requires the already running
server, existing database and signing key. It creates only a five-minute
orchestration-read session and revokes that session when finished; it never
starts a server, migrates storage or creates a key. The CLI checks the original
JSONL bytes, complete trailer, digest and current read binding before writing
anything to stdout. Failed downloads or validation produce no export output.
An interrupted or failed stdout write can leave partial bytes; only EOF and the
matching trailer establish completion. Redirects, remote origins,
legacy fallback and automatic retries are unsupported.

Member export requires the independently negotiated fixed home-export tool. It
pins the original local caller and outbound link generation, checks current home
read permission before and after the transfer, validates the original complete
JSONL and digest, and releases those exact bytes as one bounded result. It holds
no disk cache and creates no local registry or home grant. Missing tool support,
changed permission, incomplete bytes or home drift refuse without output or a
legacy fallback. The fixed result is bounded to 16 MiB plus the completion trailer.

## Go-live runbook

This sequence starts a fresh registry; it does not import legacy data. For an
import, use [transitions](workstreams-native-transition.md). These steps are
source-derived and have not been run against a live host. The isolated Linux
[native-home fixture](../../apps/server/src/jones/workstreams/testing/nativeHomeFixture.ts)
exercises the setup services with synthetic launcher metadata; it does not qualify
an installed runtime or a complete UI journey.

### Native home

Use a selected home with an established environment ID and qualified launcher
runtime. Authority enrollment checks that runtime and its current build identity;
an unstamped source checkout is insufficient. An isolated development candidate
needs both a preview version and a matching `jonesSource` stamp. Synthetic fixture
receipts must not be used to qualify a real installation.

1. `t3 workstream authority enroll --base-dir <home>` records the home's native
   placement authority.
2. `t3 workstream registry keys init --home <home> --key-provider
   server-secret-store --request '<registry-init JSON>'` creates the content,
   local and index keys in `<home>/userdata/secrets/` from random bytes. It
   refuses if any of them exists and prints only key names.
3. `t3 workstream registry init` with the same flags and request creates the
   registry, its keys metadata, principal, grant and binding.
4. Move `<home>/workstreams/activation.env` aside and unset the
   `T3_WORKSTREAM_*` control-plane variables. Either one fences native startup.
5. Write `<home>/workstreams-native.json` with `"mode": "native-home"`, the
   registry from the init request and `bootstrap.home`, `bootstrap.database`
   and `bootstrap.key_provider`. Start the selected server so SessionStore
   initializes its signing key before standalone source setup can issue a session.
6. Stop the selected server before standalone source setup. Its `plan` and
   `readback` require a separately quiesced rollback-journal database, with no
   WAL/SHM sidecars; stopping a WAL-mode server alone does not establish that
   condition. Run `t3 workstream registry source-setup plan`, `apply` and
   `readback` with one saved `jones-code.native-source-setup/v1` request; see
   [source setup](workstreams-native-source-setup.md). Preparing a live store
   for this boundary remains unrehearsed.
7. Set `bootstrap.source_session_id` to the AuthSession ID that source setup
   issued (the request's enrollment session ID) and restart.

### Members

A member has no registry or home keys copy. Before peer setup it needs:

- `t3 workstream authority enroll` on the member;
- current peer links both ways: `t3 environment link <url> --pairing-code
  <code> --access full-access`, using a code from `t3 pair` on the other side;
- its own local key for the home registry. Run `t3 workstream registry keys
  member-init --home <member-home> --key-provider server-secret-store --request
  '{"registry": <home registry>, "local_key_id": "<id>"}'` once, with the same
  registry object the member's `workstreams-native.json` will name. It needs the
  member's existing `userdata/statev2.sqlite`, creates
  `userdata/secrets/workstreams-content-key-<id>.bin` from random bytes and the
  active `local` key row, and prints only the key name. By default it refuses
  an existing registry, key row or key file. Without this key, member setup and
  startup stop with `key_unavailable`. Back the file up; never generate replacement
  bytes under an existing key ID.

If an interrupted attempt left a key file, repeat the exact `member-init` request
with `--resume`. Resume validates the existing 32-byte key and either inserts its
missing member row or confirms the matching row; it does not replace key bytes.
A missing file for an existing row reports `key_file_missing`: restore the
original file rather than rerunning initialization with a new key.

When an attempt created a key file and its transaction reports failure, the
command checks whether its row committed. A matching committed row succeeds and
keeps the key. A confirmed rollback removes
only a file created by that attempt. If readback cannot establish the outcome,
`outcome_unknown` keeps the file; retain the exact registry and key ID and use
`--resume` once the database is readable.

Run peer setup on the member, against the member's own running server.
`--source-environment-id` is the member and `--home-environment-id` is the home.
The member reaches the home over its outbound link.

```sh
t3 workstream peer-setup prepare --home <member-home> --setup-id <id> \
  --registry-id <uuid> --owner-id <owner> --home-environment-id <home-env> \
  --home-generation <n> --source-environment-id <member-env>
t3 workstream peer-setup apply  <same flags> --request-sha256 <from prepare>
t3 workstream peer-setup status <same flags> --request-sha256 <from prepare>
```

After `status` reports `bound`, write the member's `workstreams-native.json`
with `"mode": "native-member"` and the home's registry, then restart. After an
`unknown` status, use `peer-setup resume` with the same flags. Repeating
`prepare` with the same setup ID returns the retained request.
