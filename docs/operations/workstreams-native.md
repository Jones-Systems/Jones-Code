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

The optional source session ID identifies an existing source-issued enrollment;
it is not a token. Missing, expired or revoked enrollment stays unconfigured.
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
Linked calls do not gain native authority from routing; unsupported peer admission
continues to refuse. Scheduled provider runs use the same authenticated agent
origin and caller limits; a tool body cannot select trusted queue origin.

Authenticated clients can explicitly query the selected home or member at
`POST /api/workstreams/native/v1/read` using the existing closed
`workstreams-native-home-read-result/1.0.0` request. It returns the route-specific
value, native envelope and authorization revision; continuation pages retain one
home observation and require current permission on every request. Placement v2
requires the explicit `placement_contract` field. Missing native support refuses
without a legacy fallback. Responses are private and not cacheable by HTTP caches.

A selected home or member publishes the optional native capability after its
authorized read and placement-v2 services are composed. Configuration alone does
not advertise support. A member's home-link status reflects fresh local link and
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
