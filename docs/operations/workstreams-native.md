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
legacy transport as a fallback. Member mode is refused until its forwarding runtime is supported.

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
another origin or actor. Linked callers and member forwarding are refused until
J3. Scheduled MCP calls retain their authenticated agent origin; a queue origin
requires separate trusted queue-request evidence and is not accepted from a body.

The current optional native capability is withheld until the complete native read
projection is mounted: its frozen contract requires that read capability. The
placement header can still negotiate the mounted v2 route explicitly. Rollups are
not advertised. Setup uses the existing closed status shape without CP enrollment
instructions. Recovery must be composed with the owning Effect worker and current
retained-caller admission; constructing a service is not recovery authorization.

Use isolated synthetic state for qualification. Verify both command entry paths,
restart replay and placement negotiation with Control Plane access denied before
claiming a native runtime. Source changes alone do not install, switch or adopt
that runtime. Existing legacy installations continue to use
[legacy activation](./workstreams-activation.md) until a separately authorized
transition changes their authority.
