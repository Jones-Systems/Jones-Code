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
  }
}
```

The canonical origin is an identity, not a network destination. Configuration alone
does not initialize or activate a home. Startup must explicitly select the native
runtime and its database and key provider. Native and legacy activation together
fence registry access unless the selected mode is explicitly `legacy`. Missing
keys, incomplete imports and fenced authority refuse access; none selects the
legacy transport as a fallback. Member forwarding requires a separately supported
member runtime.

Initialization verifies explicitly provisioned home and local content keys and
the registry index key before creating registry metadata, its initial grant and
singleton binding. Reads never create keys, a home or source enrollment. Preserve
registry UUID, canonical origin, owner, principal and generation when importing
existing history. A new empty registry is not a migration of an existing registry.
Do not select live keys, initialize live state or change authority as part of a
source smoke test.

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

Use isolated synthetic state for qualification. Verify both command entry paths,
restart replay and placement negotiation with Control Plane access denied before
claiming a native runtime. Source changes alone do not install, switch or adopt
that runtime. Existing legacy installations continue to use
[legacy activation](./workstreams-activation.md) until a separately authorized
transition changes their authority.
