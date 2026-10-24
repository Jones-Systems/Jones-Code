# Workstreams T3 provider 2.0.0

Authority effect: none. This additive family separates immutable enrollment
provenance from the running build. It preserves the frozen provider v1 contract,
request definitions, enrollment records, sessions, scopes and attempt identities.
The schemas and synthetic fixtures define the wire contract; they do not prove
route installation, provider readiness or live execution.

Manifest paths resolve relative to this directory. Each file hash covers its
exact bytes; the manifest digest hashes sorted `<sha256>  <relative-path>\n`
records, excluding `manifest.json` itself. Consumers pin the exact manifest and
producer revision, not just a version label.

## Wire profile

The authenticated endpoints are GET `/api/workstreams/native/v2/context`, POST
`/api/workstreams/native/v2/attestations`, POST
`/api/workstreams/native/v2/settlements`, and POST
`/api/workstreams/native/v2/settlements/lookup`. HTTP bodies use the same
operation-specific request definitions and bytes as v1; context has an empty
request. The operation envelopes in the schema are internal discriminated unions.
Response protocol literals are `workstreams-t3-provider/2.0.0`.

Ready context is `{protocol,state:"ready",enrollment,observed_build}`.
`enrollment` is the closed original v1 Context, including protocol 1.0.0,
enrollment ID and original build. The consumer compares every field with its
configured context. `observed_build` must decode with repository
`Jones-Systems/Jones-Code`; its SHA/tree may differ from the enrolled build.
Non-ready context and mismatched enrollment fail qualification. The semantic
fixtures are structurally valid mismatches requiring consumer rejection.

The 32,768-byte request and response caps, JSON depth 10, no redirects,
5,000 ms metadata timeout, 15,000 ms settlement timeout and 300,000 ms attestation
TTL are unchanged. The bearer method, exact enrolled subject, dedicated three
scopes, session expiry/revocation and before/after authority fences are unchanged.
No scope grants thread content or general command access. Only persisted bindings
whose protocol is 1.0.0 can serve this profile. See the [v1 semantics](../v1/README.md)
for request/result association, rejection and unknown-effect boundaries.

## Durable identity and evidence

The golden vectors pin the exact pre-change Jones Effect Schema serialization.
This is schema field order, **not recursively sorted JSON**. The immutable
Context is projected from the persisted binding and encoded with the v1 Context
codec. The request is encoded with the v1 SettlementRequest codec.

- `requestBytesSha256 = SHA256(raw HTTP request bytes)`; whitespace and key order
  affect this hash even when decoded requests are equal.
- `enrollmentSha256 = SHA256(registry_origin + "\n" + session_id + "\n" + encodedContext)`.
- `nativeCommandId = "workstreams:" + SHA256(enrollmentSha256 + "\n" + encodedRequest)`.

Observed build, response profile and route version never enter these inputs.
Attempts remain keyed by `(owner_id, principal_id, command_id)`. A different
raw request hash, encoded request or enrollment digest conflicts. Persist the
full immutable request and native command ID before invocation, and dispatch-start
before calling the engine. A started attempt is observed, never dispatched again.
Lookup never creates an attempt or dispatches; an unknown attempt stays unknown.

A committed terminal requires an accepted durable receipt and one attributed
settlement event. Receipt command types are `thread.settle` / `thread.unsettle`;
event types are `thread.settled` / `thread.unsettled`, respectively. Every event
has applicationEventVersion 2, the exact native command/thread aggregate, and a
positive sequence. At most 256 events are permitted, contiguous in supplied
order, ending at receipt resultSequence and acceptedAt. Exactly one event is a
settlement event. A rejected receipt requires no events. Missing or contradictory
evidence stays unknown, including after compaction; a lost response authorizes
lookup only. Wire projections omit commandType and applicationEventVersion;
provider observation checks them before projecting the receipt/event.

Result digests retain registry canonical sorted JSON over the immutable
association plus native_outcome. Attestation evidence hashes the v1
Schema-encoded attestation request, attested_at and expires_at separated by
newlines. Golden vectors cover both settlement actions, result digests and
attestation evidence. Their source revision and qualification scope are recorded
in the fixture: pre-change codecs were executed; Jones service replay and
compaction qualification remains an integration check for the provider consumer.
