# Workstreams registration context 3.0.0

Authority effect: none. This additive family exposes immutable enrollment
provenance and explicit accepted native profiles. Frozen registration v1/v2,
provider v1 and Workstreams contracts remain unchanged. Schemas and fixtures
establish a contract, not installed routes, credentials or runtime health.

Manifest paths are relative to this directory. File digests cover exact bytes;
the manifest digest hashes sorted `<sha256>  <relative-path>\n` records, excluding
`manifest.json`. Consumers pin both the manifest and accepted producer revision.

Use signed empty GET `/workstreams/v1/t3/registration-context` with version
`workstreams-registration-context/3.0.0` and this family's manifest digest.
Authentication and routing must enforce the exact version/manifest pair and echo
it. Existing HMAC signing binds version, identity, method, target and body digest;
the manifest header is checked through the fixed version-to-manifest mapping.
No body, query, caller-selected identity/source or browser identity header is
accepted. Responses remain bounded to 8,192 bytes and JSON depth 10, with
`Cache-Control: no-store`.

Top-level owner, principal, grant, authorization revision, server generation and
registry version preserve v2 authority semantics. Sources remain the intersection
of exact configured descriptors and current active registry bindings: zero to
four T3 thread sources and at most one GitHub pull-request source. GitHub source
bytes and schema are unchanged. Duplicate `(provider, source_instance_id)` pairs
must be rejected even when generation, namespace, build or policy differ; the
semantic fixtures require rejection after structural validation.

Each T3 source retains its source, namespace, generation, provider, resource and
ID kinds and not-account-scoped provenance. It exposes `enrollment_id`,
`enrolled_protocol` (exactly `workstreams-t3-provider/1.0.0`), `enrolled_build`,
and `accepted_native_profiles`. Accepted profile lists are exactly `[v1]`,
`[v2,v1]`, or `[v2]`, with full `workstreams-t3-provider/1.0.0` and
`workstreams-t3-provider/2.0.0` strings. These lists describe registry policy;
`enrolled_*` describes configured provenance. Neither reports runtime build,
health or credential validity. Origins, credential locators, sessions, native
IDs and thread content are excluded.

V1/v2 requests retain their frozen shapes and limits. Their views omit sources
whose accepted policy excludes v1; they must not truncate an otherwise ambiguous
source set or silently select another source. V3 negotiation and production of
those views require the matching registration application implementation.
