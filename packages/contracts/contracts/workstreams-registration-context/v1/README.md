# Workstreams registration context 1.0.0

Authority effect: none. This additive family leaves Workstreams v1, placement and native-provider manifests unchanged. Native and registry consumers pin identical schema and synthetic fixture bytes. Manifest paths resolve relative to this family directory.

The signed registry route is GET `/workstreams/v1/t3/registration-context`; Jones exposes GET `/api/workstreams/registration-context`. Neither route accepts a body, query or caller-selected identity/source. The empty Request definition is the structural selector guard. Responses are bounded to 8,192 bytes, use `Cache-Control: no-store`, and decode within JSON depth 10. Existing typed HTTP failures carry authorization and transport errors; this family adds only the closed ready response.

Read the current durable Workstreams read grant and source bindings. Intersect active bindings with configured descriptors, exposing at most one T3 thread source and one GitHub pull-request source. An empty sources array can report unconfigured native providers while list/create metadata remains independent. `authorization_revision` carries the durable grant version; server and registry versions retain the existing Workstreams fences.

T3 describes the native-provider protocol and actual Jones repository/sha/tree build. GitHub omits those fields. Account provenance is not account scoped. Git object IDs require exactly 40 lowercase hexadecimal characters, including rejection of trailing newlines. The response exposes no origins, credential locators, headers, session IDs, native IDs, thread content or private fields.

Schemas prove closed structure, kinds, scalar bounds and provider cardinality. Transport evaluates current owner/principal/grant equality and active source-binding/configured descriptor/native protocol/build equality. Successful mismatch fixtures are structurally valid and must be rejected by that evaluator; decoding supplies no authority.
