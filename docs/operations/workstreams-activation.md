# Workstreams activation

The server reads one activation snapshot at startup for both Workstreams and
registration-context requests. Existing shell/environment configuration remains
supported: if any required activation variable or the optional registration
version selector or registry binding is defined, including an empty value, the complete environment
group must be valid. Missing values are never filled from a file.

When all activation variables are absent, the server reads only
`<ServerConfig.baseDir>/workstreams/activation.env`. Use the control plane's
`stage-jones` output: exactly one literal assignment for each of
`T3_WORKSTREAM_CONTROL_PLANE_URL`, `T3_WORKSTREAM_OWNER_ID`,
`T3_WORKSTREAM_PRINCIPAL_ID`, `T3_WORKSTREAM_KEY_ID`,
`T3_WORKSTREAM_SIGNING_SECRET`, and `T3_WORKSTREAM_AUTHORIZATION_REVISION`.
It may also contain one `T3_WORKSTREAM_REGISTRATION_CONTEXT_VERSION` assignment
and one `T3_WORKSTREAM_REGISTRY_BINDING` assignment.
The file is data, not shell input: no `export`, quotes, interpolation, comments,
extra keys, or duplicate keys. The URL is a canonical HTTPS root origin, or
HTTP to literal `127.0.0.1` or `[::1]` on the same host.

Keep the resolved base directory owned by the server user and not writable by
its group or others (`0755` and `0700` are accepted). The `workstreams` directory
must belong to that user at `0700`, and `activation.env` must be a regular file
at `0600` owned by that user. No path component may be a symlink. Ancestors must
belong to root or the server user and must not be group/world writable; a
root-owned sticky ancestor such as the system temporary directory is accepted.
The opened file is checked again for owner, mode, identity and size, with a
64 KiB maximum and strict UTF-8 decoding.

An absent file means unconfigured. Partial, malformed, unreadable or unsafe
configuration fails closed as invalid/unavailable without contacting the control
plane or exposing file contents. Correct the configuration through the authorized
provisioning operation, then restart the ordinary server child. Runtime updates
load the same installation file on the next child startup; they do not require
copying credentials into a runtime package, provider environment or settings.
Changes to the file during a running server do not reload activation.

## Registration-context version

The optional activation value `T3_WORKSTREAM_REGISTRATION_CONTEXT_VERSION`
selects the registration metadata contract. Omit it or set `1.0.0` for the
compatibility default; set `2.0.0` explicitly to enable multiple T3 source
descriptors. Stage it with the six required values in the private
`activation.env` file so the selection survives ordinary server-child restart.
Existing six-value files retain their exact format and select v1.

Complete environment activation takes precedence as one group. A selector
alone is incomplete activation and fails closed; it never overrides or merges
with the private file. Empty or unsupported versions, duplicate assignments
and unknown file keys invalidate activation without contacting the control
plane. There is no automatic version fallback or retry with another family.

V2 requires a Control Plane producer that supports the pinned
`workstreams-registration-context/2.0.0` contract and its exact manifest. Jones
pins the contract corpus from producer commit
`0817d23ca2547c5ff8b88b0034e6e2b1de0dd431`. A v2 request to a producer that only
supports v1 fails closed. V1 retains its singleton T3 limit.

V2 metadata permits up to four T3 sources and one GitHub source. Jones selects
its local T3 descriptor using the actual native authority's environment ID,
then checks build, authority namespace and store generation. Clients receive
only that qualified local T3 descriptor plus non-T3 sources; another
environment's matching build cannot substitute for local authority. The native
provider protocol remains `workstreams-t3-provider/1.0.0`.

Source support and a version setting do not establish installed producer
compatibility, native enrollment or live provider health. Installation,
configuration changes and restart remain separate authorized operations;
follow [native enrollment](./workstreams-native-enrollment.md) for enrollment
and running-adapter proof.

Keep credential staging and permission changes within the installation's exact
operational authorization. Do not print the file or copy it into diagnostics.

## Registry identity and loopback transport

Literal-loopback HTTP requires the independently provisioned
`T3_WORKSTREAM_REGISTRY_BINDING` token. Its canonical HTTPS origin identifies
the registry; it never changes the configured transport destination. Jones
validates the token against its pinned contract and the activation owner,
principal and authorization revision. An unbound literal-loopback activation
fails before a request. Legacy unbound HTTPS remains supported.

The token is the control plane's `stage-jones --registry-binding-version 1.0.0`
output, following the six required values and optional registration selector.
Do not construct an HTTPS origin from the loopback address or take a replacement
pin from a response. Provisioning, staging and restart need their own operational
authority. Source support does not designate a real hostname or provision an
installed registry.

The new envelope is independently pinned to producer commit
`562bd5cb51de8410560b63bba9119a104be3bb34`; its
[exact contract corpus](../../packages/shared/src/jones/workstreamsRegistryBinding/contract/spec.md.raw)
retains producer bytes in `.raw` fixtures so formatting cannot change their
digests. It does not change existing Workstreams or native-provider manifests. Jones checks
bounded response bytes and the request-bound MAC before decoding. Only the exact
authenticated pre-dispatch refusal maps to the existing command-not-current
conflict. Missing or invalid proof and lost replies retain unknown outcomes;
recovery reads the original command receipt without resubmission. Receipt GET
may itself enter producer business logic and reconcile state.
