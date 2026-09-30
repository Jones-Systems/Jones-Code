# Provider runtime identity

Routing settings describe what the user requested. They cannot establish which
backend, model, account, or service tier actually served a runtime. Keep that
request separate from provider observations, including when the values agree.
An absent identity in historical sessions means no evidence was recorded;
`unknown` and `unavailable` must never be filled from settings, launch arguments,
model catalogs, or authentication metadata.

Only provider-native evidence can create an observation. Codex's typed thread
start/resume response reports its model, backend, and sometimes service tier;
a null tier is unavailable. A native Codex reroute changes only the observed
model. Claude's SDK `system:init` reports only the model. None of these events
safely binds an account, and other adapters currently provide no attestation.
Adapter-generated turn-start model metadata is requested configuration.

Every launch receives a generation before it starts. Events retain the emitting
runtime's generation and provider instance; looking up the latest session while
emitting an old event can falsely attest a replacement runtime. Ingestion requires
an exact driver, instance, and generation match. A live model change or a change
to the explicit `serviceTier` option clears old observations while retaining the
running process's generation, so subsequent native observations remain
correlatable. The legacy `fastMode` alias is not normalized into the requested
service tier; changing that alias alone does not invalidate a prior observation.

Startup events wait until binding succeeds. Recovery publishes its new generation
boundary before releasing buffered observations. Failed or interrupted launches
discard their buffered events and restore the previous correlation. Session
projections persist the optional identity and expose it consistently through
snapshot, shell, and detail queries; historical null storage stays absent on the
wire. The fork migration ledger owns this additive column independently of the
upstream migration sequence.
