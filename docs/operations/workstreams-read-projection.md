# Native Workstreams read projection

The read projection requires an explicitly configured encrypted native home and a
currently admitted caller with `workstreams:read`. Reads never create keys or
enroll sources. Missing keys, fenced imports and revoked grants fail closed.
The home registry identity is an identifier; it is never a network destination.

Mount the domain layers with the existing native registry, SQL, key and admission
resources. HTTP, MCP and CLI entry points use `AuthorizedReads`; export uses
`ReadExport`. Responses must retain `Cache-Control: private, no-store`.
These source modules alone do not register routes or advertise a capability.

Members require the `MemberReadAdmission` and `HomeReadPort` adapters supplied by
the forwarding owner. Without them reads return unavailable. A member forwards to
its fixed home, revalidates admission and checks the returned identity and
generation. Closed v1 reads require complete live home data. A read projection
may describe cache data only with stale provenance and non-complete coverage.
No source-local observations or Control Plane fallback supply authoritative data.

The rollup-summary route requires `RollupSummaryRead`. Its implementation owns
authorization before and after reading and verifies the summary digest. An
unconfigured producer returns `provider_unconfigured`; the projection does not
generate rollups.

A projection page combines Workstreams, visible active membership counts,
appearance, lifecycle and source coverage from one home transaction. Registry
coverage describes the registry snapshot, independently of source attestation:
an orphan stays unmapped, missing evidence stays unknown, and only matching
native provider evidence with an observed time can be current. Counts use that
same registry context and observation time. Cursor invalidation preserves the
existing owner, principal, route, generation, registry version and limit fences.

Exports retain a single in-memory snapshot, bounded to 10,000 Workstreams and
8 MiB of projection material. JSONL pages are bounded to 100 items and 16 MiB
total emitted page bytes. The existing registry loader first materializes the
owner's state; these caps bound retained projection material, not that loader's
initial allocations. Each page contains the envelope, counts, offset and
items. The final trailer contains `complete`, `registry_version` and SHA-256 of
the exact UTF-8 bytes of all preceding lines, including their newline bytes.
Object keys follow the frozen snapshot canonical JSON encoder. Unsupported
canonical values fail before streaming; this is a read export, not a transition
or restore archive.

Admission, key availability, grants and the home version are rechecked before
every page and the trailer. Revocation, mixed versions, failure or cancellation
terminate the stream without a complete trailer. Consumers must require the
trailer and verify its digest before treating an export as complete. No plaintext
protected content is persisted by these modules. Client caches remain memory-only.

Two-server forwarding, transport registration and platform acceptance require
their own integration checks; local projection tests do not establish them.
