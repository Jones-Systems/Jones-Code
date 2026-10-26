# Encrypted Workstreams transitions

A transition moves a fenced registry into a single native home. Source delivery
alone does not select a home, acquire keys, read owner data or activate a registry.
Use synthetic isolated state for qualification. Live operations require their
separate operator-approved inventory, custody, fence and recovery scope.

## Prepare the receiving home

Provision the selected content and registry identity-index keys explicitly through
the native key administration route. Import never creates a missing key. Preserve
the original registry UUID, origin, principal IDs and source environment IDs.
Obtain principal/environment mapping evidence from the corresponding source;
an orphaned identity does not become local merely because import runs here.
Unmapped principals remain historical identities without environment access.
Unmapped active sources block activation.

The operator composition uses the
[transition service](../../apps/server/src/jones/workstreams/native/transition/cli.ts)
with J1 persistence. Its thin command adapter accepts `plan`, `apply`, `readback`
and `activate`. Do not serialize the returned plan to a plaintext file: its
snapshot rows remain private in memory. On restart, rebuild it from the same
encrypted input, signed permit, run ID and mapping inputs. The service checks the
durable encrypted manifest before continuing. An installed CLI must mount that
adapter with its own authorized base-directory and encrypted I/O composition.

## Import and activate

1. Fence and drain the source using its own operator procedure. Preserve the
   encrypted export and rollback material within the approved retention window.
2. Plan with the encrypted stream, its selected key ID, the signed header permit
   and the explicitly selected public verification key. A version string alone
   is insufficient: the consumer uses the byte-pinned X1 schema and class map.
3. Apply using the original run ID. Each row, encrypted original and progress
   update commit together. Repeating identical input resumes or returns the
   stored result; changed source bytes or mapping inputs conflict.
4. Read back. The service compares class counts/digests, encrypted originals,
   native destination values and registry generation/version. Partial imports
   remain inactive. The archive distinguishes present, proven absent and unknown;
   an incomplete archive never proves an old command ID unused.
5. Reconcile remaining effects through the original command IDs and their
   owning domain. Saved results precede lookup; unknown settlement is never
   dispatched again. Retained open intents are blocking recovery material.
   When the source carries only a structured command and its digest, the original
   request bytes stay explicitly unknown. J1 receipt completion must not treat
   the empty compatibility body as an executable or original request. Such an
   import stays fenced until the owning recovery path supplies a qualified
   reconciliation; do not manufacture request bytes to match a digest.
6. Activate with an explicit fixed activation ID, plan digest, destination and
   exact owner generations. Activation repeats readback and refuses incomplete
   mappings, open effects and incomplete transfer extensions. It must preserve
   the imported generation and registry version. If the reply is lost, repeat
   the exact original intent; a replacement ID is refused.

The base registry transition guard must permit the state-only
`importing → active` transition without changing CAS values. A receiver whose
migration lacks that integration refuses activation and rolls the transaction
back. Do not increment the registry version to work around that refusal.

Unsupported schemas, duplicate/conflicting keys, cycles, invalid revision chains,
broken history watermarks, signatures, GCM framing, counts or digests fail closed.
A legacy receipt incompatible with the receiving frozen receipt contract is
refused during planning rather than accepted into an unreadable replay archive.

## Reverse and transfer

Fence native writes before preparing a reverse export. Declare representable CP
principals explicitly; native-minted or unprovisioned opaque identities block
reverse planning. The export retains unchanged exact private UTF-8 payloads,
including BOMs and lexical JSON whitespace, and reconstructs changed native rows
and derived history/collection records. A native field without a qualified CP
representation is a blocking error. CP imports into a fresh migrated, fenced
registry and increments the exported owner generations once. Old receipts keep
their original generations and identities.

For Jones-to-Jones transfer, use the separately versioned transfer bundle. It
includes the complete CP projection, original lineage, next generations and the
four Jones extension classes: principal environments, retained archive, import
runs and rollup bindings. The operator signature binds the whole bundle digest
and destination. Import the base snapshot, apply and read back extensions, then
activate the destination. Retain the source's fixed transfer record; mark it
transferred out only against the matching destination readback. Members must
rebind through the cross-environment binding service before writes resume.

Transfer source tests alone do not qualify two-server transfer or member
rebinding. Those require the integrated peer/binding implementation and its
two-server acceptance run. Keep the original fenced source and rollback records
until destination readback and the approved reverse window permit retirement.
