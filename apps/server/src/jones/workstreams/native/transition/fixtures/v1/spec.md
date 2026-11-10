# Workstreams snapshot 1.0.0

Authority effect: none. This source contract authorizes no data access, key
acquisition, export, import, activation or host operation. Fixtures and keys in
`vectors.json` are synthetic and public-safe.

The producer retains the whole registry, including every principal, grant,
binding, receipt, intent, result, audit event, projection, appearance record and
rollup row. No identity is minted or inferred during export. A consumer must
validate the complete stream before activation. Legacy command IDs remain
reserved; an absent row in an incomplete import never proves absence.

## Files and pins

`manifest.json` pins the schema and vectors by SHA-256 of exact file bytes.
`vectors.sha256` pins the exact `vectors.json` bytes using sha256sum syntax.
Consumers pin the manifest bytes separately. A version string alone is not a
compatibility pin. `snapshot.schema.json` is a closed draft-07 schema for each
line, including all SQL columns of all 26 CP classes. `table_keys` and
`class_order` in the manifest define the authoritative row keys and section
order. No optional class may silently disappear.

## Plaintext framing inside encryption

UTF-8 JSON Lines, one canonical object followed by LF per line:

1. Header: `contract`, UUIDv4 `snapshot_id`, producer (system, source revision,
   source schema version), registry UUID/origin, owners (generation, registry
   version, audit high-water), fence epoch/time, export time, number of active
   reservations, and encryption mode/key ID.
2. Every class in manifest order: zero or more `{class,key,row}` objects,
   followed by exactly one `{class,count,sha256}` footer, including empty classes.
3. One `{snapshot_sha256,complete:true}` trailer. No bytes follow its LF.

CP schema version 15 and Jones native schema version 124 are accepted. Any other
source version blocks import until its mapping has been explicitly qualified.
An empty registry still has one registry-identity row and zero owner rows.
Registry identity and every owner context must agree with the header.

All row columns are retained. An encrypted SQL `ciphertext` column is replaced
by three fields:

- `payload`: the **exact decrypted UTF-8 string**, without parsing/reformatting
  its JSON. This preserves whitespace, key order, numeric lexical spelling,
  decimal values and full private intent/result bodies. Consumers parse that
  string separately when converting to domain records. There is no plaintext
  payload log or disk spool.
- `cipher`: `{key_id,revision}`, using the original CP AAD context.
- `ciphertext_sha256`: SHA-256 of the canonical original ciphertext JSON object.

The existing `cipher_revision` column, where present, is retained too.
Appearance receipts use revision 0, appearance rows use their version. External
reservation/result record IDs are `reservation:<command_id>` and
`external-result:<command_id>` with purpose `command_receipt` and revision 0.
Rollup manifest IDs are `<workstream_id>:<manifest_sha256>` and summary IDs are
`<workstream_id>:<rollup_version>`. Other contexts keep CP's existing purpose,
record ID and revision. No content key or credential is part of a snapshot.

Canonical outer JSON sorts object keys by UTF-16 code unit (JavaScript's plain
`sort`, never `localeCompare`), has no whitespace, preserves array order, uses
ordinary JSON string escaping, and permits only safe integers as JSON numbers.
Non-integer payload numbers remain inside the exact UTF-8 string. Negative zero,
non-finite numbers, non-JSON values and unsafe integers are refused.

Rows are ordered lexicographically by their manifest primary-key tuple. Text
components compare unsigned UTF-8 bytes, numeric components compare numerically,
and the boolean singleton compares false before true. Duplicate or descending
keys are invalid. CP queries use explicit `COLLATE "C"` for text keys and UTC
for SQL timestamps. SQL JSON objects are canonicalized after materialization.

`class_sha256 = SHA256(concat(canonical({class,key,row}) + LF))`.
`snapshot_sha256 = SHA256(canonical(header) + LF + concat(class + ':' + count + ':' + class_sha256 + LF))`.
The header has no digest fields to remove. Identical metadata and database state
produce byte-identical plaintext JSONL. Callers must reuse snapshot ID and
export time when comparing deterministic exports. Encrypted bytes vary.

## Encrypted transport

The whole plaintext stream, including the header and manifests, is encrypted.
No public HTTP endpoint exports it. Only the explicit operator composition may
open the repeatable-read database snapshot and acquire keys through injected
ports. An injected operator verifier must validate a permit over the exact
header before any encrypted bytes are emitted. `authorizeSnapshotExport` checks
`header_sha256` and verifies the signature over canonical
`["workstreams-snapshot-export-permit-v1",header]`, using the permit key ID. Missing keys fail closed, without creating a key or using plaintext.

The outer stream is UTF-8 JSONL frames with exactly these keys:
`{v:1,alg:"A256GCM",key_id,index,final,nonce,ct,tag}`. `index` starts at 0 and
increments by 1. `nonce` is 12 fresh random bytes, `tag` is 16 bytes; these and
`ct` use canonical standard base64 with padding. Each data frame holds at most
65,536 plaintext bytes. The last frame has `final:true` and authenticates an
empty plaintext. It is mandatory even for an empty input. Truncation, trailing
frames, wrong key, tampering and out-of-order frames fail closed.

Each frame uses AES-256-GCM and this UTF-8 canonical JSON AAD:
`["workstreams-snapshot-encryption-v1",key_id,index,final]`.
The full decrypted stream's hashes and trailer must also verify before use.
Decryption alone is not snapshot validation. Consumers must neither activate
nor expose partially authenticated plaintext. The test vector's fixed nonce is
for conformance only; production nonces always come from the cryptographic RNG.

## Drain and reverse

The database starts in `legacy`, with epoch 0. The operator's compare-and-swap
transition enters `draining`, blocking application writes and v1 reads on both
listeners. Statement triggers cover INSERT/UPDATE/DELETE/TRUNCATE on all 26
tables, including zero-row sweeps and security-definer projections. A shared
transaction advisory lock drains prior writers before the exclusive transition
commits. A session setting alone never bypasses the fence: the session must
own the fence and must not have assumed the runtime role.

Only explicitly composed reconciliation transactions can write in `draining`.
Saved results complete before provider lookup. Otherwise reconciliation looks
up first; only read-only attestation/PR observation may be repeated. Unknown
settlement is never submitted again. Running rollup claims return to pending
with `fence+1`; old publishers fail their claim fences. Final fencing requires
zero pending/unresolved receipts, active reservations and running rollups.

Reverse import is a source capability for a **fresh, migrated, fenced CP
registry**. It does not overwrite an existing archive. Operators must first
provision representable opaque principals/accounts through their separately
authorized routes; native-minted or unmappable principals block planning.
No principal, account, grant or environment mapping is invented.

The operator uses `plan`, `apply`, `readback`, then explicit `activate`.
Each row and its `(import_run_id,class,record_key)` progress/digests commit in
one transaction. The same source row is a no-op on resume; changed bytes are a
conflict. The applying stream itself is schema/digest checked, so a changed
source between plan and apply cannot activate. Progress contains IDs and
digests only. Partial imports stay fenced. No automatic transaction retry is
used after an unknown COMMIT result.

The reverse importer temporarily bypasses USER triggers inside a locked row
transaction, retaining the authority-fence trigger and all CHECK, UNIQUE and FK
constraints. Historical derived projections are imported exactly rather than
re-derived with changed history order. Immediate FK checks permit restoring
triggers before COMMIT; rollback restores trigger configuration after failure.
Membership uniqueness, cycles, revision chains and high-water are validated.

Each owner's generation becomes its snapshot generation +1; registry versions,
aggregate versions, all principal IDs and event ordering are retained.
Ciphertexts are re-encrypted with the explicitly selected CP key. Readback
verifies every stored row against its atomically recorded target digest and
checks exact counts for all classes. Ciphertext hashes change on re-encryption;
canonical logical payloads stay identical. The audit identity sequence is
advanced past imported events. Activation repeats readback and refuses open
effects, then changes the fence and import state atomically. Restoring source
versions does not grant permission to activate or replace any live authority.

Jones-only transfer extension classes are outside CP reverse import. Jones to
Jones transfer consumers must use their separately versioned/pinned extension
mapping; CP never drops unknown classes to accept a partial reverse snapshot.
