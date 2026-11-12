# Workstream operations 1.0.0

Authority effect: none. This additive Jones-owned family defines requests,
results, setup observations and HTTP payloads. It grants no runtime, deployment,
registration or credential authority. Existing frozen families are unchanged.

## Corpus and validation

Paths in `manifest.json` are relative to this directory. File hashes cover exact
bytes. The manifest SHA-256 hashes sorted `<sha256>  <relative-path>\n` records,
excluding `manifest.json`. Consumers pin this digest and the producer revision.
The three JSON Schemas supply structural validation; consumers must additionally
apply the semantics below and reject the negative entries in `semantic-cases.cases`.
The separate `behavioral_scenarios` collection documents runner and queue
requirements; it is not executable decoder conformance. JSON decoding and corpus
hash checks do not test retry, cache, sealing or interleaving behavior. Fixture `schema`
names identify `$defs` in the corresponding schema document: request, submit/read
inputs and conflict errors in operation-request; result, submit/read outputs in
operation-result; setup status in setup-status. All objects are closed, including
nested objects, and all listed fields are required unless explicitly optional.

IDs, CommandId, names, progress, lifecycle, sort order, registry versions and
UTC timestamps retain Workstreams v1 validation. String bounds use UTF-16 code
units as in the existing TypeScript contract; JSON Schema length checks alone
are preliminary for non-BMP text. Timestamps must also be real calendar dates.
No coercion, default insertion or unknown-field stripping is permitted.

## Requests and envelopes

`kind` is `set_primary` or `create_workstream`. Operation IDs use exactly the
existing CommandId schema. Queue origins require `request_id`; other origins
(`app`, `cli`, `agent`) forbid it. Queue `set_primary` requires
`expected.kind = unassigned`. Create fields reuse the exact CreateWorkstreamAction
schemas, with closed progress objects. No name normalization is performed.

Submit is `{request, wait_ms}` with an integer wait of 0–3000 inclusive; its
response is the operation result directly. Request SHA-256 covers canonical JSON
of `request` alone, never the wait or envelope. This corpus does not redefine the
gateway's canonical JSON algorithm. Read selects exactly one of `{operation_id}`,
`{thread}` or `{open: true}`. The 409 payload is
`{code: operation_id_conflict | thread_operation_in_progress, open_operation_id?}`;
the optional ID uses CommandId. Existing environment 400/403/500 errors remain
transport-owned and unchanged; this family introduces no replacement tags.

## Results and evidence

Every result includes identity, state, reason, detail, observed, result,
poll_after_ms, children and both timestamps. Absent evidence is explicit `null`,
never an empty string or fabricated ID. `updated_at` cannot precede `accepted_at`.

- `in_progress`: reason/detail/result null; poll_after_ms integer 1–60000.
- `reconciling`: reason `membership_effect_unknown`, `create_effect_unknown` or
  `verification_pending`; detail/result null; poll_after_ms integer 1–60000.
  Create accepts only `create_effect_unknown`; set-primary accepts the other two.
- `committed`: reason/detail/poll_after_ms null; result is the receipt-derived
  `{workstream_id, membership_id, registry_version}`. Create requires a workstream
  ID and null membership ID. Set-primary requires a membership ID; a null
  workstream ID means unassign and that membership ID identifies the removed
  membership. Assign/move have both IDs. Registry version is from the receipt.
- `already_satisfied`: set-primary only; reason/detail/result/poll_after_ms null;
  observed is required and non-null.
- `conflict`: set-primary only, reason `precondition_failed`, non-null observed;
  detail/result/poll_after_ms null.
- `rejected`: reason `foreign_thread`, `thread_not_found`, `registry_mismatch`,
  `permission_denied`, `workstream_not_found`, `identity_conflict`,
  `verification_rejected`, or `registry_rejected`. Only registry_rejected has
  non-null detail: `{code}` using the existing Workstreams error-code enum.
- `unavailable`: reason `not_configured`, `offline`, `registration_unavailable`,
  `placements_not_ready`, `contention`, `backpressure`, `verification_pending`,
  `verification_unresolved` or `destination_scan_bound`. Only
  registration_unavailable has non-null detail: `{availability}` with one of the
  eight reasons below. Rejected/unavailable have null result and poll_after_ms.

Observed is null, `{kind: unassigned}`, or `{kind: primary, workstream_id, name}`.
Create always has null observed because it has no thread. A committed assignment
may have null observed or a different later placement: the receipt describes the
operation's effect, while observed describes a read-back. Neither overrides the
other. Children are diagnostics only: at most 16 unique command IDs, exact existing
Workstreams CP operation names, and outcome `pending`, `committed`, `rejected` or
`unknown`. Unknown includes unresolved receipts and transport outcomes; it is
not evidence of rejection or absence. No child-ID derivation is defined here.

## Retry, cache and sealing guarantees

These are behavioral requirements, not new wire fields or enum values. The
classes below classify existing results; CONFIG is a name for the listed subset
of failures, not a result state. They require runner and queue conformance beyond
JSON validation. No migration or journal is introduced.

- **Settled:** `committed`, `already_satisfied`, `conflict`. Only settled results
  are replayed on a submit with the same operation ID and identical request
  bytes. Committed is final and sticky: after eviction or restart a fresh scan
  must recover its receipt. Already-satisfied and conflict replay while retained;
  after eviction or restart they are decided again.
- **CONFIG, unsealed:** `rejected(foreign_thread|registry_mismatch|permission_denied)`
  and `unavailable(offline|not_configured|registration_unavailable)`. Submit runs
  the same operation again. These results do not permit releasing the target or
  starting a new operation. They skip the seal check, but must adopt a committed
  receipt when the Control Plane (CP) is readable. They claim only that no commit
  was seen at the closing scan, if one ran.
- **Sealed failures:** every other `rejected` or `unavailable`. Return these only
  with the seal proof below. A submit runs the same operation again, rather than
  replaying the failure. The caller may release the target or start a new
  operation; after `contention`, it must start a new operation.
- **Open:** `in_progress`, `reconciling`. Submit joins an existing run for at most
  `wait_ms`; a sealing operation runs again. Keep the operation ID and target.

### Recovery and seal proof

For every attempt, read registry version V before scanning the operation's
membership (`mem`) or create (`cre`) receipts in index order, stopping at the
first absent index. Only verified CP `not_found` proves absence; unknown reads
cannot establish a settled non-committed result or a seal. Adopt any committed receipt,
even after a later owner move or qualification failure; read-back only fills
`observed`. Use the first absent index without skipping, and plan with V, never a
version read after the scan. Rejected receipts consume indices. Resubmission of
an unknown child uses identical retained bytes without a local version or
generation check. Neither a local refusal of that resend nor receipt absence
alone proves no effect.
For an absent receipt, a witness version read before that receipt must exceed
the child's planned version to prove no effect. A rejected receipt also proves
no effect. Resolve idempotency conflicts through receipt reads.

A run is sealed only when its closing scan read the CP witness version V_c,
then receipts through the first absent index n*, no child sent by this process
for the operation is unresolved, and at least one condition holds:

1. **S1:** n* is higher than the first absent index seen on this process's first
   scan of the operation, or no index is absent.
2. **S2:** V_c exceeds V_epoch, the version from this process's first fresh
   gateway read.
3. **S3, set-primary only:** this process has seen this operation's `reg-1`
   absent through verified `not_found`; the closing scan reads it when needed.

If a sealed-class failure cannot be sealed, return
`reconciling(membership_effect_unknown)` for set-primary or
`reconciling(create_effect_unknown)` for create, with `poll_after_ms: 30000`.
Every 30 seconds the sealing phase reruns only the closing scan and seal check,
never a new attempt. A committed receipt wins; otherwise a proven seal returns
the previously decided failure. Sealing remains open and retained but releases
the per-thread R8 lock, so other operations may run; any registry commit supplies
S2 for the next check.

R8 requires a new server process's first CP read to occur only after the previous
process exits. Overlapping processes still have at most one state-changing
commit per operation, but invalidate seals. Record each child unresolved before
sending it. First-absent index allocation also applies to registration (`reg`)
and verification (`ver`); their receipts never finish the operation. Set-primary
may send `mem` only while holding this operation's `reg` receipt.

For `already_satisfied`, `conflict`, and sealed failures, no membership or create
attempt of this request from any process has committed or can commit. Registration
and verification commands may still have committed. For set-primary decisions,
observe between V and a second version V'; restart if V' differs. The proof is
request-specific: possible actions are attach/reattach for expected unassigned
or any, plus move/remove from the expected source (any source for expected any).
Attach/reattach require no open primary; remove requires its observed membership
M open; move requires M an open primary. Every action possible for this request
fails its preconditions in a state where the planner returns conflict or
already-satisfied. With E ≤ V under R8 and V' = V, these decisions are sealed.
Queue requests expect unassigned and can only attach/reattach; a conflict can
therefore release the slot even with an exited process's attach still in flight.

### Caller and queue bindings

After open or CONFIG results, callers retry the same operation ID. A new ID is
allowed only after settled or sealed results, and required after contention.
Inline create retains its ID per (registry, name) until that point.

The queue binding comprises target thread, operation ID and identical request
bytes rebuilt from sealed intent. Change it only after a settled or sealed
result; increment `attempt` only when starting the next operation.

- Committed/already-satisfied mark assignment satisfied and continue the queue.
- Conflict on an existing target holds `assignment_conflict(observed)`; after
  five minutes or `retry-assignment`, start the next operation.
- For flexible slots, conflict or rejected thread_not_found,
  verification_rejected or identity_conflict excludes and releases the slot
  under existing preconditions, then selects another.
- Other rejected results, including CONFIG, hold `assignment_rejected:<reason>`;
  only `retry-assignment` resubmits, using the same operation.
- Unavailable contention backs off and starts the next operation. Other
  unavailable results, including CONFIG, hold `assignment_unavailable:<reason>`
  and retry the same operation after 60 seconds, two minutes, five minutes, then
  every 15 minutes.
- Open results, unknown transport outcomes, null operation reads and 409
  thread_operation_in_progress hold `assignment_pending` and resubmit the same
  operation at `poll_after_ms`, or after five seconds. Joining makes no CP call.
- HTTP 404 holds `assignment_unsupported` and resubmits the same operation every
  15 minutes. 409 operation_id_conflict holds
  `assignment_rejected:operation_id_conflict` without retry.

Until settled or sealed, keep the request and thread fenced. Owner cancellation
stops submits and releases the fence; a later commit stands. Reusing that thread
requires a new expected-unassigned operation that will observe that commit.
Without cancellation, at most one membership commit occurs per queue request,
and never on a released slot: a new operation or slot requires the prior seal.

## Read responses

Finished operations are retained for 24 hours, up to the newest 1,000. Open and
sealing operations remain until finished. Reads answer from memory only with
the latest run; they never refresh a retained failure. A retained failure is
accurate only as of its closing scan; only a new submit refreshes it.

Operation lookup returns `{operation: Result | null}`; null means no retained
operation, not proof of no effect. Thread lookup returns
`{assignment, open_operation: OpenOperation | null}`. Assignment is one of:
`{kind: primary, workstream_id, name}`, `{kind: unassigned}`, `{kind: unverified}`,
or `{kind: unavailable, reason}` using the unavailable reasons above. Unverified
and unavailable expose no asserted placement. These reads never verify or write;
expired attestation is unverified. Open listing is `{operations: OpenOperation[]}`,
at most 100 entries with unique operation IDs. Open operations permit only
in_progress/reconciling. Finished results cannot appear in either open field. A thread lookup
can carry only a set-primary open operation; create has no thread.

## Setup status

Setup is a cached observation (10-second service cache), not a health or authority
promise. It contains no secret values or credential expiry.

- Activation states are the current `enabled`, `disabled`, `invalid` states.
  Missing keys are unique names from the eight existing activation keys:
  T3_WORKSTREAM_CONTROL_PLANE_URL, OWNER_ID, PRINCIPAL_ID, KEY_ID, SIGNING_SECRET,
  AUTHORIZATION_REVISION, REGISTRY_BINDING and REGISTRATION_CONTEXT_VERSION (each
  with the T3_WORKSTREAM_ prefix). Enabled requires an empty missing-key list.
  Optional keys are reported missing only when the selected setup requires them.
- Registry is `ready`, `not_configured` or `unavailable`. Ready requires both IDs
  and a boolean can_write. Other states permit null or known IDs but require
  can_write false. Readiness is not inferred from configured IDs.
- T3 source is `{state: ready, reason: null}` or `{state: unavailable, reason}`.
  Reasons are exactly descriptor_absent, build_unavailable, contract_mismatch,
  source_mismatch, authority_mismatch, generation_mismatch, authority_fenced and
  authority_unavailable.
- Placement readiness is `ready`, `trust-provider-required` (existing placement
  values), or `unavailable` when no placement readiness can be established.
- Operations.open is a nonnegative safe integer count, not the 100-item read cap.
- Next steps contain at most three unique codes, paired exactly with repository
  documentation paths: configure_activation → docs/operations/workstreams-activation.md;
  enroll_native_source → docs/operations/workstreams-native-enrollment.md;
  verify_reference → docs/operations/workstreams-verification.md. Callers resolve
  these repository paths against their documentation base; arbitrary URLs are
  rejected.
