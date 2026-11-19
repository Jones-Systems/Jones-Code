# Requests inbox and in-app stacks

[Map index and shared recipes](README.md). These journeys describe source behavior;
this lane has no runtime acceptance evidence.

Requests opens at `/requests`. The exact designated Laptop receiver
`8750edf7-486b-41ac-81ce-cd50dfeb71eb` stays first, including an explicit unavailable
section if it is not connected. Display names never select another receiver.
The renderer discovers V2 support through the existing authenticated environment
runtime and switches reactively when its adapter is registered. Unsupported receivers retain
V1 review with its explicit 200-record cap, without access or creation features.
Owner authorization and API support are separate.

Each receiver has one navigation row for Current, Blocked, In flight, Unresolved,
Access, and Done. Current and Blocked count held admissions. Access shows the active
scope count and also lists pending and revoked grants. Done reports the number of
retained completed requests. Load older records extends the selected view's server
page. A changed snapshot revision requires refresh. Unknown outcomes stay outside Done.
The receiving server excludes peer protocol bookkeeping from queues and counts.
An exact link can still open the retained record.

Select a card to show its details alongside the list when space permits. On narrower
layouts, Back to requests returns from the detail view. The row arrow opens the exact
receiver, request and digest. Access arrows open the exact destination thread. An
exact request link must resolve before the page displays decision controls.
Live thread status and elapsed time
use the existing shell helpers, with no fabricated status when the shell is absent.
Reported source-thread context appears in expanded details only.

Approvals default to 1 day where the server explicitly marks the request grantable.
Choose This request only to omit the grant. Scheduled and unrelated operations stay
once. Duration, Deny and Approve use compact controls; dropdowns show the complete
selected value and placeholder. A duration selection in the access detail pane is
local until Update access or Restore access is pressed. Expiry is receiver-returned.
Revoke sits at the bottom-right. Pending access without a destination offers revoke
only. Restore does not approve a blocked message. Review This Message selects the
same held admission, and Manage Access selects its exact grant.

Incoming stacks are per decidable receiver, showing three newest Current held rows.
Each row and the stack collapse independently. The header arrow opens Requests;
row arrows select exact bindings. Dismissal remembers immutable attention keys
locally: resolution/removal does not re-raise a stack, but an unseen request does.
Pointer or keyboard focus freezes membership and geometry; resolved actions disable
in place until both leave. Counts can update immediately.

Threads created is separate from Threads settled. The receiver change feed supplies
recorded results; first subscription starts at its current head, never retained
history. Confirmed available creation has an exact thread arrow. Failed, unknown,
deleted or unavailable destinations lead to exact Requests evidence. Unknown never
claims success. Collapsed text reports confirmed creations and separate failed/unknown
counts. Local dismissal is neither a decision nor a delivery acknowledgement.

Requests and creation results never use OS notifications, irrespective of global
notification mode. Other features and global settings retain their behavior.

Verification must exercise independent concurrent decisions, the shared policy
barrier, stale responses, 409 conflicts, lost replies, off-page lookup, changed
cursor revision, 450-record pagination, all durations, once-only eligibility,
restore/revoke generations, and receiver changes. Test pointer AND focus freezes on
arrival, resolution and removal; exact request versus thread arrows; collapsed
stacks; no historical result replay; and 320/900/1440 widths with enlarged text.
Use synthetic isolated state only. Focused new controller/browser tests are written
but unrun under the dependency-age hold. Adapter registration and route search
validation are implemented; focused checks and integrated browser proof remain
qualification duties.
