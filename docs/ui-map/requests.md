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

Current and Blocked tab counts mean held admissions, not cards or diagnostic events.
Done has no tab count; its body reports retained records. Unresolved, in-flight and
access lists have separate cursors. Load older records traverses the server keyset;
a changed snapshot revision requires refresh. Unknown outcomes stay outside Done.

Select a compact card to show its message in the right pane. The arrow immediately
after its operation tag opens the exact receiver/request/digest binding. Access
arrows open the exact destination thread. Off-page request selection performs an
exact lookup; missing evidence is unavailable. Live thread status and elapsed time
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
