# Requests inbox and notifications

[Map index and shared recipes](README.md). Maintainer journey for reviewing
incoming tasks on their receiving environment in the shared web/desktop renderer.
These are source-derived expectations, not runtime acceptance evidence.

## Entry, scope and shared consumers

Open `/requests` directly, the sidebar Requests entry/attention link, or a global
notification's **Open Requests** action. The [route](../../apps/web/src/routes/requests.tsx)
mounts [RequestsPage](../../apps/web/src/jones/remoteAdmission/RequestsPage.tsx)
outside Settings. **Configure background evaluation** leads to
[Connections](settings.md#background-remote-evaluation), which configures the
receiving environment's evaluator account, model and options only.

Each registered environment has its own section, snapshot and Refresh action.
A connected environment is not necessarily eligible for owner review: the
receiving server must authorize the session as owner and support the endpoint.
Use synthetic owner, non-owner and unsupported endpoint fixtures independently.
Loading environments, no connected environment, loading requests, no pending
requests and inaccessible owner review are distinct states. Inaccessible
sections distinguish owner-only access, unsupported endpoints and transient
connection errors. Owner-only and unsupported states wait for an explicit
refresh; transient errors retry with backoff. A retained snapshot is explicitly
last known and its decisions are blocked. Never treat an inaccessible section
as an empty inbox.

The [shared store](../../apps/web/src/jones/remoteAdmission/store.ts) and
[subscription hook](../../apps/web/src/jones/remoteAdmission/useAdmission.ts)
feed the inbox, sidebar count/attention, configuration and
[global notifications](../../apps/web/src/jones/remoteAdmission/RemoteAdmissionNotifications.tsx).
Visible documents refresh on a ten-second interval and visibility changes.
Check cross-surface consistency rather than counting each mount as independent
state. A sidebar `?` means some environments are loading or have transient
failures; known owner-only and unsupported environments do not add uncertainty
to that count. A capped
count is a lower bound, not the complete queue size.

## Synthetic journeys

Use the common isolated recipe under the primary verification owner's control.
Seed only synthetic requests and policies. Reset by restoring/remounting that
fixture; do not approve real tasks, change live settings or authenticate providers.

1. Seed held risk findings from two senders and a separate evaluator failure.
   Verify grouping by sender identity/link and review category. Request review
   and **Evaluator service failure** must remain distinct: failure means the
   evaluator could not complete, not that the content was found unsafe. New
   failures show a bounded failure reason; old generic holds are not backfilled
   or automatically retried.
2. Expand a request, then its **Full bound request and evaluator configuration**.
   Inspect sender/client, operation, target, modes, category, full message,
   evaluator reason and bound envelope. Long summaries and nested details must
   remain readable without hiding the action explanation. Collapse and reopen.
3. With a fresh owner snapshot and matching policy revision, approve one held
   synthetic request and deny another. Verify the exact ID/binding is used,
   busy state blocks concurrent actions and the acknowledged state is shared
   across inbox and notifications. **Approved · awaiting dispatch** authorizes
   dispatch; it does not confirm execution. **Dispatch outcome unknown · awaiting
   confirmation** must not become success or offer resend/replay. A denied row
   disappears after its acknowledged decision.
4. Change the fixture policy revision after intake. Approval must explain that
   settings changed and require a newly submitted request; denial remains
   available unless another blocker applies. Exercise a rejected/lost decision
   response separately: it displays an unconfirmed-outcome error and blocks
   retry until server confirmation. Do not infer execution from an absent row.
5. Navigate away from Requests while in-app notifications are enabled. Verify
   persistent grouped review and service-failure alerts on other routes, with
   expandable review rows for at most three requests and a link/count for more.
   Dismiss an alert: its requests remain in the inbox. The same binding set is
   suppressed for that mounted notification instance; a changed set can notify
   again. Toggle the synthetic notification preference and verify the inbox
   remains usable without alerts.
6. Exercise empty, loading, failed load/recovery, last-known snapshot, busy,
   unavailable owner authorization, capped results and long content on both wide
   and narrow shared-renderer viewports. Blocked decisions must expose their
   explanation on focus/hover and resist activation.

## Limits and evidence

The server snapshot returns at most the oldest 200 pending records. The visible
cap warning is not pagination: later records can remain hidden behind older
pending records, including approved/dispatching records. Resolving held requests
and refreshing does not guarantee the later records become visible. Counts and
alerts cover the returned snapshot only.

A rejected decision response is also treated as unconfirmed. A subsequent
snapshot that still reports `held`, or omits the record, does not clear that
binding's retry block. This map does not claim recovery from that limitation.

The browser Notification path requires granted permission, a desktop-enabled
notification mode and an unfocused/hidden document. It opens Requests on click;
it is source-discovered only. Synthetic browser checks cannot qualify packaged
OS delivery, native-shell behavior, mobile, remote/relay/tunnel authorization or
live provider-account acceptance. The evaluator's tool-free assessment capability
does not constrain tools used by an accepted task after dispatch.

Source inspected at `2af74a87e5db90ca5cf7d3e2421d9a7f444fe6b2`.
Focused checks are [store tests](../../apps/web/src/jones/remoteAdmission/store.test.ts),
[inbox browser tests](../../apps/web/src/jones/remoteAdmission/RequestsPage.browser.test.tsx)
and [notification browser tests](../../apps/web/src/jones/remoteAdmission/RemoteAdmissionNotifications.browser.test.tsx)
in `jones-remote-admission`. They were not executed in this documentation lane.
Primary integration owns synthetic UI proof and its exact revision/captures.
Runtime build, viewport/text-size receipts and captures in this lane: **none**;
all journeys above remain **unverified** here.
