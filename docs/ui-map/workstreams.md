# Workstreams UI map

The sidebar and mobile thread navigation use the shared [personal Workstreams controller](../../packages/client-runtime/src/jones/workstreams/personal/index.ts). The user flow is described in [Workstreams](../user/workstreams.md).

Completed and Abandoned groups appear under **Closed Workstreams**. The closed group detail lists its saved thread assignments and offers **Reopen Workstream**. An unsettled historical member appears under **Unassigned** in the sidebar.

| Journey                                           | Surface                                                                                    | Coverage                                                              |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------- |
| Create, rename, color, lifecycle, reorder, delete | Sidebar group menu; mobile group controls                                                  | Personal browser and mobile presentation/control tests                |
| Assign, unassign, create and assign               | Thread picker, sidebar drag, mobile thread menu                                            | Personal browser tests and shared controller command tests            |
| Move a pinned thread                              | Sidebar drag; assigned members keep their pin                                              | Personal browser tests                                                |
| Find a thread drop target                         | Collapsed tint; expanded ring and insertion gap                                            | Drag browser tests, including the pinned external bridge              |
| Open group options and choose status filters      | Centered 12×12 menu; title grab, controls pointer                                          | Header geometry, menu hit-testing and status-filter browser tests     |
| Expand, collapse, hide and filter all Workstreams | Sidebar Workstreams row above the named groups                                             | Bulk-controls, status-filter and header browser tests                 |
| Tune sidebar appearance                           | Sidebar appearance panel beside the live list; Jones Code settings title-brightness slider | Appearance model, card, header, launcher and settings browser tests   |
| Inspect roster totals                             | Bottom-right non-settled total; attention pills retain snooze rules                        | Roster unit tests and filter/window/collapse browser coverage         |
| Inspect connection availability                   | Jones Code settings; ordinary per-environment rows                                         | Personal settings browser tests and mobile presentation tests         |
| Observe changes on another client                 | Subscription to the thread’s environment                                                   | Shared controller tests; isolated two-client verification             |
| Synchronize definitions between environments      | Writable client connected to both copies                                                   | Shared controller convergence tests; isolated two-server verification |

Focused source coverage lives in [personalNative.browser.test.tsx](../../apps/web/src/jones/workstreams/personalNative.browser.test.tsx), [personalBulkControls.browser.test.tsx](../../apps/web/src/jones/workstreams/personalBulkControls.browser.test.tsx), [personalSettings.browser.test.tsx](../../apps/web/src/jones/workstreams/personalSettings.browser.test.tsx), [personalNotifications.test.ts](../../apps/web/src/jones/workstreams/personalNotifications.test.ts), [mobile controls](../../apps/mobile/src/features/workstreams/Controls.operations.test.tsx), [mobile presentation](../../apps/mobile/src/features/workstreams/personalPresentation.test.ts) and the shared controller’s adjacent tests. Source tests do not attest an installed client or a live cross-environment result.

Availability rows use the ordinary settings primitives and say Ready, Offline, Update required, Read only or Couldn’t sync. Failures use deduplicated native notifications with ordinary connection/update destinations. A cached snapshot is display-only; an environment without the personal capability shows no cached grouping and receives no writes.

Assignment confirmation belongs to the thread’s environment. Definition propagation requires an eventual client bridge with write access to the lagging environment. Read-only observation and offline suppression do not prove durable synchronization. Group lifecycle is metadata and does not change thread settlement.

## Retained history boundary

The old registry reader is present at `d68c5fcd65a4249b93a742fe5af1c96cd776d3fa`, the source revision preceding this retirement’s first deletion. The delivery owner records the actual deletion commit parent if it changes before commit. No key, ciphertext, activation file, historical table or external registry service is removed by this source retirement. The existing external reader and its independent key custody remain the route to that history; actual historical readback has not been verified here. The new client provides no historical registry viewer.

Deprecated operation routes return explicit unavailable errors; the native owner/provider/enrollment routes are removed. External queue dispatchers list personal Workstreams and assign threads through the bearer routes under `/api/jones/personal-workstreams`, advertised as `workstreamsPersonalHttp`. Ordinary app authentication, Voice transport and independent updater recovery remain outside this feature retirement.
