# Workstreams sidebar

[Map index and shared recipes](README.md). This partial map covers the shared
web/desktop renderer's Workstreams groups, assignment entry points and details.
The native mobile client, desktop shell and operational enrollment are outside
this verification boundary.

## Source and layout

[`WorkstreamNativeSidebar`](../../apps/web/src/components/workstreams/WorkstreamNativeSidebar.tsx)
supplies assignment menus, pointer dragging and known thread titles to
[`WorkstreamSidebarSection`](../../apps/web/src/components/workstreams/WorkstreamSidebarSection.tsx).
The primary sidebar composes both; standalone section consumers can omit thread
rendering and retain member IDs as a fallback. Group collapse and details are
separate states. Details use the existing sidebar gutters and a flexible header
with a reachable Close action; long detail titles and members wrap.

The running server qualifies its enrolled source before membership changes.
A build mismatch disables menu assignment and cross-group dragging. Refresh
checks readiness; renewing the connection is an operational action. Group
visibility alone does not prove assignment is available.

## Journeys

Use the [common isolated launch and fixture recipe](README.md#common-environment-and-recipes).
Two groups, a member thread and an unassigned thread are sufficient. Do not
copy live data or submit provider turns to create screenshots.

| Entry and initial state         | Actions and observable result                                                                                                           | Reset and limits                                                             |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Workstream name, closed details | Click its name; inspect status and readable members. Click again to dismiss.                                                            | Selection is local and does not change membership.                           |
| Open, loading or failed details | Choose Close, or focus inside details and press Escape. The panel disappears; a late response cannot reopen it.                         | Reopen the same group. Inner dialogs retain their own Escape handling.       |
| Open details                    | Open Manage and history; inspect membership actions, statements and history. Close and reopen details; the disclosure starts collapsed. | Do not submit tracking commands during visual-only verification.             |
| Build mismatch                  | Inspect the visible reason and Refresh. Open Assign thread; destinations remain disabled.                                               | Restore the synthetic ready state; no live enrollment is renewed by Refresh. |
| Ready thread menu               | Assign an unassigned thread to the other group; verify its new group.                                                                   | Assign it back to Unassigned.                                                |
| Ready pointer drag              | Drag a thread into another group; verify the destination and updated count.                                                             | Drag back. Pinned and remote-environment paths need their own fixtures.      |

## Evidence and remaining coverage

On October 21, 2026, the primary agent drove the real changed components in
Jones Code Browser against a temporary synthetic controller and simplified
thread rows, served by the task worktree at localhost:8340. Source baseline:
`446ff6554a014c04df195ae2e54f173f29d9a4d9`; candidate: the scoped patch delivered
by the accompanying PR. This is development-renderer evidence, not a stamped
packaged build. The temporary fixture is not a production route or a reusable
seed command. Before captures used baseline versions of both sidebar components
with the same fixture and styles; after captures used the changed components.

Verified: Close, name toggle, scoped Escape, collapsed disclosure, readable
known members, mismatch-disabled menu, ready menu assignment and pointer drag
with synthetic membership readback. At 1280×800 and 900×900, the panel stayed
within its sidebar. At 320×900 with a long title, 200% root font and spacing
stress (line height 1.5, letter spacing .12em, word spacing .16em, paragraph
spacing 2em), Close stayed reachable and the document did not scroll
horizontally. Expanded legacy action buttons can overflow the panel under that
combined stress; their text reflow remains unverified. Normal-size disclosure
actions were visible and reachable.

The focused regression suite owns loading/late-response, failed-read and
read-only cases. The live registry, installed enrollment renewal, actual
primary-sidebar thread menu integration, pinned threads, remote environments,
mobile and packaged desktop remain unverified by this synthetic browser flow.
Captures and the fixture provenance are retained outside Git in the task's
evidence directory and linked from the PR when upload is available.

## Workstream operations journeys

The thread assignment submenu's **Search or create Workstream…** entry opens the
shared web/desktop picker. Its dialog uses the existing Dialog, Input and Button
components: the dialog owns its gutters, choices scroll inside it, and the title
and status wrap. Settings → Jones Code → **Workstreams setup** uses the existing
settings section layout. Mobile's thread Workstreams sheet uses the same typed
operation recovery client through its environment connection.

On servers advertising `workstream-operations/1.0.0`, primary assignment and
creation use server operations. The server prepares references. Legacy primary
assignment and secondary links keep their existing verification requirements.
Pending membership is never projected into the displayed groups. Registry IDs
for operations come from setup status; the list binding's canonical origin is
not an operation identifier.

| Initial state          | Action and expected observation                                                                                              | Coverage        |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------- | --------------- |
| Ready, currently Alpha | Search Beta, select it, observe pending with Alpha unchanged; after commitment reload and observe Beta                       | Authored, unrun |
| Beta                   | Select Unassigned; reload and observe no primary                                                                             | Authored, unrun |
| Exact existing name    | Search Alpha; choose the existing row without a create operation                                                             | Authored, unrun |
| New name               | Double-click Create; one create ID, then one assignment ID; lost reply and CONFIG retries retain those requests              | Authored, unrun |
| Reconciling            | Close/reopen or reload; check the same operation; a missing memory result resubmits only locally retained bytes              | Authored, unrun |
| Conflict               | Show observed placement; only explicit Retry changes the expected placement and starts a new operation                       | Authored, unrun |
| Linked environment     | Select a destination from the primary registry; submit to the thread's own environment; a registry mismatch remains an error | Authored, unrun |
| Mobile, never verified | Primary assignment uses one server operation; secondary links and legacy assignment retain their guard                       | Authored, unrun |
| Setup unavailable      | Inspect each of eight source reasons, not-configured and offline; read the next-step documentation without setup mutations   | Authored, unrun |

A development-only synthetic entry is provided at
`/src/jones/workstreams/operations-preview.html`. From this worktree's `apps/web`
directory, after the coordinator releases the serial slot, run
`vp dev --host 127.0.0.1 --port 8344 --strictPort` with `VITE_HTTP_URL` and
`VITE_WS_URL` unset. The coordinator owns the process, actual port readback and
teardown. Use the T3 Browser workflow, navigate to that source entry, and retain
captures at 1280×800, an intermediate width, and 320×900, including 200% text and
spacing stress. **Reset synthetic scenario** clears only the fixture's two
session-storage keys; no app bootstrap, provider turn or application database is
used. No user profile or real state is needed.

The fixture uses the real picker, operation client and setup presentation with a
scripted transport. Choose commit, pending, conflict, CONFIG or lost-reply before
opening the picker. To finish pending, close the dialog, change the scenario to
commit, then reopen. The authoritative-placement and created-count lines are
readback assertions; selecting a row alone is not success. Reload preserves the
fixture's authority and retained requests. The fixture does not prove server,
authentication, actual sidebar integration, packaged desktop or native mobile
behavior. Those require the coordinator's isolated integrated run after J-A.

This candidate has no rendered evidence yet. Existing captures above are
historical evidence for their stated revision. Before/after comparison must use
separately identified source/build candidates under the same fixture and capture
conditions; changing the scenario is not a before/after source comparison.

### Isolated full-app qualification recipe

The component entry above is only focused evidence. Acceptance also requires the
real app sidebar, Settings and owning-server transport after integrating J-A.
The coordinator owns the serial slot, processes, temporary directories and final
cleanup. No installed application, user profile or live `~/.t3/userdata` is used.

1. Reserve a fresh task-owned absolute directory, for example
   `/tmp/wso-jc-evidence-CANDIDATE/t3-home`, and record the candidate commit/tree,
   source status and frozen contract digest. Do not reuse the component fixture's
   browser storage as an app authority. Use an isolated T3 Browser profile.
2. Start J-A's test-only loopback CP harness. It must expose its captured listener
   and cleanup handle, stage **fixture-only** activation into the chosen home,
   and supply source descriptors matching the isolated server's actual environment
   ID, authority namespace, store generation and observed build. Use the existing
   activation file parser and permissions from
   [activation](../operations/workstreams-activation.md); do not shell-source the
   file or borrow a live activation. Leave ambient activation variables absent so
   that they cannot override the fixture file.
3. From the candidate worktree root, with `VITE_HTTP_URL` and `VITE_WS_URL` unset,
   launch `vp run dev --home-dir /tmp/wso-jc-evidence-CANDIDATE/t3-home --host 127.0.0.1`.
   Record the actual base directory and ports printed by the runner. Pair only to
   that instance using `node apps/server/src/bin.ts pair --base-dir /tmp/wso-jc-evidence-CANDIDATE/t3-home`
   when pairing is required. Keep normal app authentication and authorization.
4. Use the real app UI/orchestration commands to create a synthetic project and
   two empty threads without provider turns. Seed Alpha and Beta through the
   loopback CP fixture; leave one native thread never registered. Do not patch
   SQLite projections for these behavioral assertions. Open Settings and verify
   the setup result's registry ID is the signed instance UUID, while legacy list
   binding still reports the canonical HTTPS origin.
5. Capture the real thread menu and picker before selection, while pending (Alpha
   remains displayed), and after authoritative reload (Beta displayed). Repeat
   Unassigned, exact-name selection, double-submit create, lost reply, reload with
   an open operation, CONFIG same-ID retry, conflict/explicit retry, and ID conflict.
   Record server/fixture submission IDs and authoritative memberships as well as
   screenshots. Exercise all setup source reasons and offline/not-configured.
6. For linked ownership, start a second separately isolated app server with its
   own home and native source descriptor, linked through the existing connection
   flow to the same fixture registry. Assign its thread through the first app;
   observe only the owning server's operation endpoint. Change the fixture registry
   identity to prove visible mismatch. Never substitute another environment's
   native identity locally.
7. Capture desktop/shared-renderer paths with T3 Browser. Native mobile evidence
   requires the repo's `test-t3-mobile` Device workflow connected to these same
   isolated instances; capture the never-verified primary path, pending status,
   explicit retry and retained legacy guard. Component tests are not native-device
   evidence. Record any unavailable device/build prerequisite explicitly.
8. Compare baseline and candidate in separately identified source checkouts with
   separate temporary homes and equivalent synthetic data at the same viewports.
   Keep before/after screenshots, console/network findings and identity receipts
   outside Git in the coordinator's evidence directory. Stop only captured
   task-owned processes after recording their results.

**Bootstrap seam still required from J-A/coordinator:** this checkpoint exports
`startLoopbackControlPlane()` in
`apps/server/src/jones/workstreams/testing/loopbackControlPlane.ts`, but it is not
an executable full-app fixture launcher. Its sources, authority, build and native
thread assignments are fixed; `startEnvironment()` creates an authenticated test
mini-server, not the app. The integrated harness must parameterize those values,
provide the frozen operations/native registration producer behavior and controlled
pending/lost-reply/CONFIG/conflict faults, and stage fixture activation into the
explicit dev home. The signed `registry_instance_id` → setup-status UUID projection
is supplied by J-A; clients consume it without changing the legacy origin. There
is deliberately no invented CP launch command here. Root must record the actual
J-A launcher and arguments before calling this recipe runnable end to end.

No step in either recipe has been run for this candidate.

The test-only [full-app registry adapter](../../apps/server/src/jones/workstreams/operations/fullAppFixture.ts)
can support the required real-app before/after journey. Its caller must use a
unique private home, the normal server launcher and authentication, and the
server's observed environment ID, authority namespace, store generation and
stamped build. It seeds Alpha and Beta and exposes membership gates, lost replies
and competing owner assignments without replacing the app's HTTP handlers or
projections. Capture the full application sidebar and Settings, then read back
the registry and reload. A source revision, served-build correspondence and
successful teardown must accompany that evidence. This helper's presence is
not evidence that the full-app journey has run.
