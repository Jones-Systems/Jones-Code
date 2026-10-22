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
