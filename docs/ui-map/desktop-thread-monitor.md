# Desktop thread monitor

[Map index and shared recipes](README.md). Maintainer journeys for the separate
right-edge desktop window. These expectations require feature-revision evidence;
the shared renderer and a screenshot alone do not prove native window behavior.

## Entry and scope

The persistent top-right panel toolbar shows **Thread monitor** when the desktop
host supports it. Its pressed state controls visibility. The first launch shows
the narrow monitor; an explicit hide is remembered. Hiding it does not pause,
interrupt or settle work. The monitor remains visible while the main window is
minimized. Hover expands it without activating the main window; keyboard access
and Escape must work independently. **Open in Jones Code** reveals the main
window at the exact environment and thread.

The expanded header contains all four aggregate counts, including zero and
collapsed sections. The Needs You diamond is purple; individual input, approval,
plan, working and completion statuses retain Jones Code's native colors and
readable icons/text. Needs You, Working, Waiting and Done collapse independently.
Done covers authoritative terminal completions in the last hour. Missing or
future completion timestamps cannot establish recent completion. Opening a
thread does not reset this window or mark it settled.

The main renderer owns environment authentication, connections, shell
subscriptions and command scheduling. The monitor receives bounded display data
through a dedicated native bridge and opens at most one detail subscription.
It has no provider credentials, arbitrary command/URL API or second connection
runtime. Jones-owned modules live under the web, desktop, client-runtime and
server `jones/threadMonitor` directories; the shared protocol is
`packages/contracts/src/jones/desktopThreadMonitor.ts`.

## Synthetic journeys

Use only task-owned isolated server/desktop state. The primary verification
owner runs shared-renderer checks through Browser and native shell checks through
the exact-revision qualified Electron harness. A real window manager is required
to prove minimize, focus and stacking on Linux.

1. Start without a saved visibility preference, then with saved hidden and shown
   choices. Minimize the main window. Observe live synthetic thread changes and
   send a monitor reply while it remains minimized. Check the durable message
   and synthetic provider delivery. Hover must not reveal/focus main; Open in
   Jones Code must reveal the correct scoped thread. Hide/show must issue no
   run-control command.
2. Populate all four groups, including duplicate thread IDs on different
   environments, child attention, pending requests, active/background work,
   checkpoint waiting, missing state and recent completions. Verify counts,
   ordering, native suppression rules and explanatory stale/offline state.
   Collapse each group and use Show More without losing the scroll anchor.
3. Enter separate reply drafts in every group. Regroup a focused row, update its
   status/theme, collapse/reopen and hide/show. Preserve draft, selection and
   scroll. Enter sends; Shift+Enter adds a newline; IME composition must not
   submit. Type during acknowledgment and retain the newer edit. Rejected sends
   keep the draft; unknown outcomes keep identity and cannot duplicate a send.
4. Open real detail and exercise an actual offered once/deny approval, supported
   multi-question/custom/multiselect input, and Use this plan. Verify exact
   environment/request/plan identity and domain outcome. Replace or resolve a
   request before activation and ensure the old answer cannot target the next
   request. Plain reply must never imply approval or an input response.
5. Verify guarded Settle rejects any unresolved request, queued/active work,
   actionable plan, active goal, background/child work, expected-result duty or
   stale source revision,
   including a race after the control becomes available. On a valid completed
   thread, verify actual settlement. Restore thread uses its real settlement ID
   and restores supported placement only; it cannot reverse execution, terminal
   cleanup or project-script effects. Missing guarded/restore capabilities have
   explanations and no unsafe fallback or fabricated Undo.
6. Age a Done row across exactly one hour, including while its editor/detail is
   focused. Counts expire together; active editing remains available outside the
   recent cohort, without draft loss or implicit settlement. Exercise resume,
   clock changes and missing/future completion timestamps.
7. Remove an environment with visible and hidden drafts/detail. Re-register the
   same IDs and deliver late deltas, detail results and acknowledgments from its
   retired epoch: nothing may reappear or become actionable. Disconnect/reconnect
   and hide/show preserve drafts. Cloud-account cleanup removes only environments
   actually removed; a surviving direct route keeps its authorized state. A
   nonresponsive monitor peer must be recreated before redisplay.
8. Exercise toolbar reachability on thread, workspace, pull-request and panel
   layouts, without duplicate controls. Verify live light/dark/custom semantic
   tokens, 320/384px width, 200% text, keyboard-only use, reduced motion, display
   changes and work-area bounds. Web/mobile without a native bridge show no dead
   floating-window toggle.

## Evidence and platform limits

Record exact source/build correspondence, native harness identity, fixture,
viewport/scale/text size, theme, OS/backend, expected/observed outcome, captures
and cleanup. Tests, captures and runtime evidence remain outside source control.
No installed application update follows from source delivery.

Linux native/ambiguous Wayland cannot promise positioning and always-on-top;
the control must explain unavailability rather than silently fail or change
global platform flags. X11 qualification does not prove macOS Spaces/fullscreen
or Windows DPI/taskbar behavior. Hosted build results do not prove interactive
OS behavior.

Implementation verification is pending for this map's initial source revision.
The delivery owner must replace this statement with exact feature-revision
results and retain any untested platform or interaction limits.
