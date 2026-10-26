# Thread and chat

[Map index and shared recipes](README.md). Purpose: reach a project's draft or
thread and inspect its conversation without confusing a local draft with a
server-owned thread. Navigation journeys are source-discovered and **unverified**;
the bounded Needs input check has a separate evidence receipt below.

## Source, states and layout

The pathless [`_chat` layout](../../apps/web/src/routes/_chat.tsx) gates access on
authenticated or hosted-static state, otherwise redirects to `/pair`.
[`ThreadRouteView`](../../apps/web/src/components/ThreadRouteView.tsx) renders
[`ChatView`](../../apps/web/src/components/ChatView.tsx) for both
[`/$environmentId/$threadId`](../../apps/web/src/routes/_chat.$environmentId.$threadId.tsx)
and [`/draft/$draftId`](../../apps/web/src/routes/_chat.draft.$draftId.tsx).
The leaf routes render no independent view. Draft promotion keeps the composer
mounted while replacing the URL; testing only the final URL misses this transition.

[`/`](../../apps/web/src/routes/_chat.index.tsx) waits for shell bootstrap, opens
a draft for the most recently updated project, or shows
[NoProjectsHero](../../apps/web/src/components/NoProjectsHero.tsx). A failed
start offers **Try again**. Hosted-static mode without environments instead
shows **Open Connections**. Missing draft state returns to `/`; a missing server
thread redirects only after an authoritative shell snapshot and when other
threads exist. Loading can retain an already known thread shell.

ChatView composes the header, timeline, composer and responsive right panel.
Important states include empty draft, populated timeline, active/queued turn,
request/approval controls, failed send/recovery, attachments, and open/closed
terminal, diff, files, Browser or Device panels. These deeper states are not a
complete feature inventory and need explicit fixtures before runtime claims.

Layout implementation lives in ChatView, ThreadRouteView and their components;
this map creates no replacement design contract. Preserve the draft/server mount
relationship, timeline/composer scroll behavior, mobile composer expansion and
panel sheet versus wide-panel behavior. Test long synthetic titles/messages,
keyboard focus, narrow widths and text enlargement when those areas change.

Known shared consumers:

- [ChatComposer](../../apps/web/src/components/chat/ChatComposer.tsx) and
  [MessagesTimeline](../../apps/web/src/components/chat/MessagesTimeline.tsx)
  are composed by ChatView across draft/server and responsive variants.
- [Sidebar](../../apps/web/src/components/Sidebar.tsx),
  [LegacySidebar](../../apps/web/src/components/LegacySidebar.tsx) and
  [CommandPalette](../../apps/web/src/components/CommandPalette.tsx) share
  [new-thread handling](../../apps/web/src/hooks/useHandleNewThread.ts).
  A current-sidebar check does not cover the legacy variant.
- Both sidebar status filters consume
  [thread attention](../../packages/shared/src/jones/threadAttention.ts) through
  [the status-filter model](../../apps/web/src/jones/threadStatusFilter/model.ts).
  A proposed plan alone creates no input status; explicit own/child requests and
  native approvals retain precedence. Working includes displayed Waiting rows in
  its count and selection; double-click Hide working excludes both. Filtering
  preserves the roster order, including pin order. The optional activity window
  in Settings → Jones Code limits status counts and positive selections to real
  message/run activity and includes read successful completions with reliable
  completion times. All preserves the scoped roster; Hide working still removes
  every working row. Current and legacy views expire rows within a minute. Existing user-input cards render the
  instruction, optional choices and open response for `t3_request_user_response`.
- [RightPanelTabs](../../apps/web/src/components/RightPanelTabs.tsx) also serves
  [Pull requests](../../apps/web/src/routes/_chat.pull-requests.tsx), outside
  this map's journeys. Shared workspace header consumers are listed in
  [Settings](settings.md#shared-layout-and-consumers).

### Expected thread responses

An explicit expected-result request keeps its sender **Waiting** after the
foreground turn ends. The pending-work banner identifies the expected response.
An ordinary message or navigation link alone does not establish a wait. A
correlated answer or explicit release clears that dependency; unrelated replies
and the recipient finishing a turn do not. Multiple requests clear independently.
Active foreground work, owner input and approval controls retain their existing
status precedence.

For isolated verification, send an expected-result request between two synthetic
threads, finish the sender's turn, and check its sidebar status and chat banner.
Reconnect without a new conversation event and verify that Waiting persists.
Deliver an unrelated reply, then the correlated result, and check that only the
result clears the wait and wakes the sender. Repeat with two requests, a release,
and an ordinary no-response message. Inspect the existing mobile thread list and
pending-work sheet as well as the shared desktop renderer. These journeys require
feature-revision evidence before being reported as verified.

### Sidebar build label

The environment-filter footer in current and legacy sidebars, and the Settings
sidebar, shares `SidebarEnvironmentFilters`. Keep its environment buttons at
their normal size. The build label uses the remaining width with a 2px minimum
gap, shrinks from its normal type size to 6px, then shortens timed labels to the
last four digits. Widening restores the full label. Focus/hover retains the exact
build identity; source/version fallbacks are not shortened to a fabricated time.

The browser sizing regression beside the component covers the 6px boundary,
shortening, expansion, containment and filter interaction. The existing browser
test covers the source fallback. On October 25, 2026, isolated development
readback at 1280 CSS pixels and device scale 2 exercised the current sidebar
from 256–420px and back: the full label reached 6.05px at a 268.3px sidebar,
then used four digits below that boundary with at least a 2px button gap.
The fixture used a synthetic build timestamp and an empty task-owned project;
it did not run a provider. Candidate: base `139eee2f067297f93813e1218b253f08705e9be1`,
component SHA-256 `a743ea20a26e73bb08644156fa86f4140592d0b4ac82596d98042f9ccac4ba5f`.
PR attachments show the actual component before/after at 280px in Chromium.
Integrated Browser snapshots timed out; geometry and filter readback succeeded.
Settings, legacy navigation and packaged native shells were not exercised.

### Linked thread details

[ThreadDetailsPanel](../../apps/web/src/components/chat/ThreadDetailsPanel.tsx)
mounts [ThreadRelationshipsControl](../../apps/web/src/components/chat/ThreadRelationshipsControl.tsx).
Its [Linked section](../../apps/web/src/jones/linkedThreads/LinkedThreadsSection.tsx)
starts collapsed and resets closed when the selected thread changes. The header
shows distinct related threads, a plus sign for incomplete results, the running
agent count and a Continued shortcut when a successor exists. Expanding it
exposes Continuations, Parents, Delegated tasks, Children, and Siblings & peers.
The groups retain fork ancestry, context transfers, previous agents and their
existing task controls. A related thread appears once, with additional
relationships shown as secondary labels.

[useLinkedThreads](../../apps/web/src/jones/linkedThreads/useLinkedThreads.ts)
combines navigation links from connected capable environments with existing
lineage. Remote rows retain environment identity and destination-aware
navigation; offline destinations are disabled and incomplete coverage remains
visible. Old servers retain the lineage view. Links grant no task control or
connection access. The section menu offers Link thread and Copy handoff text;
continuation recipients can accept an offered handoff from the row menu.

For the source-discovered journey, open details on synthetic threads with a
continuation, launch parent, sibling, fork and delegated task. Expand Linked,
open its groups, navigate related threads, and use an existing task control.
Create a manual link, accept a received handoff, copy its full origin/return
address, unlink it and reload to verify persistence. Switch threads and confirm
Linked starts closed again. Test unavailable environments and the lineage-only
compatibility view separately.

The [Linked browser regression](../../apps/web/src/jones/linkedThreads/LinkedThreads.browser.test.tsx)
and [lineage browser regression](../../apps/web/src/jones/remoteAgentLineage/ThreadRelationships.browser.test.tsx)
cover component interactions; integrated runtime captures and backend readback
are separate evidence. This map does not claim the journey has been exercised.

### Queued-message batch steering

The active thread composer exposes
[QueuedRunsControl](../../apps/web/src/components/chat/QueuedRunsControl.tsx).
Its header uses the [batch action](../../apps/web/src/jones/queue/QueuedSteerBatchAction.tsx)
and the [shared selector](../../packages/client-runtime/src/jones/queue/queuedSteerBatch.ts);
native mobile consumes the same selector through its own queue sheet.

For the account-change journey, start a synthetic active Codex turn and queue
four ordinary messages whose saved account, provider thread and model differ
from that turn. With no pending request or edit, **Steer all (4)** must be
enabled. Click once: one command must contain all four ordered message IDs.
Server readback must retain the four original messages and create one combined
provider input. Automatic tool-boundary delivery must still reject mismatched
queued configurations. Pending requests, stale message versions, maintenance
commands, ended turns and input limits retain their own guards.

The adjacent [browser regression](../../apps/web/src/jones/queue/QueuedSteerBatch.browser.test.tsx),
[client tests](../../packages/client-runtime/src/jones/queue/queuedSteerBatch.test.ts)
and [server integration](../../apps/server/src/jones/orchestration/QueuedSteerBatch.integration.test.ts)
cover those layers separately. Use the Browser-panel workflow for interactive
candidate evidence and retain its exact fixture, source and captures with the
PR. Synthetic component evidence does not qualify a live provider, packaged
desktop shell or native mobile interaction. Reset only the task-owned fixture;
do not consume or remove messages in another thread to reproduce this case.

### Destination shell blocks

The accepted code-block toolbar also hosts the destination chip and Play control
for `jones-shell` fences. [ChatMarkdown](../../apps/web/src/components/ChatMarkdown.tsx)
dispatches these to the [Jones controller and view](../../apps/web/src/jones/shellDestination/ShellDestinationCodeBlock.tsx).
Ordinary shell fences retain their existing source-thread terminal callback;
`environment=` on an ordinary shell fence suppresses Play and explains the
dedicated grammar. Other Markdown consumers without a thread, shell callback or stable
message identity show the destination but cannot run. Each assistant message and
block keeps its own terminal association. Legacy associations without message
identity show an ambiguity warning and disable ordinary Play; Run again explicitly
starts a new terminal without attaching or changing the old association. Native mobile has no new Play control.

The chip uses the clicking client's exact-ID connection catalog. Its label is
authoritative; an unknown or mismatched agent label is shown as a claim and cannot
select a computer. The toolbar retains its existing insets and actions. Status
and long labels wrap within the block; the inline terminal has a 256px frame
and a wrapping header with destination, destination cwd, status, Close and Run
again. Check narrow widths, keyboard focus and enlarged text alongside an ordinary
shell block and Mermaid block, which share the toolbar/frame.
The inline terminal's bottom-right **Copy terminal contents** button uses the
message-copy confirmation. Run a synthetic command, click Copy, then paste into
an isolated text field: expect the current plain text including retained
scrollback, with soft-wrapped lines rejoined and the selection unchanged. Append
more output and copy again to check freshness. The ordinary drawer does not
enable this footer. The Jones terminal-contents unit and browser fixtures cover
formatting and the real surface/button interaction; they do not qualify a live
remote connection or the native desktop shell.
Command terminals close on the owning server after thirty minutes of observed idle
shell time, confirmed by prompt/preexec hooks in Bash 4.4+ or Zsh. Shell
builtins running without children remain protected. Unsupported shells, old
sessions without hooks, or startup configuration overriding the hooks stay open.
Input, output and detected commands reset the interval. Running
subprocesses, shell helpers, a replaced root command or uncertain inspection keep
the terminal open; disconnecting the client does not disable cleanup. Expiry
retains history on the server but removes the live session. The inline card then
shows unavailable; explicit Run again allocates a fresh terminal. Revisiting a
source drawer command attaches without launch fields and cannot recreate its
expired shell. Ordinary manually opened terminals retain their existing lifetime.
New sessions on updated servers recognize the reserved remote IDs; existing
shell processes without the new prompt hooks remain open. The fresh source-drawer
identity requires an updated web/desktop client. Source delivery does not update
installed clients or servers.

The composer [code-block node](../../apps/web/src/components/chat/ComposerCodeBlockNodeView.tsx)
also uses the frame. [File previews](../../apps/web/src/components/files/FileMarkdownPreview.tsx),
[pull-request Markdown](../../apps/web/src/components/pullRequest/PullRequestMarkdown.tsx),
plans, item inspection and pending user-input panels consume ChatMarkdown without
the project shell callback; their new destination blocks must remain disabled.
These secondary consumer journeys remain runtime-unverified.

Runtime evidence: primary verification on the repaired source in
`e84312a33afe1d9ae932b26ad820bceb445bdd95` used two isolated server environments
on one VPS. Play opened the destination-local PTY and cwd, accepted interactive
input, reattached after reload without replay, and explicitly closed. An unknown
destination stayed disabled. Screenshot and recording are published in
[PR #521](https://github.com/Jones-Systems/Jones-Code/pull/521).
This establishes the shared-renderer native terminal path; it does not establish
physical multi-host behavior or packaged macOS behavior. Navigation, process
restart, source-self behavior and the other disabled-state variations below have
focused harness coverage where applicable and remain runtime-unverified.

Runtime qualification recipe:

1. Start two isolated synthetic environments using the common development recipe;
   connect the same browser client to both and retain their exact IDs and labels.
   Give each a distinct disposable server cwd. Use a project-backed thread on A.
2. Supply a completed assistant fixture with a `jones-shell` fence whose metadata
   is `environment=<B's exact ID> label="<B's label>"`, and whose single command
   prints `pwd` plus a unique marker. Use the existing supported fixture/message
   path; no private conversation or credential copy is part of this recipe.
3. Click Play twice quickly. Observe one B-local terminal and one marker, B's
   server cwd, and no terminal opened on A. Type into the inline terminal to check
   interactivity. Navigate away/back and reload; output remains without replay.
4. Explicitly Close; verify the session and history are removed. Run again should
   allocate a different terminal ID. Let a separate run exit and check its output
   remains readable. Stop/restart only the owned B fixture and reattach: show the
   missing/restarted state, with no automatic command or replacement shell.
5. Repeat with A as the destination: the existing thread drawer/cwd behavior must
   remain. Test unknown ID, an ID prefix, mismatched label, disconnected B and a
   terminal-permission-limited synthetic connection; each must visibly disable
   execution. Also check incomplete/streaming and multiline blocks.

Pure resolver/controller tests and the focused browser harness exercise the
real Markdown toolbar with synthetic connection/terminal adapters. They do not
prove native PTY behavior, transport reconnect, desktop packaging or another
client's catalog. The primary agent owns actual two-environment execution,
captures, candidate/build/viewport evidence and fixture cleanup. Explicit Close
is the per-run cleanup; navigation and component unmount intentionally leave
sessions alive. The all-terminals view can expose these destination-local
synthetic-thread sessions.

## Entry points and journeys

Every row uses the [common launch/pair/capture/reset procedure](README.md#common-environment-and-recipes).
Use only IDs and workspaces produced by the isolated fixture. Each row is a
separate entry path; a success on one does not verify another. Capture the route,
visible selected thread and composer/timeline state after each action. For
navigation-only journeys, return to the starting fixture route to reset; close
only overlays opened by the journey. Preserve state for inspection.

| Entry and initial fixture                                     | Ordered actions                                                                                                                                                                                                                                                                                                                                 | Observable expectation and additional reset/limits                                                                                                                                               |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `/`, empty connected environment                              | Navigate to `/`; wait for bootstrap; choose **Add project**, select a disposable workspace through the palette, then open its new thread. Separately choose **Start without a project** when available.                                                                                                                                         | Empty hero gives way to a draft; project and scratch paths are separate checks. Keep the created synthetic project/draft in the owned home; do not delete shared projects to reset.              |
| `/`, existing synthetic project                               | Navigate to `/`; observe automatic draft opening. If the source's failure state is safely reproduced, click **Try again**.                                                                                                                                                                                                                      | Draft belongs to the chosen recent project. Failure/retry needs a controlled fixture; do not break a real environment to force it.                                                               |
| Direct server route                                           | Create a synthetic thread through supported app commands, then navigate to its `/<environmentId>/<threadId>` URL and reload.                                                                                                                                                                                                                    | Correct scoped thread and timeline; no cross-environment ID substitution. Missing/loading cases require separate fixtures and bootstrap observation.                                             |
| Direct draft route                                            | Open a draft normally, retain its generated URL, navigate away and back.                                                                                                                                                                                                                                                                        | Same draft prompt/attachments when retained. An arbitrary invented draft ID is not a fixture; absent drafts return to `/`.                                                                       |
| Sidebar and legacy sidebar                                    | Click the synthetic thread or draft row; separately use the project/new-thread action in each enabled sidebar variant.                                                                                                                                                                                                                          | Selected row and route agree. Use actual snapshot names, not hard-coded coordinates.                                                                                                             |
| Palette and shortcuts                                         | Open the command palette, select a thread or new-thread action. Separately exercise `chat.new`, `chat.newLocal`, and `chat.newWithoutProject` using the current configured bindings.                                                                                                                                                            | Draft target follows context; multiple project groups may open the project chooser. Binding definitions and user overrides, not guessed key chords, control the shortcut.                        |
| Prompt link [`/new`](../../apps/web/src/routes/_chat.new.tsx) | Use a synthetic link accepted by [NewThreadLinkView](../../apps/web/src/components/promptLinks/NewThreadLinkView.tsx); inspect its project/provider prerequisites before any start.                                                                                                                                                             | Link preparation and resulting thread are distinct states. Starting can execute a provider/workspace action; blocked without that separately scoped fixture.                                     |
| Thread references and citations                               | Click a fixture's [Markdown thread link](../../apps/web/src/components/chat/MarkdownThreadLink.tsx), [context chip](../../apps/web/src/components/ThreadContextChip.tsx), or [assistant citation](../../apps/web/src/components/chat/AssistantCitationChip.tsx); test each separately.                                                          | Correct target thread, with citation navigation handled by [assistantCitationNavigation](../../apps/web/src/lib/assistantCitationNavigation.ts). Missing target/environment is a separate state. |
| Relationships and timeline links                              | Open [ThreadRelationshipsControl](../../apps/web/src/components/chat/ThreadRelationshipsControl.tsx), then select a synthetic related thread; separately follow a timeline thread link.                                                                                                                                                         | Target route and selected thread agree. Relationship creation/merge is not authorized by this navigation recipe.                                                                                 |
| Notifications                                                 | With an authorized synthetic notification fixture, choose **Open thread** in [ThreadNotificationCoordinator](../../apps/web/src/components/ThreadNotificationCoordinator.tsx); separately click its native notification or a [settlement notification](../../apps/web/src/jones/settlementNotifications/SettlementNotificationCoordinator.tsx). | Correct thread opens. Native notification permission/host behavior and settlement fixtures are unverified; no live notifications are reused.                                                     |
| Reopen/restore                                                | Close a fixture view through the app, then invoke the configured [reopen shortcut](../../apps/web/src/components/ReopenClosedViewShortcut.tsx); separately exercise thread-action restore in [useThreadActions](../../apps/web/src/hooks/useThreadActions.ts).                                                                                  | Restored draft/server route matches its retained target; reset by returning to the initial view. Archive/delete/restore effects need task-owned fixture state.                                   |
| Composer promotion and recovery                               | With separately authorized provider execution, submit a synthetic draft; observe route promotion and retained composer. Exercise background/fan-out navigation and **Restore prompt** only with their controlled success/failure fixtures.                                                                                                      | No lost newer draft and correct server target. These ChatView paths are discovered, not provider-tested; there is no provider-free business-behavior stand-in here.                              |
| Chat to Settings                                              | Follow provider/model setup, scheduled-task or connection links described in [Settings entry paths](settings.md#entry-paths-and-journeys).                                                                                                                                                                                                      | Correct settings section and scope; use Escape/back navigation to return to the fixture thread.                                                                                                  |

## Evidence record

Original navigation source check: **2026-10-20**, Jones Code
`e7527d88ef438daa443e49a84d59e9891415918b`. Route and caller inspection found the
paths above; no navigation journey was executed in that documentation lane.
Those rows remain `unverified`. Runtime verification was `blocked` in that lane by the absence of
an owned running candidate, paired test tab and seeded scenarios. Build,
viewport/device/text size, harness run and capture artifacts: **none**.

The primary verification owner must supply those prerequisites and record
expected versus observed outcomes for each exercised row. Native, provider,
remote-connection and deeper panel states remain gaps. This is a partial map
addition, not a full maintenance pass or a product defect finding.

### Working includes Waiting

Use the common isolated-home recipe with two connected-idle Waiting threads
pinned in distinct order and an ordinary idle control. On All, record their
order and the Working count. Click the status pill once: both Waiting rows must
remain in pin order and count as Working. Double-click within 250 ms: both must
vanish while the idle control remains. Click once to return to All and verify
the original order. Projection fixtures prove renderer behavior only.

The primary Browser check verified this journey on October 23, 2026 using
base `7f5caa1eb1255e228d44a88c37f5a9d78227cd19` plus the Working/Waiting
classifier change, at 1280×800 CSS pixels, device scale 2, light theme and default
text size. Before the change the two Waiting rows counted as zero and vanished
on Working; afterward they counted as two, stayed in pin order, and were hidden
by double-click. Both filter variants and input/approval precedence have focused
model and browser tests. The live journey used the current sidebar; legacy,
native mobile, packaged desktop and provider execution were not exercised.

### Needs input filter and open response

Use the common isolated-home recipe with two schema-valid synthetic threads:
a completed, acknowledged review with an old proposed plan and no request, and
a completed run with an explicit pending message-mode user-input request.
Follow the SQLite fixture skill; direct projection data proves rendering only.
Open the explicit-action thread, cycle the header from all to working to Needs
input with more than 250 ms between clicks, and type into **Your answer**.
Expect only the explicit-action row, its actionable instruction, an open response
field and an enabled **Send** button. Do not submit without separately scoped
provider execution. Clear the typed draft to reset; preserve the owned fixture
and captures for review, and stop only the captured development-server session.

This bounded journey was **verified** on **2026-10-20** in the shared web renderer
at 1280×800, light theme and default text size, using Jones Code collaborative
Browser and the documented Node 24.13.1/Vite development runner. Candidate
`7fd787fe9aa6ca9776b1635d576d5bb9b65eb668`, with this map-only edit, contains
the observed application source. The explicit instruction rendered, the stale review was excluded, and
typing enabled Send. Before/after captures are retained in
[PR #453](https://github.com/Jones-Systems/Jones-Code/pull/453).

The legacy filter is covered by its affected model tests, not this manual
journey. Native desktop/mobile, text enlargement, real provider delivery and
other navigation entries remain unverified here. Serialized request creation,
completion/recovery persistence, answer acceptance and dismissal use separate
command integration tests; this projection fixture does not qualify them.

### Settled-thread notification details

Use the isolated development origin and its local-storage settlement inbox to
seed synthetic entries with short titles, a long unbroken title, short notes,
a long continuation note and one entry without notes. Never reuse live notices.
The shared renderer's settlement coordinator and toast provider own this overlay;
the native mobile overlay is a separate consumer.

Expand **Show settled threads**. The toast fits its longest natural line between
its initial width and twice that width, capped by the viewport gutters. With
short content it stays at its initial width. Click a note-bearing row outside
its title to hide or show that row's notes; use its chevron button for keyboard
access. The title opens the thread independently. Close and reopen the list and
publish another synthetic entry to check that row choices survive. Check the
single-thread description and an ordinary expandable toast as shared-component
regressions, then dismiss only the synthetic notification to reset.

Record the candidate, viewport, theme, text stress, measured widths and interaction
captures in the change's PR evidence. This local-storage fixture proves renderer
behavior; it does not prove settlement detection, backend persistence, native
notification delivery, or the separate React Native overlay.

### Sub-agent account glyphs

Individual timeline links and collapsed sub-agent groups both render
`EnvironmentSubagentAvatar` in `V2LifecycleRow.tsx`. A matching local provider
catalog entry supplies the same resolved account name as the composer; the
Jones `ProviderAccountGlyph` draws its provider mark behind outlined initials.
Remote children, mismatched drivers and missing catalog entries retain their
existing fallback. Timeline initials are neutral and do not read usage limits.

Focused browser verification on **2026-10-21** used the production avatar with
an isolated synthetic provider atom, four Codex/Claude account instances, a
three-avatar group and a remote child. Comparison source was
`34716429f0a5265f3f993caa07a1f4fb850921c1`; candidate changed only the glyph and
identity-selection hook described above. Comparable captures used 1280×800 CSS
pixels, device scale 2, dark theme; a 375×812 light-theme capture additionally
verified wrapping and no horizontal overflow. Grouped rendered label bounds
were 11.41 CSS pixels wide with centers 18 pixels apart. Status dots remained
above the label; clicking the surrounding synthetic row reached its handler.
This verifies avatar rendering and pointer pass-through, not live child-thread
navigation, provider execution, native mobile, or packaged desktop behavior.
PR attachments retain the reviewed before/after evidence. The temporary browser
fixture was removed after capture; no live application state was used.

## Structured owner questions

`t3_request_user_response` accepts a brief Markdown instruction or 1–5 structured
questions, with optional impact descriptions on choices. Involved context belongs
in the preceding chat message. Web and desktop share the question card; mobile
uses the same sequential answer semantics. Existing routed Markdown keeps PR and
thread links actionable.

Start with nothing selected. Choose or type an answer, then use **Continue**;
selection and digit shortcuts never advance or send. The progress buttons beside
`i/N` jump to any question and expand a collapsed card, including its current
question. **Back**, jumps and collapse preserve drafts. On the final question,
**Send** remains blocked until every question has an answer, with a reason naming
the first missing answer. Native permission prompts remain separate.

For message-mode owner requests, text supplements a selected choice as a note;
**Sends:** previews the reply. **Clear choice** keeps the text as the whole answer.
Enter adds a newline and only the explicit controls continue or send. Replies
have no length cap; the field scrolls after ten lines on web and within a bounded
height on mobile. Live provider questions keep their existing replacement and
field-key semantics, with select-only choices.

Evidence: focused automated contracts, server delivery, helper and headless-browser
checks cover these behaviors. Integrated renderer journeys and native mobile
checks are **pending root verification** on the candidate revision; the earlier
Needs-input-count fixture without a visible card proves no answering journey.
Packaged desktop is **unverified**. Gallery publication is out of scope.
