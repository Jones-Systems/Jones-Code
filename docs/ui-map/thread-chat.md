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
  native approvals retain precedence. Existing user-input cards render the
  instruction, optional choices and open response for `t3_request_user_response`.
- [RightPanelTabs](../../apps/web/src/components/RightPanelTabs.tsx) also serves
  [Pull requests](../../apps/web/src/routes/_chat.pull-requests.tsx), outside
  this map's journeys. Shared workspace header consumers are listed in
  [Settings](settings.md#shared-layout-and-consumers).

### Destination shell blocks

The accepted code-block toolbar also hosts the destination chip and Play control
for `jones-shell` fences. [ChatMarkdown](../../apps/web/src/components/ChatMarkdown.tsx)
dispatches these to the [Jones controller and view](../../apps/web/src/jones/shellDestination/ShellDestinationCodeBlock.tsx).
Ordinary shell fences retain their existing source-thread terminal callback;
`environment=` on an ordinary shell fence suppresses Play and explains the
dedicated grammar. Other Markdown consumers without a thread and shell callback
show the destination but cannot run. Native mobile has no new Play control.

The chip uses the clicking client's exact-ID connection catalog. Its label is
authoritative; an unknown or mismatched agent label is shown as a claim and cannot
select a computer. The toolbar retains its existing insets and actions. Status
and long labels wrap within the block; the inline terminal has a 256px viewport
and a wrapping header with destination, destination cwd, status, Close and Run
again. Check narrow widths, keyboard focus and enlarged text alongside an ordinary
shell block and Mermaid block, which share the toolbar/frame.
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
