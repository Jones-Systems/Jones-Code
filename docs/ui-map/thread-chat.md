# Thread and chat

[Map index and shared recipes](README.md). Purpose: reach a project's draft or
thread and inspect its conversation without confusing a local draft with a
server-owned thread. All journeys below are source-discovered and **unverified**.

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
- [RightPanelTabs](../../apps/web/src/components/RightPanelTabs.tsx) also serves
  [Pull requests](../../apps/web/src/routes/_chat.pull-requests.tsx), outside
  this map's journeys. Shared workspace header consumers are listed in
  [Settings](settings.md#shared-layout-and-consumers).

## Entry points and journeys

Every row uses the [common launch/pair/capture/reset procedure](README.md#common-environment-and-recipes).
Use only IDs and workspaces produced by the isolated fixture. Each row is a
separate entry path; a success on one does not verify another. Capture the route,
visible selected thread and composer/timeline state after each action. For
navigation-only journeys, return to the starting fixture route to reset; close
only overlays opened by the journey. Preserve state for inspection.

| Entry and initial fixture | Ordered actions | Observable expectation and additional reset/limits |
| --- | --- | --- |
| `/`, empty connected environment | Navigate to `/`; wait for bootstrap; choose **Add project**, select a disposable workspace through the palette, then open its new thread. Separately choose **Start without a project** when available. | Empty hero gives way to a draft; project and scratch paths are separate checks. Keep the created synthetic project/draft in the owned home; do not delete shared projects to reset. |
| `/`, existing synthetic project | Navigate to `/`; observe automatic draft opening. If the source's failure state is safely reproduced, click **Try again**. | Draft belongs to the chosen recent project. Failure/retry needs a controlled fixture; do not break a real environment to force it. |
| Direct server route | Create a synthetic thread through supported app commands, then navigate to its `/<environmentId>/<threadId>` URL and reload. | Correct scoped thread and timeline; no cross-environment ID substitution. Missing/loading cases require separate fixtures and bootstrap observation. |
| Direct draft route | Open a draft normally, retain its generated URL, navigate away and back. | Same draft prompt/attachments when retained. An arbitrary invented draft ID is not a fixture; absent drafts return to `/`. |
| Sidebar and legacy sidebar | Click the synthetic thread or draft row; separately use the project/new-thread action in each enabled sidebar variant. | Selected row and route agree. Use actual snapshot names, not hard-coded coordinates. |
| Palette and shortcuts | Open the command palette, select a thread or new-thread action. Separately exercise `chat.new`, `chat.newLocal`, and `chat.newWithoutProject` using the current configured bindings. | Draft target follows context; multiple project groups may open the project chooser. Binding definitions and user overrides, not guessed key chords, control the shortcut. |
| Prompt link [`/new`](../../apps/web/src/routes/_chat.new.tsx) | Use a synthetic link accepted by [NewThreadLinkView](../../apps/web/src/components/promptLinks/NewThreadLinkView.tsx); inspect its project/provider prerequisites before any start. | Link preparation and resulting thread are distinct states. Starting can execute a provider/workspace action; blocked without that separately scoped fixture. |
| Thread references and citations | Click a fixture's [Markdown thread link](../../apps/web/src/components/chat/MarkdownThreadLink.tsx), [context chip](../../apps/web/src/components/ThreadContextChip.tsx), or [assistant citation](../../apps/web/src/components/chat/AssistantCitationChip.tsx); test each separately. | Correct target thread, with citation navigation handled by [assistantCitationNavigation](../../apps/web/src/lib/assistantCitationNavigation.ts). Missing target/environment is a separate state. |
| Relationships and timeline links | Open [ThreadRelationshipsControl](../../apps/web/src/components/chat/ThreadRelationshipsControl.tsx), then select a synthetic related thread; separately follow a timeline thread link. | Target route and selected thread agree. Relationship creation/merge is not authorized by this navigation recipe. |
| Notifications | With an authorized synthetic notification fixture, choose **Open thread** in [ThreadNotificationCoordinator](../../apps/web/src/components/ThreadNotificationCoordinator.tsx); separately click its native notification or a [settlement notification](../../apps/web/src/jones/settlementNotifications/SettlementNotificationCoordinator.tsx). | Correct thread opens. Native notification permission/host behavior and settlement fixtures are unverified; no live notifications are reused. |
| Reopen/restore | Close a fixture view through the app, then invoke the configured [reopen shortcut](../../apps/web/src/components/ReopenClosedViewShortcut.tsx); separately exercise thread-action restore in [useThreadActions](../../apps/web/src/hooks/useThreadActions.ts). | Restored draft/server route matches its retained target; reset by returning to the initial view. Archive/delete/restore effects need task-owned fixture state. |
| Composer promotion and recovery | With separately authorized provider execution, submit a synthetic draft; observe route promotion and retained composer. Exercise background/fan-out navigation and **Restore prompt** only with their controlled success/failure fixtures. | No lost newer draft and correct server target. These ChatView paths are discovered, not provider-tested; there is no provider-free business-behavior stand-in here. |
| Chat to Settings | Follow provider/model setup, scheduled-task or connection links described in [Settings entry paths](settings.md#entry-paths-and-journeys). | Correct settings section and scope; use Escape/back navigation to return to the fixture thread. |

## Evidence record

Source check: **2026-10-20**, Jones Code
`e7527d88ef438daa443e49a84d59e9891415918b`. Route and caller inspection found the
paths above; no runtime action was executed. All rows remain `unverified`.
Runtime verification is `blocked` in this documentation lane by the absence of
an owned running candidate, paired test tab and seeded scenarios. Build,
viewport/device/text size, harness run and capture artifacts: **none**.

The primary verification owner must supply those prerequisites and record
expected versus observed outcomes for each exercised row. Native, provider,
remote-connection and deeper panel states remain gaps. This is a partial map
addition, not a full maintenance pass or a product defect finding.
