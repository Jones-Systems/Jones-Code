# Settings

[Map index and shared recipes](README.md). Purpose: find the correct settings
section and owning scope, then verify a reversible change without modifying the
wrong environment. All entries are source-discovered and **unverified**.

## Routes and states

[`settings.tsx`](../../apps/web/src/routes/settings.tsx) redirects `/settings`
to `/settings/general` and unauthenticated access to `/pair`; hosted-static
access is also accepted. It supplies the breadcrumb, scope boundary and content
outlet. The sibling routes below are the discovered section boundary, not proof
that each setting or external integration works.

| Route under `/settings/` | Section/source                                                                      | Prerequisites or distinct states                                                                       |
| ------------------------ | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `general`                | [General](../../apps/web/src/routes/settings.general.tsx)                           | Device and scoped settings; **Restore device defaults** is shown here, not a server-state reset.       |
| `appearance`             | [Appearance](../../apps/web/src/routes/settings.appearance.tsx)                     | Device-only theme/typography controls; verify persistence on the isolated client.                      |
| `projects`               | [Projects](../../apps/web/src/routes/settings.projects.tsx)                         | Synthetic project/checkout targets; navigation visibility depends on scope.                            |
| `keybindings`            | [Keybindings](../../apps/web/src/routes/settings.keybindings.tsx)                   | Current bindings and overrides; retain the original value before edits.                                |
| `snap-shot`              | [SnapShots](../../apps/web/src/routes/settings.snap-shot.tsx)                       | Native capture bridge/setup states; browser display does not qualify native capture.                   |
| `providers`              | [Providers](../../apps/web/src/routes/settings.providers.tsx)                       | Single selected environment and provider readiness/configuration; credentials are outside this recipe. |
| `integrations`           | [Integrations](../../apps/web/src/routes/settings.integrations.tsx)                 | Selected environment and available integration capabilities; external services need separate fixtures. |
| `scheduled-tasks`        | [Scheduled Tasks](../../apps/web/src/routes/settings.scheduled-tasks.tsx)           | Scope-derived target; execution and scheduler effects need separate authorization/fixtures.            |
| `source-control`         | [Source Control](../../apps/web/src/routes/settings.source-control.tsx)             | Host/provider settings and repository context; do not run external Git effects to prove navigation.    |
| `storage`                | [Storage](../../apps/web/src/routes/settings.storage.tsx)                           | Environment state; destructive cleanup is outside a navigation check.                                  |
| `connections`            | [Connections](../../apps/web/src/routes/settings.connections.tsx)                   | Device-only route; administration requires suitable scopes and isolated endpoints.                     |
| `archived`               | [Archive](../../apps/web/src/routes/settings.archived.tsx)                          | Synthetic archived threads for restore checks.                                                         |
| `jones-code`             | [Jones Code](../../apps/web/src/routes/settings.jones-code.tsx)                     | Device-only route rendering PromptsSettings.                                                           |
| `diagnostics`            | [Diagnostics](../../apps/web/src/routes/settings.diagnostics.tsx)                   | Environment-specific view; reachable from General/search, excluded from ordinary section navigation.   |
| `open-source-licenses`   | [Open source licenses](../../apps/web/src/routes/settings.open-source-licenses.tsx) | General's link; distinct breadcrumb but General remains the selected sidebar section.                  |

[SettingsScopeContext](../../apps/web/src/components/settings/SettingsScopeContext.tsx)
and [scope navigation](../../apps/web/src/components/settings/settingsScopeNavigation.ts)
retain the `project`, `machine` and `checkout` search axes. Scope changes clear
the search target hash. Unavailable targets, disconnected environments, missing
write permission and unsupported search targets are meaningful states, not
empty successful loads. Providers selects one environment; device-only paths
are declared in [SettingsScopeSentence](../../apps/web/src/components/settings/SettingsScopeSentence.tsx).

## Shared layout and consumers

The implementation contract is the settings shell plus
[settingsLayout](../../apps/web/src/components/settings/settingsLayout.tsx):
scrollable page content, named search targets, focus/highlight behavior and
responsive row control placement. Test long labels/descriptions, narrow widths,
text enlargement, keyboard focus and scope notices when changing these surfaces.
No separate reviewed geometry baseline is asserted by this map.

- SettingsPageContainer is shared by General/Appearance/Archive in
  [SettingsPanels](../../apps/web/src/components/settings/SettingsPanels.tsx),
  [ProviderSettingsPanel](../../apps/web/src/components/settings/ProviderSettingsPanel.tsx),
  [ConnectionsSettings](../../apps/web/src/components/settings/ConnectionsSettings.tsx),
  [ProjectsSettings](../../apps/web/src/components/settings/ProjectsSettings.tsx),
  [ProjectSettingsPanel](../../apps/web/src/components/settings/ProjectSettingsPanel.tsx),
  [KeybindingsSettings](../../apps/web/src/components/settings/KeybindingsSettings.tsx),
  [SnapShotSettings](../../apps/web/src/components/settings/SnapShotSettings.tsx),
  [IntegrationsSettings](../../apps/web/src/components/settings/IntegrationsSettings.tsx),
  [ScheduledTasksSettings](../../apps/web/src/components/settings/ScheduledTasksSettings.tsx),
  [SourceControlSettings](../../apps/web/src/components/settings/SourceControlSettings.tsx),
  [StorageSettings](../../apps/web/src/components/settings/StorageSettings.tsx),
  [DiagnosticsSettings](../../apps/web/src/components/settings/DiagnosticsSettings.tsx),
  [OpenSourceLicenses](../../apps/web/src/components/settings/OpenSourceLicenses.tsx)
  and [SettingsScopeNotice](../../apps/web/src/components/settings/SettingsScopeNotice.tsx).
  SettingsRow/control variants need affected-consumer checks; one section is not
  coverage of all these consumers.
- [SettingsSidebarNav](../../apps/web/src/components/settings/SettingsSidebarNav.tsx)
  and [CommandPalette](../../apps/web/src/components/CommandPalette.tsx) share
  [settingsSearch](../../apps/web/src/components/settings/settingsSearch.ts).
  Check both search entrances when search targets change.
- [WorkspacePageHeader](../../apps/web/src/components/WorkspacePageHeader.tsx)
  is also consumed by the chat index/empty states,
  [NewThreadLinkView](../../apps/web/src/components/promptLinks/NewThreadLinkView.tsx),
  [NoActiveThreadState](../../apps/web/src/components/NoActiveThreadState.tsx),
  [Pull requests](../../apps/web/src/routes/_chat.pull-requests.tsx),
  [Usage](../../apps/web/src/components/usage/UsagePage.tsx),
  [Conversations](../../apps/web/src/components/conversations/ConversationLibraryPage.tsx),
  [Work queue](../../apps/web/src/jones/workQueue/UnifiedQueuePage.tsx) and
  [Design requests](../../apps/web/src/jones/designRequests/DesignRequestsPage.tsx).
  These consumers are untested; the last five are outside this partial map.
  Shared UI primitives have broader consumers not inventoried here.

## Entry paths and journeys

Use the [common isolated launch/pair/fixture recipe](README.md#common-environment-and-recipes).
Begin from a synthetic thread or the empty index. Record initial scope and any
setting value before changing it. For each navigation path below, inspect the
**Settings breadcrumb**, selected section, target scope and visible content;
return with Escape or the app's back navigation. The shell's
[main-app location helper](../../apps/web/src/components/sidebar/mainAppLocation.ts)
owns the return target. Reaching a section is not verification of its mutations.

| Entry path and source                         | Ordered actions and expected observation                                                                                                                                                                                                                                                                                                                                                                                                               | Fixture/reset limits                                                                                                                                                                   |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Direct `/settings` and each sibling URL above | Navigate to the path; observe General redirect or the matching section breadcrumb/content. Repeat each affected sibling independently.                                                                                                                                                                                                                                                                                                                 | No section qualifies another; preserve the initial scope and return route.                                                                                                             |
| App settings action                           | Invoke the app's **open-settings** menu action handled by [AppSidebarLayout](../../apps/web/src/components/AppSidebarLayout.tsx). Separately select **Open settings** in the command palette.                                                                                                                                                                                                                                                          | Both should reach General from chat. Desktop menu delivery itself needs native coverage.                                                                                               |
| Settings sidebar                              | Select a visible section by its displayed name; inspect active section and breadcrumb.                                                                                                                                                                                                                                                                                                                                                                 | Diagnostics and licenses use their separate entrances; Projects visibility is scoped.                                                                                                  |
| Settings search                               | Type a known synthetic target query in **Search settings**, choose its result, inspect scrolling/focus/highlight; use **Clear settings search** afterward. Separately select the equivalent settings result in the command palette.                                                                                                                                                                                                                    | Search target availability depends on scope/capabilities. Hash handling can clear the URL after focusing; verify the visible target, not only the hash.                                |
| Scope selectors and project settings          | Choose only the fixture's environment/project/checkout; navigate to another section and inspect retained axes. Also follow the project's settings action in Sidebar/palette and the [legacy project URL redirect](../../apps/web/src/routes/projects.$projectKey.tsx).                                                                                                                                                                                 | Correct owning scope is the expectation. Restore the starting scope; unavailable/reconnect notices need isolated fixtures, not disconnecting a live environment.                       |
| General cross-links                           | Follow **Diagnostics** and **Open source licenses** from SettingsPanels; inspect their distinct content/breadcrumbs and return to General.                                                                                                                                                                                                                                                                                                             | Diagnostics retains its environment target. Do not publish diagnostic private data.                                                                                                    |
| Provider setup links                          | Use ChatView/model setup and [ModelPickerContent](../../apps/web/src/components/chat/ModelPickerContent.tsx); separately follow [SidebarProviderUpdatePill](../../apps/web/src/components/sidebar/SidebarProviderUpdatePill.tsx), [ProviderUpdateLaunchNotification](../../apps/web/src/components/ProviderUpdateLaunchNotification.tsx) and [ProviderUpdatePrimaryNotification](../../apps/web/src/components/ProviderUpdatePrimaryNotification.tsx). | Providers opens with applicable context. Notifications require a synthetic update fixture; do not install or authenticate providers.                                                   |
| Settings-to-provider links                    | Follow provider links from General, [ProjectDefaultsSettings](../../apps/web/src/components/settings/ProjectDefaultsSettings.tsx) and [SourceControlWritingSettings](../../apps/web/src/components/settings/SourceControlWritingSettings.tsx).                                                                                                                                                                                                         | Inspect Providers scope; return without changing provider configuration.                                                                                                               |
| Source-control action                         | Follow [GitActionsControl](../../apps/web/src/components/GitActionsControl.tsx)'s settings action; separately invoke the palette's source-control settings action.                                                                                                                                                                                                                                                                                     | Source Control opens; no Git publication is part of this check.                                                                                                                        |
| Connections recovery/onboarding               | Use **Open Connections** on the hosted-static empty index; separately use the palette's connection action, [PermissionUpdateNotice](../../apps/web/src/components/PermissionUpdateNotice.tsx), [ServerUpdateAction](../../apps/web/src/components/ServerUpdateAction.tsx) and ScheduledTasksSettings' connection link.                                                                                                                                 | Connections appears. Each notice requires its own synthetic state; no live route, token or permission changes.                                                                         |
| Scheduled-task links from chat                | Follow the scheduled-task link in [MessagesTimeline](../../apps/web/src/components/chat/MessagesTimeline.tsx) and each applicable action in [ThreadAutomationsPanel](../../apps/web/src/components/chat/ThreadAutomationsPanel.tsx).                                                                                                                                                                                                                   | Scheduled Tasks and its intended target open. Creating/running jobs is outside navigation proof.                                                                                       |
| Native SnapShot setup resume                  | In a separately qualified native fixture, exercise the startup resume condition in [the root route](../../apps/web/src/routes/__root.tsx).                                                                                                                                                                                                                                                                                                             | Redirect to SnapShots when the bridge and resume state apply. Blocked without the native fixture; a direct browser URL is not a substitute.                                            |
| Reversible device setting                     | On the isolated client, change one device preference, navigate away/back and reload; restore the recorded original value and reload again.                                                                                                                                                                                                                                                                                                             | Both change and restoration persist. **Restore device defaults** affects more than one preference; use it only when the entire client fixture is disposable and its scope is intended. |

No universal settings reset is supplied. Per-setting reset controls in
settingsLayout distinguish defaults from inherited values; neither is a reset
of unrelated server/project state. Avoid storage cleanup, update installation,
provider login and scheduled execution as navigation fixtures. When a safe
fixture/reset is absent, leave that journey blocked rather than weakening it.

## Evidence record

Source check: **2026-10-20**, Jones Code
`e7527d88ef438daa443e49a84d59e9891415918b`. Sibling routes and entry callers were
inspected; no runtime action was executed. All paths and behavior expectations
remain `unverified`. Runtime execution is `blocked` in this docs-only lane:
there is no owned running candidate, paired tab or seeded scope/notification
fixture. Build, viewport/device/text size, harness run and captures: **none**.

The primary verification owner must record expected/observed results for the
specific paths exercised. Native capture/menu delivery, external authentication,
remote connection variants, setting writes and untested shared consumers remain
gaps. This map addition is not a full maintenance pass or product qualification.
